/*
 * pubsub-server: a single-threaded TCP pub/sub broker built on epoll.
 *
 * One event loop owns every socket. Each turn of the loop:
 *   1. handles the ready fds (accept, read + parse frames, write),
 *   2. flushes every connection that got new output this turn (so a fan-out
 *      to N subscribers costs one send() per subscriber, not one per message),
 *   3. frees connections that died during the turn.
 * Freeing is deferred so a connection can never be freed while a later event
 * in the same epoll batch, or a fan-out loop, still points at it.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include "buffer.h"
#include "protocol.h"
#include "topics.h"

#define MAX_EVENTS 1024
#define READ_CHUNK (64 * 1024)
#define READS_PER_WAKEUP 4      /* keep one busy client from starving the rest */
#define ACCEPTS_PER_WAKEUP 256
#define MAX_TOPICS_PER_CONN 1024
#define IDLE_BUF_RELEASE (256 * 1024)
#define DRAIN_LIMIT_FRAMES 4    /* how much input we swallow while closing */

struct config {
    const char *bind_addr;
    int port;
    size_t max_clients;
    size_t max_frame;
    size_t max_wbuf;
    bool verbose;
};

struct conn {
    int fd;
    uint32_t evmask;        /* events currently registered with epoll */
    bool dead;              /* will be freed at the end of this loop turn */
    bool close_after_flush; /* protocol error: send what's queued, then close */
    bool draining;          /* ERR sent and write side shut; discarding input */
    size_t drained;         /* bytes discarded while draining */
    bool in_dirty;          /* already on the flush list for this turn */
    struct buf rbuf;        /* leftover bytes of a partially received frame */
    struct buf wbuf;        /* output not yet accepted by the kernel */
    struct topic **topics;  /* topics this connection is subscribed to */
    uint32_t ntopics;
    uint32_t topics_cap;
};

struct stats {
    uint64_t accepted, rejected;
    uint64_t msgs_in, msgs_out;
    uint64_t bytes_in, bytes_out;
    uint64_t slow_drops;
};

struct connlist {
    struct conn **items;
    size_t len, cap;
};

struct server {
    struct config cfg;
    int epfd, lfd, sfd, spare_fd;
    bool running;
    size_t nconns;
    struct conn **by_fd; /* fd -> connection */
    size_t by_fd_cap;
    struct topic_table topics;
    struct connlist dirty; /* got output this turn */
    struct connlist dead;  /* to free at the end of this turn */
    struct stats st;
};

static uint8_t readbuf[READ_CHUNK];

static void debugf(const struct server *s, const char *fmt, ...)
{
    if (!s->cfg.verbose)
        return;
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
}

static void connlist_push(struct connlist *l, struct conn *c)
{
    if (l->len == l->cap) {
        size_t ncap = l->cap ? l->cap * 2 : 64;
        struct conn **n = realloc(l->items, ncap * sizeof *n);
        if (!n) {
            perror("realloc");
            exit(1);
        }
        l->items = n;
        l->cap = ncap;
    }
    l->items[l->len++] = c;
}

static void conn_kill(struct server *s, struct conn *c)
{
    if (c->dead)
        return;
    c->dead = true;
    connlist_push(&s->dead, c);
}

static void conn_destroy(struct server *s, struct conn *c)
{
    for (uint32_t i = 0; i < c->ntopics; i++)
        topic_remove_sub(&s->topics, c->topics[i], c);
    free(c->topics);
    s->by_fd[c->fd] = NULL;
    close(c->fd);
    buf_free(&c->rbuf);
    buf_free(&c->wbuf);
    free(c);
    s->nconns--;
}

/* Level-triggered epoll: only ask for EPOLLOUT while there is something to
 * write, otherwise it would fire on every turn. */
static void conn_update_events(struct server *s, struct conn *c)
{
    if (c->dead)
        return;
    uint32_t want = 0;
    /* While an ERR is still on its way out we stop reading; once it is
     * written we read again, but only to drain (see conn_read). */
    if (!c->close_after_flush || c->draining)
        want |= EPOLLIN | EPOLLRDHUP;
    if (buf_used(&c->wbuf) > 0)
        want |= EPOLLOUT;
    if (want == c->evmask)
        return;
    struct epoll_event ev = { .events = want, .data.fd = c->fd };
    if (epoll_ctl(s->epfd, EPOLL_CTL_MOD, c->fd, &ev) < 0) {
        conn_kill(s, c);
        return;
    }
    c->evmask = want;
}

/* Queue one frame on c. Returns false if c is (or just became) dead. */
static bool conn_queue_frame(struct server *s, struct conn *c, uint8_t type,
                             const void *a, size_t alen, const void *b, size_t blen)
{
    if (c->dead || c->close_after_flush)
        return false; /* closing: nothing more after the ERR */
    size_t body = 1 + alen + blen;
    size_t total = PS_HEADER_LEN + body;

    /* A subscriber that stops reading would otherwise make us buffer forever. */
    if (buf_used(&c->wbuf) + total > s->cfg.max_wbuf) {
        s->st.slow_drops++;
        debugf(s, "fd %d: slow consumer (%zu bytes queued), dropping\n", c->fd,
               buf_used(&c->wbuf));
        conn_kill(s, c);
        return false;
    }
    if (buf_reserve(&c->wbuf, total) < 0) {
        conn_kill(s, c);
        return false;
    }
    uint8_t *p = c->wbuf.data + c->wbuf.len;
    ps_put_u32(p, (uint32_t)body);
    p[4] = type;
    if (alen)
        memcpy(p + 5, a, alen);
    if (blen)
        memcpy(p + 5 + alen, b, blen);
    c->wbuf.len += total;

    if (!c->in_dirty) {
        c->in_dirty = true;
        connlist_push(&s->dirty, c);
    }
    return true;
}

static void conn_ok(struct server *s, struct conn *c, uint8_t acked)
{
    conn_queue_frame(s, c, PS_OK, &acked, 1, NULL, 0);
}

static void conn_error(struct server *s, struct conn *c, uint8_t code, const char *why,
                       bool fatal)
{
    conn_queue_frame(s, c, PS_ERR, &code, 1, why, strlen(why));
    if (fatal)
        c->close_after_flush = true;
}

static void conn_flush(struct server *s, struct conn *c)
{
    while (buf_used(&c->wbuf) > 0) {
        ssize_t n = send(c->fd, buf_head(&c->wbuf), buf_used(&c->wbuf), MSG_NOSIGNAL);
        if (n > 0) {
            buf_consume(&c->wbuf, (size_t)n);
            s->st.bytes_out += (uint64_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR)
            continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            break;
        conn_kill(s, c);
        return;
    }
    if (buf_used(&c->wbuf) == 0) {
        if (c->close_after_flush && !c->draining) {
            /* Don't close() yet: with unread input still queued, Linux would
             * answer with an RST and the client could lose the ERR. Send a FIN
             * instead and swallow input until the client closes its side. */
            shutdown(c->fd, SHUT_WR);
            c->draining = true;
        }
        if (c->wbuf.cap > IDLE_BUF_RELEASE)
            buf_free(&c->wbuf); /* don't pin a big buffer on an idle client */
    }
    conn_update_events(s, c);
}

static bool conn_find_topic(const struct conn *c, const struct topic *tp, uint32_t *idx)
{
    for (uint32_t i = 0; i < c->ntopics; i++) {
        if (c->topics[i] == tp) {
            if (idx)
                *idx = i;
            return true;
        }
    }
    return false;
}

static void handle_sub(struct server *s, struct conn *c, const uint8_t *p, size_t n)
{
    if (!ps_topic_valid(p, n)) {
        conn_error(s, c, PS_E_BAD_TOPIC, "invalid topic", false);
        return;
    }
    struct topic *tp = topics_find(&s->topics, p, n);
    if (tp && conn_find_topic(c, tp, NULL)) {
        conn_ok(s, c, PS_SUB); /* already subscribed */
        return;
    }
    if (c->ntopics >= MAX_TOPICS_PER_CONN) {
        conn_error(s, c, PS_E_LIMIT, "too many subscriptions", false);
        return;
    }
    if (c->ntopics == c->topics_cap) {
        uint32_t ncap = c->topics_cap ? c->topics_cap * 2 : 4;
        struct topic **nt = realloc(c->topics, ncap * sizeof *nt);
        if (!nt) {
            conn_kill(s, c);
            return;
        }
        c->topics = nt;
        c->topics_cap = ncap;
    }
    if (!tp)
        tp = topics_get_or_create(&s->topics, p, n);
    if (!tp)
        goto oom;
    if (topic_add_sub(&s->topics, tp, c) < 0) {
        topic_remove_sub(&s->topics, tp, c); /* frees tp if we just made it */
        goto oom;
    }
    c->topics[c->ntopics++] = tp;
    conn_ok(s, c, PS_SUB);
    return;

oom:
    conn_kill(s, c);
}

static void handle_unsub(struct server *s, struct conn *c, const uint8_t *p, size_t n)
{
    if (!ps_topic_valid(p, n)) {
        conn_error(s, c, PS_E_BAD_TOPIC, "invalid topic", false);
        return;
    }
    struct topic *tp = topics_find(&s->topics, p, n);
    uint32_t idx;
    if (!tp || !conn_find_topic(c, tp, &idx)) {
        conn_error(s, c, PS_E_NOT_SUBSCRIBED, "not subscribed", false);
        return;
    }
    c->topics[idx] = c->topics[--c->ntopics];
    topic_remove_sub(&s->topics, tp, c);
    conn_ok(s, c, PS_UNSUB);
}

static void handle_pub(struct server *s, struct conn *c, const uint8_t *p, size_t n)
{
    if (n < 1 || (size_t)p[0] + 1 > n) {
        conn_error(s, c, PS_E_BAD_FRAME, "malformed PUB", false);
        return;
    }
    size_t tlen = p[0];
    if (!ps_topic_valid(p + 1, tlen)) {
        conn_error(s, c, PS_E_BAD_TOPIC, "invalid topic", false);
        return;
    }
    s->st.msgs_in++;
    struct topic *tp = topics_find(&s->topics, p + 1, tlen);
    if (!tp)
        return;
    /* MSG has the same payload layout as PUB, so the bytes are copied as-is.
     * A subscriber that overflows here is only marked dead, so tp->subs is
     * not modified while we walk it. */
    for (uint32_t i = 0; i < tp->nsubs; i++)
        if (conn_queue_frame(s, tp->subs[i], PS_MSG, p, n, NULL, 0))
            s->st.msgs_out++;
}

static void handle_stats(struct server *s, struct conn *c)
{
    char text[320];
    int len = snprintf(text, sizeof text,
                       "connections=%zu topics=%zu subscriptions=%zu "
                       "msgs_in=%" PRIu64 " msgs_out=%" PRIu64 " bytes_in=%" PRIu64
                       " bytes_out=%" PRIu64 " slow_drops=%" PRIu64 " rejected=%" PRIu64,
                       s->nconns, s->topics.count, s->topics.nsubscriptions,
                       s->st.msgs_in, s->st.msgs_out, s->st.bytes_in, s->st.bytes_out,
                       s->st.slow_drops, s->st.rejected);
    if (len < 0)
        return;
    if ((size_t)len >= sizeof text)
        len = (int)sizeof text - 1;
    conn_queue_frame(s, c, PS_STATS_REPLY, text, (size_t)len, NULL, 0);
}

static void handle_frame(struct server *s, struct conn *c, uint8_t type,
                         const uint8_t *p, size_t n)
{
    switch (type) {
    case PS_SUB:
        handle_sub(s, c, p, n);
        break;
    case PS_UNSUB:
        handle_unsub(s, c, p, n);
        break;
    case PS_PUB:
        handle_pub(s, c, p, n);
        break;
    case PS_PING:
        conn_queue_frame(s, c, PS_PONG, p, n, NULL, 0);
        break;
    case PS_STATS:
        handle_stats(s, c);
        break;
    default:
        conn_error(s, c, PS_E_UNKNOWN_TYPE, "unknown frame type", false);
        break;
    }
}

/* Dispatch every complete frame in data[0, len). Returns bytes consumed. */
static size_t conn_parse(struct server *s, struct conn *c, const uint8_t *data, size_t len)
{
    size_t off = 0;
    while (!c->dead && !c->close_after_flush && len - off >= PS_HEADER_LEN) {
        uint32_t body = ps_get_u32(data + off);
        if (body == 0) {
            conn_error(s, c, PS_E_BAD_FRAME, "empty frame", true);
            break;
        }
        if (body > s->cfg.max_frame) {
            conn_error(s, c, PS_E_TOO_LARGE, "frame too large", true);
            break;
        }
        if (len - off - PS_HEADER_LEN < body)
            break; /* wait for the rest of this frame */
        const uint8_t *f = data + off + PS_HEADER_LEN;
        handle_frame(s, c, f[0], f + 1, body - 1);
        off += PS_HEADER_LEN + body;
    }
    return off;
}

/* After a fatal error: read and throw away whatever the client is still
 * sending, until it closes or sends too much. */
static void conn_drain(struct server *s, struct conn *c)
{
    for (int i = 0; i < READS_PER_WAKEUP; i++) {
        ssize_t n = recv(c->fd, readbuf, sizeof readbuf, 0);
        if (n < 0 && errno == EINTR)
            continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            return;
        if (n <= 0) {
            conn_kill(s, c);
            return;
        }
        c->drained += (size_t)n;
        if (c->drained > DRAIN_LIMIT_FRAMES * s->cfg.max_frame) {
            conn_kill(s, c);
            return;
        }
    }
}

static void conn_read(struct server *s, struct conn *c)
{
    if (c->draining) {
        conn_drain(s, c);
        return;
    }
    for (int i = 0; i < READS_PER_WAKEUP && !c->dead && !c->close_after_flush; i++) {
        ssize_t n = recv(c->fd, readbuf, sizeof readbuf, 0);
        if (n == 0) {
            conn_kill(s, c);
            return;
        }
        if (n < 0) {
            if (errno == EINTR)
                continue;
            if (errno != EAGAIN && errno != EWOULDBLOCK)
                conn_kill(s, c);
            return;
        }
        s->st.bytes_in += (uint64_t)n;

        /* Common case: nothing left over from last time, so parse straight out
         * of the shared read buffer and only copy a trailing partial frame.
         * This keeps per-connection memory tiny with thousands of clients. */
        if (buf_used(&c->rbuf) == 0) {
            size_t used = conn_parse(s, c, readbuf, (size_t)n);
            if (used < (size_t)n && !c->dead && !c->close_after_flush &&
                buf_append(&c->rbuf, readbuf + used, (size_t)n - used) < 0) {
                conn_kill(s, c);
                return;
            }
        } else {
            if (buf_append(&c->rbuf, readbuf, (size_t)n) < 0) {
                conn_kill(s, c);
                return;
            }
            size_t used = conn_parse(s, c, buf_head(&c->rbuf), buf_used(&c->rbuf));
            buf_consume(&c->rbuf, used);
        }
        if (buf_used(&c->rbuf) == 0 && c->rbuf.cap > IDLE_BUF_RELEASE)
            buf_free(&c->rbuf);
        if ((size_t)n < sizeof readbuf)
            return; /* socket drained */
    }
}

static int ensure_fd_slot(struct server *s, int fd)
{
    size_t need = (size_t)fd + 1;
    if (need <= s->by_fd_cap)
        return 0;
    size_t ncap = s->by_fd_cap ? s->by_fd_cap : 1024;
    while (ncap < need)
        ncap *= 2;
    struct conn **n = realloc(s->by_fd, ncap * sizeof *n);
    if (!n)
        return -1;
    memset(n + s->by_fd_cap, 0, (ncap - s->by_fd_cap) * sizeof *n);
    s->by_fd = n;
    s->by_fd_cap = ncap;
    return 0;
}

static void server_accept(struct server *s)
{
    for (int i = 0; i < ACCEPTS_PER_WAKEUP; i++) {
        int fd = accept4(s->lfd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EINTR || errno == ECONNABORTED)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return;
            if (errno == EMFILE || errno == ENFILE) {
                /* Out of fds. Free the spare one, accept and immediately close
                 * the pending connection, then take the spare back. Otherwise
                 * the listener stays readable and we spin. */
                if (s->spare_fd >= 0) {
                    close(s->spare_fd);
                    s->spare_fd = -1;
                }
                int tmp = accept(s->lfd, NULL, NULL);
                if (tmp >= 0)
                    close(tmp);
                s->spare_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
                s->st.rejected++;
                return;
            }
            perror("accept4");
            return;
        }
        if (s->nconns >= s->cfg.max_clients) {
            close(fd);
            s->st.rejected++;
            continue;
        }
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);

        struct conn *c = calloc(1, sizeof *c);
        if (!c || ensure_fd_slot(s, fd) < 0) {
            free(c);
            close(fd);
            s->st.rejected++;
            continue;
        }
        c->fd = fd;
        c->evmask = EPOLLIN | EPOLLRDHUP;
        struct epoll_event ev = { .events = c->evmask, .data.fd = fd };
        if (epoll_ctl(s->epfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
            free(c);
            close(fd);
            continue;
        }
        s->by_fd[fd] = c;
        s->nconns++;
        s->st.accepted++;
    }
}

static void handle_conn_event(struct server *s, int fd, uint32_t events)
{
    if (fd < 0 || (size_t)fd >= s->by_fd_cap)
        return;
    struct conn *c = s->by_fd[fd];
    if (!c || c->dead)
        return;
    if (events & (EPOLLERR | EPOLLHUP)) {
        conn_kill(s, c);
        return;
    }
    if (events & EPOLLIN)
        conn_read(s, c);
    else if (events & EPOLLRDHUP) {
        conn_kill(s, c);
        return;
    }
    if (!c->dead && (events & EPOLLOUT))
        conn_flush(s, c);
}

static void server_end_turn(struct server *s)
{
    for (size_t i = 0; i < s->dirty.len; i++) {
        struct conn *c = s->dirty.items[i];
        c->in_dirty = false;
        if (!c->dead)
            conn_flush(s, c);
    }
    s->dirty.len = 0;

    for (size_t i = 0; i < s->dead.len; i++)
        conn_destroy(s, s->dead.items[i]);
    s->dead.len = 0;
}

static void server_shutdown(struct server *s)
{
    server_end_turn(s);
    for (size_t fd = 0; fd < s->by_fd_cap; fd++)
        if (s->by_fd[fd])
            conn_destroy(s, s->by_fd[fd]);

    fprintf(stderr,
            "shutdown: accepted=%" PRIu64 " rejected=%" PRIu64 " msgs_in=%" PRIu64
            " msgs_out=%" PRIu64 " slow_drops=%" PRIu64 "\n",
            s->st.accepted, s->st.rejected, s->st.msgs_in, s->st.msgs_out, s->st.slow_drops);

    topics_free(&s->topics);
    free(s->by_fd);
    free(s->dirty.items);
    free(s->dead.items);
    if (s->lfd >= 0)
        close(s->lfd);
    if (s->sfd >= 0)
        close(s->sfd);
    if (s->epfd >= 0)
        close(s->epfd);
    if (s->spare_fd >= 0)
        close(s->spare_fd);
}

static int setup_listener(const struct config *cfg, int *bound_port)
{
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0)
        return -1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)cfg->port);
    if (inet_pton(AF_INET, cfg->bind_addr, &addr.sin_addr) != 1) {
        fprintf(stderr, "bad bind address: %s\n", cfg->bind_addr);
        close(fd);
        return -1;
    }
    if (bind(fd, (struct sockaddr *)&addr, sizeof addr) < 0 || listen(fd, 4096) < 0) {
        perror("bind/listen");
        close(fd);
        return -1;
    }
    socklen_t alen = sizeof addr;
    if (getsockname(fd, (struct sockaddr *)&addr, &alen) < 0) {
        close(fd);
        return -1;
    }
    *bound_port = ntohs(addr.sin_port);
    return fd;
}

/* SIGINT/SIGTERM arrive as readable events on a signalfd, so shutdown runs
 * from the loop instead of from inside a signal handler. */
static int setup_signals(void)
{
    signal(SIGPIPE, SIG_IGN);
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    if (sigprocmask(SIG_BLOCK, &set, NULL) < 0)
        return -1;
    return signalfd(-1, &set, SFD_NONBLOCK | SFD_CLOEXEC);
}

static void raise_fd_limit(void)
{
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur < rl.rlim_max) {
        rl.rlim_cur = rl.rlim_max;
        setrlimit(RLIMIT_NOFILE, &rl);
    }
}

static int parse_num(const char *s, size_t *out)
{
    char *end;
    errno = 0;
    unsigned long long v = strtoull(s, &end, 10);
    if (errno || end == s || *end != '\0')
        return -1;
    *out = (size_t)v;
    return 0;
}

static void usage(const char *prog)
{
    fprintf(stderr,
            "usage: %s [-b addr] [-p port] [-c max_clients] [-f max_frame] [-w max_wbuf] [-v]\n"
            "  -b  address to bind (default 0.0.0.0)\n"
            "  -p  port, 0 picks a free one (default %d)\n"
            "  -c  max concurrent clients (default 10000)\n"
            "  -f  max frame body in bytes (default %u)\n"
            "  -w  max queued output per client before it is dropped (default 4 MiB)\n"
            "  -v  log to stderr\n",
            prog, PS_DEFAULT_PORT, PS_DEFAULT_MAX_FRAME);
}

int main(int argc, char **argv)
{
    struct server s;
    memset(&s, 0, sizeof s);
    s.cfg.bind_addr = "0.0.0.0";
    s.cfg.port = PS_DEFAULT_PORT;
    s.cfg.max_clients = 10000;
    s.cfg.max_frame = PS_DEFAULT_MAX_FRAME;
    s.cfg.max_wbuf = 4u << 20;
    s.lfd = s.sfd = s.epfd = s.spare_fd = -1;

    int opt;
    size_t v;
    while ((opt = getopt(argc, argv, "b:p:c:f:w:vh")) != -1) {
        switch (opt) {
        case 'b':
            s.cfg.bind_addr = optarg;
            break;
        case 'p':
            if (parse_num(optarg, &v) < 0 || v > 65535)
                goto bad_args;
            s.cfg.port = (int)v;
            break;
        case 'c':
            if (parse_num(optarg, &v) < 0 || v == 0)
                goto bad_args;
            s.cfg.max_clients = v;
            break;
        case 'f':
            if (parse_num(optarg, &v) < 0 || v < 2 || v > (16u << 20))
                goto bad_args;
            s.cfg.max_frame = v;
            break;
        case 'w':
            if (parse_num(optarg, &v) < 0 || v < 1024)
                goto bad_args;
            s.cfg.max_wbuf = v;
            break;
        case 'v':
            s.cfg.verbose = true;
            break;
        case 'h':
            usage(argv[0]);
            return 0;
        default:
            goto bad_args;
        }
    }

    if (s.cfg.max_wbuf < s.cfg.max_frame + PS_HEADER_LEN) {
        /* Otherwise one legal frame could get an idle subscriber dropped. */
        fprintf(stderr, "-w must be at least -f + %u\n", PS_HEADER_LEN);
        goto bad_args;
    }

    raise_fd_limit();
    int port = 0;
    if (topics_init(&s.topics, 1024) < 0) {
        perror("topics_init");
        return 1;
    }
    s.lfd = setup_listener(&s.cfg, &port);
    s.sfd = setup_signals();
    s.spare_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
    s.epfd = epoll_create1(EPOLL_CLOEXEC);
    if (s.lfd < 0 || s.sfd < 0 || s.epfd < 0) {
        perror("startup");
        server_shutdown(&s);
        return 1;
    }
    struct epoll_event ev = { .events = EPOLLIN, .data.fd = s.lfd };
    epoll_ctl(s.epfd, EPOLL_CTL_ADD, s.lfd, &ev);
    ev.data.fd = s.sfd;
    epoll_ctl(s.epfd, EPOLL_CTL_ADD, s.sfd, &ev);

    printf("listening on %s:%d\n", s.cfg.bind_addr, port);
    fflush(stdout);

    struct epoll_event events[MAX_EVENTS];
    s.running = true;
    while (s.running) {
        int n = epoll_wait(s.epfd, events, MAX_EVENTS, -1);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("epoll_wait");
            break;
        }
        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            if (fd == s.lfd) {
                server_accept(&s);
            } else if (fd == s.sfd) {
                struct signalfd_siginfo si;
                while (read(s.sfd, &si, sizeof si) == (ssize_t)sizeof si)
                    s.running = false;
            } else {
                handle_conn_event(&s, fd, events[i].events);
            }
        }
        server_end_turn(&s);
    }

    server_shutdown(&s);
    return 0;

bad_args:
    usage(argv[0]);
    return 2;
}
