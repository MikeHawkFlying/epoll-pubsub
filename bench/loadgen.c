/*
 * loadgen: load generator + correctness checker for pubsub-server.
 *
 * Opens S subscriber and P publisher connections from one epoll loop.
 * Subscriber i follows topic "t<i % T>"; publishers round-robin over all
 * topics at a target rate. Every message carries (publisher id, sequence,
 * send timestamp), so each subscriber can measure end-to-end latency and
 * detect lost, duplicated or reordered messages.
 *
 * Timeline:  connect + subscribe | warmup | measure | drain
 * Only messages *published* inside the measure window count toward the
 * reported rate and latency.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <time.h>
#include <unistd.h>

#include "buffer.h"
#include "protocol.h"

#define HIST_MAX_US 1000000u /* 1 s, slower samples go in the last bucket */
#define READ_CHUNK (64 * 1024)
#define PUB_HIGH_WATER (256 * 1024)
#define MSG_HEADER 20u /* u32 publisher id + u64 seq + u64 send time (ns) */
#define TIMER_ID UINT32_MAX
#define NS_PER_SEC 1000000000ull

enum role { ROLE_SUB, ROLE_PUB };

struct peer {
    int fd;
    enum role role;
    int idx;
    int topic; /* subscribers only */
    bool connected;
    bool subscribed;
    bool closed;
    uint32_t evmask;
    struct buf rbuf;
    struct buf wbuf;
    uint64_t *next_seq; /* subscriber: next expected seq per publisher */
    uint64_t *seq;      /* publisher: next seq per topic */
    double credit;      /* publisher: messages it may send now */
    int next_topic;     /* publisher: round-robin cursor */
};

static struct {
    const char *host;
    int port;
    int subs, pubs, topics;
    double rate; /* publishes per second, all publishers together; 0 = as fast as possible */
    int warmup, duration, drain;
    size_t msg_size;
    const char *json;
    bool verify;
    bool quiet;
} opt = {
    .host = "127.0.0.1",
    .port = PS_DEFAULT_PORT,
    .subs = 1000,
    .pubs = 10,
    .topics = 10,
    .rate = 1000,
    .warmup = 2,
    .duration = 10,
    .drain = 5,
    .msg_size = 64,
};

static struct {
    uint64_t connected, subscribed;
    uint64_t published, published_window;
    uint64_t delivered, delivered_window;
    uint64_t gaps, dups, disconnects, errors;
    uint64_t samples, max_us;
} st;

static struct peer *peers;
static int npeers;
static int epfd, tfd;
static struct sockaddr_in server_addr;
static uint64_t *hist;
static int *subs_per_topic;
static uint64_t window_start, window_end;
static bool publishing;
static uint8_t readbuf[READ_CHUNK];

static void die(const char *what)
{
    perror(what);
    exit(1);
}

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * NS_PER_SEC + (uint64_t)ts.tv_nsec;
}

static int topic_name(char *out, size_t cap, int t)
{
    return snprintf(out, cap, "t%d", t);
}

static void peer_close(struct peer *p, const char *why)
{
    if (p->closed)
        return;
    p->closed = true;
    st.disconnects++;
    if (!opt.quiet && st.disconnects <= 5)
        fprintf(stderr, "%s %d disconnected: %s\n", p->role == ROLE_SUB ? "sub" : "pub",
                p->idx, why);
    epoll_ctl(epfd, EPOLL_CTL_DEL, p->fd, NULL);
    close(p->fd);
}

static void peer_set_events(struct peer *p)
{
    if (p->closed)
        return;
    uint32_t want = EPOLLIN;
    if (!p->connected || buf_used(&p->wbuf) > 0)
        want |= EPOLLOUT;
    if (want == p->evmask)
        return;
    struct epoll_event ev = { .events = want, .data.u32 = (uint32_t)(p - peers) };
    if (epoll_ctl(epfd, EPOLL_CTL_MOD, p->fd, &ev) < 0)
        die("epoll_ctl");
    p->evmask = want;
}

static void peer_flush(struct peer *p)
{
    while (!p->closed && buf_used(&p->wbuf) > 0) {
        ssize_t n = send(p->fd, buf_head(&p->wbuf), buf_used(&p->wbuf), MSG_NOSIGNAL);
        if (n > 0) {
            buf_consume(&p->wbuf, (size_t)n);
            continue;
        }
        if (n < 0 && errno == EINTR)
            continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            break;
        peer_close(p, "send failed");
        return;
    }
    peer_set_events(p);
}

static void queue_frame(struct peer *p, uint8_t type, const void *payload, size_t n)
{
    uint8_t hdr[5];
    ps_put_u32(hdr, (uint32_t)(n + 1));
    hdr[4] = type;
    if (buf_append(&p->wbuf, hdr, sizeof hdr) < 0 || buf_append(&p->wbuf, payload, n) < 0)
        die("out of memory");
}

static void publish_one(struct peer *p, uint64_t now)
{
    int t = p->next_topic;
    p->next_topic = (p->next_topic + 1) % opt.topics;

    char topic[16];
    size_t tl = (size_t)topic_name(topic, sizeof topic, t);
    size_t body = 1 + 1 + tl + opt.msg_size; /* type, topic_len, topic, message */
    if (buf_reserve(&p->wbuf, PS_HEADER_LEN + body) < 0)
        die("out of memory");

    uint8_t *w = p->wbuf.data + p->wbuf.len;
    ps_put_u32(w, (uint32_t)body);
    w[4] = PS_PUB;
    w[5] = (uint8_t)tl;
    memcpy(w + 6, topic, tl);
    uint8_t *m = w + 6 + tl;
    uint32_t id = (uint32_t)p->idx;
    uint64_t seq = p->seq[t]++;
    memcpy(m, &id, 4);
    memcpy(m + 4, &seq, 8);
    memcpy(m + 12, &now, 8);
    memset(m + MSG_HEADER, 'x', opt.msg_size - MSG_HEADER);
    p->wbuf.len += PS_HEADER_LEN + body;

    st.published++;
    if (now >= window_start && now < window_end)
        st.published_window++;
}

static void on_msg(struct peer *p, const uint8_t *pl, size_t n, uint64_t now)
{
    if (n < 1 || n < 1 + (size_t)pl[0] + MSG_HEADER) {
        st.errors++;
        return;
    }
    const uint8_t *m = pl + 1 + pl[0];
    uint32_t id;
    uint64_t seq, sent;
    memcpy(&id, m, 4);
    memcpy(&seq, m + 4, 8);
    memcpy(&sent, m + 12, 8);
    st.delivered++;

    if (id >= (uint32_t)opt.pubs) {
        st.errors++;
        return;
    }
    uint64_t *next = &p->next_seq[id];
    if (seq == *next) {
        (*next)++;
    } else if (seq > *next) {
        st.gaps += seq - *next; /* everything in between went missing */
        *next = seq + 1;
    } else {
        st.dups++; /* duplicate or reordered */
    }

    if (sent >= window_start && sent < window_end) {
        uint64_t us = now > sent ? (now - sent) / 1000 : 0;
        if (us > st.max_us)
            st.max_us = us; /* exact, even past the histogram range */
        hist[us > HIST_MAX_US ? HIST_MAX_US : us]++;
        st.samples++;
        st.delivered_window++;
    }
}

static void on_frame(struct peer *p, uint8_t type, const uint8_t *pl, size_t n, uint64_t now)
{
    switch (type) {
    case PS_OK:
        if (p->role == ROLE_SUB && !p->subscribed) {
            p->subscribed = true;
            st.subscribed++;
        }
        break;
    case PS_MSG:
        on_msg(p, pl, n, now);
        break;
    case PS_ERR:
        st.errors++;
        if (!opt.quiet && st.errors <= 5)
            fprintf(stderr, "server error %u: %.*s\n", n ? pl[0] : 0u,
                    n > 1 ? (int)(n - 1) : 0, (const char *)pl + 1);
        break;
    default:
        break;
    }
}

static size_t parse_frames(struct peer *p, const uint8_t *d, size_t len, uint64_t now)
{
    size_t off = 0;
    while (len - off >= PS_HEADER_LEN) {
        uint32_t body = ps_get_u32(d + off);
        if (body == 0 || body > (16u << 20)) {
            fprintf(stderr, "bad frame length %u from server\n", body);
            exit(1);
        }
        if (len - off - PS_HEADER_LEN < body)
            break;
        const uint8_t *f = d + off + PS_HEADER_LEN;
        on_frame(p, f[0], f + 1, body - 1, now);
        off += PS_HEADER_LEN + body;
    }
    return off;
}

static void peer_read(struct peer *p)
{
    for (int i = 0; i < 8 && !p->closed; i++) {
        ssize_t n = recv(p->fd, readbuf, sizeof readbuf, 0);
        if (n == 0) {
            peer_close(p, "closed by server");
            return;
        }
        if (n < 0) {
            if (errno == EINTR)
                continue;
            if (errno != EAGAIN && errno != EWOULDBLOCK)
                peer_close(p, strerror(errno));
            return;
        }
        uint64_t now = now_ns();
        if (buf_used(&p->rbuf) == 0) {
            size_t used = parse_frames(p, readbuf, (size_t)n, now);
            if (buf_append(&p->rbuf, readbuf + used, (size_t)n - used) < 0)
                die("out of memory");
        } else {
            if (buf_append(&p->rbuf, readbuf, (size_t)n) < 0)
                die("out of memory");
            buf_consume(&p->rbuf, parse_frames(p, buf_head(&p->rbuf), buf_used(&p->rbuf), now));
        }
        if ((size_t)n < sizeof readbuf)
            return;
    }
}

static void peer_start(struct peer *p)
{
    p->fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (p->fd < 0)
        die("socket");
    int one = 1;
    setsockopt(p->fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    if (connect(p->fd, (struct sockaddr *)&server_addr, sizeof server_addr) < 0 &&
        errno != EINPROGRESS)
        die("connect");
    p->evmask = EPOLLIN | EPOLLOUT; /* writable == connect finished */
    struct epoll_event ev = { .events = p->evmask, .data.u32 = (uint32_t)(p - peers) };
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, p->fd, &ev) < 0)
        die("epoll_ctl");
}

static void peer_connected(struct peer *p)
{
    int err = 0;
    socklen_t len = sizeof err;
    if (getsockopt(p->fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0)
        err = errno;
    if (err) {
        fprintf(stderr, "connect: %s\n", strerror(err));
        exit(1);
    }
    p->connected = true;
    st.connected++;
    if (p->role == ROLE_SUB) {
        char topic[16];
        int tl = topic_name(topic, sizeof topic, p->topic);
        queue_frame(p, PS_SUB, topic, (size_t)tl);
    }
    peer_flush(p);
}

static void handle_event(uint32_t idx, uint32_t events)
{
    struct peer *p = &peers[idx];
    if (p->closed)
        return;
    if (!p->connected) {
        if (events & (EPOLLOUT | EPOLLERR | EPOLLHUP))
            peer_connected(p);
        return;
    }
    if (events & EPOLLIN)
        peer_read(p);
    if (p->closed)
        return;
    if (events & (EPOLLERR | EPOLLHUP)) {
        peer_close(p, "socket error");
        return;
    }
    if (events & EPOLLOUT)
        peer_flush(p);
}

/* One epoll_wait. Returns true if the 1 ms timer fired. */
static bool pump(int timeout_ms)
{
    struct epoll_event evs[1024];
    bool ticked = false;
    int n = epoll_wait(epfd, evs, 1024, timeout_ms);
    if (n < 0) {
        if (errno == EINTR)
            return false;
        die("epoll_wait");
    }
    for (int i = 0; i < n; i++) {
        if (evs[i].data.u32 == TIMER_ID) {
            uint64_t expirations;
            if (read(tfd, &expirations, sizeof expirations) > 0)
                ticked = true;
            continue;
        }
        handle_event(evs[i].data.u32, evs[i].events);
    }
    return ticked;
}

static void on_tick(uint64_t now, uint64_t *last)
{
    double dt = (double)(now - *last) / (double)NS_PER_SEC;
    *last = now;
    double per_pub = opt.rate / opt.pubs;

    for (int i = opt.subs; i < npeers; i++) {
        struct peer *p = &peers[i];
        if (p->closed)
            continue;
        if (opt.rate > 0) {
            /* Never burst more than a second's worth (or one message, for
             * rates below one per second per publisher). */
            double cap = per_pub > 1.0 ? per_pub : 1.0;
            p->credit += per_pub * dt;
            if (p->credit > cap)
                p->credit = cap;
        } else {
            p->credit = 1e9;
        }
        /* Stop generating when the socket is backed up, so we measure what the
         * server can absorb instead of growing our own buffer forever. */
        while (p->credit >= 1.0 && buf_used(&p->wbuf) < PUB_HIGH_WATER) {
            publish_one(p, now);
            p->credit -= 1.0;
        }
        if (opt.rate <= 0)
            p->credit = 0;
        peer_flush(p);
    }
}

static uint64_t expected_deliveries(void)
{
    uint64_t total = 0;
    for (int i = opt.subs; i < npeers; i++)
        for (int t = 0; t < opt.topics; t++)
            total += peers[i].seq[t] * (uint64_t)subs_per_topic[t];
    return total;
}

static uint64_t count_lost(void)
{
    uint64_t lost = st.gaps;
    for (int i = 0; i < opt.subs; i++) {
        struct peer *s = &peers[i];
        for (int j = 0; j < opt.pubs; j++) {
            uint64_t sent = peers[opt.subs + j].seq[s->topic];
            if (sent > s->next_seq[j])
                lost += sent - s->next_seq[j]; /* never arrived at all */
        }
    }
    return lost;
}

static uint64_t percentile(double q)
{
    if (st.samples == 0)
        return 0;
    uint64_t target = (uint64_t)(q * (double)st.samples);
    if (target >= st.samples)
        target = st.samples - 1;
    uint64_t seen = 0;
    for (uint64_t us = 0; us <= HIST_MAX_US; us++) {
        seen += hist[us];
        if (seen > target)
            return us;
    }
    return HIST_MAX_US;
}

static void raise_fd_limit(void)
{
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur < rl.rlim_max) {
        rl.rlim_cur = rl.rlim_max;
        setrlimit(RLIMIT_NOFILE, &rl);
    }
}

static void usage(const char *prog)
{
    fprintf(stderr,
            "usage: %s [options]\n"
            "  -H host      server address (default 127.0.0.1)\n"
            "  -p port      server port (default %d)\n"
            "  -s n         subscriber connections (default 1000)\n"
            "  -P n         publisher connections (default 10)\n"
            "  -t n         topics; subscriber i follows topic i %% n (default 10)\n"
            "  -r rate      publishes/sec across all publishers, 0 = unlimited (default 1000)\n"
            "  -m bytes     message size, >= %u (default 64)\n"
            "  -W secs      warmup (default 2)\n"
            "  -d secs      measured window (default 10)\n"
            "  -D secs      max time to wait for in-flight messages (default 5)\n"
            "  -j file      also write results as JSON\n"
            "  -V           verify mode: exit 3 on any loss, duplicate or disconnect\n"
            "  -q           quiet\n",
            prog, PS_DEFAULT_PORT, MSG_HEADER);
}

static int parse_int(const char *s, int lo, int hi, int *out)
{
    char *end;
    long v = strtol(s, &end, 10);
    if (end == s || *end || v < lo || v > hi)
        return -1;
    *out = (int)v;
    return 0;
}

int main(int argc, char **argv)
{
    int c, tmp;
    while ((c = getopt(argc, argv, "H:p:s:P:t:r:m:W:d:D:j:Vqh")) != -1) {
        int bad = 0;
        switch (c) {
        case 'H': opt.host = optarg; break;
        case 'p': bad = parse_int(optarg, 1, 65535, &opt.port); break;
        case 's': bad = parse_int(optarg, 1, 1000000, &opt.subs); break;
        case 'P': bad = parse_int(optarg, 1, 10000, &opt.pubs); break;
        case 't': bad = parse_int(optarg, 1, 100000, &opt.topics); break;
        case 'r': opt.rate = strtod(optarg, NULL); bad = opt.rate < 0; break;
        case 'm': bad = parse_int(optarg, (int)MSG_HEADER, 60000, &tmp); opt.msg_size = (size_t)tmp; break;
        case 'W': bad = parse_int(optarg, 0, 3600, &opt.warmup); break;
        case 'd': bad = parse_int(optarg, 1, 3600, &opt.duration); break;
        case 'D': bad = parse_int(optarg, 0, 3600, &opt.drain); break;
        case 'j': opt.json = optarg; break;
        case 'V': opt.verify = true; break;
        case 'q': opt.quiet = true; break;
        case 'h': usage(argv[0]); return 0;
        default: bad = 1; break;
        }
        if (bad) {
            usage(argv[0]);
            return 2;
        }
    }

    raise_fd_limit();
    memset(&server_addr, 0, sizeof server_addr);
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons((uint16_t)opt.port);
    if (inet_pton(AF_INET, opt.host, &server_addr.sin_addr) != 1) {
        fprintf(stderr, "bad host: %s\n", opt.host);
        return 2;
    }

    npeers = opt.subs + opt.pubs;
    peers = calloc((size_t)npeers, sizeof *peers);
    hist = calloc(HIST_MAX_US + 1, sizeof *hist);
    subs_per_topic = calloc((size_t)opt.topics, sizeof *subs_per_topic);
    epfd = epoll_create1(EPOLL_CLOEXEC);
    if (!peers || !hist || !subs_per_topic || epfd < 0)
        die("setup");

    uint64_t t0 = now_ns();
    for (int i = 0; i < npeers; i++) {
        struct peer *p = &peers[i];
        if (i < opt.subs) {
            p->role = ROLE_SUB;
            p->idx = i;
            p->topic = i % opt.topics;
            p->next_seq = calloc((size_t)opt.pubs, sizeof *p->next_seq);
            if (!p->next_seq)
                die("calloc");
            subs_per_topic[p->topic]++;
        } else {
            p->role = ROLE_PUB;
            p->idx = i - opt.subs;
            p->next_topic = p->idx % opt.topics;
            /* Stagger publishers across one send interval. If they all start
             * at zero credit they fire on the same tick, and the burst lands
             * on hundreds of sockets that this one thread then has to read in
             * turn, which shows up as fake server latency. */
            p->credit = (double)p->idx / opt.pubs;
            p->seq = calloc((size_t)opt.topics, sizeof *p->seq);
            if (!p->seq)
                die("calloc");
        }
        peer_start(p);
        if (i % 256 == 255)
            pump(0); /* let the first connections finish while we open more */
    }

    /* Wait until every connection is up and every SUB is acknowledged, so no
     * message can be published to a topic before its subscribers exist. */
    uint64_t setup_deadline = now_ns() + 120 * NS_PER_SEC;
    while (st.connected < (uint64_t)npeers || st.subscribed < (uint64_t)opt.subs) {
        if (now_ns() > setup_deadline || st.disconnects > 0) {
            fprintf(stderr, "setup failed: %" PRIu64 "/%d connected, %" PRIu64 "/%d subscribed\n",
                    st.connected, npeers, st.subscribed, opt.subs);
            return 1;
        }
        pump(50);
    }
    double setup_s = (double)(now_ns() - t0) / (double)NS_PER_SEC;

    tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    struct itimerspec its = { .it_interval = { 0, 1000000 }, .it_value = { 0, 1000000 } };
    struct epoll_event tev = { .events = EPOLLIN, .data.u32 = TIMER_ID };
    if (tfd < 0 || timerfd_settime(tfd, 0, &its, NULL) < 0 ||
        epoll_ctl(epfd, EPOLL_CTL_ADD, tfd, &tev) < 0)
        die("timerfd");

    uint64_t start = now_ns();
    window_start = start + (uint64_t)opt.warmup * NS_PER_SEC;
    window_end = window_start + (uint64_t)opt.duration * NS_PER_SEC;
    uint64_t drain_end = window_end + (uint64_t)opt.drain * NS_PER_SEC;
    uint64_t last_tick = start;
    uint64_t expected = 0;
    publishing = true;

    for (;;) {
        bool ticked = pump(5);
        uint64_t now = now_ns();
        if (publishing && now >= window_end) {
            publishing = false;
            expected = expected_deliveries();
        }
        if (publishing) {
            if (ticked)
                on_tick(now, &last_tick);
        } else if (now >= drain_end || st.delivered >= expected) {
            break;
        }
    }

    uint64_t lost = count_lost();
    double window_s = (double)opt.duration;
    double pub_rate = (double)st.published_window / window_s;
    double del_rate = (double)st.delivered_window / window_s;
    uint64_t p50 = percentile(0.50), p90 = percentile(0.90), p99 = percentile(0.99),
             p999 = percentile(0.999);

    printf("connections   %d (%d subscribers, %d publishers, %d topics)\n", npeers, opt.subs,
           opt.pubs, opt.topics);
    printf("setup         %.2f s\n", setup_s);
    printf("published     %.0f msgs/s (target %.0f)\n", pub_rate, opt.rate);
    printf("delivered     %.0f msgs/s\n", del_rate);
    printf("latency (us)  p50=%" PRIu64 " p90=%" PRIu64 " p99=%" PRIu64 " p99.9=%" PRIu64
           " max=%" PRIu64 "\n", p50, p90, p99, p999, st.max_us);
    printf("checks        lost=%" PRIu64 " dup/reordered=%" PRIu64 " disconnects=%" PRIu64
           " errors=%" PRIu64 "\n", lost, st.dups, st.disconnects, st.errors);

    if (opt.json) {
        FILE *f = fopen(opt.json, "w");
        if (!f)
            die(opt.json);
        fprintf(f,
                "{\"subscribers\":%d,\"publishers\":%d,\"topics\":%d,\"connections\":%d,"
                "\"msg_size\":%zu,\"target_rate\":%.0f,\"duration_s\":%d,\"setup_s\":%.2f,"
                "\"published_per_sec\":%.0f,\"delivered_per_sec\":%.0f,"
                "\"p50_us\":%" PRIu64 ",\"p90_us\":%" PRIu64 ",\"p99_us\":%" PRIu64
                ",\"p999_us\":%" PRIu64 ",\"max_us\":%" PRIu64 ",\"published\":%" PRIu64
                ",\"delivered\":%" PRIu64 ",\"lost\":%" PRIu64 ",\"dups\":%" PRIu64
                ",\"disconnects\":%" PRIu64 ",\"errors\":%" PRIu64 "}\n",
                opt.subs, opt.pubs, opt.topics, npeers, opt.msg_size, opt.rate, opt.duration,
                setup_s, pub_rate, del_rate, p50, p90, p99, p999, st.max_us, st.published,
                st.delivered, lost, st.dups, st.disconnects, st.errors);
        fclose(f);
    }

    if (opt.verify && st.delivered == 0) {
        fprintf(stderr, "verify: FAILED, nothing was delivered so nothing was checked\n");
        return 3;
    }
    if (opt.verify && (lost || st.dups || st.disconnects || st.errors)) {
        fprintf(stderr, "verify: FAILED\n");
        return 3;
    }
    return 0;
}
