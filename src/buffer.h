#ifndef PUBSUB_BUFFER_H
#define PUBSUB_BUFFER_H

#include <stddef.h>
#include <stdint.h>

/*
 * Growable byte buffer. Valid bytes live in data[off, len). Consuming bytes
 * only advances `off`; the space is reclaimed lazily by buf_reserve(), so
 * consuming is O(1) and memmove happens at most once per growth step.
 */
struct buf {
    uint8_t *data;
    size_t off;
    size_t len;
    size_t cap;
};

void buf_free(struct buf *b);
int buf_reserve(struct buf *b, size_t extra);
int buf_append(struct buf *b, const void *p, size_t n);
void buf_consume(struct buf *b, size_t n);

static inline size_t buf_used(const struct buf *b)
{
    return b->len - b->off;
}

static inline uint8_t *buf_head(const struct buf *b)
{
    return b->data + b->off;
}

#endif
