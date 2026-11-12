#include "buffer.h"

#include <stdlib.h>
#include <string.h>

void buf_free(struct buf *b)
{
    free(b->data);
    b->data = NULL;
    b->off = b->len = b->cap = 0;
}

int buf_reserve(struct buf *b, size_t extra)
{
    if (b->cap - b->len >= extra)
        return 0;

    /* Reclaim consumed space at the front before growing. */
    if (b->off > 0) {
        size_t used = b->len - b->off;
        memmove(b->data, b->data + b->off, used);
        b->len = used;
        b->off = 0;
        if (b->cap - b->len >= extra)
            return 0;
    }

    size_t need = b->len + extra;
    if (need < b->len)
        return -1; /* overflow */
    size_t ncap = b->cap ? b->cap : 4096;
    while (ncap < need) {
        if (ncap > SIZE_MAX / 2)
            return -1;
        ncap *= 2;
    }
    uint8_t *n = realloc(b->data, ncap);
    if (!n)
        return -1;
    b->data = n;
    b->cap = ncap;
    return 0;
}

int buf_append(struct buf *b, const void *p, size_t n)
{
    if (n == 0)
        return 0;
    if (buf_reserve(b, n) < 0)
        return -1;
    memcpy(b->data + b->len, p, n);
    b->len += n;
    return 0;
}

void buf_consume(struct buf *b, size_t n)
{
    b->off += n;
    if (b->off >= b->len)
        b->off = b->len = 0;
}
