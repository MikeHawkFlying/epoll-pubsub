#include "topics.h"

#include <stdlib.h>
#include <string.h>

static uint32_t fnv1a(const uint8_t *p, size_t n)
{
    uint32_t h = 2166136261u;
    for (size_t i = 0; i < n; i++) {
        h ^= p[i];
        h *= 16777619u;
    }
    return h;
}

int topics_init(struct topic_table *t, size_t nbuckets)
{
    memset(t, 0, sizeof *t);
    size_t n = 16;
    while (n < nbuckets)
        n *= 2;
    t->buckets = calloc(n, sizeof *t->buckets);
    if (!t->buckets)
        return -1;
    t->nbuckets = n;
    return 0;
}

void topics_free(struct topic_table *t)
{
    for (size_t i = 0; i < t->nbuckets; i++) {
        struct topic *tp = t->buckets[i];
        while (tp) {
            struct topic *next = tp->next;
            free(tp->subs);
            free(tp);
            tp = next;
        }
    }
    free(t->buckets);
    memset(t, 0, sizeof *t);
}

struct topic *topics_find(const struct topic_table *t, const uint8_t *name, size_t len)
{
    if (len == 0 || len > PS_MAX_TOPIC_LEN)
        return NULL;
    uint32_t h = fnv1a(name, len);
    for (struct topic *tp = t->buckets[h & (t->nbuckets - 1)]; tp; tp = tp->next)
        if (tp->hash == h && tp->len == len && memcmp(tp->name, name, len) == 0)
            return tp;
    return NULL;
}

static void topics_grow(struct topic_table *t)
{
    size_t n = t->nbuckets * 2;
    struct topic **nb = calloc(n, sizeof *nb);
    if (!nb)
        return; /* keep the old table; chains just get longer */
    for (size_t i = 0; i < t->nbuckets; i++) {
        struct topic *tp = t->buckets[i];
        while (tp) {
            struct topic *next = tp->next;
            size_t idx = tp->hash & (n - 1);
            tp->next = nb[idx];
            nb[idx] = tp;
            tp = next;
        }
    }
    free(t->buckets);
    t->buckets = nb;
    t->nbuckets = n;
}

struct topic *topics_get_or_create(struct topic_table *t, const uint8_t *name, size_t len)
{
    struct topic *tp = topics_find(t, name, len);
    if (tp)
        return tp;
    if (len == 0 || len > PS_MAX_TOPIC_LEN)
        return NULL;
    if (t->count >= t->nbuckets * 2)
        topics_grow(t);

    tp = calloc(1, sizeof *tp);
    if (!tp)
        return NULL;
    tp->hash = fnv1a(name, len);
    tp->len = (uint8_t)len;
    memcpy(tp->name, name, len);
    size_t idx = tp->hash & (t->nbuckets - 1);
    tp->next = t->buckets[idx];
    t->buckets[idx] = tp;
    t->count++;
    return tp;
}

int topic_add_sub(struct topic_table *t, struct topic *tp, struct conn *c)
{
    if (tp->nsubs == tp->cap) {
        uint32_t ncap = tp->cap ? tp->cap * 2 : 4;
        struct conn **n = realloc(tp->subs, ncap * sizeof *n);
        if (!n)
            return -1;
        tp->subs = n;
        tp->cap = ncap;
    }
    tp->subs[tp->nsubs++] = c;
    t->nsubscriptions++;
    return 0;
}

void topic_remove_sub(struct topic_table *t, struct topic *tp, struct conn *c)
{
    for (uint32_t i = 0; i < tp->nsubs; i++) {
        if (tp->subs[i] == c) {
            tp->subs[i] = tp->subs[--tp->nsubs];
            t->nsubscriptions--;
            break;
        }
    }
    if (tp->nsubs > 0)
        return;

    size_t idx = tp->hash & (t->nbuckets - 1);
    for (struct topic **pp = &t->buckets[idx]; *pp; pp = &(*pp)->next) {
        if (*pp == tp) {
            *pp = tp->next;
            free(tp->subs);
            free(tp);
            t->count--;
            return;
        }
    }
}
