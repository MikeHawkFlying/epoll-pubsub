#ifndef PUBSUB_TOPICS_H
#define PUBSUB_TOPICS_H

#include <stddef.h>
#include <stdint.h>

#include "protocol.h"

struct conn;

/* A topic and the connections subscribed to it. Topics exist only while
 * they have at least one subscriber. */
struct topic {
    struct topic *next; /* hash-chain link */
    struct conn **subs; /* subscribers, unordered (swap-remove) */
    uint32_t nsubs;
    uint32_t cap;
    uint32_t hash;
    uint8_t len;
    uint8_t name[PS_MAX_TOPIC_LEN];
};

/* Chained hash table keyed by topic name (FNV-1a), doubled when the load
 * factor passes 2. */
struct topic_table {
    struct topic **buckets;
    size_t nbuckets; /* always a power of two */
    size_t count;
    size_t nsubscriptions;
};

int topics_init(struct topic_table *t, size_t nbuckets);
void topics_free(struct topic_table *t);
struct topic *topics_find(const struct topic_table *t, const uint8_t *name, size_t len);
struct topic *topics_get_or_create(struct topic_table *t, const uint8_t *name, size_t len);
int topic_add_sub(struct topic_table *t, struct topic *tp, struct conn *c);
/* Removes c from tp; frees tp (and unlinks it) when its last subscriber leaves. */
void topic_remove_sub(struct topic_table *t, struct topic *tp, struct conn *c);

#endif
