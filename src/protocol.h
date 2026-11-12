#ifndef PUBSUB_PROTOCOL_H
#define PUBSUB_PROTOCOL_H

/*
 * Wire format (all integers big-endian):
 *
 *   +----------------+---------+----------------------------+
 *   | body_len (u32) | type u8 | payload (body_len - 1 B)   |
 *   +----------------+---------+----------------------------+
 *
 * body_len counts the type byte plus the payload, so it is always >= 1.
 *
 * Client -> server
 *   SUB    payload = topic
 *   UNSUB  payload = topic
 *   PUB    payload = topic_len (u8) | topic | message
 *   PING   payload = anything (echoed back in PONG)
 *   STATS  payload = empty
 *
 * Server -> client
 *   OK           payload = type byte of the acknowledged request
 *   MSG          payload = topic_len (u8) | topic | message  (same layout as PUB)
 *   PONG         payload = PING payload
 *   STATS_REPLY  payload = ASCII "key=value key=value ..."
 *   ERR          payload = error code (u8) | ASCII reason
 */

#include <stddef.h>
#include <stdint.h>

#define PS_HEADER_LEN 4u
#define PS_DEFAULT_PORT 7400
#define PS_DEFAULT_MAX_FRAME 65536u
#define PS_MAX_TOPIC_LEN 64u

enum ps_type {
    PS_SUB = 0x01,
    PS_UNSUB = 0x02,
    PS_PUB = 0x03,
    PS_PING = 0x04,
    PS_STATS = 0x05,
    PS_OK = 0x81,
    PS_MSG = 0x82,
    PS_PONG = 0x83,
    PS_STATS_REPLY = 0x84,
    PS_ERR = 0x85,
};

enum ps_error {
    PS_E_BAD_FRAME = 1,      /* malformed frame (fatal for empty frames)  */
    PS_E_BAD_TOPIC = 2,      /* topic empty, too long or not printable    */
    PS_E_NOT_SUBSCRIBED = 3, /* UNSUB for a topic the client never joined */
    PS_E_UNKNOWN_TYPE = 4,   /* unsupported frame type                    */
    PS_E_TOO_LARGE = 5,      /* body_len above the server limit (fatal)   */
    PS_E_LIMIT = 6,          /* per-connection subscription limit hit     */
};

static inline uint32_t ps_get_u32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8) | (uint32_t)p[3];
}

static inline void ps_put_u32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)v;
}

/* Topics are 1-64 bytes of printable, non-space ASCII. */
static inline int ps_topic_valid(const uint8_t *t, size_t len)
{
    if (len == 0 || len > PS_MAX_TOPIC_LEN)
        return 0;
    for (size_t i = 0; i < len; i++)
        if (t[i] < 0x21 || t[i] > 0x7e)
            return 0;
    return 1;
}

#endif
