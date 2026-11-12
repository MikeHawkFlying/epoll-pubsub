# epoll-pubsub

A small publish/subscribe message broker in C. One thread, one `epoll` loop,
non-blocking sockets, and a length-prefixed binary protocol over TCP.

I built this to understand what actually happens underneath the messaging
systems I work with day to day: how one process keeps thousands of sockets
busy, what partial reads and writes look like in practice, and what TCP does
when the network drops packets. It is a learning project, not a Kafka
replacement, but it is tested and benchmarked properly.

## What it does

- Clients `SUB`/`UNSUB` to topics and `PUB` messages; the server fans each
  message out to every subscriber of that topic.
- Thousands of concurrent connections from a single thread (level-triggered
  `epoll`, `accept4`, `signalfd`).
- Per-publisher ordering is preserved, and nothing is lost or duplicated even
  with 5% packet loss on the link (checked by `scripts/netem.sh`).
- Slow subscribers that stop reading get disconnected once 4 MiB of output is
  queued for them, so one bad client can't make the server buffer forever.
- `STATS` returns live counters; `SIGTERM` shuts down cleanly with no leaks.

## Protocol

Every frame is `[u32 body_len, big-endian][u8 type][payload]`, where
`body_len` counts the type byte plus the payload.

| type | dir | payload |
|---|---|---|
| `0x01 SUB` | c → s | topic |
| `0x02 UNSUB` | c → s | topic |
| `0x03 PUB` | c → s | `u8 topic_len`, topic, message |
| `0x04 PING` | c → s | anything, echoed back |
| `0x05 STATS` | c → s | empty |
| `0x81 OK` | s → c | type of the request being acknowledged |
| `0x82 MSG` | s → c | `u8 topic_len`, topic, message (same layout as PUB) |
| `0x83 PONG` | s → c | the PING payload |
| `0x84 STATS_REPLY` | s → c | `key=value ...` text |
| `0x85 ERR` | s → c | `u8 code`, reason text |

Topics are 1-64 printable ASCII characters. Frames are capped at 64 KiB by
default (`-f`). An empty or oversized frame gets an `ERR` and the connection
is closed; any other bad request gets an `ERR` and the connection stays open.

## How it works

```
             epoll_wait()
                  |
   +--------------+---------------+--------------+
   |              |               |              |
listen fd     client fds       signalfd       (timeouts: none)
accept4()     recv -> parse    SIGTERM ->
              frames ->        stop loop
              handle SUB/PUB
                  |
        PUB: copy MSG frame into each
        subscriber's output buffer and
        mark it "dirty"
                  |
   end of turn:  send() once per dirty connection,
                 then free connections that died
```

A few decisions I'd point out:

- **Level-triggered, not edge-triggered.** With edge-triggered epoll every
  read has to drain the socket or you miss the wakeup, which lets one busy
  client starve everyone else. Level-triggered with a small per-wakeup read
  budget (4 reads) is simpler and fair. `EPOLLOUT` is only registered while a
  connection actually has queued output.
- **Coalesced writes.** Fan-out just appends to each subscriber's buffer. All
  the `send()` calls happen once at the end of the loop turn, so a subscriber
  that receives 50 messages in one turn costs one syscall, not 50.
- **Deferred close.** A connection that errors (or overflows during a fan-out)
  is only marked dead and freed at the end of the turn. Nothing can be freed
  while a later event in the same `epoll_wait` batch still refers to it.
- **Shared read buffer.** `recv()` goes into one 64 KiB buffer and complete
  frames are parsed straight out of it. Only a trailing partial frame is copied
  into the connection's own buffer, so 10k idle connections cost very little
  memory.
- **Closing after an error.** If a client sends an oversized frame, part of it
  is usually still unread when the server gives up. Calling `close()` then
  makes Linux send an RST, and the client can lose the `ERR` that explains
  why. So the server sends the `ERR`, does `shutdown(SHUT_WR)`, and keeps
  reading (and throwing away) input until the client closes its side.
- **Running out of fds.** On `EMFILE` the server closes a spare fd it keeps
  around, accepts and immediately closes the pending connection, then reopens
  the spare. Otherwise the listening socket stays readable and the loop spins.

## Build and run

Linux only (it uses `epoll`, `signalfd` and `accept4`).

```sh
make                      # build/release/pubsub-server and build/release/loadgen
./build/release/pubsub-server -p 7400
```

Options: `-b addr`, `-p port` (0 = pick a free one), `-c max_clients`,
`-f max_frame`, `-w max_queued_bytes_per_client`, `-v`.

Or with Docker (Alpine, a few MB):

```sh
docker build -t epoll-pubsub .
docker run -p 7400:7400 epoll-pubsub
```

## Tests

```sh
pip install -r tests/requirements.txt
make test
```

The pytest suite starts real server processes and talks to them over sockets:
fan-out, ordering, framing edge cases (a frame sent one byte at a time, 200
frames in one write, a frame exactly at the size limit, oversized and empty
frames), bad topics, client disconnects and resets, the slow-consumer limit,
the connection limit and clean shutdown on `SIGTERM`.

CI runs the same suite three ways: normal build, AddressSanitizer + UBSan, and
under valgrind. The server has to exit with status 0 after every test, so a
leak or memory error fails the build.

## Benchmarks

`loadgen` (in `bench/`) is a single-threaded epoll client. It opens the
subscriber and publisher connections, stamps every message with
`(publisher, sequence, send time)`, and measures end-to-end latency on the
subscriber side. It also checks every sequence number, so it reports lost,
duplicated and reordered messages, not just speed.

```sh
make bench                # several runs, results in results/bench.md
./build/release/loadgen -s 5000 -P 10 -t 50 -r 200 -d 10
```

These numbers come from a GitHub Actions runner (2 vCPU AMD EPYC 7763,
Ubuntu 24.04), with the server and loadgen on the same box, so each one gets
roughly one core. 64-byte messages, 2 s warmup, 10 s measured.

| run | connections | fan-out | delivered/s | p50 | p99 | lost |
|---|---:|---:|---:|---:|---:|---:|
| 5k subscribers, 50 topics | 5,010 | 100 | 20,000 | 1.11 ms | 2.21 ms | 0 |
| 10k subscribers, 100 topics | 10,010 | 100 | 20,000 | 1.15 ms | 2.23 ms | 0 |
| one subscriber per topic | 200 | 1 | 50,000 | 0.56 ms | 1.33 ms | 0 |
| 5k subscribers, pushed hard | 5,010 | 100 | 500,000 | 13.7 ms | 88.8 ms | 0 |

At the top end both cores are busy (server plus load generator), so latency
climbs, but nothing is lost or dropped. CI uploads the full results for every
run.

One thing I got wrong at first: my first loadgen started all publishers at
the same instant, so every burst hit hundreds of subscriber sockets at once
and the single-threaded loadgen spent ~10 ms just reading them. That showed up
as 11 ms "server" latency. Staggering the publishers brought p50 at 5k
connections down to about 1 ms, which is what the server actually does.

## Bad network test

`scripts/netem.sh` adds 5% packet loss and 100 ms delay to the loopback
interface with `tc netem`, then runs loadgen in verify mode, which exits
non-zero if a single message is lost, duplicated or reordered.

Result: 200 subscribers, 5 publishers, 20 s at 4,000 deliveries/s. Latency
goes up to a few hundred ms because of the retransmits, but the count of
lost, duplicated and reordered messages is 0. TCP is doing its job, and the
framing code copes with segments arriving in odd sizes.

## Looking at it in Wireshark

`tools/pubsub.lua` is a Wireshark dissector for the protocol. It reassembles
frames that are split across TCP segments.

```sh
sudo tcpdump -i lo -w session.pcap 'tcp port 7400'
wireshark -X lua_script:tools/pubsub.lua session.pcap
```

`scripts/capture.sh` does this automatically in CI with `tshark` and checks
that the SUB, OK, PUB and MSG frames are decoded.

## Layout

```
src/        server: event loop, protocol, buffers, topic table
bench/      loadgen
tests/      pytest suite + a small Python client
scripts/    benchmarks, netem test, packet capture, Docker smoke test
tools/      Wireshark dissector
```
