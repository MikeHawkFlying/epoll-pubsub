import os
import struct
import subprocess
import time

from client import (
    E_BAD_FRAME,
    E_BAD_TOPIC,
    E_NOT_SUBSCRIBED,
    E_TOO_LARGE,
    E_UNKNOWN_TYPE,
    PING,
    PONG,
    PUB,
    SUB,
    UNSUB,
    Client,
    frame,
    pub_payload,
    wait_for,
)
from harness import SERVER_BIN, WRAPPER

# --- basics -------------------------------------------------------------


def test_ping_pong(connect):
    c = connect()
    c.send(PING, b"hello")
    assert c.expect(PONG) == b"hello"


def test_ping_with_empty_payload(connect):
    c = connect()
    c.send(PING)
    assert c.expect(PONG) == b""


def test_subscribe_is_acknowledged(connect):
    assert connect().sub("news") == bytes([SUB])


def test_publish_reaches_subscriber(connect):
    s, p = connect(), connect()
    s.sub("news")
    p.pub("news", b"hi")
    assert s.recv_msg() == ("news", b"hi")


def test_publish_with_no_subscribers_is_fine(connect):
    p = connect()
    p.pub("nobody-home", b"x")
    p.assert_silent()
    p.send(PING)
    assert p.expect(PONG) == b""


def test_publisher_does_not_get_its_own_message(connect):
    s, p = connect(), connect()
    s.sub("t")
    p.pub("t", b"m")
    assert s.recv_msg() == ("t", b"m")
    p.assert_silent()


def test_subscribed_publisher_gets_its_own_message(connect):
    c = connect()
    c.sub("echo")
    c.pub("echo", b"self")
    assert c.recv_msg() == ("echo", b"self")


# --- subscriptions --------------------------------------------------------


def test_fanout_to_every_subscriber(connect):
    subs = [connect() for _ in range(10)]
    for s in subs:
        s.sub("all")
    connect().pub("all", b"x")
    for s in subs:
        assert s.recv_msg() == ("all", b"x")


def test_unsubscribe_stops_delivery(connect):
    s, p = connect(), connect()
    s.sub("t")
    assert s.unsub("t") == bytes([UNSUB])
    p.pub("t", b"x")
    s.assert_silent()


def test_unsubscribe_without_subscribing_is_an_error(connect):
    c = connect()
    c.send(UNSUB, b"never")
    code, _ = c.expect_err()
    assert code == E_NOT_SUBSCRIBED


def test_double_subscribe_delivers_once(connect):
    s, p = connect(), connect()
    s.sub("t")
    s.sub("t")
    p.pub("t", b"once")
    assert s.recv_msg() == ("t", b"once")
    s.assert_silent()


def test_topics_are_isolated(connect):
    a, b, p = connect(), connect(), connect()
    a.sub("a")
    b.sub("b")
    p.pub("a", b"1")
    assert a.recv_msg() == ("a", b"1")
    b.assert_silent()


def test_order_is_kept_for_one_publisher(connect):
    s, p = connect(), connect()
    s.sub("seq")
    for i in range(500):
        p.pub("seq", str(i).encode())
    for i in range(500):
        assert s.recv_msg() == ("seq", str(i).encode())


# --- payloads and topics ----------------------------------------------------


def test_binary_payload_is_untouched(connect):
    s, p = connect(), connect()
    s.sub("bin")
    payload = bytes(range(256)) * 4
    p.pub("bin", payload)
    assert s.recv_msg() == ("bin", payload)


def test_empty_message_is_allowed(connect):
    s, p = connect(), connect()
    s.sub("t")
    p.pub("t", b"")
    assert s.recv_msg() == ("t", b"")


def test_64_byte_topic_is_accepted(connect):
    s, p = connect(), connect()
    topic = "x" * 64
    s.sub(topic)
    p.pub(topic, b"m")
    assert s.recv_msg() == (topic, b"m")


def test_65_byte_topic_is_rejected(connect):
    c = connect()
    c.send(SUB, b"x" * 65)
    code, _ = c.expect_err()
    assert code == E_BAD_TOPIC
    c.send(PING)  # connection must still be usable
    c.expect(PONG)


def test_empty_topic_is_rejected(connect):
    c = connect()
    c.send(SUB, b"")
    assert c.expect_err()[0] == E_BAD_TOPIC


def test_topic_with_a_space_is_rejected(connect):
    c = connect()
    c.send(SUB, b"bad topic")
    assert c.expect_err()[0] == E_BAD_TOPIC


def test_malformed_publish_is_rejected(connect):
    c = connect()
    c.send(PUB, bytes([10]) + b"abc")  # claims a 10-byte topic, has 3
    assert c.expect_err()[0] == E_BAD_FRAME
    c.send(PING)
    c.expect(PONG)


def test_unknown_frame_type_is_rejected(connect):
    c = connect()
    c.send(0x7F)
    assert c.expect_err()[0] == E_UNKNOWN_TYPE
    c.send(PING)
    c.expect(PONG)


# --- framing ----------------------------------------------------------------


def test_frame_sent_one_byte_at_a_time(connect):
    s, p = connect(), connect()
    s.sub("slow")
    for b in frame(PUB, pub_payload("slow", b"chunked")):
        p.send_raw(bytes([b]))
        time.sleep(0.002)  # TCP_NODELAY + a pause = separate segments
    assert s.recv_msg() == ("slow", b"chunked")


def test_many_frames_in_one_write(connect):
    s, p = connect(), connect()
    s.sub("burst")
    p.send_raw(b"".join(frame(PUB, pub_payload("burst", f"m{i}".encode())) for i in range(200)))
    for i in range(200):
        assert s.recv_msg() == ("burst", f"m{i}".encode())


def test_message_at_the_frame_size_limit(connect):
    s, p = connect(), connect()
    s.sub("large")
    msg = os.urandom(65536 - 1 - 1 - len("large"))  # body is exactly 64 KiB
    p.pub("large", msg)
    assert s.recv_msg() == ("large", msg)


def test_oversized_frame_closes_the_connection(connect):
    c = connect()
    c.send_raw(struct.pack("!IB", 65537, PUB))
    assert c.expect_err()[0] == E_TOO_LARGE
    assert c.recv_frame() is None


def test_oversized_frame_with_body_still_gets_the_error(connect):
    # The whole 100 KiB body arrives, most of it unread when the server gives
    # up. The ERR must still make it back (no RST), followed by a clean close.
    c = connect()
    c.send_raw(struct.pack("!IB", 100_000, PUB) + b"y" * 99_999)
    assert c.expect_err()[0] == E_TOO_LARGE
    assert c.recv_frame() is None


def test_zero_length_frame_closes_the_connection(connect):
    c = connect()
    c.send_raw(struct.pack("!I", 0))
    assert c.expect_err()[0] == E_BAD_FRAME
    assert c.recv_frame() is None


# --- connection lifecycle ---------------------------------------------------


def test_disconnect_removes_subscriptions(connect):
    s, watcher = connect(), connect()
    s.sub("gone")
    wait_for(lambda: watcher.stats()["subscriptions"] == 1)
    s.close()
    wait_for(lambda: watcher.stats()["subscriptions"] == 0)
    assert watcher.stats()["topics"] == 0


def test_reset_connection_does_not_hurt_the_server(server, connect):
    s = connect()
    s.sub("rst")
    s.reset()
    p = connect()
    p.pub("rst", b"x")
    p.send(PING)
    p.expect(PONG)
    assert server.alive()


def test_stats_counts_traffic(connect):
    a, b = connect(), connect()
    a.sub("s")
    b.pub("s", b"x")
    a.recv_msg()
    st = b.stats()
    assert st["connections"] >= 2
    assert st["msgs_in"] >= 1 and st["msgs_out"] >= 1


def test_hundred_clients_at_once(connect):
    subs = [connect() for _ in range(100)]
    for i, s in enumerate(subs):
        s.sub(f"room{i % 5}")
    p = connect()
    for r in range(5):
        p.pub(f"room{r}", f"hello{r}".encode())
    for i, s in enumerate(subs):
        assert s.recv_msg() == (f"room{i % 5}", f"hello{i % 5}".encode())


def test_slow_consumer_gets_dropped(make_server):
    srv = make_server("-w", "131072")
    slow = Client(srv.port, rcvbuf=4096)  # subscribes, then never reads
    slow.sub("flood")
    p = Client(srv.port)
    chunk = b"z" * 32000
    for _ in range(400):
        p.pub("flood", chunk)
    wait_for(lambda: p.stats()["slow_drops"] >= 1, timeout=30)
    assert p.stats()["subscriptions"] == 0
    p.send(PING)
    p.expect(PONG)
    slow.close()
    p.close()


def test_client_limit(make_server):
    srv = make_server("-c", "2")
    a, b = Client(srv.port), Client(srv.port)
    for c in (a, b):  # make sure both are accepted before the third connects
        c.send(PING)
        c.expect(PONG)
    extra = Client(srv.port)
    assert extra.recv_frame() is None
    assert a.stats()["rejected"] >= 1
    for c in (a, b, extra):
        c.close()


def test_sigterm_exits_cleanly(make_server):
    srv = make_server()
    c = Client(srv.port)
    c.sub("x")
    assert srv.stop() == 0
    c.close()


def test_bad_arguments_are_rejected():
    res = subprocess.run([*WRAPPER, SERVER_BIN, "-p", "notaport"], capture_output=True)
    assert res.returncode == 2


def test_output_cap_smaller_than_a_frame_is_rejected():
    # -w below -f + 4 would let one legal frame get an idle subscriber dropped
    res = subprocess.run([*WRAPPER, SERVER_BIN, "-f", "65536", "-w", "65536"], capture_output=True)
    assert res.returncode == 2
