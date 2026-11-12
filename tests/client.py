"""Tiny blocking client for the pubsub wire protocol, used by the tests."""

import os
import socket
import struct
import time

TIMEOUT = float(os.environ.get("PUBSUB_TIMEOUT", "5"))

SUB, UNSUB, PUB, PING, STATS = 0x01, 0x02, 0x03, 0x04, 0x05
OK, MSG, PONG, STATS_REPLY, ERR = 0x81, 0x82, 0x83, 0x84, 0x85

E_BAD_FRAME, E_BAD_TOPIC, E_NOT_SUBSCRIBED, E_UNKNOWN_TYPE, E_TOO_LARGE, E_LIMIT = range(1, 7)


def _b(x):
    return x.encode() if isinstance(x, str) else x


def frame(ftype, payload=b""):
    return struct.pack("!IB", 1 + len(payload), ftype) + payload


def pub_payload(topic, msg):
    t = _b(topic)
    return bytes([len(t)]) + t + msg


def wait_for(cond, timeout=None, interval=0.05):
    deadline = time.monotonic() + (timeout or TIMEOUT * 2)
    while time.monotonic() < deadline:
        if cond():
            return
        time.sleep(interval)
    raise AssertionError("condition not met in time")


class Client:
    def __init__(self, port, host="127.0.0.1", rcvbuf=None):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if rcvbuf:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.settimeout(TIMEOUT)
        self.sock.connect((host, port))
        self.buf = b""

    # sending

    def send_raw(self, data):
        self.sock.sendall(data)

    def send(self, ftype, payload=b""):
        self.send_raw(frame(ftype, payload))

    def sub(self, topic):
        self.send(SUB, _b(topic))
        return self.expect(OK)

    def unsub(self, topic):
        self.send(UNSUB, _b(topic))
        return self.expect(OK)

    def pub(self, topic, msg):
        self.send(PUB, pub_payload(topic, msg))

    # receiving

    def _fill(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                return False
            self.buf += chunk
        return True

    def recv_frame(self):
        """Next (type, payload), or None once the server closed the socket."""
        try:
            if not self._fill(4):
                return None
            (length,) = struct.unpack("!I", self.buf[:4])
            if not self._fill(4 + length):
                return None
        except (ConnectionResetError, BrokenPipeError):
            return None
        body, self.buf = self.buf[4 : 4 + length], self.buf[4 + length :]
        return body[0], body[1:]

    def expect(self, ftype):
        got = self.recv_frame()
        assert got is not None, "connection closed"
        assert got[0] == ftype, f"expected frame 0x{ftype:02x}, got 0x{got[0]:02x} {got[1]!r}"
        return got[1]

    def expect_err(self):
        p = self.expect(ERR)
        return p[0], p[1:].decode()

    def recv_msg(self):
        p = self.expect(MSG)
        tlen = p[0]
        return p[1 : 1 + tlen].decode(), p[1 + tlen :]

    def assert_silent(self, wait=0.3):
        """Nothing should arrive within `wait` seconds."""
        self.sock.settimeout(wait)
        try:
            got = self.recv_frame()
        except socket.timeout:
            got = None
        finally:
            self.sock.settimeout(TIMEOUT)
        assert got is None, f"unexpected frame {got!r}"

    def stats(self):
        self.send(STATS)
        text = self.expect(STATS_REPLY).decode()
        return {k: int(v) for k, v in (kv.split("=") for kv in text.split())}

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def reset(self):
        """Close with an RST instead of a FIN."""
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        self.sock.close()
