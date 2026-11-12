#!/usr/bin/env python3
"""Quick end-to-end check against a running server, e.g. the Docker image."""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tests"))
from client import PING, PONG, Client  # noqa: E402

host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
port = int(sys.argv[2]) if len(sys.argv) > 2 else 7400

# docker-proxy accepts connections before the server inside is listening, so
# retry a full PING round trip, not just connect().
for _ in range(50):
    try:
        sub = Client(port, host=host)
        sub.send(PING, b"ready?")
        sub.expect(PONG)
        break
    except (OSError, AssertionError):
        time.sleep(0.2)
else:
    sys.exit("server not reachable")

pub = Client(port, host=host)
pub.send(PING, b"hi")
assert pub.expect(PONG) == b"hi"
sub.sub("smoke")
pub.pub("smoke", b"it works")
assert sub.recv_msg() == ("smoke", b"it works")
print("smoke ok:", pub.stats())
