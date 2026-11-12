"""Starts and stops pubsub-server processes for the tests.

Env vars:
  SERVER_BIN       server binary (default build/release/pubsub-server)
  SERVER_WRAPPER   command to run it under, e.g. "valgrind --error-exitcode=99"
"""

import os
import re
import select
import shlex
import signal
import subprocess
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER_BIN = os.environ.get("SERVER_BIN", os.path.join(ROOT, "build", "release", "pubsub-server"))
WRAPPER = shlex.split(os.environ.get("SERVER_WRAPPER", ""))
START_TIMEOUT = float(os.environ.get("PUBSUB_START_TIMEOUT", "30"))


class Server:
    def __init__(self, *args):
        if not os.path.exists(SERVER_BIN):
            raise RuntimeError(f"{SERVER_BIN} not found, run `make` first")
        self.errlog = tempfile.TemporaryFile()
        cmd = [*WRAPPER, SERVER_BIN, "-b", "127.0.0.1", "-p", "0", *map(str, args)]
        self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=self.errlog)

        ready, _, _ = select.select([self.proc.stdout], [], [], START_TIMEOUT)
        line = self.proc.stdout.readline().decode() if ready else ""
        m = re.search(r"listening on [\d.]+:(\d+)", line)
        if not m:
            self.proc.kill()
            self.proc.wait()
            raise RuntimeError(f"server failed to start: {line!r}\n{self.stderr()}")
        self.port = int(m.group(1))

    def alive(self):
        return self.proc.poll() is None

    def stderr(self):
        self.errlog.seek(0)
        return self.errlog.read().decode(errors="replace")

    def stop(self, sig=signal.SIGTERM, timeout=60):
        if self.alive():
            self.proc.send_signal(sig)
            try:
                self.proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        return self.proc.returncode
