import pytest

from client import Client
from harness import Server


@pytest.fixture
def make_server():
    """Factory for servers with custom flags. Every server must exit with
    status 0 on SIGTERM; under ASan/valgrind that also means no leaks."""
    servers = []

    def factory(*args):
        s = Server(*args)
        servers.append(s)
        return s

    yield factory

    problems = []
    for s in servers:
        rc = s.stop()
        if rc != 0:
            problems.append(f"server exited with {rc}:\n{s.stderr()}")
    if problems:
        pytest.fail("\n".join(problems))


@pytest.fixture
def server(make_server):
    return make_server()


@pytest.fixture
def connect(server):
    clients = []

    def factory(**kw):
        c = Client(server.port, **kw)
        clients.append(c)
        return c

    yield factory
    for c in clients:
        c.close()
