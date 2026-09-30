"""Shared fixtures: a real docsql-server process per test session.

The server binary is expected at the repo's ``target/debug/docsql-server``
(override with DOCSQL_SERVER_BIN). The spawn mirrors the .NET suite's
``FindServer``/``StartServer`` shape: argument-list Popen (never a
shell), credentials via environment, and a bounded TCP wait loop.
"""

import os
import socket
import subprocess
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = Path(__file__).resolve().parent / "fixtures"
TOKEN = "pytest-driver-token"


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def server_bin():
    exe = os.environ.get(
        "DOCSQL_SERVER_BIN", str(REPO_ROOT / "target" / "debug" / "docsql-server")
    )
    if not os.path.exists(exe):
        pytest.skip(f"docsql-server binary not found at {exe}; run cargo build -p docsql-server")
    return exe


class Server:
    def __init__(self, proc, port, workdir, env):
        self.proc = proc
        self.port = port
        self.workdir = workdir
        self.env = env

    @property
    def addr(self):
        return ("127.0.0.1", self.port)


def _start_server(workdir, name, extra_env):
    port = _free_port()
    db = workdir / f"{name}.db"
    env = dict(os.environ)
    env.update(extra_env)
    proc = subprocess.Popen(
        [server_bin(), str(db), f"127.0.0.1:{port}"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.time() + 20
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server {name} exited early with {proc.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return Server(proc, port, workdir, env)
        except OSError:
            time.sleep(0.05)
    proc.kill()
    raise RuntimeError(f"server {name} did not come up")


@pytest.fixture(scope="session")
def server(tmp_path_factory):
    srv = _start_server(
        tmp_path_factory.mktemp("docsql"), "main", {"DOCSQL_TOKEN": TOKEN}
    )
    yield srv
    srv.proc.terminate()
    try:
        srv.proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        srv.proc.kill()


@pytest.fixture(scope="session")
def tls_server(tmp_path_factory):
    srv = _start_server(
        tmp_path_factory.mktemp("docsql-tls"),
        "tls",
        {
            "DOCSQL_TOKEN": TOKEN,
            "DOCSQL_TLS_CERT": str(FIXTURES / "server-cert.pem"),
            "DOCSQL_TLS_KEY": str(FIXTURES / "server-key.pem"),
        },
    )
    yield srv
    srv.proc.terminate()
    try:
        srv.proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        srv.proc.kill()


@pytest.fixture(scope="session")
def open_server(tmp_path_factory):
    """A token-less server: anonymous access stays open (auth disabled)."""
    srv = _start_server(tmp_path_factory.mktemp("docsql-open"), "open", {})
    yield srv
    srv.proc.terminate()
    try:
        srv.proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        srv.proc.kill()


@pytest.fixture()
def conn(server):
    import docsql

    with docsql.connect(host="127.0.0.1", port=server.port, token=TOKEN) as c:
        yield c
