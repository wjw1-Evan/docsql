"""Native TLS (data plane) and pub/sub subscription flows."""

import time

import pytest

import docsql

from conftest import FIXTURES, TOKEN


def test_tls_connects_and_queries(tls_server):
    with docsql.connect(
        host="127.0.0.1",
        port=tls_server.port,
        token=TOKEN,
        tls=True,
        tls_ca=str(FIXTURES / "server-cert.pem"),
        tls_hostname="localhost",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 'over-tls'")
            assert cur.fetchone() == ("over-tls",)


def test_tls_without_ca_encrypt_only(tls_server):
    with docsql.connect(
        host="127.0.0.1", port=tls_server.port, token=TOKEN, tls=True
    ) as conn:
        conn.ping()


def test_plaintext_client_against_tls_listener(tls_server):
    with pytest.raises(docsql.Error):
        with docsql.connect(host="127.0.0.1", port=tls_server.port, token=TOKEN) as conn:
            conn.ping()


def _mktable(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS py_pub")
        cur.execute("CREATE TABLE py_pub (id INT PRIMARY KEY)")


def test_subscribe_receives_live_and_history(conn, server):
    _mktable(conn)
    received = []
    got_all = __import__("threading").Event()

    def on_message(msg):
        received.append(msg)
        if len(received) >= 3:
            got_all.set()

    with docsql.Subscriber(
        on_message=on_message, host="127.0.0.1", port=server.port, token=TOKEN
    ) as sub:
        sub.subscribe("py-news", from_="earliest")
        for i in range(3):
            mid, receivers = conn.publish("py-news", f"msg-{i}")
            assert mid > 0
        assert got_all.wait(timeout=10), f"only got {received}"
        payloads = [m["payload"] for m in received]
        assert payloads == ["msg-0", "msg-1", "msg-2"]
        assert all(m["channel"] == "py-news" for m in received)
        assert [m["id"] for m in received] == sorted(m["id"] for m in received)


def test_resume_from_id_after_reconnect(conn, server):
    """Publish, note the id, open a subscriber from that id: the replay
    starts strictly AFTER the id (resume cursor), so only later messages
    arrive — no loss, no duplicate."""
    _mktable(conn)
    first_id, _ = conn.publish("py-resume", "before")
    received = []
    got = __import__("threading").Event()

    def on_message(msg):
        received.append(msg)
        got.set()

    with docsql.Subscriber(
        on_message=on_message, host="127.0.0.1", port=server.port, token=TOKEN
    ) as sub:
        sub.subscribe("py-resume", from_=first_id)
        conn.publish("py-resume", "after")
        assert got.wait(timeout=10), "no message after resume"
        assert received[-1]["payload"] == "after"
        assert all(m["id"] > first_id for m in received)


def test_psubscribe_glob(conn, server):
    _mktable(conn)
    received = []
    got = __import__("threading").Event()

    def on_message(msg):
        received.append(msg)
        got.set()

    with docsql.Subscriber(
        on_message=on_message, host="127.0.0.1", port=server.port, token=TOKEN
    ) as sub:
        sub.psubscribe("py-glob-*")
        conn.publish("py-glob-one", "hello")
        assert got.wait(timeout=10), "pattern delivery missing"
        assert received[-1]["payload"] == "hello"
        assert received[-1].get("kind") == "pmessage"
