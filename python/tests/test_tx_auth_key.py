"""Transactions (autocommit=False: implicit BEGIN + commit/rollback),
authentication outcomes, and the DOCSQL_KEY frame-sealing path."""

import pytest

import docsql
from docsql import OperationalError

from conftest import TOKEN


def test_implicit_transaction_commits(server):
    with docsql.connect(
        host="127.0.0.1", port=server.port, token=TOKEN, autocommit=False
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE py_tx (id INT PRIMARY KEY)")
            cur.execute("INSERT INTO py_tx VALUES (1)")
            conn.commit()
            cur.execute("SELECT COUNT(*) FROM py_tx")
            assert cur.fetchone() == (1,)


def test_rollback_discards(server):
    with docsql.connect(
        host="127.0.0.1", port=server.port, token=TOKEN, autocommit=False
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE py_rb (id INT PRIMARY KEY)")
            conn.commit()  # DDL is transactional too: make it durable first
            cur.execute("INSERT INTO py_rb VALUES (1)")
            conn.rollback()
            cur.execute("SELECT COUNT(*) FROM py_rb")
            assert cur.fetchone() == (0,)
            # The rolled-back block ended: the next execute opens a new one.
            cur.execute("INSERT INTO py_rb VALUES (2)")
            conn.commit()
            cur.execute("SELECT COUNT(*) FROM py_rb")
            assert cur.fetchone() == (1,)


def test_autocommit_default_writes_immediately(server):
    with docsql.connect(host="127.0.0.1", port=server.port, token=TOKEN) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE py_ac (id INT PRIMARY KEY)")
    # A fresh connection sees the DDL without any commit call.
    with docsql.connect(host="127.0.0.1", port=server.port, token=TOKEN) as conn2:
        with conn2.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM py_ac")
            assert cur.fetchone() == (0,)


def test_bad_token_refused_at_open(server):
    with pytest.raises(OperationalError):
        docsql.connect(host="127.0.0.1", port=server.port, token="wrong-token")


def test_user_login(server, conn):
    with conn.cursor() as cur:
        cur.execute("CREATE USER pyuser PASSWORD 'py-pass-1234'")
        cur.execute("GRANT readwrite TO pyuser")
    with docsql.connect(
        host="127.0.0.1", port=server.port, user="pyuser", password="py-pass-1234"
    ) as user_conn:
        with user_conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)
    # Wrong password refuses at open.
    with pytest.raises(OperationalError):
        docsql.connect(
            host="127.0.0.1", port=server.port, user="pyuser", password="nope"
        )
    with conn.cursor() as cur:
        cur.execute("DROP USER pyuser")


def test_keyed_connection_round_trip(server):
    pytest.importorskip("cryptography")
    key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    keyed = _start_keyed_server()
    try:
        with docsql.connect(
            host="127.0.0.1", port=keyed.port, token=TOKEN, key=key
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 6 * 7")
                assert cur.fetchone() == (42,)
    finally:
        keyed.proc.terminate()
        try:
            keyed.proc.wait(timeout=5)
        except Exception:
            keyed.proc.kill()


def test_keyed_wrong_key_fails(server):
    pytest.importorskip("cryptography")
    keyed = _start_keyed_server(key="ff" * 32)
    try:
        with pytest.raises((OperationalError, docsql.InterfaceError)):
            with docsql.connect(
                host="127.0.0.1", port=keyed.port, token=TOKEN, key="ee" * 32
            ) as conn:
                conn.ping()
    finally:
        keyed.proc.terminate()
        try:
            keyed.proc.wait(timeout=5)
        except Exception:
            keyed.proc.kill()


def _start_keyed_server(key="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"):
    import tempfile
    from pathlib import Path

    from conftest import _start_server

    workdir = Path(tempfile.mkdtemp(prefix="docsql-keyed-"))
    return _start_server(
        workdir, "keyed", {"DOCSQL_TOKEN": TOKEN, "DOCSQL_KEY": key}
    )
