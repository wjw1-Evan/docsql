"""DB-API surface basics: CRUD, fetch variants, description, errors."""

import docsql
from conftest import TOKEN
from docsql import (
    IntegrityError,
    ProgrammingError,
)


def _table(cur, name):
    cur.execute(f'DROP TABLE IF EXISTS "{name}"')
    cur.execute(
        f'CREATE TABLE "{name}" (id INT PRIMARY KEY, v TEXT, amount DECIMAL)'
    )


def test_crud_and_fetch_variants(conn):
    with conn.cursor() as cur:
        _table(cur, "py_basic")
        cur.execute("INSERT INTO py_basic VALUES (1, 'a', 1.5), (2, 'b', 2.5)")
        assert cur.rowcount == 2

        cur.execute("SELECT id, v FROM py_basic ORDER BY id")
        assert [c[0] for c in cur.description] == ["id", "v"]
        assert cur.fetchone() == (1, "a")
        assert cur.fetchmany(1) == [(2, "b")]
        assert cur.fetchall() == []

        cur.execute("SELECT id FROM py_basic ORDER BY id")
        assert cur.fetchall() == [(1,), (2,)]
        cur.execute("SELECT id FROM py_basic ORDER BY id")
        assert list(cur) == [(1,), (2,)]

        cur.execute("UPDATE py_basic SET v = 'x' WHERE id = 1")
        assert cur.rowcount == 1
        cur.execute("DELETE FROM py_basic WHERE id = 2")
        assert cur.rowcount == 1


def test_executemany_sums_affected(conn):
    with conn.cursor() as cur:
        _table(cur, "py_many")
        cur.executemany(
            "INSERT INTO py_many (id, v) VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c")],
        )
        assert cur.rowcount == 3
        cur.execute("SELECT COUNT(*) FROM py_many")
        assert cur.fetchone() == (3,)


def test_prepared_template_is_reused(conn):
    with conn.cursor() as cur:
        _table(cur, "py_prep")
        template = "INSERT INTO py_prep (id, v) VALUES (?, ?)"
        cur.execute(template, (1, "a"))
        # The handle cache is CONNECTION-level (server handles are
        # per-connection state; a per-cursor cache leaked them).
        assert template in conn._prepared
        handle = conn._prepared[template]
        cur.execute(template, (2, "b"))
        # The template is registered once and reused (same handle).
        assert conn._prepared[template] == handle
        cur.execute("SELECT COUNT(*) FROM py_prep")
        assert cur.fetchone() == (2,)


def test_error_mapping(conn):
    with conn.cursor() as cur:
        _table(cur, "py_err")
        try:
            cur.execute("SELEKT 1")
            raise AssertionError("parse error must raise")
        except ProgrammingError:
            pass
        try:
            cur.execute("INSERT INTO py_err VALUES (1, 'a', 1), (1, 'dup', 1)")
            raise AssertionError("unique violation must raise")
        except IntegrityError:
            pass


def test_closed_objects_raise(conn):
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.close()
    try:
        cur.execute("SELECT 1")
        raise AssertionError("closed cursor must raise")
    except ProgrammingError:
        pass
    conn.close()
    try:
        conn.cursor()
        raise AssertionError("closed connection must raise")
    except ProgrammingError:
        pass


def test_context_managers(server):
    with docsql.connect(host="127.0.0.1", port=server.port, token=TOKEN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 40 + 2")
            assert cur.fetchone() == (42,)


def test_ping_and_status(conn):
    conn.ping()
    status = conn.status()
    assert status["name"] == "docsql"
    assert "metrics" in status
