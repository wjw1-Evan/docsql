"""Exact-scalar round trips: DECIMAL / TIMESTAMP / BLOB / non-finite floats,
server-side binding of qmark parameters (REQ_PREPARE + REQ_EXECUTE)."""

import math
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from docsql import DataError


def _table(cur, name, with_text=False):
    cur.execute(f'DROP TABLE IF EXISTS "{name}"')
    cols = "id INT PRIMARY KEY, d DECIMAL, ts TIMESTAMP, b BLOB, f REAL"
    if with_text:
        cols += ", v TEXT"
    cur.execute(f'CREATE TABLE "{name}" ({cols})')


def test_decimal_is_exact(conn):
    with conn.cursor() as cur:
        _table(cur, "py_dec")
        exact = Decimal("12345678901234567890.123456789")
        cur.execute("INSERT INTO py_dec VALUES (1, ?, NULL, NULL, NULL)", (exact,))
        cur.execute("SELECT d FROM py_dec WHERE id = 1")
        assert cur.fetchone()[0] == exact


def test_decimal_survives_where_comparison(conn):
    with conn.cursor() as cur:
        _table(cur, "py_dec2")
        cur.execute(
            "INSERT INTO py_dec2 VALUES (1, ?, NULL, NULL, NULL)",
            (Decimal("0.1") + Decimal("0.2"),),
        )
        # The classic float trap: 0.1+0.2 as DOUBLE would not match 0.3.
        cur.execute("SELECT id FROM py_dec2 WHERE d = ?", (Decimal("0.3"),))
        assert cur.fetchone() == (1,)


def test_big_int_binds_through_decimal(conn):
    with conn.cursor() as cur:
        _table(cur, "py_bigint")
        big = 2**63  # one past int64: a bare JSON number would lose precision
        cur.execute("INSERT INTO py_bigint VALUES (1, ?, NULL, NULL, NULL)", (big,))
        cur.execute("SELECT d FROM py_bigint WHERE id = 1")
        assert cur.fetchone()[0] == Decimal(big)


def test_timestamp_round_trip(conn):
    with conn.cursor() as cur:
        _table(cur, "py_ts")
        moment = datetime(2026, 9, 29, 12, 34, 56, 789000, tzinfo=timezone.utc)
        cur.execute("INSERT INTO py_ts VALUES (1, NULL, ?, NULL, NULL)", (moment,))
        cur.execute("SELECT ts FROM py_ts WHERE id = 1")
        assert cur.fetchone()[0] == moment
        # Time-band comparison: the bound datetime must match as a
        # TIMESTAMP, not as text.
        cur.execute("SELECT id FROM py_ts WHERE ts = ?", (moment,))
        assert cur.fetchone() == (1,)


def test_blob_round_trip(conn):
    with conn.cursor() as cur:
        _table(cur, "py_blob")
        blob = bytes(range(256)) + b"\x00\xff"
        cur.execute("INSERT INTO py_blob VALUES (1, NULL, NULL, ?, NULL)", (blob,))
        cur.execute("SELECT b FROM py_blob WHERE id = 1")
        assert cur.fetchone()[0] == blob


def test_nonfinite_floats(conn):
    with conn.cursor() as cur:
        _table(cur, "py_float")
        for i, value in enumerate((float("inf"), float("-inf"), float("nan")), 1):
            cur.execute("INSERT INTO py_float VALUES (?, NULL, NULL, NULL, ?)", (i, value))
        cur.execute("SELECT f FROM py_float ORDER BY id")
        a, b, c = (r[0] for r in cur.fetchall())
        assert a == math.inf and b == -math.inf and math.isnan(c)


def test_string_cannot_break_out_of_literal(conn):
    with conn.cursor() as cur:
        _table(cur, "py_inj", with_text=True)
        sneaky = "x'); DROP TABLE py_inj; --"
        cur.execute(
            "INSERT INTO py_inj VALUES (1, NULL, NULL, NULL, NULL, ?)", (sneaky,)
        )
        # The injection text is data: the table survives and the string
        # round-trips byte for byte (server-side quote-escaped binding).
        cur.execute("SELECT v FROM py_inj WHERE id = 1")
        assert cur.fetchone()[0] == sneaky
        cur.execute("SELECT COUNT(*) FROM py_inj")
        assert cur.fetchone() == (1,)


def test_unsupported_param_type_raises(conn):
    with conn.cursor() as cur:
        _table(cur, "py_bad")
        with pytest.raises(TypeError):
            cur.execute("INSERT INTO py_bad VALUES (?, NULL, NULL, NULL, NULL)", ({1, 2},))


def test_null_round_trip(conn):
    with conn.cursor() as cur:
        _table(cur, "py_null")
        cur.execute("INSERT INTO py_null VALUES (1, ?, ?, ?, ?)", (None, None, None, None))
        cur.execute("SELECT d, ts, b, f FROM py_null WHERE id = 1")
        assert cur.fetchone() == (None, None, None, None)
