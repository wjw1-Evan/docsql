"""Driver regressions: timeout poisoning, subscriber keepalive, reconnect
semantics, error families, bind-domain guards."""

import time
from decimal import Decimal

import pytest
from conftest import TOKEN

import docsql
import docsql.subscriber as submod
from docsql import DataError, DatabaseError, InterfaceError, OperationalError
from docsql._proto import RESP_ERROR
from docsql.errors import Error
from docsql._proto import ProtocolError
from docsql.subscriber import Subscriber


def test_read_timeout_poisons_the_connection(conn):
    # The server still answers a timed-out statement; a live-looking socket
    # used to hand the NEXT round trip the PREVIOUS statement's reply (a
    # silent one-frame misalignment). The transport must refuse reuse.
    # ~0.4 s of scan work under a 20 ms read budget: the timeout fires
    # deterministically while the reply is still in flight.
    conn._transport.timeout = 0.02
    with pytest.raises(OperationalError):
        conn.cursor().execute("SELECT COUNT(*) FROM GENERATE_SERIES(1, 500000) AS t")
    with pytest.raises(InterfaceError):
        conn.cursor().execute("SELECT 1")


def test_protocol_error_is_a_dbapi_error():
    # ProtocolError used to be a bare Exception: it escaped every
    # `except Error` recovery path (subscriber loops, ping(reconnect=True))
    # and killed reader threads.
    assert issubclass(ProtocolError, InterfaceError)
    assert issubclass(ProtocolError, Error)


def test_huge_int_bind_raises_data_error(conn):
    # Beyond rust_decimal's 96-bit domain the $dec path used to degrade
    # silently into a TEXT literal server-side; reject at bind time.
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS py_reg_dec")
        cur.execute("CREATE TABLE py_reg_dec (v DECIMAL)")
        with pytest.raises(DataError):
            cur.execute("INSERT INTO py_reg_dec VALUES (?)", [2**100])
        # 2**95 stays inside the exact domain.
        cur.execute("INSERT INTO py_reg_dec VALUES (?)", [2**95])
        cur.execute("SELECT v FROM py_reg_dec")
        assert cur.fetchone() == (Decimal(str(2**95)),)


def test_subscriber_keepalive_keeps_quiet_connection_alive(server):
    # The reader used to block on a plain read until the connection timeout
    # fired, then treat the timeout as a dead link: a quiet channel self-
    # inflicted a reconnect cycle every ~15 s (with on_disconnect noise and
    # a real message-loss window). Now idle polls send PING keepalives.
    got = []
    disconnects = []
    sub = Subscriber(
        lambda m: got.append(m),
        on_disconnect=lambda: disconnects.append(1),
        host=server.addr[0],
        port=server.addr[1],
        token=TOKEN,
    )
    try:
        sub.subscribe("keepalive-ch", from_="earliest")
        orig_poll, orig_keep = submod._POLL_SECONDS, submod._KEEPALIVE_POLLS
        submod._POLL_SECONDS, submod._KEEPALIVE_POLLS = 0.2, 2
        try:
            time.sleep(1.5)  # several keepalive cycles on a quiet channel
        finally:
            submod._POLL_SECONDS, submod._KEEPALIVE_POLLS = orig_poll, orig_keep
        assert disconnects == []
        publisher = docsql.connect(
            host=server.addr[0], port=server.addr[1], token=TOKEN
        )
        try:
            publisher.publish("keepalive-ch", "ping-after-idle")
        finally:
            publisher.close()
        deadline = time.time() + 5
        while not got and time.time() < deadline:
            time.sleep(0.05)
        assert got, "no delivery after an idle keepalive period"
    finally:
        sub.close()


def test_ping_reconnect_surfaces_lost_transaction(server):
    # After ping(reconnect=True): the open transaction was rolled back by
    # the server on disconnect — commit() must say so instead of silently
    # no-op'ing, prepared handles must be dropped (per-connection server
    # state), and a fresh transaction on the same objects must work.
    setup = docsql.connect(host=server.addr[0], port=server.addr[1], token=TOKEN)
    try:
        cur = setup.cursor()
        cur.execute("DROP TABLE IF EXISTS py_reg_tx")
        cur.execute("CREATE TABLE py_reg_tx (id INT)")
    finally:
        setup.close()

    conn = docsql.connect(
        host=server.addr[0], port=server.addr[1], token=TOKEN, autocommit=False
    )
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO py_reg_tx VALUES (?)", [1])  # opens the tx
        conn._transport.close()  # simulate a dead socket
        conn.ping(reconnect=True)
        with pytest.raises(OperationalError):
            conn.commit()
        # Same cursor, same template: re-prepares on the new connection.
        cur.execute("INSERT INTO py_reg_tx VALUES (?)", [2])
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM py_reg_tx")
        assert cur.fetchone() == (1,)  # insert 1 was rolled back
    finally:
        conn.close()


def test_control_maps_server_rejection_to_database_error(server):
    # A server-side subscribe/unsubscribe rejection is a statement error;
    # raising InterfaceError used to declare the whole connection dead.
    sub = Subscriber(
        lambda m: None, host=server.addr[0], port=server.addr[1], token=TOKEN
    )
    try:
        sub._replies.put((0, RESP_ERROR, b"unknown channel thing"))
        with pytest.raises(DatabaseError):
            sub._control(0x0021, ["x"])
    finally:
        sub.close()


def test_prepared_cache_is_shared_across_cursors(conn):
    # 服务端 prepared 句柄是连接级状态:游标级缓存曾让每个短命游标都重新
    # prepare 同一模板,句柄泄漏到服务端 1024 上限后,该连接的所有参数化
    # 执行永久失败。连接级共享缓存下,上千个游标共用一个句柄。
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS prep_t (v INT)")
        cur.execute("DELETE FROM prep_t")
    for i in range(1100):
        with conn.cursor() as cur:
            cur.execute("INSERT INTO prep_t VALUES (?)", (i,))
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM prep_t")
        assert cur.fetchall() == [(1100,)]
    assert len(conn._prepared) == 1


def test_nan_and_overwide_decimal_binds_raise_data_error(conn):
    # NaN 的 Decimal 曾在值域比较处抛 InvalidOperation(非 DB-API 异常)
    # 泄漏出 execute();38 位小数(尾数装不进 2^96)曾静默把整个标记对象
    # 按 TEXT 绑定 —— 现在都在客户端响亮拒绝。
    from datetime import datetime, timezone

    with pytest.raises(DataError):
        conn.cursor().execute("SELECT ?", (Decimal("NaN"),))
    with pytest.raises(DataError):
        conn.cursor().execute("SELECT ?", (Decimal("sNaN"),))
    with pytest.raises(DataError):
        conn.cursor().execute("SELECT ?", (Decimal("0." + "1" * 38),))
    with pytest.raises(DataError):
        # 科学计数法的小值(str 会给 1E-29)在服务端走有损路径,同样拒。
        conn.cursor().execute("SELECT ?", (Decimal("1E-29"),))
    # 对照:值域内的 Decimal 与超 int64 的整数仍正常。
    with conn.cursor() as cur:
        cur.execute("SELECT ?", (Decimal("123.45"),))
        assert cur.fetchall()[0][0] == Decimal("123.45")
        big = 2**64
        cur.execute("SELECT ?", (big,))
        assert cur.fetchall()[0][0] == big


def test_out_of_domain_datetime_bind_raises_data_error(conn):
    # 9999-12-31 23:59:59.999999 的毫秒取整恰好越过引擎值域上界 1 毫秒:
    # 服务端会把它当普通对象回退成 TEXT 绑定(静默类型损坏),现在客户端拒绝。
    from datetime import datetime, timezone

    edge = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
    with pytest.raises(DataError):
        conn.cursor().execute("SELECT ?", (edge,))
    ok = datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc)
    with conn.cursor() as cur:
        cur.execute("SELECT ?", (ok,))
        assert cur.fetchall()[0][0] == ok
