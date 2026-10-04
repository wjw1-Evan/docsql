"""Driver regressions, round 4: subscriber lock/callback deadlock, resume
watermark, keepalive TOCTOU, stale-reply desync, close-vs-backoff leak,
all-unsubscribe bookkeeping; decimal early reject; bare-string params;
header-decode poisoning; surrogate bind text."""

import json
import queue
import threading
import time
from decimal import Decimal

import pytest
from conftest import TOKEN

import docsql
import docsql.connection as connmod
import docsql.subscriber as submod
from docsql import DataError, InterfaceError, OperationalError, ProgrammingError
from docsql._convert import _exact_decimal_text, execute_payload
from docsql._proto import (
    HEADER,
    MAGIC,
    REQ_PING,
    REQ_SUBSCRIBE,
    RESP_AFFECTED,
    RESP_PONG,
    RESP_PUSH,
    ProtocolError,
)
from docsql.subscriber import Subscriber


# ---- shared stubs (no server needed) ---------------------------------------

class _StubTransport:
    """Records writes; serves scripted reads. ``_poll_results`` feed
    read_frame_poll one item per call (None = idle poll); ``_frames`` feed
    read_frame for the resubscribe drain."""

    def __init__(self, poll_results=None, frames=None):
        self.written = []
        self._closed = False
        self.poisoned = 0
        self._poll_results = list(poll_results or [])
        self._frames = list(frames or [])

    def write_frame(self, frame_type, payload, flags=0):
        if self._closed:
            raise InterfaceError("connection is closed")
        self.written.append((frame_type, payload))

    def read_frame_poll(self, poll_seconds):
        return self._poll_results.pop(0) if self._poll_results else None

    def read_frame(self):
        if self._frames:
            return self._frames.pop(0)
        raise InterfaceError("stub read_frame exhausted")

    def _poison(self):
        self.poisoned += 1
        self._closed = True


class _StubConn:
    def __init__(self, transport):
        self._transport = transport
        self.closed = False

    def close(self):
        self.closed = True


def _stub_subscriber(on_message=lambda m: None):
    """A Subscriber skeleton without threads or sockets: enough state for the
    _control/_dispatch/_read_frames/_resubscribe internals to run."""
    st = Subscriber.__new__(Subscriber)
    st._on_message = on_message
    st._on_disconnect = None
    st._auto = True
    st._attempts = 2
    st._conn_kwargs = {}
    st._channels = {}
    st._patterns = {}
    st._last_id = {}
    st._replies = queue.Queue()
    st._replies_stale = False
    st._write_lock = threading.Lock()
    st._control_busy = False
    st._closed = threading.Event()
    st._conn = _StubConn(_StubTransport())
    st._reader = None
    return st


# ---- 1. user callbacks must never run under the write lock -----------------

def test_control_push_dispatch_runs_outside_write_lock():
    # 控制往返途中收到的 RESP_PUSH 曾在持有 _write_lock 时直接 _dispatch:
    # 回调内同线程重入 subscribe() = 非重入锁永久挂死。
    st = _stub_subscriber()

    def reentrant(msg):
        st.subscribe("inner")  # deadlocks if the push dispatched under the lock

    st._on_message = reentrant
    st._replies.put((0, RESP_PUSH, json.dumps({"channel": "outer", "id": 1}).encode()))
    st._replies.put((0, RESP_AFFECTED, b"\x00" * 8))  # outer control's reply
    st._replies.put((0, RESP_AFFECTED, b"\x00" * 8))  # inner subscribe's reply

    done = threading.Event()

    def run():
        st.subscribe("outer")
        done.set()

    t = threading.Thread(target=run, daemon=True)
    t.start()
    assert done.wait(5), "subscribe() deadlocked: callback re-entered control under the write lock"
    assert st._channels == {"outer": "latest", "inner": "latest"}
    assert st._control_busy is False


def test_resubscribe_push_dispatch_runs_outside_write_lock(monkeypatch):
    # 重连排水循环同样曾持锁 _dispatch —— 同一挂死形态。
    st = _stub_subscriber()
    st._channels = {"outer": "latest"}
    inner_done = threading.Event()

    def reentrant(msg):
        # Stage the inner subscribe's reply from HERE: the reconnect-side
        # drain (fix 9) empties the queue before this callback runs.
        st._replies.put((0, RESP_AFFECTED, b"\x00" * 8))
        st.subscribe("inner")
        inner_done.set()

    st._on_message = reentrant
    tr = _StubTransport(
        frames=[
            (0, RESP_PUSH, json.dumps({"channel": "outer", "id": 1}).encode()),
            (0, RESP_PONG, b""),
        ]
    )
    conn = _StubConn(tr)
    monkeypatch.setattr(submod, "connect", lambda **kw: conn)
    monkeypatch.setattr(submod.time, "sleep", lambda s: None)  # skip backoff

    assert st._resubscribe() is True
    assert inner_done.wait(5), "callback re-entered control under the write lock while resubscribing"
    assert st._channels == {"outer": "latest", "inner": "latest"}


# ---- 2. unsubscribe()/punsubscribe() with no names clears the local sets ----

def test_unsubscribe_without_names_clears_local_sets(server):
    # 服务端空 names = 全部退订(pubsub.rs unregister);本地集合曾零迭代不清,
    # 重连后所有频道/模式复活。
    got = []
    sub = Subscriber(
        lambda m: got.append(m), host=server.addr[0], port=server.addr[1], token=TOKEN
    )
    try:
        sub.subscribe("reg4-unsub-a")
        sub.subscribe("reg4-unsub-b")
        sub.psubscribe("reg4-pat-*")
        # Selective form still removes only the named entries.
        sub.unsubscribe("reg4-unsub-a")
        assert set(sub._channels) == {"reg4-unsub-b"}
        sub.subscribe("reg4-unsub-a")

        sub.unsubscribe()
        assert sub._channels == {} and sub._last_id == {}
        sub.punsubscribe()
        assert sub._patterns == {}

        # The server really dropped them: publish into the void.
        pub = docsql.connect(host=server.addr[0], port=server.addr[1], token=TOKEN)
        try:
            pub.publish("reg4-unsub-a", "ghost")
        finally:
            pub.close()
        time.sleep(0.3)
        assert got == []
    finally:
        sub.close()


# ---- 3. close() racing the reconnect backoff must not leak a connection ----

def test_close_during_backoff_sleep_never_connects(monkeypatch):
    # close() 的 join(5s) 会在最长 8s 的退避 sleep 上超时;读线程醒来后曾不复查
    # _closed 就去 connect()。睡眠后必须复查。
    st = _stub_subscriber()
    st._channels = {"ch": "latest"}
    connects = []
    monkeypatch.setattr(
        submod, "connect", lambda **kw: connects.append(1) or _StubConn(_StubTransport())
    )

    def sleep_sets_closed(seconds):
        st._closed.set()  # close() fires while the reader is mid-backoff

    monkeypatch.setattr(submod.time, "sleep", sleep_sets_closed)
    assert st._resubscribe() is False
    assert connects == [], "reconnect attempted after close()"


def test_close_racing_connect_install_closes_fresh_connection(monkeypatch):
    # close() 恰在 connect() 之后、检查之前发生:新安装的连接必须由读线程自己关
    # (close() 的事后清扫已经跑过,不会再来关它)。
    st = _stub_subscriber()
    st._channels = {"ch": "latest"}
    conns = []

    def fake_connect(**kw):
        conn = _StubConn(_StubTransport(frames=[(0, RESP_PONG, b"")]))
        conns.append(conn)
        st._closed.set()  # close() lands right AFTER the install
        return conn

    monkeypatch.setattr(submod, "connect", fake_connect)
    monkeypatch.setattr(submod.time, "sleep", lambda s: None)
    assert st._resubscribe() is False
    assert len(conns) == 1 and conns[0].closed, "freshly installed connection leaked"


def test_close_racing_drain_closes_installed_connection(monkeypatch):
    # PONG 到手、确认成功之后才发现 close():同样必须关掉再退。
    st = _stub_subscriber()
    st._channels = {"ch": "latest"}
    tr = _StubTransport()

    def read_frame():
        st._closed.set()  # close() lands mid-drain, just before the PONG
        return (0, RESP_PONG, b"")

    tr.read_frame = read_frame
    conn = _StubConn(tr)
    monkeypatch.setattr(submod, "connect", lambda **kw: conn)
    monkeypatch.setattr(submod.time, "sleep", lambda s: None)
    assert st._resubscribe() is False
    assert conn.closed, "confirmed-but-closed resubscribe leaked its connection"


# ---- 4. extreme positive exponents rejected before materializing -----------

def test_extreme_positive_exponent_decimal_rejected_before_materializing():
    # 1E+999999999 曾先物化 10**999999999(挂起/OOM)才检查 _DEC_MAX;现在按位数
    # 早拒,沿用既有 DataError 语义。
    for bad in ("1E+999999999", "9E+1000000", "0E+999999999"):
        with pytest.raises(DataError):
            _exact_decimal_text(Decimal(bad))
    # 边界不变:29 位整数仍合法(10**28 <= 2**96-1),30 位(1E+29)超域。
    assert _exact_decimal_text(Decimal("1E+28")) == "1" + "0" * 28
    with pytest.raises(DataError):
        _exact_decimal_text(Decimal("1E+29"))


def test_extreme_positive_exponent_decimal_bind_raises_data_error(conn):
    with pytest.raises(DataError):
        conn.cursor().execute("SELECT ?", (Decimal("1E+999999999"),))


# ---- 5. bare str/bytes parameters are a misuse, not a char sequence ---------

def test_bare_string_parameters_rejected(conn):
    # list("abc") == ['a','b','c']:占位符数恰吻合时曾静默绑定 97/98/99。
    with pytest.raises(ProgrammingError):
        conn.cursor().execute("SELECT ?, ?, ?", "abc")
    with pytest.raises(ProgrammingError):
        conn.cursor().execute("SELECT ?", b"x")
    with pytest.raises(ProgrammingError):
        conn.cursor().execute("SELECT ?", bytearray(b"x"))
    # 合法序列不受影响。
    with conn.cursor() as cur:
        cur.execute("SELECT ?, ?, ?", ("a", "b", "c"))
        assert cur.fetchall() == [("a", "b", "c")]
        cur.execute("SELECT ?", ["abc"])
        assert cur.fetchall() == [("abc",)]


# ---- 6. header decode failures poison the transport -------------------------

class _ScriptedSock:
    def __init__(self, data):
        self._data = data
        self.closed = False

    def recv(self, n):
        chunk, self._data = self._data[:n], self._data[n:]
        return chunk

    def settimeout(self, v):
        pass

    def close(self):
        self.closed = True

    def sendall(self, b):
        raise AssertionError("write attempted on a desynced transport")


def _transport_with_sock(sock):
    tr = connmod._Transport.__new__(connmod._Transport)
    tr._lock = threading.Lock()
    tr._closed = False
    tr._timeout = 15.0
    tr._poll_buf = bytearray()
    tr._partial_since = None
    tr._sealer = None
    tr._sock = sock
    return tr


def test_bad_magic_header_poisons_transport():
    # 坏 magic 曾不毒化:write_frame 仍会往失步的流里写下一帧,违背
    # _Transport「失败后续使用必抛」契约。
    header = HEADER.pack(0xDEADBEEF, 0, 0x0101, 0, 0)
    sock = _ScriptedSock(header)
    tr = _transport_with_sock(sock)
    with pytest.raises(ProtocolError):
        tr.read_frame()
    assert tr._closed and sock.closed, "header decode failure left the transport reusable"
    with pytest.raises(InterfaceError):
        tr.write_frame(REQ_PING, b"")


def test_oversized_advertised_length_poisons_transport():
    header = HEADER.pack(MAGIC, 0, 0x0101, 0, 64 * 1024 * 1024 + 1)
    sock = _ScriptedSock(header)
    tr = _transport_with_sock(sock)
    with pytest.raises(ProtocolError):
        tr.read_frame()
    assert tr._closed and sock.closed


def test_bad_magic_via_poll_path_poisons_transport():
    tr = _transport_with_sock(_ScriptedSock(b""))
    tr._poll_buf = bytearray(HEADER.pack(0x11223344, 0, 0x0101, 0, 0))
    with pytest.raises(ProtocolError):
        tr.read_frame_poll(0.01)
    assert tr._closed


# ---- 7. resume watermark advances only after a successful callback ----------

def test_watermark_advances_only_after_successful_callback():
    # 回调抛异常的消息曾先推进 _last_id:断线重连后按水位续传,永不补投。
    st = _stub_subscriber()
    st._channels = {"ch": None}

    def boom(msg):
        raise RuntimeError("consumer failed")

    st._on_message = boom
    st._dispatch(json.dumps({"channel": "ch", "id": 7, "payload": "x"}).encode())
    assert st._last_id == {}, "failed delivery advanced the resume cursor"

    seen = []
    st._on_message = seen.append
    st._dispatch(json.dumps({"channel": "ch", "id": 7, "payload": "x"}).encode())
    assert st._last_id == {"ch": 7}
    # 旧 id 不回退水位(既有语义不变)。
    st._dispatch(json.dumps({"channel": "ch", "id": 3, "payload": "y"}).encode())
    assert st._last_id == {"ch": 7}
    assert len(seen) == 2


# ---- 8. keepalive must not block on a held write lock ------------------------

def test_keepalive_does_not_block_on_held_write_lock():
    # _control_busy 检查与取锁之间的 TOCTOU:控制调用持锁等回复(最长 10s),
    # 读线程曾阻塞在锁上 —— 而回复恰恰只能由读线程入队。
    st = _stub_subscriber()
    tr = st._conn._transport
    release = threading.Event()

    def hold():
        with st._write_lock:
            release.wait(3)

    holder = threading.Thread(target=hold, daemon=True)
    holder.start()
    time.sleep(0.1)

    orig_poll, orig_keep = submod._POLL_SECONDS, submod._KEEPALIVE_POLLS
    submod._POLL_SECONDS, submod._KEEPALIVE_POLLS = 0.05, 2
    try:
        t0 = time.monotonic()
        reader = threading.Thread(target=st._read_frames, daemon=True)
        reader.start()
        time.sleep(0.4)
        st._closed.set()
        reader.join(1.0)
        elapsed = time.monotonic() - t0
        assert not reader.is_alive(), "reader blocked on the write lock during keepalive"
        assert elapsed < 1.5
        assert tr.written == [], "keepalive PING written while another owner held the lock"
    finally:
        submod._POLL_SECONDS, submod._KEEPALIVE_POLLS = orig_poll, orig_keep
        release.set()
        holder.join(1)


# ---- 9. late replies after a control timeout must never be mistaken for ACKs

def test_control_timeout_marks_queue_stale_and_poisons(monkeypatch):
    # 超时路径:置 stale + 排空 + 毒化 + OperationalError。
    st = _stub_subscriber()

    class _NeverReplies:
        def get(self, timeout=None):
            raise queue.Empty

        def get_nowait(self):
            raise queue.Empty

    st._replies = _NeverReplies()
    with pytest.raises(OperationalError):
        st._control(REQ_SUBSCRIBE, {"channel": "x"})
    assert st._replies_stale is True
    assert st._conn._transport.poisoned == 1


def test_reader_drops_replies_while_stale():
    # 毒化置 stale 后,读线程从旧线路上读到的回复直接丢弃,不再入队。
    st = _stub_subscriber()
    st._replies_stale = True
    st._control_busy = True  # suppress the keepalive path entirely
    st._conn._transport._poll_results = [(0, RESP_AFFECTED, b"\x00" * 8)]
    t = threading.Thread(target=st._read_frames, daemon=True)
    t.start()
    time.sleep(0.2)
    st._closed.set()
    t.join(1)
    assert not t.is_alive()
    assert st._replies.qsize() == 0, "stale frame queued for the next control call"


def test_resubscribe_drains_stale_replies_and_clears_flag(monkeypatch):
    # 迟到回复若赶在超时排水之后入队(与 put 的竞态),由重连侧的无竞产者排水
    # 兜底回收,并复位 stale —— 下一个控制调用只可能看到新连接的回复。
    st = _stub_subscriber()
    st._channels = {"ch": "latest"}
    st._replies_stale = True
    st._replies.put((0, RESP_AFFECTED, b"stale-ack"))  # the reply that raced the drain
    conn = _StubConn(_StubTransport(frames=[(0, RESP_PONG, b"")]))
    monkeypatch.setattr(submod, "connect", lambda **kw: conn)
    monkeypatch.setattr(submod.time, "sleep", lambda s: None)

    assert st._resubscribe() is True
    assert st._replies_stale is False
    assert st._replies.qsize() == 0
    # The fresh control sequence went out on the new wire.
    assert conn._transport.written[0][0] == REQ_SUBSCRIBE


# ---- 10. lone surrogates in bind text raise DataError, not UnicodeEncodeError

def test_lone_surrogate_param_raises_data_error():
    # 孤立代理项字符串在最终 .encode("utf-8") 才炸;裸 UnicodeEncodeError 曾逃出
    # execute(),绕过用户的 except Error 恢复路径。
    with pytest.raises(DataError):
        execute_payload(7, ["\ud800"])
    with pytest.raises(DataError):
        execute_payload(7, [{"k": "\udfff"}])


def test_lone_surrogate_param_via_execute(conn):
    with pytest.raises(DataError):
        conn.cursor().execute("SELECT ?", ("\ud800",))
    # 语句级错误:连接仍可用。
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchall() == [(1,)]
