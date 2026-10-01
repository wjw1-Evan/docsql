"""DB-API 2.0 connection/cursor over the DocSQL v1 binary protocol.

Design notes (deviations from PEP 249 are deliberate and documented):

- **autocommit defaults to True.** DocSQL is a network database with a
  single global writer: an implicitly-open transaction (the DB-API
  default) would park the write path for the whole deployment while the
  Python process idles. With ``autocommit=False`` the driver sends BEGIN
  before the first statement of each block (psycopg2-style) and issues
  COMMIT/ROLLBACK on ``commit()``/``rollback()``; do not write your own
  BEGIN in that mode.
- ``paramstyle`` is ``qmark``: templates go through REQ_PREPARE and
  parameters are bound SERVER-SIDE (REQ_EXECUTE) — the template with
  ``?`` placeholders is registered once per connection and reused, and a
  bound value can never break out of its literal.
- Exact scalars: ``decimal.Decimal`` ↔ DECIMAL (the ``$dec`` wire marker),
  ``datetime.datetime`` (UTC) ↔ TIMESTAMP (``$ts``), ``bytes`` ↔ BLOB
  (``$bytes``). Python ints beyond int64 bind through the DECIMAL path to
  keep full precision.
- Threadsafety is 1: one connection serializes its own round trips with a
  lock; share connections across threads only with external
  synchronization (or open one per thread).
"""

import socket
import ssl
import threading

import time

from . import _proto
from ._proto import sql_payload
from ._convert import execute_payload, rows_from_payload, ts_ms
from ._crypto import Sealer
from .errors import (
    DatabaseError,
    Error,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    map_server_error,
)

apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"

_PREPARED_CAPACITY = 96
# How long a PARTIAL frame may sit unread before the poll loop declares
# the link dead (mid-frame peer death leaves no EOF — only silence).
_PARTIAL_FRAME_MAX_WAIT = 30.0


def connect(
    host="127.0.0.1",
    port=7600,
    token=None,
    user=None,
    password=None,
    tls=False,
    tls_ca=None,
    tls_hostname=None,
    key=None,
    timeout=15.0,
    autocommit=True,
):
    """Open a connection and authenticate.

    - ``token``: node client token (REQ_AUTH). ``user``+``password``:
      database user login (REQ_AUTH_USER); when both are given the user
      login wins, like the .NET driver.
    - ``tls``: native TLS (the server needs DOCSQL_TLS_CERT/KEY).
      ``tls_ca``: PEM path verifying the server certificate; omitted =
      encrypt-only (self-signed fleets, no verification).
      ``tls_hostname``: SNI/certificate name override when dialing by IP
      against a DNS-named certificate.
    - ``key``: legacy AES-256-GCM frame sealing (DOCSQL_KEY hex). Requires
      the optional ``cryptography`` package; prefer ``tls`` on new
      deployments.
    - ``timeout``: seconds for connect AND each read/write operation. A
      timeout POISONS the connection (the server still answers the timed-
      out statement later — a reused socket would misalign requests and
      replies by one frame); reconnect after catching it.
    """
    return Connection(
        host=host,
        port=port,
        token=token,
        user=user,
        password=password,
        tls=tls,
        tls_ca=tls_ca,
        tls_hostname=tls_hostname,
        key=key,
        timeout=timeout,
        autocommit=autocommit,
    )


class _Transport:
    """One connected socket: framed reads/writes, optional TLS, optional
    AES-GCM sealing. Owns the round-trip lock.

    Any read/write failure (timeout included) POISONS the transport: the
    server still completes the in-flight statement and writes its reply,
    so a live-looking socket after a timeout would hand the NEXT round
    trip a stale answer (silent one-frame misalignment). Every later use
    raises InterfaceError until the application reconnects.
    """

    def __init__(self, host, port, tls, tls_ca, tls_hostname, key, timeout):
        self._lock = threading.Lock()
        self._closed = False
        self._timeout = timeout  # create_connection already applied it
        # Partial-frame buffer for `read_frame_poll` (keepalive loops read
        # with a bounded wait; a header may arrive without its payload).
        self._poll_buf = bytearray()
        # When the current partial frame started waiting (see
        # _PARTIAL_FRAME_MAX_WAIT): None while the buffer is empty.
        self._partial_since = None
        try:
            raw = socket.create_connection((host, port), timeout=timeout)
        except OSError as e:
            raise OperationalError(f"connect to {host}:{port} failed: {e}") from e
        self._sock = raw
        try:
            if tls:
                ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                if tls_ca:
                    ctx.load_verify_locations(cafile=tls_ca)
                    ctx.check_hostname = True
                    ctx.verify_mode = ssl.CERT_REQUIRED
                else:
                    # Encrypt-only posture, identical to the Rust/.NET
                    # dialers without a CA: traffic is protected, the peer
                    # certificate is NOT verified (self-signed fleets).
                    ctx.check_hostname = False
                    ctx.verify_mode = ssl.CERT_NONE
                raw = ctx.wrap_socket(
                    raw, server_hostname=tls_hostname or (host if _is_hostname(host) else None)
                )
            self._sock = raw
            if key is not None:
                self._sealer = Sealer(_parse_key_hex(key))
                self._read_hello()
            else:
                self._sealer = None
        except ssl.SSLError as e:
            # TLS handshake / certificate failures are reconnectable
            # conditions, not a broken Transport (none exists yet) — map
            # them or they escape every `except Error` recovery path and
            # kill subscriber reader threads.
            self._shutdown_sock()
            raise OperationalError(f"TLS handshake with {host}:{port} failed: {e}") from e
        except Exception:
            self._shutdown_sock()
            raise

    def _shutdown_sock(self):
        try:
            self._sock.close()
        except OSError:
            pass

    @property
    def timeout(self):
        """Per-operation socket timeout. The setter pushes into the socket:
        a plain attribute used to make later assignments silent no-ops (the
        recv timeout kept the connect-time value)."""
        return self._timeout

    @timeout.setter
    def timeout(self, value):
        self._timeout = value
        try:
            self._sock.settimeout(value)
        except OSError:
            pass

    def _poison(self):
        """Mark unusable and drop the socket (see the class docstring)."""
        self._closed = True
        self._shutdown_sock()

    # -- socket plumbing ---------------------------------------------------
    def _read_exact(self, n):
        buf = bytearray()
        while len(buf) < n:
            try:
                chunk = self._sock.recv(n - len(buf))
            except (socket.timeout, TimeoutError) as e:
                self._poison()
                raise OperationalError(f"read timed out after {self.timeout}s") from e
            except OSError as e:
                self._poison()
                raise InterfaceError(f"connection lost during read: {e}") from e
            if not chunk:
                self._poison()
                raise InterfaceError("connection closed by server")
            buf.extend(chunk)
        return bytes(buf)

    def _read_hello(self):
        flags, frame_type, length = _proto.decode_header(self._read_exact(_proto.HEADER_LEN))
        payload = self._read_exact(length)
        if frame_type != _proto.RESP_HELLO or length != 16:
            raise InterfaceError(
                "keyed node did not send its 16-byte RESP_HELLO challenge (version mismatch?)"
            )
        self._challenge = payload

    def _finalize_frame(self, flags, frame_type, payload):
        """Shared tail of both read paths: unseal keyed frames."""
        if self._sealer is not None:
            if not flags & _proto.FLAG_ENCRYPTED:
                raise InterfaceError(
                    f"server sent an unencrypted frame {frame_type:#06x} on a keyed connection"
                )
            try:
                payload = self._sealer.open(frame_type, flags, payload, self._challenge)
            except Error:
                self._poison()
                raise
            except Exception as e:
                self._poison()
                raise InterfaceError(f"frame decryption failed: {e}") from e
        return flags, frame_type, payload

    def read_frame(self):
        """Read one frame; returns (flags, frame_type, payload). Not sealed
        responses are rejected on keyed connections, mirroring the server."""
        flags, frame_type, length = _proto.decode_header(self._read_exact(_proto.HEADER_LEN))
        payload = self._read_exact(length)
        return self._finalize_frame(flags, frame_type, payload)

    def read_frame_poll(self, poll_seconds):
        """Read one frame with a bounded wait; None when the poll interval
        elapses fully idle. For subscription keepalive loops, where an idle
        poll means "time to PING", not "dead connection" — partial frames
        stay buffered across polls. Errors poison exactly like read_frame.
        Only the owning reader thread may call this."""
        while True:
            if len(self._poll_buf) >= _proto.HEADER_LEN:
                header = bytes(self._poll_buf[: _proto.HEADER_LEN])
                flags, frame_type, length = _proto.decode_header(header)
                if len(self._poll_buf) >= _proto.HEADER_LEN + length:
                    payload = bytes(
                        self._poll_buf[_proto.HEADER_LEN : _proto.HEADER_LEN + length]
                    )
                    del self._poll_buf[: _proto.HEADER_LEN + length]
                    self._partial_since = None
                    return self._finalize_frame(flags, frame_type, payload)
            try:
                self._sock.settimeout(poll_seconds)
                chunk = self._sock.recv(65536)
            except (socket.timeout, TimeoutError):
                if len(self._poll_buf) > 0:
                    # A partial frame is in flight: keep waiting for the
                    # rest rather than reporting idle — but never forever.
                    # A peer dying mid-frame leaves the stream truncated;
                    # an unbounded wait silenced the idle signal (and with
                    # it the keepalive PING) while the subscriber merely
                    # LOOKED alive.
                    now = time.monotonic()
                    if self._partial_since is None:
                        self._partial_since = now
                    elif now - self._partial_since > _PARTIAL_FRAME_MAX_WAIT:
                        self._poison()
                        raise InterfaceError(
                            "transport stalled mid-frame for over "
                            f"{_PARTIAL_FRAME_MAX_WAIT:.0f}s; connection poisoned"
                        )
                    continue
                self._partial_since = None
                return None
            except OSError as e:
                self._poison()
                raise InterfaceError(f"connection lost during read: {e}") from e
            finally:
                try:
                    self._sock.settimeout(self.timeout)
                except OSError:
                    pass
            if not chunk:
                self._poison()
                raise InterfaceError("connection closed by server")
            self._poll_buf.extend(chunk)

    def write_frame(self, frame_type, payload, flags=0):
        if self._closed:
            raise InterfaceError("connection is closed")
        if self._sealer is not None:
            flags |= _proto.FLAG_ENCRYPTED
            payload = self._sealer.seal(frame_type, flags, payload, self._challenge)
        wire = _proto.encode_frame(frame_type, payload, flags)
        try:
            self._sock.sendall(wire)
        except (socket.timeout, TimeoutError) as e:
            self._poison()
            raise OperationalError(f"write timed out after {self.timeout}s") from e
        except OSError as e:
            self._poison()
            raise InterfaceError(f"connection lost during write: {e}") from e

    def round_trip(self, frame_type, payload, flags=0):
        """Send one frame and read the reply, skipping stray RESP_PUSH
        frames (defensive: pushes only go to subscribed connections)."""
        with self._lock:
            self.write_frame(frame_type, payload, flags)
            while True:
                _flags, ftype, rpayload = self.read_frame()
                if ftype == _proto.RESP_PUSH:
                    continue
                return ftype, rpayload

    def close(self):
        with self._lock:
            self._closed = True
            try:
                self._sock.close()
            except OSError:
                pass


def _is_hostname(host):
    # IPs get no server_hostname (TLS 1.3 forbids IP SNI mismatch noise;
    # verification against an IP still works via the SAN IP entry).
    try:
        socket.inet_aton(host)
        return False
    except OSError:
        return True


def _parse_key_hex(hex_key):
    text = (hex_key or "").strip()
    if len(text) != 64:
        raise OperationalError(f"DOCSQL_KEY must be 64 hex chars, got {len(text)}")
    try:
        return bytes.fromhex(text)
    except ValueError as e:
        raise OperationalError(f"DOCSQL_KEY is not valid hex: {e}") from e


class Connection:
    """A DB-API connection. Use as a context manager (closes on exit)."""

    def __init__(self, **kwargs):
        self._kwargs = kwargs
        self.autocommit = kwargs.get("autocommit", True)
        self._transport = _Transport(
            host=kwargs.get("host", "127.0.0.1"),
            port=kwargs.get("port", 7600),
            tls=kwargs.get("tls", False),
            tls_ca=kwargs.get("tls_ca"),
            tls_hostname=kwargs.get("tls_hostname"),
            key=kwargs.get("key"),
            timeout=kwargs.get("timeout", 15.0),
        )
        self._cursors = set()
        # Server-side prepared handles are PER-CONNECTION state; the client
        # cache lives here too so every cursor shares it. A per-cursor cache
        # re-prepared the same template for each short-lived cursor and
        # leaked handles until the server's per-connection cap hard-blocked
        # every further prepare on this wire.
        self._prepared = {}
        self._tx_open = False
        # Set when a reconnect dropped an open transaction: the server
        # rolled it back on disconnect, so a later commit() must NOT claim
        # success (the caller would believe durable what was discarded).
        self._tx_lost = False
        self._closed = False
        try:
            self._authenticate(kwargs)
            # Fail fast on dead/broken endpoints: a PING round trip after
            # auth also proves the wire decodes end to end.
            ftype, payload = self._transport.round_trip(_proto.REQ_PING, b"")
            _check(ftype, payload)
        except Exception:
            self._transport.close()
            raise

    def _authenticate(self, kwargs):
        user = kwargs.get("user")
        password = kwargs.get("password")
        token = kwargs.get("token")
        if user:
            import json

            body = json.dumps({"user": user, "password": password or ""}).encode("utf-8")
            ftype, payload = self._transport.round_trip(_proto.REQ_AUTH_USER, body)
            _check(ftype, payload)
        elif token:
            ftype, payload = self._transport.round_trip(_proto.REQ_AUTH, token.encode("utf-8"))
            _check(ftype, payload)

    # -- DB-API surface ----------------------------------------------------
    def close(self):
        if self._closed:
            return
        self._closed = True
        for cur in list(self._cursors):
            cur._closed = True
        self._cursors.clear()
        # Release prepared handles while the wire still answers (the server
        # frees them on disconnect anyway; this just keeps the accounting
        # clean). Best effort — a poisoned transport raises below anyway.
        try:
            for h in self._prepared.values():
                self._transport.round_trip(
                    _proto.REQ_CLOSE_STMT, b'{"handle":%d}' % h
                )
        except Exception:
            pass
        self._prepared.clear()
        self._transport.close()

    def commit(self):
        if self.autocommit:
            return
        if self._tx_lost:
            self._tx_lost = False
            raise OperationalError(
                "transaction lost: the connection dropped and the server rolled "
                "the open transaction back; retry the work on the new connection"
            )
        if self._tx_open:
            self._end_tx("COMMIT")

    def rollback(self):
        if self.autocommit:
            return
        # A lost transaction is already rolled back server-side; a further
        # ROLLBACK would lie about having undone something. Just clear.
        self._tx_lost = False
        if self._tx_open:
            self._end_tx("ROLLBACK")

    def _end_tx(self, verb):
        ftype, payload = self._transport.round_trip(_proto.REQ_SQL, sql_payload(verb))
        _check(ftype, payload)
        self._tx_open = False

    def cursor(self):
        if self._closed:
            raise ProgrammingError("connection is closed")
        cur = Cursor(self)
        self._cursors.add(cur)
        return cur

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()
        finally:
            self.close()
        return False

    # -- extras -------------------------------------------------------------
    def ping(self, reconnect=False):
        """Keepalive probe (subscriptions need periodic PING when the node
        sets DOCSQL_IDLE_TIMEOUT). ``reconnect=True`` retries once on a
        lost connection with the original parameters: prepared-statement
        handles (per-connection server state) are invalidated and an open
        transaction is reported lost — the server rolled it back on
        disconnect, and a silent no-op commit() would hide that."""
        if reconnect:
            try:
                self._transport.round_trip(_proto.REQ_PING, b"")
                return
            except Exception:
                # Targeted swap, not a whole-object replace: cursors stay
                # usable, but their prepared-statement handles (per-
                # connection SERVER state) died with the old socket — clear
                # the caches so the next bound execute re-prepares instead
                # of failing on an unknown handle.
                tx_lost = self._tx_open or self._tx_lost
                self._transport.close()
                fresh = connect(**self._kwargs)
                self._transport = fresh._transport
                # Handles died with the old socket — the cache is
                # connection-level now; drop it once, not per cursor.
                self._prepared.clear()
                self._tx_open = False
                self._tx_lost = tx_lost
                return
        self._transport.round_trip(_proto.REQ_PING, b"")

    def status(self):
        """Node status report (REQ_STATUS) as a dict — handy for health
        checks and dashboards."""
        ftype, payload = self._transport.round_trip(_proto.REQ_STATUS, b"")
        _check(ftype, payload)
        import json

        return json.loads(payload.decode("utf-8"))

    def publish(self, channel, payload):
        """Persistent pub/sub publish; returns (id, receivers)."""
        import json

        body = json.dumps({"channel": channel, "payload": payload}).encode("utf-8")
        ftype, resp = self._transport.round_trip(_proto.REQ_PUBLISH, body)
        _check(ftype, resp)
        columns, rows = rows_from_payload(resp)
        if not rows:
            raise InterfaceError("PUBLISH reply carried no row")
        return rows[0][0], rows[0][1]


class Cursor:
    """DB-API cursor. Use as a context manager."""

    def __init__(self, conn):
        self._conn = conn
        self._closed = False
        self.arraysize = 1
        self._columns = None
        self._rows = []
        self._pos = 0
        self.rowcount = -1

    # -- statement execution -----------------------------------------------
    def execute(self, operation, parameters=None):
        if self._closed:
            raise ProgrammingError("cursor is closed")
        if self._conn._closed:
            raise ProgrammingError("connection is closed")
        self._columns = None
        self._rows = []
        self._pos = 0
        self.rowcount = -1
        if not self._conn.autocommit and not self._conn._tx_open:
            # Implicit transaction block (see the module docstring for why
            # this is opt-in rather than the default).
            ftype, payload = self._conn._transport.round_trip(
                _proto.REQ_SQL, sql_payload("BEGIN")
            )
            _check(ftype, payload)
            self._conn._tx_open = True
            # A new transaction supersedes any lost one: its commit will be
            # real, so the loss must not fail it retroactively.
            self._conn._tx_lost = False
        if parameters is None:
            ftype, payload = self._transport_round_trip_sql(operation)
        else:
            params = list(parameters)
            handle = self._prepare(operation)
            ftype, payload = self._conn._transport.round_trip(
                _proto.REQ_EXECUTE, execute_payload(handle, params)
            )
        if ftype == _proto.RESP_ROWS:
            self._columns, self._rows = rows_from_payload(payload)
        elif ftype == _proto.RESP_AFFECTED:
            self.rowcount = int.from_bytes(payload[:8], "little", signed=True)
        else:
            _check(ftype, payload)  # raises

    def _transport_round_trip_sql(self, sql):
        return self._conn._transport.round_trip(_proto.REQ_SQL, sql_payload(sql))

    def executemany(self, operation, seq_of_parameters):
        total = 0
        saw_rows = False
        for params in seq_of_parameters:
            self.execute(operation, params)
            if self.rowcount >= 0:
                total += self.rowcount
            else:
                saw_rows = True
        self.rowcount = -1 if saw_rows else total

    def _prepare(self, template):
        # See Connection._prepared: one shared per-connection handle cache.
        cached = self._conn._prepared.get(template)
        if cached is not None:
            return cached
        if len(self._conn._prepared) >= _PREPARED_CAPACITY:
            for h in self._conn._prepared.values():
                self._conn._transport.round_trip(
                    _proto.REQ_CLOSE_STMT, b'{"handle":%d}' % h
                )
            self._conn._prepared.clear()
        ftype, payload = self._conn._transport.round_trip(
            _proto.REQ_PREPARE, sql_payload(template)
        )
        _check(ftype, payload)
        import json

        handle = json.loads(payload.decode("utf-8"))["handle"]
        self._conn._prepared[template] = handle
        return handle

    # -- fetching ------------------------------------------------------------
    @property
    def description(self):
        if self._columns is None:
            return None
        return [(name, None, None, None, None, None, None) for name in self._columns]

    def _require_rows(self):
        if self._columns is None:
            raise ProgrammingError("no result set — the last statement produced no rows")

    def fetchone(self):
        self._require_rows()
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size=None):
        self._require_rows()
        size = self.arraysize if size is None else size
        out = self._rows[self._pos : self._pos + size]
        self._pos += len(out)
        return out

    def fetchall(self):
        self._require_rows()
        out = self._rows[self._pos :]
        self._pos = len(self._rows)
        return out

    def nextset(self):
        return None  # single result set per statement (multi-statement batches need separate execute calls)

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    def close(self):
        self._closed = True
        self._conn._cursors.discard(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    # Iteration over the result set.
    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row


def _check(frame_type, payload):
    """Raise the mapped DB-API error for a RESP_ERROR reply."""
    if frame_type == _proto.RESP_ERROR:
        text = payload.decode("utf-8", "replace")
        lowered = text.lower()
        if "unauthorized" in lowered or "bad token" in lowered or "bad username" in lowered:
            raise OperationalError(text)
        raise map_server_error(text)
    return frame_type
