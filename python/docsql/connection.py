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
    - ``timeout``: seconds for connect AND each read/write operation.
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
    AES-GCM sealing. Owns the round-trip lock."""

    def __init__(self, host, port, tls, tls_ca, tls_hostname, key, timeout):
        self._lock = threading.Lock()
        self._closed = False
        self.timeout = timeout
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
        except Exception:
            try:
                self._sock.close()
            except OSError:
                pass
            raise

    # -- socket plumbing ---------------------------------------------------
    def _read_exact(self, n):
        buf = bytearray()
        while len(buf) < n:
            try:
                chunk = self._sock.recv(n - len(buf))
            except (socket.timeout, TimeoutError) as e:
                raise OperationalError(f"read timed out after {self.timeout}s") from e
            except OSError as e:
                raise InterfaceError(f"connection lost during read: {e}") from e
            if not chunk:
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

    def read_frame(self):
        """Read one frame; returns (flags, frame_type, payload). Not sealed
        responses are rejected on keyed connections, mirroring the server."""
        flags, frame_type, length = _proto.decode_header(self._read_exact(_proto.HEADER_LEN))
        payload = self._read_exact(length)
        if self._sealer is not None:
            if not flags & _proto.FLAG_ENCRYPTED:
                raise InterfaceError(
                    f"server sent an unencrypted frame {frame_type:#06x} on a keyed connection"
                )
            try:
                payload = self._sealer.open(frame_type, flags, payload, self._challenge)
            except Exception as e:
                raise InterfaceError(f"frame decryption failed: {e}") from e
        return flags, frame_type, payload

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
            raise OperationalError(f"write timed out after {self.timeout}s") from e
        except OSError as e:
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
        self._tx_open = False
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
        self._transport.close()

    def commit(self):
        if self.autocommit:
            return
        if self._tx_open:
            self._end_tx("COMMIT")

    def rollback(self):
        if self.autocommit:
            return
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
        lost connection with the original parameters."""
        if reconnect:
            try:
                self._transport.round_trip(_proto.REQ_PING, b"")
                return
            except Error:
                self.close()
                fresh = connect(**self._kwargs)
                self.__dict__.update(fresh.__dict__)
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
        self._prepared = {}

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
        handle = self._prepared.get(template)
        if handle is not None:
            return handle
        if len(self._prepared) >= _PREPARED_CAPACITY:
            for h in self._prepared.values():
                self._conn._transport.round_trip(
                    _proto.REQ_CLOSE_STMT, b'{"handle":%d}' % h
                )
            self._prepared.clear()
        ftype, payload = self._conn._transport.round_trip(
            _proto.REQ_PREPARE, sql_payload(template)
        )
        _check(ftype, payload)
        import json

        handle = json.loads(payload.decode("utf-8"))["handle"]
        self._prepared[template] = handle
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
