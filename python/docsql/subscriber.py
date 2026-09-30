"""Persistent pub/sub subscriber: one dedicated connection + one reader
thread (subscription connections are one-way streaming — a plain
request/response connection would misread RESP_PUSH as the next reply).

Delivery is at-least-once with per-node monotonic ids. ``subscribe(ch,
from_=id)`` resumes strictly AFTER that id (server semantics:
``id > after``), so the subscriber tracks the newest delivered id per
channel and reconnects replay exactly the missed tail — no loss, no
duplicate. Pattern subscriptions resume from ``latest`` (a glob has no
single resume cursor).

Callbacks run on the reader thread: keep them fast or dispatch to your
own executor. An exception escaping a callback is logged, never fatal.
"""

import json
import queue
import threading
import time

from . import _proto
from .connection import connect
from .errors import Error, InterfaceError


class Subscriber:
    """Subscribe to DocSQL pub/sub channels with automatic resubscribe.

    Example::

        sub = docsql.Subscriber(
            on_message=lambda m: print(m["channel"], m["payload"]),
            host="127.0.0.1", port=7600, token="tok",
        )
        sub.subscribe("news")
        ...
        sub.close()
    """

    def __init__(
        self,
        on_message,
        on_disconnect=None,
        auto_resubscribe=True,
        reconnect_attempts=10,
        **conn_kwargs,
    ):
        self._on_message = on_message
        self._on_disconnect = on_disconnect
        self._auto = auto_resubscribe
        self._attempts = reconnect_attempts
        self._conn_kwargs = dict(conn_kwargs)
        self._channels = {}   # channel -> default from (str)
        self._patterns = {}   # pattern -> default from (str)
        self._last_id = {}    # channel -> newest delivered id
        self._replies = queue.Queue()
        self._write_lock = threading.Lock()
        self._closed = threading.Event()
        self._conn = None
        self._reader = threading.Thread(target=self._run, name="docsql-subscriber", daemon=True)
        self._conn = connect(**self._conn_kwargs)
        self._reader.start()

    # -- public API ---------------------------------------------------------
    def subscribe(self, channel, from_="latest"):
        """Subscribe one channel; ``from_`` is "latest", "earliest" or an
        id — replay starts strictly AFTER that id (resume cursor)."""
        self._control(_proto.REQ_SUBSCRIBE, {"channel": channel, "from": _from_str(from_)})
        self._channels[channel] = _from_str(from_)
        return self

    def psubscribe(self, pattern, from_="latest"):
        """Subscribe a Redis-style glob pattern (``*``, ``?``, ``[...]``)."""
        self._control(_proto.REQ_PSUBSCRIBE, {"pattern": pattern, "from": _from_str(from_)})
        self._patterns[pattern] = _from_str(from_)
        return self

    def unsubscribe(self, *channels):
        self._control(_proto.REQ_UNSUBSCRIBE, list(channels))
        for c in channels:
            self._channels.pop(c, None)
            self._last_id.pop(c, None)
        return self

    def punsubscribe(self, *patterns):
        self._control(_proto.REQ_PUNSUBSCRIBE, list(patterns))
        for p in patterns:
            self._patterns.pop(p, None)
        return self

    def close(self):
        self._closed.set()
        conn = self._conn
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        reader = self._reader
        if reader is not None:
            reader.join(timeout=5)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    # -- internals ------------------------------------------------------------
    def _control(self, frame_type, body):
        payload = json.dumps(body).encode("utf-8")
        with self._write_lock:
            self._conn._transport.write_frame(frame_type, payload)
            while True:
                _flags, ftype, rpayload = self._replies.get(timeout=10)
                if ftype == _proto.RESP_PUSH:
                    self._dispatch(rpayload)
                    continue
                if ftype == _proto.RESP_ERROR:
                    raise InterfaceError(rpayload.decode("utf-8", "replace"))
                break

    def _dispatch(self, payload):
        try:
            msg = json.loads(payload.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return
        channel = msg.get("channel")
        mid = msg.get("id")
        if isinstance(mid, int) and not isinstance(mid, bool):
            prev = self._last_id.get(channel)
            if prev is None or mid > prev:
                self._last_id[channel] = mid
        try:
            self._on_message(msg)
        except Exception:
            import sys

            print("docsql.Subscriber: on_message callback raised", file=sys.stderr)

    def _run(self):
        """Reader-thread main loop: read until the connection breaks, then
        (optionally) reconnect + resubscribe and keep going."""
        while not self._closed.is_set():
            try:
                self._read_frames()
            except Exception:
                if self._closed.is_set():
                    return
                if self._on_disconnect is not None:
                    try:
                        self._on_disconnect()
                    except Exception:
                        pass
                if not self._auto or not (self._channels or self._patterns):
                    return
                if not self._resubscribe():
                    return
                # Loop: keep reading on the fresh connection.

    def _read_frames(self):
        while not self._closed.is_set():
            _flags, ftype, payload = self._conn._transport.read_frame()
            if ftype == _proto.RESP_PUSH:
                self._dispatch(payload)
            else:
                self._replies.put((_flags, ftype, payload))

    def _resubscribe(self):
        """Reconnect and replay the missed tail; False when the node stays
        unreachable for the attempt budget."""
        delay = 1.0
        for _ in range(self._attempts):
            if self._closed.is_set():
                return False
            time.sleep(delay)
            delay = min(delay * 2, 8.0)
            try:
                self._conn = connect(**self._conn_kwargs)
            except Error:
                continue
            try:
                # Resubscribes resume strictly after the newest id seen on
                # the channel (server replays id > from).
                for channel in list(self._channels):
                    resume = self._last_id.get(channel, self._channels[channel])
                    body = {"channel": channel, "from": _from_str(resume)}
                    self._conn._transport.write_frame(
                        _proto.REQ_SUBSCRIBE, json.dumps(body).encode("utf-8")
                    )
                for pattern in list(self._patterns):
                    body = {"pattern": pattern, "from": _from_str(self._patterns[pattern])}
                    self._conn._transport.write_frame(
                        _proto.REQ_PSUBSCRIBE, json.dumps(body).encode("utf-8")
                    )
                # A PING marks the end of the confirmation batch: drain the
                # confirms (and any replay pushes) until the PONG lands,
                # then hand the socket back to the reader loop.
                self._conn._transport.write_frame(_proto.REQ_PING, b"")
                while True:
                    _flags, ftype, payload = self._conn._transport.read_frame()
                    if ftype == _proto.RESP_PUSH:
                        self._dispatch(payload)
                    elif ftype == _proto.RESP_PONG:
                        return True
                    # RESP_AFFECTED subscribe confirmations pass silently.
            except Error:
                continue
        return False


def _from_str(value):
    if value is None:
        return "latest"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    return str(value)
