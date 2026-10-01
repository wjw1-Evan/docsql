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
import sys
import threading
import time

from . import _proto
from .connection import connect
from .errors import OperationalError, map_server_error

# Keepalive cadence: poll the socket for a frame every _POLL_SECONDS and
# send a PING once _KEEPALIVE_POLLS consecutive polls came back idle
# (~15 s of silence). Without it the reader used to block on a plain read
# until the connection timeout fired, treat the timeout as a dead link and
# churn a reconnect cycle every ~15 s on quiet channels — self-inflicted
# disconnects with on_disconnect noise, and a real message-loss window
# while resubscribing.
_POLL_SECONDS = 5.0
_KEEPALIVE_POLLS = 3


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
        # True while a control round trip (subscribe/unsubscribe) is between
        # its request and its reply: the reader must not inject a keepalive
        # PING then, or its PONG could be mistaken for the control reply.
        self._control_busy = False
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
            self._control_busy = True
            try:
                self._conn._transport.write_frame(frame_type, payload)
                while True:
                    try:
                        _flags, ftype, rpayload = self._replies.get(timeout=10)
                    except queue.Empty as e:
                        # The reply may still land later — and from that
                        # moment the queue can no longer be matched to
                        # requests (a late ACK would be consumed as the
                        # NEXT operation's reply: a one-off control
                        # desync). Drain anything pending and POISON the
                        # transport: the reader loop then reconnects and
                        # resubscribes every recorded channel from its last
                        # seen id. This channel is NOT recorded (the
                        # subscribe did not complete) — retry it explicitly.
                        while True:
                            try:
                                self._replies.get_nowait()
                            except queue.Empty:
                                break
                        try:
                            self._conn._transport._poison()
                        except Exception:
                            pass
                        raise OperationalError(
                            "subscriber control reply timed out (no frame within 10s); "
                            "connection poisoned — the reader loop reconnects and "
                            "resubscribes recorded channels, retry this call"
                        ) from e
                    if ftype == _proto.RESP_PUSH:
                        self._dispatch(rpayload)
                        continue
                    if ftype == _proto.RESP_PONG:
                        continue  # keepalive echo racing our request
                    if ftype == _proto.RESP_ERROR:
                        # A server-side rejection is a statement error, not a
                        # transport failure — InterfaceError would wrongly
                        # declare the whole connection dead.
                        raise map_server_error(rpayload.decode("utf-8", "replace"))
                    break
            finally:
                self._control_busy = False

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
        idle_polls = 0
        while not self._closed.is_set():
            got = self._conn._transport.read_frame_poll(_POLL_SECONDS)
            if got is None:
                idle_polls += 1
                if idle_polls >= _KEEPALIVE_POLLS and not self._control_busy:
                    idle_polls = 0
                    # Idle keepalive: proves the connection (and beats
                    # DOCSQL_IDLE_TIMEOUT). The PONG echo is dropped below;
                    # never sent while a control round trip may claim it.
                    with self._write_lock:
                        if not self._conn._transport._closed:
                            self._conn._transport.write_frame(_proto.REQ_PING, b"")
                continue
            idle_polls = 0
            _flags, ftype, payload = got
            if ftype == _proto.RESP_PUSH:
                self._dispatch(payload)
            elif ftype == _proto.RESP_PONG:
                pass  # keepalive echo (the resubscribe drain has its own PONG)
            else:
                self._replies.put(got)

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
            except Exception:
                # Transport-level failures too (TLS handshake, protocol
                # garbage): catching only Error used to let them escape the
                # reader thread entirely, killing the subscriber silently.
                continue
            try:
                # The write+drain holds the write lock: a concurrent
                # subscribe() interleaving its own frames on the raw socket
                # used to corrupt both request streams.
                with self._write_lock:
                    self._control_busy = True
                    try:
                        # Resubscribes resume strictly after the newest id
                        # seen on the channel (server replays id > from).
                        # A channel with no delivered anchor falls back to
                        # its requested from — messages published inside
                        # this reconnect window are only recoverable with
                        # an explicit numeric from_.
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
                        # A PING marks the end of the confirmation batch:
                        # drain the confirms (and any replay pushes) until
                        # the PONG lands, then hand the socket back to the
                        # reader loop.
                        self._conn._transport.write_frame(_proto.REQ_PING, b"")
                        while True:
                            _flags, ftype, payload = self._conn._transport.read_frame()
                            if ftype == _proto.RESP_PUSH:
                                self._dispatch(payload)
                            elif ftype == _proto.RESP_PONG:
                                return True
                            elif ftype == _proto.RESP_ERROR:
                                # A channel failed to re-register (authz
                                # change, bad payload): swallowing this
                                # left the subscriber silently deaf on a
                                # channel it still believes in. Fail the
                                # attempt loudly and retry.
                                text = payload.decode("utf-8", "replace")
                                print(
                                    f"docsql.Subscriber: resubscribe rejected: {text}",
                                    file=sys.stderr,
                                )
                                return False
                            # RESP_AFFECTED subscribe confirmations pass silently.
                    finally:
                        self._control_busy = False
            except Exception:
                continue
        print(
            "docsql.Subscriber: reconnect budget exhausted; reader thread exiting "
            "(call close() and construct a new Subscriber to retry)",
            file=sys.stderr,
        )
        return False


def _from_str(value):
    if value is None:
        return "latest"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    return str(value)
