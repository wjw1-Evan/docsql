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
        # True once a control round trip timed out and poisoned the wire:
        # frames the reader pulls off it afterwards are late replies for a
        # dead request and must never reach the queue (the next control
        # call would mistake one for its own ACK). Cleared — after a
        # race-free drain — when the reader installs the next connection.
        self._replies_stale = False
        self._write_lock = threading.Lock()
        # True while a control round trip (subscribe/unsubscribe) is between
        # its request and its reply: the reader must not inject a keepalive
        # PING then, or its PONG could be mistaken for the control reply.
        self._control_busy = False
        self._closed = threading.Event()
        self._conn = None
        # Reader-thread generation: bumped every time _control rebuilds the
        # wire and spawns a fresh reader. A reader whose generation is stale
        # (the old one still parked in a reconnect backoff when the user
        # thread rebuilt) must exit instead of racing the new reader — two
        # readers on one transport interleave frame bytes, and the loser's
        # uninstalled connection leaks against the server's budget.
        self._generation = 0
        self._reader = threading.Thread(
            target=self._run, args=(self._generation,), name="docsql-subscriber", daemon=True
        )
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
        if not channels:
            # Server semantics: an empty name list unregisters EVERY channel
            # of this connection (pubsub.rs `unregister`). The local sets
            # used to keep everything, so the next reconnect resurrected
            # every channel the caller had asked to drop.
            self._channels.clear()
            self._last_id.clear()
        else:
            for c in channels:
                self._channels.pop(c, None)
                self._last_id.pop(c, None)
        return self

    def punsubscribe(self, *patterns):
        self._control(_proto.REQ_PUNSUBSCRIBE, list(patterns))
        if not patterns:
            # Same all-or-named semantics as unsubscribe().
            self._patterns.clear()
        else:
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
            # The reader may have been mid-reconnect while we closed the OLD
            # connection: after the join it can have installed a NEW one it
            # never got to use. Close that too (idempotent) — otherwise the
            # socket lingers against the server's connection budget.
            leaked = self._conn
            if leaked is not None and leaked is not conn:
                try:
                    leaked.close()
                except Exception:
                    pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    # -- internals ------------------------------------------------------------
    def _control(self, frame_type, body):
        # A dead reader leaves a poisoned transport behind: after a
        # disconnect with NO recorded subscriptions the loop exits (nothing
        # to resubscribe), and every later subscribe() failed forever on the
        # closed wire. Rebuild the connection and restart the reader first
        # — auto-resubscribe semantics recover the object.
        if (
            not self._closed.is_set()
            and self._conn is not None
            and self._conn._transport._closed
        ):
            with self._write_lock:
                if self._conn._transport._closed and not self._closed.is_set():
                    try:
                        self._conn.close()
                    except Exception:
                        pass
                    # Mirror _resubscribe's teardown for the fresh link: a
                    # stale _replies_stale (set by a previous timeout) made
                    # the new reader drop this attempt's ACK as a "late
                    # reply" — every retry then timed out again and the
                    # Subscriber stayed permanently unsubscribable.
                    while True:
                        try:
                            self._replies.get_nowait()
                        except queue.Empty:
                            break
                    self._replies_stale = False
                    self._conn = connect(**self._conn_kwargs)
                    self._generation += 1
                    self._reader = threading.Thread(
                        target=self._run,
                        args=(self._generation,),
                        name="docsql-subscriber",
                        daemon=True,
                    )
                    self._reader.start()
        payload = json.dumps(body).encode("utf-8")
        pushes = []
        # Snapshot the connection this REQUEST went out on: the reader loop
        # may replace `self._conn` mid-wait (reconnect finished inside the
        # 10s window) — poisoning the NEW connection below both killed a
        # healthy link and marked its replies stale for a full extra
        # backoff round.
        conn_at_send = self._conn
        try:
            with self._write_lock:
                self._control_busy = True
                try:
                    conn_at_send._transport.write_frame(frame_type, payload)
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
                            # Mark the queue stale BEFORE draining: frames the
                            # reader is about to put were read off this dying
                            # wire and belong to a dead request (the reader
                            # drops them while stale; the reconnect-side drain
                            # closes the check-vs-put race).
                            self._replies_stale = True
                            while True:
                                try:
                                    self._replies.get_nowait()
                                except queue.Empty:
                                    break
                            try:
                                conn_at_send._transport._poison()
                            except Exception:
                                pass
                            raise OperationalError(
                                "subscriber control reply timed out (no frame within 10s); "
                                "connection poisoned — the reader loop reconnects and "
                                "resubscribes recorded channels, retry this call"
                            ) from e
                        if ftype == _proto.RESP_PUSH:
                            # Buffer, never dispatch here: _dispatch runs the
                            # USER callback, and a callback calling
                            # subscribe()/unsubscribe() re-enters _control on
                            # this same thread — under the held write lock
                            # that is a permanent deadlock (non-reentrant).
                            pushes.append(rpayload)
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
        finally:
            # Deliver buffered pushes with the lock released (see above); they
            # were received before the control reply either way.
            for p in pushes:
                self._dispatch(p)

    def _dispatch(self, payload):
        try:
            msg = json.loads(payload.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return
        try:
            self._on_message(msg)
        except Exception:
            # Delivery FAILED: keep the old watermark so the next resubscribe
            # replays this message instead of resuming past it — the cursor
            # used to advance before the callback ran, so a message whose
            # consumer threw was never redelivered after a reconnect.
            print("docsql.Subscriber: on_message callback raised", file=sys.stderr)
            return
        channel = msg.get("channel")
        mid = msg.get("id")
        if (
            isinstance(mid, int)
            and not isinstance(mid, bool)
            # Pattern deliveries (pmessage) share this path but are never
            # resumed by channel anchor — tracking them grew `_last_id`
            # without bound on high-cardinality patterns while the entries
            # stayed dead weight.
            and channel in self._channels
        ):
            prev = self._last_id.get(channel)
            if prev is None or mid > prev:
                self._last_id[channel] = mid

    def _run(self, gen):
        """Reader-thread main loop: read until the connection breaks, then
        (optionally) reconnect + resubscribe and keep going. `gen` is this
        thread's generation: a stale reader (the user thread rebuilt the
        wire via _control while this one was in a backoff sleep) steps out
        instead of competing with its successor."""
        while not self._closed.is_set() and gen == self._generation:
            try:
                self._read_frames(gen)
            except Exception:
                if self._closed.is_set() or gen != self._generation:
                    return
                if self._on_disconnect is not None:
                    try:
                        self._on_disconnect()
                    except Exception:
                        pass
                if not self._auto or not (self._channels or self._patterns):
                    return
                if not self._resubscribe(gen):
                    return
                # Loop: keep reading on the fresh connection.

    def _read_frames(self, gen):
        idle_polls = 0
        while not self._closed.is_set() and gen == self._generation:
            got = self._conn._transport.read_frame_poll(_POLL_SECONDS)
            if got is None:
                idle_polls += 1
                if idle_polls >= _KEEPALIVE_POLLS and not self._control_busy:
                    idle_polls = 0
                    # Idle keepalive: proves the connection (and beats
                    # DOCSQL_IDLE_TIMEOUT). The PONG echo is dropped below;
                    # never sent while a control round trip may claim it.
                    # The lock is taken NON-BLOCKING: a plain acquire could
                    # park this reader for a control call's full 10s reply
                    # budget — and this reader is the very thread that must
                    # queue that reply (check-then-block was a TOCTOU:
                    # _control_busy can flip between the check and the
                    # lock). Skip the round when the wire is owned.
                    if self._write_lock.acquire(blocking=False):
                        try:
                            if not self._conn._transport._closed:
                                self._conn._transport.write_frame(_proto.REQ_PING, b"")
                        finally:
                            self._write_lock.release()
                continue
            idle_polls = 0
            _flags, ftype, payload = got
            if ftype == _proto.RESP_PUSH:
                self._dispatch(payload)
            elif ftype == _proto.RESP_PONG:
                pass  # keepalive echo (the resubscribe drain has its own PONG)
            elif self._replies_stale:
                # A control call timed out and poisoned this wire: any other
                # frame read off it is a late arrival for a dead request.
                # Queueing it would let the NEXT control call (already on
                # the reconnected transport) mistake it for its own ACK —
                # a one-off control desync. Drop it; the resume cursors
                # make lost replays recoverable, desynced ACKs are not.
                continue
            else:
                self._replies.put(got)

    def _resubscribe(self, gen):
        """Reconnect and replay the missed tail; False when the node stays
        unreachable for the attempt budget."""
        delay = 1.0
        for _ in range(self._attempts):
            if self._closed.is_set() or gen != self._generation:
                return False
            time.sleep(delay)
            # close() may have fired DURING the sleep: its join(5s) can have
            # already timed out (backoff sleeps grow to 8s), so close()'s
            # post-join sweep has run — a connection installed now would
            # never be swept and leak against the server's conn budget.
            if self._closed.is_set() or gen != self._generation:
                return False
            delay = min(delay * 2, 8.0)
            try:
                new_conn = connect(**self._conn_kwargs)
            except Exception:
                # Transport-level failures too (TLS handshake, protocol
                # garbage): catching only Error used to let them escape the
                # reader thread entirely, killing the subscriber silently.
                continue
            # Compare-and-install under the write lock: _control's rebuild
            # may have replaced the wire while this thread slept — an
            # unconditional install overwrote the NEWER connection (leaking
            # its socket) and left two readers on one transport.
            with self._write_lock:
                if self._closed.is_set() or gen != self._generation:
                    try:
                        new_conn.close()
                    except Exception:
                        pass
                    return False
                self._conn = new_conn
            if self._closed.is_set():
                # close() raced the install: nobody will ever read or close
                # this socket (close()'s own sweep has already passed).
                try:
                    self._conn.close()
                except Exception:
                    pass
                return False
            # New wire: drop anything still queued — it was read off the
            # poisoned transport after the timeout drain (that drain raced
            # this thread's puts; the reader is the only producer, so THIS
            # drain is race-free) — then stop dropping.
            while True:
                try:
                    self._replies.get_nowait()
                except queue.Empty:
                    break
            self._replies_stale = False
            pushes = []
            confirmed = False
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
                                # Buffered, dispatched below with the lock
                                # released — user callbacks must never run
                                # under _write_lock (see _control).
                                pushes.append(payload)
                            elif ftype == _proto.RESP_PONG:
                                confirmed = True
                                break
                            elif ftype == _proto.RESP_ERROR:
                                # A channel failed to re-register (authz
                                # change, bad payload): swallowing this
                                # left the subscriber silently deaf on a
                                # channel it still believes in. Report and
                                # keep the re-subscribe loop going: a
                                # terminal `return False` killed the read
                                # thread for ALL channels on one transient
                                # rejection and left later subscribe() calls
                                # waiting out their full 10s control budget
                                # on a connection nobody reads.
                                text = payload.decode("utf-8", "replace")
                                print(
                                    f"docsql.Subscriber: resubscribe rejected: {text}",
                                    file=sys.stderr,
                                )
                                continue
                            # RESP_AFFECTED subscribe confirmations pass silently.
                    finally:
                        self._control_busy = False
            except Exception:
                continue
            finally:
                # Replay pushes with the lock released (see _control).
                for p in pushes:
                    self._dispatch(p)
            if confirmed:
                if self._closed.is_set():
                    # close() raced the drain: same leak as above — the
                    # reader is about to exit and nobody else will close it.
                    try:
                        self._conn.close()
                    except Exception:
                        pass
                    return False
                return True
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
