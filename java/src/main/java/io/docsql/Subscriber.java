package io.docsql;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Persistent pub/sub subscriber: one dedicated connection + one reader thread (subscription
 * connections are one-way streaming — a plain request/response connection would misread
 * RESP_PUSH as the next reply).
 *
 * <p>Delivery is at-least-once with per-node monotonic ids. {@code subscribe(ch, fromId)}
 * resumes strictly AFTER that id (server semantics: id &gt; after), so the subscriber tracks the
 * newest delivered id per channel and reconnects replay exactly the missed tail — no loss, no
 * duplicate. Pattern subscriptions resume from latest (a glob has no single resume cursor).
 *
 * <p>Callbacks run on the reader thread: keep them fast or dispatch to your own executor. An
 * exception escaping a callback is logged, never fatal.
 */
public final class Subscriber implements AutoCloseable {

    /** Delivered on the reader thread; keep fast. */
    public interface Listener {
        void onMessage(Map<String, Object> message);
    }

    private static final int POLL_SECONDS = 5;
    private static final int KEEPALIVE_POLLS = 3;
    private static final long CONTROL_REPLY_TIMEOUT_MS = 10_000;

    private final Listener onMessage;
    private final Runnable onDisconnect;
    private final boolean auto;
    private final int attempts;
    private final ConnectConfig config;
    private final Map<String, String> channels = new LinkedHashMap<>();
    private final Map<String, String> patterns = new LinkedHashMap<>();
    private final Map<String, Long> lastId = new HashMap<>();
    private final LinkedBlockingQueue<Transport.Frame> replies = new LinkedBlockingQueue<>();
    private volatile boolean repliesStale;
    private final ReentrantLock writeLock = new ReentrantLock();
    private volatile boolean controlBusy;
    private volatile boolean closedFlag;
    private volatile int generation;
    private volatile DocsqlConnection conn;

    public Subscriber(ConnectConfig config, Listener onMessage) {
        this(config, onMessage, null, true, 10);
    }

    public Subscriber(ConnectConfig config, Listener onMessage, Runnable onDisconnect,
            boolean autoResubscribe, int reconnectAttempts) {
        this.onMessage = onMessage;
        this.onDisconnect = onDisconnect;
        this.auto = autoResubscribe;
        this.attempts = reconnectAttempts;
        this.config = config;
        this.conn = new DocsqlConnection(config);
        Thread reader = new Thread(() -> run(generation), "docsql-subscriber");
        reader.setDaemon(true);
        reader.start();
    }

    /** Subscribe one channel; replays start strictly AFTER the from id (resume cursor). */
    public Subscriber subscribe(String channel, String from) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("channel", channel);
        body.put("from", from);
        control(Wire.REQ_SUBSCRIBE, body);
        channels.put(channel, from);
        return this;
    }

    public Subscriber subscribe(String channel) {
        return subscribe(channel, "latest");
    }

    public Subscriber subscribe(String channel, long fromId) {
        return subscribe(channel, String.valueOf(fromId));
    }

    /** Subscribe a Redis-style glob pattern ({@code *}, {@code ?}, {@code [...]}). */
    public Subscriber psubscribe(String pattern, String from) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("pattern", pattern);
        body.put("from", from);
        control(Wire.REQ_PSUBSCRIBE, body);
        patterns.put(pattern, from);
        return this;
    }

    public Subscriber psubscribe(String pattern) {
        return psubscribe(pattern, "latest");
    }

    /** Unsubscribe channels; empty varargs unregisters every channel of this connection. */
    public Subscriber unsubscribe(String... names) {
        java.util.List<String> list = java.util.Arrays.asList(names);
        control(Wire.REQ_UNSUBSCRIBE, list);
        if (names.length == 0) {
            channels.clear();
            lastId.clear();
        } else {
            for (String c : names) {
                channels.remove(c);
                lastId.remove(c);
            }
        }
        return this;
    }

    public Subscriber punsubscribe(String... names) {
        java.util.List<String> list = java.util.Arrays.asList(names);
        control(Wire.REQ_PUNSUBSCRIBE, list);
        if (names.length == 0) {
            patterns.clear();
        } else {
            for (String p : names) {
                patterns.remove(p);
            }
        }
        return this;
    }

    @Override
    public void close() {
        closedFlag = true;
        DocsqlConnection c = conn;
        if (c != null) {
            try {
                c.close();
            } catch (RuntimeException ignore) {
                // best effort
            }
        }
    }

    // -- internals ---------------------------------------------------------

    private void control(int frameType, Object body) {
        ensureLiveConnection();
        byte[] payload = DocsqlConnection.utf8(Json.write(body));
        java.util.List<byte[]> pushes = new java.util.ArrayList<>();
        // Snapshot the connection this REQUEST went out on: the reader loop may replace
        // self.conn mid-wait (reconnect finished inside the 10s window) — poisoning the NEW
        // connection below would both kill a healthy link and mark its replies stale.
        DocsqlConnection connAtSend = conn;
        boolean controlBusyPrev = false;
        try {
            writeLock.lock();
            controlBusy = true;
            try {
                connAtSend.transport.writeFrame(frameType, 0, payload);
                while (true) {
                    Transport.Frame f;
                    try {
                        f = replies.poll(CONTROL_REPLY_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new DocsqlException.Operational("subscriber control call interrupted");
                    }
                    if (f == null) {
                        // The reply may still land later — and from that moment the queue can
                        // no longer be matched to requests (a late ACK would be consumed as the
                        // NEXT operation's reply: a one-off control desync). Mark stale BEFORE
                        // draining; frames the reader is about to put were read off this dying
                        // wire and belong to a dead request.
                        repliesStale = true;
                        replies.clear();
                        connAtSend.transport.poison();
                        throw new DocsqlException.Operational(
                                "subscriber control reply timed out (no frame within 10s); "
                                        + "connection poisoned — the reader loop reconnects and "
                                        + "resubscribes recorded channels, retry this call");
                    }
                    if (f.type == Wire.RESP_PUSH) {
                        // Buffer, never dispatch here: a callback calling
                        // subscribe()/unsubscribe() re-enters control() on this same thread
                        // under the held write lock — permanent deadlock. Dispatch after unlock.
                        pushes.add(f.payload);
                        continue;
                    }
                    if (f.type == Wire.RESP_PONG) {
                        continue; // keepalive echo racing our request
                    }
                    if (f.type == Wire.RESP_ERROR) {
                        // A server-side rejection is a statement error, not a transport
                        // failure — InterfaceError would wrongly declare the link dead.
                        throw DocsqlConnection.mapServerError(DocsqlConnection.text(f));
                    }
                    break;
                }
            } finally {
                controlBusy = false;
            }
        } finally {
            writeLock.unlock();
            for (byte[] p : pushes) {
                dispatch(p);
            }
        }
    }

    private void dispatch(byte[] payload) {
        Object parsed = Json.parse(payload);
        if (!(parsed instanceof Map)) {
            return;
        }
        Map<String, Object> msg = castMessage(parsed);
        try {
            onMessage.onMessage(msg);
            updateWatermark(msg);
        } catch (RuntimeException e) {
            // Delivery FAILED: keep the old watermark so the next resubscribe replays this
            // message instead of resuming past it — the cursor must not advance before the
            // callback ran.
            System.err.println("io.docsql.Subscriber: onMessage callback raised " + e);
        }
    }

    private void updateWatermark(Map<String, Object> msg) {
        Object channel = msg.get("channel");
        Object mid = msg.get("id");
        // Pattern deliveries (pmessage) share this path but are never resumed by channel
        // anchor — tracking them grew lastId without bound on high-cardinality patterns.
        if (mid instanceof Long && channel != null && channels.containsKey(channel)) {
            long id = (Long) mid;
            Long prev = lastId.get(channel);
            if (prev == null || id > prev) {
                lastId.put((String) channel, id);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMessage(Object parsed) {
        return (Map<String, Object>) parsed;
    }

    private void ensureLiveConnection() {
        if (closedFlag) {
            throw new DocsqlException.Programming("subscriber is closed");
        }
        DocsqlConnection c = conn;
        if (c != null && c.transport.isClosed()) {
            // A dead reader with no recorded subscriptions exits (nothing to resubscribe);
            // every later subscribe() would fail forever on the closed wire. Rebuild first.
            writeLock.lock();
            try {
                c = conn;
                if (c.transport.isClosed() && !closedFlag) {
                    try {
                        c.close();
                    } catch (RuntimeException ignore) {
                        // best effort
                    }
                    replies.clear();
                    repliesStale = false;
                    conn = new DocsqlConnection(config);
                    generation++;
                    Thread reader = new Thread(() -> run(generation), "docsql-subscriber");
                    reader.setDaemon(true);
                    reader.start();
                }
            } finally {
                writeLock.unlock();
            }
        }
    }

    private void run(int gen) {
        while (!closedFlag && gen == generation) {
            try {
                readFrames(gen);
            } catch (RuntimeException e) {
                if (closedFlag || gen != generation) {
                    return;
                }
                if (onDisconnect != null) {
                    try {
                        onDisconnect.run();
                    } catch (RuntimeException ignore) {
                        // never fatal
                    }
                }
                if (!auto || (channels.isEmpty() && patterns.isEmpty())) {
                    return;
                }
                if (!resubscribe(gen)) {
                    return;
                }
            }
        }
    }

    private void readFrames(int gen) {
        int idlePolls = 0;
        while (!closedFlag && gen == generation) {
            Transport.Frame f = conn.transport.readFramePoll(POLL_SECONDS * 1000);
            if (f == null) {
                idlePolls++;
                if (idlePolls >= KEEPALIVE_POLLS && !controlBusy) {
                    idlePolls = 0;
                    // Idle keepalive: proves the connection (and beats DOCSQL_IDLE_TIMEOUT).
                    // The lock is taken NON-BLOCKING: a plain acquire could park this reader
                    // for a control call's full 10s reply budget — and this reader is the very
                    // thread that must queue that reply (check-then-block was a TOCTOU).
                    if (writeLock.tryLock()) {
                        try {
                            if (!conn.transport.isClosed()) {
                                conn.transport.writeFrame(Wire.REQ_PING, 0, new byte[0]);
                            }
                        } finally {
                            writeLock.unlock();
                        }
                    }
                }
                continue;
            }
            idlePolls = 0;
            if (f.type == Wire.RESP_PUSH) {
                dispatch(f.payload);
            } else if (f.type == Wire.RESP_PONG) {
                // keepalive echo (the resubscribe drain has its own PONG)
            } else if (repliesStale) {
                // A control call timed out and poisoned this wire: any other frame read off it
                // is a late arrival for a dead request. Queueing it would let the NEXT control
                // call (already on the reconnected transport) mistake it for its own ACK.
                continue;
            } else {
                replies.add(f);
            }
            // keep going
        }
    }

    /**
     * Reconnect and replay the missed tail; false when the node stays unreachable for the
     * attempt budget.
     */
    private boolean resubscribe(int gen) {
        long delayMs = 1000;
        for (int attempt = 0; attempt < attempts; attempt++) {
            if (closedFlag || gen != generation) {
                return false;
            }
            sleep(delayMs);
            if (closedFlag || gen != generation) {
                return false;
            }
            delayMs = Math.min(delayMs * 2, 8000);
            DocsqlConnection newConn;
            try {
                newConn = new DocsqlConnection(config);
            } catch (RuntimeException e) {
                continue;
            }
            // Compare-and-install under the write lock: control()'s rebuild may have replaced
            // the wire while this thread slept — an unconditional install overwrote the NEWER
            // connection (leaking its socket) and left two readers on one transport.
            writeLock.lock();
            try {
                if (closedFlag || gen != generation) {
                    try {
                        newConn.close();
                    } catch (RuntimeException ignore) {
                        // best effort
                    }
                    return false;
                }
                conn = newConn;
            } finally {
                writeLock.unlock();
            }
            if (closedFlag || gen != generation) {
                try {
                    conn.close();
                } catch (RuntimeException ignore) {
                    // best effort
                }
                return false;
            }
            replies.clear();
            repliesStale = false;
            boolean confirmed = doResubscribeWrites();
            if (!confirmed) {
                continue;
            }
            if (closedFlag || gen != generation) {
                try {
                    conn.close();
                } catch (RuntimeException ignore) {
                    // best effort
                }
                return false;
            }
        }
        System.err.println("io.docsql.Subscriber: reconnect budget exhausted; reader thread "
                + "exiting (close() and construct a new Subscriber to retry)");
        return false;
    }

    /** Write the re-subscribe batch on the fresh wire and drain until the PONG confirms. */
    private boolean doResubscribeWrites() {
        java.util.List<byte[]> pushes = new java.util.ArrayList<>();
        boolean confirmed = false;
        boolean busyWasSet = false;
        try {
            writeLock.lock();
            controlBusy = true;
            busyWasSet = true;
            try {
                // Resubscribes resume strictly after the newest id seen on the channel. A
                // channel with no delivered anchor falls back to its requested from — messages
                // published inside this reconnect window are only recoverable with an explicit
                // numeric from.
                for (Map.Entry<String, String> e : channels.entrySet()) {
                    String resume = lastId.containsKey(e.getKey())
                            ? String.valueOf(lastId.get(e.getKey()))
                            : e.getValue();
                    Map<String, Object> body = new LinkedHashMap<>();
                    body.put("channel", e.getKey());
                    body.put("from", resume);
                    conn.transport.writeFrame(
                            Wire.REQ_SUBSCRIBE, 0, DocsqlConnection.utf8(Json.write(body)));
                }
                for (Map.Entry<String, String> e : patterns.entrySet()) {
                    Map<String, Object> body = new LinkedHashMap<>();
                    body.put("pattern", e.getKey());
                    body.put("from", e.getValue());
                    conn.transport.writeFrame(
                            Wire.REQ_PSUBSCRIBE, 0, DocsqlConnection.utf8(Json.write(body)));
                }
                // A PING marks the end of the confirmation batch: drain confirms (and any
                // replay pushes) until the PONG lands, then hand the socket back to the
                // reader loop.
                conn.transport.writeFrame(Wire.REQ_PING, 0, new byte[0]);
                while (true) {
                    Transport.Frame f = conn.transport.readFrame();
                    if (f.type == Wire.RESP_PUSH) {
                        // Buffered, dispatched by the caller with the lock released — user
                        // callbacks must never run under writeLock (see control()).
                        pushes.add(f.payload);
                    } else if (f.type == Wire.RESP_PONG) {
                        confirmed = true;
                        break;
                    } else if (f.type == Wire.RESP_ERROR) {
                        // A channel failed to re-register (authz change, bad payload):
                        // report and keep the re-subscribe loop going — a terminal exit killed
                        // the read thread for ALL channels on one transient rejection.
                        System.err.println("io.docsql.Subscriber: resubscribe rejected: "
                                + DocsqlConnection.text(f));
                        continue;
                    }
                    // RESP_AFFECTED subscribe confirmations pass silently.
                }
            } finally {
                controlBusy = false;
            }
        } catch (RuntimeException e) {
            return false;
        } finally {
            if (busyWasSet) {
                controlBusy = false;
            }
            writeLock.unlock();
            for (byte[] p : pushes) {
                dispatch(p);
            }
        }
        return confirmed;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
