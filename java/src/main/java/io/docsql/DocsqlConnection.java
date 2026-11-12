package io.docsql;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Core native connection over the DocSQL v1 wire protocol — the JDBC layer builds on this.
 *
 * <p>Threadsafety: one connection serializes its own round trips (threadsafety 1, DB-API
 * convention). Share connections across threads only with external synchronization.
 *
 * <p>Transactions: with {@code autocommit=false} the driver sends BEGIN before the first
 * statement of each block and issues COMMIT/ROLLBACK on {@link #commit()}/{@link #rollback()};
 * do not write your own BEGIN in that mode. Because DocSQL is a single-writer engine, an idle
 * connection with an open transaction parks the global write path until it commits, rolls back or
 * disconnects.
 */
public class DocsqlConnection implements AutoCloseable {

    static final int PREPARED_CAPACITY = 96;

    Transport transport;
    public final ConnectConfig config;
    private final Map<String, Long> prepared = new HashMap<>();
    private boolean txOpen;
    private boolean txLost;
    private boolean closed;
    private volatile boolean autocommit;

    public DocsqlConnection(ConnectConfig config) {
        this.config = config;
        this.autocommit = config.autocommit;
        this.transport = new Transport(
                config.host, config.port, config.tls, config.tlsCaPath, config.tlsHostname,
                config.key, config.timeoutMs);
        try {
            authenticate();
            transport.roundTrip(Wire.REQ_PING, new byte[0]);
        } catch (RuntimeException e) {
            transport.close();
            throw e;
        }
    }

    private void authenticate() {
        if (config.user != null && !config.user.isEmpty()) {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("user", config.user);
            body.put("password", config.password == null ? "" : config.password);
            Transport.Frame f = transport.roundTrip(
                    Wire.REQ_AUTH_USER, Json.write(body).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            checkAuthReply(f);
        } else if (config.token != null && !config.token.isEmpty()) {
            Transport.Frame f = transport.roundTrip(
                    Wire.REQ_AUTH, config.token.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            checkAuthReply(f);
        }
    }

    private static void checkAuthReply(Transport.Frame f) {
        if (f.type == Wire.RESP_ERROR) {
            throw new DocsqlException.Operational(text(f));
        }
    }

    // -- statement execution ------------------------------------------------

    /** Execute one statement (autocommit per the connection setting). */
    public Result execute(String sql) {
        return execute(sql, (Object[]) null);
    }

    /** Execute one statement with server-side bound parameters (qmark templates). */
    public Result execute(String sql, Object... params) {
        ensureOpen();
        Object[] p = params == null ? new Object[0] : params;
        Long handle = null;
        boolean bound = p.length > 0;
        if (bound) {
            handle = prepareHandle(sql);
        }
        beginTxIfNeeded();
        Transport.Frame f;
        if (bound) {
            StringBuilder sb = new StringBuilder("{\"handle\":").append(handle)
                    .append(",\"params\":[");
            for (int i = 0; i < p.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(Values.paramJson(p[i]));
            }
            sb.append("]}");
            f = transport.roundTrip(Wire.REQ_EXECUTE, utf8(sb.toString()));
        } else {
            f = transport.roundTrip(Wire.REQ_SQL, Wire.sqlPayload(sql));
        }
        return toResult(f);
    }

    /** Server-side prepared handle cache; closes all handles when the cap overflows. */
    private long prepareHandle(String template) {
        Long cached = prepared.get(template);
        if (cached != null) {
            return cached;
        }
        if (prepared.size() >= PREPARED_CAPACITY) {
            for (Long h : prepared.values()) {
                transport.roundTrip(Wire.REQ_CLOSE_STMT, utf8("{\"handle\":" + h + "}"));
            }
            prepared.clear();
        }
        Transport.Frame f = transport.roundTrip(Wire.REQ_PREPARE, Wire.sqlPayload(template));
        if (f.type == Wire.RESP_ERROR) {
            throw mapServerError(text(f));
        }
        Object parsed = Json.parse(f.payload);
        if (!(parsed instanceof Map)) {
            throw new DocsqlException.Interface("malformed prepare reply");
        }
        Object h = ((Map<?, ?>) parsed).get("handle");
        if (!(h instanceof Long)) {
            throw new DocsqlException.Interface("malformed prepare reply: handle missing");
        }
        long handle = (Long) h;
        prepared.put(template, handle);
        return handle;
    }

    private void beginTxIfNeeded() {
        if (!autocommit && !txOpen && !closed) {
            Transport.Frame f = transport.roundTrip(Wire.REQ_SQL, Wire.sqlPayload("BEGIN"));
            checkReply(f);
            txOpen = true;
            txLost = false;
        }
    }

    public void commit() {
        if (autocommit) {
            return;
        }
        if (txLost) {
            txLost = false;
            throw new DocsqlException.Operational(
                    "transaction lost: the connection dropped and the server rolled "
                            + "the open transaction back; retry the work on the new connection");
        }
        if (txOpen) {
            endTx("COMMIT");
        }
    }

    public void rollback() {
        if (autocommit) {
            return;
        }
        txLost = false;
        if (txOpen) {
            endTx("ROLLBACK");
        }
    }

    private void endTx(String verb) {
        Transport.Frame f = transport.roundTrip(Wire.REQ_SQL, Wire.sqlPayload(verb));
        checkReply(f);
        txOpen = false;
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    /** Toggle auto-commit; switching ON commits the open block (JDBC semantics). */
    public void setAutocommit(boolean value) {
        if (autocommit == value) {
            return;
        }
        boolean wasTxOpen = txOpen;
        autocommit = value;
        if (value && wasTxOpen) {
            endTx("COMMIT");
        }
    }

    /** Keepalive probe. */
    public void ping() {
        ensureOpen();
        transport.roundTrip(Wire.REQ_PING, new byte[0]);
    }

    /**
     * Keepalive probe with reconnect: retries once on a lost connection with the original
     * parameters. Prepared-statement handles (per-connection server state) die with the old
     * socket; an open transaction is reported lost — the server rolled it back on disconnect,
     * and a silent no-op commit() would hide that.
     */
    public void ping(boolean reconnect) {
        if (!reconnect) {
            ping();
            return;
        }
        try {
            ping();
            return;
        } catch (RuntimeException e) {
            boolean lost = txOpen || txLost;
            try {
                transport.close();
            } catch (RuntimeException ignore) {
                // the old wire is dead either way
            }
            this.transport = new Transport(
                    config.host, config.port, config.tls, config.tlsCaPath, config.tlsHostname,
                    config.key, config.timeoutMs);
            this.prepared.clear();
            this.txOpen = false;
            this.txLost = lost;
            try {
                authenticate();
                transport.roundTrip(Wire.REQ_PING, new byte[0]);
            } catch (RuntimeException e2) {
                try {
                    transport.close();
                } catch (RuntimeException ignore) {
                    // best effort
                }
                throw e2;
            }
        }
    }

    /** Node status report (REQ_STATUS) as a map — health checks and dashboards. */
    @SuppressWarnings("unchecked")
    public java.util.Map<String, Object> status() {
        ensureOpen();
        Transport.Frame f = transport.roundTrip(Wire.REQ_STATUS, new byte[0]);
        checkReply(f);
        Object parsed = Json.parse(f.payload);
        if (!(parsed instanceof Map)) {
            throw new DocsqlException.Interface("malformed status payload");
        }
        return (java.util.Map<String, Object>) parsed;
    }

    /** Persistent pub/sub publish; returns {id, receivers}. */
    public long[] publish(String channel, String payload) {
        ensureOpen();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("channel", channel);
        body.put("payload", payload);
        Transport.Frame f = transport.roundTrip(Wire.REQ_PUBLISH, utf8(Json.write(body)));
        Result r = toResult(f);
        if (r.rows.isEmpty()) {
            throw new DocsqlException.Interface("PUBLISH reply carried no row");
        }
        Object id = r.rows.get(0)[0];
        Object receivers = r.rows.get(0)[1];
        return new long[] {asLong(id), asLong(receivers)};
    }

    private static long asLong(Object v) {
        if (v instanceof Long) {
            return (Long) v;
        }
        if (v instanceof Double) {
            return ((Double) v).longValue();
        }
        if (v instanceof java.math.BigDecimal) {
            return ((java.math.BigDecimal) v).longValue();
        }
        if (v instanceof BigInteger) {
            return ((BigInteger) v).longValue();
        }
        if (v instanceof String) {
            return Long.parseLong((String) v);
        }
        return -1;
    }

    // -- reply plumbing -------------------------------------------------------

    Result toResult(Transport.Frame f) {
        if (f.type == Wire.RESP_ROWS) {
            Result r = Values.decodeRows(f.payload);
            return r;
        }
        if (f.type == Wire.RESP_AFFECTED) {
            Result r = new Result();
            r.rowcount = Wire.decodeAffected(f.payload);
            return r;
        }
        if (f.type == Wire.RESP_ERROR) {
            throw mapServerError(text(f));
        }
        throw new DocsqlException.Interface(
                "unexpected reply frame 0x" + Integer.toHexString(f.type)
                        + " for statement — connection is desynchronized");
    }

    private static void checkReply(Transport.Frame f) {
        if (f.type == Wire.RESP_ERROR) {
            throw mapServerError(text(f));
        }
    }

    DocsqlConnection ensureOpen() {
        if (closed) {
            throw new DocsqlException.Programming("connection is closed");
        }
        return this;
    }

    static byte[] utf8(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    static String text(Transport.Frame f) {
        return new String(f.payload, java.nio.charset.StandardCharsets.UTF_8);
    }

    /** Map a server error TEXT to the DB-API-style exception hierarchy. */
    static DocsqlException mapServerError(String msg) {
        String lowered = msg == null ? "" : msg.toLowerCase(java.util.Locale.ROOT);
        if (lowered.contains("unauthorized") || lowered.contains("bad token")
                || lowered.contains("bad username")) {
            return new DocsqlException.Operational(msg);
        }
        if (lowered.contains("unique") || lowered.contains("foreign key")
                || lowered.contains("check constraint") || lowered.contains("not null")) {
            return new DocsqlException.Integrity(msg);
        }
        if (lowered.contains("too large") || lowered.contains("exceeds")
                || lowered.contains("out of range") || lowered.contains("invalid date")) {
            return new DocsqlException.Data(msg);
        }
        if (lowered.contains("parse") || lowered.contains("syntax") || lowered.contains("expected")
                || lowered.contains("unknown ") || lowered.contains("unrecognized")) {
            return new DocsqlException.Programming(msg);
        }
        return new DocsqlException.Database(msg);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            for (Long h : prepared.values()) {
                transport.roundTrip(Wire.REQ_CLOSE_STMT, utf8("{\"handle\":" + h + "}"));
            }
        } catch (RuntimeException ignore) {
            // best effort: the server frees handles on disconnect anyway
        }
        prepared.clear();
        transport.close();
    }

    public boolean isClosed() {
        return closed;
    }
}
