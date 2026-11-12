package io.docsql;

public final class Wire {

    private Wire() {}

    static final int MAGIC = 0x31515344;
    static final int HEADER_LEN = 20;
    static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;

    static final int FLAG_REPLICATION = 0x0002;
    static final int FLAG_ENCRYPTED = 0x0004;

    static final int REQ_SQL = 0x0001;
    static final int REQ_AUTH = 0x0002;
    static final int REQ_PREPARE = 0x0003;
    static final int REQ_EXECUTE = 0x0004;
    static final int REQ_CLOSE_STMT = 0x0005;
    static final int REQ_PING = 0x0006;
    static final int REQ_STATUS = 0x0008;
    static final int REQ_SUBSCRIBE = 0x0009;
    static final int REQ_PSUBSCRIBE = 0x000A;
    static final int REQ_UNSUBSCRIBE = 0x000B;
    static final int REQ_PUNSUBSCRIBE = 0x000C;
    static final int REQ_PUBLISH = 0x000D;
    static final int REQ_AUTH_USER = 0x0018;

    static final int RESP_ROWS = 0x0101;
    static final int RESP_AFFECTED = 0x0102;
    static final int RESP_ERROR = 0x0103;
    static final int RESP_PONG = 0x0105;
    static final int RESP_STATUS = 0x0106;
    static final int RESP_PUSH = 0x0107;
    static final int RESP_PREPARED = 0x010E;
    static final int RESP_HELLO = 0x010F;

    private static final byte[] EMPTY = {};

    static byte[] encodeFrame(int frameType, int flags, byte[] payload) {
        if (payload == null) {
            payload = EMPTY;
        }
        if (payload.length > MAX_FRAME_BYTES) {
            throw new DocsqlException.Protocol(
                    "frame payload " + payload.length + " bytes exceeds the "
                            + MAX_FRAME_BYTES + "-byte wire cap");
        }
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(HEADER_LEN + payload.length);
        buf.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        buf.putInt(MAGIC);
        buf.putShort((short) flags);
        buf.putShort((short) frameType);
        buf.putLong(0L);
        buf.putInt(payload.length);
        buf.put(payload);
        return buf.array();
    }

    /**
     * Decode a 20-byte header; returns the payload length, validating magic and the transport cap
     * BEFORE the payload is buffered. The connection is unusable after a decode failure.
     */
    static int decodePayloadLen(byte[] header) {
        if (header.length < HEADER_LEN) {
            throw new DocsqlException.Protocol(
                    "truncated header: " + header.length + " < " + HEADER_LEN + " bytes");
        }
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(header);
        buf.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        int magic = buf.getInt();
        if (magic != MAGIC) {
            throw new DocsqlException.Protocol(String.format("bad frame magic 0x%08x", magic));
        }
        buf.position(16);
        int len = buf.getInt();
        if (len < 0 || len > MAX_FRAME_BYTES) {
            throw new DocsqlException.Protocol(
                    "server advertised a " + len + "-byte frame (cap " + MAX_FRAME_BYTES + ")");
        }
        return len;
    }

    /** SQL payload: tag 0x04 + u32 LE length + UTF-8 bytes (engine Value::Str encoding). */
    static byte[] sqlPayload(String sql) {
        byte[] body = sql.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (body.length > MAX_FRAME_BYTES) {
            throw new DocsqlException.Protocol("SQL text too large for one frame: " + body.length);
        }
        byte[] out = new byte[5 + body.length];
        out[0] = 0x04;
        out[1] = (byte) (body.length & 0xFF);
        out[2] = (byte) ((body.length >> 8) & 0xFF);
        out[3] = (byte) ((body.length >> 16) & 0xFF);
        out[4] = (byte) ((body.length >> 24) & 0xFF);
        System.arraycopy(body, 0, out, 5, body.length);
        return out;
    }

    /** RESP_AFFECTED payload: u64 LE, two's-complement long, 0 when absent. */
    static long decodeAffected(byte[] payload) {
        if (payload == null || payload.length < 8) {
            return 0L;
        }
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(payload);
        buf.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        return buf.getLong();
    }
}