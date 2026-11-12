package io.docsql;

import java.math.BigDecimal;
import java.math.BigInteger;

/** Unit tests that need no server. */
public class UnitTests {

    static int passed;
    static int failed;

    static void check(String name, boolean cond) {
        if (cond) {
            passed++;
        } else {
            failed++;
            System.out.println("FAIL: " + name);
        }
    }

    static void expectThrows(String name, Class<? extends Throwable> type, Runnable r) {
        try {
            r.run();
            check(name, false); // did not throw
        } catch (Throwable t) {
            check(name, type.isInstance(t));
        }
    }

    public static void runAll() {
        wireTests();
        jsonTests();
        valuesTests();
        sealerTests();
        errorTests();
        urlTests();
    }

    static void wireTests() {
        byte[] f = Wire.encodeFrame(Wire.REQ_SQL, 0, new byte[] {1, 2, 3});
        check("wire.header_len", f.length == 20 + 3);
        check("wire.magic_le", f[0] == 'D' && f[1] == 'S' && f[2] == 'Q' && f[3] == '1');
        java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(f).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        bb.getInt(); // magic
        check("wire.flags_le", (bb.getShort() & 0xFFFF) == 0);
        check("wire.type_le", (bb.getShort() & 0xFFFF) == Wire.REQ_SQL);
        check("wire.topology_zero", bb.getLong() == 0);
        check("wire.len_le", bb.getInt() == 3);
        // bad magic
        byte[] bad = Wire.encodeFrame(Wire.REQ_PING, 0, new byte[0]);
        bad[0] ^= 0xFF;
        expectThrows("wire.bad_magic", DocsqlException.Protocol.class, () -> Wire.decodePayloadLen(bad));
        // truncated
        expectThrows("wire.truncated_header", DocsqlException.Protocol.class,
                () -> Wire.decodePayloadLen(new byte[19]));
        // sqlPayload UTF-8 roundtrip with CJK + emoji
        String s = "SELECT * FROM t WHERE s = '中文🎉'";
        byte[] p = Wire.sqlPayload(s);
        check("wire.sql_payload_tag", p[0] == 0x04);
        int len = (p[1] & 0xFF) | ((p[2] & 0xFF) << 8) | ((p[3] & 0xFF) << 16) | ((p[4] & 0xFF) << 24);
        check("wire.sql_payload_len", len == s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
        check("wire.decode_affected", Wire.decodeAffected(new byte[8]) == 0);
        check("wire.decode_affected_7", Wire.decodeAffected(
                new byte[] {7, 0, 0, 0, 0, 0, 0, 0}) == 7);
        check("wire.decode_affected_absent", Wire.decodeAffected(new byte[0]) == 0);
    }

    static void jsonTests() {
        // parse/write roundtrips
        check("json.int", (Long) Json.parse("42") == 42L);
        check("json.neg", (Long) Json.parse("-7") == -7L);
        check("json.double", (Double) Json.parse("1.5") == 1.5);
        check("json.exp", (Double) Json.parse("1.0E2") == 100.0);
        check("json.true", (Boolean) Json.parse("true"));
        check("json.null", Json.parse("null") == null);
        java.util.Map<?, ?> m = (java.util.Map<?, ?>) Json.parse(
                "{\"a\":1,\"b\":[1,2,{\"c\":\"x\"}],\"s\":\"\\u4e2d\"}");
        check("json.nested_map", m.get("a").equals(1L) && m.get("s").equals("中"));
        java.util.Map<?, ?> inner = (java.util.Map<?, ?>) ((java.util.List<?>) m.get("b")).get(2);
        check("json.deep", inner.get("c").equals("x"));
        java.util.Map<String, Object> wmap = new java.util.LinkedHashMap<>();
        wmap.put("a", 1L);
        java.util.List<Object> wlist = new java.util.ArrayList<>();
        wlist.add(1L);
        wlist.add(2L);
        java.util.Map<String, Object> winner = new java.util.LinkedHashMap<>();
        winner.put("c", "x");
        wlist.add(winner);
        wmap.put("b", wlist);
        check("json.writer_map", Json.write(wmap).equals(
                "{\"a\":1,\"b\":[1,2,{\"c\":\"x\"}]}"));
        expectThrows("json.trailing_garbage",
                DocsqlException.Interface.class, () -> Json.parse("{}x"));
        // control chars escaped, UTF-8 passthrough
        String w = Json.write("a\"b\\c\n\u0001中");
        check("json.write_escapes", w.contains("\\n") && w.contains("\\u0001")
                && w.contains("中") && w.contains("\\\""));
        expectThrows("json.surrogate_rejected", DocsqlException.Data.class,
                () -> Json.write("\ud800"));
        // big integer fallback
        Object big = Json.parse("123456789012345678901234567890");
        check("json.bigint_fallback", big instanceof BigInteger);
        expectThrows("json.parse_error", DocsqlException.Interface.class, () -> Json.parse("{\"a\":}"));
        expectThrows("json.parse_error2", DocsqlException.Interface.class, () -> Json.parse("[1,"));
    }

    static void valuesTests() {
        check("v.null", Values.paramJson(null).equals("null"));
        check("v.bool", Values.paramJson(Boolean.TRUE).equals("true"));
        check("v.int", Values.paramJson(42).equals("42"));
        check("v.long", Values.paramJson(42L).equals("42"));
        check("v.double", Values.paramJson(1.5d).equals("1.5"));
        check("v.double_exp", Values.paramJson(1e100).contains("E"));
        check("v.nan", Values.paramJson(Double.NaN).equals("{\"$float\":\"NaN\"}"));
        check("v.inf", Values.paramJson(Double.POSITIVE_INFINITY).equals("{\"$float\":\"inf\"}"));
        check("v.ninf", Values.paramJson(Double.NEGATIVE_INFINITY).equals("{\"$float\":\"-inf\"}"));
        check("v.string", Values.paramJson("it's").equals("\"it's\""));
        check("v.string_quote", Values.paramJson("a\"b").equals("\"a\\\"b\""));
        check("v.bytes", Values.paramJson(new byte[] {0, 1, (byte) 255}).equals("{\"$bytes\":[0,1,255]}"));
        check("v.bignum_via_dec", Values.paramJson(BigInteger.ONE.shiftLeft(70))
                .equals("{\"$dec\":\"1180591620717411303424\"}"));
        check("v.decimal_plain", Values.paramJson(new BigDecimal("1.23")).equals("{\"$dec\":\"1.23\"}"));
        check("v.decimal_neg_scale", Values.paramJson(new BigDecimal("1E+9"))
                .equals("{\"$dec\":\"1000000000\"}"));
        expectThrows("v.bigint_over_2^96", DocsqlException.Data.class,
                () -> Values.paramJson(BigInteger.ONE.shiftLeft(96 + 1)));
        expectThrows("v.decimal_scale29", DocsqlException.Data.class,
                () -> Values.paramJson(new BigDecimal("1.2345678901234567890123456789" + "0" + "1")));
        expectThrows("v.decimal_huge_exp_rejected_fast", DocsqlException.Data.class, () -> {
            Values.paramJson(new BigDecimal("1E+999999999"));
        });
        expectThrows("v.unsupported_type", DocsqlException.Programming.class,
                () -> Values.paramJson(new Object()));
        // timestamps
        check("v.ts_marker", Values.paramJson(java.sql.Timestamp.valueOf("2026-11-13 12:00:00"))
                .startsWith("{\"$ts\":"));
        expectThrows("v.ts_domain_max_rejected", DocsqlException.Data.class, () -> {
            Values.paramJson(new java.util.Date(253_402_300_800_000L));
        });
        check("v.ts_edge_min", Values.paramJson(new java.util.Date(Values.TIMESTAMP_MIN_MS))
                .equals("{\"$ts\":-62135596800000}"));
        check("v.doc_bind", Values.paramJson(java.util.Collections.singletonMap("k", "v"))
                .equals("{\"k\":\"v\"}"));
        check("v.array_bind", Values.paramJson(new Object[] {1L, "x"})
                .equals("[1,\"x\"]"));
        // row decode markers
        Result r = Values.decodeRows(
                "{\"columns\":[\"a\"],\"rows\":[[{\"$dec\":\"1.5\"},{\"$ts\":0}]]}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        check("v.rows_cols", r.columns.size() == 1 && r.columns.get(0).equals("a"));
        check("v.dec_marker", r.rows.get(0)[0] instanceof BigDecimal
                && ((BigDecimal) r.rows.get(0)[0]).compareTo(new BigDecimal("1.5")) == 0);
        check("v.ts_marker_decode", r.rows.get(0)[1] instanceof java.sql.Timestamp);
        Result b = Values.decodeRows(
                "{\"columns\":[],\"rows\":[[{\"$bytes\":[104,105]}]]}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        check("v.bytes_marker", b.rows.get(0)[0] instanceof byte[]
                && new String((byte[]) b.rows.get(0)[0], java.nio.charset.StandardCharsets.UTF_8).equals("hi"));
        Result n = Values.decodeRows(
                "{\"columns\":[],\"rows\":[[{\"$float\":\"NaN\"}]]}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        check("v.nan_marker", ((Double) n.rows.get(0)[0]).isNaN());
        Result ood = Values.decodeRows(
                "{\"columns\":[],\"rows\":[[{\"$ts\":999999999999999999}]]}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        check("v.ood_ts_stays_map", ood.rows.get(0)[0] instanceof java.util.Map);
    }

    static void sealerTests() {
        String keyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        Sealer a = new Sealer(Sealer.parseKeyHex(keyHex));
        byte[] challenge = new byte[16];
        new java.util.Random(42).nextBytes(challenge);
        byte[] sealed = a.seal(Wire.REQ_SQL, 0, "SELECT 1".getBytes(java.nio.charset.StandardCharsets.UTF_8), challenge);
        byte[] opened = a.open(Wire.REQ_SQL, 0, sealed, challenge);
                check("seal.roundtrip", new String(opened, java.nio.charset.StandardCharsets.UTF_8).equals("SELECT 1"));
        // AAD binding: flip frame type → decrypt fails
        expectThrows("seal.aad_type_tamper", DocsqlException.class,
                () -> a.open(Wire.REQ_PING, 0, sealed, challenge));
        // AAD binding: flip flags → decrypt fails
        expectThrows("seal.aad_flags_tamper", DocsqlException.class,
                () -> a.open(Wire.REQ_SQL, 4, sealed, challenge));
        // AAD binding: flip challenge → decrypt fails
        byte[] otherChal = new byte[16];
        otherChal[0] = 1;
        expectThrows("seal.aad_challenge_tamper", DocsqlException.class,
                () -> a.open(Wire.REQ_SQL, 0, sealed, otherChal));
        // replay: re-open the SAME sealed payload → counter not increasing → rejected
        byte[] sealed2 = a.seal(Wire.REQ_SQL, 0, "SELECT 2".getBytes(java.nio.charset.StandardCharsets.UTF_8), challenge);
        check("seal.roundtrip2", new String(a.open(Wire.REQ_SQL, 0, sealed2, challenge), java.nio.charset.StandardCharsets.UTF_8).equals("SELECT 2"));
        expectThrows("seal.replay_rejected", DocsqlException.Protocol.class,
                () -> a.open(Wire.REQ_SQL, 0, sealed2, challenge));
        // reordered counter (valid seal with counter n, then open an OLDER frame) — both from same sealer
        // wrong key length
        expectThrows("seal.key_len", DocsqlException.Interface.class, () -> new Sealer(new byte[31]));
        // hex parse
        expectThrows("seal.hex_len", DocsqlException.Interface.class, () -> Sealer.parseKeyHex("ab"));
        expectThrows("seal.hex_len", DocsqlException.Interface.class, () -> Sealer.parseKeyHex("ab"));
    }

    static void errorTests() {
        check("err.integrity", DocsqlConnection.mapServerError("UNIQUE constraint failed: t.id")
                instanceof DocsqlException.Integrity);
        check("err.integrity_fk", DocsqlConnection.mapServerError("FOREIGN KEY constraint failed")
                instanceof DocsqlException.Integrity);
        check("err.data", DocsqlConnection.mapServerError("document too large")
                instanceof DocsqlException.Data);
        check("err.programming", DocsqlConnection.mapServerError("parse error: ...")
                instanceof DocsqlException.Programming);
        check("err.auth", DocsqlConnection.mapServerError("unauthorized")
                instanceof DocsqlException.Operational);
        check("err.auth2", DocsqlConnection.mapServerError("bad token")
                instanceof DocsqlException.Operational);
        check("err.database_catchall", DocsqlConnection.mapServerError("something else")
                instanceof DocsqlException.Database);
    }

    static void urlTests() {
        io.docsql.ConnectConfig c = io.docsql.jdbc.DriverUrl.parse(
                "jdbc:docsql://127.0.0.1:17601?token=tok&tls=true", null);
        check("url.parse_basic", c.host.equals("127.0.0.1") && c.port == 17601 && "tok".equals(c.token));
        check("url.tls_flag", c.tls);
        io.docsql.ConnectConfig c2 = io.docsql.jdbc.DriverUrl.parse(
                "jdbc:docsql://h1:7600?user=u&password=p%40ss", null);
        check("url.percent_decode", "p@ss".equals(c2.password));
        io.docsql.ConnectConfig c3 = io.docsql.jdbc.DriverUrl.parse("jdbc:docsql://h1?user=x", null);
        check("url.default_port", c3.port == 7600);
        check("url.not_ours", io.docsql.jdbc.DriverUrl.parse("jdbc:mysql://x", null) == null);
        check("url.no_host_rejected", io.docsql.jdbc.DriverUrl.parse("jdbc:docsql:", null) == null);
        // Properties fallback fills what the URL omits
        java.util.Properties props = new java.util.Properties();
        props.setProperty("token", "from-props");
        io.docsql.ConnectConfig c4 = io.docsql.jdbc.DriverUrl.parse("jdbc:docsql://h1", props);
        check("url.props_fallback", "from-props".equals(c4.token));
    }
}
