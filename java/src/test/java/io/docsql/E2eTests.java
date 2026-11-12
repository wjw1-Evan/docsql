package io.docsql;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/** E2E suite against a real docsql-server (mirrors the Python driver suite). */
public class E2eTests {

    static final String TOKEN = "java-e2e-token";
    static final String KEY_HEX = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    static E2eServer server;
    static E2eServer tlsServer;
    static E2eServer keyedServer;
    static E2eServer openServer;
    static String fixtures;
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
            check(name, false);
        } catch (Throwable t) {
            check(name, type.isInstance(t));
        }
    }

    static DocsqlConnection connect(int port) {
        return connect(port, true);
    }

    static DocsqlConnection connect(int port, boolean withToken) {
        ConnectConfig.Builder b = ConnectConfig.builder().host("127.0.0.1").port(port);
        if (withToken) {
            b.token(TOKEN);
        }
        return new DocsqlConnection(b.build());
    }

    static DocsqlConnection connectKeyed(int port, String keyHex) {
        return new DocsqlConnection(ConnectConfig.builder()
                .host("127.0.0.1").port(port)
                .token(TOKEN)
                .key(keyHex)
                .build());
    }

    public static void startAll() throws Exception {
        fixtures = "../python/tests/fixtures";
        Map<String, String> tlsEnv = new HashMap<String, String>();
        tlsEnv.put("DOCSQL_TLS_CERT", fixtures + "/server-cert.pem");
        tlsEnv.put("DOCSQL_TLS_KEY", fixtures + "/server-key.pem");
        tlsServer = E2eServer.start("tls", tlsEnv);
        Map<String, String> keyedEnv = new HashMap<String, String>();
        keyedEnv.put("DOCSQL_KEY", KEY_HEX);
        keyedServer = E2eServer.start("keyed", keyedEnv);
        openServer = E2eServer.start("open", new HashMap<String, String>());
        Map<String, String> mainEnv = new HashMap<String, String>();
        mainEnv.put("DOCSQL_TOKEN", TOKEN);
        server = E2eServer.start("main", mainEnv);
    }

    public static void stopAll() {
        for (E2eServer s : new E2eServer[] {server, tlsServer, keyedServer, openServer}) {
            if (s != null) {
                s.stop();
            }
        }
    }

    static void basicTests() throws Exception {
        try (DocsqlConnection c = connect(server.port)) {
            Result r = c.execute("SELECT 6 * 7");
            check("basic.select42", ((Long) r.rows.get(0)[0]) == 42L);
            c.execute("DROP TABLE IF EXISTS java_basic");
            c.execute("CREATE TABLE java_basic (id INT PRIMARY KEY, name TEXT)");
            c.execute("INSERT INTO java_basic VALUES (1, 'x')");
            r = c.execute("SELECT id, name FROM java_basic WHERE id = 1");
            check("basic.roundtrip", r.rows.get(0)[0] instanceof Long
                    && "x".equals(r.rows.get(0)[1]));
            check("basic.rowcount", r.rowcount == -1);
            Result u = c.execute("UPDATE java_basic SET name = 'y' WHERE id = 1");
            check("basic.update_count", u.rowcount == 1);
            r = c.execute("SELECT COUNT(*) FROM java_basic");
            check("basic.count", ((Long) r.rows.get(0)[0]) == 1L);
            c.execute("DROP TABLE java_basic");
            Map<String, Object> st = c.status();
            check("basic.status_version", st.containsKey("version"));
            check("basic.status_peers", st.get("peers") instanceof java.util.List);
        }
        try (DocsqlConnection c = connect(server.port)) {
            // exact scalars roundtrip through the wire markers
            Result r = c.execute("SELECT CAST('1.50' AS DECIMAL)");
            check("basic.decimal_roundtrip", r.rows.get(0)[0] instanceof BigDecimal
                    && ((BigDecimal) r.rows.get(0)[0]).compareTo(new BigDecimal("1.50")) == 0);
            r = c.execute("SELECT CAST('2026-11-13 08:00:00' AS TIMESTAMP)");
            check("basic.ts_roundtrip", r.rows.get(0)[0] instanceof java.sql.Timestamp
                    && ((java.sql.Timestamp) r.rows.get(0)[0]).getTime() > 0);
            r = c.execute("SELECT x'48656c6c6f'");
            check("basic.blob_roundtrip", r.rows.get(0)[0] instanceof byte[]
                    && "Hello".equals(new String((byte[]) r.rows.get(0)[0],
                            java.nio.charset.StandardCharsets.UTF_8)));
        }
    }

    static void txTests() throws Exception {
        try (DocsqlConnection c = connect(server.port)) {
            check("tx.autocommit_default", c.isAutocommit());
            c.execute("DROP TABLE IF EXISTS java_tx");
            c.execute("CREATE TABLE java_tx (id INT PRIMARY KEY)");
        }
        try (DocsqlConnection c = connect(server.port)) {
            Result r = c.execute("SELECT COUNT(*) FROM java_tx");
            check("tx.autocommit_visible", ((Long) r.rows.get(0)[0]) == 0L);
        }
        try (DocsqlConnection c = connect(server.port)) {
            c.setAutocommit(false);
            c.execute("INSERT INTO java_tx VALUES (1)");
            c.commit();
            c.execute("INSERT INTO java_tx VALUES (2)");
            c.rollback();
            Result r = c.execute("SELECT COUNT(*) FROM java_tx");
            check("tx.commit_rollback", ((Long) r.rows.get(0)[0]) == 1L);
            c.execute("CREATE TABLE java_tx_ddl (id INT PRIMARY KEY)");
            c.commit();
            c.execute("INSERT INTO java_tx_ddl VALUES (1)");
            c.rollback();
            Result ddlr = c.execute("SELECT COUNT(*) FROM java_tx_ddl");
            check("tx.row_rolled_back_in_ddl_table", ((Long) ddlr.rows.get(0)[0]) == 0L);
            c.execute("INSERT INTO java_tx VALUES (3)");
            c.commit();
            r = c.execute("SELECT COUNT(*) FROM java_tx");
            check("tx.new_block", ((Long) r.rows.get(0)[0]) == 2L);
        }
        try (DocsqlConnection c = connect(server.port)) {
            c.setAutocommit(false);
            c.execute("INSERT INTO java_tx VALUES (4)");
            c.setAutocommit(true);
        }
        try (DocsqlConnection c = connect(server.port)) {
            Result r = c.execute("SELECT COUNT(*) FROM java_tx");
            check("tx.setautocommit_commits", ((Long) r.rows.get(0)[0]) == 3L);
        }
    }

    static void preparedTests() throws Exception {
        try (DocsqlConnection c = connect(server.port)) {
            c.execute("DROP TABLE IF EXISTS java_bind");
            c.execute("CREATE TABLE java_bind (id INT PRIMARY KEY, name TEXT, price DECIMAL(10,2), at TIMESTAMP, blob_col BLOB)");
            c.execute("INSERT INTO java_bind VALUES (?, ?, ?, ?, ?)",
                    1L, "it's a \"quoted\" value", new BigDecimal("19.99"),
                    new java.sql.Timestamp(0), new byte[] {1, 2, (byte) 255});
            Result r = c.execute("SELECT name, price, at, blob_col FROM java_bind WHERE id = 1");
            check("bind.quote_safe", "it's a \"quoted\" value".equals(r.rows.get(0)[0]));
            check("bind.decimal", ((BigDecimal) r.rows.get(0)[1]).compareTo(new BigDecimal("19.99")) == 0);
            check("bind.ts", ((java.sql.Timestamp) r.rows.get(0)[2]).getTime() == 0L);
            byte[] got = (byte[]) r.rows.get(0)[3];
            check("bind.bytes", got.length == 3 && got[2] == (byte) 255);
            c.execute("INSERT INTO java_bind VALUES (?, ?, ?, ?, ?)", 2L, null, null, null, null);
            r = c.execute("SELECT name FROM java_bind WHERE id = 2");
            check("bind.null", r.rows.get(0)[0] == null);
            c.execute("INSERT INTO java_bind VALUES (?, ?, ?, ?, ?)", 3L, true, false, 3L, null);
            r = c.execute("SELECT name, price FROM java_bind WHERE id = 3");
            check("bind.bools", Boolean.TRUE.equals(r.rows.get(0)[0]) && Boolean.FALSE.equals(r.rows.get(0)[1]));
            c.execute("INSERT INTO java_bind VALUES (?, ?, ?, ?, ?)", 4L, Double.NaN, Double.NEGATIVE_INFINITY, 4L, null);
            r = c.execute("SELECT name, price FROM java_bind WHERE id = 4");
            check("bind.nan_inf", ((Double) r.rows.get(0)[0]).isNaN() && Double.isInfinite((Double) r.rows.get(0)[1]));
            Map<String, Object> doc = new HashMap<String, Object>();
            doc.put("k", "v");
            doc.put("n", 7L);
            c.execute("INSERT INTO java_bind VALUES (?, ?, ?, ?, ?)", 5L, doc, null, 5L, null);
            r = c.execute("SELECT name FROM java_bind WHERE id = 5");
            check("bind.document", r.rows.get(0)[0] instanceof String
                    && r.rows.get(0)[0].toString().contains("\"k\""));
        }
        try (DocsqlConnection c = connect(server.port)) {
            c.execute("INSERT INTO java_bind VALUES (?, ?, ?, ?, ?)",
                    6L, BigInteger.ONE.shiftLeft(70), null, 6L, null);
            Result r = c.execute("SELECT name FROM java_bind WHERE id = 6");
            check("bind.bignum", ((BigDecimal) r.rows.get(0)[0]).compareTo(BigDecimal.valueOf(2).pow(70)) == 0);
        }
    }

    static void tlsTests() throws Exception {
        try (DocsqlConnection c = new DocsqlConnection(ConnectConfig.builder()
                .host("127.0.0.1").port(tlsServer.port)
                .token(TOKEN)
                .tls(true)
                .tlsCaPath(fixtures + "/server-cert.pem")
                .tlsHostname("localhost")
                .build())) {
            Result r = c.execute("SELECT 'over-tls'");
            check("tls.ca_verified", "over-tls".equals(r.rows.get(0)[0]));
        }
        try (DocsqlConnection c = new DocsqlConnection(ConnectConfig.builder()
                .host("127.0.0.1").port(tlsServer.port)
                .token(TOKEN)
                .tls(true)
                .build())) {
            c.ping();
            check("tls.encrypt_only", true);
        }
        expectThrows("tls.plaintext_fails", DocsqlException.Interface.class, () -> {
            DocsqlConnection c = connect(tlsServer.port);
            c.ping();
        });
        try (DocsqlConnection c = connectKeyed(keyedServer.port, KEY_HEX)) {
            Result r = c.execute("SELECT 6 * 7");
            check("keyed.roundtrip", ((Long) r.rows.get(0)[0]) == 42L);
        }
        expectThrows("keyed.wrong_key", DocsqlException.class, () -> {
            DocsqlConnection c = connectKeyed(keyedServer.port, "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
            c.ping();
        });
        expectThrows("keyed.plaintext_fails", DocsqlException.class, () -> {
            DocsqlConnection c = connect(keyedServer.port);
            c.ping();
        });
    }

    static void pubsubTests() throws Exception {
        try (DocsqlConnection c = connect(server.port)) {
            c.execute("DROP TABLE IF EXISTS java_pub");
            c.execute("CREATE TABLE java_pub (id INT PRIMARY KEY)");
        }
        final java.util.List<Map<String, Object>> received = new java.util.ArrayList<Map<String, Object>>();
        final java.util.concurrent.CountDownLatch got3 = new java.util.concurrent.CountDownLatch(3);
        Subscriber sub = new Subscriber(ConnectConfig.builder()
                .host("127.0.0.1").port(server.port).token(TOKEN).build(),
                new Subscriber.Listener() {
                    public void onMessage(Map<String, Object> m) {
                        synchronized (received) {
                            received.add(m);
                            got3.countDown();
                        }
                    }
                });
        try {
            sub.subscribe("java-news", "earliest");
            try (DocsqlConnection c = connect(server.port)) {
                for (int i = 0; i < 3; i++) {
                    long[] ids = c.publish("java-news", "msg-" + i);
                    check("pub.id_positive", ids[0] > 0);
                }
            }
            check("pub.live_and_history", got3.await(10, java.util.concurrent.TimeUnit.SECONDS));
            synchronized (received) {
                check("pub.count", received.size() >= 3);
                for (int i = 0; i < 3; i++) {
                    check("pub.payload_" + i, ("msg-" + i).equals(received.get(i).get("payload")));
                }
            }
        } finally {
            sub.close();
        }
    }

    static void pubsubResumeTests() throws Exception {
        long firstId;
        try (DocsqlConnection c = connect(server.port)) {
            firstId = c.publish("java-resume", "before")[0];
        }
        final java.util.List<Map<String, Object>> r2 = new java.util.ArrayList<Map<String, Object>>();
        final java.util.concurrent.CountDownLatch got1 = new java.util.concurrent.CountDownLatch(1);
        Subscriber sub2 = new Subscriber(ConnectConfig.builder()
                .host("127.0.0.1").port(server.port).token(TOKEN).build(),
                new Subscriber.Listener() {
                    public void onMessage(Map<String, Object> m) {
                        synchronized (r2) {
                            r2.add(m);
                            got1.countDown();
                        }
                    }
                });
        try {
            sub2.subscribe("java-resume", String.valueOf(firstId));
            try (DocsqlConnection c = connect(server.port)) {
                c.publish("java-resume", "after");
            }
            check("pub.resume", got1.await(10, java.util.concurrent.TimeUnit.SECONDS));
            synchronized (r2) {
                check("pub.resume_after_only", r2.size() >= 1 && "after".equals(r2.get(r2.size() - 1).get("payload")));
            }
        } finally {
            sub2.close();
        }
    }

    static void pubsubPatternTests() throws Exception {
        final java.util.List<Map<String, Object>> r3 = new java.util.ArrayList<Map<String, Object>>();
        final java.util.concurrent.CountDownLatch gotP = new java.util.concurrent.CountDownLatch(1);
        Subscriber sub3 = new Subscriber(ConnectConfig.builder()
                .host("127.0.0.1").port(server.port).token(TOKEN).build(),
                new Subscriber.Listener() {
                    public void onMessage(Map<String, Object> m) {
                        synchronized (r3) {
                            r3.add(m);
                            gotP.countDown();
                        }
                    }
                });
        try {
            sub3.psubscribe("java-glob-*");
            try (DocsqlConnection c = connect(server.port)) {
                c.publish("java-glob-one", "hello");
            }
            check("pub.glob", gotP.await(10, java.util.concurrent.TimeUnit.SECONDS));
            synchronized (r3) {
                check("pub.glob_kind", "pmessage".equals(r3.get(r3.size() - 1).get("kind")));
            }
        } finally {
            sub3.close();
        }
    }

    static void authTests() throws Exception {
        expectThrows("auth.bad_token", DocsqlException.Operational.class, () -> {
            try (DocsqlConnection c = new DocsqlConnection(ConnectConfig.builder()
                    .host("127.0.0.1").port(server.port).token("wrong-token").build())) {
                c.ping();
            }
        });
        try (DocsqlConnection c = connect(server.port)) {
            c.execute("CREATE USER javauser PASSWORD 'java-pass-1234'");
            c.execute("GRANT readwrite TO javauser");
        }
        try (DocsqlConnection c = new DocsqlConnection(ConnectConfig.builder()
                .host("127.0.0.1").port(server.port).user("javauser").password("java-pass-1234").build())) {
            Result r = c.execute("SELECT 1");
            check("auth.user_login", ((Long) r.rows.get(0)[0]) == 1L);
        }
        expectThrows("auth.wrong_password", DocsqlException.Operational.class, () -> {
            try (DocsqlConnection c = new DocsqlConnection(ConnectConfig.builder()
                    .host("127.0.0.1").port(server.port).user("javauser").password("nope").build())) {
                c.ping();
            }
        });
        try (DocsqlConnection c = connect(server.port)) {
            c.execute("DROP USER javauser");
        }
    }

    static void jdbcTests() throws Exception {
        String url = "jdbc:docsql://127.0.0.1:" + server.port + "?token=" + TOKEN;
        try (java.sql.Connection c = java.sql.DriverManager.getConnection(url)) {
            check("jdbc.via_drivermanager", !c.isClosed());
            check("jdbc.autocommit_on", c.getAutoCommit());
            java.sql.Statement st = c.createStatement();
            java.sql.ResultSet rs = st.executeQuery("SELECT 1 AS n, 'x' AS s, 2.5 AS d");
            rs.next();
            check("jdbc.rs_getters", rs.getInt(1) == 1 && "x".equals(rs.getString(2))
                    && rs.getBigDecimal(3).compareTo(new BigDecimal("2.5")) == 0);
            check("jdbc.rs_by_name", rs.getInt("n") == 1);
            check("jdbc.wasNull", !rs.wasNull());
            check("jdbc.metadata_count", rs.getMetaData().getColumnCount() == 3);
            check("jdbc.metadata_names", "n".equals(rs.getMetaData().getColumnName(1)));
            check("jdbc.metadata_types", "BIGINT".equals(rs.getMetaData().getColumnTypeName(1)));
            check("jdbc.metadata_types_double", "DOUBLE".equals(rs.getMetaData().getColumnTypeName(3)));
            java.sql.PreparedStatement ps = c.prepareStatement("INSERT INTO java_bind VALUES (?, ?, ?, ?, ?)");
            ps.setLong(1, 7L);
            ps.setString(2, "jdbc-row");
            ps.setBigDecimal(3, new BigDecimal("9.99"));
            ps.setTimestamp(4, new java.sql.Timestamp(86400000L));
            ps.setBytes(5, new byte[] {(byte) 0xAA, 0x55});
            check("jdbc.ps_executeUpdate", ps.executeUpdate() == 1);
            java.sql.PreparedStatement q = c.prepareStatement("SELECT name, at, blob_col FROM java_bind WHERE id = ?");
            q.setLong(1, 7L);
            java.sql.ResultSet qr = q.executeQuery();
            qr.next();
            check("jdbc.ps_query", "jdbc-row".equals(qr.getString(1)));
            check("jdbc.ps_ts", qr.getTimestamp(2).getTime() == 86400000L);
            byte[] b = qr.getBytes(3);
            check("jdbc.ps_bytes", b.length == 2 && (b[0] & 0xFF) == 0xAA);
            qr.close();
            q.close();
            java.sql.DatabaseMetaData md = c.getMetaData();
            check("jdbc.md_product", "DocSQL".equals(md.getDatabaseProductName()));
            check("jdbc.md_version_nonempty", !md.getDatabaseProductVersion().isEmpty());
            check("jdbc.md_url", md.getURL().contains("jdbc:docsql://127.0.0.1"));
            java.sql.ResultSet tables = md.getTables(null, null, "JAVA_%", new String[] {"TABLE"});
            boolean foundBind = false;
            while (tables.next()) {
                if ("JAVA_BIND".equalsIgnoreCase(tables.getString("TABLE_NAME"))) {
                    foundBind = true;
                }
            }
            check("jdbc.md_tables", foundBind);
            java.sql.ResultSet cols = md.getColumns(null, null, "JAVA_BIND", "%");
            boolean foundCol = false;
            while (cols.next()) {
                if ("NAME".equalsIgnoreCase(cols.getString("COLUMN_NAME"))) {
                    foundCol = true;
                }
            }
            check("jdbc.md_columns", foundCol);
            st.close();
            ps.close();
        }
    }

    static void savepointTests() throws Exception {
        String url = "jdbc:docsql://127.0.0.1:" + server.port + "?token=" + TOKEN;
        try (java.sql.Connection c = java.sql.DriverManager.getConnection(url)) {
            c.setAutoCommit(false);
            java.sql.Statement st = c.createStatement();
            st.execute("INSERT INTO java_bind VALUES (8, 'svpt', NULL, NULL, NULL)");
            java.sql.Savepoint sp = c.setSavepoint();
            st.execute("INSERT INTO java_bind VALUES (9, 'after-svpt', NULL, NULL, NULL)");
            c.rollback(sp);
            java.sql.ResultSet r = st.executeQuery("SELECT COUNT(*) FROM java_bind WHERE id = 9");
            r.next();
            check("jdbc.savepoint_rollback", r.getInt(1) == 0);
            c.rollback();
        }
        // open-server (anonymous access stays open when no token is configured)
        try (DocsqlConnection c = connect(openServer.port, false)) {
            Result r = c.execute("SELECT 1");
            check("open.anonymous", ((Long) r.rows.get(0)[0]) == 1L);
        }
    }
}
