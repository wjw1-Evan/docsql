package io.docsql.jdbc;

import io.docsql.ConnectConfig;
import java.util.Properties;

public final class DriverUrl {

    private DriverUrl() {}

    public static ConnectConfig parse(String url, Properties info) {
        if (url == null || !url.toLowerCase(java.util.Locale.ROOT).startsWith("jdbc:docsql:")) {
            return null;
        }
        String rest = url.substring(12);
        while (rest.startsWith("/")) {
            rest = rest.substring(1);
        }
        if (rest.isEmpty()) {
            return null;
        }
        String query = "";
        int qIdx = rest.indexOf('?');
        if (qIdx >= 0) {
            query = rest.substring(qIdx + 1);
            rest = rest.substring(0, qIdx);
        }
        String[] hostPort = rest.split(":", 2);
        String host = hostPort[0].trim();
        int port = 7600;
        if (hostPort.length == 2) {
            port = Integer.parseInt(hostPort[1].trim());
        }
        ConnectConfig.Builder b = ConnectConfig.builder();
        b.host(host).port(port);
        applyParams(b, query, info);
        return b.build();
    }

    /** URL query params win; Properties(info) fills anything the URL omits. */
    private static void applyParams(ConnectConfig.Builder b, String query, Properties info) {
        java.util.Map<String, String> q = new java.util.LinkedHashMap<>();
        if (query != null && !query.isEmpty()) {
            for (String pair : query.split("&")) {
                int eq = pair.indexOf('=');
                if (eq > 0) {
                    q.put(urlDecode(pair.substring(0, eq)),
                            urlDecode(pair.substring(eq + 1)));
                }
            }
        }
        if (info != null) {
            for (String k : new String[] {"token", "user", "password", "tls", "tlsCa",
                        "tlsHostname", "key", "timeout", "autocommit"}) {
                if (!q.containsKey(k) && info.getProperty(k) != null) {
                    q.put(k, info.getProperty(k));
                }
            }
        }
        String v = q.get("token");
        if (v != null) {
            b.token(v);
        }
        v = q.get("user");
        if (v != null) {
            b.user(v);
        }
        v = q.get("password");
        if (v != null) {
            b.password(v);
        }
        v = q.get("tls");
        if (v != null) {
            b.tls(Boolean.parseBoolean(v.trim()));
        }
        v = q.get("tlsCa");
        if (v != null) {
            b.tlsCaPath(v);
        }
        v = q.get("tlsHostname");
        if (v != null) {
            b.tlsHostname(v);
        }
        v = q.get("key");
        if (v != null) {
            b.key(v);
        }
        v = q.get("timeout");
        if (v != null) {
            b.timeoutMs((int) Math.round(Double.parseDouble(v.trim()) * 1000.0));
        }
        v = q.get("autocommit");
        if (v != null) {
            b.autocommit(Boolean.parseBoolean(v.trim()));
        }
    }

    private static String urlDecode(String s) {
        try {
            return java.net.URLDecoder.decode(s, "UTF-8");
        } catch (java.io.UnsupportedEncodingException e) {
            return s;
        }
    }
}
