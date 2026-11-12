package io.docsql;

/**
 * Immutable connection settings shared by {@link DocsqlConnection} and {@link Subscriber}.
 * Defaults: host 127.0.0.1, port 7600, no credentials, plaintext, 15s timeout,
 * autocommit ON (deliberate Java-side default: DocSQL is single-writer, an idle open
 * transaction parks the write path; the JDBC layer maps autoCommit=false to the implicit
 * BEGIN block exactly like the Python driver).
 */
public final class ConnectConfig {

    public final String host;
    public final int port;
    public final String token;
    public final String user;
    public final String password;
    public final boolean tls;
    public final String tlsCaPath;
    public final String tlsHostname;
    public final String key;
    public final int timeoutMs;
    public final boolean autocommit;

    private ConnectConfig(Builder b) {
        this.host = b.host;
        this.port = b.port;
        this.token = b.token;
        this.user = b.user;
        this.password = b.password;
        this.tls = b.tls;
        this.tlsCaPath = b.tlsCaPath;
        this.tlsHostname = b.tlsHostname;
        this.key = b.key;
        this.timeoutMs = b.timeoutMs;
        this.autocommit = b.autocommit;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        Builder b = new Builder();
        b.host = host;
        b.port = port;
        b.token = token;
        b.user = user;
        b.password = password;
        b.tls = tls;
        b.tlsCaPath = tlsCaPath;
        b.tlsHostname = tlsHostname;
        b.key = key;
        b.timeoutMs = timeoutMs;
        b.autocommit = autocommit;
        return b;
    }

    public static final class Builder {
        private String host = "127.0.0.1";
        private int port = 7600;
        private String token;
        private String user;
        private String password;
        private boolean tls;
        private String tlsCaPath;
        private String tlsHostname;
        private String key;
        private int timeoutMs = 15_000;
        private boolean autocommit = true;

        public Builder host(String v) {
            this.host = v;
            return this;
        }

        public Builder port(int v) {
            this.port = v;
            return this;
        }

        public Builder token(String v) {
            this.token = v;
            return this;
        }

        public Builder user(String v) {
            this.user = v;
            return this;
        }

        public Builder password(String v) {
            this.password = v;
            return this;
        }

        public Builder tls(boolean v) {
            this.tls = v;
            return this;
        }

        public Builder tlsCaPath(String v) {
            this.tlsCaPath = v;
            return this;
        }

        public Builder tlsHostname(String v) {
            this.tlsHostname = v;
            return this;
        }

        public Builder key(String v) {
            this.key = v;
            return this;
        }

        public Builder timeoutMs(int v) {
            this.timeoutMs = v;
            return this;
        }

        public Builder autocommit(boolean v) {
            this.autocommit = v;
            return this;
        }

        public ConnectConfig build() {
            return new ConnectConfig(Builder.this);
        }
    }
}
