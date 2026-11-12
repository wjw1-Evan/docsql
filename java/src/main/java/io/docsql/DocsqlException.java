package io.docsql;

/**
 * DB-API-style unchecked exception hierarchy, mapped from server replies.
 *
 * <p>The mapping is heuristic over the server's error TEXT (the v1 protocol has no structured
 * error codes); unrecognized texts raise {@link DocsqlDatabaseException}. The connection is NOT
 * automatically closed on {@link DocsqlDatabaseException} subclasses — only transport failures
 * ({@link DocsqlInterfaceException}) leave it unusable, matching the server's per-statement error
 * semantics.
 */
public class DocsqlException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public DocsqlException(String message) {
        super(message);
    }

    public DocsqlException(String message, Throwable cause) {
        super(message, cause);
    }

    /** Transport/protocol failure — the connection must be discarded. */
    public static class Interface extends DocsqlException {
        private static final long serialVersionUID = 1L;

        public Interface(String message) {
            super(message);
        }

        public Interface(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** Framing-level failure (bad magic, oversized frame, EOF mid-frame). */
    public static class Protocol extends DocsqlException.Interface {
        private static final long serialVersionUID = 1L;

        public Protocol(String message) {
            super(message);
        }

        public Protocol(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** Server-side statement failure (parse, constraint, execution...). */
    public static class Database extends DocsqlException {
        private static final long serialVersionUID = 1L;

        public Database(String message) {
            super(message);
        }

        public Database(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** Value out of domain (oversized frame, bad literal...). */
    public static class Data extends DocsqlException.Database {
        private static final long serialVersionUID = 1L;

        public Data(String message) {
            super(message);
        }
    }

    /** Connection/credential/timeout trouble (reconnectable). */
    public static class Operational extends DocsqlException.Database {
        private static final long serialVersionUID = 1L;

        public Operational(String message) {
            super(message);
        }

        public Operational(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** Constraint violation (UNIQUE / FOREIGN KEY / CHECK / NOT NULL). */
    public static class Integrity extends DocsqlException.Database {
        private static final long serialVersionUID = 1L;

        public Integrity(String message) {
            super(message);
        }
    }

    /** SQL syntax/parse errors and driver misuse (wrong arity...). */
    public static class Programming extends DocsqlException.Database {
        private static final long serialVersionUID = 1L;

        public Programming(String message) {
            super(message);
        }
    }
}
