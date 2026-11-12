package io.docsql.jdbc;

import io.docsql.DocsqlException;
import java.sql.SQLException;

/** Maps core DocsqlException types onto the standard JDBC SQLException family. */
final class JdbcSql {

    private JdbcSql() {}

    static SQLException wrap(DocsqlException e) {
        if (e instanceof DocsqlException.Integrity) {
            return new java.sql.SQLIntegrityConstraintViolationException(e.getMessage(), "23000", e);
        }
        if (e instanceof DocsqlException.Data) {
            return new java.sql.SQLDataException(e.getMessage(), "22000", e);
        }
        if (e instanceof DocsqlException.Programming) {
            return new java.sql.SQLSyntaxErrorException(e.getMessage(), "42000", e);
        }
        if (e instanceof DocsqlException.Operational) {
            return new java.sql.SQLTransientConnectionException(e.getMessage(), "08001", e);
        }
        if (e instanceof DocsqlException.Interface) {
            return new java.sql.SQLNonTransientConnectionException(e.getMessage(), "08006", e);
        }
        return new SQLException(e.getMessage(), "HY000", e);
    }

    static SQLException closed() {
        return new java.sql.SQLNonTransientConnectionException("connection is closed", "08003");
    }

    static SQLException feature(String what) {
        return new java.sql.SQLFeatureNotSupportedException(what);
    }

    static SQLException unsupported(String what) {
        return new java.sql.SQLFeatureNotSupportedException(what);
    }
}
