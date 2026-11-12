package io.docsql.jdbc;

import io.docsql.DocsqlException;
import io.docsql.Result;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * java.sql.PreparedStatement over the server-side prepared-statement protocol (qmark templates,
 * REQ_PREPARE + REQ_EXECUTE). Parameters bind SERVER-SIDE — the template is registered once per
 * connection and reused, and a bound value can never break out of its literal (the engine renders
 * typed literals itself).
 */
public class JdbcPreparedStatement extends JdbcStatement implements java.sql.PreparedStatement {

    private final String sql;
    private final Object[] params = new Object[0];
    private final Map<Integer, Object> bound = new HashMap<>();
    private int paramCount;

    JdbcPreparedStatement(JdbcConnection conn, String sql) {
        super(conn);
        this.sql = sql;
    }

    private void set(int idx, Object v) throws SQLException {
        checkOpen();
        if (idx < 1) {
            throw new SQLException("parameter index must be >= 1");
        }
        bound.put(idx, v);
        paramCount = Math.max(paramCount, idx);
    }

    private Object[] snapshot() throws SQLException {
        checkOpen();
        Object[] out = new Object[paramCount];
        for (int i = 1; i <= paramCount; i++) {
            if (!bound.containsKey(i)) {
                throw new SQLException("parameter " + i + " is not set", "07000");
            }
            out[i - 1] = bound.get(i);
        }
        return out;
    }

    @Override
    public java.sql.ResultSet executeQuery() throws SQLException {
        Object[] p = snapshot();
        try {
            Result r = conn.core().execute(sql, p);
            if (!r.hasResultSet()) {
                throw new SQLException("statement did not produce a result set", "01000");
            }
            current = r;
            return new JdbcResultSet(this, r);
        } catch (DocsqlException e) {
            throw JdbcSql.wrap(e);
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        Object[] p = snapshot();
        try {
            Result r = conn.core().execute(sql, p);
            if (r.hasResultSet()) {
                throw new SQLException("statement produced a result set", "01000");
            }
            current = r;
            return (int) Math.min(Integer.MAX_VALUE, Math.max(Integer.MIN_VALUE, r.rowcount));
        } catch (DocsqlException e) {
            throw JdbcSql.wrap(e);
        }
    }

    @Override
    public boolean execute() throws SQLException {
        Object[] p = snapshot();
        try {
            Result r = conn.core().execute(sql, p);
            current = r.hasResultSet() ? r : null;
            return r.hasResultSet();
        } catch (DocsqlException e) {
            throw JdbcSql.wrap(e);
        }
    }

    @Override
    public void setByte(int i, byte x) throws SQLException {
        set(i, x);
    }

    @Override
    public void setShort(int i, short x) throws SQLException {
        set(i, x);
    }

    @Override
    public void setInt(int i, int x) throws SQLException {
        set(i, x);
    }

    @Override
    public void setLong(int i, long x) throws SQLException {
        set(i, x);
    }

    @Override
    public void setFloat(int i, float x) throws SQLException {
        set(i, x);
    }

    @Override
    public void setDouble(int i, double x) throws SQLException {
        set(i, x);
    }

    @Override
    public void setBigDecimal(int i, BigDecimal x) throws SQLException {
        set(i, x);
    }

    @Override
    public void setString(int i, String x) throws SQLException {
        set(i, x);
    }

    @Override
    public void setBytes(int i, byte[] x) throws SQLException {
        set(i, x);
    }

    @Override
    public void setDate(int i, java.sql.Date x) throws SQLException {
        set(i, x);
    }

    @Override
    public void setTime(int i, java.sql.Time x) throws SQLException {
        set(i, x);
    }

    @Override
    public void setTimestamp(int i, java.sql.Timestamp x) throws SQLException {
        set(i, x);
    }

    @Override
    public void setTimestamp(int i, java.sql.Timestamp x, java.util.Calendar cal) throws SQLException {
        set(i, x);
    }

    @Override
    public void setAsciiStream(int i, java.io.InputStream x, int length) throws SQLException {
        throw JdbcSql.feature("streams are not supported; use byte[]");
    }

    @Override
    public void setUnicodeStream(int i, java.io.InputStream x, int length) throws SQLException {
        throw JdbcSql.feature("streams are not supported; use byte[]");
    }

    @Override
    public void setBinaryStream(int i, java.io.InputStream x, int length) throws SQLException {
        throw JdbcSql.feature("streams are not supported; use byte[]");
    }

    @Override
    public void setObject(int i, Object x) throws SQLException {
        set(i, x);
    }

    @Override
    public void setObject(int i, Object x, int targetSqlType) throws SQLException {
        set(i, convert(x, targetSqlType));
    }

    @Override
    public void setObject(int i, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        Object v = convert(x, targetSqlType);
        if (v instanceof BigDecimal && scaleOrLength >= 0) {
            v = ((BigDecimal) v).setScale(scaleOrLength, java.math.RoundingMode.HALF_EVEN);
        }
        set(i, v);
    }

    private static Object convert(Object x, int targetSqlType) throws SQLException {
        if (x == null) {
            return null;
        }
        switch (targetSqlType) {
            case java.sql.Types.VARCHAR:
            case java.sql.Types.CHAR:
            case java.sql.Types.LONGVARCHAR:
                return String.valueOf(x);
            case java.sql.Types.INTEGER:
                if (x instanceof Number) {
                    return ((Number) x).intValue();
                }
                return Integer.parseInt(String.valueOf(x).trim());
            case java.sql.Types.BIGINT:
                if (x instanceof Number) {
                    return ((Number) x).longValue();
                }
                return Long.parseLong(String.valueOf(x).trim());
            case java.sql.Types.DOUBLE:
            case java.sql.Types.FLOAT:
                if (x instanceof Number) {
                    return ((Number) x).doubleValue();
                }
                return Double.parseDouble(String.valueOf(x).trim());
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                if (x instanceof BigDecimal) {
                    return x;
                }
                return new BigDecimal(String.valueOf(x).trim());
            case java.sql.Types.BOOLEAN:
            case java.sql.Types.BIT:
                if (x instanceof Boolean) {
                    return x;
                }
                if (x instanceof Number) {
                    return ((Number) x).doubleValue() != 0.0;
                }
                return Boolean.parseBoolean(String.valueOf(x).trim());
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
                if (x instanceof byte[]) {
                    return x;
                }
                throw new SQLException("cannot convert to BINARY");
            case java.sql.Types.TIMESTAMP:
                return toTimestamp(x);
            case java.sql.Types.DATE:
                return new java.sql.Date(toTimestamp(x).getTime());
            case java.sql.Types.TIME:
                return new java.sql.Time(toTimestamp(x).getTime());
            case java.sql.Types.NULL:
                return null;
            case java.sql.Types.OTHER:
            case java.sql.Types.JAVA_OBJECT:
                return x;
            default:
                throw JdbcSql.unsupported("target SQL type " + targetSqlType);
        }
    }

    private static java.sql.Timestamp toTimestamp(Object x) throws SQLException {
        if (x instanceof java.sql.Timestamp) {
            return (java.sql.Timestamp) x;
        }
        if (x instanceof java.util.Date) {
            return new java.sql.Timestamp(((java.util.Date) x).getTime());
        }
        if (x instanceof java.time.Instant) {
            return java.sql.Timestamp.from((java.time.Instant) x);
        }
        if (x instanceof java.time.LocalDateTime) {
            return java.sql.Timestamp.valueOf((java.time.LocalDateTime) x);
        }
        if (x instanceof Number) {
            return new java.sql.Timestamp(((Number) x).longValue());
        }
        throw new SQLException("cannot convert to TIMESTAMP: " + x.getClass().getName());
    }

    @Override
    public void setNull(int i, int sqlType) throws SQLException {
        set(i, null);
    }

    @Override
    public void setNull(int i, int sqlType, String typeName) throws SQLException {
        set(i, null);
    }

    @Override
    public void setBoolean(int i, boolean x) throws SQLException {
        set(i, x);
    }

    @Override
    public void setCharacterStream(int i, java.io.Reader x, int length) throws SQLException {
        throw JdbcSql.feature("streams are not supported; use String");
    }

    @Override
    public void setRef(int i, java.sql.Ref x) throws SQLException {
        throw JdbcSql.feature("Ref is not supported");
    }

    @Override
    public void setBlob(int i, java.sql.Blob x) throws SQLException {
        throw JdbcSql.feature("LOB locators are not supported; use byte[]");
    }

    @Override
    public void setClob(int i, java.sql.Clob x) throws SQLException {
        throw JdbcSql.feature("LOB locators are not supported; use String");
    }

    @Override
    public void setArray(int i, java.sql.Array x) throws SQLException {
        throw JdbcSql.feature("Array is not supported");
    }

    @Override
    public void setDate(int i, java.sql.Date x, java.util.Calendar cal) throws SQLException {
        set(i, x);
    }

    @Override
    public void setTime(int i, java.sql.Time x, java.util.Calendar cal) throws SQLException {
        set(i, x);
    }

    @Override
    public void setURL(int i, java.net.URL x) throws SQLException {
        set(i, x == null ? null : x.toString());
    }

    @Override
    public void setNString(int i, String x) throws SQLException {
        set(i, x);
    }

    @Override
    public void setNCharacterStream(int i, java.io.Reader x) throws SQLException {
        throw JdbcSql.feature("streams are not supported; use String");
    }

    @Override
    public void setAsciiStream(int i, java.io.InputStream x) throws SQLException {
        throw JdbcSql.feature("streams are not supported; use byte[]");
    }

    @Override
    public void setAsciiStream(int i, java.io.InputStream x, long length) throws SQLException {
        throw JdbcSql.feature("streams/LOB locators are not supported; use byte[]");
    }

    @Override
    public void setBinaryStream(int i, java.io.InputStream x) throws SQLException {
        throw JdbcSql.feature("streams are not supported; use byte[]");
    }

    @Override
    public void setBinaryStream(int i, java.io.InputStream x, long length) throws SQLException {
        throw JdbcSql.feature("streams are not supported; use byte[]");
    }

    @Override
    public void setCharacterStream(int i, java.io.Reader x) throws SQLException {
        throw JdbcSql.feature("streams are not supported; use String");
    }

    @Override
    public void setCharacterStream(int i, java.io.Reader x, long length) throws SQLException {
        throw JdbcSql.feature("streams are not supported; use String");
    }

    @Override
    public void setBlob(int i, java.io.InputStream x, long length) throws SQLException {
        throw JdbcSql.feature("LOB locators are not supported; use byte[]");
    }

    @Override
    public void setBlob(int i, java.io.InputStream x) throws SQLException {
        throw JdbcSql.feature("LOB locators are not supported; use byte[]");
    }

    @Override
    public void setClob(int i, java.io.Reader reader) throws SQLException {
        throw JdbcSql.feature("LOB locators are not supported; use String");
    }

    @Override
    public void setClob(int i, java.io.Reader reader, long length) throws SQLException {
        throw JdbcSql.feature("LOB locators are not supported; use String");
    }

    @Override
    public void setNClob(int i, java.io.Reader x) throws SQLException {
        throw JdbcSql.feature("NClob is not supported");
    }

    @Override
    public void setNClob(int i, java.io.Reader x, long length) throws SQLException {
        throw JdbcSql.feature("NClob is not supported");
    }

    @Override
    public void setNCharacterStream(int i, java.io.Reader x, long length) throws SQLException {
        throw JdbcSql.feature("streams are not supported; use String");
    }

    @Override
    public void setNClob(int i, java.sql.NClob x) throws SQLException {
        throw JdbcSql.feature("NClob is not supported");
    }

    @Override
    public void setRowId(int i, java.sql.RowId x) throws SQLException {
        throw JdbcSql.feature("RowId is not supported");
    }

    @Override
    public void setSQLXML(int i, java.sql.SQLXML x) throws SQLException {
        throw JdbcSql.feature("SQLXML is not supported");
    }

    @Override
    public void clearParameters() throws SQLException {
        checkOpen();
        bound.clear();
        paramCount = 0;
    }

    @Override
    public java.sql.ResultSetMetaData getMetaData() throws SQLException {
        checkOpen();
        // No wire-level parameter metadata; the result-set shape comes back with the rows.
        throw JdbcSql.feature("PreparedStatement.getMetaData is not supported");
    }

    @Override
    public java.sql.ParameterMetaData getParameterMetaData() throws SQLException {
        checkOpen();
        throw JdbcSql.feature("ParameterMetaData is not supported");
    }

    @Override
    public void addBatch() throws SQLException {
        checkOpen();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("not a wrapper for " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
