package io.docsql.jdbc;

import io.docsql.Result;
import java.math.BigDecimal;
import java.sql.SQLException;

/** ResultSet metadata: names from the wire, types inferred from the first non-null cell. */
public final class JdbcResultSetMetaData implements java.sql.ResultSetMetaData {

    private final Result r;

    JdbcResultSetMetaData(Result r) {
        this.r = r;
    }

    private int typeOf(int col) {
        for (Object[] row : r.rows) {
            Object v = row[col - 1];
            if (v == null) {
                continue;
            }
            if (v instanceof String) {
                return java.sql.Types.VARCHAR;
            }
            if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
                return java.sql.Types.BIGINT;
            }
            if (v instanceof BigDecimal) {
                return java.sql.Types.DECIMAL;
            }
            if (v instanceof Double || v instanceof Float) {
                return java.sql.Types.DOUBLE;
            }
            if (v instanceof Boolean) {
                return java.sql.Types.BOOLEAN;
            }
            if (v instanceof byte[]) {
                return java.sql.Types.BINARY;
            }
            if (v instanceof java.sql.Timestamp) {
                return java.sql.Types.TIMESTAMP;
            }
            return java.sql.Types.OTHER;
        }
        return java.sql.Types.OTHER;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return r.columns.size();
    }

    @Override
    public int getColumnType(int col) throws SQLException {
        return typeOf(col - 1 + 1);
    }

    @Override
    public String getColumnTypeName(int col) throws SQLException {
        switch (typeOf(col)) {
            case java.sql.Types.VARCHAR: return "VARCHAR";
            case java.sql.Types.BIGINT: return "BIGINT";
            case java.sql.Types.DECIMAL: return "DECIMAL";
            case java.sql.Types.DOUBLE: return "DOUBLE";
            case java.sql.Types.BOOLEAN: return "BOOLEAN";
            case java.sql.Types.BINARY: return "BLOB";
            case java.sql.Types.TIMESTAMP: return "TIMESTAMP";
            default: return "JSON";
        }
    }

    @Override
    public String getColumnName(int col) throws SQLException {
        checkRange(col);
        return r.columns.get(col - 1);
    }

    @Override
    public String getColumnLabel(int col) throws SQLException {
        checkRange(col);
        return r.columns.get(col - 1);
    }

    @Override
    public String getSchemaName(int col) throws SQLException {
        return "";
    }

    @Override
    public String getTableName(int col) throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName(int col) throws SQLException {
        return "";
    }

    private void checkRange(int col) throws SQLException {
        if (col < 1 || col > r.columns.size()) {
            throw new SQLException("column index out of range: " + col);
        }
    }
    @Override
    public boolean isAutoIncrement(int col) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int col) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int col) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int col) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int col) throws SQLException {
        return java.sql.ResultSetMetaData.columnNullableUnknown;
    }

    @Override
    public boolean isSigned(int col) throws SQLException {
        return true;
    }

    @Override
    public boolean isDefinitelyWritable(int col) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int col) throws SQLException {
        return false;
    }

    @Override
    public boolean isReadOnly(int col) throws SQLException {
        return false;
    }

    @Override
    public int getPrecision(int col) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int col) throws SQLException {
        return 0;
    }

    @Override
    public int getColumnDisplaySize(int col) throws SQLException {
        return 0;
    }

    @Override
    public String getColumnClassName(int col) throws SQLException {
        switch (typeOf(col)) {
            case java.sql.Types.VARCHAR: return "java.lang.String";
            case java.sql.Types.BIGINT: return "java.lang.Long";
                       case java.sql.Types.DECIMAL: return "java.math.BigDecimal";
            case java.sql.Types.DOUBLE: return "java.lang.Double";
            case java.sql.Types.BOOLEAN: return "java.lang.Boolean";
            case java.sql.Types.BINARY: return "[B";
            case java.sql.Types.TIMESTAMP: return "java.sql.Timestamp";
            default: return "java.lang.Object";
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("not a wrapper for " + iface.getName());
    }
}
