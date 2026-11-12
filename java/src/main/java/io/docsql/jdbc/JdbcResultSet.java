package io.docsql.jdbc;

import io.docsql.Result;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.List;

/**
 * Materialized java.sql.ResultSet: the RESP_ROWS frame already carries the whole result, so all
 * getters read the in-memory rows (the server has no row-streaming cursor on this protocol).
 */
public final class JdbcResultSet implements java.sql.ResultSet {

    private final JdbcStatement stmt;
    private final Result r;
    private int pos = -1;
    private boolean wasNull;
    private boolean closed;

    JdbcResultSet(JdbcStatement stmt, Result r) {
        this.stmt = stmt;
        this.r = r;
    }

    private void checkOpen() throws SQLException {
        if (closed || (stmt != null && stmt.isClosed())) {
            throw JdbcSql.closed();
        }
    }

    private void checkRow() throws SQLException {
        checkOpen();
        if (pos < 0 || pos >= r.rows.size()) {
            throw new SQLException("no current row", "24000");
        }
        return;
    }

    private Object cell(int col) throws SQLException {
        checkRow();
        Object[] row = r.rows.get(pos);
        if (col < 1 || col > row.length) {
            throw new SQLException("column index out of range: " + col);
        }
        Object v = row[col - 1];
        wasNull = v == null;
        return v;
    }

    private Object cellByName(String name) throws SQLException {
        return cell(findColumn(name));
    }

    @Override
    public boolean next() throws SQLException {
        checkOpen();
        if (pos < r.rows.size() - 1) {
            pos++;
            return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException {
        closed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkOpen();
        return wasNull;
    }

    @Override
    public String getString(int col) throws SQLException {
        Object v = cell(col);
        if (v == null) {
            return null;
        }
        if (v instanceof String) {
            return (String) v;
        }
        if (v instanceof byte[]) {
            return new String((byte[]) v, java.nio.charset.StandardCharsets.UTF_8);
        }
        if (v instanceof BigDecimal) {
            return ((BigDecimal) v).toPlainString();
        }
        return String.valueOf(v);
    }

    @Override
    public boolean getBoolean(int col) throws SQLException {
        Object v = cell(col);
        if (v == null) {
            return false;
        }
        if (v instanceof Boolean) {
            return (Boolean) v;
        }
        if (v instanceof Number) {
            return ((Number) v).doubleValue() != 0.0;
        }
        String s = String.valueOf(v).trim();
        return s.equalsIgnoreCase("true") || s.equals("1") || s.equalsIgnoreCase("yes");
    }

    @Override
    public byte getByte(int col) throws SQLException {
        return (byte) getInt(col);
    }

    @Override
    public short getShort(int col) throws SQLException {
        return (short) getInt(col);
    }

    @Override
    public int getInt(int col) throws SQLException {
        Object v = cell(col);
        if (v == null) {
            return 0;
        }
        if (v instanceof Long) {
            return (int) Math.max(Integer.MIN_VALUE, Math.min(Integer.MAX_VALUE, (Long) v));
        }
        if (v instanceof Number) {
            return ((Number) v).intValue();
        }
        if (v instanceof Boolean) {
            return (Boolean) v ? 1 : 0;
        }
        return Integer.parseInt(getString(col).trim());
    }

    @Override
    public long getLong(int col) throws SQLException {
        Object v = cell(col);
        if (v == null) {
            return 0L;
        }
        if (v instanceof Long) {
            return (Long) v;
        }
        if (v instanceof BigInteger) {
            return ((BigInteger) v).longValueExact();
        }
        if (v instanceof Number) {
            return ((Number) v).longValue();
        }
        return Long.parseLong(getString(col).trim());
    }

    @Override
    public float getFloat(int col) throws SQLException {
        return (float) getDouble(col);
    }

    @Override
    public double getDouble(int col) throws SQLException {
        Object v = cell(col);
        if (v == null) {
            return 0.0;
        }
        if (v instanceof Number) {
            return ((Number) v).doubleValue();
        }
        return Double.parseDouble(getString(col).trim());
    }

    @Override
    public BigDecimal getBigDecimal(int col, int scale) throws SQLException {
        BigDecimal bd = getBigDecimal(col);
        if (bd != null) {
            bd = bd.setScale(scale, java.math.RoundingMode.HALF_EVEN);
        }
        return bd;
    }

    @Override
    public BigDecimal getBigDecimal(int col) throws SQLException {
        Object v = cell(col);
        if (v == null) {
            return null;
        }
        if (v instanceof BigDecimal) {
            return (BigDecimal) v;
        }
        if (v instanceof BigInteger) {
            return new BigDecimal((BigInteger) v);
        }
        if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
            return BigDecimal.valueOf(((Number) v).longValue());
        }
        if (v instanceof Double || v instanceof Float) {
            return BigDecimal.valueOf(((Number) v).doubleValue());
        }
        return new BigDecimal(getString(col).trim());
    }

    @Override
    public byte[] getBytes(int col) throws SQLException {
        Object v = cell(col);
        if (v == null) {
            return null;
        }
        if (v instanceof byte[]) {
            return (byte[]) v;
        }
        if (v instanceof String) {
            return ((String) v).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
        throw new SQLException("column " + col + " is not binary");
    }

    @Override
    public java.sql.Date getDate(int col) throws SQLException {
        java.sql.Timestamp ts = getTimestamp(col);
        if (ts == null) {
            return null;
        }
        return new java.sql.Date(ts.getTime());
    }

    @Override
    public java.sql.Time getTime(int col) throws SQLException {
        java.sql.Timestamp ts = getTimestamp(col);
        if (ts == null) {
            return null;
        }
        return new java.sql.Time(ts.getTime());
    }

    @Override
    public java.sql.Timestamp getTimestamp(int col) throws SQLException {
        Object v = cell(col);
        if (v == null) {
            return null;
        }
        if (v instanceof java.sql.Timestamp) {
            return (java.sql.Timestamp) v;
        }
        if (v instanceof Long) {
            return new java.sql.Timestamp((Long) v);
        }
        if (v instanceof java.time.LocalDateTime) {
            return java.sql.Timestamp.valueOf((java.time.LocalDateTime) v);
        }
        String s = String.valueOf(v).trim();
        try {
            return java.sql.Timestamp.valueOf(s);
        } catch (IllegalArgumentException e) {
            try {
                return new java.sql.Timestamp(java.time.Instant.parse(s).toEpochMilli());
            } catch (RuntimeException e2) {
                throw new SQLException("column " + col + " is not a timestamp: " + s);
            }
        }
    }

    @Override
    public java.io.InputStream getAsciiStream(int col) throws SQLException {
        String s = getString(col);
        return s == null ? null : new java.io.ByteArrayInputStream(
                s.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
    }

    @Override
    public java.io.InputStream getUnicodeStream(int col) throws SQLException {
        String s = getString(col);
        return s == null ? null : new java.io.ByteArrayInputStream(
                s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    @Override
    public java.io.InputStream getBinaryStream(int col) throws SQLException {
        byte[] b = getBytes(col);
        return b == null ? null : new java.io.ByteArrayInputStream(b);
    }

    @Override
    public java.io.Reader getCharacterStream(int col) throws SQLException {
        String s = getString(col);
        return s == null ? null : new java.io.StringReader(s);
    }

    @Override
    public Object getObject(int col) throws SQLException {
        return cell(col);
    }

    @Override
    public <T> T getObject(int col, Class<T> type) throws SQLException {
        Object v = cell(col);
        if (v == null) {
            return null;
        }
        if (type.isInstance(v)) {
            return type.cast(v);
        }
        if (type == String.class) {
            return type.cast(getString(col));
        }
        if (type == BigDecimal.class) {
            return type.cast(getBigDecimal(col));
        }
        if (type == Boolean.class) {
            return type.cast(getBoolean(col));
        }
        if (type == Short.class) {
            return type.cast((short) getInt(col));
        }
        if (type == Integer.class) {
            return type.cast(getInt(col));
        }
        if (type == Long.class) {
            return type.cast(getLong(col));
        }
        if (type == Float.class) {
            return type.cast(getFloat(col));
        }
        if (type == Double.class) {
            return type.cast(getDouble(col));
        }
        if (type == byte[].class) {
            return type.cast(getBytes(col));
        }
        if (type == java.sql.Timestamp.class) {
            return type.cast(getTimestamp(col));
        }
        if (type == java.sql.Date.class) {
            return type.cast(getDate(col));
        }
        if (type == java.sql.Time.class) {
            return type.cast(getTime(col));
        }
        if (type == java.time.LocalDateTime.class) {
            java.sql.Timestamp ts = getTimestamp(col);
            return ts == null ? null : type.cast(ts.toLocalDateTime());
        }
        if (type == java.time.Instant.class) {
            java.sql.Timestamp ts = getTimestamp(col);
            return ts == null ? null : type.cast(ts.toInstant());
        }
        throw JdbcSql.unsupported("getObject conversion to " + type.getName());
    }

    @Override
    public int findColumn(String name) throws SQLException {
        checkOpen();
        for (int i = 0; i < r.columns.size(); i++) {
            if (r.columns.get(i).equals(name)) {
                return i + 1;
            }
        }
        for (int i = 0; i < r.columns.size(); i++) {
            if (r.columns.get(i).equalsIgnoreCase(name)) {
                return i + 1;
            }
        }
        throw new SQLException("no such column: " + name);
    }

    // -- scrollable conveniences (rows are materialized) -----------------------

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkOpen();
        return pos < 0 && !r.rows.isEmpty();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkOpen();
        return pos >= r.rows.size();
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkOpen();
        return pos == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkOpen();
        return pos == r.rows.size() - 1 && r.rows.size() > 0;
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkOpen();
        pos = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        checkOpen();
        pos = r.rows.size();
    }

    @Override
    public boolean first() throws SQLException {
        checkOpen();
        if (r.rows.isEmpty()) {
            return false;
        }
        pos = 0;
        return true;
    }

    @Override
    public boolean last() throws SQLException {
        checkOpen();
        if (r.rows.isEmpty()) {
            return false;
        }
        pos = r.rows.size() - 1;
        return true;
    }

    @Override
    public int getRow() throws SQLException {
        checkOpen();
        return pos >= 0 && pos < r.rows.size() ? pos + 1 : 0;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkOpen();
        if (r.rows.isEmpty()) {
            return false;
        }
        if (row < 0) {
            row = r.rows.size() + 1 + row;
        }
        if (row <= 0 || row > r.rows.size()) {
            pos = r.rows.size();
            return false;
        }
        pos = row - 1;
        return true;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkOpen();
        int target = pos + rows;
        if (target < -1) {
            target = -1;
        }
        if (target > r.rows.size()) {
            target = r.rows.size();
        }
        pos = target;
        return pos >= 0 && pos < r.rows.size();
    }

    @Override
    public boolean previous() throws SQLException {
        checkOpen();
        if (pos > 0) {
            pos--;
            return true;
        }
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkOpen();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkOpen();
        return java.sql.ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkOpen();
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkOpen();
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        checkOpen();
        return java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkOpen();
        return java.sql.ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getHoldability() throws SQLException {
        checkOpen();
        return java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public java.sql.ResultSetMetaData getMetaData() throws SQLException {
        checkOpen();
        return new JdbcResultSetMetaData(r);
    }

    @Override
    public java.sql.SQLWarning getWarnings() throws SQLException {
        checkOpen();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkOpen();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        checkOpen();
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        checkOpen();
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        checkOpen();
        return false;
    }

    @Override
    public void insertRow() throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateRow() throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void refreshRow() throws SQLException {
        checkOpen();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        checkOpen();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        checkOpen();
    }

    @Override
    public java.sql.Statement getStatement() throws SQLException {
        checkOpen();
        return stmt;
    }

    @Override
    public String getCursorName() throws SQLException {
        throw JdbcSql.unsupported("positioned updates are not supported");
    }

    @Override
    public java.sql.Ref getRef(int col) throws SQLException {
        throw JdbcSql.unsupported("Ref is not supported");
    }

    @Override
    public java.sql.Blob getBlob(int col) throws SQLException {
        throw JdbcSql.unsupported("LOB locators are not supported; use getBytes");
    }

    @Override
    public java.sql.Clob getClob(int col) throws SQLException {
        throw JdbcSql.unsupported("LOB locators are not supported; use getString");
    }

    @Override
    public java.sql.Array getArray(int col) throws SQLException {
        throw JdbcSql.unsupported("Array is not supported");
    }

    @Override
    public java.sql.SQLXML getSQLXML(int col) throws SQLException {
        throw JdbcSql.unsupported("SQLXML is not supported");
    }

    @Override
    public java.sql.SQLXML getSQLXML(String name) throws SQLException {
        throw JdbcSql.unsupported("SQLXML is not supported");
    }

    @Override
    public java.sql.RowId getRowId(int col) throws SQLException {
        throw JdbcSql.unsupported("RowId is not supported");
    }

    @Override
    public java.net.URL getURL(int col) throws SQLException {
        String s = getString(col);
        try {
            return s == null ? null : new java.net.URL(s);
        } catch (java.net.MalformedURLException e) {
            throw new SQLException("column " + col + " is not a URL: " + s);
        }
    }

    @Override
    public java.net.URL getURL(String name) throws SQLException {
        return getURL(findColumn(name));
    }

    @Override
    public java.sql.NClob getNClob(int col) throws SQLException {
        throw JdbcSql.unsupported("NClob is not supported");
    }

    @Override
    public java.sql.NClob getNClob(String name) throws SQLException {
        throw JdbcSql.unsupported("NClob is not supported");
    }

    @Override
    public java.sql.RowId getRowId(String name) throws SQLException {
        throw JdbcSql.unsupported("RowId is not supported");
    }

    @Override
    public java.sql.Date getDate(int col, java.util.Calendar cal) throws SQLException {
        return getDate(col);
    }

    @Override
    public java.sql.Time getTime(int col, java.util.Calendar cal) throws SQLException {
        return getTime(col);
    }

    @Override
    public java.sql.Timestamp getTimestamp(int col, java.util.Calendar cal) throws SQLException {
        return getTimestamp(col);
    }

    @Override
    public String getNString(int col) throws SQLException {
        return getString(col);
    }

    @Override
    public java.io.Reader getNCharacterStream(int col) throws SQLException {
        return getCharacterStream(col);
    }

    @Override
    public void updateNull(int col) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateBoolean(int col, boolean x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateByte(int col, byte x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateShort(int col, short x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateInt(int col, int x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateLong(int col, long x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateDouble(int col, double x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateBigDecimal(int col, BigDecimal x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateString(int col, String x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateBytes(int col, byte[] x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateDate(int col, java.sql.Date x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateTime(int col, java.sql.Time x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateTimestamp(int col, java.sql.Timestamp x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateAsciiStream(int col, java.io.InputStream x, int length) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateAsciiStream(int col, java.io.InputStream x, long length) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateAsciiStream(int col, java.io.InputStream x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateBinaryStream(int col, java.io.InputStream x, int length) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateBinaryStream(int col, java.io.InputStream x, long length) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateBinaryStream(int col, java.io.InputStream x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateCharacterStream(int col, java.io.Reader x, int length) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateCharacterStream(int col, java.io.Reader x, long length) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateCharacterStream(int col, java.io.Reader x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateObject(int col, Object x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateObject(String name, Object x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateObject(String name, Object x, int scaleOrLength) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateObject(String name, Object x, java.sql.SQLType type) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateObject(String name, Object x, java.sql.SQLType type, int scaleOrLength)
            throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateObject(int col, Object x, int scaleOrLength) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateObject(int col, Object x, java.sql.SQLType type) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateObject(int col, Object x, java.sql.SQLType type, int scaleOrLength)
            throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateBlob(int col, java.sql.Blob x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateBlob(int col, java.io.InputStream x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateBlob(int col, java.io.InputStream x, long length) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateBlob(String name, java.sql.Blob x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateBlob(String name, java.io.InputStream x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateBlob(String name, java.io.InputStream x, long length) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateAsciiStream(String name, java.io.InputStream x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateAsciiStream(String name, java.io.InputStream x, int length) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateAsciiStream(String name, java.io.InputStream x, long length) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateBinaryStream(String name, java.io.InputStream x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateBinaryStream(String name, java.io.InputStream x, int length) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateBinaryStream(String name, java.io.InputStream x, long length) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateCharacterStream(String name, java.io.Reader x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateCharacterStream(String name, java.io.Reader x, int length) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateCharacterStream(String name, java.io.Reader x, long length) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateNCharacterStream(String name, java.io.Reader x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateNCharacterStream(String name, java.io.Reader x, long length) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateNCharacterStream(int col, java.io.Reader x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateNCharacterStream(int col, java.io.Reader x, long length) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateClob(int col, java.sql.Clob x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateClob(int col, java.io.Reader x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateClob(int col, java.io.Reader x, long length) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateClob(String name, java.sql.Clob x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateClob(String name, java.io.Reader x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateClob(String name, java.io.Reader x, long length) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateArray(int col, java.sql.Array x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateArray(String name, java.sql.Array x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateRef(int col, java.sql.Ref x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateRef(String name, java.sql.Ref x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateNString(int col, String x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateNString(String name, String x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateNClob(int col, java.sql.NClob x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateNClob(int col, java.io.Reader x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateNClob(int col, java.io.Reader x, long length) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateNClob(String name, java.sql.NClob x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateNClob(String name, java.io.Reader x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateNClob(String name, java.io.Reader x, long length) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateRowId(int col, java.sql.RowId x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateRowId(String name, java.sql.RowId x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateSQLXML(int col, java.sql.SQLXML x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateSQLXML(String name, java.sql.SQLXML x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public String getNString(String name) throws SQLException {
        return getNString(findColumn(name));
    }

    @Override
    public java.io.Reader getNCharacterStream(String name) throws SQLException {
        return getCharacterStream(findColumn(name));
    }

    @Override
    public <T> T getObject(String name, Class<T> type) throws SQLException {
        return getObject(findColumn(name), type);
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
    @Override
    public String getString(String name) throws SQLException {
        return getString(findColumn(name));
    }

    @Override
    public boolean getBoolean(String name) throws SQLException {
        return getBoolean(findColumn(name));
    }

    @Override
    public byte getByte(String name) throws SQLException {
        return getByte(findColumn(name));
    }

    @Override
    public short getShort(String name) throws SQLException {
        return getShort(findColumn(name));
    }

    @Override
    public int getInt(String name) throws SQLException {
        return getInt(findColumn(name));
    }

    @Override
    public long getLong(String name) throws SQLException {
        return getLong(findColumn(name));
    }

    @Override
    public float getFloat(String name) throws SQLException {
        return getFloat(findColumn(name));
    }

    @Override
    public double getDouble(String name) throws SQLException {
        return getDouble(findColumn(name));
    }

    @Override
    public BigDecimal getBigDecimal(String name, int scale) throws SQLException {
        return getBigDecimal(findColumn(name), scale);
    }

    @Override
    public BigDecimal getBigDecimal(String name) throws SQLException {
        return getBigDecimal(findColumn(name));
    }

    @Override
    public byte[] getBytes(String name) throws SQLException {
        return getBytes(findColumn(name));
    }

    @Override
    public java.sql.Date getDate(String name) throws SQLException {
        return getDate(findColumn(name));
    }

    @Override
    public java.sql.Time getTime(String name) throws SQLException {
        return getTime(findColumn(name));
    }

    @Override
    public java.sql.Timestamp getTimestamp(String name) throws SQLException {
        return getTimestamp(findColumn(name));
    }

    @Override
    public java.io.InputStream getAsciiStream(String name) throws SQLException {
        return getAsciiStream(findColumn(name));
    }

    @Override
    public java.io.InputStream getUnicodeStream(String name) throws SQLException {
        return getUnicodeStream(findColumn(name));
    }

    @Override
    public java.io.InputStream getBinaryStream(String name) throws SQLException {
        return getBinaryStream(findColumn(name));
    }

    @Override
    public java.io.Reader getCharacterStream(String name) throws SQLException {
        return getCharacterStream(findColumn(name));
    }

    @Override
    public Object getObject(String name) throws SQLException {
        return cell(findColumn(name));
    }

    @Override
    public java.sql.Date getDate(String name, java.util.Calendar cal) throws SQLException {
        return getDate(findColumn(name), cal);
    }

    @Override
    public java.sql.Time getTime(String name, java.util.Calendar cal) throws SQLException {
        return getTime(findColumn(name), cal);
    }

    @Override
    public java.sql.Timestamp getTimestamp(String name, java.util.Calendar cal) throws SQLException {
        return getTimestamp(findColumn(name), cal);
    }

    @Override
    public java.sql.Ref getRef(String name) throws SQLException {
        throw JdbcSql.unsupported("Ref is not supported");
    }

    @Override
    public java.sql.Blob getBlob(String name) throws SQLException {
        throw JdbcSql.unsupported("LOB locators are not supported; use getBytes");
    }

    @Override
    public java.sql.Clob getClob(String name) throws SQLException {
        throw JdbcSql.unsupported("LOB locators are not supported; use getString");
    }

    @Override
    public java.sql.Array getArray(String name) throws SQLException {
        throw JdbcSql.unsupported("Array is not supported");
    }

    @Override
    public Object getObject(String name, java.util.Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(name));
    }

    @Override
    public Object getObject(int col, java.util.Map<String, Class<?>> map) throws SQLException {
        return getObject(col);
    }

    // -- by-name update variants (all unsupported: read-only result sets) -----

    @Override
    public void updateNull(String name) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateBoolean(String name, boolean x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateByte(String name, byte x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateShort(String name, short x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateInt(String name, int x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateLong(String name, long x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateFloat(String name, float x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateDouble(String name, double x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateBigDecimal(String name, BigDecimal x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateString(String name, String x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateBytes(String name, byte[] x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateDate(String name, java.sql.Date x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateTime(String name, java.sql.Time x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }

    @Override
    public void updateTimestamp(String name, java.sql.Timestamp x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }
    @Override
    public void updateFloat(int col, float x) throws SQLException {
        throw JdbcSql.unsupported("read-only result sets");
    }





}
