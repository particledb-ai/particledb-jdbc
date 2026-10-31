package org.particledb;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * A fixed, in-memory forward-only {@link ResultSet} used ONLY by
 * {@link PdbDatabaseMetaData} to return JDBC-spec-shaped metadata result sets
 * (getTables/getColumns/getPrimaryKeys/…). The column list + types are the
 * exact ones the JDBC spec mandates for each metadata method, so a caller that
 * reads them by the documented column name (Flyway, ORMs) sees what it expects
 * regardless of what the underlying PDB catalog query column names are.
 *
 * <p>Column values are read by the JDBC-spec column NAME (case-insensitive) or
 * by 1-based index. Types are coerced leniently: getInt/getShort parse a
 * numeric value; getString stringifies. Nulls report {@link #wasNull()}.
 */
final class MetaResultSet implements ResultSet {

    private final String[] columnNames;
    private final int[] columnJdbcTypes; // java.sql.Types.* per column
    private final List<Object[]> rows;
    private int cursor = -1;
    private boolean wasNull = false;
    private boolean closed = false;

    MetaResultSet(String[] columnNames, int[] columnJdbcTypes, List<Object[]> rows) {
        this.columnNames = columnNames;
        this.columnJdbcTypes = columnJdbcTypes;
        this.rows = rows;
    }

    private Object[] row() throws SQLException {
        if (cursor < 0 || cursor >= rows.size()) throw new SQLException("cursor out of range");
        return rows.get(cursor);
    }

    private Object val(int idx) throws SQLException {
        if (idx < 1 || idx > columnNames.length) {
            throw new SQLException("column index out of range: " + idx);
        }
        Object[] r = row();
        Object v = (idx - 1) < r.length ? r[idx - 1] : null;
        wasNull = (v == null);
        return v;
    }

    private Object val(String name) throws SQLException {
        return val(findColumn(name));
    }

    @Override public int findColumn(String name) throws SQLException {
        for (int i = 0; i < columnNames.length; i++) {
            if (columnNames[i].equalsIgnoreCase(name)) return i + 1;
        }
        throw new SQLException("column not found: " + name);
    }

    @Override public boolean next() { cursor++; return cursor < rows.size(); }
    @Override public void close() { closed = true; }
    @Override public boolean isClosed() { return closed; }
    @Override public boolean wasNull() { return wasNull; }

    @Override public String getString(int c) throws SQLException {
        Object v = val(c); return v == null ? null : v.toString();
    }
    @Override public String getString(String c) throws SQLException {
        Object v = val(c); return v == null ? null : v.toString();
    }
    @Override public boolean getBoolean(int c) throws SQLException {
        Object v = val(c);
        if (v == null) return false;
        if (v instanceof Boolean) return (Boolean) v;
        if (v instanceof Number) return ((Number) v).longValue() != 0;
        return Boolean.parseBoolean(v.toString());
    }
    @Override public boolean getBoolean(String c) throws SQLException { return getBoolean(findColumn(c)); }
    @Override public byte getByte(int c) throws SQLException { return (byte) getLong(c); }
    @Override public byte getByte(String c) throws SQLException { return (byte) getLong(c); }
    @Override public short getShort(int c) throws SQLException { return (short) getLong(c); }
    @Override public short getShort(String c) throws SQLException { return (short) getLong(c); }
    @Override public int getInt(int c) throws SQLException { return (int) getLong(c); }
    @Override public int getInt(String c) throws SQLException { return (int) getLong(c); }
    @Override public long getLong(int c) throws SQLException {
        Object v = val(c);
        if (v == null) return 0L;
        if (v instanceof Number) return ((Number) v).longValue();
        String s = v.toString().trim();
        if (s.isEmpty()) return 0L;
        return Long.parseLong(s);
    }
    @Override public long getLong(String c) throws SQLException { return getLong(findColumn(c)); }
    @Override public float getFloat(int c) throws SQLException { return (float) getDouble(c); }
    @Override public float getFloat(String c) throws SQLException { return (float) getDouble(c); }
    @Override public double getDouble(int c) throws SQLException {
        Object v = val(c);
        if (v == null) return 0.0;
        if (v instanceof Number) return ((Number) v).doubleValue();
        return Double.parseDouble(v.toString().trim());
    }
    @Override public double getDouble(String c) throws SQLException { return getDouble(findColumn(c)); }
    @Override public Object getObject(int c) throws SQLException { return val(c); }
    @Override public Object getObject(String c) throws SQLException { return val(c); }

    @Override public ResultSetMetaData getMetaData() {
        return new MetaMeta(columnNames, columnJdbcTypes);
    }

    // ── ResultSet boilerplate ────────────────────────────────────────────────
    @Override public SQLWarning getWarnings() { return null; }
    @Override public void clearWarnings() {}
    @Override public String getCursorName() { return null; }
    @Override public boolean isBeforeFirst() { return cursor < 0; }
    @Override public boolean isAfterLast() { return cursor >= rows.size(); }
    @Override public boolean isFirst() { return cursor == 0; }
    @Override public boolean isLast() { return cursor == rows.size() - 1; }
    @Override public void beforeFirst() { cursor = -1; }
    @Override public void afterLast() { cursor = rows.size(); }
    @Override public boolean first() { cursor = 0; return !rows.isEmpty(); }
    @Override public boolean last() { cursor = rows.size() - 1; return !rows.isEmpty(); }
    @Override public int getRow() { return cursor + 1; }
    @Override public boolean absolute(int r) { cursor = r - 1; return cursor >= 0 && cursor < rows.size(); }
    @Override public boolean relative(int r) { cursor += r; return cursor >= 0 && cursor < rows.size(); }
    @Override public boolean previous() { cursor--; return cursor >= 0; }
    @Override public void setFetchDirection(int d) {}
    @Override public int getFetchDirection() { return FETCH_FORWARD; }
    @Override public void setFetchSize(int r) {}
    @Override public int getFetchSize() { return 0; }
    @Override public int getType() { return TYPE_FORWARD_ONLY; }
    @Override public int getConcurrency() { return CONCUR_READ_ONLY; }
    @Override public int getHoldability() { return 1; }
    @Override public Statement getStatement() { return null; }

    @Override public BigDecimal getBigDecimal(int c) throws SQLException {
        Object v = val(c); if (v == null) return null;
        if (v instanceof BigDecimal) return (BigDecimal) v;
        return new BigDecimal(v.toString());
    }
    @Override public BigDecimal getBigDecimal(String c) throws SQLException { return getBigDecimal(findColumn(c)); }
    @Override public BigDecimal getBigDecimal(int c, int s) throws SQLException {
        BigDecimal d = getBigDecimal(c); return d == null ? null : d.setScale(s, java.math.RoundingMode.HALF_UP);
    }
    @Override public BigDecimal getBigDecimal(String c, int s) throws SQLException { return getBigDecimal(findColumn(c), s); }
    @Override public byte[] getBytes(int c) throws SQLException {
        Object v = val(c);
        if (v == null) return null;
        if (v instanceof byte[]) return (byte[]) v;
        return v.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
    @Override public byte[] getBytes(String c) throws SQLException { return getBytes(findColumn(c)); }

    // ── Unsupported reads / all mutators ─────────────────────────────────────
    @Override public Date getDate(int c) throws SQLException { Object v = val(c); return v == null ? null : Date.valueOf(v.toString()); }
    @Override public Date getDate(String c) throws SQLException { return getDate(findColumn(c)); }
    @Override public Time getTime(int c) throws SQLException { Object v = val(c); return v == null ? null : Time.valueOf(v.toString()); }
    @Override public Time getTime(String c) throws SQLException { return getTime(findColumn(c)); }
    @Override public Timestamp getTimestamp(int c) throws SQLException { Object v = val(c); return v == null ? null : Timestamp.valueOf(v.toString()); }
    @Override public Timestamp getTimestamp(String c) throws SQLException { return getTimestamp(findColumn(c)); }
    @Override public Date getDate(int c, Calendar cal) throws SQLException { return getDate(c); }
    @Override public Date getDate(String c, Calendar cal) throws SQLException { return getDate(c); }
    @Override public Time getTime(int c, Calendar cal) throws SQLException { return getTime(c); }
    @Override public Time getTime(String c, Calendar cal) throws SQLException { return getTime(c); }
    @Override public Timestamp getTimestamp(int c, Calendar cal) throws SQLException { return getTimestamp(c); }
    @Override public Timestamp getTimestamp(String c, Calendar cal) throws SQLException { return getTimestamp(c); }
    @Override public Object getObject(int c, Map<String, Class<?>> m) throws SQLException { return getObject(c); }
    @Override public Object getObject(String c, Map<String, Class<?>> m) throws SQLException { return getObject(c); }
    @Override public <T> T getObject(int c, Class<T> type) throws SQLException {
        Object v = val(c);
        if (v == null) return null;
        if (type.isInstance(v)) return type.cast(v);
        if (type == String.class) return type.cast(v.toString());
        if (type == Integer.class) return type.cast((int) getLong(c));
        if (type == Long.class) return type.cast(getLong(c));
        if (type == Boolean.class) return type.cast(getBoolean(c));
        throw new SQLException("cannot convert to " + type);
    }
    @Override public <T> T getObject(String c, Class<T> type) throws SQLException { return getObject(findColumn(c), type); }

    private static SQLFeatureNotSupportedException nys() {
        return new SQLFeatureNotSupportedException("metadata result set: not supported");
    }
    @Override public InputStream getAsciiStream(int c) throws SQLException { throw nys(); }
    @Override public InputStream getAsciiStream(String c) throws SQLException { throw nys(); }
    @Override public InputStream getUnicodeStream(int c) throws SQLException { throw nys(); }
    @Override public InputStream getUnicodeStream(String c) throws SQLException { throw nys(); }
    @Override public InputStream getBinaryStream(int c) throws SQLException { throw nys(); }
    @Override public InputStream getBinaryStream(String c) throws SQLException { throw nys(); }
    @Override public Reader getCharacterStream(int c) throws SQLException { throw nys(); }
    @Override public Reader getCharacterStream(String c) throws SQLException { throw nys(); }
    @Override public Ref getRef(int c) throws SQLException { throw nys(); }
    @Override public Ref getRef(String c) throws SQLException { throw nys(); }
    @Override public Blob getBlob(int c) throws SQLException { throw nys(); }
    @Override public Blob getBlob(String c) throws SQLException { throw nys(); }
    @Override public Clob getClob(int c) throws SQLException { throw nys(); }
    @Override public Clob getClob(String c) throws SQLException { throw nys(); }
    @Override public Array getArray(int c) throws SQLException { throw nys(); }
    @Override public Array getArray(String c) throws SQLException { throw nys(); }
    @Override public URL getURL(int c) throws SQLException { throw nys(); }
    @Override public URL getURL(String c) throws SQLException { throw nys(); }
    @Override public RowId getRowId(int c) throws SQLException { throw nys(); }
    @Override public RowId getRowId(String c) throws SQLException { throw nys(); }
    @Override public NClob getNClob(int c) throws SQLException { throw nys(); }
    @Override public NClob getNClob(String c) throws SQLException { throw nys(); }
    @Override public SQLXML getSQLXML(int c) throws SQLException { throw nys(); }
    @Override public SQLXML getSQLXML(String c) throws SQLException { throw nys(); }
    @Override public String getNString(int c) throws SQLException { return getString(c); }
    @Override public String getNString(String c) throws SQLException { return getString(c); }
    @Override public Reader getNCharacterStream(int c) throws SQLException { throw nys(); }
    @Override public Reader getNCharacterStream(String c) throws SQLException { throw nys(); }

    @Override public boolean rowUpdated() { return false; }
    @Override public boolean rowInserted() { return false; }
    @Override public boolean rowDeleted() { return false; }
    @Override public void insertRow() throws SQLException { throw nys(); }
    @Override public void updateRow() throws SQLException { throw nys(); }
    @Override public void deleteRow() throws SQLException { throw nys(); }
    @Override public void refreshRow() throws SQLException { throw nys(); }
    @Override public void cancelRowUpdates() throws SQLException { throw nys(); }
    @Override public void moveToInsertRow() throws SQLException { throw nys(); }
    @Override public void moveToCurrentRow() throws SQLException { throw nys(); }

    // update* - all unsupported (read-only metadata)
    @Override public void updateNull(int c) throws SQLException { throw nys(); }
    @Override public void updateNull(String c) throws SQLException { throw nys(); }
    @Override public void updateBoolean(int c, boolean x) throws SQLException { throw nys(); }
    @Override public void updateBoolean(String c, boolean x) throws SQLException { throw nys(); }
    @Override public void updateByte(int c, byte x) throws SQLException { throw nys(); }
    @Override public void updateByte(String c, byte x) throws SQLException { throw nys(); }
    @Override public void updateShort(int c, short x) throws SQLException { throw nys(); }
    @Override public void updateShort(String c, short x) throws SQLException { throw nys(); }
    @Override public void updateInt(int c, int x) throws SQLException { throw nys(); }
    @Override public void updateInt(String c, int x) throws SQLException { throw nys(); }
    @Override public void updateLong(int c, long x) throws SQLException { throw nys(); }
    @Override public void updateLong(String c, long x) throws SQLException { throw nys(); }
    @Override public void updateFloat(int c, float x) throws SQLException { throw nys(); }
    @Override public void updateFloat(String c, float x) throws SQLException { throw nys(); }
    @Override public void updateDouble(int c, double x) throws SQLException { throw nys(); }
    @Override public void updateDouble(String c, double x) throws SQLException { throw nys(); }
    @Override public void updateBigDecimal(int c, BigDecimal x) throws SQLException { throw nys(); }
    @Override public void updateBigDecimal(String c, BigDecimal x) throws SQLException { throw nys(); }
    @Override public void updateString(int c, String x) throws SQLException { throw nys(); }
    @Override public void updateString(String c, String x) throws SQLException { throw nys(); }
    @Override public void updateBytes(int c, byte[] x) throws SQLException { throw nys(); }
    @Override public void updateBytes(String c, byte[] x) throws SQLException { throw nys(); }
    @Override public void updateDate(int c, Date x) throws SQLException { throw nys(); }
    @Override public void updateDate(String c, Date x) throws SQLException { throw nys(); }
    @Override public void updateTime(int c, Time x) throws SQLException { throw nys(); }
    @Override public void updateTime(String c, Time x) throws SQLException { throw nys(); }
    @Override public void updateTimestamp(int c, Timestamp x) throws SQLException { throw nys(); }
    @Override public void updateTimestamp(String c, Timestamp x) throws SQLException { throw nys(); }
    @Override public void updateAsciiStream(int c, InputStream x, int l) throws SQLException { throw nys(); }
    @Override public void updateAsciiStream(String c, InputStream x, int l) throws SQLException { throw nys(); }
    @Override public void updateBinaryStream(int c, InputStream x, int l) throws SQLException { throw nys(); }
    @Override public void updateBinaryStream(String c, InputStream x, int l) throws SQLException { throw nys(); }
    @Override public void updateCharacterStream(int c, Reader x, int l) throws SQLException { throw nys(); }
    @Override public void updateCharacterStream(String c, Reader x, int l) throws SQLException { throw nys(); }
    @Override public void updateObject(int c, Object x, int s) throws SQLException { throw nys(); }
    @Override public void updateObject(String c, Object x, int s) throws SQLException { throw nys(); }
    @Override public void updateObject(int c, Object x) throws SQLException { throw nys(); }
    @Override public void updateObject(String c, Object x) throws SQLException { throw nys(); }
    @Override public void updateRef(int c, Ref x) throws SQLException { throw nys(); }
    @Override public void updateRef(String c, Ref x) throws SQLException { throw nys(); }
    @Override public void updateBlob(int c, Blob x) throws SQLException { throw nys(); }
    @Override public void updateBlob(String c, Blob x) throws SQLException { throw nys(); }
    @Override public void updateClob(int c, Clob x) throws SQLException { throw nys(); }
    @Override public void updateClob(String c, Clob x) throws SQLException { throw nys(); }
    @Override public void updateArray(int c, Array x) throws SQLException { throw nys(); }
    @Override public void updateArray(String c, Array x) throws SQLException { throw nys(); }
    @Override public void updateRowId(int c, RowId x) throws SQLException { throw nys(); }
    @Override public void updateRowId(String c, RowId x) throws SQLException { throw nys(); }
    @Override public void updateNString(int c, String x) throws SQLException { throw nys(); }
    @Override public void updateNString(String c, String x) throws SQLException { throw nys(); }
    @Override public void updateNClob(int c, NClob x) throws SQLException { throw nys(); }
    @Override public void updateNClob(String c, NClob x) throws SQLException { throw nys(); }
    @Override public void updateSQLXML(int c, SQLXML x) throws SQLException { throw nys(); }
    @Override public void updateSQLXML(String c, SQLXML x) throws SQLException { throw nys(); }
    @Override public void updateNCharacterStream(int c, Reader x, long l) throws SQLException { throw nys(); }
    @Override public void updateNCharacterStream(String c, Reader x, long l) throws SQLException { throw nys(); }
    @Override public void updateAsciiStream(int c, InputStream x, long l) throws SQLException { throw nys(); }
    @Override public void updateAsciiStream(String c, InputStream x, long l) throws SQLException { throw nys(); }
    @Override public void updateBinaryStream(int c, InputStream x, long l) throws SQLException { throw nys(); }
    @Override public void updateBinaryStream(String c, InputStream x, long l) throws SQLException { throw nys(); }
    @Override public void updateCharacterStream(int c, Reader x, long l) throws SQLException { throw nys(); }
    @Override public void updateCharacterStream(String c, Reader x, long l) throws SQLException { throw nys(); }
    @Override public void updateBlob(int c, InputStream x, long l) throws SQLException { throw nys(); }
    @Override public void updateBlob(String c, InputStream x, long l) throws SQLException { throw nys(); }
    @Override public void updateClob(int c, Reader x, long l) throws SQLException { throw nys(); }
    @Override public void updateClob(String c, Reader x, long l) throws SQLException { throw nys(); }
    @Override public void updateNClob(int c, Reader x, long l) throws SQLException { throw nys(); }
    @Override public void updateNClob(String c, Reader x, long l) throws SQLException { throw nys(); }
    @Override public void updateNCharacterStream(int c, Reader x) throws SQLException { throw nys(); }
    @Override public void updateNCharacterStream(String c, Reader x) throws SQLException { throw nys(); }
    @Override public void updateAsciiStream(int c, InputStream x) throws SQLException { throw nys(); }
    @Override public void updateAsciiStream(String c, InputStream x) throws SQLException { throw nys(); }
    @Override public void updateBinaryStream(int c, InputStream x) throws SQLException { throw nys(); }
    @Override public void updateBinaryStream(String c, InputStream x) throws SQLException { throw nys(); }
    @Override public void updateCharacterStream(int c, Reader x) throws SQLException { throw nys(); }
    @Override public void updateCharacterStream(String c, Reader x) throws SQLException { throw nys(); }
    @Override public void updateBlob(int c, InputStream x) throws SQLException { throw nys(); }
    @Override public void updateBlob(String c, InputStream x) throws SQLException { throw nys(); }
    @Override public void updateClob(int c, Reader x) throws SQLException { throw nys(); }
    @Override public void updateClob(String c, Reader x) throws SQLException { throw nys(); }
    @Override public void updateNClob(int c, Reader x) throws SQLException { throw nys(); }
    @Override public void updateNClob(String c, Reader x) throws SQLException { throw nys(); }

    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("not a wrapper for " + iface);
    }
    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }

    /** Metadata for a MetaResultSet: fixed column names + JDBC types. */
    static final class MetaMeta implements ResultSetMetaData {
        private final String[] names;
        private final int[] types;
        MetaMeta(String[] names, int[] types) { this.names = names; this.types = types; }
        @Override public int getColumnCount() { return names.length; }
        @Override public boolean isAutoIncrement(int c) { return false; }
        @Override public boolean isCaseSensitive(int c) { return true; }
        @Override public boolean isSearchable(int c) { return true; }
        @Override public boolean isCurrency(int c) { return false; }
        @Override public int isNullable(int c) { return columnNullableUnknown; }
        @Override public boolean isSigned(int c) { return true; }
        @Override public int getColumnDisplaySize(int c) { return 0; }
        @Override public String getColumnLabel(int c) { return names[c - 1]; }
        @Override public String getColumnName(int c) { return names[c - 1]; }
        @Override public String getSchemaName(int c) { return ""; }
        @Override public int getPrecision(int c) { return 0; }
        @Override public int getScale(int c) { return 0; }
        @Override public String getTableName(int c) { return ""; }
        @Override public String getCatalogName(int c) { return ""; }
        @Override public int getColumnType(int c) { return types[c - 1]; }
        @Override public String getColumnTypeName(int c) {
            return types[c - 1] == java.sql.Types.INTEGER ? "int4" : "text";
        }
        @Override public boolean isReadOnly(int c) { return true; }
        @Override public boolean isWritable(int c) { return false; }
        @Override public boolean isDefinitelyWritable(int c) { return false; }
        @Override public String getColumnClassName(int c) {
            return types[c - 1] == java.sql.Types.INTEGER
                    ? Integer.class.getName() : String.class.getName();
        }
        @Override public <T> T unwrap(Class<T> iface) throws SQLException {
            if (iface.isInstance(this)) return iface.cast(this);
            throw new SQLException("not a wrapper for " + iface);
        }
        @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }
    }
}
