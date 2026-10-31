package org.particledb;

import org.particledb.wire.StreamState;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * JDBC {@link ResultSet} backed by an Arrow IPC stream payload.
 *
 * <p>Decoded via {@link ArrowDecoder}. Apache Arrow Java
 * ({@code org.apache.arrow:arrow-vector} +
 * {@code org.apache.arrow:arrow-memory-netty}) MUST be on the classpath
 * for any SELECT that returns rows. {@code ArrowDecoder.decode} throws
 * with an actionable error if the optional deps or JVM open flags are
 * missing - the result set construction does NOT silently fall back to
 * raw bytes (that fallback was removed in 1.0.0).
 *
 * <p>For pure DML responses (no Arrow payload), this collapses to an
 * empty result.
 */
public final class PdbResultSet implements ResultSet {

    // Rows are stored POSITIONALLY (Object[] per row) with a parallel
    // column-name list, so getX(int) indexes by projection position and
    // duplicate output labels stay individually addressable (finding F18).
    private List<Object[]> rows;
    /**
     * A STREAMED result ({@code setFetchSize > 0}): rows arrive one record
     * batch at a time, {@link #rows} holds only the current batch, and the
     * result set is forward-only. Null for an ordinary (whole) result.
     */
    private StreamedRows streamed;
    /** Rows of the batches before the current one (streamed results). */
    private long delivered;
    /** The streamed result has no more batches. */
    private boolean exhausted;
    private final List<String> columnNames;
    // Per-column Arrow logical type token (see ArrowDecoder.arrowTypeToken),
    // parallel to columnNames. Drives ResultSetMetaData's JDBC type contract.
    private final List<String> columnTypes;
    // Per-column Arrow field custom metadata, parallel to columnNames. Carries
    // the engine's `pdb:decl` declared-type stamp, which is the only element-type
    // channel for an ARRAY column (shipped as Arrow Utf8 holding ARRAY[…] text).
    private final List<Map<String, String>> columnMetadata;
    private int cursor = -1;
    private boolean closed = false;
    private boolean wasNull = false;
    private Statement owner;
    /**
     * {@code Statement.setMaxRows} cap, 0 = unlimited. JDBC requires rows past
     * the limit to be DROPPED, so the cap is applied here, at the one place
     * that knows how many rows there are.
     */
    private int maxRows = 0;

    /**
     * Arrow field-metadata key under which the engine stamps a column's DECLARED
     * SQL type ({@code INT[]}, {@code TEXT[]}, {@code uuid}, {@code numeric}, …).
     * See the server's scan operator {@code field_with_decl}.
     */
    static final String DECL_KEY = "pdb:decl";

    /** The {@code pdb:decl} stamp for a 1-based column, or null when absent. */
    static String declFrom(List<Map<String, String>> md, int oneBasedIdx) {
        int i = oneBasedIdx - 1;
        if (md == null || i < 0 || i >= md.size()) return null;
        Map<String, String> m = md.get(i);
        return m == null ? null : m.get(DECL_KEY);
    }

    public PdbResultSet(StreamState.StreamResult r) {
        this(r, ArrowDecoder.ResultDecoder.AUTO);
    }

    /** A result set whose rows are decoded with {@code decoder} (the connection's {@code resultDecoder}). */
    PdbResultSet(StreamState.StreamResult r, ArrowDecoder.ResultDecoder decoder) {
        if (r != null && r.streamed instanceof StreamedRows) {
            this.streamed = (StreamedRows) r.streamed;
            ArrowDecoder.Decoded d = streamed.schema();
            this.rows = java.util.Collections.emptyList();
            this.columnNames = d.columnNames;
            this.columnTypes = d.columnTypes;
            this.columnMetadata = d.columnMetadata;
        } else if (r != null && r.kind == StreamState.StreamResult.Kind.ARROW) {
            // The Arrow stream carries the schema even when it has 0 record
            // batches, so decodePositional recovers the real projected column
            // names for an empty result set (finding F7) and keeps duplicate
            // labels distinct by position (finding F18).
            ArrowDecoder.Decoded d = ArrowDecoder.decodePositional(r.payload, decoder);
            this.rows = d.rows;
            List<String> names = d.columnNames;
            // Defensive fallback: if the positional decode somehow yielded rows
            // but no schema names, recover them via the schema-only reader.
            if (names.isEmpty() && r.payload != null && r.payload.length > 0) {
                names = ArrowDecoder.decodeColumnNames(r.payload);
            }
            this.columnNames = names;
            this.columnTypes = d.columnTypes;
            this.columnMetadata = d.columnMetadata;
        } else {
            this.rows = Collections.emptyList();
            this.columnNames = Collections.emptyList();
            this.columnTypes = Collections.emptyList();
            this.columnMetadata = Collections.emptyList();
        }
    }

    public static PdbResultSet empty() {
        return new PdbResultSet(StreamState.StreamResult.ok(new byte[0]));
    }

    public void setOwner(Statement s) { this.owner = s; }

    /** Apply the owning Statement's {@code maxRows} cap (0 = unlimited). */
    void setMaxRows(int max) { this.maxRows = Math.max(max, 0); }

    /** Visible row count after the {@code maxRows} cap. */
    private int rowCount() {
        return (maxRows > 0 && maxRows < rows.size()) ? maxRows : rows.size();
    }

    private Object[] row() throws SQLException {
        if (cursor < 0 || cursor >= rowCount()) {
            throw new SQLException("cursor out of range");
        }
        return rows.get(cursor);
    }

    private Object getValue(int idx) throws SQLException {
        ensureOpen();
        if (idx < 1 || idx > columnNames.size()) {
            throw new SQLException("column index out of range: " + idx);
        }
        Object[] r = row();
        Object v = (idx - 1) < r.length ? r[idx - 1] : null;
        wasNull = (v == null);
        return v;
    }

    /**
     * 1-based index of the column a label resolves to, or 0 when absent.
     *
     * <p>JDBC column labels are case-INSENSITIVE and resolve to the FIRST
     * matching column (same rule as {@link #findColumn}), so a present value is
     * NEVER read as null merely because of identifier case (e.g. a quoted
     * {@code AS "Val"} projection read via {@code getString("val")}).
     */
    private int columnIndexOrZero(String name) {
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equalsIgnoreCase(name)) return i + 1;
        }
        return 0;
    }

    private Object getValue(String name) throws SQLException {
        ensureOpen();
        return getValue(findColumn(name));
    }

    /**
     * Coerce a decoded Arrow value to the requested JDBC type. Arrow hands us
     * the widest natural carrier (e.g. Long for any integer, Double for any
     * float), so a bare {@code type.cast(v)} would throw ClassCastException for
     * {@code getObject(i, Integer.class)}. Convert the common JDBC target types.
     */
    @SuppressWarnings("unchecked")
    private <T> T convert(Object v, Class<T> type) throws SQLException {
        if (type == null) throw new SQLException("target type must not be null");
        if (v == null) return null;
        if (type.isInstance(v)) return type.cast(v);
        refuseNonFinite(v, type);
        try {
            if (type == String.class)     return (T) v.toString();
            if (type == Boolean.class)    return (T) (Boolean) coerceBool(v);
            // Integer conversions are RANGE-CHECKED and truncate toward zero,
            // as getLong / getInt do: an out-of-range value throws 22003
            // instead of silently wrapping.
            if (type == Long.class)       return (T) (Long) toIntegral(v, "long");
            if (type == Integer.class)    return (T) (Integer) (int) toIntegral(v, "int");
            if (type == Short.class)      return (T) (Short) (short) toIntegral(v, "short");
            if (type == Byte.class)       return (T) (Byte) (byte) toIntegral(v, "byte");
            if (type == Float.class)      return (T) (Float) (float) coerceDouble(v);
            if (type == Double.class)     return (T) (Double) coerceDouble(v);
            if (type == BigDecimal.class)
                return (T) (v instanceof BigDecimal ? v : new BigDecimal(v.toString()));
        } catch (NumberFormatException nfe) {
            throw new SQLException("cannot convert value to " + type.getName() + ": " + v, nfe);
        }
        // Unknown target type: attempt a direct cast, surfacing a genuinely
        // incompatible request as a SQLException (not a raw ClassCastException).
        try {
            return type.cast(v);
        } catch (ClassCastException cce) {
            throw new SQLException("cannot convert " + v.getClass().getName()
                    + " to " + type.getName(), cce);
        }
    }

    /**
     * A cell as the Java integer type {@code tname}: a number through
     * {@link PgNumeric#toIntegral} (truncated toward zero, 22003 when out of
     * range, NaN or infinite, as pgjdbc), text parsed as a long first.
     */
    private static long toIntegral(Object v, String tname) throws SQLException {
        if (v instanceof Number) return PgNumeric.toIntegral((Number) v, tname);
        return PgNumeric.toIntegral(Long.parseLong(v.toString().trim()), tname);
    }

    /**
     * The PostgreSQL text of a non-finite float carrier ({@code NaN},
     * {@code Infinity}, {@code -Infinity}), or null for any other value. A
     * numeric special decodes to the same {@link Double} carrier
     * ({@link ArrowDecoder.Decoded}), so this covers real, double precision
     * and numeric columns alike.
     */
    private static String nonFiniteText(Object v) {
        if (v instanceof Double && !Double.isFinite((Double) v)) return v.toString();
        if (v instanceof Float && !Float.isFinite((Float) v)) return v.toString();
        return null;
    }

    private static SQLException badValue(String typeName, String text) {
        return PgNumeric.badValue(typeName, text);
    }

    /**
     * NaN and the infinities have no boolean or BigDecimal value (the integer
     * getters refuse them in {@link PgNumeric#toIntegral}). Without this check
     * a NaN read as false and an Infinity as true. The errors are pgjdbc's:
     * 22003 for BigDecimal, 42846 for boolean.
     */
    private static void refuseNonFinite(Object v, Class<?> type) throws SQLException {
        String nf = nonFiniteText(v);
        if (nf == null) {
            if (type == BigDecimal.class && v instanceof String
                    && ArrowDecoder.Decoded.numericSpecial(((String) v).trim()) != null) {
                throw badValue("BigDecimal", ((String) v).trim());
            }
            return;
        }
        if (type == BigDecimal.class) throw badValue("BigDecimal", nf);
        if (type == Boolean.class) throw new SQLException("Cannot cast to boolean: \"" + nf + "\"", "42846");
    }

    private static double coerceDouble(Object v) {
        if (v instanceof Number) return ((Number) v).doubleValue();
        return Double.parseDouble(v.toString().trim());
    }
    private static boolean coerceBool(Object v) {
        if (v instanceof Boolean) return (Boolean) v;
        if (v instanceof Number) return ((Number) v).longValue() != 0;
        // PG boolean text forms (t/f/true/false/1/0/y/n/yes/no/on/off).
        switch (v.toString().trim().toLowerCase()) {
            case "t": case "true":  case "1": case "y": case "yes": case "on":  return true;
            case "f": case "false": case "0": case "n": case "no":  case "off": return false;
            default: return Boolean.parseBoolean(v.toString().trim());
        }
    }

    @Override public boolean next() throws SQLException {
        ensureOpen();
        if (streamed != null) return nextStreamed();
        if (cursor < rowCount()) cursor++;
        return cursor < rowCount();
    }

    /** {@link #next} of a streamed result: the next row of this batch, else of the next batch. */
    private boolean nextStreamed() throws SQLException {
        if (exhausted) return false;
        if (maxRows > 0 && delivered + cursor + 1 >= maxRows) {
            finishStreamed();
            return false;
        }
        if (cursor + 1 < rows.size()) {
            cursor++;
            return true;
        }
        while (true) {
            ArrowDecoder.Decoded d = streamed.nextBatch();
            if (d == null) {
                delivered += rows.size();
                rows = java.util.Collections.emptyList();
                cursor = 0;
                exhausted = true;
                return false;
            }
            delivered += rows.size();
            rows = d.rows;
            cursor = -1;
            if (!rows.isEmpty()) {
                cursor = 0;
                return true;
            }
        }
    }

    /** Stop a streamed result: what is still to come is dropped as it arrives. */
    private void finishStreamed() {
        if (streamed != null && !exhausted) {
            exhausted = true;
            delivered += Math.max(cursor + 1, 0);
            rows = java.util.Collections.emptyList();
            cursor = 0;
            streamed.discard();
        }
    }

    /** Scrolling needs every row; a streamed result has only the current batch. */
    private void forwardOnly(String op) throws SQLException {
        if (streamed != null) {
            throw new SQLException(op + " on a streamed (setFetchSize > 0) result set, which is forward-only", "24000");
        }
    }

    @Override public void close() {
        if (closed) return;
        finishStreamed();
        closed = true;
        if (owner instanceof PdbStatement) ((PdbStatement) owner).resultClosed(this);
    }
    @Override public boolean isClosed() { return closed; }
    @Override public boolean wasNull() { return wasNull; }

    /**
     * Every read on a CLOSED ResultSet must raise {@link SQLException} (JDBC
     * contract). A closed cursor that keeps serving rows lets a use-after-close
     * bug run to completion and produce plausible-looking output instead of
     * failing where the defect is.
     */
    private void ensureOpen() throws SQLException {
        if (closed) throw new SQLException("result set is closed");
    }

    // ── getX(int) ─────────────────────────────────────────────────────────
    @Override public String getString(int idx) throws SQLException {
        Object v = getValue(idx);
        if (v != null && isTimestamptz(idx)) {
            // PostgreSQL's text, in the session zone the server stamped.
            return Timestamptz.pgText(tzInstant(idx, v), renderZone(idx));
        }
        if (v instanceof java.time.LocalDateTime) {
            // A `timestamp` reads as PostgreSQL's text (`2024-07-01 12:00:00`),
            // as pgjdbc's getString does, not Java's ISO `2024-07-01T12:00`.
            return Timestamptz.pgTextLocal((java.time.LocalDateTime) v);
        }
        if (v instanceof byte[]) {
            // A bytea reads as PostgreSQL's text form (`\x0aff`), as pgjdbc's
            // getString does -- not the array's identity (`[B@1b6d3586`).
            byte[] b = (byte[]) v;
            StringBuilder s = new StringBuilder(2 + 2 * b.length).append("\\x");
            for (byte x : b) s.append(Character.forDigit((x >> 4) & 0xF, 16)).append(Character.forDigit(x & 0xF, 16));
            return s.toString();
        }
        return v == null ? null : v.toString();
    }
    /**
     * The PG text forms of a boolean ({@code t}/{@code f}, and the long
     * spellings) go through the SAME {@link #coerceBool} that
     * {@code getObject(i, Boolean.class)} uses. A bare
     * {@code Boolean.parseBoolean} answered FALSE for {@code 't'} - PG's
     * canonical external form - so the two halves of this driver disagreed
     * about the same cell and whichever one the application called decided
     * whether the row was read correctly.
     */
    @Override public boolean getBoolean(int idx) throws SQLException {
        Object v = getValue(idx);
        if (v == null) return false;
        refuseNonFinite(v, Boolean.class);
        return coerceBool(v);
    }
    // Integer getters are RANGE-CHECKED and truncate toward zero, as pgjdbc
    // does (PgNumeric): a silent wrap turned 2^64 into 0 and 2^31 into
    // -2147483648 and called it a value; pgjdbc throws 22003.
    @Override public byte getByte(int idx) throws SQLException {
        return (byte) integral(idx, "byte");
    }
    @Override public short getShort(int idx) throws SQLException {
        return (short) integral(idx, "short");
    }
    @Override public int getInt(int idx) throws SQLException {
        return (int) integral(idx, "int");
    }
    @Override public long getLong(int idx) throws SQLException {
        return integral(idx, "long");
    }
    /** The cell as the Java integer type {@code tname}; 0 for SQL NULL. */
    private long integral(int idx, String tname) throws SQLException {
        Object v = getValue(idx);
        if (v == null) return 0;
        if (v instanceof Number) return PgNumeric.toIntegral((Number) v, tname);
        // An unparseable cell must surface as SQLException. A raw
        // NumberFormatException is UNCHECKED and escapes every framework's
        // catch(SQLException), killing the caller's thread instead of failing
        // the query.
        long parsed;
        try {
            parsed = Long.parseLong(v.toString().trim());
        } catch (NumberFormatException nfe) {
            throw new SQLException("column " + idx + " is not an integer: " + v, nfe);
        }
        return PgNumeric.toIntegral(parsed, tname);
    }
    @Override public float getFloat(int idx) throws SQLException { return (float) getDouble(idx); }
    @Override public double getDouble(int idx) throws SQLException {
        Object v = getValue(idx);
        if (v == null) return 0.0;
        if (v instanceof Number) return ((Number) v).doubleValue();
        try {
            return Double.parseDouble(v.toString().trim());
        } catch (NumberFormatException nfe) {
            throw new SQLException("column " + idx + " is not a number: " + v, nfe);
        }
    }
    @Override public BigDecimal getBigDecimal(int idx, int scale) throws SQLException {
        BigDecimal d = getBigDecimal(idx);
        return d == null ? null : d.setScale(scale, java.math.RoundingMode.HALF_UP);
    }
    @Override public byte[] getBytes(int idx) throws SQLException {
        Object v = getValue(idx);
        if (v == null) return null;
        if (v instanceof byte[]) return (byte[]) v;
        return v.toString().getBytes(StandardCharsets.UTF_8);
    }
    // ── temporal getters ──────────────────────────────────────────────────
    //
    // Every temporal getter resolves the cell to ONE carrier - a
    // java.time.LocalDateTime, the zone-less wall clock PG stores for DATE /
    // TIME / TIMESTAMP - and then projects that to the requested JDBC type.
    // Going through a single resolution has two consequences the per-getter
    // code could not have:
    //
    //   * a getter is no longer tied to "its own" column type. getDate() on a
    //     TIMESTAMP column, and getTimestamp() on a DATE column, are ordinary
    //     JDBC and used to throw java.lang.IllegalArgumentException - UNCHECKED,
    //     so it escaped every catch(SQLException) upstream.
    //   * a bare numeric carrier is interpreted by the column's DECLARED Arrow
    //     type instead of one getter's guess. days-since-epoch (date32),
    //     millis-since-epoch (date64), micros-since-epoch (timestamp) and
    //     micros-OF-DAY (time) are all "a Long"; reading a TIME's micros-of-day
    //     as epoch millis made 12:00:00 read back as 07:00:00 in
    //     America/New_York and 12:00:00 in UTC - the same stored value, a
    //     different answer per JVM.

    /** Arrow logical-type token for a 1-based column, "" when unknown. */
    private String typeToken(int oneBasedIdx) {
        int i = oneBasedIdx - 1;
        return (i >= 0 && i < columnTypes.size()) ? columnTypes.get(i) : "";
    }

    private static final java.time.LocalDate EPOCH_DAY = java.time.LocalDate.ofEpochDay(0);

    /**
     * The cell as the zone-less wall clock it represents, or null for SQL NULL.
     *
     * @throws SQLException if the cell is not a temporal value at all (a
     *         checked failure, never an unchecked parse blow-up)
     */
    private java.time.LocalDateTime wallClock(int idx, Object v) throws SQLException {
        return wallClock(idx, v, java.time.ZoneId.systemDefault());
    }

    /**
     * {@link #wallClock(int, Object)}, with the zone a {@code timestamptz}
     * cell's INSTANT is read in ({@code zone}: the JVM's, or a Calendar's).
     * A zone-less cell is its own wall clock whatever {@code zone} says.
     */
    private java.time.LocalDateTime wallClock(int idx, Object v, java.time.ZoneId zone)
            throws SQLException {
        if (v == null) return null;
        if (isTimestamptz(idx)) {
            return java.time.LocalDateTime.ofInstant(tzInstant(idx, v), zone);
        }
        if (v instanceof java.time.LocalDateTime) return (java.time.LocalDateTime) v;
        if (v instanceof java.time.LocalDate) return ((java.time.LocalDate) v).atStartOfDay();
        if (v instanceof java.time.LocalTime) return ((java.time.LocalTime) v).atDate(EPOCH_DAY);
        if (v instanceof Timestamp) return ((Timestamp) v).toLocalDateTime();
        if (v instanceof Date) return ((Date) v).toLocalDate().atStartOfDay();
        if (v instanceof Time) return ((Time) v).toLocalTime().atDate(EPOCH_DAY);
        if (v instanceof java.time.Instant) {
            return java.time.LocalDateTime.ofInstant((java.time.Instant) v,
                    java.time.ZoneId.systemDefault());
        }
        if (v instanceof Number) {
            long n = ((Number) v).longValue();
            String tok = typeToken(idx);
            switch (tok) {
                case "date32":
                    return java.time.LocalDate.ofEpochDay(n).atStartOfDay();
                case "date64":
                    return java.time.LocalDateTime.ofEpochSecond(
                            Math.floorDiv(n, 1000L),
                            (int) (Math.floorMod(n, 1000L) * 1_000_000L),
                            java.time.ZoneOffset.UTC);
                case "time":
                    // micros OF DAY - a wall-clock offset, not an instant.
                    return java.time.LocalTime.ofNanoOfDay(
                            Math.floorMod(n, 86_400_000_000L) * 1000L).atDate(EPOCH_DAY);
                case "timestamp":
                default:
                    // micros since epoch (what the engine ships for TIMESTAMP).
                    return java.time.LocalDateTime.ofEpochSecond(
                            Math.floorDiv(n, 1_000_000L),
                            (int) (Math.floorMod(n, 1_000_000L) * 1000L),
                            java.time.ZoneOffset.UTC);
            }
        }
        return parseTemporalText(idx, v.toString().trim());
    }

    /** Text carriers: ISO {@code T}-separated, PG's space-separated, or bare. */
    private static java.time.LocalDateTime parseTemporalText(int idx, String s)
            throws SQLException {
        String iso = s.length() > 10 && s.charAt(10) == ' '
                ? s.substring(0, 10) + 'T' + s.substring(11) : s;
        // A trailing zone/offset is stripped: PG's DATE/TIME/TIMESTAMP are
        // zone-less, and a caller asking for a zone passes a Calendar.
        int z = iso.length() > 10 ? zoneSuffixStart(iso) : -1;
        if (z > 0) iso = iso.substring(0, z);
        try { return java.time.LocalDateTime.parse(iso); }
        catch (java.time.format.DateTimeParseException ignored) {}
        try { return java.time.LocalDate.parse(iso).atStartOfDay(); }
        catch (java.time.format.DateTimeParseException ignored) {}
        try { return java.time.LocalTime.parse(iso).atDate(EPOCH_DAY); }
        catch (java.time.format.DateTimeParseException ignored) {}
        throw new SQLException("column " + idx + " is not a date/time value: " + s);
    }

    /** Index of a trailing {@code Z} / {@code +hh[:mm]} / {@code -hh[:mm]}, else -1. */
    private static int zoneSuffixStart(String iso) {
        if (iso.endsWith("Z")) return iso.length() - 1;
        for (int i = iso.length() - 1; i >= 11; i--) {
            char c = iso.charAt(i);
            if (c == '+' || c == '-') return i;
            if (c != ':' && !Character.isDigit(c)) return -1;
        }
        return -1;
    }

    /** The zone a Calendar overload asks for; the JVM default when null. */
    private static java.time.ZoneId zoneOf(Calendar cal) {
        return cal == null ? java.time.ZoneId.systemDefault() : cal.getTimeZone().toZoneId();
    }

    /**
     * Is this column a {@code timestamp with time zone}? Decided by the
     * engine's {@code pdb:decl} stamp ONLY: both types travel as the same Arrow
     * {@code Timestamp(µs)}, and only the stamp says the micros are a UTC
     * instant rather than a wall clock ({@link Timestamptz}).
     */
    private boolean isTimestamptz(int idx) {
        return Timestamptz.isDecl(declFrom(columnMetadata, idx));
    }

    /** The instant a non-null {@code timestamptz} cell holds. */
    private java.time.Instant tzInstant(int idx, Object v) throws SQLException {
        try {
            return Timestamptz.instant(v);
        } catch (RuntimeException e) {
            throw new SQLException("column " + idx + " is not a timestamptz value: " + v, e);
        }
    }

    /** The session TimeZone this column's timestamptz values render in. */
    private java.time.ZoneId renderZone(int idx) {
        int i = idx - 1;
        Map<String, String> m = (columnMetadata == null || i < 0 || i >= columnMetadata.size())
                ? null : columnMetadata.get(i);
        return Timestamptz.zoneOfSetting(m == null ? null : m.get(Timestamptz.TZ_KEY));
    }

    /** A timestamptz cell as the instant JDBC's {@link Timestamp} carries. */
    private Timestamp tzTimestamp(int idx, Object v) throws SQLException {
        return v == null ? null : Timestamp.from(tzInstant(idx, v));
    }

    @Override public Date getDate(int idx) throws SQLException {
        java.time.LocalDateTime w = wallClock(idx, getValue(idx));
        return w == null ? null : Date.valueOf(w.toLocalDate());
    }
    @Override public Time getTime(int idx) throws SQLException {
        java.time.LocalDateTime w = wallClock(idx, getValue(idx));
        return w == null ? null : Time.valueOf(w.toLocalTime());
    }
    @Override public Timestamp getTimestamp(int idx) throws SQLException {
        Object v = getValue(idx);
        if (isTimestamptz(idx)) return tzTimestamp(idx, v);
        java.time.LocalDateTime w = wallClock(idx, v);
        return w == null ? null : Timestamp.valueOf(w);
    }
    @Override public InputStream getAsciiStream(int idx) throws SQLException { throw nys(); }
    @Override public InputStream getUnicodeStream(int idx) throws SQLException { throw nys(); }
    @Override public InputStream getBinaryStream(int idx) throws SQLException { throw nys(); }

    // ── getX(String) ──────────────────────────────────────────────────────
    @Override public String getString(String n) throws SQLException { return getString(findColumn(n)); }
    @Override public boolean getBoolean(String n) throws SQLException {
        return getBoolean(findColumn(n));
    }
    @Override public byte getByte(String n) throws SQLException { return getByte(findColumn(n)); }
    @Override public short getShort(String n) throws SQLException { return getShort(findColumn(n)); }
    @Override public int getInt(String n) throws SQLException { return getInt(findColumn(n)); }
    @Override public long getLong(String n) throws SQLException { return getLong(findColumn(n)); }
    @Override public float getFloat(String n) throws SQLException { return getFloat(findColumn(n)); }
    @Override public double getDouble(String n) throws SQLException { return getDouble(findColumn(n)); }
    @Override public BigDecimal getBigDecimal(String n, int scale) throws SQLException { return getBigDecimal(findColumn(n), scale); }
    @Override public byte[] getBytes(String n) throws SQLException { return getBytes(findColumn(n)); }
    @Override public Date getDate(String n) throws SQLException { return getDate(findColumn(n)); }
    @Override public Time getTime(String n) throws SQLException { return getTime(findColumn(n)); }
    @Override public Timestamp getTimestamp(String n) throws SQLException { return getTimestamp(findColumn(n)); }
    @Override public InputStream getAsciiStream(String n) throws SQLException { throw nys(); }
    @Override public InputStream getUnicodeStream(String n) throws SQLException { throw nys(); }
    @Override public InputStream getBinaryStream(String n) throws SQLException { throw nys(); }

    @Override public SQLWarning getWarnings() { return null; }
    @Override public void clearWarnings() {}
    @Override public String getCursorName() { return null; }
    @Override public ResultSetMetaData getMetaData() throws SQLException {
        ensureOpen();
        PdbConnection conn = null;
        if (owner != null) {
            java.sql.Connection c = owner.getConnection();
            if (c instanceof PdbConnection) conn = (PdbConnection) c;
        }
        return new SimpleMeta(columnNames, columnTypes, columnMetadata, conn);
    }

    /**
     * The column value as a plain Java object - and, for an ARRAY column, the
     * SAME {@link java.sql.Array} {@link #getArray(int)} builds, not the raw
     * {@code ARRAY[…]} text. That is what pgjdbc returns, and what any caller
     * doing {@code (Array) rs.getObject(n)} needs.
     *
     * <p>{@link #getString(int)} deliberately still returns the text form (also
     * pgjdbc's behavior). "Is this an array?" comes from the declared type
     * ONLY - see {@link #isArrayColumn}.
     */
    @Override public Object getObject(int idx) throws SQLException {
        Object v = getValue(idx); // sets wasNull
        // A timestamptz is the instant, as java.sql.Timestamp: what
        // getColumnClassName reports and what pgjdbc returns.
        if (v != null && isTimestamptz(idx)) return tzTimestamp(idx, v);
        if (v == null || !isArrayColumn(idx)) return v;
        return arrayFrom(idx, v);
    }

    @Override public Object getObject(String n) throws SQLException { return getObject(findColumn(n)); }

    /**
     * Is this column an ARRAY column? Decided ONLY by the {@code pdb:decl}
     * declared-type stamp (mirrors {@code field_is_array_decl} in
     * the server's PostgreSQL-wire layer) - never by looking at the value, which
     * is just {@code ARRAY[…]} text in which a {@code text[]} element "1" and an
     * {@code int4[]} element 1 are indistinguishable. An unstamped column stays
     * text, exactly as the PG wire advertises it.
     */
    private boolean isArrayColumn(int oneBasedIdx) {
        return PdbArray.isArrayDecl(declaredTypeOf(oneBasedIdx));
    }

    @Override public int findColumn(String name) throws SQLException {
        ensureOpen();
        int idx = columnIndexOrZero(name);
        if (idx == 0) throw new SQLException("column not found: " + name);
        return idx;
    }

    @Override public Reader getCharacterStream(int idx) throws SQLException { throw nys(); }
    @Override public Reader getCharacterStream(String n) throws SQLException { throw nys(); }
    @Override public BigDecimal getBigDecimal(int idx) throws SQLException {
        Object v = getValue(idx);
        if (v == null) return null;
        if (v instanceof BigDecimal) return (BigDecimal) v;
        refuseNonFinite(v, BigDecimal.class);
        try {
            return new BigDecimal(v.toString().trim());
        } catch (NumberFormatException nfe) {
            throw new SQLException("column " + idx + " is not a decimal: " + v, nfe);
        }
    }
    @Override public BigDecimal getBigDecimal(String n) throws SQLException { return getBigDecimal(findColumn(n)); }

    // ── cursor positioning ────────────────────────────────────────────────
    //
    // The boolean these return means "the cursor is now ON a row", so it must
    // describe the cursor they actually set. It did not: absolute(0) parked the
    // cursor at -1 (before-first) and returned TRUE, and absolute(-1) - JDBC's
    // spelling for "the last row" - parked it at -2 and also returned TRUE, so
    // the caller's very next getter threw "cursor out of range". Positions are
    // clamped to the two out-of-band states (before-first / after-last) that
    // JDBC defines, and never to an index no getter can serve.
    @Override public boolean isBeforeFirst() throws SQLException {
        if (streamed != null) return delivered == 0 && cursor < 0 && !exhausted;
        return cursor < 0 && rowCount() > 0;
    }
    @Override public boolean isAfterLast() throws SQLException {
        if (streamed != null) return exhausted && delivered > 0;
        return cursor >= rowCount() && rowCount() > 0;
    }
    @Override public boolean isFirst() throws SQLException {
        if (streamed != null) return delivered == 0 && cursor == 0 && !exhausted;
        return cursor == 0 && rowCount() > 0;
    }
    @Override public boolean isLast() throws SQLException {
        forwardOnly("isLast");
        return rowCount() > 0 && cursor == rowCount() - 1;
    }
    @Override public void beforeFirst() throws SQLException { forwardOnly("beforeFirst"); cursor = -1; }
    @Override public void afterLast() throws SQLException { forwardOnly("afterLast"); cursor = rowCount(); }
    @Override public boolean first() throws SQLException {
        forwardOnly("first");
        cursor = rowCount() == 0 ? -1 : 0;
        return rowCount() > 0;
    }
    @Override public boolean last() throws SQLException { forwardOnly("last"); cursor = rowCount() - 1; return rowCount() > 0; }
    /** 1-based row number, or 0 when the cursor is not on a row (JDBC). */
    @Override public int getRow() {
        if (streamed != null) return (!exhausted && cursor >= 0 && cursor < rows.size()) ? (int) (delivered + cursor + 1) : 0;
        return onRow() ? cursor + 1 : 0;
    }
    @Override public boolean absolute(int row) throws SQLException {
        forwardOnly("absolute");
        // row > 0 counts from the start, row < 0 counts back from the end
        // (-1 == last row), row == 0 means "before the first row".
        int target = row > 0 ? row - 1 : (row < 0 ? rowCount() + row : -1);
        return seek(target);
    }
    @Override public boolean relative(int offset) throws SQLException {
        forwardOnly("relative");
        return seek(cursor + offset);
    }
    @Override public boolean previous() throws SQLException { forwardOnly("previous"); return seek(cursor - 1); }

    /** Park the cursor at {@code target}, clamped to before-first/after-last. */
    private boolean seek(int target) {
        if (target < 0) { cursor = -1; return false; }
        if (target >= rowCount()) { cursor = rowCount(); return false; }
        cursor = target;
        return true;
    }

    private boolean onRow() { return cursor >= 0 && cursor < rowCount(); }
    @Override public void setFetchDirection(int dir) {}
    @Override public int getFetchDirection() { return FETCH_FORWARD; }
    @Override public void setFetchSize(int rows) {}
    @Override public int getFetchSize() { return 0; }
    @Override public int getType() { return TYPE_FORWARD_ONLY; }
    @Override public int getConcurrency() { return CONCUR_READ_ONLY; }
    @Override public boolean rowUpdated() { return false; }
    @Override public boolean rowInserted() { return false; }
    @Override public boolean rowDeleted() { return false; }

    // ── update*: not supported (read-only result sets) ────────────────────
    @Override public void updateNull(int idx) throws SQLException { throw nys(); }
    @Override public void updateBoolean(int idx, boolean x) throws SQLException { throw nys(); }
    @Override public void updateByte(int idx, byte x) throws SQLException { throw nys(); }
    @Override public void updateShort(int idx, short x) throws SQLException { throw nys(); }
    @Override public void updateInt(int idx, int x) throws SQLException { throw nys(); }
    @Override public void updateLong(int idx, long x) throws SQLException { throw nys(); }
    @Override public void updateFloat(int idx, float x) throws SQLException { throw nys(); }
    @Override public void updateDouble(int idx, double x) throws SQLException { throw nys(); }
    @Override public void updateBigDecimal(int idx, BigDecimal x) throws SQLException { throw nys(); }
    @Override public void updateString(int idx, String x) throws SQLException { throw nys(); }
    @Override public void updateBytes(int idx, byte[] x) throws SQLException { throw nys(); }
    @Override public void updateDate(int idx, Date x) throws SQLException { throw nys(); }
    @Override public void updateTime(int idx, Time x) throws SQLException { throw nys(); }
    @Override public void updateTimestamp(int idx, Timestamp x) throws SQLException { throw nys(); }
    @Override public void updateAsciiStream(int idx, InputStream x, int len) throws SQLException { throw nys(); }
    @Override public void updateBinaryStream(int idx, InputStream x, int len) throws SQLException { throw nys(); }
    @Override public void updateCharacterStream(int idx, Reader x, int len) throws SQLException { throw nys(); }
    @Override public void updateObject(int idx, Object x, int scale) throws SQLException { throw nys(); }
    @Override public void updateObject(int idx, Object x) throws SQLException { throw nys(); }
    @Override public void updateNull(String n) throws SQLException { throw nys(); }
    @Override public void updateBoolean(String n, boolean x) throws SQLException { throw nys(); }
    @Override public void updateByte(String n, byte x) throws SQLException { throw nys(); }
    @Override public void updateShort(String n, short x) throws SQLException { throw nys(); }
    @Override public void updateInt(String n, int x) throws SQLException { throw nys(); }
    @Override public void updateLong(String n, long x) throws SQLException { throw nys(); }
    @Override public void updateFloat(String n, float x) throws SQLException { throw nys(); }
    @Override public void updateDouble(String n, double x) throws SQLException { throw nys(); }
    @Override public void updateBigDecimal(String n, BigDecimal x) throws SQLException { throw nys(); }
    @Override public void updateString(String n, String x) throws SQLException { throw nys(); }
    @Override public void updateBytes(String n, byte[] x) throws SQLException { throw nys(); }
    @Override public void updateDate(String n, Date x) throws SQLException { throw nys(); }
    @Override public void updateTime(String n, Time x) throws SQLException { throw nys(); }
    @Override public void updateTimestamp(String n, Timestamp x) throws SQLException { throw nys(); }
    @Override public void updateAsciiStream(String n, InputStream x, int len) throws SQLException { throw nys(); }
    @Override public void updateBinaryStream(String n, InputStream x, int len) throws SQLException { throw nys(); }
    @Override public void updateCharacterStream(String n, Reader x, int len) throws SQLException { throw nys(); }
    @Override public void updateObject(String n, Object x, int scale) throws SQLException { throw nys(); }
    @Override public void updateObject(String n, Object x) throws SQLException { throw nys(); }
    @Override public void insertRow() throws SQLException { throw nys(); }
    @Override public void updateRow() throws SQLException { throw nys(); }
    @Override public void deleteRow() throws SQLException { throw nys(); }
    @Override public void refreshRow() throws SQLException { throw nys(); }
    @Override public void cancelRowUpdates() throws SQLException { throw nys(); }
    @Override public void moveToInsertRow() throws SQLException { throw nys(); }
    @Override public void moveToCurrentRow() throws SQLException { throw nys(); }

    @Override public Statement getStatement() { return owner; }
    @Override public Object getObject(int idx, Map<String, Class<?>> map) throws SQLException { return getObject(idx); }
    @Override public Object getObject(String n, Map<String, Class<?>> map) throws SQLException { return getObject(n); }
    @Override public Ref getRef(int idx) throws SQLException { throw nys(); }
    @Override public Blob getBlob(int idx) throws SQLException { throw nys(); }
    @Override public Clob getClob(int idx) throws SQLException { throw nys(); }
    @Override public Array getArray(int idx) throws SQLException {
        Object v = getValue(idx); // sets wasNull
        if (v == null) return null;
        return arrayFrom(idx, v);
    }

    /**
     * Build the {@link java.sql.Array} for a non-null column value. The ONE
     * array-construction path - {@link #getArray(int)}, {@link #getObject(int)}
     * and {@code getObject(int, Array.class)} all come through here, so they can
     * never disagree about elements or element type.
     */
    private Array arrayFrom(int idx, Object v) throws SQLException {
        Object[] elems;
        if (v instanceof String) {
            // PDB serves an array column as a Utf8 cell carrying the PG array
            // EXTERNAL TEXT form ('{alpha,"be,ta",…}') - see
            // the server's execution context. Parse it into elements.
            elems = PdbArray.parsePgArrayText((String) v);
        } else if (v instanceof Object[]) {
            // Defensive: a real Arrow List column normalizes to Object[].
            elems = (Object[]) v;
        } else if (v instanceof java.util.List) {
            elems = ((java.util.List<?>) v).toArray();
        } else if (v.getClass().isArray()) {
            int len = java.lang.reflect.Array.getLength(v);
            elems = new Object[len];
            for (int i = 0; i < len; i++) elems[i] = java.lang.reflect.Array.get(v, i);
        } else {
            throw new SQLException("column " + idx + " is not an array: " + v.getClass());
        }
        // Element TYPE comes from the column's declared SQL type, which the
        // engine stamps on the Arrow field as `pdb:decl` (`INT[]`, `TEXT[]`, or
        // the `_pdb_array` sentinel when a computed array's element type could
        // not be resolved). The cell itself is just ARRAY[…] text, so the stamp
        // is the ONLY sound source: a text[] element "1" and an int4[] element 1
        // are byte-identical in the payload, and inferring the type from the
        // VALUES would mistype the former. An absent/sentinel stamp falls back
        // to text rather than guessing.
        return PdbArray.forColumnDecl(declaredTypeOf(idx), elems);
    }

    /**
     * The column's declared SQL type from the Arrow field's {@code pdb:decl}
     * metadata, or null when the field carries no stamp.
     */
    private String declaredTypeOf(int oneBasedIdx) {
        return declFrom(columnMetadata, oneBasedIdx);
    }
    @Override public Ref getRef(String n) throws SQLException { throw nys(); }
    @Override public Blob getBlob(String n) throws SQLException { throw nys(); }
    @Override public Clob getClob(String n) throws SQLException { throw nys(); }
    @Override public Array getArray(String n) throws SQLException { return getArray(findColumn(n)); }
    // The Calendar overloads exist so an application can pin the zone used to
    // interpret a zone-LESS DATE / TIME / TIMESTAMP. Discarding the Calendar
    // (what these did) makes the answer depend on the JVM's default zone  -
    // exactly the corruption the overload was added to prevent, and invisible
    // to any test that runs in one zone. Hibernate and Spring both call these.
    // A null Calendar means "JVM default", i.e. identical to the no-arg form
    // (JDBC javadoc).
    @Override public Date getDate(int idx, Calendar cal) throws SQLException {
        java.time.LocalDateTime w = wallClock(idx, getValue(idx), zoneOf(cal));
        if (w == null) return null;
        return new Date(w.toLocalDate().atStartOfDay(zoneOf(cal)).toInstant().toEpochMilli());
    }
    @Override public Date getDate(String n, Calendar cal) throws SQLException {
        return getDate(findColumn(n), cal);
    }
    @Override public Time getTime(int idx, Calendar cal) throws SQLException {
        java.time.LocalDateTime w = wallClock(idx, getValue(idx), zoneOf(cal));
        if (w == null) return null;
        return new Time(w.toLocalTime().atDate(EPOCH_DAY)
                .atZone(zoneOf(cal)).toInstant().toEpochMilli());
    }
    @Override public Time getTime(String n, Calendar cal) throws SQLException {
        return getTime(findColumn(n), cal);
    }
    @Override public Timestamp getTimestamp(int idx, Calendar cal) throws SQLException {
        Object v = getValue(idx);
        // A timestamptz is an instant: the Calendar names a zone for a
        // zone-LESS value and has nothing to add here (pgjdbc agrees).
        if (isTimestamptz(idx)) return tzTimestamp(idx, v);
        java.time.LocalDateTime w = wallClock(idx, v);
        if (w == null) return null;
        java.time.Instant inst = w.atZone(zoneOf(cal)).toInstant();
        Timestamp ts = new Timestamp(inst.toEpochMilli());
        ts.setNanos(inst.getNano());
        return ts;
    }
    @Override public Timestamp getTimestamp(String n, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(n), cal);
    }
    @Override public URL getURL(int idx) throws SQLException {
        Object v = getValue(idx); if (v == null) return null;
        try { return new URL(v.toString()); } catch (Exception e) { throw new SQLException(e); }
    }
    @Override public URL getURL(String n) throws SQLException { return getURL(findColumn(n)); }
    @Override public void updateRef(int idx, Ref x) throws SQLException { throw nys(); }
    @Override public void updateRef(String n, Ref x) throws SQLException { throw nys(); }
    @Override public void updateBlob(int idx, Blob x) throws SQLException { throw nys(); }
    @Override public void updateBlob(String n, Blob x) throws SQLException { throw nys(); }
    @Override public void updateClob(int idx, Clob x) throws SQLException { throw nys(); }
    @Override public void updateClob(String n, Clob x) throws SQLException { throw nys(); }
    @Override public void updateArray(int idx, Array x) throws SQLException { throw nys(); }
    @Override public void updateArray(String n, Array x) throws SQLException { throw nys(); }
    @Override public RowId getRowId(int idx) throws SQLException { throw nys(); }
    @Override public RowId getRowId(String n) throws SQLException { throw nys(); }
    @Override public void updateRowId(int idx, RowId x) throws SQLException { throw nys(); }
    @Override public void updateRowId(String n, RowId x) throws SQLException { throw nys(); }
    @Override public int getHoldability() { return 1; }
    @Override public void updateNString(int idx, String x) throws SQLException { throw nys(); }
    @Override public void updateNString(String n, String x) throws SQLException { throw nys(); }
    @Override public void updateNClob(int idx, NClob x) throws SQLException { throw nys(); }
    @Override public void updateNClob(String n, NClob x) throws SQLException { throw nys(); }
    @Override public NClob getNClob(int idx) throws SQLException { throw nys(); }
    @Override public NClob getNClob(String n) throws SQLException { throw nys(); }
    @Override public SQLXML getSQLXML(int idx) throws SQLException { throw nys(); }
    @Override public SQLXML getSQLXML(String n) throws SQLException { throw nys(); }
    @Override public void updateSQLXML(int idx, SQLXML x) throws SQLException { throw nys(); }
    @Override public void updateSQLXML(String n, SQLXML x) throws SQLException { throw nys(); }
    @Override public String getNString(int idx) throws SQLException { return getString(idx); }
    @Override public String getNString(String n) throws SQLException { return getString(n); }
    @Override public Reader getNCharacterStream(int idx) throws SQLException { throw nys(); }
    @Override public Reader getNCharacterStream(String n) throws SQLException { throw nys(); }
    @Override public void updateNCharacterStream(int idx, Reader x, long len) throws SQLException { throw nys(); }
    @Override public void updateNCharacterStream(String n, Reader x, long len) throws SQLException { throw nys(); }
    @Override public void updateAsciiStream(int idx, InputStream x, long len) throws SQLException { throw nys(); }
    @Override public void updateBinaryStream(int idx, InputStream x, long len) throws SQLException { throw nys(); }
    @Override public void updateCharacterStream(int idx, Reader x, long len) throws SQLException { throw nys(); }
    @Override public void updateAsciiStream(String n, InputStream x, long len) throws SQLException { throw nys(); }
    @Override public void updateBinaryStream(String n, InputStream x, long len) throws SQLException { throw nys(); }
    @Override public void updateCharacterStream(String n, Reader x, long len) throws SQLException { throw nys(); }
    @Override public void updateBlob(int idx, InputStream x, long len) throws SQLException { throw nys(); }
    @Override public void updateBlob(String n, InputStream x, long len) throws SQLException { throw nys(); }
    @Override public void updateClob(int idx, Reader x, long len) throws SQLException { throw nys(); }
    @Override public void updateClob(String n, Reader x, long len) throws SQLException { throw nys(); }
    @Override public void updateNClob(int idx, Reader x, long len) throws SQLException { throw nys(); }
    @Override public void updateNClob(String n, Reader x, long len) throws SQLException { throw nys(); }
    @Override public void updateNCharacterStream(int idx, Reader x) throws SQLException { throw nys(); }
    @Override public void updateNCharacterStream(String n, Reader x) throws SQLException { throw nys(); }
    @Override public void updateAsciiStream(int idx, InputStream x) throws SQLException { throw nys(); }
    @Override public void updateBinaryStream(int idx, InputStream x) throws SQLException { throw nys(); }
    @Override public void updateCharacterStream(int idx, Reader x) throws SQLException { throw nys(); }
    @Override public void updateAsciiStream(String n, InputStream x) throws SQLException { throw nys(); }
    @Override public void updateBinaryStream(String n, InputStream x) throws SQLException { throw nys(); }
    @Override public void updateCharacterStream(String n, Reader x) throws SQLException { throw nys(); }
    @Override public void updateBlob(int idx, InputStream x) throws SQLException { throw nys(); }
    @Override public void updateBlob(String n, InputStream x) throws SQLException { throw nys(); }
    @Override public void updateClob(int idx, Reader x) throws SQLException { throw nys(); }
    @Override public void updateClob(String n, Reader x) throws SQLException { throw nys(); }
    @Override public void updateNClob(int idx, Reader x) throws SQLException { throw nys(); }
    @Override public void updateNClob(String n, Reader x) throws SQLException { throw nys(); }
    /**
     * Typed variant. An ARRAY column answers {@code Array.class} (and
     * {@code Object.class}, so it agrees with {@link #getObject(int)}) with the
     * same {@link java.sql.Array} {@link #getArray(int)} builds; every other
     * target type - {@code String.class} in particular - still gets the value
     * converted, i.e. the raw {@code ARRAY[…]} text.
     */
    @Override public <T> T getObject(int idx, Class<T> type) throws SQLException {
        if (type != null && (Array.class.isAssignableFrom(type) || type == Object.class)
                && isArrayColumn(idx)) {
            Object v = getValue(idx); // sets wasNull
            return v == null ? null : type.cast(arrayFrom(idx, v));
        }
        if (type != null && isTimestamptz(idx)) {
            Object v = getValue(idx); // sets wasNull
            return v == null ? null : tzAs(idx, tzInstant(idx, v), type);
        }
        return convert(getValue(idx), type);
    }
    /**
     * A timestamptz instant as the java.time / java.sql type asked for. The
     * zone-less targets ({@code LocalDateTime}, {@code LocalDate},
     * {@code LocalTime}) take the JVM zone's wall clock, which is what
     * {@code getTimestamp(i).toLocalDateTime()} gives.
     */
    @SuppressWarnings("unchecked")
    private <T> T tzAs(int idx, java.time.Instant i, Class<T> type) throws SQLException {
        java.time.ZoneId jvm = java.time.ZoneId.systemDefault();
        if (type == java.time.OffsetDateTime.class) return (T) i.atOffset(java.time.ZoneOffset.UTC);
        if (type == java.time.Instant.class) return (T) i;
        if (type == java.time.ZonedDateTime.class) return (T) i.atZone(jvm);
        if (type == Timestamp.class || type == Object.class || type == java.util.Date.class) {
            return (T) Timestamp.from(i);
        }
        if (type == java.time.LocalDateTime.class) return (T) java.time.LocalDateTime.ofInstant(i, jvm);
        if (type == java.time.LocalDate.class) return (T) i.atZone(jvm).toLocalDate();
        if (type == java.time.LocalTime.class) return (T) i.atZone(jvm).toLocalTime();
        if (type == Date.class) return (T) Date.valueOf(i.atZone(jvm).toLocalDate());
        if (type == String.class) return (T) Timestamptz.pgText(i, renderZone(idx));
        throw new SQLException("cannot convert a timestamptz to " + type.getName());
    }

    @Override public <T> T getObject(String n, Class<T> type) throws SQLException {
        int idx = columnIndexOrZero(n);
        // Absent label stays lenient (null), but a null target type still throws.
        if (idx == 0) { wasNull = true; return convert(null, type); }
        return getObject(idx, type);
    }
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("not a wrapper for " + iface);
    }
    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }

    private static SQLFeatureNotSupportedException nys() {
        return new SQLFeatureNotSupportedException("v1: not yet supported");
    }

    /**
     * ResultSetMetaData backed by the Arrow schema. Column names come from the
     * projection; JDBC types are derived from the per-column Arrow logical-type
     * token ({@link ArrowDecoder#arrowTypeToken}). This gives frameworks that
     * row-map by type (Spring JDBC, JDBI, MyBatis) the correct
     * {@code getColumnType}/{@code getColumnTypeName}/{@code getColumnClassName}/
     * {@code getPrecision}/{@code getScale} instead of the old
     * {@code OTHER}/{@code ""}/{@code Object} stubs.
     */
    static final class SimpleMeta implements ResultSetMetaData {
        private final List<String> cols;
        private final List<String> types; // Arrow type tokens, may be empty/short
        // Per-column Arrow field metadata, may be empty/short. Only the
        // `pdb:decl` stamp is read: an ARRAY column ships as Arrow Utf8, so the
        // type token alone would report it as plain text/VARCHAR.
        private final List<Map<String, String>> meta;

        SimpleMeta(List<String> cols) { this(cols, Collections.emptyList()); }
        SimpleMeta(List<String> cols, List<String> types) {
            this(cols, types, Collections.emptyList());
        }
        SimpleMeta(List<String> cols, List<String> types, List<Map<String, String>> meta) {
            this(cols, types, meta, null);
        }
        SimpleMeta(List<String> cols, List<String> types, List<Map<String, String>> meta,
                   PdbConnection conn) {
            this.cols = cols;
            this.types = types == null ? Collections.emptyList() : types;
            this.meta = meta == null ? Collections.<Map<String, String>>emptyList() : meta;
            this.conn = conn;
        }

        /** Resolves a column's source-table OID to its name; null for a detached result. */
        private final PdbConnection conn;

        /** Arrow field-metadata key of the column's source table OID (0 / absent: not a table column). */
        static final String SRC_TABLE_OID_KEY = "pdb:src_table_oid";

        /** The column's source-table OID, or 0 when it is not a plain table column. */
        long srcTableOid(int c) {
            int i = c - 1;
            if (i < 0 || i >= meta.size() || meta.get(i) == null) return 0;
            String v = meta.get(i).get(SRC_TABLE_OID_KEY);
            if (v == null) return 0;
            try { return Long.parseLong(v.trim()); } catch (NumberFormatException e) { return 0; }
        }

        /**
         * Every per-column accessor is bounds-checked into a
         * {@link SQLException}. {@code cols.get(c - 1)} threw
         * {@link IndexOutOfBoundsException} - unchecked, so it escaped every
         * framework's {@code catch (SQLException)} and killed the caller
         * instead of reporting a bad column number.
         */
        private void checkColumn(int c) throws SQLException {
            if (c < 1 || c > cols.size()) {
                throw new SQLException("column index out of range: " + c
                        + " (result has " + cols.size() + " columns)");
            }
        }

        private String token(int c) {
            int i = c - 1;
            return (i >= 0 && i < types.size()) ? types.get(i) : "";
        }

        /** The column's declared SQL type, or null when unstamped. */
        private String decl(int c) { return declFrom(meta, c); }

        /** Is this column an ARRAY? Declared type only - never the values. */
        private boolean isArray(int c) { return PdbArray.isArrayDecl(decl(c)); }

        @Override public int getColumnCount() { return cols.size(); }
        @Override public boolean isAutoIncrement(int c) throws SQLException { checkColumn(c); return false; }
        @Override public boolean isCaseSensitive(int c) throws SQLException {
            int t = getColumnType(c);
            // ARRAY is here because its elements are compared as text - and
            // because pgjdbc's TypeInfoCache answers true for every array OID
            // (only the numeric/temporal scalars are listed as insensitive).
            return t == java.sql.Types.VARCHAR || t == java.sql.Types.CHAR
                    || t == java.sql.Types.LONGVARCHAR || t == java.sql.Types.OTHER
                    || t == java.sql.Types.ARRAY;
        }
        @Override public boolean isSearchable(int c) throws SQLException { checkColumn(c); return true; }
        @Override public boolean isCurrency(int c) throws SQLException { checkColumn(c); return false; }
        @Override public int isNullable(int c) throws SQLException { checkColumn(c); return columnNullableUnknown; }
        @Override public boolean isSigned(int c) throws SQLException {
            switch (getColumnType(c)) {
                case java.sql.Types.SMALLINT:
                case java.sql.Types.INTEGER:
                case java.sql.Types.BIGINT:
                case java.sql.Types.REAL:
                case java.sql.Types.DOUBLE:
                case java.sql.Types.NUMERIC:
                case java.sql.Types.DECIMAL:
                    return true;
                default:
                    return false;
            }
        }
        @Override public int getColumnDisplaySize(int c) throws SQLException {
            switch (getColumnType(c)) {
                case java.sql.Types.SMALLINT: return 6;
                case java.sql.Types.INTEGER:  return 11;
                case java.sql.Types.BIGINT:   return 20;
                case java.sql.Types.BOOLEAN:  return 5;
                case java.sql.Types.DATE:     return 10;
                case java.sql.Types.TIMESTAMP: return 29;
                default: return 0;
            }
        }
        @Override public String getColumnLabel(int c) throws SQLException {
            checkColumn(c); return cols.get(c - 1);
        }
        @Override public String getColumnName(int c) throws SQLException {
            checkColumn(c); return cols.get(c - 1);
        }
        @Override public String getSchemaName(int c) throws SQLException { checkColumn(c); return ""; }
        @Override public int getPrecision(int c) throws SQLException {
            checkColumn(c);
            String tok = token(c);
            if (tok.startsWith("decimal(")) {
                int lp = tok.indexOf('('), comma = tok.indexOf(',');
                if (lp >= 0 && comma > lp) {
                    try { return Integer.parseInt(tok.substring(lp + 1, comma).trim()); }
                    catch (NumberFormatException ignored) {}
                }
            }
            switch (getColumnType(c)) {
                case java.sql.Types.SMALLINT: return 5;
                case java.sql.Types.INTEGER:  return 10;
                case java.sql.Types.BIGINT:   return 19;
                case java.sql.Types.REAL:     return 8;
                case java.sql.Types.DOUBLE:   return 17;
                default: return 0;
            }
        }
        @Override public int getScale(int c) throws SQLException {
            checkColumn(c);
            String tok = token(c);
            if (tok.startsWith("decimal(")) {
                int comma = tok.indexOf(','), rp = tok.indexOf(')');
                if (comma >= 0 && rp > comma) {
                    try { return Integer.parseInt(tok.substring(comma + 1, rp).trim()); }
                    catch (NumberFormatException ignored) {}
                }
            }
            return 0;
        }
        /**
         * The name of the table the column reads, as pgjdbc answers it: the
         * relation of the RowDescription table OID, {@code ""} for a computed
         * column (an expression, a literal, a merged USING column) or a result
         * with no connection to ask.
         */
        @Override public String getTableName(int c) throws SQLException {
            checkColumn(c);
            long oid = srcTableOid(c);
            if (oid <= 0 || conn == null) return "";
            java.util.List<Long> all = new java.util.ArrayList<>(cols.size());
            for (int k = 1; k <= cols.size(); k++) all.add(srcTableOid(k));
            return conn.relationName(oid, all);
        }
        @Override public String getCatalogName(int c) throws SQLException { checkColumn(c); return ""; }
        // An ARRAY column is Arrow Utf8 carrying ARRAY[…] text, so its type token
        // says "utf8" and only the declared type distinguishes it. pgjdbc reports
        // Types.ARRAY / `_int4` / java.sql.Array for the same column, so these
        // three answers are driven by the stamp when one is present.
        @Override public int getColumnType(int c) throws SQLException {
            checkColumn(c);
            return isArray(c) ? java.sql.Types.ARRAY : jdbcType(token(c));
        }
        @Override public String getColumnTypeName(int c) throws SQLException {
            checkColumn(c);
            return isArray(c) ? pgArrayTypeName(decl(c)) : pgTypeName(token(c));
        }
        @Override public boolean isReadOnly(int c) throws SQLException { checkColumn(c); return true; }
        @Override public boolean isWritable(int c) throws SQLException { checkColumn(c); return false; }
        @Override public boolean isDefinitelyWritable(int c) throws SQLException { checkColumn(c); return false; }
        @Override public String getColumnClassName(int c) throws SQLException {
            // Derived from the REPORTED JDBC type, so it can never contradict
            // getColumnType (an array column ⇒ java.sql.Array).
            return javaClassNameForJdbcType(getColumnType(c));
        }
        @Override public <T> T unwrap(Class<T> iface) throws SQLException {
            if (iface.isInstance(this)) return iface.cast(this);
            throw new SQLException("not a wrapper for " + iface);
        }
        @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }

        // ── Arrow-token → JDBC contract mapping ──────────────────────────────
        static int jdbcType(String tok) {
            if (tok == null) return java.sql.Types.OTHER;
            if (tok.startsWith("decimal")) return java.sql.Types.NUMERIC;
            switch (tok) {
                case "int8":   return java.sql.Types.TINYINT;   // rare in PDB
                case "int16":  return java.sql.Types.SMALLINT;
                case "int32":  return java.sql.Types.INTEGER;
                case "int64":  return java.sql.Types.BIGINT;
                case "float32": return java.sql.Types.REAL;
                case "float64": return java.sql.Types.DOUBLE;
                case "utf8":
                case "largeutf8": return java.sql.Types.VARCHAR;
                case "binary": return java.sql.Types.BINARY;
                case "bool":   return java.sql.Types.BOOLEAN;
                case "date32":
                case "date64": return java.sql.Types.DATE;
                case "timestamp": return java.sql.Types.TIMESTAMP;
                case "time":   return java.sql.Types.TIME;
                // PDB's OWN array columns never reach this branch: the engine
                // ships them as Utf8 + a `pdb:decl` stamp, which getColumnType
                // handles before consulting the token. This stays for a genuine
                // Arrow List column, which the read path does support end to end
                // (ArrowDecoder.Direct.normalize flattens a List cell to
                // Object[]; getArray/arrayFrom wrap that Object[]).
                case "list":   return java.sql.Types.ARRAY;
                default:       return java.sql.Types.OTHER;
            }
        }

        static String pgTypeName(String tok) {
            if (tok == null) return "unknown";
            if (tok.startsWith("decimal")) return "numeric";
            switch (tok) {
                case "int8":   return "int2";
                case "int16":  return "int2";
                case "int32":  return "int4";
                case "int64":  return "int8";
                case "float32": return "float4";
                case "float64": return "float8";
                case "utf8":
                case "largeutf8": return "text";
                case "binary": return "bytea";
                case "bool":   return "bool";
                case "date32":
                case "date64": return "date";
                case "timestamp": return "timestamp";
                case "time":   return "time";
                // Same story as jdbcType("list"): unreachable for PDB's own
                // arrays (they carry a decl). A bare List token names no element
                // type, so it degrades to `_text` - the same fallback the
                // computed-array sentinel takes.
                case "list":   return "_text";
                default:       return tok.isEmpty() ? "unknown" : tok;
            }
        }

        /**
         * PG type name for an ARRAY column, in PG's internal {@code _elem}
         * spelling ({@code INT[]} → {@code _int4}) - what pgjdbc reports for the
         * same column. The element name is canonicalized exactly as the element
         * TYPE is, so {@code INT[]}/{@code INTEGER[]} agree, a modifier is
         * dropped ({@code NUMERIC(10,2)[]} → {@code _numeric}), and an
         * unknown/sentinel element gives {@code _text}.
         */
        static String pgArrayTypeName(String decl) {
            return "_" + PdbArray.canonicalTypeName(PdbArray.elementTypeFromArrayDecl(decl));
        }

        /**
         * Java class name for a REPORTED JDBC type. Keyed off the type rather
         * than the Arrow token so it always agrees with {@code getColumnType},
         * including for an ARRAY column, whose token is only {@code utf8}.
         */
        static String javaClassNameForJdbcType(int jdbcType) {
            switch (jdbcType) {
                case java.sql.Types.TINYINT:
                case java.sql.Types.SMALLINT:
                case java.sql.Types.INTEGER: return Integer.class.getName();
                case java.sql.Types.BIGINT:  return Long.class.getName();
                case java.sql.Types.REAL:    return Float.class.getName();
                case java.sql.Types.DOUBLE:  return Double.class.getName();
                case java.sql.Types.NUMERIC:
                case java.sql.Types.DECIMAL: return java.math.BigDecimal.class.getName();
                case java.sql.Types.VARCHAR:
                case java.sql.Types.CHAR:    return String.class.getName();
                case java.sql.Types.BINARY:  return "[B";
                case java.sql.Types.BOOLEAN: return Boolean.class.getName();
                case java.sql.Types.DATE:    return java.sql.Date.class.getName();
                case java.sql.Types.TIMESTAMP: return java.sql.Timestamp.class.getName();
                case java.sql.Types.TIME:    return java.sql.Time.class.getName();
                case java.sql.Types.ARRAY:   return java.sql.Array.class.getName();
                default:                     return Object.class.getName();
            }
        }
    }
}
