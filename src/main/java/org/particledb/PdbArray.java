package org.particledb;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * JDBC {@link java.sql.Array} for a single-dimensional SQL array (the only
 * array shape Conductor's {@code postgres-persistence} uses - {@code text[]} /
 * {@code varchar[]} index-word lists in {@code PostgresArchiveDAO}).
 *
 * <p>Two roles:
 * <ul>
 *   <li><b>Bind side</b> - produced by {@link PdbConnection#createArrayOf} and
 *       consumed by {@link PdbPreparedStatement#setArray}. The driver ships a
 *       complete SQL string (client-side substitution), so the array is rendered
 *       to a PostgreSQL array literal ({@code '{"a","b"}'::text[]}) - see
 *       {@link #toSqlLiteral()}. PDB parses that literal (verified against
 *       the server's SQL parser array-literal + {@code ::text[]} cast
 *       support).</li>
 *   <li><b>Read side</b> - wrapped by {@link PdbResultSet#getArray} around the
 *       {@code Object[]} that {@link ArrowDecoder} produced for an Arrow
 *       {@code List<Utf8>} column, so {@code rs.getArray("index_data").getArray()}
 *       returns a {@code String[]} (the shape {@code PostgresArchiveDAO:1846}
 *       casts to).</li>
 * </ul>
 *
 * <p>Matches {@code org.postgresql}'s behavior for the methods Conductor uses:
 * {@link #getArray()} returns a typed Java array, {@link #getBaseTypeName()}
 * returns the element type name, {@link #getBaseType()} the JDBC element type.
 */
public final class PdbArray implements Array {

    /** PG element type name as passed to createArrayOf (e.g. "text", "varchar"). */
    private final String baseTypeName;
    /** JDBC {@link Types} code for the element type. */
    private final int baseType;
    /** Element values (already normalized to plain Java objects). */
    private Object[] elements;

    PdbArray(String baseTypeName, Object[] elements) {
        // Normalize the element type name to a canonical PG name. createArrayOf
        // callers pass "text", "varchar", "VARCHAR", "int4", "integer", ...
        this.baseTypeName = canonicalTypeName(baseTypeName);
        this.baseType = jdbcTypeForName(this.baseTypeName);
        this.elements = elements == null ? new Object[0] : elements;
    }

    /**
     * The engine's declared-type stamp for a computed array whose element type
     * could not be resolved. Mirrors {@code ARRAY_DECL_SENTINEL} in
     * the server's PostgreSQL-wire layer / the server's operator utilities.
     */
    static final String ARRAY_DECL_SENTINEL = "_pdb_array";

    /**
     * Build a read-side array for a result column from its declared SQL type
     * (the Arrow field's {@code pdb:decl} stamp, e.g. {@code INT[]},
     * {@code TEXT[]}, {@code NUMERIC(10,2)[]}, or the {@code _pdb_array}
     * sentinel) plus the raw parsed elements.
     *
     * <p>The declared type is the ONLY sound source of the element type: the
     * engine ships an array column as an Arrow {@code Utf8} cell holding
     * {@code ARRAY[…]} text, in which a {@code text[]} element {@code '1'} and
     * an {@code int4[]} element {@code 1} are indistinguishable. A missing or
     * sentinel stamp therefore degrades to {@code text} - never to a type
     * guessed from the values.
     */
    static PdbArray forColumnDecl(String arrayDecl, Object[] elements) {
        return new PdbArray(elementTypeFromArrayDecl(arrayDecl), elements);
    }

    /**
     * Does a declared type name an ARRAY column? Mirrors
     * {@code field_is_array_decl} in the server's PostgreSQL-wire layer: a
     * trailing {@code []} (whatever the element type), or the computed-array
     * sentinel.
     *
     * <p>A null/absent stamp is deliberately NOT an array - the engine only
     * stamps an array-shaped decl on a real array column, and the PG wire
     * advertises an unstamped {@code Utf8} column as plain {@code text}. So the
     * driver reports it as text too instead of guessing from the cell contents.
     */
    static boolean isArrayDecl(String arrayDecl) {
        if (arrayDecl == null) return false;
        String d = arrayDecl.trim();
        return d.equalsIgnoreCase(ARRAY_DECL_SENTINEL) || d.endsWith("[]");
    }

    /**
     * Element type name from an array declared type: the text before the
     * trailing {@code []}. The sentinel, a null stamp, or a decl with no
     * {@code []} suffix all yield {@code text}. Mirrors
     * {@code array_element_oid_from_decl} in the server's PostgreSQL-wire layer.
     */
    static String elementTypeFromArrayDecl(String arrayDecl) {
        if (arrayDecl == null) return "text";
        String d = arrayDecl.trim();
        if (d.equalsIgnoreCase(ARRAY_DECL_SENTINEL)) return "text";
        if (!d.endsWith("[]")) return "text";
        String elem = d.substring(0, d.length() - 2).trim();
        return elem.isEmpty() ? "text" : elem;
    }

    static String canonicalTypeName(String t) {
        if (t == null) return "text";
        String s = t.trim().toLowerCase();
        // Drop any type modifier - `varchar(64)`, `numeric(10,2)` - before
        // matching, so a constrained element type still resolves to its family.
        // Mirrors `array_element_oid_from_elem_text` in the server's PostgreSQL-wire layer.
        int paren = s.indexOf('(');
        if (paren >= 0) s = s.substring(0, paren).trim();
        switch (s) {
            case "varchar":
            case "character varying":
            case "text":
            case "bpchar":
            case "char":
            case "character":
                return "text";
            case "int2":
            case "smallint":
                return "int2";
            case "int4":
            case "int":
            case "integer":
                return "int4";
            case "int8":
            case "bigint":
                return "int8";
            case "float4":
            case "real":
                return "float4";
            case "float8":
            case "double precision":
                return "float8";
            case "bool":
            case "boolean":
                return "bool";
            case "numeric":
            case "decimal":
                return "numeric";
            case "float":
                return "float8";
            case "timestamp":
            case "timestamp without time zone":
                return "timestamp";
            case "timestamptz":
            case "timestamp with time zone":
                return "timestamptz";
            default:
                return s;
        }
    }

    private static int jdbcTypeForName(String canonical) {
        switch (canonical) {
            case "text":    return Types.VARCHAR;
            case "int2":    return Types.SMALLINT;
            case "int4":    return Types.INTEGER;
            case "int8":    return Types.BIGINT;
            case "float4":  return Types.REAL;
            case "float8":  return Types.DOUBLE;
            case "bool":    return Types.BOOLEAN;
            case "numeric": return Types.NUMERIC;
            case "date":    return Types.DATE;
            case "timestamp":
            case "timestamptz": return Types.TIMESTAMP;
            case "bytea":   return Types.BINARY;
            default:        return Types.OTHER; // uuid, json, jsonb, ... (as pgjdbc)
        }
    }

    /**
     * The bind-side shape of this array ({@link org.particledb.wire.ArrayParams}):
     * dimensions plus every element converted to the element type, computed
     * once. {@link PdbConnection#createArrayOf} computes it eagerly, so an
     * unsupported element type or a bad element fails there.
     *
     * @throws java.sql.SQLFeatureNotSupportedException the element type cannot be bound
     * @throws java.sql.SQLDataException an element does not convert, or a nested array is ragged
     */
    org.particledb.wire.ArrayParams.Shape shape() throws SQLException {
        org.particledb.wire.ArrayParams.Shape sh = shape;
        if (sh == null) {
            sh = org.particledb.wire.ArrayParams.normalize(baseTypeName, elements);
            shape = sh;
        }
        return sh;
    }

    private volatile org.particledb.wire.ArrayParams.Shape shape;

    /**
     * Render this array to a PostgreSQL array literal usable inline in a SQL
     * string, e.g. {@code '{"a","b,c","x\"y"}'::text[]} -- exactly what the
     * server renders for the same array bound as a parameter. Used only when
     * the connection renders statements as text ({@code serverPrepare=false}).
     */
    String toSqlLiteral() throws SQLException {
        return shape().sqlLiteral();
    }

    private boolean quotesElements() {
        switch (baseType) {
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.BOOLEAN:
                return false;
            default:
                return true;
        }
    }

    @Override public String getBaseTypeName() { return baseTypeName; }
    @Override public int getBaseType() { return baseType; }

    /**
     * The Java component type this array's elements map to, matching what
     * {@code org.postgresql}'s {@code PgArray} returns for the same PG element
     * type. An element type the driver has no specific mapping for stays
     * {@code Object} (values pass through untouched) rather than being forced
     * into a lossy carrier.
     */
    private Class<?> componentType() {
        switch (baseTypeName) {
            case "text":        return String.class;
            case "int2":        return Short.class;
            case "int4":        return Integer.class;
            case "int8":        return Long.class;
            case "float4":      return Float.class;
            case "float8":      return Double.class;
            case "numeric":     return BigDecimal.class;
            case "bool":        return Boolean.class;
            case "uuid":        return java.util.UUID.class;
            case "date":        return java.sql.Date.class;
            case "timestamp":
            case "timestamptz": return java.sql.Timestamp.class;
            case "bytea":       return byte[].class;
            case "json":
            case "jsonb":       return String.class;
            default:            return Object.class;
        }
    }

    /**
     * Convert one raw element to the declared element type. Raw elements arrive
     * as Strings on the read path (parsed out of the {@code ARRAY[…]} cell) and
     * as real Java objects on the bind path, so both are accepted.
     *
     * <p>The conversion is driven ENTIRELY by the declared type - the value is
     * never inspected to decide what it is. A {@code text[]} element keeps its
     * String identity even when it reads like a number.
     */
    private Object coerceElement(Object e) throws SQLException {
        if (e == null) return null;
        Class<?> want = componentType();
        if (want == Object.class || want.isInstance(e)) {
            return want == String.class || want == Object.class ? e : want.cast(e);
        }
        String s = e.toString().trim();
        try {
            if (want == String.class)     return e.toString();
            if (want == java.util.UUID.class) return java.util.UUID.fromString(s);
            if (want == java.sql.Date.class) return java.sql.Date.valueOf(s);
            if (want == java.sql.Timestamp.class) return parseTimestamp(s);
            if (want == byte[].class)     return parseBytea(s);
            if (want == Boolean.class)    return parseBool(s);
            if (want == BigDecimal.class) return new BigDecimal(s);
            if (e instanceof Number) {
                Number n = (Number) e;
                if (want == Short.class)   return n.shortValue();
                if (want == Integer.class) return n.intValue();
                if (want == Long.class)    return n.longValue();
                if (want == Float.class)   return n.floatValue();
                if (want == Double.class)  return n.doubleValue();
            }
            if (want == Short.class)   return Short.valueOf(s);
            if (want == Integer.class) return Integer.valueOf(s);
            if (want == Long.class)    return Long.valueOf(s);
            if (want == Float.class)   return Float.valueOf(s);
            if (want == Double.class)  return Double.valueOf(s);
        } catch (IllegalArgumentException | java.time.DateTimeException nfe) {
            throw new SQLException("array element '" + s + "' is not a valid "
                    + baseTypeName + " value", nfe);
        }
        return e;
    }

    /**
     * A {@code timestamp} / {@code timestamptz} element's text. With an offset
     * ({@code 2024-07-01 12:00:00+00}, {@code +05:30}) it is the instant; without
     * one the JVM zone's wall clock -- the two {@link java.sql.Timestamp}
     * readings pgjdbc gives the same column types.
     */
    private static java.sql.Timestamp parseTimestamp(String s) {
        String t = s.replace('T', ' ');
        int sign = Math.max(t.lastIndexOf('+'), t.lastIndexOf('-'));
        if (t.endsWith("Z")) {
            return java.sql.Timestamp.from(java.time.OffsetDateTime.parse(s.replace(' ', 'T')).toInstant());
        }
        if (sign > 10) { // past yyyy-mm-dd: an offset
            String off = t.substring(sign);
            if (off.length() == 3) off = off + ":00";
            java.time.LocalDateTime l = java.sql.Timestamp.valueOf(t.substring(0, sign).trim()).toLocalDateTime();
            return java.sql.Timestamp.from(l.atOffset(java.time.ZoneOffset.of(off)).toInstant());
        }
        return java.sql.Timestamp.valueOf(t);
    }

    /**
     * A {@code bytea} element's text: {@code \x} hex (PostgreSQL), or the
     * native wire's {@code X'hex'} (which the ARRAY[...] parser hands over as
     * {@code X} followed by the hex).
     */
    private static byte[] parseBytea(String s) {
        String h = s.startsWith("\\x") ? s.substring(2)
                : (s.startsWith("X'") && s.endsWith("'")) ? s.substring(2, s.length() - 1)
                : (s.startsWith("X") || s.startsWith("x")) ? s.substring(1) : s;
        if (h.length() % 2 != 0) throw new IllegalArgumentException("odd-length bytea hex");
        byte[] out = new byte[h.length() / 2];
        for (int i = 0; i < out.length; i++) {
            int hi = Character.digit(h.charAt(2 * i), 16), lo = Character.digit(h.charAt(2 * i + 1), 16);
            if (hi < 0 || lo < 0) throw new IllegalArgumentException("not bytea hex");
            out[i] = (byte) ((hi << 4) | lo);
        }
        return out;
    }

    /** PG array text renders booleans as t/f; accept the spelled forms too. */
    private static Boolean parseBool(String s) throws SQLException {
        String v = s.trim().toLowerCase();
        switch (v) {
            case "t": case "true":  case "y": case "yes": case "on":  case "1": return Boolean.TRUE;
            case "f": case "false": case "n": case "no":  case "off": case "0": return Boolean.FALSE;
            default: throw new SQLException("array element '" + s + "' is not a valid bool value");
        }
    }

    /**
     * Build a typed Java array over {@code src}, coercing each element. A
     * multidimensional array (nested {@code Object[]} levels, as the read side
     * parses {@code {{1,2},{3,4}}} and as a caller may pass to
     * {@code createArrayOf}) becomes a nested typed array, {@code Integer[][]},
     * as pgjdbc returns it.
     */
    private Object typedArray(Object[] src) throws SQLException {
        int depth = depthOf(src);
        Class<?> comp = componentType();
        for (int d = 1; d < depth; d++) comp = java.lang.reflect.Array.newInstance(comp, 0).getClass();
        Object out = java.lang.reflect.Array.newInstance(comp, src.length);
        for (int i = 0; i < src.length; i++) {
            Object e = src[i];
            if (depth > 1) {
                java.lang.reflect.Array.set(out, i, e == null ? null : typedArray(asObjects(e)));
            } else {
                java.lang.reflect.Array.set(out, i, coerceElement(e));
            }
        }
        return out;
    }

    /** Is {@code e} a nested level (and not an element)? A {@code byte[]} is a bytea element. */
    private boolean isLevel(Object e) {
        if (e == null) return false;
        if (e instanceof byte[]) return !"bytea".equals(baseTypeName);
        return e instanceof Object[] || e instanceof java.util.Collection || e.getClass().isArray();
    }

    private static Object[] asObjects(Object e) {
        if (e instanceof Object[]) return (Object[]) e;
        if (e instanceof java.util.Collection) return ((java.util.Collection<?>) e).toArray();
        int n = java.lang.reflect.Array.getLength(e);
        Object[] out = new Object[n];
        for (int i = 0; i < n; i++) out[i] = java.lang.reflect.Array.get(e, i);
        return out;
    }

    /** Dimensions of {@code src}, along its first non-null entries. */
    private int depthOf(Object[] src) {
        for (Object e : src) {
            if (e == null) continue;
            return isLevel(e) ? 1 + depthOf(asObjects(e)) : 1;
        }
        return 1;
    }

    @Override public Object getArray() throws SQLException {
        // A typed Java array matching the DECLARED element type, so
        // `(Integer[]) rs.getArray(n).getArray()` works the way it does on
        // pgjdbc - int4[] → Integer[], text[] → String[], bool[] → Boolean[], …
        return typedArray(elements);
    }

    @Override public Object getArray(Map<String, Class<?>> map) throws SQLException { return getArray(); }

    @Override public Object getArray(long index, int count) throws SQLException {
        return typedArray(slice(index, count));
    }

    @Override public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return getArray(index, count);
    }

    /** 1-based, range-checked slice of the raw elements. */
    private Object[] slice(long index, int count) throws SQLException {
        int start = (int) (index - 1);
        if (start < 0 || start > elements.length) {
            throw new SQLException("array slice start out of range: " + index);
        }
        int end = Math.min(elements.length, start + Math.max(count, 0));
        Object[] out = new Object[Math.max(0, end - start)];
        System.arraycopy(elements, start, out, 0, out.length);
        return out;
    }

    /**
     * JDBC's two-column array result set: {@code INDEX} (1-based position,
     * BIGINT) and {@code VALUE} (the element, reported and carried as the
     * DECLARED element type - same typing {@link #getArray()} applies).
     */
    @Override public ResultSet getResultSet() throws SQLException {
        return getResultSet(1L, elements.length);
    }
    @Override public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        return getResultSet();
    }
    @Override public ResultSet getResultSet(long index, int count) throws SQLException {
        Object[] window = slice(index, count);
        List<Object[]> rows = new ArrayList<>(window.length);
        for (int i = 0; i < window.length; i++) {
            // INDEX is the element's position in the FULL array, not in the slice.
            rows.add(new Object[]{ index + i, coerceElement(window[i]) });
        }
        return new MetaResultSet(new String[]{"INDEX", "VALUE"},
                new int[]{Types.BIGINT, baseType}, rows);
    }
    @Override public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
            throws SQLException {
        return getResultSet(index, count);
    }

    @Override public void free() { elements = new Object[0]; }

    /**
     * The PG array literal for these elements plus the element type, e.g.
     * {@code {1,2}::int4[]}. Consistent with the typing {@link #getArray()}
     * reports, so a {@code text[]} of {@code "1"} never renders as an int array.
     */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder("{");
        boolean quote = quotesElements();
        for (int i = 0; i < elements.length; i++) {
            if (i > 0) sb.append(',');
            Object e = elements[i];
            if (e == null) sb.append("NULL");
            else if (quote) sb.append('"').append(e).append('"');
            else sb.append(e);
        }
        return sb.append("}::").append(baseTypeName).append("[]").toString();
    }

    /**
     * Parse a ParticleDB array TEXT rendering into element objects.
     * {@link PdbResultSet#getArray} routes a String array-cell value through here.
     *
     * <p>PDB's native wire renders an array column value as the SQL
     * {@code ARRAY[...]} constructor form with SINGLE-quoted elements
     * ({@code ARRAY['alpha','be,ta','gam"ma']}) - NOT the PostgreSQL external
     * text form {@code {…}} that pgjdbc expects (VERIFIED live against a running
     * PDB server). This parser therefore handles BOTH:
     * <ul>
     *   <li>{@code ARRAY['a','b',…]} - single-quoted elements, {@code ''} escapes
     *       an embedded single quote, a bare {@code NULL} token is a null element;
     *   <li>{@code {a,"b,c",…}} - the PG external text form (double-quoted
     *       elements, backslash escapes), kept as a defensive fallback for the
     *       PG-wire path / future rendering changes.
     * </ul>
     * A multidimensional array's inner levels ({@code ARRAY[ARRAY[1,2],…]},
     * {@code {{1,2},…}}) parse to nested {@code Object[]}.
     */
    static Object[] parsePgArrayText(String s) {
        if (s == null) return null;
        String t = s.trim();
        if (t.isEmpty()) return new Object[0];
        // ARRAY[...] constructor form (what the native wire emits).
        if (t.regionMatches(true, 0, "ARRAY[", 0, 6) && t.endsWith("]")) {
            return parseArrayCtor(t.substring(6, t.length() - 1));
        }
        // PG external text form { ... }.
        if (t.charAt(0) == '{' && t.charAt(t.length() - 1) == '}') {
            return parsePgBraceForm(t.substring(1, t.length() - 1));
        }
        // Bare single element (no wrapper) - return as a 1-element array.
        return new Object[]{ t };
    }

    /** Parse the inside of {@code ARRAY[ … ]} - single-quoted, comma-separated. */
    private static Object[] parseArrayCtor(String body) {
        if (body.trim().isEmpty()) return new Object[0];
        List<Object> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false, quotedElem = false;
        int depth = 0; // nested ARRAY[...] bracket depth
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (inQuotes) {
                if (c == '\'') {
                    if (i + 1 < body.length() && body.charAt(i + 1) == '\'') {
                        // Inside a nested level keep the text verbatim: the
                        // level is parsed again on its own.
                        cur.append(depth > 0 ? "''" : "'");
                        i++;
                    } else {
                        inQuotes = false;
                        if (depth > 0) cur.append('\'');
                    }
                } else {
                    cur.append(c);
                }
                continue;
            }
            switch (c) {
                case '\'':
                    inQuotes = true;
                    if (depth > 0) cur.append('\''); else quotedElem = true;
                    break;
                case '[': depth++; cur.append(c); break;
                case ']': depth--; cur.append(c); break;
                case ',':
                    if (depth > 0) { cur.append(c); }
                    else { out.add(finishCtorElement(cur, quotedElem)); cur.setLength(0); quotedElem = false; }
                    break;
                default: cur.append(c);
            }
        }
        out.add(finishCtorElement(cur, quotedElem));
        return out.toArray();
    }

    private static Object finishCtorElement(StringBuilder cur, boolean quoted) {
        if (quoted) return cur.toString(); // single-quoted: verbatim (unescaped)
        String tr = cur.toString().trim();
        if (tr.equalsIgnoreCase("NULL") || tr.isEmpty()) return null;
        // A multidimensional array's inner level: ARRAY[ARRAY[1,2],ARRAY[3,4]].
        if (tr.regionMatches(true, 0, "ARRAY[", 0, 6) && tr.endsWith("]")) {
            return parseArrayCtor(tr.substring(6, tr.length() - 1));
        }
        return tr; // unquoted numeric/bool element
    }

    /** Parse the inside of a PG external text {@code { … }} array. */
    private static Object[] parsePgBraceForm(String body) {
        if (body.isEmpty()) return new Object[0];
        List<Object> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false, quotedElem = false;
        int depth = 0;
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (inQuotes) {
                // Inside a nested level keep the text verbatim (escapes and
                // quotes included): the level is parsed again on its own.
                if (c == '\\' && i + 1 < body.length()) {
                    if (depth > 0) cur.append(c);
                    cur.append(body.charAt(++i));
                } else if (c == '"') {
                    inQuotes = false;
                    if (depth > 0) cur.append(c);
                } else { cur.append(c); }
                continue;
            }
            switch (c) {
                case '"':
                    inQuotes = true;
                    if (depth > 0) cur.append(c); else quotedElem = true;
                    break;
                case '{': depth++; cur.append(c); break;
                case '}': depth--; cur.append(c); break;
                case ',':
                    if (depth > 0) { cur.append(c); }
                    else { out.add(finishBraceElement(cur, quotedElem)); cur.setLength(0); quotedElem = false; }
                    break;
                default: cur.append(c);
            }
        }
        out.add(finishBraceElement(cur, quotedElem));
        return out.toArray();
    }

    private static Object finishBraceElement(StringBuilder cur, boolean quoted) {
        if (quoted) return cur.toString();
        String tr = cur.toString().trim();
        if (tr.equalsIgnoreCase("NULL")) return null;
        // A multidimensional array's inner level: {{1,2},{3,4}}.
        if (tr.length() >= 2 && tr.charAt(0) == '{' && tr.charAt(tr.length() - 1) == '}') {
            return parsePgBraceForm(tr.substring(1, tr.length() - 1));
        }
        return tr;
    }
}
