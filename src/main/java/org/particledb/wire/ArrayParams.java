package org.particledb.wire;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * The {@link WireV2Codec#MSG_EXECUTE} ARRAY parameter (tag 17), offered by a
 * server that echoed {@link WireV2Codec#CAP_EXT_ARRAY_PARAMS}:
 *
 * <pre>
 * [u8 17][u8 elem_tag][u8 ndims][ndims x u32 LE len]
 * then len_1 x .. x len_n elements, row-major, each [u8 tag][value]
 * with tag = elem_tag, or 0 for a NULL element
 * </pre>
 *
 * Elements are encoded exactly as top-level parameters of {@code elem_tag}
 * (server: the server's native-wire parameter decoder). {@code ndims} 0 is the
 * empty array; at most {@link #MAX_DIMS}; every length at least 1; lower
 * bounds are 1.
 *
 * <p>One normalisation ({@link #normalize}) feeds both the wire encoding and
 * the inline SQL literal ({@link Shape#sqlLiteral}) the driver renders only
 * when the user turned server-side binding off ({@code serverPrepare=false}).
 * The literal spells every element as the server spells the bound array
 * ({@code native_literal::array_external_text}), so both paths store the same
 * value.
 */
public final class ArrayParams {

    private ArrayParams() {}

    public static final byte PARAM_ARRAY = 17;
    /** PostgreSQL's MAXDIM, enforced by the server too. */
    public static final int MAX_DIMS = 6;

    /** The element types an array parameter can carry (canonical names, {@code PdbArray.canonicalTypeName}). */
    public static final List<String> SUPPORTED_TYPES = List.of(
            "text", "int2", "int4", "int8", "float4", "float8", "bool", "numeric",
            "uuid", "timestamp", "timestamptz", "date", "bytea", "json", "jsonb");

    /** The wire tag of an element type, or -1 when arrays of it cannot be bound. */
    public static int elemTag(String canonical) {
        switch (canonical) {
            case "int8":        return WireV2Codec.PARAM_I64;
            case "text":        return WireV2Codec.PARAM_TEXT;
            case "bool":        return WireV2Codec.PARAM_BOOL;
            case "float8":      return TypedParams.PARAM_FLOAT8;
            case "float4":      return TypedParams.PARAM_FLOAT4;
            case "numeric":     return TypedParams.PARAM_NUMERIC;
            case "date":        return TypedParams.PARAM_DATE;
            case "timestamp":   return TypedParams.PARAM_TIMESTAMP;
            case "timestamptz": return TypedParams.PARAM_TIMESTAMPTZ;
            case "uuid":        return TypedParams.PARAM_UUID;
            case "bytea":       return TypedParams.PARAM_BYTEA;
            case "int2":        return 13;
            case "int4":        return 14;
            case "json":        return 15;
            case "jsonb":       return 16;
            default:            return -1;
        }
    }

    /** {@link SQLFeatureNotSupportedException} for an element type no array parameter carries. */
    public static SQLFeatureNotSupportedException unsupportedType(String typeName) {
        return new SQLFeatureNotSupportedException("arrays of type \"" + typeName
                + "\" cannot be bound as parameters; supported element types: "
                + String.join(", ", SUPPORTED_TYPES), "0A000");
    }

    /** A parameter value already in its tag-17 bytes (tag included). */
    public static final class Encoded {
        public final byte[] bytes;
        Encoded(byte[] bytes) { this.bytes = bytes; }
    }

    /**
     * An array's dimensions and its elements flattened row-major, each null or
     * normalised to the one Java carrier of its element type: {@code Long}
     * (int2/4/8), {@code Double}, {@code Float}, {@code BigDecimal},
     * {@code Boolean}, {@code String} (text/json/jsonb), {@code UUID},
     * {@code LocalDate}, {@code Long} micros of a wall clock (timestamp),
     * {@code OffsetDateTime} at whole microseconds (timestamptz),
     * {@code byte[]}.
     */
    public static final class Shape {
        public final String type;
        public final int[] dims;
        public final Object[] flat;

        Shape(String type, int[] dims, Object[] flat) {
            this.type = type;
            this.dims = dims;
            this.flat = flat;
        }

        /** The tag-17 parameter. */
        public Encoded encode() {
            int tag = elemTag(type);
            java.io.ByteArrayOutputStream b = new java.io.ByteArrayOutputStream(16 + flat.length * 9);
            b.write(PARAM_ARRAY);
            b.write(tag);
            b.write(dims.length);
            for (int d : dims) writeInt(b, d);
            for (Object v : flat) {
                if (v == null) { b.write(WireV2Codec.PARAM_NULL); continue; }
                b.write(tag);
                switch (type) {
                    case "int8":  writeLong(b, (Long) v); break;
                    case "int4":  writeInt(b, (int) (long) (Long) v); break;
                    case "int2":  { short s = (short) (long) (Long) v; b.write(s & 0xFF); b.write((s >> 8) & 0xFF); break; }
                    case "bool":  b.write(((Boolean) v) ? 1 : 0); break;
                    case "float8": writeLong(b, Double.doubleToRawLongBits((Double) v)); break;
                    case "float4": writeInt(b, Float.floatToRawIntBits((Float) v)); break;
                    case "numeric": {
                        BigDecimal d = (BigDecimal) v;
                        byte[] u = d.unscaledValue().toByteArray();
                        writeInt(b, d.scale());
                        writeInt(b, u.length);
                        b.write(u, 0, u.length);
                        break;
                    }
                    case "date": writeInt(b, (int) ((LocalDate) v).toEpochDay()); break;
                    case "timestamp": writeLong(b, (Long) v); break;
                    case "timestamptz": {
                        OffsetDateTime o = (OffsetDateTime) v;
                        writeLong(b, o.toEpochSecond() * 1_000_000L + o.getNano() / 1000);
                        writeInt(b, o.getOffset().getTotalSeconds());
                        break;
                    }
                    case "uuid": {
                        UUID u = (UUID) v;
                        writeLongBE(b, u.getMostSignificantBits());
                        writeLongBE(b, u.getLeastSignificantBits());
                        break;
                    }
                    case "bytea": { byte[] x = (byte[]) v; writeInt(b, x.length); b.write(x, 0, x.length); break; }
                    default: { // text, json, jsonb
                        byte[] x = ((String) v).getBytes(StandardCharsets.UTF_8);
                        writeInt(b, x.length);
                        b.write(x, 0, x.length);
                    }
                }
            }
            return new Encoded(b.toByteArray());
        }

        /**
         * The inline SQL literal, {@code '{"a",NULL}'::text[]}: what the server
         * renders for the same bound array, element for element.
         */
        public String sqlLiteral() {
            StringBuilder body = new StringBuilder(2 + flat.length * 8);
            if (dims.length == 0) {
                body.append("{}");
            } else {
                int[] blocks = new int[dims.length];
                int acc = 1;
                for (int d = dims.length - 1; d >= 0; d--) { acc *= dims[d]; blocks[d] = acc; }
                for (int k = 0; k < flat.length; k++) {
                    if (k > 0) body.append(',');
                    for (int bl : blocks) if (k % bl == 0) body.append('{');
                    appendElement(body, flat[k]);
                    for (int bl : blocks) if ((k + 1) % bl == 0) body.append('}');
                }
            }
            StringBuilder sb = new StringBuilder(body.length() + 16);
            sb.append('\'');
            for (int i = 0; i < body.length(); i++) {
                char c = body.charAt(i);
                if (c == '\'') sb.append('\'');
                sb.append(c);
            }
            return sb.append("'::").append(type).append("[]").toString();
        }

        private void appendElement(StringBuilder sb, Object v) {
            if (v == null) { sb.append("NULL"); return; }
            switch (type) {
                case "int2": case "int4": case "int8": sb.append(v); return;
                case "bool": sb.append(((Boolean) v) ? "true" : "false"); return;
                case "float8": {
                    double d = (Double) v;
                    sb.append(Double.isNaN(d) ? "NaN" : d == Double.POSITIVE_INFINITY ? "Infinity"
                            : d == Double.NEGATIVE_INFINITY ? "-Infinity" : Double.toString(d));
                    return;
                }
                case "float4": {
                    float f = (Float) v;
                    sb.append(Float.isNaN(f) ? "NaN" : f == Float.POSITIVE_INFINITY ? "Infinity"
                            : f == Float.NEGATIVE_INFINITY ? "-Infinity" : Float.toString(f));
                    return;
                }
                case "numeric": sb.append(((BigDecimal) v).toPlainString()); return;
                case "date": quote(sb, dateText((LocalDate) v)); return;
                case "timestamp": quote(sb, timestampText((Long) v)); return;
                case "timestamptz": quote(sb, ((OffsetDateTime) v).toString()); return;
                case "bytea": {
                    StringBuilder h = new StringBuilder(2 + 2 * ((byte[]) v).length).append("\\x");
                    for (byte x : (byte[]) v) h.append(Character.forDigit((x >> 4) & 0xF, 16)).append(Character.forDigit(x & 0xF, 16));
                    quote(sb, h.toString());
                    return;
                }
                default: quote(sb, v.toString()); // text, json, jsonb, uuid
            }
        }
    }

    private static void quote(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' || c == '"') sb.append('\\');
            sb.append(c);
        }
        sb.append('"');
    }

    /** {@code native_literal::date_text}: {@code yyyy-mm-dd}, {@code BC} before year 1. */
    static String dateText(LocalDate d) {
        int y = d.getYear();
        boolean bc = y <= 0;
        String s = String.format("%04d-%02d-%02d", bc ? 1 - y : y, d.getMonthValue(), d.getDayOfMonth());
        return bc ? s + " BC" : s;
    }

    /** {@code native_literal::timestamp_text} of wall-clock micros. */
    static String timestampText(long micros) {
        long days = Math.floorDiv(micros, 86_400_000_000L);
        long tod = Math.floorMod(micros, 86_400_000_000L);
        StringBuilder s = new StringBuilder(dateText(LocalDate.ofEpochDay(days)));
        boolean bc = s.toString().endsWith(" BC");
        if (bc) s.setLength(s.length() - 3);
        long secs = tod / 1_000_000L, frac = tod % 1_000_000L;
        s.append(String.format(" %02d:%02d:%02d.", secs / 3600, secs / 60 % 60, secs % 60));
        if (frac == 0) s.append('0');
        else {
            String f = String.format("%06d", frac);
            int end = f.length();
            while (f.charAt(end - 1) == '0') end--;
            s.append(f, 0, end);
        }
        if (bc) s.append(" BC");
        return s.toString();
    }

    // ── normalisation ───────────────────────────────────────────────────────

    /**
     * The shape of {@code elements} as an array of {@code type} (a canonical
     * name): nested {@code Object[]}, primitive arrays and {@code Collection}s
     * are dimensions (a {@code byte[]} is an element of a {@code bytea} array),
     * which must be rectangular and at most {@link #MAX_DIMS} deep.
     *
     * @throws SQLFeatureNotSupportedException {@code type} is not an element type an array parameter carries
     * @throws SQLDataException an element cannot be converted to {@code type}, or the nesting is ragged
     */
    public static Shape normalize(String type, Object[] elements) throws SQLException {
        if (elemTag(type) < 0) throw unsupportedType(type);
        List<Integer> dims = new ArrayList<>();
        List<Object> flat = new ArrayList<>();
        Object[] top = elements == null ? new Object[0] : elements;
        if (top.length > 0) {
            shapeOf(type, top, 0, dims);
            flatten(type, top, 0, dims, flat);
        }
        int[] d = new int[dims.size()];
        for (int i = 0; i < d.length; i++) d[i] = dims.get(i);
        if (flat.isEmpty()) d = new int[0];
        Object[] out = flat.toArray();
        for (int i = 0; i < out.length; i++) out[i] = convert(type, out[i], i);
        return new Shape(type, d, out);
    }

    private static boolean isNested(String type, Object v) {
        if (v == null) return false;
        if (v instanceof byte[]) return !"bytea".equals(type);
        return v instanceof Object[] || v instanceof Collection || v.getClass().isArray();
    }

    private static Object[] asArray(Object v) {
        if (v instanceof Object[]) return (Object[]) v;
        if (v instanceof Collection) return ((Collection<?>) v).toArray();
        int n = java.lang.reflect.Array.getLength(v);
        Object[] out = new Object[n];
        for (int i = 0; i < n; i++) out[i] = java.lang.reflect.Array.get(v, i);
        return out;
    }

    /** Record the dimensions along the first element at each depth. */
    private static void shapeOf(String type, Object[] level, int depth, List<Integer> dims) throws SQLException {
        if (depth >= MAX_DIMS) {
            throw new SQLDataException("array has more than " + MAX_DIMS + " dimensions", "54000");
        }
        dims.add(level.length);
        if (level.length > 0 && isNested(type, level[0])) shapeOf(type, asArray(level[0]), depth + 1, dims);
    }

    private static void flatten(String type, Object[] level, int depth, List<Integer> dims, List<Object> out)
            throws SQLException {
        if (level.length != dims.get(depth)) {
            throw new SQLDataException("multidimensional arrays must be rectangular (dimension "
                    + (depth + 1) + " has lengths " + dims.get(depth) + " and " + level.length + ")", "2202E");
        }
        boolean leaf = depth == dims.size() - 1;
        for (Object v : level) {
            if (leaf) {
                if (isNested(type, v)) {
                    throw new SQLDataException("multidimensional arrays must be rectangular"
                            + " (a nested array where dimension " + (depth + 1) + " holds elements)", "2202E");
                }
                out.add(v);
            } else {
                if (v == null || !isNested(type, v)) {
                    throw new SQLDataException("multidimensional arrays must be rectangular"
                            + " (an element where dimension " + (depth + 2) + " expects an array)", "2202E");
                }
                flatten(type, asArray(v), depth + 1, dims, out);
            }
        }
    }

    private static SQLDataException bad(String type, Object v, int i, String why) {
        return new SQLDataException("array element " + (i + 1) + " (" + v.getClass().getSimpleName() + " "
                + abbreviate(v) + ") is not a valid " + type + " value" + (why == null ? "" : ": " + why), "22P02");
    }

    private static String abbreviate(Object v) {
        String s = v instanceof byte[] ? "byte[" + ((byte[]) v).length + "]" : String.valueOf(v);
        return s.length() > 64 ? s.substring(0, 61) + "..." : s;
    }

    static Object convert(String type, Object v, int i) throws SQLException {
        if (v == null) return null;
        try {
            switch (type) {
                case "text": case "json": case "jsonb":
                    return v.toString();
                case "int2": return integral(type, v, i, Short.MIN_VALUE, Short.MAX_VALUE);
                case "int4": return integral(type, v, i, Integer.MIN_VALUE, Integer.MAX_VALUE);
                case "int8": return integral(type, v, i, Long.MIN_VALUE, Long.MAX_VALUE);
                case "float8":
                    if (v instanceof Number) return ((Number) v).doubleValue();
                    return Double.valueOf(v.toString().trim());
                case "float4":
                    if (v instanceof Number) return ((Number) v).floatValue();
                    return Float.valueOf(v.toString().trim());
                case "numeric": {
                    if (v instanceof BigDecimal) return v;
                    if (v instanceof BigInteger) return new BigDecimal((BigInteger) v);
                    if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
                        return BigDecimal.valueOf(((Number) v).longValue());
                    }
                    if (v instanceof Double || v instanceof Float) {
                        double d = ((Number) v).doubleValue();
                        if (Double.isNaN(d) || Double.isInfinite(d)) throw bad(type, v, i, "not finite");
                        return new BigDecimal(v.toString());
                    }
                    return new BigDecimal(v.toString().trim());
                }
                case "bool": {
                    if (v instanceof Boolean) return v;
                    switch (v.toString().trim().toLowerCase()) {
                        case "t": case "true": case "y": case "yes": case "on": case "1": return Boolean.TRUE;
                        case "f": case "false": case "n": case "no": case "off": case "0": return Boolean.FALSE;
                        default: throw bad(type, v, i, null);
                    }
                }
                case "uuid":
                    if (v instanceof UUID) return v;
                    return UUID.fromString(v.toString().trim());
                case "date":
                    if (v instanceof LocalDate) return v;
                    if (v instanceof java.sql.Date) return ((java.sql.Date) v).toLocalDate();
                    if (v instanceof java.sql.Timestamp) return ((java.sql.Timestamp) v).toLocalDateTime().toLocalDate();
                    if (v instanceof LocalDateTime) return ((LocalDateTime) v).toLocalDate();
                    if (v instanceof java.util.Date) return new java.sql.Date(((java.util.Date) v).getTime()).toLocalDate();
                    return LocalDate.parse(v.toString().trim());
                case "timestamp": {
                    LocalDateTime l;
                    if (v instanceof java.sql.Timestamp) l = ((java.sql.Timestamp) v).toLocalDateTime();
                    else if (v instanceof LocalDateTime) l = (LocalDateTime) v;
                    else if (v instanceof LocalDate) l = ((LocalDate) v).atStartOfDay();
                    else if (v instanceof java.sql.Date) l = ((java.sql.Date) v).toLocalDate().atStartOfDay();
                    else if (v instanceof java.util.Date) l = new java.sql.Timestamp(((java.util.Date) v).getTime()).toLocalDateTime();
                    else l = java.sql.Timestamp.valueOf(v.toString().trim()).toLocalDateTime();
                    return wallMicros(l);
                }
                case "timestamptz": {
                    OffsetDateTime o;
                    ZoneId zone = ZoneId.systemDefault();
                    if (v instanceof OffsetDateTime) o = (OffsetDateTime) v;
                    else if (v instanceof ZonedDateTime) o = ((ZonedDateTime) v).toOffsetDateTime();
                    else if (v instanceof Instant) o = ((Instant) v).atZone(zone).toOffsetDateTime();
                    else if (v instanceof java.sql.Timestamp) o = ((java.sql.Timestamp) v).toInstant().atZone(zone).toOffsetDateTime();
                    else if (v instanceof LocalDateTime) o = ((LocalDateTime) v).atZone(zone).toOffsetDateTime();
                    else if (v instanceof java.util.Date) o = ((java.util.Date) v).toInstant().atZone(zone).toOffsetDateTime();
                    else o = OffsetDateTime.parse(v.toString().trim());
                    return roundMicros(o);
                }
                case "bytea":
                    if (v instanceof byte[]) return v;
                    throw bad(type, v, i, "a bytea element must be a byte[]");
                default:
                    throw unsupportedType(type);
            }
        } catch (SQLException e) {
            throw e;
        } catch (RuntimeException e) {
            throw bad(type, v, i, e.getMessage());
        }
    }

    private static Long integral(String type, Object v, int i, long min, long max) throws SQLException {
        long l;
        if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
            l = ((Number) v).longValue();
        } else if (v instanceof BigInteger || v instanceof BigDecimal) {
            BigDecimal d = v instanceof BigInteger ? new BigDecimal((BigInteger) v) : (BigDecimal) v;
            try {
                l = d.longValueExact();
            } catch (ArithmeticException e) {
                throw outOfRange(type, v, i);
            }
        } else if (v instanceof Number) {
            throw bad(type, v, i, "not an integer");
        } else {
            try {
                l = Long.parseLong(v.toString().trim());
            } catch (NumberFormatException e) {
                throw bad(type, v, i, null);
            }
        }
        if (l < min || l > max) throw outOfRange(type, v, i);
        return l;
    }

    private static SQLDataException outOfRange(String type, Object v, int i) {
        return new SQLDataException("array element " + (i + 1) + " (" + abbreviate(v)
                + ") is out of range for type " + type, "22003");
    }

    /** Wall-clock micros of {@code l}, rounded to the microsecond as PostgreSQL rounds (half to even). */
    static long wallMicros(LocalDateTime l) {
        long micros = l.toEpochSecond(ZoneOffset.UTC) * 1_000_000L + l.getNano() / 1000;
        int rem = l.getNano() % 1000;
        if (rem > 500 || (rem == 500 && (micros & 1) == 1)) micros++;
        return micros;
    }

    private static OffsetDateTime roundMicros(OffsetDateTime o) {
        int rem = o.getNano() % 1000;
        if (rem == 0) return o;
        long micros = o.getNano() / 1000;
        OffsetDateTime t = o.withNano((int) (micros * 1000));
        if (rem > 500 || (rem == 500 && (micros & 1) == 1)) t = t.plusNanos(1000);
        return t;
    }

    private static void writeInt(java.io.ByteArrayOutputStream b, int v) {
        for (int k = 0; k < 4; k++) b.write((v >>> (8 * k)) & 0xFF);
    }

    private static void writeLong(java.io.ByteArrayOutputStream b, long v) {
        for (int k = 0; k < 8; k++) b.write((int) ((v >>> (8 * k)) & 0xFF));
    }

    private static void writeLongBE(java.io.ByteArrayOutputStream b, long v) {
        for (int k = 7; k >= 0; k--) b.write((int) ((v >>> (8 * k)) & 0xFF));
    }
}
