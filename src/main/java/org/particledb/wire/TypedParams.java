package org.particledb.wire;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * The typed {@link WireV2Codec#MSG_EXECUTE} parameters a server that echoed
 * {@link WireV2Codec#CAP_EXT_TYPED_PARAMS} accepts (tags 4..16), and the rule
 * that decides whether a value may travel typed at all.
 *
 * <p>THE RULE: a statement moved from "inline the values, send QUERY" to
 * "send EXECUTE with typed values" must parse to the same types and values.
 * Until the server binds values into cached plans it renders each typed value
 * back into the statement text (engine {@code native_literal}), in exactly the
 * spelling {@code PdbPreparedStatement.appendLiteral} uses for the same Java
 * value. A value is typed only when that holds BYTE FOR BYTE:
 *
 * <ul>
 *   <li>{@code Double} / {@code Float}: NaN and ±Infinity always (the server
 *       spells them {@code 'NaN'::float8} like the inline renderer); a finite
 *       value only when this JVM's {@code Double.toString} / {@code
 *       Float.toString} IS the canonical spelling the server produces
 *       ({@link #canonicalDouble}): the shortest round-trip digits, nearest
 *       the value, laid out as Java lays them out. A JDK before 19 prints a
 *       few values with extra digits; those keep the text path, because the
 *       inline text is exact NUMERIC and its digits ARE its value.</li>
 *   <li>{@code BigDecimal}: when {@code toString()} has no exponent (plain
 *       digits, which the server reproduces from scale + unscaled).</li>
 *   <li>{@code java.sql.Timestamp}, {@code java.sql.Date},
 *       {@code OffsetDateTime}: years 1..9999 (outside it the JDK's
 *       {@code toString} spelling is not a date literal), and no
 *       sub-microsecond part (the wire carries microseconds).</li>
 *   <li>{@code java.sql.Time}: always, as the {@code hh:mm:ss} its
 *       {@code toString()} shows (the inline text drops the milliseconds, so
 *       the typed value does too).</li>
 *   <li>{@code byte[]}, {@code UUID}: always.</li>
 * </ul>
 */
public final class TypedParams {

    private TypedParams() {}

    public static final byte PARAM_FLOAT8 = 4;
    public static final byte PARAM_FLOAT4 = 5;
    public static final byte PARAM_NUMERIC = 6;
    public static final byte PARAM_DATE = 7;
    public static final byte PARAM_TIMESTAMP = 8;
    public static final byte PARAM_TIMESTAMPTZ = 9;
    public static final byte PARAM_TIME = 10;
    public static final byte PARAM_UUID = 11;
    public static final byte PARAM_BYTEA = 12;

    /** The server's numeric scale cap (PostgreSQL's 16383). */
    private static final int MAX_NUMERIC_SCALE = 16383;

    /** True when {@code v} may travel as a typed tag 4..12 (see the class doc). */
    public static boolean typedBindable(Object v) {
        if (v instanceof Double) return doubleBindable((Double) v);
        if (v instanceof Float) return floatBindable((Float) v);
        if (v instanceof BigDecimal) return bigDecimalBindable((BigDecimal) v);
        if (v instanceof java.sql.Timestamp) {
            java.sql.Timestamp t = (java.sql.Timestamp) v;
            return t.getNanos() % 1000 == 0 && yearInRange(t.toLocalDateTime().getYear());
        }
        if (v instanceof java.sql.Date) return yearInRange(((java.sql.Date) v).toLocalDate().getYear());
        if (v instanceof java.sql.Time) return true;
        if (v instanceof OffsetDateTime) {
            OffsetDateTime o = (OffsetDateTime) v;
            return o.getNano() % 1000 == 0 && yearInRange(o.getYear());
        }
        return v instanceof byte[] || v instanceof UUID;
    }

    private static boolean yearInRange(int y) {
        return y >= 1 && y <= 9999;
    }

    static boolean doubleBindable(double d) {
        if (Double.isNaN(d) || Double.isInfinite(d)) return true;
        String s = Double.toString(d);
        // Fast path, no BigDecimal: a NORMAL double printed with at most 15
        // significant digits in Java's own layout is canonical. Two decimals of
        // <= 15 digits never read back as the same normal double (10^15 < 2^53),
        // so no shorter or nearer decimal of that length exists.
        if (Math.abs(d) >= Double.MIN_NORMAL && javaLayoutOf(s) && sigDigits(s) <= 15) return true;
        return s.equals(canonicalDouble(d));
    }

    static boolean floatBindable(float f) {
        if (Float.isNaN(f) || Float.isInfinite(f)) return true;
        String s = Float.toString(f);
        // As for doubles, with 6 digits (10^6 < 2^24).
        if (Math.abs(f) >= Float.MIN_NORMAL && javaLayoutOf(s) && sigDigits(s) <= 6) return true;
        return s.equals(canonicalFloat(f));
    }

    /**
     * {@code s} has the layout {@link #layout} produces for its own digits:
     * the fraction is "0" or ends in a non-zero digit (a JDK before 19 can
     * print {@code 0.0020}).
     */
    private static boolean javaLayoutOf(String s) {
        int e = s.indexOf('E');
        int end = e >= 0 ? e : s.length();
        int dot = s.indexOf('.');
        if (dot < 0 || dot >= end - 1) return false;
        return (end - dot == 2 && s.charAt(dot + 1) == '0') || s.charAt(end - 1) != '0';
    }

    static boolean bigDecimalBindable(BigDecimal b) {
        // No exponent in toString() <=> scale >= 0 and adjusted exponent >= -6.
        // Within the server's numeric envelope (131072 integer digits, scale
        // 16383): past it the inline text would parse as an approximate
        // float8, which a typed numeric (refused with 22003) does not.
        return b.scale() >= 0 && b.scale() <= MAX_NUMERIC_SCALE
                && (b.precision() - 1 - b.scale()) >= -6
                && (b.precision() - b.scale()) <= 131_072;
    }

    // ── Canonical float spelling (must equal the server's java_double_text) ──

    /**
     * The spelling the server renders for finite {@code d}: the smallest
     * {@code p} for which the decimal nearest {@code d} with {@code p}
     * significant digits reads back as {@code d} -- when that {@code p} is 1,
     * the nearest 2-digit decimal instead (JDK 19+'s rule; {@code 4.9E-324}) --
     * in {@code Double.toString}'s layout.
     */
    public static String canonicalDouble(double d) {
        if (d == 0.0) return (1.0 / d < 0) ? "-0.0" : "0.0";
        double a = Math.abs(d);
        BigDecimal exact = new BigDecimal(a);
        long bits = Double.doubleToRawLongBits(a);
        boolean pow2 = (bits & 0x000F_FFFF_FFFF_FFFFL) == 0;
        int hint = sigDigits(Double.toString(a));
        BigDecimal r = shortest(exact, hint, 17, pow2, x -> Double.parseDouble(x.toString()) == a);
        return layout(d < 0, r);
    }

    /** {@link #canonicalDouble} for a {@code float} ({@code Float.toString}). */
    public static String canonicalFloat(float f) {
        if (f == 0.0f) return (1.0f / f < 0) ? "-0.0" : "0.0";
        float a = Math.abs(f);
        BigDecimal exact = new BigDecimal((double) a);
        int bits = Float.floatToRawIntBits(a);
        boolean pow2 = (bits & 0x007F_FFFF) == 0;
        int hint = sigDigits(Float.toString(a));
        BigDecimal r = shortest(exact, hint, 9, pow2, x -> Float.parseFloat(x.toString()) == a);
        return layout(f < 0, r);
    }

    private interface RoundTrip { boolean test(BigDecimal x); }

    private static BigDecimal round(BigDecimal exact, int p) {
        return exact.round(new MathContext(p, RoundingMode.HALF_EVEN));
    }

    /**
     * Smallest p in 1..max whose nearest p-digit decimal round-trips, found by
     * walking from {@code hint}. "Round-trips at p" is monotone in p except at
     * a power of two (its rounding interval is asymmetric), where every smaller
     * p is tried.
     */
    private static BigDecimal shortest(BigDecimal exact, int hint, int max, boolean pow2, RoundTrip rt) {
        int p;
        if (pow2) {
            p = 1;
            while (p < max && !rt.test(round(exact, p))) p++;
        } else {
            p = Math.max(1, Math.min(hint, max));
            if (rt.test(round(exact, p))) {
                while (p > 1 && rt.test(round(exact, p - 1))) p--;
            } else {
                while (p < max && !rt.test(round(exact, p))) p++;
            }
        }
        if (p == 1) {
            BigDecimal two = round(exact, 2);
            if (rt.test(two)) return two;
        }
        return round(exact, p);
    }

    /** Significant digits of a Java float spelling ("1.0E10" -> 1, "0.0020" -> 1). */
    static int sigDigits(String s) {
        int e = s.indexOf('E');
        String m = e >= 0 ? s.substring(0, e) : s;
        StringBuilder digits = new StringBuilder();
        for (int i = 0; i < m.length(); i++) {
            char c = m.charAt(i);
            if (c >= '0' && c <= '9') digits.append(c);
        }
        int start = 0;
        while (start < digits.length() - 1 && digits.charAt(start) == '0') start++;
        int end = digits.length();
        while (end > start + 1 && digits.charAt(end - 1) == '0') end--;
        return end - start;
    }

    /** {@code Double.toString}'s layout of the positive decimal {@code r}. */
    static String layout(boolean neg, BigDecimal r) {
        BigDecimal s = r.stripTrailingZeros();
        String digits = s.unscaledValue().abs().toString();
        int exp = digits.length() - 1 - s.scale();
        StringBuilder out = new StringBuilder(digits.length() + 8);
        if (neg) out.append('-');
        if (exp >= -3 && exp < 7) {
            if (exp >= 0) {
                int intLen = exp + 1;
                if (digits.length() <= intLen) {
                    out.append(digits);
                    for (int i = digits.length(); i < intLen; i++) out.append('0');
                    out.append(".0");
                } else {
                    out.append(digits, 0, intLen).append('.').append(digits, intLen, digits.length());
                }
            } else {
                out.append("0.");
                for (int i = 0; i < -exp - 1; i++) out.append('0');
                out.append(digits);
            }
        } else {
            out.append(digits.charAt(0)).append('.');
            out.append(digits.length() > 1 ? digits.substring(1) : "0");
            out.append('E').append(exp);
        }
        return out.toString();
    }

    // ── Encoding ─────────────────────────────────────────────────────────

    /** Encoded size of a typed value (tag byte included); the value must be {@link #typedBindable}. */
    static int size(Object v) {
        if (v instanceof Double) return 9;
        if (v instanceof Float) return 5;
        if (v instanceof BigDecimal) return 9 + ((BigDecimal) v).unscaledValue().toByteArray().length;
        if (v instanceof java.sql.Timestamp) return 9;
        if (v instanceof java.sql.Date) return 5;
        if (v instanceof java.sql.Time) return 9;
        if (v instanceof OffsetDateTime) return 13;
        if (v instanceof UUID) return 17;
        if (v instanceof byte[]) return 5 + ((byte[]) v).length;
        throw new IllegalArgumentException("not a typed parameter: " + v.getClass());
    }

    /** Write tag + value at {@code p}; returns the new offset. */
    static int write(byte[] out, int p, Object v) {
        if (v instanceof Double) {
            out[p++] = PARAM_FLOAT8;
            return putLong(out, p, Double.doubleToRawLongBits((Double) v));
        }
        if (v instanceof Float) {
            out[p++] = PARAM_FLOAT4;
            return putInt(out, p, Float.floatToRawIntBits((Float) v));
        }
        if (v instanceof BigDecimal) {
            BigDecimal b = (BigDecimal) v;
            byte[] u = b.unscaledValue().toByteArray();
            out[p++] = PARAM_NUMERIC;
            p = putInt(out, p, b.scale());
            p = putInt(out, p, u.length);
            System.arraycopy(u, 0, out, p, u.length);
            return p + u.length;
        }
        if (v instanceof java.sql.Timestamp) {
            // The SAME fields toString() prints (the JVM zone's wall clock,
            // its hybrid Julian/Gregorian calendar), as microseconds of that
            // wall clock: the server prints those fields back.
            java.sql.Timestamp t = (java.sql.Timestamp) v;
            LocalDateTime l = t.toLocalDateTime();
            out[p++] = PARAM_TIMESTAMP;
            return putLong(out, p, l.toEpochSecond(ZoneOffset.UTC) * 1_000_000L + l.getNano() / 1000);
        }
        if (v instanceof java.sql.Date) {
            LocalDate l = ((java.sql.Date) v).toLocalDate();
            out[p++] = PARAM_DATE;
            return putInt(out, p, (int) l.toEpochDay());
        }
        if (v instanceof java.sql.Time) {
            java.time.LocalTime l = ((java.sql.Time) v).toLocalTime(); // hh:mm:ss, as toString()
            out[p++] = PARAM_TIME;
            return putLong(out, p, l.toSecondOfDay() * 1_000_000L);
        }
        if (v instanceof OffsetDateTime) {
            OffsetDateTime o = (OffsetDateTime) v;
            out[p++] = PARAM_TIMESTAMPTZ;
            p = putLong(out, p, o.toEpochSecond() * 1_000_000L + o.getNano() / 1000);
            return putInt(out, p, o.getOffset().getTotalSeconds());
        }
        if (v instanceof UUID) {
            UUID u = (UUID) v;
            out[p++] = PARAM_UUID;
            p = putLongBE(out, p, u.getMostSignificantBits());
            return putLongBE(out, p, u.getLeastSignificantBits());
        }
        if (v instanceof byte[]) {
            byte[] b = (byte[]) v;
            out[p++] = PARAM_BYTEA;
            p = putInt(out, p, b.length);
            System.arraycopy(b, 0, out, p, b.length);
            return p + b.length;
        }
        throw new IllegalArgumentException("not a typed parameter: " + v.getClass());
    }

    private static int putInt(byte[] out, int p, int v) {
        for (int b = 0; b < 4; b++) out[p++] = (byte) (v >>> (8 * b));
        return p;
    }

    private static int putLong(byte[] out, int p, long v) {
        for (int b = 0; b < 8; b++) out[p++] = (byte) (v >>> (8 * b));
        return p;
    }

    private static int putLongBE(byte[] out, int p, long v) {
        for (int b = 7; b >= 0; b--) out[p++] = (byte) (v >>> (8 * b));
        return p;
    }
}
