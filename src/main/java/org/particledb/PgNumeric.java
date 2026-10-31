package org.particledb;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.SQLException;

/**
 * Integer conversions of a numeric or floating cell, as pgjdbc 42.7 performs
 * them on a text-format result (a Statement, and a PreparedStatement before
 * its prepare threshold): the value is the text PostgreSQL sends, truncated
 * toward zero ({@code getInt} on 1.7 is 1, on -1.7 is -1), and a value outside
 * the target type raises SQLSTATE 22003 {@code Bad value for type int : <text>}.
 *
 * <p>A floating value is converted from PostgreSQL's text for it (the shortest
 * decimal that reads back as the same value, {@link #pgFloatText}), as pgjdbc
 * does: a {@code real} 2147483648 is sent as {@code 2.1474836e+09}, so its
 * {@code getInt} is 2147483600, and a {@code double precision}
 * 9223372036854775807 is {@code 9.223372036854776e+18}, above
 * {@code Long.MAX_VALUE}, so its {@code getLong} fails.
 *
 * <p>Public so the Java SDK's {@code Row} converts exactly as this driver
 * does; not intended for application use.
 */
public final class PgNumeric {

    private PgNumeric() {}

    /** pgjdbc's error for a value its target type cannot hold, SQLSTATE 22003. */
    public static SQLException badValue(String typeName, String text) {
        return new SQLException("Bad value for type " + typeName + " : " + text, "22003");
    }

    /**
     * {@code v} (a non-null {@link Number}) as the Java integer type named
     * {@code typeName} ({@code long}, {@code int}, {@code short} or
     * {@code byte}), truncated toward zero.
     *
     * @throws SQLException 22003 when the value is NaN, infinite or outside
     *         the type's range
     */
    public static long toIntegral(Number v, String typeName) throws SQLException {
        long lo, hi;
        switch (typeName) {
            case "long": lo = Long.MIN_VALUE; hi = Long.MAX_VALUE; break;
            case "int": lo = Integer.MIN_VALUE; hi = Integer.MAX_VALUE; break;
            case "short": lo = Short.MIN_VALUE; hi = Short.MAX_VALUE; break;
            case "byte": lo = Byte.MIN_VALUE; hi = Byte.MAX_VALUE; break;
            default: throw new IllegalArgumentException("not an integer type: " + typeName);
        }
        if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
            long x = v.longValue();
            if (x < lo || x > hi) throw badValue(typeName, Long.toString(x));
            return x;
        }
        String text;
        BigInteger whole;
        if (v instanceof BigDecimal) {
            text = ((BigDecimal) v).toPlainString();
            whole = ((BigDecimal) v).toBigInteger();
        } else if (v instanceof BigInteger) {
            text = v.toString();
            whole = (BigInteger) v;
        } else if (v instanceof Float || v instanceof Double) {
            text = pgFloatText(v);
            double d = v.doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) throw badValue(typeName, text);
            whole = new BigDecimal(text).toBigInteger();
        } else {
            long x = v.longValue();
            if (x < lo || x > hi) throw badValue(typeName, Long.toString(x));
            return x;
        }
        if (whole.compareTo(BigInteger.valueOf(lo)) < 0 || whole.compareTo(BigInteger.valueOf(hi)) > 0) {
            throw badValue(typeName, text);
        }
        return whole.longValue();
    }

    /**
     * PostgreSQL's text for a {@code real} ({@link Float}) or
     * {@code double precision} ({@link Double}) value, as PostgreSQL 12 and
     * later send it: the shortest decimal that reads back as the same value,
     * in fixed notation when its decimal exponent is from -4 up to 5
     * ({@code real}) or 14 ({@code double precision}), otherwise as
     * {@code d.ddde+XX}.
     */
    public static String pgFloatText(Number v) {
        boolean single = v instanceof Float;
        double d = v.doubleValue();
        if (Double.isNaN(d)) return "NaN";
        if (Double.isInfinite(d)) return d > 0 ? "Infinity" : "-Infinity";
        if (d == 0) return (1 / d < 0) ? "-0" : "0";
        BigDecimal exact = new BigDecimal(d);
        // The decimals that stand for this value lie STRICTLY between the
        // midpoints to its neighbours: PostgreSQL's Ryu never emits a midpoint,
        // even one that would read back as this value under round-half-even
        // (a real 33554448 is 3.3554448e+07, not 3.355445e+07).
        double below = single ? Math.nextDown((float) d) : Math.nextDown(d);
        double above = single ? Math.nextUp((float) d) : Math.nextUp(d);
        BigDecimal two = BigDecimal.valueOf(2);
        // Past the largest finite value the gap is the one on the other side.
        BigDecimal lo = Double.isInfinite(below) ? null : exact.add(new BigDecimal(below)).divide(two);
        BigDecimal hi = Double.isInfinite(above) ? null : exact.add(new BigDecimal(above)).divide(two);
        if (lo == null) lo = exact.subtract(hi.subtract(exact));
        if (hi == null) hi = exact.add(exact.subtract(lo));
        BigDecimal best = null;
        for (int p = 1; p <= (single ? 9 : 17) && best == null; p++) {
            for (RoundingMode m : new RoundingMode[]{RoundingMode.HALF_EVEN, RoundingMode.FLOOR, RoundingMode.CEILING}) {
                BigDecimal c = exact.round(new MathContext(p, m));
                boolean same = c.compareTo(lo) > 0 && c.compareTo(hi) < 0;
                if (same && (best == null
                        || c.subtract(exact).abs().compareTo(best.subtract(exact).abs()) < 0)) {
                    best = c;
                }
            }
        }
        if (best == null) best = exact;
        best = best.stripTrailingZeros();
        int exp = best.precision() - best.scale() - 1;
        if (exp >= -4 && exp < (single ? 6 : 15)) return best.toPlainString();
        String digits = best.unscaledValue().abs().toString();
        StringBuilder s = new StringBuilder();
        if (best.signum() < 0) s.append('-');
        s.append(digits.charAt(0));
        if (digits.length() > 1) s.append('.').append(digits, 1, digits.length());
        s.append('e').append(exp < 0 ? '-' : '+');
        int a = Math.abs(exp);
        if (a < 10) s.append('0');
        return s.append(a).toString();
    }
}
