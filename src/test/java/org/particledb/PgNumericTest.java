package org.particledb;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link PgNumeric}: the integer conversions pgjdbc 42.7 applies to a
 * text-format numeric / float cell, and PostgreSQL 17's float text they are
 * computed from. Expected values were read from pgjdbc 42.7.11 on PostgreSQL
 * 17 ({@code NumericIntegerPg17DifferentialTest} re-checks them live) and
 * from {@code psql} with {@code extra_float_digits = 3}.
 */
public class PgNumericTest {

    private static void bad(String type, String text, Number v) {
        SQLException e = assertThrows(SQLException.class, () -> PgNumeric.toIntegral(v, type), v + " as " + type);
        assertEquals("22003", e.getSQLState());
        assertEquals("Bad value for type " + type + " : " + text, e.getMessage());
    }

    @Test
    void floatTextIsPostgresShortestText() {
        assertEquals("100000000000000", PgNumeric.pgFloatText(1e14));
        assertEquals("1e+15", PgNumeric.pgFloatText(1e15));
        assertEquals("123456789012345.6", PgNumeric.pgFloatText(123456789012345.6));
        assertEquals("0.0001", PgNumeric.pgFloatText(1e-4));
        assertEquals("1e-05", PgNumeric.pgFloatText(1e-5));
        assertEquals("1.5e-05", PgNumeric.pgFloatText(1.5e-5));
        assertEquals("1.25e+20", PgNumeric.pgFloatText(1.25e20));
        assertEquals("1e-300", PgNumeric.pgFloatText(1e-300));
        assertEquals("5e-324", PgNumeric.pgFloatText(Double.MIN_VALUE));
        assertEquals("9.223372036854776e+18", PgNumeric.pgFloatText(9.223372036854775807E18));
        assertEquals("-9.223372036854776e+18", PgNumeric.pgFloatText(-9.223372036854775807E18));
        assertEquals("2147483648.1", PgNumeric.pgFloatText(2147483648.1));
        assertEquals("32767", PgNumeric.pgFloatText(32767d));
        assertEquals("1.7", PgNumeric.pgFloatText(1.7));
        assertEquals("-0", PgNumeric.pgFloatText(-0d));
        assertEquals("0", PgNumeric.pgFloatText(0d));
        assertEquals("NaN", PgNumeric.pgFloatText(Double.NaN));
        assertEquals("-Infinity", PgNumeric.pgFloatText(Double.NEGATIVE_INFINITY));

        assertEquals("100000", PgNumeric.pgFloatText(1e5f));
        assertEquals("1e+06", PgNumeric.pgFloatText(1e6f));
        assertEquals("123456", PgNumeric.pgFloatText(123456f));
        assertEquals("1.234567e+06", PgNumeric.pgFloatText(1234567f));
        assertEquals("0.0001", PgNumeric.pgFloatText(0.0001f));
        assertEquals("1e-05", PgNumeric.pgFloatText(0.00001f));
        assertEquals("3.4028235e+38", PgNumeric.pgFloatText(Float.MAX_VALUE));
        // 2^31 sits at a power-of-two boundary: the shortest text lies below it.
        assertEquals("2.1474836e+09", PgNumeric.pgFloatText(2147483648f));
        assertEquals("9.223372e+18", PgNumeric.pgFloatText(9.223372036854775807E18f));
        assertEquals("1.7", PgNumeric.pgFloatText(1.7f));
        // 3.355445e+07 is the midpoint to the next real and reads back as this
        // one under round-half-even; PostgreSQL never emits a midpoint.
        assertEquals("3.3554448e+07", PgNumeric.pgFloatText(33554448f));
        assertEquals("1.7976931348623157e+308", PgNumeric.pgFloatText(Double.MAX_VALUE));
        assertEquals("-3.4028235e+38", PgNumeric.pgFloatText(-Float.MAX_VALUE));
        assertEquals("1e-45", PgNumeric.pgFloatText(Float.MIN_VALUE));
    }

    @Test
    void fractionsTruncateTowardZero() throws SQLException {
        assertEquals(1, PgNumeric.toIntegral(new BigDecimal("1.7"), "int"));
        assertEquals(-1, PgNumeric.toIntegral(new BigDecimal("-1.7"), "int"));
        assertEquals(2, PgNumeric.toIntegral(new BigDecimal("2.5"), "short"));
        assertEquals(0, PgNumeric.toIntegral(new BigDecimal("-0.5"), "byte"));
        assertEquals(0, PgNumeric.toIntegral(new BigDecimal("0.9999"), "long"));
        assertEquals(1, PgNumeric.toIntegral(1.7, "int"));
        assertEquals(-1, PgNumeric.toIntegral(-1.7f, "long"));
        assertEquals(Integer.MAX_VALUE, PgNumeric.toIntegral(new BigDecimal("2147483647.9"), "int"));
        assertEquals(Integer.MIN_VALUE, PgNumeric.toIntegral(-2147483648.9, "int"));
        assertEquals(Long.MAX_VALUE, PgNumeric.toIntegral(new BigDecimal("9223372036854775807.5"), "long"));
    }

    @Test
    void boundaries() throws SQLException {
        assertEquals(Long.MAX_VALUE, PgNumeric.toIntegral(new BigDecimal("9223372036854775807"), "long"));
        assertEquals(Long.MIN_VALUE, PgNumeric.toIntegral(new BigDecimal("-9223372036854775808"), "long"));
        bad("long", "9223372036854775808", new BigDecimal("9223372036854775808"));
        bad("long", "-9223372036854775809", new BigDecimal("-9223372036854775809"));
        bad("long", "18446744073709551616.00", new BigDecimal("18446744073709551616.00"));
        bad("long", "18446744073709551616", new BigInteger("18446744073709551616"));
        bad("int", "2147483648", 2147483648L);
        bad("int", "-2147483649", -2147483649L);
        bad("int", "2147483648.1", new BigDecimal("2147483648.1"));
        bad("short", "32768", 32768);
        bad("short", "-32769", new BigDecimal("-32769"));
        bad("byte", "128", (short) 128);
        bad("byte", "-129", -129.0);
        assertEquals(Integer.MAX_VALUE, PgNumeric.toIntegral(2147483647L, "int"));
        assertEquals(Short.MIN_VALUE, PgNumeric.toIntegral(-32768.0, "short"));
        assertEquals(Byte.MAX_VALUE, PgNumeric.toIntegral(127f, "byte"));
    }

    @Test
    void floatsConvertFromTheirPostgresText() throws SQLException {
        // A double 9223372036854775807 is 2^63 and is sent as 9.223372036854776e+18.
        bad("long", "9.223372036854776e+18", 9.223372036854775807E18);
        bad("long", "-9.223372036854776e+18", -9.223372036854775808E18);
        bad("long", "1e+19", 1e19);
        bad("int", "1e+300", 1e300);
        // A real 2147483648 is sent as 2.1474836e+09, which is 2147483600.
        assertEquals(2147483600L, PgNumeric.toIntegral(2147483648f, "int"));
        assertEquals(9223372000000000000L, PgNumeric.toIntegral(9.223372036854775807E18f, "long"));
        assertEquals(33554448, PgNumeric.toIntegral(33554448f, "int"));
        bad("int", "9.223372e+18", 9.223372036854775807E18f);
        bad("long", "NaN", Double.NaN);
        bad("int", "Infinity", Float.POSITIVE_INFINITY);
    }
}
