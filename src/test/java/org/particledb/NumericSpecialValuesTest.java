package org.particledb;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * NaN, Infinity and -Infinity through every scalar getter, decoded from Arrow
 * streams in the shapes the engine ships: {@code real} as Float32,
 * {@code double precision} as Float64, an unconstrained {@code numeric} as
 * Utf8 stamped {@code pdb:decl=numeric}, and a {@code numeric(10,2)} as Utf8
 * stamped {@code pdb:decl=NUMERIC(10,2)} (a batch holding a NaN cannot be a
 * Decimal array).
 *
 * <p>The expected values are pgjdbc 42.7.11's against PostgreSQL 17:
 * getObject is a Float for {@code real} and a Double for the other two types,
 * getBigDecimal raises 22003 {@code Bad value for type BigDecimal : NaN}, and
 * the integer and boolean getters refuse the value instead of answering 0,
 * Long.MAX_VALUE or true. 1.0.10 returned the numeric specials as Strings.
 * {@code NumericSpecialPg17DifferentialTest} checks the same over the wire
 * against the oracle itself.
 */
@Tag("arrow-java")
public class NumericSpecialValuesTest {

    private static final String[] TEXT = {"NaN", "Infinity", "-Infinity"};
    private static final double[] VALUE = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};

    private static PdbResultSet stream(ArrowDecoder.ResultDecoder decoder) throws Exception {
        return ArrowIpc.build()
                .float32("r", Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 1.5f)
                .float64("d", Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 1.5)
                .utf8Decl("n", "numeric", "NaN", "Infinity", "-Infinity", "1.5")
                .utf8Decl("n2", "NUMERIC(10,2)", "NaN", "NaN", "NaN", "1.50")
                .utf8("t", "NaN", "Infinity", "-Infinity", "1.5")
                .rs(decoder);
    }

    private static void assertSqlError(String state, String message, ThrowingGetter g) {
        SQLException e = assertThrows(SQLException.class, g::get);
        assertEquals(state, e.getSQLState(), e.getMessage());
        assertEquals(message, e.getMessage());
    }

    @FunctionalInterface
    private interface ThrowingGetter { Object get() throws Exception; }

    @ParameterizedTest
    @EnumSource(value = ArrowDecoder.ResultDecoder.class, names = {"BUILTIN", "ARROW"})
    void specialsReadAsPgjdbcReadsThem(ArrowDecoder.ResultDecoder decoder) throws Exception {
        try (PdbResultSet rs = stream(decoder)) {
            for (int row = 0; row < 3; row++) {
                assertTrue(rs.next());
                String text = TEXT[row];
                double value = VALUE[row];
                for (int col = 1; col <= 4; col++) {
                    // n2 holds NaN in every special row.
                    String t = col == 4 ? "NaN" : text;
                    double v = col == 4 ? Double.NaN : value;
                    String where = decoder + " row " + (row + 1) + " col " + col;
                    final int c = col;

                    Object o = rs.getObject(col);
                    if (col == 1) {
                        assertEquals(Float.class, o.getClass(), where);
                        assertEquals((float) v, (Float) o, 0f, where);
                    } else {
                        assertEquals(Double.class, o.getClass(), where);
                        assertEquals(v, (Double) o, 0d, where);
                    }
                    assertEquals(v, rs.getDouble(col), 0d, where);
                    assertEquals((float) v, rs.getFloat(col), 0f, where);
                    assertEquals(t, rs.getString(col), where);
                    assertEquals(v, rs.getObject(col, Double.class), 0d, where);
                    assertFalse(rs.wasNull(), where);

                    String bad = "Bad value for type BigDecimal : " + t;
                    assertSqlError("22003", bad, () -> rs.getBigDecimal(c));
                    assertSqlError("22003", bad, () -> rs.getBigDecimal(c, 2));
                    assertSqlError("22003", bad, () -> rs.getObject(c, BigDecimal.class));
                    assertSqlError("22003", "Bad value for type long : " + t, () -> rs.getLong(c));
                    assertSqlError("22003", "Bad value for type int : " + t, () -> rs.getInt(c));
                    assertSqlError("22003", "Bad value for type short : " + t, () -> rs.getShort(c));
                    assertSqlError("22003", "Bad value for type byte : " + t, () -> rs.getByte(c));
                    assertSqlError("22003", "Bad value for type long : " + t, () -> rs.getObject(c, Long.class));
                    assertSqlError("42846", "Cannot cast to boolean: \"" + t + "\"", () -> rs.getBoolean(c));
                }
                // A text column holding the same characters stays text (pgjdbc:
                // getObject is the String); getBigDecimal refuses it the same way.
                assertEquals(text, rs.getObject(5));
                assertSqlError("22003", "Bad value for type BigDecimal : " + text, () -> rs.getBigDecimal(5));
            }

            // The ordinary row of the same batch keeps its BigDecimal.
            assertTrue(rs.next());
            assertEquals(1.5f, rs.getObject(1));
            assertEquals(1.5d, rs.getObject(2));
            assertEquals(new BigDecimal("1.5"), rs.getObject(3));
            assertEquals(new BigDecimal("1.50"), rs.getObject(4));
            assertEquals(new BigDecimal("1.50"), rs.getBigDecimal(4));
            assertEquals("1.50", rs.getString(4));
            assertFalse(rs.next());

            ResultSetMetaData md = rs.getMetaData();
            assertEquals(Types.REAL, md.getColumnType(1));
            assertEquals(Types.DOUBLE, md.getColumnType(2));
            assertEquals(Types.NUMERIC, md.getColumnType(3));
            assertEquals("numeric", md.getColumnTypeName(3));
            assertEquals(Types.NUMERIC, md.getColumnType(4));
            assertEquals("numeric", md.getColumnTypeName(4));
            assertEquals(10, md.getPrecision(4));
            assertEquals(2, md.getScale(4));
            assertEquals(Types.VARCHAR, md.getColumnType(5));
        }
    }

    @org.junit.jupiter.api.Test
    void numericDeclarationsMapToTheirDecimalToken() {
        assertEquals("decimal", ArrowDecoder.Decoded.numericDeclToken("numeric"));
        assertEquals("decimal", ArrowDecoder.Decoded.numericDeclToken("DECIMAL"));
        assertEquals("decimal(10,2)", ArrowDecoder.Decoded.numericDeclToken("NUMERIC(10,2)"));
        assertEquals("decimal(10,2)", ArrowDecoder.Decoded.numericDeclToken("decimal ( 10 , 2 )"));
        assertEquals("decimal(7,0)", ArrowDecoder.Decoded.numericDeclToken("numeric(7)"));
        assertEquals("decimal(5,-2)", ArrowDecoder.Decoded.numericDeclToken("numeric(5,-2)"));
        assertEquals(null, ArrowDecoder.Decoded.numericDeclToken("text"));
        assertEquals(null, ArrowDecoder.Decoded.numericDeclToken("numeric[]"));
        assertEquals(null, ArrowDecoder.Decoded.numericDeclToken("NUMERIC(10,2)[]"));
        assertEquals(null, ArrowDecoder.Decoded.numericDeclToken("numrange"));
    }
}
