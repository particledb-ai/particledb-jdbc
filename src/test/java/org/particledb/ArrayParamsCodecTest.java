package org.particledb;

import org.junit.jupiter.api.Test;
import org.particledb.wire.ArrayParams;
import org.particledb.wire.WireV2Codec;

import java.math.BigDecimal;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The EXECUTE array parameter (tag 17, {@link ArrayParams}) without a server:
 * the bytes, the inline literal (the SAME golden spellings the server's
 * the server's array-parameter rendering test
 * asserts for its rendering of the bound array), element conversion, the
 * shapes refused, and the element types inferred for {@code setObject}.
 */
public class ArrayParamsCodecTest {

    private static String lit(String type, Object... elems) throws SQLException {
        return ArrayParams.normalize(type, elems).sqlLiteral();
    }

    @Test
    void literalsMatchTheServersRendering() throws Exception {
        assertEquals("'{\"a\",NULL,\"o''x \\\\ \\\"q\\\" {,}\"}'::text[]", lit("text", "a", null, "o'x \\ \"q\" {,}"));
        assertEquals("'{\"NULL\"}'::text[]", lit("text", "NULL"));
        assertEquals("'{\"\"}'::text[]", lit("text", ""));
        assertEquals("'{}'::text[]", lit("text"));
        assertEquals("'{{1,2,3},{4,5,6}}'::int4[]",
                lit("int4", new Object[] {1, 2, 3}, new Object[] {4, 5, 6}));
        assertEquals("'{-5,NULL}'::int2[]", lit("int2", (short) -5, null));
        assertEquals("'{{{-9223372036854775808,7}}}'::int8[]",
                lit("int8", (Object) new Object[] {new Object[] {Long.MIN_VALUE, 7L}}));
        assertEquals("'{true,false}'::bool[]", lit("bool", true, false));
        assertEquals("'{1.5,NaN,-Infinity,1.0E10}'::float8[]",
                lit("float8", 1.5, Double.NaN, Double.NEGATIVE_INFINITY, 1e10));
        assertEquals("'{1.1}'::float4[]", lit("float4", 1.1f));
        assertEquals("'{-123.45,0.005}'::numeric[]", lit("numeric", new BigDecimal("-123.45"), new BigDecimal("0.005")));
        assertEquals("'{\"abababab-abab-abab-abab-abababababab\"}'::uuid[]",
                lit("uuid", UUID.fromString("abababab-abab-abab-abab-abababababab")));
        assertEquals("'{\"1969-12-31\"}'::date[]", lit("date", LocalDate.ofEpochDay(-1)));
        assertEquals("'{\"1970-01-01 00:00:00.000001\"}'::timestamp[]",
                lit("timestamp", LocalDateTime.of(1970, 1, 1, 0, 0, 0, 1000)));
        assertEquals("'{\"1969-12-31T23:00-01:00\"}'::timestamptz[]",
                lit("timestamptz", OffsetDateTime.of(1969, 12, 31, 23, 0, 0, 0, ZoneOffset.ofHours(-1))));
        assertEquals("'{\"\\\\x01ff\",\"\\\\x\"}'::bytea[]", lit("bytea", new byte[] {1, (byte) 0xff}, new byte[0]));
        assertEquals("'{\"{\\\"k\\\":\\\"v\\\"}\"}'::jsonb[]", lit("jsonb", "{\"k\":\"v\"}"));
        assertEquals("'{\"[1]\"}'::json[]", lit("json", "[1]"));
    }

    @Test
    void encodingIsTheServersLayout() throws Exception {
        byte[] got = ArrayParams.normalize("text", new Object[] {"a", null}).encode().bytes;
        assertArrayEquals(new byte[] {17, WireV2Codec.PARAM_TEXT, 1, 2, 0, 0, 0, WireV2Codec.PARAM_TEXT, 1, 0, 0, 0, 'a', 0}, got);
        byte[] empty = ArrayParams.normalize("int4", new Object[0]).encode().bytes;
        assertArrayEquals(new byte[] {17, 14, 0}, empty);
        byte[] two = ArrayParams.normalize("int2", new Object[] {new Object[] {1}, new Object[] {-2}}).encode().bytes;
        assertArrayEquals(new byte[] {17, 13, 2, 2, 0, 0, 0, 1, 0, 0, 0, 13, 1, 0, 13, (byte) 0xfe, (byte) 0xff}, two);
        // An EXECUTE carrying it: [u32 name][name][u16 n][param...].
        byte[] exec = WireV2Codec.encodeExecutePayload(new byte[] {'s'},
                new Object[] {7L, ArrayParams.normalize("text", new Object[] {"a", null}).encode()}, 2);
        assertEquals(4 + 1 + 2 + 9 + got.length, exec.length);
        assertArrayEquals(got, Arrays.copyOfRange(exec, exec.length - got.length, exec.length));
    }

    @Test
    void elementsConvertToTheDeclaredType() throws Exception {
        ArrayParams.Shape s = ArrayParams.normalize("int4", new Object[] {1, 2L, "3", new BigDecimal("4"), null});
        assertArrayEquals(new Object[] {1L, 2L, 3L, 4L, null}, s.flat);
        ArrayParams.Shape n = ArrayParams.normalize("numeric", new Object[] {1, 2.5, "1e3"});
        assertEquals("'{1,2.5,1000}'::numeric[]", n.sqlLiteral());
        ArrayParams.Shape ts = ArrayParams.normalize("timestamp",
                new Object[] {java.sql.Timestamp.valueOf("2024-01-01 12:00:00.1234565")});
        assertEquals("'{\"2024-01-01 12:00:00.123456\"}'::timestamp[]", ts.sqlLiteral(), "half to even");
        ArrayParams.Shape tx = ArrayParams.normalize("text", new Object[] {42, UUID.fromString("00000000-0000-0000-0000-000000000001")});
        assertEquals("'{\"42\",\"00000000-0000-0000-0000-000000000001\"}'::text[]", tx.sqlLiteral());
        ArrayParams.Shape list = ArrayParams.normalize("int8", new Object[] {List.of(1L, 2L), new long[] {3, 4}});
        assertArrayEquals(new int[] {2, 2}, list.dims);
    }

    @Test
    void refusals() {
        SQLFeatureNotSupportedException t = assertThrows(SQLFeatureNotSupportedException.class,
                () -> ArrayParams.normalize("point", new Object[] {"(1,2)"}));
        assertEquals("0A000", t.getSQLState());
        assertTrue(t.getMessage().contains("\"point\"") && t.getMessage().contains("jsonb"), t.getMessage());
        assertEquals("22003", assertThrows(SQLDataException.class,
                () -> ArrayParams.normalize("int2", new Object[] {40000})).getSQLState());
        assertEquals("22P02", assertThrows(SQLDataException.class,
                () -> ArrayParams.normalize("int4", new Object[] {"x"})).getSQLState());
        assertEquals("22P02", assertThrows(SQLDataException.class,
                () -> ArrayParams.normalize("int4", new Object[] {1.5})).getSQLState());
        assertEquals("22P02", assertThrows(SQLDataException.class,
                () -> ArrayParams.normalize("uuid", new Object[] {"not-a-uuid"})).getSQLState());
        assertEquals("2202E", assertThrows(SQLDataException.class,
                () -> ArrayParams.normalize("int4", new Object[] {new Object[] {1, 2}, new Object[] {3}})).getSQLState());
        assertEquals("2202E", assertThrows(SQLDataException.class,
                () -> ArrayParams.normalize("int4", new Object[] {new Object[] {1}, 2})).getSQLState());
        Object deep = 1;
        for (int i = 0; i < 7; i++) deep = new Object[] {deep};
        Object finalDeep = deep;
        assertEquals("54000", assertThrows(SQLDataException.class,
                () -> ArrayParams.normalize("int4", (Object[]) finalDeep)).getSQLState());
    }

    @Test
    void createArrayOfRefusesWhatCannotBeBound() throws Exception {
        assertThrows(SQLFeatureNotSupportedException.class, () -> new PdbArray("hstore", new Object[0]).shape());
        assertEquals("int4", new PdbArray("INTEGER", new Object[] {1}).getBaseTypeName());
        assertEquals("timestamptz", new PdbArray("timestamp with time zone", new Object[0]).getBaseTypeName());
        assertEquals("text", new PdbArray("VARCHAR(64)", new Object[0]).getBaseTypeName());
    }

    @Test
    void setObjectInfersTheElementType() throws Exception {
        assertEquals("text", PdbPreparedStatement.inferElementType(new String[0]));
        assertEquals("int4", PdbPreparedStatement.inferElementType(new int[] {1}));
        assertEquals("int4", PdbPreparedStatement.inferElementType(new Integer[][] {{1}}));
        assertEquals("int8", PdbPreparedStatement.inferElementType(new long[0]));
        assertEquals("float8", PdbPreparedStatement.inferElementType(new double[0]));
        assertEquals("bool", PdbPreparedStatement.inferElementType(new boolean[0]));
        assertEquals("uuid", PdbPreparedStatement.inferElementType(new UUID[0]));
        assertEquals("bytea", PdbPreparedStatement.inferElementType(new byte[][] {{1}}));
        assertEquals("timestamptz", PdbPreparedStatement.inferElementType(new OffsetDateTime[0]));
        assertEquals("int8", PdbPreparedStatement.inferElementType(List.of(1L, 2L)));
        assertEquals("text", PdbPreparedStatement.inferElementType(new Object[] {null, "x"}));
        assertEquals("42P18", assertThrows(SQLException.class,
                () -> PdbPreparedStatement.inferElementType(List.of())).getSQLState());
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> PdbPreparedStatement.inferElementType(List.of(new StringBuilder("x"))));
    }

    @Test
    void readSideTypesNewElementTypes() throws Exception {
        PdbArray u = PdbArray.forColumnDecl("UUID[]", new Object[] {"00000000-0000-0000-0000-000000000002", null});
        assertArrayEquals(new UUID[] {UUID.fromString("00000000-0000-0000-0000-000000000002"), null}, (UUID[]) u.getArray());
        PdbArray d = PdbArray.forColumnDecl("DATE[]", new Object[] {"2024-02-29"});
        assertEquals(java.sql.Date.valueOf("2024-02-29"), ((java.sql.Date[]) d.getArray())[0]);
        PdbArray b = PdbArray.forColumnDecl("BYTEA[]", new Object[] {"\\x00ff"});
        assertArrayEquals(new byte[] {0, (byte) 0xff}, ((byte[][]) b.getArray())[0]);
        // The native wire's multidimensional cell, and quoting inside a nested level.
        Object[] cell = PdbArray.parsePgArrayText("ARRAY[ARRAY['a,b','it''s'],ARRAY[NULL,'x]']]");
        assertArrayEquals(new String[][] {{"a,b", "it's"}, {null, "x]"}},
                (String[][]) PdbArray.forColumnDecl("TEXT[]", cell).getArray());
        Object[] brace = PdbArray.parsePgArrayText("{{\"a,\\\"b\",NULL},{c,d}}");
        assertArrayEquals(new String[][] {{"a,\"b", null}, {"c", "d"}},
                (String[][]) PdbArray.forColumnDecl("TEXT[]", brace).getArray());
        assertArrayEquals(new byte[] {0, (byte) 0xff},
                ((byte[][]) PdbArray.forColumnDecl("BYTEA[]", PdbArray.parsePgArrayText("ARRAY[X'00ff']")).getArray())[0]);
        PdbArray m = PdbArray.forColumnDecl("INT[]", new Object[] {new Object[] {"1", "2"}, new Object[] {"3", null}});
        Integer[][] mm = (Integer[][]) m.getArray();
        assertArrayEquals(new Integer[] {3, null}, mm[1]);
    }
}
