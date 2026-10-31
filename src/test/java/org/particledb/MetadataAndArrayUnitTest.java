package org.particledb;

import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PURE-LOGIC unit tests for the Conductor-gap driver work. None of these need a
 * live PDB server - they exercise the array-literal rendering, the
 * information_schema→JDBC type mapping, the Arrow-type-token parsing, the
 * ResultSetMetaData type contract, and the in-memory metadata ResultSet.
 *
 * <p>The LIVE end-to-end behavior (metadata methods issuing catalog queries
 * against a running server, array/bytea round-trip through the wire) is covered
 * by {@code DriverContractValidationTest}-style tests that run
 * against a real particledb binary.
 */
public class MetadataAndArrayUnitTest {

    // ── PdbArray → PG array literal (setArray bind path) ────────────────────
    @Test
    void textArrayLiteral_quotesAndEscapes() throws Exception {
        PdbArray a = new PdbArray("text", new Object[]{"a", "b,c", "he\"llo", "o'brien", null});
        String lit = a.toSqlLiteral();
        // {"a","b,c","he\"llo","o''brien",NULL}::text[]  (single-quote SQL-doubled)
        assertTrue(lit.startsWith("'{"), "must open with a quoted brace: " + lit);
        assertTrue(lit.endsWith("}'::text[]"), "must cast to text[]: " + lit);
        assertTrue(lit.contains("\"b,c\""), "comma element must be quoted: " + lit);
        assertTrue(lit.contains("\\\"llo"), "embedded double-quote must be backslash-escaped: " + lit);
        assertTrue(lit.contains("o''brien"), "single-quote must be SQL-doubled: " + lit);
        assertTrue(lit.contains(",NULL,") || lit.contains(",NULL}"), "null element must be NULL: " + lit);
    }

    @Test
    void varcharTypeNormalizesToText() throws Exception {
        PdbArray a = new PdbArray("VARCHAR", new Object[]{"x"});
        assertEquals("text", a.getBaseTypeName());
        assertEquals(Types.VARCHAR, a.getBaseType());
        assertTrue(a.toSqlLiteral().endsWith("::text[]"));
    }

    @Test
    void intArrayLiteral_unquotedElements() throws Exception {
        PdbArray a = new PdbArray("int4", new Object[]{1, 2, 3});
        String lit = a.toSqlLiteral();
        assertEquals("'{1,2,3}'::int4[]", lit);
        assertEquals(Types.INTEGER, a.getBaseType());
    }

    @Test
    void getArray_returnsStringArrayForText() throws Exception {
        PdbArray a = new PdbArray("text", new Object[]{"a", "b", null});
        Object arr = a.getArray();
        assertTrue(arr instanceof String[], "text[] must return String[]");
        assertArrayEquals(new String[]{"a", "b", null}, (String[]) arr);
    }

    @Test
    void getArraySlice_isOneBased() throws Exception {
        PdbArray a = new PdbArray("text", new Object[]{"a", "b", "c", "d"});
        Object slice = a.getArray(2, 2); // elements 2..3 → b,c
        assertArrayEquals(new String[]{"b", "c"}, (String[]) slice);
    }

    // ── setArray drives the interpolated SQL (client-side substitution) ─────
    @Test
    void interpolate_substitutesArrayLiteral() throws Exception {
        PdbArray a = new PdbArray("text", new Object[]{"tag1", "tag2"});
        String sql = PdbPreparedStatement.interpolate(
                "INSERT INTO t(id, words) VALUES (?, ?)", new Object[]{5, a.shape()});
        assertEquals("INSERT INTO t(id, words) VALUES (5, '{\"tag1\",\"tag2\"}'::text[])", sql);
    }

    @Test
    void interpolate_bytea_hexEncodes() throws Exception {
        String sql = PdbPreparedStatement.interpolate(
                "INSERT INTO t(b) VALUES (?)", new Object[]{new byte[]{0x00, (byte) 0xff, 0x10}});
        assertEquals("INSERT INTO t(b) VALUES ('\\x00ff10'::bytea)", sql);
    }

    // ── array read-back parsing (getArray on a Utf8 array cell) ─────────────
    // PDB's NATIVE wire renders an array cell as ARRAY['a','b',…] (verified live).
    @Test
    void parseArrayText_arrayCtorForm_isTheRealServerShape() {
        // The exact shape a live PDB server returns for a text[] column.
        assertArrayEquals(new Object[]{"alpha", "be,ta", "gam\"ma"},
                PdbArray.parsePgArrayText("ARRAY['alpha','be,ta','gam\"ma']"));
        // single-quote doubling inside an element
        assertArrayEquals(new Object[]{"o'brien", "x"},
                PdbArray.parsePgArrayText("ARRAY['o''brien','x']"));
        // case-insensitive keyword + NULL element + empty array
        assertArrayEquals(new Object[]{"a", null},
                PdbArray.parsePgArrayText("array['a',NULL]"));
        assertArrayEquals(new Object[]{}, PdbArray.parsePgArrayText("ARRAY[]"));
    }

    @Test
    void parseArrayText_pgBraceForm_defensiveFallback() {
        // The PG external text form is still parsed (PG-wire path / future change).
        assertArrayEquals(new Object[]{"a", "b", "c"},
                PdbArray.parsePgArrayText("{a,b,c}"));
        assertArrayEquals(new Object[]{"alpha", "be,ta", "gam\"ma"},
                PdbArray.parsePgArrayText("{alpha,\"be,ta\",\"gam\\\"ma\"}"));
        assertArrayEquals(new Object[]{"x", null, "y"},
                PdbArray.parsePgArrayText("{x,NULL,y}"));
        assertArrayEquals(new Object[]{}, PdbArray.parsePgArrayText("{}"));
    }

    // ── information_schema.columns.data_type → JDBC Types ───────────────────
    @Test
    void pgTypeMap_coversPdbDataTypes() {
        assertEquals(Types.SMALLINT, PgTypeMap.sqlType("smallint"));
        assertEquals(Types.INTEGER, PgTypeMap.sqlType("integer"));
        assertEquals(Types.BIGINT, PgTypeMap.sqlType("bigint"));
        assertEquals(Types.DOUBLE, PgTypeMap.sqlType("double precision"));
        assertEquals(Types.REAL, PgTypeMap.sqlType("real"));
        assertEquals(Types.NUMERIC, PgTypeMap.sqlType("numeric"));
        assertEquals(Types.VARCHAR, PgTypeMap.sqlType("text"));
        assertEquals(Types.VARCHAR, PgTypeMap.sqlType("character varying"));
        assertEquals(Types.BINARY, PgTypeMap.sqlType("bytea"));
        assertEquals(Types.BOOLEAN, PgTypeMap.sqlType("boolean"));
        assertEquals(Types.TIMESTAMP, PgTypeMap.sqlType("timestamp without time zone"));
        assertEquals(Types.DATE, PgTypeMap.sqlType("date"));
        assertEquals(Types.ARRAY, PgTypeMap.sqlType("ARRAY"));
        // jsonb / uuid → OTHER (pgjdbc parity), still string-readable
        assertEquals(Types.OTHER, PgTypeMap.sqlType("jsonb"));
        assertEquals(Types.OTHER, PgTypeMap.sqlType("uuid"));
    }

    @Test
    void pgTypeMap_columnSizeAndScale() {
        assertEquals(Integer.valueOf(10), PgTypeMap.columnSize("integer", null, null));
        assertEquals(Integer.valueOf(255), PgTypeMap.columnSize("character varying", 255, null));
        assertEquals(Integer.valueOf(0), PgTypeMap.decimalDigits("integer", null));
        assertEquals(Integer.valueOf(2), PgTypeMap.decimalDigits("numeric", 2));
        assertEquals(Integer.valueOf(12), PgTypeMap.columnSize("numeric", null, 12));
    }

    // ── Arrow type token parsing (metadata type inference) ──────────────────
    @Test
    void arrowTypeToken_parsesCommonTypes() {
        assertEquals("int32", ArrowDecoder.arrowTypeToken(new FakeArrowType("Int(32, true)")));
        assertEquals("int64", ArrowDecoder.arrowTypeToken(new FakeArrowType("Int(64, true)")));
        assertEquals("int16", ArrowDecoder.arrowTypeToken(new FakeArrowType("Int(16, true)")));
        assertEquals("float32", ArrowDecoder.arrowTypeToken(new FakeArrowType("FloatingPoint(SINGLE)")));
        assertEquals("float64", ArrowDecoder.arrowTypeToken(new FakeArrowType("FloatingPoint(DOUBLE)")));
        assertEquals("utf8", ArrowDecoder.arrowTypeToken(new FakeArrowType("Utf8")));
        assertEquals("bool", ArrowDecoder.arrowTypeToken(new FakeArrowType("Bool")));
        assertEquals("binary", ArrowDecoder.arrowTypeToken(new FakeArrowType("Binary")));
        assertEquals("date32", ArrowDecoder.arrowTypeToken(new FakeArrowType("Date(DAY)")));
        assertEquals("timestamp", ArrowDecoder.arrowTypeToken(new FakeArrowType("Timestamp(MICROSECOND, null)")));
        assertEquals("list", ArrowDecoder.arrowTypeToken(new FakeArrowType("List")));
        assertEquals("decimal(38,9)", ArrowDecoder.arrowTypeToken(new FakeArrowType("Decimal(38, 9, 128)")));
        assertEquals("null", ArrowDecoder.arrowTypeToken(new FakeArrowType("Null")));
    }

    /** Stand-in for an Arrow ArrowType - arrowTypeToken only uses toString(). */
    private static final class FakeArrowType {
        private final String s;
        FakeArrowType(String s) { this.s = s; }
        @Override public String toString() { return s; }
    }

    // ── ResultSetMetaData type contract (SimpleMeta) ────────────────────────
    @Test
    void simpleMeta_reportsJdbcTypesFromArrowTokens() throws Exception {
        List<String> cols = Arrays.asList("id", "name", "amount", "flag");
        List<String> types = Arrays.asList("int64", "utf8", "decimal(10,2)", "bool");
        PdbResultSet.SimpleMeta md = new PdbResultSet.SimpleMeta(cols, types);
        assertEquals(4, md.getColumnCount());

        assertEquals(Types.BIGINT, md.getColumnType(1));
        assertEquals("int8", md.getColumnTypeName(1));
        assertEquals(Long.class.getName(), md.getColumnClassName(1));
        assertTrue(md.isSigned(1));

        assertEquals(Types.VARCHAR, md.getColumnType(2));
        assertEquals("text", md.getColumnTypeName(2));
        assertEquals(String.class.getName(), md.getColumnClassName(2));

        assertEquals(Types.NUMERIC, md.getColumnType(3));
        assertEquals("numeric", md.getColumnTypeName(3));
        assertEquals(10, md.getPrecision(3));
        assertEquals(2, md.getScale(3));
        assertEquals(java.math.BigDecimal.class.getName(), md.getColumnClassName(3));

        assertEquals(Types.BOOLEAN, md.getColumnType(4));
        assertEquals("bool", md.getColumnTypeName(4));
    }

    @Test
    void simpleMeta_missingTokensDegradeToOther() throws Exception {
        // No type tokens (older decode path) → OTHER/Object, never throws.
        PdbResultSet.SimpleMeta md = new PdbResultSet.SimpleMeta(Arrays.asList("a", "b"));
        assertEquals(2, md.getColumnCount());
        assertEquals(Types.OTHER, md.getColumnType(1));
        assertEquals(Object.class.getName(), md.getColumnClassName(1));
    }

    // ── MetaResultSet navigation + typed reads ──────────────────────────────
    @Test
    void metaResultSet_navigatesAndReadsByNameAndIndex() throws Exception {
        String[] cols = {"TABLE_NAME", "ORDINAL_POSITION", "IS_NULLABLE"};
        int[] jt = {Types.VARCHAR, Types.INTEGER, Types.VARCHAR};
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{"workflow", 1, "NO"});
        rows.add(new Object[]{"task", 2, "YES"});
        MetaResultSet rs = new MetaResultSet(cols, jt, rows);

        assertTrue(rs.next());
        assertEquals("workflow", rs.getString("TABLE_NAME"));
        assertEquals("workflow", rs.getString(1));
        assertEquals(1, rs.getInt("ORDINAL_POSITION"));
        assertEquals("NO", rs.getString("IS_NULLABLE"));
        assertFalse(rs.wasNull());

        assertTrue(rs.next());
        assertEquals("task", rs.getString("TABLE_NAME"));
        assertEquals(2, rs.getInt("ORDINAL_POSITION"));

        assertFalse(rs.next(), "only two rows");

        ResultSetMetaData md = rs.getMetaData();
        assertEquals(3, md.getColumnCount());
        assertEquals("TABLE_NAME", md.getColumnName(1));
        assertEquals(Types.INTEGER, md.getColumnType(2));
    }

    @Test
    void metaResultSet_nullValueReportsWasNull() throws Exception {
        MetaResultSet rs = new MetaResultSet(
                new String[]{"COLUMN_DEF"}, new int[]{Types.VARCHAR},
                java.util.Collections.singletonList(new Object[]{null}));
        assertTrue(rs.next());
        assertNull(rs.getString("COLUMN_DEF"));
        assertTrue(rs.wasNull());
        assertEquals(0, rs.getInt("COLUMN_DEF"));
        assertTrue(rs.wasNull());
    }

    // ── getObject normalization of an Arrow list column → Object[] wraps ─────
    @Test
    void getArrayColumn_wrapsObjectArrayAsSqlArray() throws Exception {
        // Simulate what ArrowDecoder produces for a text[] column: an Object[]
        // whose PdbResultSet.getArray wraps into a java.sql.Array of String[].
        Object normalized = new Object[]{"w1", "w2", "w3"};
        // The PdbArray read-side constructor is what getArray uses.
        PdbArray a = new PdbArray("text", (Object[]) normalized);
        assertArrayEquals(new String[]{"w1", "w2", "w3"}, (String[]) a.getArray());
    }
}
