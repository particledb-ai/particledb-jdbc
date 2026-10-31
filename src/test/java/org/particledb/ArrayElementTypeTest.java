package org.particledb;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.particledb.wire.StreamState;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Array element TYPING over the native wire, end to end, with NO live server.
 *
 * <p>The server materializes a SQL array column as an Arrow {@code Utf8} cell
 * carrying the engine's {@code ARRAY[…]} text, and stamps the DECLARED SQL type
 * into the Arrow field's custom metadata under {@code pdb:decl} - the element
 * type with a trailing {@code []} ({@code INT[]}, {@code TEXT[]}), or the
 * {@code _pdb_array} sentinel for a computed array of unknown element type.
 * (See the server's scan operator {@code field_with_decl} /
 * {@code decl_is_array} and the server's PostgreSQL-wire layer
 * {@code field_is_array_decl}.)
 *
 * <p>These tests build a REAL Arrow IPC stream carrying that exact shape, so
 * they exercise the true decode path ({@link ArrowDecoder} →
 * {@link PdbResultSet#getArray} → {@link PdbArray}) rather than a hand-made
 * {@code PdbArray}.
 */
@org.junit.jupiter.api.Tag("arrow-java")
public class ArrayElementTypeTest {

    /**
     * Build a one-column Arrow IPC stream whose field carries {@code pdb:decl},
     * matching what the engine ships for an array column.
     */
    static byte[] arrayColumnIpc(String colName, String pdbDecl, String... cells) throws Exception {
        Map<String, String> md = new HashMap<>();
        if (pdbDecl != null) md.put("pdb:decl", pdbDecl);
        Field f = new Field(colName,
                new FieldType(true, new ArrowType.Utf8(), null, md), null);
        Schema schema = new Schema(Collections.singletonList(f));
        try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            VarCharVector vec = (VarCharVector) root.getVector(0);
            vec.allocateNew(Math.max(cells.length, 1));
            for (int i = 0; i < cells.length; i++) {
                if (cells[i] == null) vec.setNull(i);
                else vec.setSafe(i, cells[i].getBytes(StandardCharsets.UTF_8));
            }
            vec.setValueCount(cells.length);
            root.setRowCount(cells.length);
            try (ArrowStreamWriter w =
                         new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                w.start();
                w.writeBatch();
                w.end();
            }
            return out.toByteArray();
        }
    }

    /**
     * Build a TWO-column Arrow IPC stream - an array column (Utf8 +
     * {@code pdb:decl}) followed by a plain {@code BIGINT} column (Int64 +
     * {@code pdb:decl="bigint"}) - matching what the engine ships for
     * {@code SELECT ns, id FROM …}. Used to prove that widening an ARRAY
     * column's metadata leaves its NON-array neighbour untouched.
     */
    static byte[] arrayPlusBigintIpc(String pdbDecl, String cell, long id) throws Exception {
        Map<String, String> amd = new HashMap<>();
        if (pdbDecl != null) amd.put("pdb:decl", pdbDecl);
        Map<String, String> imd = new HashMap<>();
        imd.put("pdb:decl", "bigint");
        Field af = new Field("ns", new FieldType(true, new ArrowType.Utf8(), null, amd), null);
        Field idf = new Field("id",
                new FieldType(true, new ArrowType.Int(64, true), null, imd), null);
        Schema schema = new Schema(Arrays.asList(af, idf));
        try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            VarCharVector av = (VarCharVector) root.getVector(0);
            BigIntVector iv = (BigIntVector) root.getVector(1);
            av.allocateNew(1);
            iv.allocateNew(1);
            if (cell == null) av.setNull(0);
            else av.setSafe(0, cell.getBytes(StandardCharsets.UTF_8));
            iv.setSafe(0, id);
            av.setValueCount(1);
            iv.setValueCount(1);
            root.setRowCount(1);
            try (ArrowStreamWriter w =
                         new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                w.start();
                w.writeBatch();
                w.end();
            }
            return out.toByteArray();
        }
    }

    private static ResultSet rsOver(byte[] ipc) throws Exception {
        ResultSet rs = new PdbResultSet(StreamState.StreamResult.arrow(ipc));
        assertTrue(rs.next(), "expected one row");
        return rs;
    }

    // ── EVIDENCE: the declared type really does survive Arrow IPC ───────────

    /**
     * The load-bearing premise of the whole fix: Arrow IPC serializes per-field
     * custom metadata, so {@code pdb:decl} that the engine stamps server-side is
     * READABLE by the Java client. If this ever fails, the element type is NOT
     * on the wire and the fix would have to move server-side.
     */
    @Test
    void pdbDeclMetadataSurvivesArrowIpcRoundTrip() throws Exception {
        byte[] ipc = arrayColumnIpc("ns", "INT[]", "ARRAY[1,2,3]");
        try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
             ArrowStreamReader r = new ArrowStreamReader(new ByteArrayInputStream(ipc), alloc)) {
            Field f = r.getVectorSchemaRoot().getSchema().getFields().get(0);
            assertEquals("INT[]", f.getMetadata().get("pdb:decl"),
                    "Arrow IPC must carry the pdb:decl stamp to the client");
        }
    }

    /** ArrowDecoder must surface that metadata rather than dropping it. */
    @Test
    void arrowDecoderExposesColumnMetadata() throws Exception {
        ArrowDecoder.Decoded d = ArrowDecoder.decodePositional(
                arrayColumnIpc("ns", "INT[]", "ARRAY[1,2,3]"));
        assertEquals(1, d.columnMetadata.size());
        assertEquals("INT[]", d.columnMetadata.get(0).get("pdb:decl"));
    }

    // ── per-element-type read-back typing ──────────────────────────────────

    @Test
    void int4Array_returnsIntegerArray() throws Exception {
        Array a = rsOver(arrayColumnIpc("ns", "INT[]", "ARRAY[1,2,3]")).getArray(1);
        Object arr = a.getArray();
        assertInstanceOf(Integer[].class, arr, "int4[] must return Integer[]");
        assertArrayEquals(new Integer[]{1, 2, 3}, (Integer[]) arr);
        assertEquals("int4", a.getBaseTypeName());
        assertEquals(Types.INTEGER, a.getBaseType());
    }

    @Test
    void int8Array_returnsLongArray() throws Exception {
        Array a = rsOver(arrayColumnIpc("ns", "BIGINT[]",
                "ARRAY[9223372036854775807,-2]")).getArray(1);
        Object arr = a.getArray();
        assertInstanceOf(Long[].class, arr, "int8[] must return Long[]");
        assertArrayEquals(new Long[]{9223372036854775807L, -2L}, (Long[]) arr);
        assertEquals("int8", a.getBaseTypeName());
        assertEquals(Types.BIGINT, a.getBaseType());
    }

    @Test
    void int2Array_returnsShortArray() throws Exception {
        Array a = rsOver(arrayColumnIpc("ns", "SMALLINT[]", "ARRAY[1,-2]")).getArray(1);
        assertInstanceOf(Short[].class, a.getArray(), "int2[] must return Short[]");
        assertArrayEquals(new Short[]{(short) 1, (short) -2}, (Short[]) a.getArray());
        assertEquals(Types.SMALLINT, a.getBaseType());
    }

    @Test
    void textArray_returnsStringArray() throws Exception {
        Array a = rsOver(arrayColumnIpc("tags", "TEXT[]",
                "ARRAY['alpha','beta']")).getArray(1);
        Object arr = a.getArray();
        assertInstanceOf(String[].class, arr, "text[] must return String[]");
        assertArrayEquals(new String[]{"alpha", "beta"}, (String[]) arr);
        assertEquals("text", a.getBaseTypeName());
        assertEquals(Types.VARCHAR, a.getBaseType());
    }

    @Test
    void varcharArray_returnsStringArray() throws Exception {
        Array a = rsOver(arrayColumnIpc("tags", "VARCHAR[]", "ARRAY['x']")).getArray(1);
        assertInstanceOf(String[].class, a.getArray(), "varchar[] must return String[]");
        assertEquals(Types.VARCHAR, a.getBaseType());
    }

    @Test
    void boolArray_returnsBooleanArray() throws Exception {
        Array a = rsOver(arrayColumnIpc("flags", "BOOLEAN[]",
                "ARRAY[true,false,t,f]")).getArray(1);
        Object arr = a.getArray();
        assertInstanceOf(Boolean[].class, arr, "bool[] must return Boolean[]");
        assertArrayEquals(new Boolean[]{true, false, true, false}, (Boolean[]) arr);
        assertEquals("bool", a.getBaseTypeName());
        assertEquals(Types.BOOLEAN, a.getBaseType());
    }

    @Test
    void float8Array_returnsDoubleArray() throws Exception {
        Array a = rsOver(arrayColumnIpc("ds", "DOUBLE PRECISION[]",
                "ARRAY[1.5,-0.25]")).getArray(1);
        Object arr = a.getArray();
        assertInstanceOf(Double[].class, arr, "float8[] must return Double[]");
        assertArrayEquals(new Double[]{1.5d, -0.25d}, (Double[]) arr);
        assertEquals("float8", a.getBaseTypeName());
        assertEquals(Types.DOUBLE, a.getBaseType());
    }

    @Test
    void float4Array_returnsFloatArray() throws Exception {
        Array a = rsOver(arrayColumnIpc("fs", "REAL[]", "ARRAY[1.5,-0.25]")).getArray(1);
        assertInstanceOf(Float[].class, a.getArray(), "float4[] must return Float[]");
        assertEquals(Types.REAL, a.getBaseType());
    }

    @Test
    void numericArray_returnsBigDecimalArray() throws Exception {
        Array a = rsOver(arrayColumnIpc("amts", "NUMERIC(10,2)[]",
                "ARRAY[1.25,3.50]")).getArray(1);
        Object arr = a.getArray();
        assertInstanceOf(BigDecimal[].class, arr, "numeric[] must return BigDecimal[]");
        assertArrayEquals(new BigDecimal[]{new BigDecimal("1.25"), new BigDecimal("3.50")},
                (BigDecimal[]) arr);
        assertEquals("numeric", a.getBaseTypeName());
        assertEquals(Types.NUMERIC, a.getBaseType());
    }

    // ── THE critical negative case ─────────────────────────────────────────

    /**
     * A {@code text[]} whose elements merely LOOK numeric must stay
     * {@code String[]}. This is the case that forbids value sniffing: typing is
     * driven by the DECLARED type, never by the shape of the data.
     */
    @Test
    void textArrayOfNumericLookingStrings_staysStringArray() throws Exception {
        Array a = rsOver(arrayColumnIpc("tags", "TEXT[]", "ARRAY['1','2']")).getArray(1);
        Object arr = a.getArray();
        assertInstanceOf(String[].class, arr,
                "a text[] holding \"1\",\"2\" must NOT be sniffed into Integer[]");
        assertArrayEquals(new String[]{"1", "2"}, (String[]) arr);
        assertEquals(Types.VARCHAR, a.getBaseType());
    }

    /** Same guard for a bool-looking and a decimal-looking text element. */
    @Test
    void textArrayOfTypeLookalikes_staysStringArray() throws Exception {
        Array a = rsOver(arrayColumnIpc("tags", "TEXT[]",
                "ARRAY['true','1.5','NaN']")).getArray(1);
        assertArrayEquals(new String[]{"true", "1.5", "NaN"}, (String[]) a.getArray());
    }

    // ── NULL elements / empty array / unknown element type ─────────────────

    @Test
    void nullElementsArePreservedPerElementType() throws Exception {
        Array ints = rsOver(arrayColumnIpc("ns", "INT[]", "ARRAY[1,NULL,3]")).getArray(1);
        assertArrayEquals(new Integer[]{1, null, 3}, (Integer[]) ints.getArray());

        Array txt = rsOver(arrayColumnIpc("tags", "TEXT[]", "ARRAY['a',NULL]")).getArray(1);
        assertArrayEquals(new String[]{"a", null}, (String[]) txt.getArray());
    }

    @Test
    void emptyArrayKeepsElementTyping() throws Exception {
        Array a = rsOver(arrayColumnIpc("ns", "INT[]", "ARRAY[]")).getArray(1);
        Object arr = a.getArray();
        assertInstanceOf(Integer[].class, arr, "an empty int4[] is still Integer[]");
        assertEquals(0, ((Integer[]) arr).length);
        assertEquals(Types.INTEGER, a.getBaseType());
    }

    @Test
    void sqlNullArrayColumnReturnsNull() throws Exception {
        ResultSet rs = new PdbResultSet(
                StreamState.StreamResult.arrow(arrayColumnIpc("ns", "INT[]", (String) null)));
        assertTrue(rs.next());
        assertNull(rs.getArray(1));
        assertTrue(rs.wasNull());
    }

    /**
     * A computed array of unknown element type stamps the {@code _pdb_array}
     * sentinel. With no element type known, the honest answer is text/String[]  -
     * NOT a guess derived from the values.
     */
    @Test
    void sentinelDeclFallsBackToText() throws Exception {
        Array a = rsOver(arrayColumnIpc("c", "_pdb_array", "ARRAY[1,2]")).getArray(1);
        assertInstanceOf(String[].class, a.getArray(),
                "unknown element type must fall back to String[], not sniff Integer[]");
        assertEquals("text", a.getBaseTypeName());
        assertEquals(Types.VARCHAR, a.getBaseType());
    }

    /** No metadata at all (older server / derived column) → text fallback. */
    @Test
    void missingDeclFallsBackToText() throws Exception {
        Array a = rsOver(arrayColumnIpc("c", null, "ARRAY['a','b']")).getArray(1);
        assertInstanceOf(String[].class, a.getArray());
        assertEquals(Types.VARCHAR, a.getBaseType());
    }

    // ── value fidelity must not regress ────────────────────────────────────

    /**
     * The element VALUES fixed in fe71f942c must still round-trip exactly:
     * an embedded single quote ({@code o'k}) and an embedded comma
     * ({@code a,b}).
     */
    @Test
    void textElementValueFidelityIsPreserved() throws Exception {
        Array a = rsOver(arrayColumnIpc("tags", "TEXT[]",
                "ARRAY['alpha','beta','o''k','a,b']")).getArray(1);
        assertArrayEquals(new String[]{"alpha", "beta", "o'k", "a,b"},
                (String[]) a.getArray());
    }

    // ── getArray(index,count) / getResultSet / toString consistency ────────

    @Test
    void sliceKeepsElementTyping() throws Exception {
        Array a = rsOver(arrayColumnIpc("ns", "INT[]", "ARRAY[10,20,30,40]")).getArray(1);
        Object slice = a.getArray(2, 2);
        assertInstanceOf(Integer[].class, slice, "a slice must keep the element type");
        assertArrayEquals(new Integer[]{20, 30}, (Integer[]) slice);
    }

    @Test
    void getResultSetYieldsIndexAndTypedValue() throws Exception {
        Array a = rsOver(arrayColumnIpc("ns", "INT[]", "ARRAY[7,NULL,9]")).getArray(1);
        try (ResultSet rs = a.getResultSet()) {
            assertEquals(2, rs.getMetaData().getColumnCount());
            assertEquals("INDEX", rs.getMetaData().getColumnName(1).toUpperCase());
            assertEquals("VALUE", rs.getMetaData().getColumnName(2).toUpperCase());
            assertEquals(Types.INTEGER, rs.getMetaData().getColumnType(2),
                    "VALUE column must report the element type");

            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(Integer.valueOf(7), rs.getObject(2));

            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertNull(rs.getObject(2));
            assertTrue(rs.wasNull());

            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertEquals(Integer.valueOf(9), rs.getObject(2));

            assertFalse(rs.next());
        }
    }

    @Test
    void getResultSetSliceIsOneBasedAndKeepsIndices() throws Exception {
        Array a = rsOver(arrayColumnIpc("ns", "INT[]", "ARRAY[10,20,30,40]")).getArray(1);
        try (ResultSet rs = a.getResultSet(2, 2)) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(Integer.valueOf(20), rs.getObject(2));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertEquals(Integer.valueOf(30), rs.getObject(2));
            assertFalse(rs.next());
        }
    }

    @Test
    void toStringRendersElementTypeAndValues() throws Exception {
        Array ints = rsOver(arrayColumnIpc("ns", "INT[]", "ARRAY[1,2]")).getArray(1);
        String s = ints.toString();
        assertTrue(s.contains("int4"), "toString must name the element type: " + s);
        assertTrue(s.contains("1") && s.contains("2"), "toString must show values: " + s);

        Array txt = rsOver(arrayColumnIpc("t", "TEXT[]", "ARRAY['1']")).getArray(1);
        assertTrue(txt.toString().contains("text"),
                "a text[] must not render as int4: " + txt.toString());
    }

    @Test
    void freeEmptiesTheArray() throws Exception {
        Array a = rsOver(arrayColumnIpc("ns", "INT[]", "ARRAY[1,2]")).getArray(1);
        a.free();
        assertEquals(0, ((Integer[]) a.getArray()).length);
    }

    // ── ResultSetMetaData for an ARRAY column ──────────────────────────────
    // An array column ships as Arrow Utf8, so the Arrow token alone says
    // "varchar/text". Only the `pdb:decl` stamp distinguishes it, and pgjdbc
    // reports java.sql.Types.ARRAY / `_int4` / java.sql.Array for the same
    // column - so the stamp has to reach ResultSetMetaData too.

    @Test
    void metadata_int4ArrayReportsArrayTypeNameAndClass() throws Exception {
        ResultSetMetaData md = rsOver(arrayColumnIpc("ns", "INT[]", "ARRAY[1,2,3]")).getMetaData();
        assertEquals(Types.ARRAY, md.getColumnType(1),
                "an int4[] column must report Types.ARRAY, not VARCHAR");
        assertEquals("_int4", md.getColumnTypeName(1),
                "type name must be PG's array spelling, not 'text'");
        assertEquals(java.sql.Array.class.getName(), md.getColumnClassName(1),
                "an array column's class name is java.sql.Array");
        assertFalse(md.isSigned(1), "an array column is not a signed number");
    }

    @Test
    void metadata_perElementTypeArrayNames() throws Exception {
        assertEquals("_text", rsOver(arrayColumnIpc("t", "TEXT[]", "ARRAY['a']"))
                .getMetaData().getColumnTypeName(1));
        assertEquals("_text", rsOver(arrayColumnIpc("t", "VARCHAR(64)[]", "ARRAY['a']"))
                .getMetaData().getColumnTypeName(1));
        assertEquals("_bool", rsOver(arrayColumnIpc("f", "BOOLEAN[]", "ARRAY[true]"))
                .getMetaData().getColumnTypeName(1));
        assertEquals("_numeric", rsOver(arrayColumnIpc("a", "NUMERIC(10,2)[]", "ARRAY[1.25]"))
                .getMetaData().getColumnTypeName(1));
        assertEquals("_int8", rsOver(arrayColumnIpc("b", "BIGINT[]", "ARRAY[1]"))
                .getMetaData().getColumnTypeName(1));
        // Every one of them is still Types.ARRAY.
        for (String decl : new String[]{"TEXT[]", "BOOLEAN[]", "NUMERIC(10,2)[]", "BIGINT[]"}) {
            assertEquals(Types.ARRAY,
                    rsOver(arrayColumnIpc("c", decl, "ARRAY[1]")).getMetaData().getColumnType(1),
                    decl + " must report Types.ARRAY");
        }
    }

    /**
     * An array column must stay case-SENSITIVE (pgjdbc's TypeInfoCache answers
     * true for every array OID, and PDB compares the element text) - widening
     * the reported JDBC type must not silently flip this.
     */
    @Test
    void metadata_arrayColumnStaysCaseSensitive() throws Exception {
        assertTrue(rsOver(arrayColumnIpc("t", "TEXT[]", "ARRAY['a']"))
                .getMetaData().isCaseSensitive(1));
    }

    /** A NON-array column's metadata is untouched by the array widening. */
    @Test
    void metadata_nonArrayColumnIsUnchanged() throws Exception {
        ResultSetMetaData md =
                rsOver(arrayPlusBigintIpc("INT[]", "ARRAY[1,2]", 42L)).getMetaData();
        assertEquals(2, md.getColumnCount());
        assertEquals(Types.ARRAY, md.getColumnType(1));
        // column 2 is a plain BIGINT and must report exactly what it did before.
        assertEquals(Types.BIGINT, md.getColumnType(2));
        assertEquals("int8", md.getColumnTypeName(2));
        assertEquals(Long.class.getName(), md.getColumnClassName(2));
        assertTrue(md.isSigned(2));

        // A plain TEXT column (stamped `text`, no []) stays VARCHAR/text/String.
        ResultSetMetaData tmd =
                rsOver(arrayColumnIpc("note", "text", "hello")).getMetaData();
        assertEquals(Types.VARCHAR, tmd.getColumnType(1));
        assertEquals("text", tmd.getColumnTypeName(1));
        assertEquals(String.class.getName(), tmd.getColumnClassName(1));
    }

    /**
     * The computed-array sentinel IS an array column (the PG wire advertises
     * {@code text[]}/1009 for it) - but its ELEMENT type stays text, never a
     * value-sniffed guess.
     */
    @Test
    void metadata_sentinelIsArrayOfText() throws Exception {
        ResultSetMetaData md = rsOver(arrayColumnIpc("c", "_pdb_array", "ARRAY[1,2]")).getMetaData();
        assertEquals(Types.ARRAY, md.getColumnType(1));
        assertEquals("_text", md.getColumnTypeName(1));
        assertEquals(java.sql.Array.class.getName(), md.getColumnClassName(1));
    }

    /** No stamp at all → the column is plain text, NOT an array. */
    @Test
    void metadata_missingStampStaysText() throws Exception {
        ResultSetMetaData md = rsOver(arrayColumnIpc("c", null, "ARRAY['a','b']")).getMetaData();
        assertEquals(Types.VARCHAR, md.getColumnType(1),
                "an unstamped Utf8 column must not be promoted to ARRAY");
        assertEquals("text", md.getColumnTypeName(1));
        assertEquals(String.class.getName(), md.getColumnClassName(1));
    }

    // ── getObject on an ARRAY column ───────────────────────────────────────
    // pgjdbc hands back a java.sql.Array; returning the raw ARRAY[…] String
    // breaks any caller that does `(Array) rs.getObject(n)`.

    @Test
    void getObject_byIndexReturnsTypedArray() throws Exception {
        Object o = rsOver(arrayColumnIpc("ns", "INT[]", "ARRAY[1,2,3]")).getObject(1);
        assertInstanceOf(java.sql.Array.class, o,
                "getObject on an array column must return a java.sql.Array");
        Object arr = ((Array) o).getArray();
        assertInstanceOf(Integer[].class, arr);
        assertArrayEquals(new Integer[]{1, 2, 3}, (Integer[]) arr);
    }

    @Test
    void getObject_byLabelReturnsTypedArray() throws Exception {
        ResultSet rs = rsOver(arrayColumnIpc("tags", "TEXT[]", "ARRAY['alpha','o''k','a,b']"));
        Object o = rs.getObject("tags");
        assertInstanceOf(java.sql.Array.class, o);
        assertArrayEquals(new String[]{"alpha", "o'k", "a,b"},
                (String[]) ((Array) o).getArray(),
                "element values must round-trip through getObject exactly");
        // Label lookup is case-insensitive, same as getArray/findColumn.
        assertInstanceOf(java.sql.Array.class, rs.getObject("TAGS"));
    }

    @Test
    void getObject_matchesGetArrayForTheSameColumn() throws Exception {
        ResultSet rs = rsOver(arrayColumnIpc("ns", "BIGINT[]", "ARRAY[7,8]"));
        Array viaGetArray = rs.getArray(1);
        Array viaGetObject = (Array) rs.getObject(1);
        assertEquals(viaGetArray.getBaseType(), viaGetObject.getBaseType());
        assertEquals(viaGetArray.getBaseTypeName(), viaGetObject.getBaseTypeName());
        assertArrayEquals((Long[]) viaGetArray.getArray(), (Long[]) viaGetObject.getArray());
    }

    /** No value sniffing through getObject either: text[] of "1","2" is String[]. */
    @Test
    void getObject_textArrayOfNumericLookingStringsStaysStringArray() throws Exception {
        Object o = rsOver(arrayColumnIpc("tags", "TEXT[]", "ARRAY['1','2']")).getObject(1);
        assertArrayEquals(new String[]{"1", "2"}, (String[]) ((Array) o).getArray());
    }

    @Test
    void getObject_typedAsArrayClassAndAsStringClass() throws Exception {
        ResultSet rs = rsOver(arrayColumnIpc("ns", "INT[]", "ARRAY[1,2,3]"));
        Array a = rs.getObject(1, java.sql.Array.class);
        assertNotNull(a, "getObject(n, Array.class) must work on an array column");
        assertArrayEquals(new Integer[]{1, 2, 3}, (Integer[]) a.getArray());
        assertNotNull(rs.getObject("ns", java.sql.Array.class),
                "getObject(label, Array.class) must work too");
        // Asking for the text form still gives the raw cell.
        assertEquals("ARRAY[1,2,3]", rs.getObject(1, String.class));
        // Object.class is the same answer as the untyped getObject.
        assertInstanceOf(java.sql.Array.class, rs.getObject(1, Object.class));
    }

    /** getString must KEEP returning the text form (pgjdbc does the same). */
    @Test
    void getString_onArrayColumnStillReturnsText() throws Exception {
        ResultSet rs = rsOver(arrayColumnIpc("ns", "INT[]", "ARRAY[1,2,3]"));
        assertEquals("ARRAY[1,2,3]", rs.getString(1));
        assertEquals("ARRAY[1,2,3]", rs.getString("ns"));
    }

    /** Sentinel → still an Array, elements text. Missing stamp → raw text. */
    @Test
    void getObject_sentinelIsArrayAndMissingStampIsText() throws Exception {
        Object sentinel = rsOver(arrayColumnIpc("c", "_pdb_array", "ARRAY[1,2]")).getObject(1);
        assertInstanceOf(java.sql.Array.class, sentinel);
        assertArrayEquals(new String[]{"1", "2"}, (String[]) ((Array) sentinel).getArray());

        Object unstamped = rsOver(arrayColumnIpc("c", null, "ARRAY['a','b']")).getObject(1);
        assertInstanceOf(String.class, unstamped,
                "an unstamped column must keep returning its raw text");
        assertEquals("ARRAY['a','b']", unstamped);
    }

    @Test
    void getObject_nonArrayColumnIsUnchanged() throws Exception {
        ResultSet rs = rsOver(arrayPlusBigintIpc("INT[]", "ARRAY[1,2]", 42L));
        assertInstanceOf(java.sql.Array.class, rs.getObject(1));
        assertEquals(42L, rs.getObject(2), "a BIGINT column still returns a Long");
        assertEquals(42L, rs.getObject("id"));
        assertEquals(Integer.valueOf(42), rs.getObject(2, Integer.class));
    }

    @Test
    void getObject_nullArrayColumnIsNullAndSetsWasNull() throws Exception {
        ResultSet rs = new PdbResultSet(
                StreamState.StreamResult.arrow(arrayColumnIpc("ns", "INT[]", (String) null)));
        assertTrue(rs.next());
        assertNull(rs.getObject(1));
        assertTrue(rs.wasNull());
        assertNull(rs.getObject("ns"));
        assertTrue(rs.wasNull());
        assertNull(rs.getObject(1, java.sql.Array.class));
        assertTrue(rs.wasNull());
    }
}
