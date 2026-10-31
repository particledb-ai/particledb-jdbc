package org.particledb;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A DICTIONARY-encoded column must read as its VALUE type through every
 * accessor (a long read/write consistency run: 16,391 reads of a text column returned a
 * {@code java.lang.Byte}).
 *
 * <p>The server's scan dictionary-encodes a text column of {@code >= 1000}
 * rows with {@code <= 256} distinct values, and servers up to 1.1.19-rc.7
 * shipped that {@code Dictionary(UInt8, Utf8)} over the native wire unchanged.
 * The Arrow Java decoder of those releases read the INDEX vector: {@code getObject} was the {@code Byte} index,
 * {@code getString} its decimal text, and the column was typed as a small
 * integer.
 *
 * <p>Two halves:
 * <ul>
 *   <li><b>Driver, no server:</b> a real Arrow IPC stream carrying a dictionary
 *   column (UInt8 and Int32 indices, NULL index, Int64 values) decodes to the
 *   values, with the same metadata as the plain column. This is what protects
 *   a 1.0.7 driver against an OLDER server that still sends dictionaries.</li>
 *   <li><b>Live server:</b> 1500 rows, 5 distinct values, read through IN,
 *   range and full scans on {@code Statement} and {@code PreparedStatement},
 *   for TEXT, VARCHAR and CHAR.</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@org.junit.jupiter.api.Tag("arrow-java")
public class DictionaryEncodedTextTest {

    private static final String[] VALUES = {"ab", "cd", "ef", "gh", "z"};
    private static final int ROWS = 1500;

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    // ── Driver half: a dictionary stream, no server ─────────────────────────

    /** Build an IPC stream: column 0 {@code k} BIGINT, column 1 {@code v}
     *  dictionary-encoded with {@code indexType} over {@code dictValues}. */
    private static byte[] dictStream(ArrowType.Int indexType, Object[] dictValues, Integer[] indices)
            throws Exception {
        boolean textValues = dictValues[0] instanceof String;
        ArrowType valueType = textValues ? new ArrowType.Utf8() : new ArrowType.Int(64, true);
        DictionaryEncoding enc = new DictionaryEncoding(7L, false, indexType);
        try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            FieldVector dv = textValues
                    ? new VarCharVector(new Field("dict", FieldType.nullable(valueType), null), alloc)
                    : new BigIntVector(new Field("dict", FieldType.nullable(valueType), null), alloc);
            dv.allocateNew();
            for (int i = 0; i < dictValues.length; i++) {
                if (textValues) {
                    ((VarCharVector) dv).setSafe(i, ((String) dictValues[i]).getBytes(StandardCharsets.UTF_8));
                } else {
                    ((BigIntVector) dv).setSafe(i, (Long) dictValues[i]);
                }
            }
            dv.setValueCount(dictValues.length);
            Dictionary dict = new Dictionary(dv, enc);
            DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
            provider.put(dict);

            Field kField = new Field("k", FieldType.nullable(new ArrowType.Int(64, true)), null);
            Field vField = new Field("v", new FieldType(true, indexType, enc), null);
            Schema schema = new Schema(Arrays.asList(kField, vField));
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
                 ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                BigIntVector k = (BigIntVector) root.getVector(0);
                FieldVector v = root.getVector(1);
                k.allocateNew(indices.length);
                v.setInitialCapacity(indices.length);
                v.allocateNew();
                for (int r = 0; r < indices.length; r++) {
                    k.setSafe(r, r);
                    if (indices[r] == null) {
                        v.setNull(r);
                    } else if (v instanceof UInt1Vector) {
                        ((UInt1Vector) v).setSafe(r, indices[r]);
                    } else {
                        ((IntVector) v).setSafe(r, indices[r]);
                    }
                }
                k.setValueCount(indices.length);
                v.setValueCount(indices.length);
                root.setRowCount(indices.length);
                try (ArrowStreamWriter w = new ArrowStreamWriter(root, provider, Channels.newChannel(out))) {
                    w.start();
                    w.writeBatch();
                    w.end();
                }
                dv.close();
                return out.toByteArray();
            }
        }
    }

    private static PdbResultSet rs(byte[] ipc) {
        return new PdbResultSet(org.particledb.wire.StreamState.StreamResult.arrow(ipc));
    }

    /** UInt8 indices - exactly what the server's scan produced (rc.7). */
    @Test
    void uint8DictionaryTextReadsAsTextThroughEveryAccessor() throws Exception {
        String[] dictValues = {"ab", "cd", "z"};
        Integer[] idx = {2, 0, 1, null, 2};
        String[] want = {"z", "ab", "cd", null, "z"};
        byte[] ipc = dictStream(new ArrowType.Int(8, false), dictValues, idx);
        assertTextColumn(ipc, want);
    }

    /** Int32 indices - the other index width Arrow writers commonly pick. */
    @Test
    void int32DictionaryTextReadsAsTextThroughEveryAccessor() throws Exception {
        String[] dictValues = {"ab", "cd", "z"};
        Integer[] idx = {1, 1, 0, 2, null};
        String[] want = {"cd", "cd", "ab", "z", null};
        byte[] ipc = dictStream(new ArrowType.Int(32, true), dictValues, idx);
        assertTextColumn(ipc, want);
    }

    private static void assertTextColumn(byte[] ipc, String[] want) throws Exception {
        // The reference: the same values as a PLAIN Utf8 column.
        ResultSetMetaData plainMd;
        try (PdbResultSet plain = ArrowIpc.build().int64("k", 0L).utf8("v", "x").rs()) {
            plainMd = plain.getMetaData();
            assertEquals(Types.VARCHAR, plainMd.getColumnType(2), "reference column type");
        }
        try (PdbResultSet r = rs(ipc)) {
            ResultSetMetaData md = r.getMetaData();
            assertEquals(plainMd.getColumnType(2), md.getColumnType(2),
                    "a dictionary text column must report the VALUE type, not the index type");
            assertEquals(plainMd.getColumnTypeName(2), md.getColumnTypeName(2));
            assertEquals(plainMd.getColumnClassName(2), md.getColumnClassName(2));
            int row = 0;
            while (r.next()) {
                Object o = r.getObject(2);
                if (want[row] == null) {
                    assertNull(o, "row " + row + ": a NULL index is a NULL value");
                    assertNull(r.getString(2));
                    assertTrue(r.wasNull());
                } else {
                    assertTrue(o instanceof String,
                            "row " + row + ": getObject returned " + (o == null ? "null" : o.getClass().getName())
                                    + " (" + o + ") for a text column -- the dictionary INDEX, not the value");
                    assertEquals(want[row], o, "row " + row + " getObject");
                    assertEquals(want[row], r.getString(2), "row " + row + " getString");
                    assertEquals(want[row], r.getString("v"), "row " + row + " getString(label)");
                }
                row++;
            }
            assertEquals(want.length, row, "row count");
        }
    }

    /** A dictionary over BIGINT values: getInt/getLong read the value, not the index. */
    @Test
    void int64DictionaryReadsTheValueThroughNumericAccessors() throws Exception {
        Object[] dictValues = {1000L, -7L, 42L};
        Integer[] idx = {2, 0, 1};
        long[] want = {42L, 1000L, -7L};
        try (PdbResultSet r = rs(dictStream(new ArrowType.Int(8, false), dictValues, idx))) {
            assertEquals(Types.BIGINT, r.getMetaData().getColumnType(2));
            int row = 0;
            while (r.next()) {
                assertEquals(want[row], r.getLong(2), "row " + row + " getLong");
                assertEquals((int) want[row], r.getInt(2), "row " + row + " getInt");
                assertEquals(Long.valueOf(want[row]), r.getObject(2), "row " + row + " getObject");
                row++;
            }
            assertEquals(3, row);
        }
    }

    // ── Live half: the server's own dictionary path ─────────────────────────

    private Connection open() throws SQLException {
        return DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + server.port
                + "/test?preferUnixSocket=false");
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static String valueOf(long k, int pad) {
        String v = VALUES[(int) (k % VALUES.length)];
        StringBuilder sb = new StringBuilder(v);
        while (sb.length() < pad) sb.append(' ');
        return sb.toString();
    }

    private static void check(String what, ResultSet r, int expectRows, int pad) throws SQLException {
        assertEquals(Types.VARCHAR, r.getMetaData().getColumnType(2),
                what + ": column type " + r.getMetaData().getColumnTypeName(2));
        int n = 0;
        while (r.next()) {
            long k = r.getLong(1);
            Object o = r.getObject(2);
            assertTrue(o instanceof String, what + ": k=" + k + " getObject returned "
                    + (o == null ? "null" : o.getClass().getName() + " " + o) + ", not a String");
            assertEquals(valueOf(k, pad), o, what + ": k=" + k + " getObject");
            assertEquals(valueOf(k, pad), r.getString(2), what + ": k=" + k + " getString");
            n++;
        }
        assertEquals(expectRows, n, what + ": row count");
    }

    @Test
    void lowCardinalityTextScansReadAsTextOverTheNativeWire() throws Exception {
        Assumptions.assumeTrue(server != null,
                "particledb binary not available -- skipping the live half (the driver half still ran)");
        String[][] types = {{"dt_text", "TEXT", "0"}, {"dt_varchar", "VARCHAR(16)", "0"}, {"dt_char", "CHAR(2)", "2"}};
        try (Connection c = open()) {
            for (String[] t : types) {
                String table = t[0];
                int pad = Integer.parseInt(t[2]);
                exec(c, "CREATE TABLE " + table + " (k BIGINT PRIMARY KEY, v " + t[1] + ")");
                for (int base = 0; base < ROWS; base += 500) {
                    StringBuilder sb = new StringBuilder("INSERT INTO " + table + " VALUES ");
                    for (int k = base; k < base + 500; k++) {
                        if (k > base) sb.append(", ");
                        sb.append('(').append(k).append(", '").append(VALUES[k % VALUES.length]).append("')");
                    }
                    exec(c, sb.toString());
                }
                String[][] queries = {
                        {"IN list", "SELECT k, v FROM " + table + " WHERE k IN (3, 7, 11, 1499) ORDER BY k", "4"},
                        {"range", "SELECT k, v FROM " + table + " WHERE k >= 100 AND k < 140 ORDER BY k", "40"},
                        {"full scan", "SELECT k, v FROM " + table + " ORDER BY k", String.valueOf(ROWS)},
                };
                for (String[] q : queries) {
                    int n = Integer.parseInt(q[2]);
                    try (Statement st = c.createStatement(); ResultSet r = st.executeQuery(q[1])) {
                        check(t[1] + " " + q[0] + " (Statement)", r, n, pad);
                    }
                    try (PreparedStatement ps = c.prepareStatement(q[1]); ResultSet r = ps.executeQuery()) {
                        check(t[1] + " " + q[0] + " (PreparedStatement)", r, n, pad);
                    }
                }
                // A bound range: the server-prepared path with a parameter.
                try (PreparedStatement ps = c.prepareStatement(
                        "SELECT k, v FROM " + table + " WHERE k >= ? AND k < ? ORDER BY k")) {
                    ps.setLong(1, 1000);
                    ps.setLong(2, 1100);
                    try (ResultSet r = ps.executeQuery()) {
                        check(t[1] + " bound range", r, 100, pad);
                    }
                }
            }
        }
    }
}
