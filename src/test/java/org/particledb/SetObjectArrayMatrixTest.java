package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.particledb.wire.WireV2Codec;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * pdb#68: {@code setObject(i, <Java array>)} binds an SQL array (1.0.8 sent
 * {@code "[I@6d06d69c"}, "malformed array literal") -- every Java array shape
 * pgjdbc 42.7.x binds, on every route the driver has for an array:
 *
 * <ul>
 *   <li>{@code bound}: by server-side handle, the array as EXECUTE tag 17;</li>
 *   <li>{@code literal}: {@code serverPrepare=false}, the typed PG literal;</li>
 *   <li>{@code no-array-cap}: a server that did not echo
 *       {@link WireV2Codec#CAP_EXT_ARRAY_PARAMS} (a {@link HelloCapStrippingProxy}
 *       clears the bit) -- the typed literal, not a refusal;</li>
 *   <li>{@code no-prepare-cap}: a server without {@link WireV2Codec#CAP_PREPARED}.</li>
 * </ul>
 *
 * <p>Invariants (no oracle needed): the read-back values, {@code cardinality},
 * {@code array_ndims}, {@code = ANY}, and an INSERT into int4[] / text[]
 * columns read back. With {@code PG_ORACLE_URL} naming a PostgreSQL 17, the
 * same {@code setObject} calls through pgjdbc must answer the same text form,
 * cardinality, dimensions and values. Skipped without a {@code particledb}
 * binary ({@code PDB_BIN}).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SetObjectArrayMatrixTest {

    private PdbTestServer server;
    private HelloCapStrippingProxy noArrayCap;
    private HelloCapStrippingProxy noPrepareCap;
    private String pgUrl;
    private String schema;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null, "particledb binary not available");
        noArrayCap = new HelloCapStrippingProxy(server.port, (byte) 0, WireV2Codec.CAP_EXT_ARRAY_PARAMS);
        noPrepareCap = new HelloCapStrippingProxy(server.port, WireV2Codec.CAP_PREPARED, (byte) 0);
        String url = System.getProperty("PG_ORACLE_URL", System.getenv("PG_ORACLE_URL"));
        if (url != null && !url.isEmpty()) {
            Class.forName("org.postgresql.Driver");
            try (Connection pg = DriverManager.getConnection(url); Statement st = pg.createStatement();
                 ResultSet rs = st.executeQuery("SELECT version()")) {
                rs.next();
                String banner = rs.getString(1);
                if (banner.startsWith("PostgreSQL 17") && !banner.contains("ParticleDB")) pgUrl = url;
            } catch (SQLException e) {
                pgUrl = null;
            }
        }
        if (pgUrl != null) {
            schema = "jdbc_setobj_" + Long.toHexString(System.nanoTime());
            try (Connection pg = DriverManager.getConnection(pgUrl); Statement st = pg.createStatement()) {
                st.execute("CREATE SCHEMA " + schema);
            }
        }
    }

    @AfterAll
    void tearDown() throws Exception {
        if (schema != null) {
            try (Connection pg = DriverManager.getConnection(pgUrl); Statement st = pg.createStatement()) {
                st.execute("DROP SCHEMA " + schema + " CASCADE"); // created by this test, above
            }
        }
        if (noArrayCap != null) noArrayCap.close();
        if (noPrepareCap != null) noPrepareCap.close();
        if (server != null) server.stop();
    }

    /** Route name -> a fresh connection on that route. */
    private Map<String, Connection> routes() throws SQLException {
        Map<String, Connection> m = new LinkedHashMap<>();
        String q = "/test?preferUnixSocket=false";
        m.put("bound", DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + server.port + q));
        m.put("literal", DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + server.port + q
                + "&serverPrepare=false"));
        m.put("no-array-cap", DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + noArrayCap.port + q));
        m.put("no-prepare-cap", DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + noPrepareCap.port + q));
        PdbConnection bound = m.get("bound").unwrap(PdbConnection.class);
        assertTrue(bound.arrayParamsEnabled(), "the real server echoes the array-parameter capability");
        assertFalse(m.get("no-array-cap").unwrap(PdbConnection.class).arrayParamsEnabled());
        assertTrue(m.get("no-array-cap").unwrap(PdbConnection.class).serverPrepareEnabled(),
                "no-array-cap still prepares: only the array bit is gone");
        assertFalse(m.get("no-prepare-cap").unwrap(PdbConnection.class).serverPrepareEnabled());
        assertTrue(noArrayCap.rewrittenHellos > 0 && noPrepareCap.rewrittenHellos > 0,
                "the proxies rewrote the HELLO_OK they claim to strip");
        return m;
    }

    private static void closeAll(Map<String, Connection> m) throws SQLException {
        for (Connection c : m.values()) c.close();
    }

    static final String[] WORDS = {"a", "b c", "q\"uote", "back\\slash", "com,ma", "{brace}", "NULL", null, "",
            " sp ", "it's", "tab\there"};

    /** {name, value, expected read-back (norm), expected cardinality, expected ndims (null for empty)}. */
    static Object[][] cases() {
        return new Object[][] {
                {"int[]", new int[] {1, 2, 3}, "[1,2,3]", 3, 1},
                {"long[]", new long[] {1, Long.MAX_VALUE, -2}, "[1,9223372036854775807,-2]", 3, 1},
                {"short[]", new short[] {1, -2}, "[1,-2]", 2, 1},
                {"double[]", new double[] {1.5, -2.25}, "[Double:1.5,Double:-2.25]", 2, 1},
                {"float[]", new float[] {1.5f, -0.25f}, "[Float:1.5,Float:-0.25]", 2, 1},
                {"boolean[]", new boolean[] {true, false}, "[Boolean:true,Boolean:false]", 2, 1},
                {"String[]", WORDS, "[String:a,String:b c,String:q\"uote,String:back\\slash,String:com,ma,"
                        + "String:{brace},String:NULL,null,String:,String: sp ,String:it's,String:tab\there]", 12, 1},
                {"Integer[]", new Integer[] {1, null, 3}, "[1,null,3]", 3, 1},
                {"Long[]", new Long[] {null, 5L}, "[null,5]", 2, 1},
                {"Short[]", new Short[] {(short) 7, null}, "[7,null]", 2, 1},
                {"Double[]", new Double[] {1.0, null}, "[Double:1.0,null]", 2, 1},
                {"Float[]", new Float[] {2.5f, null}, "[Float:2.5,null]", 2, 1},
                {"Boolean[]", new Boolean[] {true, null}, "[Boolean:true,null]", 2, 1},
                {"int[0]", new int[0], "[]", 0, null},
                {"String[0]", new String[0], "[]", 0, null},
                {"Integer[0]", new Integer[0], "[]", 0, null},
                {"int[][]", new int[][] {{1, 2}, {3, 4}}, "[[1,2],[3,4]]", 4, 2},
                {"Integer[][]", new Integer[][] {{1, null}, {3, 4}}, "[[1,null],[3,4]]", 4, 2},
                {"createArrayOf int4", "CAO:int4", "[1,null,3]", 3, 1},
                {"createArrayOf text", "CAO:text", "[String:a\"b,null,String:NULL,String:x,y]", 4, 1},
        };
    }

    static Object bindValue(Connection c, Object v) throws SQLException {
        if ("CAO:int4".equals(v)) return c.createArrayOf("int4", new Object[] {1, null, 3});
        if ("CAO:text".equals(v)) return c.createArrayOf("text", new Object[] {"a\"b", null, "NULL", "x,y"});
        return v;
    }

    /** A value as comparable text: arrays element-wise, integers bare, other values with their class. */
    static String norm(Object v) throws SQLException {
        if (v == null) return "null";
        if (v instanceof Array) return norm(((Array) v).getArray());
        if (v.getClass().isArray()) {
            List<String> p = new ArrayList<>();
            for (int i = 0; i < java.lang.reflect.Array.getLength(v); i++) p.add(norm(java.lang.reflect.Array.get(v, i)));
            return "[" + String.join(",", p) + "]";
        }
        if (v instanceof Number && !(v instanceof Double || v instanceof Float)) return Long.toString(((Number) v).longValue());
        return v.getClass().getSimpleName() + ":" + v;
    }

    static final String PROBE = "SELECT c::text, cardinality(c), array_ndims(c), c FROM (SELECT ? AS c) s";

    /** {text form, cardinality, ndims, value} of {@code v} bound by setObject (with Types.ARRAY when asked). */
    static List<String> probe(Connection c, Object v, boolean typesArray) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(PROBE)) {
            Object b = bindValue(c, v);
            if (typesArray) ps.setObject(1, b, Types.ARRAY);
            else ps.setObject(1, b);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                List<String> r = List.of(rs.getString(1), String.valueOf(rs.getInt(2)),
                        String.valueOf(rs.getObject(3)), norm(rs.getObject(4)));
                assertFalse(rs.next());
                return r;
            }
        }
    }

    @Test
    void everyShapeRoundTripsOnEveryRoute() throws Exception {
        Map<String, Connection> routes = routes();
        try {
            for (Map.Entry<String, Connection> route : routes.entrySet()) {
                PdbConnection pc = route.getValue().unwrap(PdbConnection.class);
                long boundBefore = pc.serverPreparedExecutions();
                int executions = 0;
                for (boolean typesArray : new boolean[] {false, true}) {
                    for (Object[] k : cases()) {
                        String what = route.getKey() + (typesArray ? " Types.ARRAY " : " ") + k[0];
                        List<String> r = probe(route.getValue(), k[1], typesArray);
                        executions++;
                        assertEquals(k[2], r.get(3), what + ": value read back");
                        assertEquals(String.valueOf(k[3]), r.get(1), what + ": cardinality");
                        assertEquals(String.valueOf(k[4]), r.get(2), what + ": array_ndims");
                    }
                }
                long bound = pc.serverPreparedExecutions() - boundBefore;
                if (route.getKey().equals("bound")) {
                    assertEquals(executions, bound, "every execution went by handle on the bound route");
                } else {
                    assertEquals(0, bound, route.getKey() + ": the array was rendered as its literal");
                }
            }
        } finally {
            closeAll(routes);
        }
    }

    @Test
    void anyMatchesAndColumnsStoreTheValueOnEveryRoute() throws Exception {
        Map<String, Connection> routes = routes();
        try {
            int n = 0;
            for (Map.Entry<String, Connection> route : routes.entrySet()) {
                Connection c = route.getValue();
                String r = route.getKey();
                // The issue's own statement.
                for (Object ints : new Object[] {new int[] {1, 2, 3}, new Integer[] {1, 2, null}, new long[] {2}}) {
                    try (PreparedStatement ps = c.prepareStatement("SELECT 1 WHERE 2 = ANY(?)")) {
                        ps.setObject(1, ints);
                        try (ResultSet rs = ps.executeQuery()) {
                            assertTrue(rs.next(), r + ": 2 = ANY(" + norm(ints) + ")");
                        }
                    }
                }
                try (PreparedStatement ps = c.prepareStatement("SELECT 1 WHERE 9 = ANY(?)")) {
                    ps.setObject(1, new int[] {1, 2, 3});
                    try (ResultSet rs = ps.executeQuery()) { assertFalse(rs.next(), r + ": 9 is not in {1,2,3}"); }
                }
                try (PreparedStatement ps = c.prepareStatement("SELECT 1 WHERE 'NULL' = ANY(?) AND 'q\"uote' = ANY(?)")) {
                    ps.setObject(1, WORDS);
                    ps.setObject(2, WORDS);
                    try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next(), r + ": text = ANY"); }
                }
                String t = "setobj_t" + (n++);
                try (Statement st = c.createStatement()) {
                    st.execute("CREATE TABLE " + t + " (id INT PRIMARY KEY, i int4[], s text[])");
                }
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + t + " VALUES (?, ?, ?)")) {
                    ps.setInt(1, 1);
                    ps.setObject(2, new int[] {4, -5, 6});
                    ps.setObject(3, WORDS);
                    assertEquals(1, ps.executeUpdate(), r);
                    ps.setInt(1, 2);
                    ps.setObject(2, new Integer[] {null, 7}, Types.ARRAY);
                    ps.setObject(3, new String[0], Types.ARRAY);
                    ps.addBatch();
                    ps.setInt(1, 3);
                    ps.setObject(2, new int[][] {{1, 2}, {3, 4}});
                    ps.setObject(3, new String[] {"x"});
                    ps.addBatch();
                    assertArrayEquals(new int[] {1, 1}, ps.executeBatch(), r + ": batch with arrays");
                }
                try (Statement st = c.createStatement();
                     ResultSet rs = st.executeQuery("SELECT id, i, s, i::text FROM " + t + " ORDER BY id")) {
                    assertTrue(rs.next());
                    assertEquals("[4,-5,6]", norm(rs.getArray(2)), r);
                    assertEquals(norm(WORDS), norm(rs.getArray(3)), r + ": text[] elements stored verbatim");
                    assertTrue(rs.next());
                    assertEquals("[null,7]", norm(rs.getArray(2)), r);
                    assertEquals("[]", norm(rs.getArray(3)), r);
                    assertTrue(rs.next());
                    assertEquals("{{1,2},{3,4}}", rs.getString(4), r);
                    assertEquals("[String:x]", norm(rs.getArray(3)), r);
                    assertFalse(rs.next());
                }
            }
        } finally {
            closeAll(routes);
        }
    }

    /**
     * Inference from VALUES ({@code Object[]}, {@code Collection}) needs every
     * value to name one type. pgjdbc 42.7 refuses every {@code Object[]} with
     * 07006; this driver binds a homogeneous one and refuses a mixed one with
     * the same SQLState, never coercing to the first element's type.
     */
    @Test
    void objectArraysBindOnlyWhenTheirValuesAgree() throws Exception {
        Map<String, Connection> routes = routes();
        try {
            for (Map.Entry<String, Connection> route : routes.entrySet()) {
                Connection c = route.getValue();
                assertEquals("[1,2]", probe(c, new Object[] {1, 2}, false).get(3));
                assertEquals("[String:x,null]", probe(c, java.util.Arrays.asList("x", null), false).get(3));
                for (Object mixed : new Object[] {new Object[] {1, "a"}, new Object[] {"a", 1}, new Object[] {1, 2L},
                        new Object[] {new int[] {1}, new long[] {2}}, java.util.Arrays.asList("1", 2)}) {
                    try (PreparedStatement ps = c.prepareStatement(PROBE)) {
                        SQLException e = assertThrows(SQLException.class, () -> ps.setObject(1, mixed),
                                route.getKey() + ": " + norm(mixed));
                        assertEquals("07006", e.getSQLState(), e.toString());
                    }
                }
                try (PreparedStatement ps = c.prepareStatement(PROBE)) {
                    SQLException e = assertThrows(SQLException.class, () -> ps.setObject(1, new Object[] {null}));
                    assertEquals("42P18", e.getSQLState(), e.toString());
                }
            }
        } finally {
            closeAll(routes);
        }
    }

    /**
     * A foreign {@code java.sql.Array} (pgjdbc 42.7.4 binds its
     * {@code toString()}, "malformed array literal") binds its elements here,
     * whether {@code getArray()} answers {@code Object[]} or a primitive array.
     */
    @Test
    void aForeignSqlArrayBindsItsElements() throws Exception {
        Map<String, Connection> routes = routes();
        try {
            for (Map.Entry<String, Connection> route : routes.entrySet()) {
                Connection c = route.getValue();
                assertEquals("[5,null,6]", probe(c, new ForeignArray("int4", new Integer[] {5, null, 6}), false).get(3));
                assertEquals("[7,8]", probe(c, new ForeignArray("integer", new int[] {7, 8}), true).get(3));
                List<String> r = probe(c, new ForeignArray("text", new String[] {"p\"q", null, "NULL"}), false);
                assertEquals("{\"p\\\"q\",NULL,\"NULL\"}", r.get(0), route.getKey());
                try (PreparedStatement ps = c.prepareStatement("SELECT cardinality(?)")) {
                    ps.setArray(1, new ForeignArray("int8", new long[] {1, 2, 3}));
                    try (ResultSet rs = ps.executeQuery()) { rs.next(); assertEquals(3, rs.getInt(1)); }
                }
            }
        } finally {
            closeAll(routes);
        }
    }

    /** Every shape pgjdbc binds: the same text form, cardinality, dimensions and values as pgjdbc + PostgreSQL 17. */
    @Test
    void everyShapeAnswersAsPgjdbcOnPostgres17() throws Exception {
        Assumptions.assumeTrue(pgUrl != null, "PG_ORACLE_URL does not name a PostgreSQL 17");
        Map<String, Connection> routes = routes();
        try (Connection pg = DriverManager.getConnection(pgUrl)) {
            for (boolean typesArray : new boolean[] {false, true}) {
                for (Object[] k : cases()) {
                    List<String> want = probe(pg, k[1], typesArray);
                    for (Map.Entry<String, Connection> route : routes.entrySet()) {
                        assertEquals(want, probe(route.getValue(), k[1], typesArray),
                                "PG17 vs ParticleDB " + route.getKey() + (typesArray ? " Types.ARRAY " : " ") + k[0]);
                    }
                }
            }
        } finally {
            closeAll(routes);
        }
    }

    /**
     * A 2-D text array: quoted elements in a nested literal (and the same
     * array bound by tag 17) must stay two-dimensional. The literal form is
     * parsed by the server; both routes must answer PG17's
     * {@code {{a,b},{c,NULL}}}, 2 dimensions, 4 elements.
     */
    @Test
    void twoDimensionalTextArrayKeepsItsDimensions() throws Exception {
        Map<String, Connection> routes = routes();
        try {
            for (Map.Entry<String, Connection> route : routes.entrySet()) {
                List<String> r = probe(route.getValue(), new String[][] {{"a", "b"}, {"c", null}}, false);
                assertEquals(List.of("{{a,b},{c,NULL}}", "4", "2", "[[String:a,String:b],[String:c,null]]"), r,
                        route.getKey());
                r = probe(route.getValue(), new String[][] {{"q\"uote", "com,ma"}, {"NULL", "{x}"}}, true);
                assertEquals(List.of("{{\"q\\\"uote\",\"com,ma\"},{\"NULL\",\"{x}\"}}", "4", "2",
                        "[[String:q\"uote,String:com,ma],[String:NULL,String:{x}]]"), r, route.getKey());
            }
        } finally {
            closeAll(routes);
        }
    }

    /** A {@code java.sql.Array} from no driver: what an application or another library hands in. */
    static final class ForeignArray implements Array {
        final String type;
        final Object elements;
        ForeignArray(String type, Object elements) { this.type = type; this.elements = elements; }
        @Override public String getBaseTypeName() { return type; }
        @Override public int getBaseType() { return Types.OTHER; }
        @Override public Object getArray() { return elements; }
        @Override public Object getArray(Map<String, Class<?>> map) { return elements; }
        @Override public Object getArray(long index, int count) { throw new UnsupportedOperationException(); }
        @Override public Object getArray(long index, int count, Map<String, Class<?>> map) { throw new UnsupportedOperationException(); }
        @Override public ResultSet getResultSet() { throw new UnsupportedOperationException(); }
        @Override public ResultSet getResultSet(Map<String, Class<?>> map) { throw new UnsupportedOperationException(); }
        @Override public ResultSet getResultSet(long index, int count) { throw new UnsupportedOperationException(); }
        @Override public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) { throw new UnsupportedOperationException(); }
        @Override public void free() { }
    }
}
