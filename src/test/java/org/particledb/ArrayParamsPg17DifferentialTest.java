package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Array parameters, differential against PostgreSQL 17 through pgjdbc: the
 * SAME {@code createArrayOf} + {@code setArray} binds, sent by pgjdbc to
 * PostgreSQL 17 and by this driver (typed native-wire array, EXECUTE tag 17)
 * to ParticleDB, must store and compute the same values -- element types
 * text, int2/4/8, float4/8, bool, numeric, uuid, date, timestamp, timestamptz
 * (a JVM-zone {@code Timestamp}), bytea and jsonb; NULL elements; quoting
 * hazards; the empty array; a 2-D array -- read back with {@code getArray} and
 * through array functions and operators evaluated on the bound value
 * ({@code cardinality}, subscripts, {@code ANY}, {@code @>}, {@code unnest},
 * {@code array_to_string}, {@code array_ndims}, {@code array_upper}).
 *
 * <p>The oracle is {@code PG_ORACLE_URL} (system property or env); its
 * identity is checked ({@code version()} names PostgreSQL 17, not
 * ParticleDB). Every PostgreSQL object lives in a schema this test creates and
 * drops. Skipped when the oracle or the {@code particledb} binary is absent.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ArrayParamsPg17DifferentialTest {

    private String pgUrl;
    private String schema;
    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        Class.forName("org.postgresql.Driver");
        pgUrl = System.getProperty("PG_ORACLE_URL", System.getenv("PG_ORACLE_URL"));
        Assumptions.assumeTrue(pgUrl != null && !pgUrl.isEmpty(), "PG_ORACLE_URL not set");
        String banner = null;
        try (Connection pg = DriverManager.getConnection(pgUrl);
             Statement st = pg.createStatement();
             ResultSet rs = st.executeQuery("SELECT version()")) {
            if (rs.next()) banner = rs.getString(1);
        } catch (SQLException e) {
            banner = null;
        }
        Assumptions.assumeTrue(banner != null && banner.startsWith("PostgreSQL 17")
                && !banner.contains("ParticleDB"), "not a PostgreSQL 17 oracle: " + banner);
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null, "particledb binary not available");
        schema = "jdbc_arrdiff_" + Long.toHexString(System.nanoTime());
        try (Connection pg = DriverManager.getConnection(pgUrl); Statement st = pg.createStatement()) {
            st.execute("CREATE SCHEMA " + schema);
        }
    }

    @AfterAll
    void tearDown() throws Exception {
        if (schema != null) {
            try (Connection pg = DriverManager.getConnection(pgUrl); Statement st = pg.createStatement()) {
                st.execute("DROP SCHEMA " + schema + " CASCADE"); // created by this test, above
            }
        }
        if (server != null) server.stop();
    }

    private Connection pg() throws SQLException {
        Connection c = DriverManager.getConnection(pgUrl);
        try (Statement st = c.createStatement()) { st.execute("SET search_path TO " + schema); }
        return c;
    }

    private Connection pdb() throws SQLException {
        return DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + server.port + "/test?preferUnixSocket=false");
    }

    /** A Java value as comparable text (arrays element-wise, bytea as hex, objects by toString). */
    static String norm(Object v) throws SQLException {
        if (v == null) return "null";
        if (v instanceof Array) return norm(((Array) v).getArray());
        if (v instanceof byte[]) {
            StringBuilder sb = new StringBuilder("\\x");
            for (byte b : (byte[]) v) sb.append(String.format("%02x", b));
            return sb.toString();
        }
        if (v instanceof Object[]) {
            List<String> parts = new ArrayList<>();
            for (Object e : (Object[]) v) parts.add(norm(e));
            return "[" + String.join(", ", parts) + "]";
        }
        if (v instanceof BigDecimal) return ((BigDecimal) v).toPlainString();
        if (v instanceof Number && !(v instanceof Double || v instanceof Float)) return Long.toString(((Number) v).longValue());
        String s = v.toString();
        // jsonb comes back as PGobject from pgjdbc and as String here; both print the document.
        return s;
    }

    private static List<String> rows(Connection c, String sql, Object[] binds, String elemType) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < binds.length; i++) {
                Object b = binds[i];
                if (b instanceof Object[]) ps.setArray(i + 1, c.createArrayOf(elemType, (Object[]) b));
                else ps.setObject(i + 1, b);
            }
            List<String> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData md = rs.getMetaData();
                while (rs.next()) {
                    List<String> r = new ArrayList<>();
                    for (int k = 1; k <= md.getColumnCount(); k++) r.add(norm(rs.getObject(k)));
                    out.add(String.join(" | ", r));
                }
            }
            return out;
        }
    }

    private void same(Connection pg, Connection pdb, String sql, String elemType, Object... binds) throws SQLException {
        List<String> want = rows(pg, sql, binds, elemType);
        List<String> got = rows(pdb, sql, binds, elemType);
        assertEquals(want, got, "PG17 vs ParticleDB: " + sql);
    }

    private void sameSorted(Connection pg, Connection pdb, String sql, String elemType, Object... binds)
            throws SQLException {
        List<String> want = rows(pg, sql, binds, elemType);
        List<String> got = rows(pdb, sql, binds, elemType);
        java.util.Collections.sort(want);
        java.util.Collections.sort(got);
        assertEquals(want, got, "PG17 vs ParticleDB: " + sql);
    }

    @Test
    void storedArraysReadBackAsOnPostgres() throws Exception {
        Timestamp ts = Timestamp.valueOf("2024-01-01 12:34:56.789");
        Object[][] cases = {
                {"text[]", "text", new Object[] {"x", null, "", "NULL", "a\"b\\c'd", "{,}", " sp "}},
                {"varchar(30)[]", "varchar", new Object[] {"v", "w"}},
                {"int2[]", "int2", new Object[] {(short) -32768, null, (short) 7}},
                {"int4[]", "int4", new Object[] {1, Integer.MAX_VALUE, null}},
                {"int8[]", "int8", new Object[] {Long.MIN_VALUE, 0L}},
                {"float4[]", "float4", new Object[] {1.5f, -0.25f}},
                {"float8[]", "float8", new Object[] {0.1, 1e300, null}},
                {"bool[]", "bool", new Object[] {true, false, null}},
                {"numeric[]", "numeric", new Object[] {new BigDecimal("-123.45"), new BigDecimal("7"), null}},
                {"uuid[]", "uuid", new Object[] {UUID.fromString("1f1f1f1f-0000-4000-8000-00000000abcd"), null}},
                {"date[]", "date", new Object[] {java.sql.Date.valueOf("2024-02-29")}},
                {"timestamp[]", "timestamp", new Object[] {ts, null}},
                {"timestamptz[]", "timestamptz", new Object[] {ts}},
                // byte[][], not Object[]: pgjdbc's createArrayOf("bytea") takes only that.
                {"bytea[]", "bytea", new byte[][] {{0, (byte) 0xff, 0x27}, {}}},
                {"jsonb[]", "jsonb", new Object[] {"{\"k\": [1, \"v\"]}"}},
                {"text[]", "text", new Object[0]},
                {"int4[]", "int4", new Object[] {new Object[] {1, 2, 3}, new Object[] {4, null, 6}}},
        };
        try (Connection pg = pg(); Connection pdb = pdb()) {
            for (int k = 0; k < cases.length; k++) {
                String col = (String) cases[k][0], elem = (String) cases[k][1];
                Object[] in = (Object[]) cases[k][2];
                String t = "ad" + k;
                for (Connection c : new Connection[] {pg, pdb}) {
                    try (Statement st = c.createStatement()) {
                        st.execute("CREATE TABLE " + t + " (id INT PRIMARY KEY, v " + col + ")");
                    }
                    try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + t + " VALUES (?, ?)")) {
                        ps.setInt(1, 1);
                        ps.setArray(2, c.createArrayOf(elem, in));
                        assertEquals(1, ps.executeUpdate());
                    }
                }
                same(pg, pdb, "SELECT v, cardinality(v) FROM " + t, elem);
            }
            assertTrue(pdb.unwrap(PdbConnection.class).serverPreparedExecutions() >= cases.length,
                    "the ParticleDB inserts went by server-side handle");
        }
    }

    @Test
    void functionsOverABoundArrayAnswerAsOnPostgres() throws Exception {
        Object[] words = {"a", null, "b,c", "q\"uote", "back\\slash", "it's"};
        Object[] grid = {new Object[] {1, 2}, new Object[] {3, 4}};
        try (Connection pg = pg(); Connection pdb = pdb()) {
            same(pg, pdb, "SELECT cardinality(?), array_length(?, 1)", "text", words, words);
            same(pg, pdb, "SELECT (?::text[])[3]", "text", (Object) words);
            same(pg, pdb, "SELECT 'b,c' = ANY(?), ? @> ARRAY['it''s']", "text", words, words);
            // Unordered (sorted here): a macOS PostgreSQL collates text by locale.
            sameSorted(pg, pdb, "SELECT x FROM unnest(?::text[]) AS u(x) WHERE x IS NOT NULL", "text", (Object) words);
            same(pg, pdb, "SELECT array_to_string(?, ',', '*')", "text", (Object) words);
            same(pg, pdb, "SELECT array_position(?, 'q\"uote')", "text", (Object) words);
            same(pg, pdb, "SELECT array_ndims(?), array_upper(?, 2), (?::int4[])[2][1]", "int4", grid, grid, grid);
            same(pg, pdb, "SELECT 3 = ANY(?), cardinality(?)", "int8", new Object[] {1L, 3L, null}, new Object[] {1L, 3L, null});
            same(pg, pdb, "SELECT cardinality(?)", "text", (Object) new Object[0]);
        }
    }
}
