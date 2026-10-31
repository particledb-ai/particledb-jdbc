package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Array parameters end to end against a live server: {@code setArray} of
 * {@code createArrayOf(...)}, {@code setObject} of Java arrays and
 * collections, NULL elements, multidimensional arrays, batches, and
 * {@code getArray} on the way back.
 *
 * <p>Pinned at the wire: an execution with an array parameter is sent by
 * server-side handle -- EXECUTE frames only ({@link PdbConnection#serverPreparedExecutions}
 * grows by one per execution and {@link PdbConnection#requestsSent} by exactly
 * that plus the one PREPARE, so no QUERY frame carried inlined text) -- and a
 * statement that cannot be bound raises {@link SQLFeatureNotSupportedException}
 * instead of falling back to text. {@code serverPrepare=false} is the one
 * explicit text route, and it stores the same values.
 *
 * <p>Skipped via {@link Assumptions} when the {@code particledb} binary is absent
 * (an error under {@code -Dpdb.require.server=true}).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ArrayParamsLiveTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null, "particledb binary not available");
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private Connection open(boolean serverPrepare) throws SQLException {
        return DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + server.port
                + "/test?preferUnixSocket=false&serverPrepare=" + serverPrepare);
    }

    private static PdbConnection pc(Connection c) throws SQLException {
        return c.unwrap(PdbConnection.class);
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) { st.execute(sql); }
    }

    static final String SHARD = "task_archive_shard_00";
    static final String SHARD_DDL = "CREATE TABLE " + SHARD + " (tenant_id varchar(255) NOT NULL, task_id varchar(255) NOT NULL,"
            + " task_reference_name varchar(255) NOT NULL, workflow_id varchar(255) NOT NULL, task_type varchar(255),"
            + " workflow_name varchar(255), created_on bigint, scheduled_on bigint, modified_on bigint, end_on bigint,"
            + " status varchar(255) NOT NULL, task_input jsonb, task_output jsonb, json_data jsonb, index_data text[],"
            + " PRIMARY KEY (tenant_id, task_id))";
    /** A Conductor task upsert (PostgresExecutionDAO shape). */
    static final String UPSERT = "INSERT INTO " + SHARD + " (tenant_id, task_id, task_reference_name, workflow_id, task_type, workflow_name, "
            + "created_on, scheduled_on, modified_on, end_on, status, task_input, task_output, json_data, index_data) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?::jsonb, ?) "
            + "ON CONFLICT (tenant_id, task_id) DO UPDATE SET "
            + "json_data = EXCLUDED.json_data, modified_on = EXCLUDED.modified_on, end_on = EXCLUDED.end_on, "
            + "status = EXCLUDED.status, task_output = EXCLUDED.task_output, index_data = EXCLUDED.index_data "
            + "WHERE " + SHARD + ".modified_on <= EXCLUDED.modified_on "
            + "OR EXCLUDED.status IN ('COMPLETED','FAILED','CANCELED','TIMED_OUT','FAILED_WITH_TERMINAL_ERROR','SKIPPED')";

    static String[] words(String tid, String status) {
        return new String[] {"simulatedTask", status, "simple_1", tid, "wf-1", "loadtest_parallel", "SIMPLE",
                "a,b", "q\"uote", "back\\slash", "it's", "{brace}", "NULL", ""};
    }

    static void upsert(Connection c, String tid, String status, long now) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(UPSERT)) {
            ps.setString(1, "0000");
            ps.setString(2, tid);
            ps.setString(3, "simple_1");
            ps.setString(4, "wf-1");
            ps.setString(5, "simulatedTask");
            ps.setString(6, "loadtest_parallel");
            ps.setLong(7, now);
            ps.setLong(8, now);
            ps.setLong(9, now);
            ps.setLong(10, 0);
            ps.setString(11, status);
            ps.setString(12, "{\"taskIndex\":1}");
            ps.setString(13, "{}");
            ps.setString(14, "{\"taskId\":\"" + tid + "\",\"status\":\"" + status + "\"}");
            ps.setArray(15, c.createArrayOf("text", words(tid, status)));
            assertEquals(1, ps.executeUpdate());
        }
    }

    /**
     * The Conductor task upsert with {@code setArray}: EXECUTE frames only --
     * the request counter proves no text QUERY went out -- and the row holds
     * exactly the bound words.
     */
    @Test
    void conductorUpsertGoesByHandleNeverAsText() throws Exception {
        try (Connection c = open(true)) {
            assertTrue(pc(c).arrayParamsEnabled(), "the server must echo the array-parameter capability");
            exec(c, SHARD_DDL);
            long execs0 = pc(c).serverPreparedExecutions(), req0 = pc(c).requestsSent();
            int n = 0;
            long now = 1_760_000_000_000L;
            for (String tid : new String[] {"t-1", "t-2"}) {
                for (String status : new String[] {"SCHEDULED", "IN_PROGRESS", "COMPLETED"}) {
                    upsert(c, tid, status, ++now);
                    n++;
                }
            }
            assertEquals(n, pc(c).serverPreparedExecutions() - execs0, "every upsert by server-side handle");
            assertEquals(n + 1, pc(c).requestsSent() - req0, "EXECUTEs plus ONE PREPARE: no text QUERY was sent");
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT task_id, status, index_data FROM " + SHARD + " ORDER BY task_id")) {
                for (String tid : new String[] {"t-1", "t-2"}) {
                    assertTrue(rs.next());
                    assertEquals(tid, rs.getString(1));
                    assertEquals("COMPLETED", rs.getString(2));
                    assertArrayEquals(words(tid, "COMPLETED"), (String[]) rs.getArray(3).getArray());
                    assertArrayEquals(words(tid, "COMPLETED"), (String[]) ((Array) rs.getObject(3)).getArray());
                }
                assertFalse(rs.next());
            }
            // Conductor's search predicate, the array bound too.
            try (PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM " + SHARD + " WHERE index_data @> ?")) {
                ps.setArray(1, c.createArrayOf("varchar", new Object[] {"q\"uote", "COMPLETED"}));
                try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); }
            }
        }
    }

    /** One element type: bind by handle and by explicit text, read back both. */
    private void roundTrip(Connection h, Connection t, int k, String colType, String elemType, Object[] in,
                           Object expected) throws Exception {
        String table = "art" + k;
        exec(h, "CREATE TABLE " + table + " (id INT PRIMARY KEY, v " + colType + ")");
        long execs0 = pc(h).serverPreparedExecutions();
        try (PreparedStatement ps = h.prepareStatement("INSERT INTO " + table + " VALUES (?, ?)")) {
            ps.setInt(1, 1);
            ps.setArray(2, h.createArrayOf(elemType, in));
            assertEquals(1, ps.executeUpdate());
        }
        assertEquals(1, pc(h).serverPreparedExecutions() - execs0, colType + ": by handle");
        try (PreparedStatement ps = t.prepareStatement("INSERT INTO " + table + " VALUES (?, ?)")) {
            ps.setInt(1, 2);
            ps.setArray(2, t.createArrayOf(elemType, in));
            assertEquals(1, ps.executeUpdate());
        }
        assertEquals(0, pc(t).serverPreparedExecutions(), "serverPrepare=false never binds");
        try (Statement st = h.createStatement();
             ResultSet rs = st.executeQuery("SELECT id, v FROM " + table + " ORDER BY id")) {
            for (int id = 1; id <= 2; id++) {
                assertTrue(rs.next());
                Object got = rs.getArray(2).getArray();
                assertTrue(Arrays.deepEquals(new Object[] {expected}, new Object[] {got}),
                        colType + " row " + id + ": " + Arrays.deepToString(new Object[] {got}));
            }
        }
        // The stored array equals the same array bound in a predicate. (Not for
        // date[] / timestamp[] / timestamptz[] / bytea[] / jsonb[] columns: the engine
        // keeps those as text[] and has no text[] = date[] operator -- the
        // same refusal for a literal '{..}'::date[], so not the wire's.)
        if (!STORED_AS_TEXT.contains(colType)) try (PreparedStatement ps = h.prepareStatement("SELECT count(*) FROM " + table + " WHERE v = ?")) {
            ps.setArray(1, h.createArrayOf(elemType, in));
            try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); assertEquals(2, rs.getInt(1), colType); }
        }
    }

    private static final List<String> STORED_AS_TEXT = List.of("date[]", "timestamp[]", "timestamptz[]", "bytea[]", "jsonb[]");

    @Test
    void everyElementTypeRoundTrips() throws Exception {
        UUID u = UUID.fromString("1f1f1f1f-0000-4000-8000-00000000abcd");
        Timestamp ts = Timestamp.valueOf("2024-01-01 12:34:56.789");
        try (Connection h = open(true); Connection t = open(false)) {
            int k = 0;
            roundTrip(h, t, k++, "text[]", "text", new Object[] {"x", null, "", "NULL", "a\"b\\c'd"},
                    new String[] {"x", null, "", "NULL", "a\"b\\c'd"});
            roundTrip(h, t, k++, "varchar(20)[]", "varchar", new Object[] {"v"}, new String[] {"v"});
            roundTrip(h, t, k++, "int2[]", "int2", new Object[] {(short) -32768, null, 7}, new Short[] {-32768, null, 7});
            roundTrip(h, t, k++, "int4[]", "integer", new Object[] {1, Integer.MAX_VALUE, null},
                    new Integer[] {1, Integer.MAX_VALUE, null});
            roundTrip(h, t, k++, "int8[]", "bigint", new Object[] {Long.MIN_VALUE, 0L}, new Long[] {Long.MIN_VALUE, 0L});
            roundTrip(h, t, k++, "float4[]", "real", new Object[] {1.5f, -0.25f}, new Float[] {1.5f, -0.25f});
            roundTrip(h, t, k++, "float8[]", "float8", new Object[] {0.1, 1e300, null}, new Double[] {0.1, 1e300, null});
            roundTrip(h, t, k++, "bool[]", "boolean", new Object[] {true, false, null}, new Boolean[] {true, false, null});
            roundTrip(h, t, k++, "numeric[]", "numeric", new Object[] {new BigDecimal("-123.45"), new BigDecimal("7")},
                    new BigDecimal[] {new BigDecimal("-123.45"), new BigDecimal("7")});
            roundTrip(h, t, k++, "uuid[]", "uuid", new Object[] {u, null}, new UUID[] {u, null});
            roundTrip(h, t, k++, "date[]", "date", new Object[] {java.sql.Date.valueOf("2024-02-29")},
                    new java.sql.Date[] {java.sql.Date.valueOf("2024-02-29")});
            roundTrip(h, t, k++, "timestamp[]", "timestamp", new Object[] {ts}, new Timestamp[] {ts});
            roundTrip(h, t, k++, "timestamptz[]", "timestamptz", new Object[] {ts}, new Timestamp[] {ts});
            roundTrip(h, t, k++, "bytea[]", "bytea", new Object[] {new byte[] {0, (byte) 0xff, 0x27}},
                    new byte[][] {{0, (byte) 0xff, 0x27}});
            roundTrip(h, t, k++, "jsonb[]", "jsonb", new Object[] {"{\"k\": [1, \"v\"]}"},
                    new String[] {"{\"k\": [1, \"v\"]}"});
            roundTrip(h, t, k++, "text[]", "text", new Object[0], new String[0]);
            roundTrip(h, t, k++, "int4[]", "int4", new Object[] {new Object[] {1, 2, 3}, new Object[] {4, null, 6}},
                    new Integer[][] {{1, 2, 3}, {4, null, 6}});
        }
    }

    @Test
    void setObjectBindsJavaArraysAndCollections() throws Exception {
        try (Connection c = open(true)) {
            exec(c, "CREATE TABLE aso (id INT PRIMARY KEY, t text[], i int4[], l int8[], m int4[])");
            long execs0 = pc(c).serverPreparedExecutions();
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO aso VALUES (?, ?, ?, ?, ?)")) {
                ps.setInt(1, 1);
                ps.setObject(2, new String[] {"a", null});
                ps.setObject(3, new int[] {3, 4});
                ps.setObject(4, List.of(5L, 6L));
                ps.setObject(5, new Integer[][] {{1, 2}, {3, 4}});
                assertEquals(1, ps.executeUpdate());
            }
            assertEquals(1, pc(c).serverPreparedExecutions() - execs0);
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT t, i, l, m FROM aso")) {
                assertTrue(rs.next());
                assertArrayEquals(new String[] {"a", null}, (String[]) rs.getArray(1).getArray());
                assertArrayEquals(new Integer[] {3, 4}, (Integer[]) rs.getArray(2).getArray());
                assertArrayEquals(new Long[] {5L, 6L}, (Long[]) rs.getArray(3).getArray());
                assertArrayEquals(new Integer[][] {{1, 2}, {3, 4}}, (Integer[][]) rs.getArray(4).getArray());
            }
            try (PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM aso WHERE 4 = ANY(?)")) {
                ps.setObject(1, new long[] {4});
                try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); }
            }
        }
    }

    @Test
    void whatCannotBeBoundIsRefusedNotInlined() throws Exception {
        try (Connection c = open(true)) {
            exec(c, "CREATE TABLE arf (id INT PRIMARY KEY, v text[], at timestamp)");
            SQLFeatureNotSupportedException e = assertThrows(SQLFeatureNotSupportedException.class,
                    () -> c.createArrayOf("point", new Object[] {"(1,2)"}));
            assertTrue(e.getMessage().contains("point"), e.getMessage());
            long req0 = pc(c).requestsSent();
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO arf VALUES (?, ?, ?)")) {
                ps.setInt(1, 1);
                ps.setArray(2, c.createArrayOf("text", new Object[] {"x"}));
                ps.setObject(3, java.time.LocalDateTime.of(2024, 1, 1, 0, 0)); // no typed encoding
                SQLFeatureNotSupportedException f = assertThrows(SQLFeatureNotSupportedException.class, ps::executeUpdate);
                assertTrue(f.getMessage().contains("parameter 3"), f.getMessage());
            }
            // A template the driver rewrites (pg_catalog) cannot run by handle.
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = ANY(?)")) {
                ps.setArray(1, c.createArrayOf("text", new Object[] {"arf"}));
                SQLFeatureNotSupportedException f = assertThrows(SQLFeatureNotSupportedException.class, ps::executeQuery);
                assertTrue(f.getMessage().contains("pg_catalog"), f.getMessage());
            }
            assertEquals(req0, pc(c).requestsSent(), "nothing was sent for a refused execution");
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT count(*) FROM arf")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
            // A failing PREPARE raises the server's error, not a text retry.
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO no_such_table VALUES (?)")) {
                ps.setArray(1, c.createArrayOf("text", new Object[] {"x"}));
                SQLException f = assertThrows(SQLException.class, ps::executeUpdate);
                assertFalse(f instanceof SQLFeatureNotSupportedException, f.toString());
            }
        }
    }

    @Test
    void batchWithArraysRunsEveryRowByHandle() throws Exception {
        try (Connection c = open(true)) {
            exec(c, "CREATE TABLE abt (id INT PRIMARY KEY, v text[])");
            long execs0 = pc(c).serverPreparedExecutions(), req0 = pc(c).requestsSent();
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO abt VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setArray(2, null); // a row with no array first: it joins the bound batch
                ps.addBatch();
                for (int i = 2; i <= 4; i++) {
                    ps.setInt(1, i);
                    ps.setArray(2, c.createArrayOf("text", new Object[] {"r" + i, null}));
                    ps.addBatch();
                }
                assertArrayEquals(new int[] {1, 1, 1, 1}, ps.executeBatch());
                // The first failing row stops the batch; earlier rows' counts are reported.
                ps.setInt(1, 5);
                ps.setArray(2, c.createArrayOf("text", new Object[] {"r5"}));
                ps.addBatch();
                ps.setInt(1, 2); // duplicate key
                ps.setArray(2, c.createArrayOf("text", new Object[] {"dup"}));
                ps.addBatch();
                ps.setInt(1, 6);
                ps.setArray(2, c.createArrayOf("text", new Object[] {"never"}));
                ps.addBatch();
                BatchUpdateException b = assertThrows(BatchUpdateException.class, ps::executeBatch);
                assertEquals("23505", b.getSQLState());
                assertArrayEquals(new int[] {1}, b.getUpdateCounts());
            }
            // 5 rows succeeded by handle; the failing row was an EXECUTE too
            // (6 EXECUTEs + 1 PREPARE requests), and the row after it never ran.
            assertEquals(5, pc(c).serverPreparedExecutions() - execs0, "every successful row by handle");
            assertEquals(7, pc(c).requestsSent() - req0, "one PREPARE and six EXECUTEs; nothing as text");
            List<String> got = new ArrayList<>();
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT id, v FROM abt ORDER BY id")) {
                while (rs.next()) {
                    Array a = rs.getArray(2);
                    got.add(rs.getInt(1) + "=" + (a == null ? "null" : Arrays.toString((Object[]) a.getArray())));
                }
            }
            assertEquals(List.of("1=null", "2=[r2, null]", "3=[r3, null]", "4=[r4, null]", "5=[r5]"), got);
        }
    }

    @Test
    void arraysInATransactionCommitAndRollBackWithIt() throws Exception {
        try (Connection c = open(true)) {
            exec(c, "CREATE TABLE atx (id INT PRIMARY KEY, v int8[])");
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO atx VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setArray(2, c.createArrayOf("int8", new Object[] {1L, 2L}));
                ps.executeUpdate();
                c.rollback();
                ps.setInt(1, 2);
                ps.setArray(2, c.createArrayOf("int8", new Object[] {3L}));
                ps.executeUpdate();
                c.commit();
            }
            c.setAutoCommit(true);
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT id, v FROM atx")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertArrayEquals(new Long[] {3L}, (Long[]) rs.getArray(2).getArray());
                assertFalse(rs.next());
            }
        }
    }
}
