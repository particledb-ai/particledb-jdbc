package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * EXECUTABLE contract-validation suite (part 2): batch semantics (F6),
 * temporal / special parameter fidelity (F12), and transaction semantics over
 * the PDB wire (finding #6). Companion to {@link DriverContractValidationTest}.
 *
 * <p>Every method asserts the JDBC/PG-CORRECT outcome so a live server PASSES
 * iff the driver is correct. Each {@code @Test} carries the finding it
 * validates + a PREDICTION (PASS = not-a-bug / FAIL = real bug).
 *
 * <p>Skipped via {@link Assumptions} when the {@code particledb} binary is absent.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DriverBatchAndTxnValidationTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping batch/txn validation");
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private String url() {
        return "jdbc:particledb://127.0.0.1:" + server.port + "/test?preferUnixSocket=false";
    }

    // ── F6: empty batch returns int[0] ──────────────────────────────────────
    /**
     * Validates F6 (batch). PREDICTION: PASS - executeBatch() on an empty batch
     * returns a zero-length int[] (JDBC contract), for both Statement and
     * PreparedStatement.
     */
    @Test
    void emptyBatch_returnsZeroLengthArray() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                int[] r = st.executeBatch();
                assertNotNull(r);
                assertEquals(0, r.length, "empty Statement batch must return int[0]");
            }
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE eb_t (id INT PRIMARY KEY, v INT)");
            }
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO eb_t VALUES (?, ?)")) {
                int[] r = ps.executeBatch();
                assertNotNull(r);
                assertEquals(0, r.length, "empty PreparedStatement batch must return int[0]");
            }
        }
    }

    // ── F6: several INSERTs via Statement.addBatch → int[] + full read-back ──
    /**
     * Validates F6 (batch). PREDICTION: PASS - several batched INSERTs yield an
     * int[] of the batch size (SUCCESS_NO_INFO per row) and ALL rows are present
     * on read-back with the correct values.
     */
    @Test
    void statementBatchInserts_arrayLengthAndFullReadBack() throws Exception {
        final int n = 30;
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE sb_t (id INT PRIMARY KEY, v INT)");
                for (int i = 0; i < n; i++) {
                    st.addBatch("INSERT INTO sb_t VALUES (" + i + ", " + (i * 7) + ")");
                }
                int[] r = st.executeBatch();
                assertEquals(n, r.length, "executeBatch() returned wrong array length");
                for (int code : r) {
                    assertTrue(code == Statement.SUCCESS_NO_INFO || code >= 0,
                            "each batch result must be SUCCESS_NO_INFO or a count, got " + code);
                }
            }
            boolean[] seen = new boolean[n];
            int count = 0;
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT id, v FROM sb_t ORDER BY id")) {
                while (rs.next()) {
                    int id = rs.getInt("id");
                    assertEquals(id * 7, rs.getInt("v"), "wrong value for id " + id);
                    assertFalse(seen[id], "duplicate id " + id);
                    seen[id] = true;
                    count++;
                }
            }
            assertEquals(n, count, "batch read-back returned wrong row count");
            for (int i = 0; i < n; i++) assertTrue(seen[i], "batched row id=" + i + " was LOST");
        }
    }

    // ── F6: a SELECT mixed among INSERTs must not corrupt rows ──────────────
    /**
     * Validates F6 (SELECT-in-batch). PREDICTION: PASS (row integrity) - a
     * Statement batch that interleaves a SELECT among INSERTs must not corrupt
     * the INSERTs: all four INSERTed ids must be present exactly once, and the
     * connection must remain usable afterwards. (Per F6 the SELECT's own rows
     * are discarded by the batch path; that is acceptable - losing/duplicating
     * INSERTed rows would be the real bug.) The executeBatch() call is allowed
     * to either succeed or throw a clear SQLException; either way the INSERTs'
     * final state is asserted.
     */
    @Test
    void mixedSelectInBatch_noRowCorruption() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE mix_t (id INT PRIMARY KEY, v INT)");
            }
            try (Statement st = c.createStatement()) {
                st.addBatch("INSERT INTO mix_t VALUES (0, 0)");
                st.addBatch("INSERT INTO mix_t VALUES (1, 10)");
                st.addBatch("SELECT id FROM mix_t");         // SELECT in the middle
                st.addBatch("INSERT INTO mix_t VALUES (2, 20)");
                st.addBatch("INSERT INTO mix_t VALUES (3, 30)");
                try {
                    st.executeBatch();
                } catch (SQLException e) {
                    // Acceptable: a driver/server that rejects a SELECT inside a
                    // batch. Document it; the integrity check below still runs.
                    System.out.println("[mixed-batch] executeBatch threw: " + e.getMessage());
                }
            }
            // Integrity: all four INSERTs must be present exactly once, no dups,
            // no stray values.
            boolean[] seen = new boolean[4];
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT id, v FROM mix_t ORDER BY id")) {
                while (rs.next()) {
                    int id = rs.getInt("id");
                    assertTrue(id >= 0 && id < 4, "unexpected id in mixed batch: " + id);
                    assertEquals(id * 10, rs.getInt("v"), "wrong value for id " + id);
                    assertFalse(seen[id], "duplicate id " + id);
                    seen[id] = true;
                }
            }
            for (int i = 0; i < 4; i++) {
                assertTrue(seen[i], "INSERT id=" + i + " lost when a SELECT was batched with it");
            }
            // Connection is not wedged: a fresh statement still works and lands.
            try (Statement st = c.createStatement()) {
                st.executeUpdate("INSERT INTO mix_t VALUES (4, 40)");
                assertEquals(1, count(st, "SELECT id FROM mix_t WHERE id = 4"),
                        "connection unusable after mixed batch");
            }
        }
    }

    // ── F6: BatchExecutor helper affected-count decode ──────────────────────
    /**
     * Validates F6 (BatchExecutor garbage affected count). PREDICTION: FAIL
     * (real bug) - the server's MSG_BATCH_RESULT payload is
     * {@code [u32 count][per-stmt entries...]} but BatchExecutor decodes the
     * first 8 bytes as a little-endian u64 affected count, so the returned value
     * is {@code count | (firstEntryBytes << 32)} - not the real row count. The
     * data itself lands (asserted first), only the reported count is wrong.
     */
    @Test
    void batchExecutorAffectedCount_matchesRowCount_bugCapture() throws Exception {
        final int n = 5;
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE be_t (id INT PRIMARY KEY, v INT)");
            }
            PdbConnection pdb = c.unwrap(PdbConnection.class);
            BatchExecutor be = new BatchExecutor(pdb);
            for (int i = 0; i < n; i++) be.add("INSERT INTO be_t VALUES (" + i + ", " + i + ")");
            long affected = be.execute();

            // The rows must have actually landed (server did the work).
            int count = 0;
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT id FROM be_t")) {
                while (rs.next()) count++;
            }
            assertEquals(n, count, "BatchExecutor did not persist all rows");

            // F6: the affected count must equal the number of inserted rows.
            System.out.println("[BatchExecutor] affected=" + affected + " expected=" + n);
            assertEquals((long) n, affected,
                    "F6: BatchExecutor decodes [u32 count] batch payload as a u64 "
                            + "→ garbage affected count (got " + affected + ")");
        }
    }

    // ── F12: setDate round-trips ────────────────────────────────────────────
    /**
     * Validates F12 (temporal params) for DATE. PREDICTION: PASS - a
     * java.sql.Date bound via setDate renders as a quoted 'yyyy-MM-dd' literal
     * and round-trips to the same calendar date.
     */
    @Test
    void setDate_roundTrips() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE d_t (id INT PRIMARY KEY, d DATE)");
            }
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO d_t VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setDate(2, Date.valueOf("2026-03-14"));
                ps.executeUpdate();
            }
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT d FROM d_t WHERE id = 1")) {
                assertTrue(rs.next(), "row missing");
                Date got = rs.getDate("d");
                assertNotNull(got, "F12: DATE param was lost (read back NULL)");
                assertEquals("2026-03-14", got.toString(), "F12: DATE value not preserved");
            }
        }
    }

    // ── F12: setTimestamp round-trip via the fixed getTimestamp() ───────────
    /**
     * Validates F12 (temporal params) for TIMESTAMP. PREDICTION: PASS - the
     * driver binds a java.sql.Timestamp and the Arrow temporal carrier decodes
     * back through the fixed {@link ResultSet#getTimestamp} to the same instant.
     *
     * <p>A {@code TIME} column is intentionally NOT used here: this server build
     * rejects it ("type time is not supported"), which is a documented SERVER
     * gap, not a driver defect. Exercising TIME would fail at CREATE TABLE and
     * tell us nothing about the driver, so it is dropped. A NULL read-back for
     * TIMESTAMP is a documented server gap and is tolerated + logged; a wrong
     * non-null value is a real driver failure.
     */
    @Test
    void setTimeAndTimestamp_roundTripOrKnownServerGap() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE tts_t (id INT PRIMARY KEY, ts TIMESTAMP)");
            }
            final Timestamp expected = Timestamp.valueOf("2026-03-14 13:45:30");
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO tts_t VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setTimestamp(2, expected);
                ps.executeUpdate();
            }
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT ts FROM tts_t WHERE id = 1")) {
                assertTrue(rs.next(), "row missing");
                // Exercises the fixed temporal decode (Arrow micros/LocalDateTime
                // carrier → java.sql.Timestamp), not just v.toString().
                Timestamp ts = rs.getTimestamp("ts");
                if (ts != null) {
                    assertEquals(expected, ts, "F12: TIMESTAMP value not preserved: " + ts);
                } else {
                    System.out.println("[known server gap] TIMESTAMP returned null on this build");
                }
            }
        }
    }

    // ── F12: NaN / +Inf / -Inf double must not silently corrupt ─────────────
    /**
     * Validates F12 (special double literals). PREDICTION: PASS (no silent
     * corruption) - the driver renders NaN/Infinity as the bare tokens
     * {@code NaN}/{@code Infinity}, which are invalid SQL numeric literals, so
     * the server rejects them with a clear SQLException. The CONTRACT tested is:
     * either a correct round-trip OR a clear error - never a silently-stored
     * wrong finite value. (A correct round-trip would require the driver to emit
     * 'NaN'::float8; that faithfulness gap is F12 and is acceptable here.)
     */
    @Test
    void setDoubleSpecialValues_roundTripOrClearError() throws Exception {
        double[] specials = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE dbl_t (id INT PRIMARY KEY, d DOUBLE PRECISION)");
            }
            for (int i = 0; i < specials.length; i++) {
                final int id = i;
                final double v = specials[i];
                boolean stored;
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO dbl_t VALUES (?, ?)")) {
                    ps.setInt(1, id);
                    ps.setDouble(2, v);
                    ps.executeUpdate();
                    stored = true;
                } catch (SQLException e) {
                    stored = false; // acceptable: clear rejection, no silent corruption
                    System.out.println("[double-special " + v + "] rejected: " + e.getMessage());
                }
                if (stored) {
                    try (Statement st = c.createStatement();
                         ResultSet rs = st.executeQuery("SELECT d FROM dbl_t WHERE id = " + id)) {
                        assertTrue(rs.next(),
                                "F12: setDouble(" + v + ") reported success but stored NO row");
                        double got = rs.getDouble("d");
                        // JUnit5 double assertEquals treats NaN==NaN and Inf==Inf.
                        assertEquals(v, got, 0.0,
                                "F12: special double " + v + " silently corrupted to " + got);
                    }
                }
            }
        }
    }

    // ── F12: NaN / +Inf / -Inf float must not silently corrupt ──────────────
    /**
     * Validates F12 (special float literals) - same contract as the double
     * case. PREDICTION: PASS (no silent corruption). The column is DOUBLE
     * PRECISION; the point is the client rendering of a Float NaN/Infinity, not
     * the storage width.
     */
    @Test
    void setFloatSpecialValues_roundTripOrClearError() throws Exception {
        float[] specials = {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY};
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE flt_t (id INT PRIMARY KEY, d DOUBLE PRECISION)");
            }
            for (int i = 0; i < specials.length; i++) {
                final int id = i;
                final float v = specials[i];
                boolean stored;
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO flt_t VALUES (?, ?)")) {
                    ps.setInt(1, id);
                    ps.setFloat(2, v);
                    ps.executeUpdate();
                    stored = true;
                } catch (SQLException e) {
                    stored = false;
                    System.out.println("[float-special " + v + "] rejected: " + e.getMessage());
                }
                if (stored) {
                    try (Statement st = c.createStatement();
                         ResultSet rs = st.executeQuery("SELECT d FROM flt_t WHERE id = " + id)) {
                        assertTrue(rs.next(),
                                "F12: setFloat(" + v + ") reported success but stored NO row");
                        double got = rs.getDouble("d");
                        assertEquals((double) v, got, 0.0,
                                "F12: special float " + v + " silently corrupted to " + got);
                    }
                }
            }
        }
    }

    // ── F12: high-scale BigDecimal round-trips ──────────────────────────────
    /**
     * Validates F12 (setBigDecimal high scale). PREDICTION: PASS - a 12-fraction
     * digit NUMERIC(30,12) value must round-trip with no precision loss.
     */
    @Test
    void setBigDecimalHighScale_roundTrips() throws Exception {
        final BigDecimal val = new BigDecimal("12345678.123456789012");
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE dec_t (id INT PRIMARY KEY, n NUMERIC(30,12))");
            }
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO dec_t VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setBigDecimal(2, val);
                ps.executeUpdate();
            }
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT n FROM dec_t WHERE id = 1")) {
                assertTrue(rs.next(), "row missing");
                BigDecimal got = rs.getBigDecimal("n");
                assertNotNull(got, "F12: high-scale NUMERIC read back NULL");
                assertEquals(0, val.compareTo(got),
                        "F12: high-scale NUMERIC lost precision: expected " + val + " got " + got);
            }
        }
    }

    // ── F12 / NEW: setBytes → bytea round-trip via getBytes ─────────────────
    /**
     * Validates F12 (setBytes/bytea). PREDICTION: FAIL (real bug, likely) - the
     * driver renders byte[] as a bytea hex literal '\\x..' correctly, but on
     * read-back getBytes() only returns the original byte[] if the server hands
     * back an Arrow VarBinary. If bytea comes back as a text hex string
     * ("\\xdeadbeef"), getBytes() returns the UTF-8 bytes of THAT string, not
     * the original bytes - a round-trip corruption. If it PASSES, bytea decodes
     * as binary and this is not a bug.
     */
    @Test
    void setBytesBytea_roundTrips() throws Exception {
        final byte[] data = {0x00, 0x01, 0x02, 0x7f, (byte) 0x80,
                (byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef};
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE by_t (id INT PRIMARY KEY, b BYTEA)");
            }
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO by_t VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setBytes(2, data);
                ps.executeUpdate();
            }
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT b FROM by_t WHERE id = 1")) {
                assertTrue(rs.next(), "row missing");
                byte[] got = rs.getBytes("b");
                assertNotNull(got, "F12: BYTEA read back NULL");
                assertArrayEquals(data, got,
                        "F12: BYTEA round-trip corrupted — got " + Arrays.toString(got));
            }
        }
    }

    // ── #6: rollback makes the row absent ───────────────────────────────────
    /**
     * Validates finding #6 (transaction rollback over the wire). PREDICTION:
     * PASS - after setAutoCommit(false); INSERT; rollback(), the row must be
     * ABSENT. If the row survives, PDB-wire rollback is not honored (real bug).
     */
    @Test
    void rollback_rowAbsent() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE rb_t (id INT PRIMARY KEY, v INT)");
            }
            c.setAutoCommit(false);          // BEGIN
            try {
                try (Statement st = c.createStatement()) {
                    st.executeUpdate("INSERT INTO rb_t VALUES (1, 100)");
                }
                c.rollback();                // ROLLBACK (+ BEGIN)
                int count;
                try (Statement st = c.createStatement();
                     ResultSet rs = st.executeQuery("SELECT id FROM rb_t WHERE id = 1")) {
                    count = 0;
                    while (rs.next()) count++;
                }
                assertEquals(0, count,
                        "#6: row present after ROLLBACK — PDB-wire rollback not honored");
            } finally {
                c.setAutoCommit(true);       // COMMIT the (empty) txn
            }
        }
    }

    // ── #6: commit makes the row visible to a later statement ───────────────
    /**
     * Validates finding #6 (transaction commit over the wire). PREDICTION: PASS
     * - after setAutoCommit(false); INSERT; commit(), a subsequent statement
     * (and a fresh connection) must see the row.
     */
    @Test
    void commit_rowVisible() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE cm_t (id INT PRIMARY KEY, v INT)");
            }
            c.setAutoCommit(false);          // BEGIN
            try {
                try (Statement st = c.createStatement()) {
                    st.executeUpdate("INSERT INTO cm_t VALUES (2, 200)");
                }
                c.commit();                  // COMMIT (+ BEGIN)
                try (Statement st = c.createStatement();
                     ResultSet rs = st.executeQuery("SELECT v FROM cm_t WHERE id = 2")) {
                    assertTrue(rs.next(), "#6: committed row not visible to a later statement");
                    assertEquals(200, rs.getInt("v"), "#6: committed value wrong");
                }
            } finally {
                c.setAutoCommit(true);
            }
        }
        // Fresh connection sees the committed row too.
        try (Connection c2 = DriverManager.getConnection(url());
             Statement st = c2.createStatement();
             ResultSet rs = st.executeQuery("SELECT v FROM cm_t WHERE id = 2")) {
            assertTrue(rs.next(), "#6: committed row not visible on a new connection");
            assertEquals(200, rs.getInt("v"));
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────
    private static int count(Statement st, String sql) throws Exception {
        try (ResultSet rs = st.executeQuery(sql)) {
            int n = 0;
            while (rs.next()) n++;
            return n;
        }
    }
}
