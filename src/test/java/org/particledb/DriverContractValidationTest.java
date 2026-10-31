package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * EXECUTABLE contract-validation suite for the UN-validated static findings
 * from {@code /tmp/jdbc_bughunt_A.md} plus a deeper read of the driver source.
 * Every method asserts the JDBC/PG-CORRECT outcome, so a LIVE server PASSES the
 * test iff the driver is correct and FAILS if the bug is real. The main session
 * runs these to classify each finding as REAL or NOT-A-BUG.
 *
 * <p>This class covers the read-path / result-set / metadata / error / null
 * contracts. Batch, temporal-param, and transaction behavior live in
 * {@link DriverBatchAndTxnValidationTest}.
 *
 * <p>Each {@code @Test} carries a one-line comment naming the finding it
 * validates and a PREDICTION (PASS = not-a-bug / FAIL = real bug).
 *
 * <p>Skipped via {@link Assumptions} when the {@code particledb} binary is absent
 * (the sandbox cannot run a DB server; the caller runs this with a real
 * server on the path).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DriverContractValidationTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping driver contract validation");
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private String url() {
        return "jdbc:particledb://127.0.0.1:" + server.port + "/test?preferUnixSocket=false";
    }

    // ── F7: empty result set must still know its projected columns ───────────
    /**
     * Validates F7 (empty ResultSet reports 0 columns / findColumn throws).
     * PREDICTION: FAIL (real bug) - PdbResultSet derives columnNames from the
     * FIRST decoded row only, so a 0-row SELECT reports getColumnCount()==0 and
     * findColumn("id") throws, even though the projection has real columns.
     */
    @Test
    void emptyResultSet_reportsRealColumnCountAndFindColumn() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE empty_rs (id INT PRIMARY KEY, v INT)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT id, v FROM empty_rs WHERE id = -999")) {
                assertFalse(rs.next(), "table should be empty");
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(2, md.getColumnCount(),
                        "F7: empty result set must still report the 2 projected columns");
                int idColIdx = rs.findColumn("id"); // must not throw
                assertTrue(idColIdx >= 1, "F7: findColumn(\"id\") must resolve on empty RS");
            }
        }
    }

    // ── NULL / wasNull: type defaults + wasNull()==true on every getter ──────
    /**
     * Validates finding #4 (NULL handling). PREDICTION: PASS - ArrowDecoder puts
     * every column key on every row map (including nulls), so each getX on a
     * NULL column returns the JDBC type default and wasNull() is true.
     */
    @Test
    void nullColumns_returnTypeDefaultsAndWasNull() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE nulls_t (id INT PRIMARY KEY, "
                    + "i4 INT, i8 BIGINT, f8 DOUBLE PRECISION, txt TEXT, "
                    + "b BOOL, num NUMERIC(10,2))");
            st.executeUpdate("INSERT INTO nulls_t VALUES "
                    + "(1, NULL, NULL, NULL, NULL, NULL, NULL)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT i4, i8, f8, txt, b, num FROM nulls_t WHERE id = 1")) {
                assertTrue(rs.next(), "row 1 missing");

                assertEquals(0, rs.getInt("i4"));
                assertTrue(rs.wasNull(), "getInt(NULL): wasNull() must be true");

                assertEquals(0L, rs.getLong("i8"));
                assertTrue(rs.wasNull(), "getLong(NULL): wasNull() must be true");

                assertEquals(0.0, rs.getDouble("f8"), 0.0);
                assertTrue(rs.wasNull(), "getDouble(NULL): wasNull() must be true");

                assertNull(rs.getString("txt"));
                assertTrue(rs.wasNull(), "getString(NULL): wasNull() must be true");

                assertFalse(rs.getBoolean("b"));
                assertTrue(rs.wasNull(), "getBoolean(NULL): wasNull() must be true");

                assertNull(rs.getBigDecimal("num"));
                assertTrue(rs.wasNull(), "getBigDecimal(NULL): wasNull() must be true");

                assertNull(rs.getObject("i4"));
                assertTrue(rs.wasNull(), "getObject(NULL): wasNull() must be true");

                assertNull(rs.getBytes("txt"));
                assertTrue(rs.wasNull(), "getBytes(NULL): wasNull() must be true");
            }
        }
    }

    /**
     * Validates finding #4 (empty string is NOT NULL). PREDICTION: PASS - an
     * empty TEXT value round-trips as "" with wasNull()==false.
     */
    @Test
    void emptyStringIsNotNull() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE es_t (id INT PRIMARY KEY, txt TEXT)");
            st.executeUpdate("INSERT INTO es_t VALUES (1, '')");
            try (ResultSet rs = st.executeQuery("SELECT txt FROM es_t WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("", rs.getString("txt"), "empty string round-trip failed");
                assertFalse(rs.wasNull(), "empty string must NOT be reported as NULL");
            }
        }
    }

    // ── ERROR / SQLSTATE mapping ─────────────────────────────────────────────
    /**
     * Validates finding #5 (error mapping) for a missing table. PREDICTION:
     * PASS - a SELECT from a non-existent table must raise a SQLException with a
     * non-empty message and a real SQLSTATE. The exact class depends on how much
     * the server's wire code carries ({@link SqlStateMappingTest} pins that);
     * what this asserts is the floor every build must meet - never {@code null}.
     */
    @Test
    void selectFromMissingTable_throwsSQLException() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () -> {
                try (ResultSet rs = st.executeQuery(
                        "SELECT * FROM no_such_table_9d3f")) {
                    rs.next();
                }
            }, "SELECT from a missing table must throw SQLException");
            assertNotNull(ex.getMessage(), "SQLException message must not be null");
            assertFalse(ex.getMessage().trim().isEmpty(),
                    "SQLException message must be non-empty");
            System.out.println("[missing-table] SQLState=" + ex.getSQLState()
                    + " vendorCode=" + ex.getErrorCode() + " msg=" + ex.getMessage());
            assertSqlState(ex);
        }
    }

    /**
     * Validates finding #5 (error mapping) for a syntax error. PREDICTION: PASS
     * - an unparseable statement must throw SQLException, not return silently.
     */
    @Test
    void syntaxError_throwsSQLException() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () -> {
                try (ResultSet rs = st.executeQuery("SELCT nonsense FRM")) {
                    rs.next();
                }
            }, "a syntax error must throw SQLException");
            assertNotNull(ex.getMessage());
            System.out.println("[syntax-error] SQLState=" + ex.getSQLState()
                    + " vendorCode=" + ex.getErrorCode() + " msg=" + ex.getMessage());
            assertSqlState(ex);
        }
    }

    /**
     * Validates finding #5 (error mapping) for a duplicate primary key.
     * PREDICTION: PASS - inserting a duplicate PK must throw SQLException and
     * must NOT overwrite/corrupt the existing row.
     */
    @Test
    void duplicatePrimaryKey_throwsSQLException() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE dup_pk (id INT PRIMARY KEY, v INT)");
            st.executeUpdate("INSERT INTO dup_pk VALUES (1, 10)");
            SQLException ex = assertThrows(SQLException.class, () ->
                            st.executeUpdate("INSERT INTO dup_pk VALUES (1, 20)"),
                    "duplicate PRIMARY KEY insert must throw SQLException");
            assertNotNull(ex.getMessage());
            System.out.println("[dup-pk] SQLState=" + ex.getSQLState()
                    + " vendorCode=" + ex.getErrorCode() + " msg=" + ex.getMessage());
            assertSqlState(ex);
            // Invariant: the original row is untouched.
            assertEquals(10, scalar(st, "SELECT v FROM dup_pk WHERE id = 1"),
                    "duplicate-key insert must not overwrite the existing value");
        }
    }


    /**
     * Every wire error must reach the caller with a real 5-character SQLSTATE.
     * JDBC error handling keys on it - a {@code null} left Spring / Hibernate /
     * Flyway unable to tell a duplicate key from a syntax error.
     */
    private static void assertSqlState(SQLException ex) {
        assertNotNull(ex.getSQLState(), "SQLState must never be null");
        assertEquals(5, ex.getSQLState().length(),
                "SQLSTATE must be 5 characters, got " + ex.getSQLState());
    }

    // ── getResultSet / getUpdateCount / getMoreResults contract ─────────────
    /**
     * Validates finding #7 (result-kind contract). PREDICTION: PASS  -
     * executeQuery ⇒ ResultSet + updateCount==-1; executeUpdate ⇒ null RS +
     * updateCount>=0; execute(SELECT) ⇒ true+RS; execute(DML) ⇒ false+null RS;
     * getMoreResults() ⇒ false (single-result model).
     */
    @Test
    void resultKindContract_queryVsUpdateVsExecute() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE rk (id INT PRIMARY KEY, v INT)");
            st.executeUpdate("INSERT INTO rk VALUES (1,10),(2,20),(3,30)");

            // executeQuery
            ResultSet q = st.executeQuery("SELECT id FROM rk ORDER BY id");
            assertSame(q, st.getResultSet(), "getResultSet() must return the query RS");
            assertEquals(-1, st.getUpdateCount(), "getUpdateCount() must be -1 after a query");
            int n = 0;
            while (q.next()) n++;
            assertEquals(3, n);
            assertFalse(st.getMoreResults(), "getMoreResults() must be false (single result)");

            // executeUpdate
            int u = st.executeUpdate("INSERT INTO rk VALUES (4,40)");
            assertTrue(u == 1 || u == 0, "unexpected update count " + u);
            assertNull(st.getResultSet(), "getResultSet() must be null after executeUpdate");
            assertTrue(st.getUpdateCount() >= 0, "getUpdateCount() must be >= 0 after update");

            // execute(SELECT) ⇒ true
            assertTrue(st.execute("SELECT id FROM rk"),
                    "execute() on SELECT must return true");
            assertNotNull(st.getResultSet(), "getResultSet() must be non-null after execute(SELECT)");
            assertEquals(-1, st.getUpdateCount());

            // execute(DML) ⇒ false
            assertFalse(st.execute("INSERT INTO rk VALUES (5,50)"),
                    "execute() on INSERT must return false");
            assertNull(st.getResultSet(), "getResultSet() must be null after execute(DML)");
            assertTrue(st.getUpdateCount() >= 0);
        }
    }

    // ── Large result integrity (chunked Arrow reassembly) ───────────────────
    /**
     * Validates finding #8 (large-result integrity). PREDICTION: PASS - 5000
     * rows must all come back exactly once with the right values, stressing
     * chunked Arrow IPC reassembly (no truncation, loss, or duplication).
     */
    @Test
    void largeResult_5000Rows_checksumIntact() throws Exception {
        final int rows = 5000;
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE big_rs (id INT PRIMARY KEY, v INT)");
            }
            try (java.sql.PreparedStatement ps =
                         c.prepareStatement("INSERT INTO big_rs VALUES (?, ?)")) {
                for (int i = 0; i < rows; i++) {
                    ps.setInt(1, i);
                    ps.setInt(2, i * 2);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            long expectedSum = (long) rows * (rows - 1) / 2; // sum 0..4999
            boolean[] seen = new boolean[rows];
            long sum = 0;
            int count = 0;
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT id, v FROM big_rs")) {
                while (rs.next()) {
                    int id = rs.getInt("id");
                    assertEquals(id * 2, rs.getInt("v"), "wrong v for id " + id);
                    assertTrue(id >= 0 && id < rows, "id out of range: " + id);
                    assertFalse(seen[id], "duplicate id " + id);
                    seen[id] = true;
                    sum += id;
                    count++;
                }
            }
            assertEquals(rows, count, "not all 5000 rows returned");
            assertEquals(expectedSum, sum, "id checksum mismatch — rows lost/duplicated");
        }
    }

    // ── DatabaseMetaData basics ─────────────────────────────────────────────
    /**
     * getMetaData() is non-null; by default the driver advertises a PG-compatible
     * identity ("PostgreSQL"/version >= 9.0) so Flyway/Hibernate select their
     * PostgreSQL dialect (Conductor-gap fix). getTables/getColumns now ENUMERATE
     * real schema by querying information_schema, so a table created here shows up
     * with its columns.
     */
    @Test
    void databaseMetaData_enumeratesRealSchema() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE meta_probe (id INT PRIMARY KEY, name TEXT, amt NUMERIC(10,2))");

            DatabaseMetaData md = c.getMetaData();
            assertNotNull(md, "getMetaData() must be non-null");
            // Default pgCompat=true → advertise PostgreSQL so Flyway detects it.
            assertEquals("PostgreSQL", md.getDatabaseProductName());
            assertTrue(md.getDatabaseMajorVersion() >= 9,
                    "Flyway requires PG >= 9.0; got major " + md.getDatabaseMajorVersion());

            boolean sawTable = false;
            try (ResultSet rs = md.getTables(null, null, "meta_probe", new String[]{"TABLE"})) {
                while (rs.next()) {
                    if ("meta_probe".equalsIgnoreCase(rs.getString("TABLE_NAME"))) {
                        sawTable = true;
                        assertEquals("TABLE", rs.getString("TABLE_TYPE"));
                    }
                }
            }
            assertTrue(sawTable, "getTables must enumerate the created table");

            int colCount = 0;
            boolean sawAmt = false;
            try (ResultSet rs = md.getColumns(null, null, "meta_probe", "%")) {
                while (rs.next()) {
                    colCount++;
                    if ("amt".equalsIgnoreCase(rs.getString("COLUMN_NAME"))) {
                        sawAmt = true;
                        assertEquals(java.sql.Types.NUMERIC, rs.getInt("DATA_TYPE"));
                    }
                }
            }
            assertEquals(3, colCount, "getColumns must enumerate all 3 columns");
            assertTrue(sawAmt, "getColumns must report the numeric column with DATA_TYPE=NUMERIC");

            // Primary key reflection.
            boolean sawPk = false;
            try (ResultSet rs = md.getPrimaryKeys(null, null, "meta_probe")) {
                while (rs.next()) {
                    if ("id".equalsIgnoreCase(rs.getString("COLUMN_NAME"))) sawPk = true;
                }
            }
            assertTrue(sawPk, "getPrimaryKeys must report the id PK column");
        }
    }

    /**
     * The pgCompat=false escape hatch preserves the native identity for callers
     * that want it (no framework dialect detection).
     */
    @Test
    void databaseMetaData_pgCompatFalseReportsNativeIdentity() throws Exception {
        String nativeUrl = "jdbc:particledb://127.0.0.1:" + server.port
                + "/test?preferUnixSocket=false&pgCompat=false";
        try (Connection c = DriverManager.getConnection(nativeUrl)) {
            assertEquals("ParticleDB", c.getMetaData().getDatabaseProductName());
        }
    }

    // ── Array + bytea round-trip (Conductor PostgresArchiveDAO gap) ──────────
    /**
     * createArrayOf("text", …) + setArray binds a text[] parameter; getArray reads
     * it back as a String[]. This is exactly the shape Conductor's
     * PostgresArchiveDAO uses for index_data word lists.
     */
    @Test
    void textArrayBindAndReadBack() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE arr_probe (id INT PRIMARY KEY, words TEXT[])");
            try (java.sql.PreparedStatement ps =
                         c.prepareStatement("INSERT INTO arr_probe VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setArray(2, c.createArrayOf("text",
                        new String[]{"alpha", "be,ta", "gam\"ma"}));
                assertEquals(1, ps.executeUpdate());
            }
            try (ResultSet rs = st.executeQuery("SELECT words FROM arr_probe WHERE id = 1")) {
                assertTrue(rs.next());
                java.sql.Array a = rs.getArray("words");
                assertNotNull(a, "getArray must return the text[] value");
                Object raw = a.getArray();
                assertTrue(raw instanceof String[], "text[] must read back as String[]");
                assertArrayEquals(new String[]{"alpha", "be,ta", "gam\"ma"}, (String[]) raw);
            }
        }
    }

    /** setBytes/getBytes round-trip a bytea value (Conductor proto payloads). */
    @Test
    void byteaBindAndReadBack() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE bytea_probe (id INT PRIMARY KEY, b BYTEA)");
            byte[] payload = {0x00, (byte) 0xff, 0x10, 0x7f};
            try (java.sql.PreparedStatement ps =
                         c.prepareStatement("INSERT INTO bytea_probe VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setBytes(2, payload);
                assertEquals(1, ps.executeUpdate());
            }
            try (ResultSet rs = st.executeQuery("SELECT b FROM bytea_probe WHERE id = 1")) {
                assertTrue(rs.next());
                assertArrayEquals(payload, rs.getBytes("b"), "bytea round-trip mismatch");
            }
        }
    }

    /** ResultSetMetaData now reports real JDBC column types from the Arrow schema. */
    @Test
    void resultSetMetaData_reportsRealColumnTypes() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE rsmd_probe (id BIGINT PRIMARY KEY, name TEXT, flag BOOL)");
            st.executeUpdate("INSERT INTO rsmd_probe VALUES (1, 'x', true)");
            try (ResultSet rs = st.executeQuery("SELECT id, name, flag FROM rsmd_probe")) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(3, md.getColumnCount());
                assertEquals(java.sql.Types.BIGINT, md.getColumnType(1));
                assertEquals(java.sql.Types.VARCHAR, md.getColumnType(2));
                assertEquals(java.sql.Types.BOOLEAN, md.getColumnType(3));
                assertEquals(String.class.getName(), md.getColumnClassName(2));
            }
        }
    }

    // ── NEW BUG (deeper hunt): duplicate output column labels collapse ───────
    /**
     * NEW finding F18 (found while reading ArrowDecoder). PREDICTION: FAIL (real
     * bug) - ArrowDecoder builds each row as a LinkedHashMap keyed by field
     * NAME, so two output columns with the same label (common in self-joins or
     * `SELECT a, a`) collapse: the second overwrites the first, columnNames has
     * one entry, getColumnCount() reports 1, and getInt(2) is out of range.
     * PostgreSQL/pgjdbc address such columns positionally.
     */
    @Test
    void duplicateColumnLabels_bothAddressable() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT 1 AS dup, 2 AS dup")) {
            assertTrue(rs.next(), "row missing");
            assertEquals(2, rs.getMetaData().getColumnCount(),
                    "F18: duplicate labels collapse — column 2 is lost in the row map");
            assertEquals(1, rs.getInt(1), "F18: first duplicate column value wrong/lost");
            assertEquals(2, rs.getInt(2),
                    "F18: second duplicate column unaddressable (index out of range)");
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────
    private static int scalar(Statement st, String sql) throws Exception {
        try (ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "scalar query returned no rows: " + sql);
            return rs.getInt(1);
        }
    }
}
