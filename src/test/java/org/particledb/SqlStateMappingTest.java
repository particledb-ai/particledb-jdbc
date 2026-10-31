package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * LIVE contract: every native-wire error reaches the caller with a real
 * PostgreSQL SQLSTATE on {@link SQLException#getSQLState()}.
 *
 * <p>JDBC error handling keys on SQLSTATE - Spring's
 * {@code SQLErrorCodeSQLExceptionTranslator}, Hibernate and Flyway all branch on
 * it. A {@code null} SQLSTATE made a duplicate key (23505, routinely handled or
 * retried) indistinguishable from a syntax error (42601, never retried).
 *
 * <p><b>Server-version gates.</b> How much class information exists on the wire
 * depends on the server, so two capabilities are probed at startup and the
 * version-dependent assertions are skipped when absent:
 * <ul>
 *   <li>{@code sqlStateCapable} - the server resolves errors through its
 *       SQLSTATE table (banded and marker-carried classes: 0A000, 25P02,
 *       25006, 42601). Older builds send the generic {@code 1064} for these.</li>
 *   <li>{@code typedEngineErrors} - typed engine errors (constraint violations,
 *       parse/catalog errors) also carry their class on the v2 mux path the
 *       driver uses. Not true of any server built so far: the mux classifies by
 *       message, so these still arrive as {@code 1064}.</li>
 * </ul>
 * The two contracts that hold on EVERY server - a non-null SQLSTATE always, and
 * 40001 on a serialization conflict - are asserted unconditionally. The
 * driver-side mapping table itself is pinned by {@link PdbSqlStateTest}, which
 * needs no server.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SqlStateMappingTest {

    private PdbTestServer server;
    private boolean sqlStateCapable;
    private boolean typedEngineErrors;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping SQLSTATE mapping test");
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE ss_probe (id INT PRIMARY KEY, v INT)");
            st.executeUpdate("INSERT INTO ss_probe VALUES (1, 10)");
            SQLException dup = assertThrows(SQLException.class, () ->
                    st.executeUpdate("INSERT INTO ss_probe VALUES (1, 20)"));
            typedEngineErrors = dup.getErrorCode() != PdbSqlState.ERR_GENERIC_SQL;

            SQLException unsupported = null;
            try {
                st.execute("CREATE DATABASE ss_probe_db");
            } catch (SQLException e) {
                unsupported = e;
            }
            sqlStateCapable = unsupported != null
                    && unsupported.getErrorCode() != PdbSqlState.ERR_GENERIC_SQL;

            System.out.println("[sqlstate-probe] sqlStateCapable=" + sqlStateCapable
                    + " typedEngineErrors=" + typedEngineErrors
                    + " dup=" + dup.getSQLState() + "/" + dup.getErrorCode()
                    + " unsupported=" + (unsupported == null ? "none"
                        : unsupported.getSQLState() + "/" + unsupported.getErrorCode()));
        }
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private String url() {
        return "jdbc:particledb://127.0.0.1:" + server.port + "/test?preferUnixSocket=false";
    }

    private Connection conn() throws SQLException {
        return DriverManager.getConnection(url());
    }

    // ── contracts that hold against EVERY server build ──────────────────────

    /**
     * The floor: whatever the server sends, the caller never sees a null
     * SQLSTATE. On a server that classifies, a duplicate key is 23505; on one
     * that sends the generic 1064 it is HY000 - never {@code null}, which is
     * what left framework error handling with nothing to branch on.
     */
    @Test
    void wireErrorNeverYieldsNullSqlState() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE ss_null (id INT PRIMARY KEY, v INT)");
            st.executeUpdate("INSERT INTO ss_null VALUES (1, 10)");
            SQLException ex = assertThrows(SQLException.class, () ->
                    st.executeUpdate("INSERT INTO ss_null VALUES (1, 20)"));
            System.out.println("[never-null] SQLState=" + ex.getSQLState()
                    + " vendorCode=" + ex.getErrorCode());
            assertNotNull(ex.getSQLState(), "SQLState must never be null");
            assertEquals(5, ex.getSQLState().length(),
                    "SQLSTATE must be 5 characters, got " + ex.getSQLState());
            assertTrue(ex.getErrorCode() != 0, "vendor code must carry the wire code");
        }
    }

    /**
     * 40001 is the one class every client MUST be able to see - retry loops key
     * on it. Two transactions write the same row; the loser has to arrive as
     * serialization_failure, as a {@link SQLTransactionRollbackException}, and
     * not as an opaque wire error.
     *
     * <p>Both transactions run at REPEATABLE READ, requested explicitly. The
     * server's default is READ COMMITTED (PostgreSQL parity), where the second
     * writer correctly WAITS for the first to finish and then re-reads; on one
     * thread that wait never ends (A cannot commit while B's statement is
     * blocked), so the test would sit in its own lock wait until the statement
     * timeout. Under REPEATABLE READ the second writer is refused at once with
     * 40001 (first updater wins).
     */
    @Test
    void serializationFailureCarries40001() throws Exception {
        try (Connection setup = conn(); Statement st = setup.createStatement()) {
            st.executeUpdate("CREATE TABLE ss_ser (id INT PRIMARY KEY, v INT)");
            st.executeUpdate("INSERT INTO ss_ser VALUES (1, 0)");
        }

        try (Connection a = conn(); Connection b = conn()) {
            a.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            b.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            a.setAutoCommit(false);
            b.setAutoCommit(false);
            try (Statement sa = a.createStatement(); Statement sb = b.createStatement()) {
                SQLException conflict = null;
                try {
                    // The conflict may surface at the losing write or at its
                    // COMMIT depending on the engine's conflict-detection point;
                    // either way it must carry 40001.
                    sa.executeUpdate("UPDATE ss_ser SET v = v + 1 WHERE id = 1");
                    sb.executeUpdate("UPDATE ss_ser SET v = v + 2 WHERE id = 1");
                    a.commit();
                    b.commit();
                } catch (SQLException e) {
                    conflict = e;
                }
                assertNotNull(conflict,
                        "two concurrent writers of the same row must conflict");
                System.out.println("[serialization] SQLState=" + conflict.getSQLState()
                        + " vendorCode=" + conflict.getErrorCode()
                        + " class=" + conflict.getClass().getSimpleName());
                assertEquals("40001", conflict.getSQLState(),
                        "serialization failure must surface as 40001 — retry loops key on it");
                assertTrue(conflict instanceof SQLTransactionRollbackException,
                        "40001 must be a SQLTransactionRollbackException, got "
                                + conflict.getClass().getName());
            }
        }
    }

    // ── contracts that need a SQLSTATE-carrying server ──────────────────────

    /**
     * The 6xxxx band round-trips. A letter-bearing SQLSTATE has no u16 form, so
     * the server maps it into a reserved numeric band ({@code 0A000} → 60007,
     * {@code 25P02} → 60003); the driver must hand back the letter form, not the
     * band number. Also covers the marker-carried all-digit classes on the same
     * paths (25006, 42601).
     */
    @Test
    void bandedAndMarkerClassesDecodeToTheirRealSqlState() throws Exception {
        Assumptions.assumeTrue(sqlStateCapable,
                "server predates native-wire SQLSTATEs (every error is 1064)");

        List<String> failures = new ArrayList<>();
        try (Connection c = conn(); Statement st = c.createStatement()) {
            // 0A000 feature_not_supported - banded as 60007. CREATE DATABASE
            // is a documented refusal (one database per server).
            check(failures, "unsupported-statement", "0A000", st, "CREATE DATABASE ss_db");
            // 25P01 no_active_sql_transaction - banded. SAVEPOINT is
            // implemented; outside a transaction block it is refused as in
            // PostgreSQL.
            check(failures, "savepoint-outside-block", "25P01", st, "SAVEPOINT ss_sp");
            // 42601 syntax_error - all-digit, verbatim.
            check(failures, "invalid-txn-mode", "42601", st,
                    "BEGIN ISOLATION LEVEL BANANA");
        }

        // 25P02 in_failed_sql_transaction - banded as 60003.
        try (Connection c = conn()) {
            c.setAutoCommit(false);
            try (Statement st = c.createStatement()) {
                try { st.execute("SELCT bad"); } catch (SQLException ignored) {}
                check(failures, "aborted-block", "25P02", st, "SELECT 1");
            }
        }

        // 25006 read_only_sql_transaction - all-digit, verbatim.
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE ss_ro (id INT PRIMARY KEY, v INT)");
            st.execute("BEGIN READ ONLY");
            check(failures, "read-only-write", "25006", st,
                    "INSERT INTO ss_ro VALUES (1, 1)");
            try { st.execute("ROLLBACK"); } catch (SQLException ignored) {}
        }

        if (!failures.isEmpty()) {
            fail("SQLSTATE not propagated:\n  " + String.join("\n  ", failures));
        }
    }

    /**
     * Every typed engine error class. Gated: the v2 mux path the driver uses
     * classifies by message, so these still arrive as the generic 1064 and the
     * test skips. It flips to asserting the moment the server resolves them
     * through its SQLSTATE table ({@code wire_error_code_typed}).
     */
    @Test
    void everyEngineErrorClassCarriesItsPostgresSqlState() throws Exception {
        Assumptions.assumeTrue(typedEngineErrors,
                "server sends the generic 1064 for typed engine errors "
                        + "(native-wire mux classifies by message)");

        List<String> failures = new ArrayList<>();
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE ss_dup (id INT PRIMARY KEY, v INT)");
            st.executeUpdate("INSERT INTO ss_dup VALUES (1, 10)");
            check(failures, "duplicate-key", "23505", st,
                    "INSERT INTO ss_dup VALUES (1, 20)");

            st.executeUpdate("CREATE TABLE ss_nn (id INT PRIMARY KEY, v INT NOT NULL)");
            check(failures, "not-null", "23502", st,
                    "INSERT INTO ss_nn VALUES (1, NULL)");

            st.executeUpdate("CREATE TABLE ss_parent (id INT PRIMARY KEY)");
            st.executeUpdate("CREATE TABLE ss_child (id INT PRIMARY KEY, "
                    + "pid INT REFERENCES ss_parent(id))");
            check(failures, "foreign-key", "23503", st,
                    "INSERT INTO ss_child VALUES (1, 999)");

            st.executeUpdate("CREATE TABLE ss_chk (id INT PRIMARY KEY, "
                    + "v INT CHECK (v > 0))");
            check(failures, "check-constraint", "23514", st,
                    "INSERT INTO ss_chk VALUES (1, -5)");

            check(failures, "divide-by-zero", "22012", st, "SELECT 1 / 0");
            check(failures, "syntax-error", "42601", st, "SELCT nonsense FRM");
            check(failures, "missing-table", "42P01", st,
                    "SELECT * FROM ss_no_such_table_4f2a");
            check(failures, "missing-column", "42703", st,
                    "SELECT no_such_column_4f2a FROM ss_dup");
        }

        if (!failures.isEmpty()) {
            fail("SQLSTATE not propagated:\n  " + String.join("\n  ", failures));
        }
    }

    private static void check(List<String> failures, String label, String expected,
                              Statement st, String sql) {
        SQLException ex = null;
        try {
            st.execute(sql);
        } catch (SQLException e) {
            ex = e;
        }
        if (ex == null) {
            failures.add(label + ": expected SQLSTATE " + expected
                    + " but the statement succeeded: " + sql);
            return;
        }
        System.out.println("[" + label + "] SQLState=" + ex.getSQLState()
                + " vendorCode=" + ex.getErrorCode() + " msg=" + ex.getMessage());
        if (!expected.equals(ex.getSQLState())) {
            failures.add(label + ": expected SQLSTATE " + expected + ", got "
                    + ex.getSQLState() + " (vendorCode=" + ex.getErrorCode()
                    + ", msg=" + ex.getMessage() + ")");
        }
    }
}
