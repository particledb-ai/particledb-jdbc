package org.particledb;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The edges of {@code allowDdlOutsideTransaction=true}: what it does NOT cover
 * (batches, multi-statement strings), where its warning lives (the Connection,
 * not the Statement), and that it survives a pool -- a connection returned to
 * a real HikariCP pool mid-transaction and handed to the next borrower starts
 * clean. {@link ConductorFlywayMigrationTest} covers the Flyway path and the
 * single-statement contract. Every outcome is judged from a SECOND,
 * autocommit connection.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AllowDdlOutsideTransactionScopeTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        // Pinned to `--transactional-ddl off`: this suite asserts the refusal of
        // DDL inside a transaction (25001), the server default in earlier releases.
        server = PdbTestServer.tryStart("--transactional-ddl", "off");
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping allowDdlOutsideTransaction scope test");
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private String url(String extra) {
        return "jdbc:particledb://127.0.0.1:" + server.port + "/test?preferUnixSocket=false" + extra;
    }

    private Connection withOption() throws SQLException {
        Connection c = DriverManager.getConnection(url("&allowDdlOutsideTransaction=true"));
        c.setAutoCommit(false);
        return c;
    }

    private Connection observer() throws SQLException {
        return DriverManager.getConnection(url(""));
    }

    /** A pool configured as Conductor's (hikari.auto-commit=false, READ COMMITTED), size 1. */
    private HikariDataSource pool(String extra) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(url(extra));
        cfg.setAutoCommit(false);
        cfg.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
        cfg.setMaximumPoolSize(1);
        cfg.setMinimumIdle(1);
        cfg.setPoolName("ddl-scope-" + extra.hashCode());
        return new HikariDataSource(cfg);
    }

    private static long count(Connection c, String table) throws SQLException {
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM " + table)) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    private static boolean tableExists(Connection c, String table) {
        try {
            count(c, table);
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static int warningCount(SQLWarning w) {
        int n = 0;
        for (; w != null; w = w.getNextWarning()) n++;
        return n;
    }

    private static PdbConnection physical(Connection c) throws SQLException {
        return c.unwrap(PdbConnection.class);
    }

    // ── what the option does not cover ───────────────────────────────────

    /**
     * A batch's failure is the server's 25001 refusal, and the
     * {@link BatchUpdateException} carries THAT SQLSTATE: the driver negotiates
     * per-entry SQLSTATEs ({@code CAP_EXT_BATCH_SQLSTATE}) by default. (A failed
     * batch entry used to carry no code and read as {@code HY000}.) The
     * table-absence checks are what prove nothing ran.
     */
    private static void assertServerRefusal25001(BatchUpdateException e) {
        assertEquals("25001", e.getSQLState(), e.getMessage());
        assertTrue(String.valueOf(e.getMessage()).contains("CREATE cannot run inside a transaction block"),
                e.getSQLState() + " " + e.getMessage());
    }

    /**
     * {@code executeBatch} always opens the transaction, option or not, so a
     * DDL in a batch -- even as the first statement -- is refused 25001 and
     * changes nothing. {@code executeLargeBatch} is not implemented and sends
     * nothing.
     */
    @Test
    void aBatchStillOpensTheTransactionAndItsDdlIsRefused() throws Exception {
        try (Connection o = observer()) {
            try (Connection c = withOption(); Statement st = c.createStatement()) {
                st.addBatch("CREATE TABLE sc_batch (id INT)");
                BatchUpdateException e = assertThrows(BatchUpdateException.class, st::executeBatch);
                assertServerRefusal25001(e);
                c.rollback();
            }
            assertFalse(tableExists(o, "sc_batch"), "a batched DDL ran outside the transaction");

            // A DDL first, then a DML, in one batch: refused as a whole.
            try (Connection c = withOption(); Statement st = c.createStatement()) {
                st.addBatch("CREATE TABLE sc_batch2 (id INT)");
                st.addBatch("INSERT INTO sc_batch2 VALUES (1)");
                BatchUpdateException e = assertThrows(BatchUpdateException.class, st::executeBatch);
                assertServerRefusal25001(e);
                c.rollback();
            }
            assertFalse(tableExists(o, "sc_batch2"), "a batched DDL;DML ran");

            // executeLargeBatch is not implemented by this driver (java.sql's
            // default: UnsupportedOperationException), so it sends nothing --
            // it cannot run a DDL outside the transaction either.
            try (Connection c = withOption(); Statement st = c.createStatement()) {
                st.addBatch("CREATE TABLE sc_lbatch (id INT)");
                assertThrows(UnsupportedOperationException.class, st::executeLargeBatch);
                c.rollback();
            }
            assertFalse(tableExists(o, "sc_lbatch"), "a large-batched DDL ran");
        }
    }

    /**
     * A multi-statement string led by a DDL is NOT run outside the transaction:
     * its DML would leave the caller's transaction with it, where rollback()
     * could not undo it. It opens the transaction and the DDL is refused 25001;
     * nothing of the string survives. A single statement with a trailing
     * {@code ;}, or a {@code ;} inside a literal, is still one statement.
     *
     * <p>Today's server refuses multi-statement text on the native wire anyway
     * (42601, "only single statements are supported"), so without the driver's
     * guard nothing escapes either -- the guard is what keeps that true if the
     * server ever accepts such text. Sabotage check (2026-10-08): with the
     * guard removed this test fails on the SQLSTATE (42601, not 25001), so it
     * does observe the guard.
     */
    @Test
    void aMixedDdlDmlStringIsRefusedAndNoneOfItRuns() throws Exception {
        try (Connection o = observer()) {
            try (Connection c = withOption()) {
                SQLException e = assertThrows(SQLException.class, () -> exec(c,
                        "CREATE TABLE sc_multi (id INT); INSERT INTO sc_multi VALUES (1)"));
                assertEquals("25001", e.getSQLState(), e.getMessage());
                assertNull(c.getWarnings(), "a refused string must not warn that it ran");
                c.rollback();
            }
            assertFalse(tableExists(o, "sc_multi"),
                    "a DDL;DML string ran outside the transaction");

            exec(o, "CREATE TABLE sc_base (id INT PRIMARY KEY)");
            try (Connection c = withOption()) {
                SQLException e = assertThrows(SQLException.class, () -> exec(c,
                        "ALTER TABLE sc_base ADD COLUMN v INT;\n-- next\nINSERT INTO sc_base VALUES (7, 7)"));
                assertEquals("25001", e.getSQLState(), e.getMessage());
                c.rollback();
            }
            assertEquals(0, count(o, "sc_base"), "the string's DML escaped the transaction");

            try (Connection c = withOption()) {
                exec(c, "CREATE TABLE sc_trailing (id INT);  -- one statement\n");
                assertEquals(1, warningCount(c.getWarnings()));
                exec(c, "COMMENT ON TABLE sc_trailing IS 'a;b; INSERT INTO sc_base VALUES (8)'");
                assertEquals(2, warningCount(c.getWarnings()));
                c.rollback();
            }
            assertTrue(tableExists(o, "sc_trailing"));
            assertEquals(0, count(o, "sc_base"));
        }
    }

    /** The 01000 warning is reported on the Connection, not on the Statement. */
    @Test
    void theWarningIsOnTheConnectionNotTheStatement() throws Exception {
        try (Connection c = withOption(); Statement st = c.createStatement()) {
            c.clearWarnings();
            st.execute("CREATE TABLE sc_warn (id INT)");
            assertNull(st.getWarnings(), "documented: the warning is not on the Statement");
            SQLWarning w = c.getWarnings();
            assertNotNull(w);
            assertEquals("01000", w.getSQLState());
            c.clearWarnings();
            assertNull(c.getWarnings());
        }
    }

    // ── pool reuse ───────────────────────────────────────────────────────

    /**
     * Borrower 1 runs a DDL outside the transaction, opens a transaction with a
     * DML, and returns the connection WITHOUT ending it. Hikari rolls it back
     * and clears warnings. Borrower 2 gets the SAME physical connection with no
     * transaction open and no stale warning; its first DDL again runs outside
     * (exactly one warning, its own) and its DML commits.
     */
    @Test
    void aReturnedConnectionMidTransactionIsCleanForTheNextBorrower() throws Exception {
        try (HikariDataSource ds = pool("&allowDdlOutsideTransaction=true");
             Connection o = observer()) {
            PdbConnection first;
            try (Connection c = ds.getConnection()) {
                first = physical(c);
                assertFalse(c.getAutoCommit());
                exec(c, "CREATE TABLE pr_a (id INT PRIMARY KEY)");
                assertNotNull(c.getWarnings());
                exec(c, "INSERT INTO pr_a VALUES (1)");
                assertTrue(first.inServerTransaction());
                // returned without commit/rollback
            }
            assertTrue(tableExists(o, "pr_a"));
            assertEquals(0, count(o, "pr_a"), "the pool must roll back an abandoned transaction");

            try (Connection c = ds.getConnection()) {
                assertSame(first, physical(c), "pool size 1 must hand back the same connection");
                assertFalse(c.getAutoCommit());
                assertFalse(physical(c).inServerTransaction(),
                        "the next borrower must start with no transaction open");
                assertNull(c.getWarnings(), "a previous borrower's warning leaked");
                exec(c, "CREATE TABLE pr_b (id INT PRIMARY KEY)");
                assertEquals(1, warningCount(c.getWarnings()));
                assertFalse(physical(c).inServerTransaction());
                exec(c, "INSERT INTO pr_b VALUES (2)");
                c.commit();
            }
            assertEquals(1, count(o, "pr_b"));
        }
    }

    /**
     * Borrower 1 leaves a FAILED transaction (a DDL refused 25001 after a DML)
     * and returns the connection. Borrower 2 starts clean: the failed block is
     * gone, the option still applies to its first DDL, and its work commits.
     */
    @Test
    void aReturnedConnectionWithAFailedTransactionIsCleanForTheNextBorrower() throws Exception {
        try (HikariDataSource ds = pool("&allowDdlOutsideTransaction=true");
             Connection o = observer()) {
            exec(o, "CREATE TABLE pf_base (id INT PRIMARY KEY)");
            try (Connection c = ds.getConnection()) {
                exec(c, "INSERT INTO pf_base VALUES (1)");
                SQLException e = assertThrows(SQLException.class,
                        () -> exec(c, "CREATE TABLE pf_refused (id INT)"));
                assertEquals("25001", e.getSQLState());
            }
            assertEquals(0, count(o, "pf_base"));
            assertFalse(tableExists(o, "pf_refused"));

            try (Connection c = ds.getConnection()) {
                assertFalse(physical(c).inServerTransaction());
                exec(c, "CREATE TABLE pf_next (id INT)");
                exec(c, "INSERT INTO pf_base VALUES (2)");
                c.commit();
            }
            assertTrue(tableExists(o, "pf_next"));
            assertEquals(1, count(o, "pf_base"));
        }
    }

    /**
     * The option belongs to the URL that made the connection: a pool without
     * it, on the same server and alongside one with it, keeps the strict
     * refusal for every borrower.
     */
    @Test
    void thePoolWithoutTheOptionKeepsRefusing() throws Exception {
        try (HikariDataSource strict = pool("");
             HikariDataSource lenient = pool("&allowDdlOutsideTransaction=true");
             Connection o = observer()) {
            for (int i = 0; i < 2; i++) {
                try (Connection c = lenient.getConnection()) {
                    exec(c, "CREATE TABLE po_lenient_" + i + " (id INT)");
                }
                try (Connection c = strict.getConnection()) {
                    final int k = i;
                    SQLException e = assertThrows(SQLException.class,
                            () -> exec(c, "CREATE TABLE po_strict_" + k + " (id INT)"));
                    assertEquals("25001", e.getSQLState());
                }
                assertTrue(tableExists(o, "po_lenient_" + i));
                assertFalse(tableExists(o, "po_strict_" + i));
            }
        }
    }
}
