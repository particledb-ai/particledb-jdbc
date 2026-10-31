package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * With {@code autoCommit=false}, EVERY statement runs inside the transaction
 * the server knows about -- or is refused loudly. Never silently outside it.
 *
 * <p>The defect this pins (found in a pre-release consistency review): the driver
 * begins transactions lazily, and a DDL issued as the FIRST statement after
 * {@code setAutoCommit(false)} was sent WITHOUT the BEGIN, ran as an autocommit
 * statement and was ACCEPTED; {@code rollback()} then silently did not undo it
 * (the table persisted). The same CREATE after a DML was correctly refused with
 * {@code 25001}, and over the PostgreSQL wire (pgjdbc sends BEGIN eagerly) it
 * is always refused. Every assertion here is judged from a SECOND connection --
 * the durable outcome, not the error alone.
 *
 * <p>Also pinned: the other statement kinds keep the transaction (SET,
 * SAVEPOINT, SELECT, SQL-text BEGIN/COMMIT), the prepared and batch routes, the
 * statement-after-commit()/rollback() position, the same scenario over the
 * PostgreSQL wire with pgjdbc against the SAME server, and the opt-in
 * {@code raiseExceptionOnSilentRollback} signal (item I).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AutocommitOffScopeTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        // Pinned to `--transactional-ddl off`: this suite asserts the refusal of
        // DDL inside a transaction (25001), the server default in earlier releases.
        server = PdbTestServer.tryStart("--transactional-ddl", "off");
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping autocommit-off scope test");
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private Connection open() throws SQLException {
        return open("");
    }

    private Connection open(String extra) throws SQLException {
        return DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + server.port
                + "/test?preferUnixSocket=false" + extra);
    }

    private static long sent(Connection c) throws SQLException {
        return c.unwrap(PdbConnection.class).requestsSent();
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

    private static boolean columnExists(Connection c, String table, String col) {
        try (Statement st = c.createStatement()) {
            st.executeQuery("SELECT " + col + " FROM " + table).close();
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

    private static SQLException refusal(String what, org.junit.jupiter.api.function.Executable ex) {
        SQLException e = assertThrows(SQLException.class, ex,
                what + ": a DDL with autoCommit=false must be refused, not run outside the transaction");
        assertEquals("25001", e.getSQLState(), what + ": " + e.getMessage());
        return e;
    }

    /**
     * The reported shape, for each DDL kind: {@code setAutoCommit(false)}, the
     * DDL as the FIRST statement, {@code rollback()}. Refused 25001, and the
     * catalog is exactly as before.
     */
    @Test
    void aDdlAsTheFirstStatementIsRefusedAndChangesNothing() throws Exception {
        try (Connection other = open()) {
            exec(other, "CREATE TABLE ao_base (id BIGINT PRIMARY KEY, v INT)");
            exec(other, "INSERT INTO ao_base VALUES (1, 1)");
            exec(other, "CREATE TABLE ao_drop (id BIGINT PRIMARY KEY)");

            String[] ddls = {
                    "CREATE TABLE ao_new (id BIGINT PRIMARY KEY)",
                    "  /* migration header */ create table ao_new_lc (id INT)",
                    "ALTER TABLE ao_base ADD COLUMN w INT",
                    "DROP TABLE ao_drop",
                    "TRUNCATE ao_base",
                    "CREATE INDEX ao_base_v ON ao_base (v)",
                    "COMMENT ON TABLE ao_base IS 'x'",
            };
            for (String ddl : ddls) {
                try (Connection c = open()) {
                    c.setAutoCommit(false);
                    refusal(ddl, () -> exec(c, ddl));
                    c.rollback();
                    // The connection is usable and still transactional.
                    exec(c, "INSERT INTO ao_base VALUES (100, 100)");
                    assertEquals(1, count(other, "ao_base"),
                            ddl + ": the statement after the refusal ran in autocommit");
                    c.rollback();
                }
                assertFalse(tableExists(other, "ao_new"), ddl);
                assertFalse(tableExists(other, "ao_new_lc"), ddl);
                assertFalse(columnExists(other, "ao_base", "w"), ddl);
                assertTrue(tableExists(other, "ao_drop"), ddl + ": the refused DROP took effect");
                assertEquals(1, count(other, "ao_base"), ddl + ": the refused TRUNCATE took effect");
            }
        }
    }

    /** The first statement after commit() and after rollback() is in the same position. */
    @Test
    void aDdlFirstAfterCommitOrRollbackIsRefused() throws Exception {
        try (Connection c = open(); Connection other = open()) {
            exec(c, "CREATE TABLE ao_pos (id BIGINT PRIMARY KEY)");
            c.setAutoCommit(false);
            exec(c, "INSERT INTO ao_pos VALUES (1)");
            c.commit();
            refusal("after commit()", () -> exec(c, "CREATE TABLE ao_pos_c (id INT)"));
            c.rollback();
            exec(c, "INSERT INTO ao_pos VALUES (2)");
            c.rollback();
            refusal("after rollback()", () -> exec(c, "CREATE TABLE ao_pos_r (id INT)"));
            c.rollback();
            assertFalse(tableExists(other, "ao_pos_c"));
            assertFalse(tableExists(other, "ao_pos_r"));
            assertEquals(1, count(other, "ao_pos"));
            c.setAutoCommit(true);
            // Autocommit DDL is unaffected.
            exec(c, "CREATE TABLE ao_pos_auto (id INT)");
            assertTrue(tableExists(other, "ao_pos_auto"));
        }
    }

    /** The PreparedStatement and batch routes reach the same answer. */
    @Test
    void preparedAndBatchedDdlAreRefusedToo() throws Exception {
        try (Connection c = open(); Connection other = open()) {
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement("CREATE TABLE ao_ps (id INT)")) {
                refusal("PreparedStatement", ps::execute);
            }
            c.rollback();
            assertFalse(tableExists(other, "ao_ps"));

            try (Statement st = c.createStatement()) {
                st.addBatch("CREATE TABLE ao_batch (id INT)");
                BatchUpdateException e = assertThrows(BatchUpdateException.class, st::executeBatch,
                        "a batched DDL with autoCommit=false must be refused");
                assertNotNull(e.getMessage());
            }
            c.rollback();
            assertFalse(tableExists(other, "ao_batch"), "the refused batched CREATE took effect");
            c.setAutoCommit(true);
        }
    }

    /**
     * Every other statement kind opens the transaction when it is first: the
     * server reports a block after it, so commit()/rollback() have something
     * to end (one request). Nothing runs outside the transaction.
     */
    @Test
    void everyOtherFirstStatementKindOpensTheTransaction() throws Exception {
        try (Connection c = open(); Connection other = open()) {
            exec(c, "CREATE TABLE ao_kinds (id BIGINT PRIMARY KEY)");
            c.setAutoCommit(false);
            for (String first : new String[] {
                    "SELECT 1",
                    "SHOW transaction_isolation",
                    "SET statement_timeout = 0",
                    "SAVEPOINT s1",
                    "INSERT INTO ao_kinds VALUES (1)",
                    "BEGIN",
            }) {
                exec(c, first);
                long before = sent(c);
                c.rollback();
                assertEquals(1, sent(c) - before,
                        "`" + first + "` as the first statement did not open the server's transaction");
            }
            assertEquals(0, count(other, "ao_kinds"), "the rolled-back INSERT survived");

            // SAVEPOINT as the first statement is a real savepoint in that block.
            exec(c, "SAVEPOINT s1");
            exec(c, "INSERT INTO ao_kinds VALUES (1)");
            exec(c, "ROLLBACK TO SAVEPOINT s1");
            exec(c, "INSERT INTO ao_kinds VALUES (2)");
            c.commit();
            try (Statement st = other.createStatement();
                 ResultSet rs = st.executeQuery("SELECT id FROM ao_kinds")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getLong(1));
                assertFalse(rs.next());
            }
            c.setAutoCommit(true);
        }
    }

    /**
     * The same scenario through pgjdbc over the SAME server's PostgreSQL wire:
     * the answer the native driver now matches.
     */
    @Test
    void pgWireAnswersTheSame() throws Exception {
        Class.forName("org.postgresql.Driver");
        String url = "jdbc:postgresql://127.0.0.1:" + server.pgPort + "/postgres?user=postgres";
        try (Connection pg = DriverManager.getConnection(url); Connection other = open()) {
            pg.setAutoCommit(false);
            SQLException e = assertThrows(SQLException.class,
                    () -> exec(pg, "CREATE TABLE ao_pg (id INT)"));
            assertEquals("25001", e.getSQLState(), e.getMessage());
            pg.rollback();
            assertFalse(tableExists(other, "ao_pg"));
        }
    }

    // ── Item I: a definite signal that commit() rolled back ───────────────

    private static void failTheTransaction(Connection c, String table) throws SQLException {
        exec(c, "INSERT INTO " + table + " VALUES (2)");
        assertThrows(SQLException.class, () -> exec(c, "INSERT INTO " + table + " VALUES (1)"));
    }

    /** Default: pgjdbc's behaviour - commit() returns, the write is discarded. */
    @Test
    void byDefaultCommitOfAFailedTransactionReturnsNormally() throws Exception {
        try (Connection c = open(); Connection other = open()) {
            exec(c, "CREATE TABLE ao_sr0 (id BIGINT PRIMARY KEY)");
            exec(c, "INSERT INTO ao_sr0 VALUES (1)");
            c.setAutoCommit(false);
            failTheTransaction(c, "ao_sr0");
            assertDoesNotThrow(c::commit);
            assertEquals(1, count(other, "ao_sr0"));
            c.setAutoCommit(true);
        }
    }

    /**
     * {@code raiseExceptionOnSilentRollback=true}: a COMMIT the server answered
     * by rolling back raises 25P02 naming the cause, by every route; a healthy
     * commit and an explicit rollback do not; the connection stays usable.
     */
    @Test
    void optInRaisesWhenTheServerRolledBackTheCommit() throws Exception {
        try (Connection c = open("&raiseExceptionOnSilentRollback=true"); Connection other = open()) {
            exec(c, "CREATE TABLE ao_sr (id BIGINT PRIMARY KEY)");
            exec(c, "INSERT INTO ao_sr VALUES (1)");
            c.setAutoCommit(false);

            failTheTransaction(c, "ao_sr");
            SQLException e = assertThrows(SQLException.class, c::commit,
                    "commit() of a rolled-back transaction must raise under the opt-in");
            assertEquals("25P02", e.getSQLState(), e.getMessage());
            assertTrue(e.getMessage().contains("ROLLBACK"), e.getMessage());
            assertEquals(1, count(other, "ao_sr"), "the failed transaction's insert was committed");

            // COMMIT as SQL text, through a Statement and a PreparedStatement.
            failTheTransaction(c, "ao_sr");
            e = assertThrows(SQLException.class, () -> exec(c, "COMMIT"));
            assertEquals("25P02", e.getSQLState(), e.getMessage());
            failTheTransaction(c, "ao_sr");
            try (PreparedStatement ps = c.prepareStatement("COMMIT")) {
                e = assertThrows(SQLException.class, ps::execute);
                assertEquals("25P02", e.getSQLState(), e.getMessage());
            }
            assertEquals(1, count(other, "ao_sr"));

            // A healthy commit and an explicit rollback raise nothing.
            exec(c, "INSERT INTO ao_sr VALUES (3)");
            assertDoesNotThrow(c::commit);
            assertEquals(2, count(other, "ao_sr"));
            failTheTransaction(c, "ao_sr");
            assertDoesNotThrow(() -> c.rollback());

            // setAutoCommit(true) ending a failed transaction is a commit too.
            failTheTransaction(c, "ao_sr");
            e = assertThrows(SQLException.class, () -> c.setAutoCommit(true));
            assertEquals("25P02", e.getSQLState(), e.getMessage());
            assertEquals(2, count(other, "ao_sr"));
            // Usable afterwards.
            exec(c, "INSERT INTO ao_sr VALUES (4)");
            assertEquals(3, count(other, "ao_sr"));
        }
    }

    /**
     * The EAGER protocol (an older server without transaction status) keeps a
     * BEGIN open at all times and never opens one lazily. A COMMIT that raises
     * under {@code raiseExceptionOnSilentRollback} must still re-open it: the
     * driver used to skip the BEGIN after the exception, leaving
     * {@code autoCommit=false} with NO transaction, so every later write
     * committed on its own and survived {@code rollback()}. Judged from a
     * second connection.
     */
    @Test
    void eagerProtocolReopensTheTransactionAfterARaisedSilentRollback() throws Exception {
        try (Connection c = open("&raiseExceptionOnSilentRollback=true"); Connection other = open()) {
            exec(c, "CREATE TABLE ao_eager (id BIGINT PRIMARY KEY)");
            exec(c, "INSERT INTO ao_eager VALUES (1)");
            c.unwrap(PdbConnection.class).forceEagerProtocol = true;
            c.setAutoCommit(false);

            failTheTransaction(c, "ao_eager");
            SQLException e = assertThrows(SQLException.class, c::commit);
            assertEquals("25P02", e.getSQLState(), e.getMessage());

            // Still in a transaction: this write must be undone by rollback().
            exec(c, "INSERT INTO ao_eager VALUES (5)");
            assertEquals(1, count(other, "ao_eager"),
                    "the write after a raised silent rollback ran OUTSIDE a transaction");
            c.rollback();
            assertEquals(1, count(other, "ao_eager"), "rollback() did not undo the write");

            // setAutoCommit(true) ending a failed block: raises, and the
            // connection is honestly in autocommit afterwards.
            failTheTransaction(c, "ao_eager");
            assertThrows(SQLException.class, () -> c.setAutoCommit(true));
            assertTrue(c.getAutoCommit());
            exec(c, "INSERT INTO ao_eager VALUES (6)");
            assertEquals(2, count(other, "ao_eager"));
        }
    }

    /**
     * A transaction-control request whose outcome the driver cannot know --
     * here the waiting thread is interrupted, after the request was handed to
     * the socket -- must not leave the connection running under a GUESSED
     * transaction state. Either the connection is closed, or the state it
     * reports is the server's: a write made in the reported mode behaves that
     * way, judged from a second connection.
     */
    @Test
    void anOutcomeUnknownTransactionControlNeverLeavesAGuessedState() throws Exception {
        for (boolean eager : new boolean[] {true, false}) {
            String t = "ao_unk_" + (eager ? "e" : "l");
            try (Connection setup = open()) {
                exec(setup, "CREATE TABLE " + t + " (id BIGINT PRIMARY KEY)");
            }
            // setAutoCommit(false), interrupted (eager: the BEGIN is sent).
            try (Connection c = open(); Connection other = open()) {
                c.unwrap(PdbConnection.class).forceEagerProtocol = eager;
                Thread.currentThread().interrupt();
                try {
                    c.setAutoCommit(false);
                } catch (SQLException expected) {
                    // outcome unknown
                } finally {
                    Thread.interrupted();
                }
                assertReportedStateIsTrue(c, other, t, 1, eager);
            }
            // commit(), interrupted.
            try (Connection c = open(); Connection other = open()) {
                c.unwrap(PdbConnection.class).forceEagerProtocol = eager;
                c.setAutoCommit(false);
                exec(c, "INSERT INTO " + t + " VALUES (10)");
                Thread.currentThread().interrupt();
                try {
                    c.commit();
                } catch (SQLException expected) {
                    // outcome unknown
                } finally {
                    Thread.interrupted();
                }
                assertReportedStateIsTrue(c, other, t, 20, eager);
            }
        }
    }

    /** Closed, or the reported autoCommit mode is what a write actually gets. */
    private static void assertReportedStateIsTrue(Connection c, Connection other, String t,
                                                  int id, boolean eager) throws SQLException {
        if (c.isClosed()) return;
        long before = count(other, t);
        exec(c, "INSERT INTO " + t + " VALUES (" + id + ")");
        long after = count(other, t);
        if (c.getAutoCommit()) {
            assertEquals(before + 1, after, (eager ? "[eager] " : "[lazy] ")
                    + "the connection reports autocommit, but its write is not visible: a transaction "
                    + "the driver does not know about is open on the server");
        } else {
            assertEquals(before, after, (eager ? "[eager] " : "[lazy] ")
                    + "the connection reports a transaction, but its write committed on its own");
            c.rollback();
            assertEquals(before, count(other, t));
        }
    }
}
