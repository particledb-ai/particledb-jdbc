package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Lazy transactions: {@code setAutoCommit(false)} / {@code commit()} /
 * {@code rollback()} send nothing to START a transaction; the first statement
 * carries the BEGIN ({@code FLAG_TXN_BEGIN}). Pinned here, against a live
 * server:
 *
 * <ul>
 *   <li>the Conductor DAO shape ({@code setAutoCommit(false)}, one statement,
 *       {@code commit()}, {@code setAutoCommit(true)}) is exactly 2 requests;</li>
 *   <li>and every JDBC semantic the eager BEGIN used to provide still holds,
 *       judged from a SECOND connection: rollback undoes, commit publishes,
 *       {@code setAutoCommit(true)} mid-transaction commits, DDL never commits
 *       the open transaction and never runs outside one (it is refused with
 *       25001, also as the first statement), a READ ONLY connection's
 *       writes are refused with 25006, a failed
 *       statement is recoverable by rollback, batches are transactional, and
 *       transaction control written as SQL text keeps the state right.</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LazyTransactionTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        // Pinned to `--transactional-ddl off`: this suite asserts the refusal of
        // DDL inside a transaction (25001), the server default in earlier releases.
        server = PdbTestServer.tryStart("--transactional-ddl", "off");
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping lazy-transaction test");
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private Connection open() throws SQLException {
        return DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + server.port
                + "/test?preferUnixSocket=false");
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

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    @Test
    void theConductorDaoShapeIsTwoRequests() throws Exception {
        try (Connection c = open(); Connection other = open()) {
            exec(c, "CREATE TABLE lt_dao (id BIGINT PRIMARY KEY, v BIGINT)");
            exec(c, "INSERT INTO lt_dao VALUES (1, 0)");
            String sql = "UPDATE lt_dao SET v = v + 1 WHERE id = ?";
            for (int i = 0; i < 3; i++) {
                // Warm-up iteration 0 also PREPAREs the statement (one extra
                // request); iterations 1..2 are the steady state.
                long before = sent(c);
                boolean prev = c.getAutoCommit();
                c.setAutoCommit(false);
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    ps.setLong(1, 1);
                    assertEquals(1, ps.executeUpdate());
                }
                c.commit();
                c.setAutoCommit(prev);
                if (i > 0) {
                    assertEquals(2, sent(c) - before,
                            "setAutoCommit(false); stmt; commit(); setAutoCommit(true) must be 2 requests");
                }
            }
            try (ResultSet rs = other.createStatement().executeQuery("SELECT v FROM lt_dao WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getLong(1), "all three committed transactions are visible elsewhere");
            }
        }
    }

    @Test
    void emptyTransactionsSendNothing() throws Exception {
        try (Connection c = open()) {
            long before = sent(c);
            c.setAutoCommit(false);
            c.commit();
            c.rollback();
            c.setAutoCommit(true);
            assertEquals(0, sent(c) - before, "no statement ran: nothing to begin or end");
        }
    }

    @Test
    void rollbackUndoesAndCommitPublishes() throws Exception {
        try (Connection c = open(); Connection other = open()) {
            exec(c, "CREATE TABLE lt_rb (id BIGINT PRIMARY KEY)");
            c.setAutoCommit(false);
            exec(c, "INSERT INTO lt_rb VALUES (1)");
            assertEquals(0, count(other, "lt_rb"), "uncommitted row visible to another connection");
            c.rollback();
            assertEquals(0, count(c, "lt_rb"), "rollback did not undo");
            exec(c, "INSERT INTO lt_rb VALUES (2)");
            assertEquals(0, count(other, "lt_rb"));
            c.commit();
            assertEquals(1, count(other, "lt_rb"), "commit did not publish");
            // After commit the NEXT statement is transactional again.
            exec(c, "INSERT INTO lt_rb VALUES (3)");
            assertEquals(1, count(other, "lt_rb"), "statement after commit ran in autocommit");
            c.rollback();
            assertEquals(1, count(other, "lt_rb"));
            c.setAutoCommit(true);
        }
    }

    @Test
    void switchingAutocommitOnMidTransactionCommits() throws Exception {
        try (Connection c = open(); Connection other = open()) {
            exec(c, "CREATE TABLE lt_ac (id BIGINT PRIMARY KEY)");
            c.setAutoCommit(false);
            exec(c, "INSERT INTO lt_ac VALUES (1)");
            assertEquals(0, count(other, "lt_ac"));
            c.setAutoCommit(true);
            assertEquals(1, count(other, "lt_ac"), "setAutoCommit(true) must commit the open transaction");
        }
    }

    /**
     * {@code autoCommit=false; INSERT; CREATE TABLE; rollback()}: in PostgreSQL
     * (and pgjdbc) the rollback undoes both. ParticleDB cannot roll a DDL back,
     * so the DDL is REFUSED inside the open transaction (25001) -- and the
     * INSERT must still be undone. This driver used to COMMIT the open
     * transaction before running the DDL, which left the INSERT committed.
     */
    @Test
    void ddlNeverCommitsTheOpenTransaction() throws Exception {
        try (Connection c = open(); Connection other = open()) {
            exec(c, "CREATE TABLE lt_ddl (id BIGINT PRIMARY KEY)");
            c.setAutoCommit(false);
            exec(c, "INSERT INTO lt_ddl VALUES (1)");
            SQLException refused = null;
            try {
                exec(c, "CREATE TABLE lt_ddl2 (id BIGINT PRIMARY KEY)");
            } catch (SQLException e) {
                refused = e;
            }
            c.rollback();
            assertEquals(0, count(other, "lt_ddl"),
                    "INSERT; CREATE TABLE; rollback() left the INSERT committed: the DDL "
                    + "committed the open transaction");
            assertNotNull(refused, "a DDL inside an open transaction must be refused, not run");
            assertEquals("25001", refused.getSQLState(), refused.getMessage());
            assertThrows(SQLException.class, () -> count(other, "lt_ddl2"),
                    "the refused CREATE TABLE took effect");

            // No transaction open yet (the first statement after rollback()): the
            // DDL still carries the BEGIN and is refused the same way -- it never
            // runs outside the transaction (AutocommitOffScopeTest pins every
            // route). It used to run on its own with only a SQLWarning, and a
            // later rollback() silently left it in place.
            SQLException first = assertThrows(SQLException.class,
                    () -> exec(c, "CREATE TABLE lt_ddl3 (id BIGINT PRIMARY KEY)"));
            assertEquals("25001", first.getSQLState(), first.getMessage());
            c.rollback();
            assertThrows(SQLException.class, () -> count(other, "lt_ddl3"),
                    "the refused first-statement CREATE TABLE took effect");

            // A FAILING DDL must not leave the connection in autocommit either.
            assertThrows(SQLException.class, () -> exec(c, "CREATE TABLE lt_ddl (id BIGINT)"));
            c.rollback(); // the refusal aborted the block, as any error does
            exec(c, "INSERT INTO lt_ddl VALUES (3)");
            assertEquals(0, count(other, "lt_ddl"), "DML after a failed DDL ran in autocommit");
            c.rollback();
            assertEquals(0, count(other, "lt_ddl"));
            c.setAutoCommit(true);
        }
    }

    /**
     * {@code setReadOnly(true)} reaches the server as the session default, and
     * a write inside the transaction is then refused with 25006
     * (read_only_sql_transaction), as in PostgreSQL. The native wire used to
     * accept the read-only request and discard it.
     */
    @Test
    void readOnlyConnectionRefusesWritesInsideATransaction() throws Exception {
        try (Connection c = open(); Connection other = open()) {
            exec(c, "CREATE TABLE lt_ro (id BIGINT PRIMARY KEY)");
            c.setReadOnly(true);
            // Autocommit too: PostgreSQL applies default_transaction_read_only
            // to the implicit transaction of every autocommit statement.
            SQLException auto = assertThrows(SQLException.class,
                    () -> exec(c, "INSERT INTO lt_ro VALUES (0)"),
                    "an autocommit write on a read-only connection succeeded");
            assertEquals("25006", auto.getSQLState(), auto.getMessage());
            c.setAutoCommit(false);
            assertEquals(0, count(c, "lt_ro"), "reads are allowed");
            SQLException e = assertThrows(SQLException.class,
                    () -> exec(c, "INSERT INTO lt_ro VALUES (1)"),
                    "a write in a READ ONLY transaction succeeded");
            assertEquals("25006", e.getSQLState(), e.getMessage());
            c.rollback();
            c.setReadOnly(false);
            exec(c, "INSERT INTO lt_ro VALUES (2)");
            c.commit();
            c.setAutoCommit(true);
            assertEquals(1, count(other, "lt_ro"));
        }
    }

    @Test
    void aFailedStatementIsRecoveredByRollback() throws Exception {
        try (Connection c = open(); Connection other = open()) {
            exec(c, "CREATE TABLE lt_err (id BIGINT PRIMARY KEY)");
            exec(c, "INSERT INTO lt_err VALUES (1)");
            c.setAutoCommit(false);
            exec(c, "INSERT INTO lt_err VALUES (2)");
            assertThrows(SQLException.class, () -> exec(c, "INSERT INTO lt_err VALUES (1)"));
            c.rollback();
            assertEquals(1, count(other, "lt_err"));
            // The connection is usable, and transactional, after the rollback.
            exec(c, "INSERT INTO lt_err VALUES (4)");
            c.commit();
            assertEquals(2, count(other, "lt_err"));
            c.setAutoCommit(true);
        }
    }

    @Test
    void theFirstStatementOfATransactionFailingStillLeavesItRollbackable() throws Exception {
        try (Connection c = open(); Connection other = open()) {
            exec(c, "CREATE TABLE lt_first (id BIGINT PRIMARY KEY)");
            exec(c, "INSERT INTO lt_first VALUES (1)");
            c.setAutoCommit(false);
            // This statement carries the BEGIN and fails.
            assertThrows(SQLException.class, () -> exec(c, "INSERT INTO lt_first VALUES (1)"));
            c.rollback();
            exec(c, "INSERT INTO lt_first VALUES (2)");
            assertEquals(1, count(other, "lt_first"), "statement after rollback ran in autocommit");
            c.commit();
            assertEquals(2, count(other, "lt_first"));
            c.setAutoCommit(true);
        }
    }

    @Test
    void batchesAreTransactional() throws Exception {
        try (Connection c = open(); Connection other = open()) {
            exec(c, "CREATE TABLE lt_batch (id BIGINT PRIMARY KEY)");
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO lt_batch VALUES (?)")) {
                for (int i = 1; i <= 3; i++) {
                    ps.setInt(1, i);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            assertEquals(0, count(other, "lt_batch"));
            c.rollback();
            assertEquals(0, count(c, "lt_batch"));
            c.setAutoCommit(true);
            // addBatch/executeBatch is pipelined: the whole batch is ONE request
            // (one BATCH frame), not one round trip per statement.
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO lt_batch VALUES (?)")) {
                for (int i = 10; i < 20; i++) {
                    ps.setInt(1, i);
                    ps.addBatch();
                }
                long before = sent(c);
                ps.executeBatch();
                assertEquals(1, sent(c) - before, "a 10-statement batch must be one request");
            }
            assertEquals(10, count(other, "lt_batch"));
        }
    }

    @Test
    void transactionControlWrittenAsSqlKeepsTheStateRight() throws Exception {
        try (Connection c = open(); Connection other = open()) {
            exec(c, "CREATE TABLE lt_sql (id BIGINT PRIMARY KEY)");
            c.setAutoCommit(false);
            exec(c, "INSERT INTO lt_sql VALUES (1)");
            exec(c, "COMMIT");
            assertEquals(1, count(other, "lt_sql"));
            // The driver knows the transaction ended: the next statement opens one.
            exec(c, "INSERT INTO lt_sql VALUES (2)");
            assertEquals(1, count(other, "lt_sql"), "statement after a SQL COMMIT ran in autocommit");
            c.rollback();
            assertEquals(1, count(c, "lt_sql"));
            c.setAutoCommit(true);
        }
    }

    /**
     * The reported defect: a COMMIT executed through a PreparedStatement ended
     * the server's transaction without the driver knowing, so the next insert
     * ran in AUTOCOMMIT and survived rollback(). The state is now the server's.
     */
    @Test
    void aPreparedCommitIsSeenAndTheNextStatementIsTransactional() throws Exception {
        try (Connection c = open(); Connection other = open()) {
            exec(c, "CREATE TABLE lt_pc (id BIGINT PRIMARY KEY)");
            c.setAutoCommit(false);
            exec(c, "INSERT INTO lt_pc VALUES (1)");
            try (PreparedStatement ps = c.prepareStatement("COMMIT")) {
                ps.execute();
            }
            assertEquals(1, count(other, "lt_pc"));
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO lt_pc VALUES (?)")) {
                ps.setLong(1, 2);
                ps.executeUpdate();
            }
            assertEquals(1, count(other, "lt_pc"), "the insert after a prepared COMMIT ran in autocommit");
            c.rollback();
            assertEquals(1, count(c, "lt_pc"), "rollback() did not undo the insert after a prepared COMMIT");
            c.setAutoCommit(true);
        }
    }

    /** Prepared and text transaction control mixed, in both directions. */
    @Test
    void preparedAndTextTransactionControlMix() throws Exception {
        try (Connection c = open(); Connection other = open()) {
            exec(c, "CREATE TABLE lt_mix (id BIGINT PRIMARY KEY)");
            // autocommit ON, a prepared BEGIN, a text ROLLBACK.
            try (PreparedStatement b = c.prepareStatement("BEGIN")) {
                b.execute();
            }
            exec(c, "INSERT INTO lt_mix VALUES (1)");
            assertEquals(0, count(other, "lt_mix"), "insert after a prepared BEGIN autocommitted");
            exec(c, "ROLLBACK");
            assertEquals(0, count(c, "lt_mix"));
            // autocommit OFF, a prepared ROLLBACK ends it; the next statement opens a new one.
            c.setAutoCommit(false);
            exec(c, "INSERT INTO lt_mix VALUES (2)");
            try (PreparedStatement r = c.prepareStatement("ROLLBACK")) {
                r.execute();
            }
            exec(c, "INSERT INTO lt_mix VALUES (3)");
            assertEquals(0, count(other, "lt_mix"), "insert after a prepared ROLLBACK autocommitted");
            c.commit();
            assertEquals(1, count(other, "lt_mix"), "commit() missed the transaction a prepared ROLLBACK left");
            c.setAutoCommit(true);
        }
    }

    /** SQL savepoints keep the transaction open; the driver must not lose it. */
    @Test
    void sqlSavepointsKeepTheTransaction() throws Exception {
        try (Connection c = open(); Connection other = open()) {
            exec(c, "CREATE TABLE lt_sp (id BIGINT PRIMARY KEY)");
            c.setAutoCommit(false);
            exec(c, "INSERT INTO lt_sp VALUES (1)");
            exec(c, "SAVEPOINT s1");
            exec(c, "INSERT INTO lt_sp VALUES (2)");
            exec(c, "ROLLBACK TO SAVEPOINT s1");
            exec(c, "INSERT INTO lt_sp VALUES (3)");
            assertEquals(0, count(other, "lt_sp"), "work after ROLLBACK TO SAVEPOINT autocommitted");
            c.commit();
            assertEquals(2, count(other, "lt_sp"), "expected rows 1 and 3");
            c.setAutoCommit(true);
        }
    }

    /**
     * A failed block (the server reports FAILED) is ended by commit() -- PG
     * turns it into a rollback -- and the driver then opens a NEW transaction
     * for the next statement.
     */
    @Test
    void aFailedBlockEndsAndTheNextTransactionOpens() throws Exception {
        try (Connection c = open(); Connection other = open()) {
            exec(c, "CREATE TABLE lt_fb (id BIGINT PRIMARY KEY)");
            exec(c, "INSERT INTO lt_fb VALUES (100)");
            c.setAutoCommit(false);
            exec(c, "INSERT INTO lt_fb VALUES (1)");
            assertThrows(SQLException.class, () -> exec(c, "INSERT INTO lt_fb VALUES (100)"));
            assertThrows(SQLException.class, () -> exec(c, "INSERT INTO lt_fb VALUES (2)"),
                    "a failed block must refuse further statements");
            // PostgreSQL answers the COMMIT of a failed block with the tag
            // ROLLBACK and no error, and pgjdbc's commit() returns normally;
            // pdb-jdbc must do the same (an exception here is what made
            // Conductor log a stack trace per failed transaction).
            assertDoesNotThrow(c::commit, "commit() of a failed block must return like pgjdbc's");
            assertEquals(1, count(other, "lt_fb"), "the failed block's insert must not be committed");
            exec(c, "INSERT INTO lt_fb VALUES (3)");
            assertEquals(1, count(other, "lt_fb"), "statement after a failed block autocommitted");
            c.rollback();
            assertEquals(1, count(c, "lt_fb"));
            c.setAutoCommit(true);
        }
    }

    @Test
    void isolationAndReadOnlyDoNotOpenATransaction() throws Exception {
        try (Connection c = open()) {
            c.setAutoCommit(false);
            long before = sent(c);
            c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            assertEquals(Connection.TRANSACTION_SERIALIZABLE, c.getTransactionIsolation());
            assertEquals(1, sent(c) - before, "one SET, no BEGIN");
            long mid = sent(c);
            c.rollback();
            assertEquals(0, sent(c) - mid, "the SET must not have opened a transaction");
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            c.setAutoCommit(true);
        }
    }
}
