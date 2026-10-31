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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link Connection} CONTRACT conformance against a live server.
 *
 * <p>The theme of this file is settings that were STORED CLIENT-SIDE AND NEVER
 * SENT. Those are the worst kind of gap to leave untested, because every check
 * an application can make agrees with the setting it asked for while the server
 * is doing something else: {@code setTransactionIsolation(SERIALIZABLE)}
 * followed by {@code getTransactionIsolation() == SERIALIZABLE} was true of a
 * connection running at the server default. Each assertion below therefore
 * verifies the SERVER's answer ({@code SHOW …}), never the driver's own field.
 *
 * <p>Every refusal is paired with a POSITIVE CONTROL, so "it threw" can never
 * be a harness that throws on everything.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JdbcConnectionContractTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        // Pinned to `--transactional-ddl off`: this suite asserts the refusal of
        // DDL inside a transaction (25001), the server default in earlier releases.
        server = PdbTestServer.tryStart("--transactional-ddl", "off");
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping Connection contract suite");
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private String url() {
        return "jdbc:particledb://127.0.0.1:" + server.port + "/test?preferUnixSocket=false";
    }

    /** The SERVER's answer to a {@code SHOW}, not the driver's stored field. */
    private static String show(Connection c, String setting) throws SQLException {
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SHOW " + setting)) {
            assertTrue(rs.next(), "SHOW " + setting + " returned no row");
            return rs.getString(1);
        }
    }

    // ── transaction isolation ──────────────────────────────────────────────
    /**
     * {@code setTransactionIsolation} must reach the server. The driver's own
     * {@code getTransactionIsolation()} is NOT evidence - it used to be a
     * client-side field that echoed back whatever was passed in, so the round
     * trip through the driver agreed with the caller while the server never
     * heard about it.
     */
    @Test
    void setTransactionIsolation_reachesTheServer() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            assertEquals("serializable", show(c, "transaction_isolation"),
                    "setTransactionIsolation(SERIALIZABLE) never reached the server");
            assertEquals(Connection.TRANSACTION_SERIALIZABLE, c.getTransactionIsolation());

            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            assertEquals("repeatable read", show(c, "transaction_isolation"));
            assertEquals(Connection.TRANSACTION_REPEATABLE_READ, c.getTransactionIsolation());

            // POSITIVE CONTROL: the default is reachable again, so the setter
            // moves the server in both directions rather than latching.
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            assertEquals("read committed", show(c, "transaction_isolation"));
        }
    }

    /**
     * A FRESH connection must report the level the SERVER runs it at, and a
     * request for READ COMMITTED on it must reach the server. The driver used
     * to start its cached level at READ COMMITTED unconditionally, so on a
     * session running REPEATABLE READ it reported READ COMMITTED, and
     * {@code setTransactionIsolation(READ_COMMITTED)} - what a connection
     * pool's {@code transactionIsolation} setting calls on every new
     * connection - matched the stale field and sent nothing: the application
     * asked for READ COMMITTED and ran at REPEATABLE READ.
     *
     * <p>The server default is READ COMMITTED (as in PostgreSQL), so a request
     * for READ COMMITTED on an untouched session cannot tell a sent request
     * from a skipped one. The probe connections therefore first move their
     * session to REPEATABLE READ with plain SQL, which the driver does not
     * track, and only then exercise the driver's own calls.
     */
    @Test
    void freshConnection_reportsAndHonoursTheServersLevel() throws Exception {
        // The default itself: a session that set nothing runs READ COMMITTED,
        // and the driver reports what the server says.
        try (Connection c = DriverManager.getConnection(url())) {
            assertEquals("read committed", show(c, "transaction_isolation"),
                    "a session that set nothing must run at the server default, READ COMMITTED");
            assertEquals("read committed", show(c, "default_transaction_isolation"));
            assertEquals(Connection.TRANSACTION_READ_COMMITTED, c.getTransactionIsolation());
        }
        try (Connection c = DriverManager.getConnection(url())) {
            moveSessionToRepeatableRead(c);
            assertEquals(Connection.TRANSACTION_REPEATABLE_READ, c.getTransactionIsolation(),
                    "a fresh connection reported a level the server is not running");
        }
        try (Connection c = DriverManager.getConnection(url())) {
            moveSessionToRepeatableRead(c);
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            assertEquals("read committed", show(c, "transaction_isolation"),
                    "setTransactionIsolation(READ_COMMITTED) on a fresh connection never reached the server");
            assertEquals(Connection.TRANSACTION_READ_COMMITTED, c.getTransactionIsolation());
            // Inside a lazily-begun transaction too: the level is the session's.
            c.setAutoCommit(false);
            assertEquals("read committed", show(c, "transaction_isolation"));
            c.commit();
        }
    }

    /**
     * Put the session at REPEATABLE READ with plain SQL the driver does not
     * intercept, and prove it took. This is the POSITIVE CONTROL for the test
     * above: on a session already at READ COMMITTED a skipped request would
     * pass as a sent one.
     */
    private static void moveSessionToRepeatableRead(Connection c) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        }
        assertEquals("repeatable read", show(c, "transaction_isolation"),
                "premise: plain SQL moved the session to REPEATABLE READ");
    }

    /**
     * An isolation constant the driver cannot deliver must be REFUSED, and must
     * leave the previously negotiated level untouched. Accepting it and storing
     * it is how "we asked for SERIALIZABLE" becomes a false belief.
     */
    @Test
    void setTransactionIsolation_rejectsLevelsItCannotDeliver() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            // TRANSACTION_NONE means "no transactions" and the JDBC javadoc
            // explicitly excludes it as an argument here.
            assertThrows(SQLException.class,
                    () -> c.setTransactionIsolation(Connection.TRANSACTION_NONE));
            assertThrows(SQLException.class,
                    () -> c.setTransactionIsolation(999));
            assertEquals(Connection.TRANSACTION_SERIALIZABLE, c.getTransactionIsolation(),
                    "a rejected level must not overwrite the negotiated one");
            assertEquals("serializable", show(c, "transaction_isolation"),
                    "a rejected level must not have moved the server either");
        }
    }

    // ── read-only ──────────────────────────────────────────────────────────
    @Test
    void setReadOnly_reachesTheServer() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            assertFalse(c.isReadOnly());
            c.setReadOnly(true);
            assertEquals("on", show(c, "transaction_read_only"),
                    "setReadOnly(true) never reached the server");
            assertTrue(c.isReadOnly());
            // POSITIVE CONTROL: it comes back off.
            c.setReadOnly(false);
            assertEquals("off", show(c, "transaction_read_only"));
            assertFalse(c.isReadOnly());
        }
    }

    // ── schema / search_path ───────────────────────────────────────────────
    /**
     * {@code setSchema} must move the server's {@code search_path}. Storing the
     * name client-side means every unqualified statement afterwards resolved in
     * the DEFAULT schema while {@code getSchema()} reported the requested one.
     */
    @Test
    void setSchema_reachesTheServer() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            String before = show(c, "search_path");
            c.setSchema("public");
            assertEquals("public", c.getSchema());
            String after = show(c, "search_path");
            assertTrue(after.contains("public"),
                    "search_path after setSchema(\"public\") was " + after);
            assertNotEquals(before, after,
                    "setSchema never reached the server (search_path unchanged: "
                            + before + ")");
            // REFUSAL + POSITIVE CONTROL on the argument.
            assertThrows(SQLException.class, () -> c.setSchema(null));
            assertThrows(SQLException.class, () -> c.setSchema(""));
            assertDoesNotThrow(() -> c.setSchema("public"));
        }
    }

    // ── auto-commit / commit / rollback ────────────────────────────────────
    /**
     * {@code commit()} / {@code rollback()} in auto-commit mode are an ERROR per
     * the JDBC contract, not a no-op. Ignoring them lets a block that believes
     * it is transactional run to completion and report success while every
     * statement committed on its own.
     */
    @Test
    void commitAndRollbackInAutoCommitMode_areRefused() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            assertTrue(c.getAutoCommit(), "a fresh connection starts in auto-commit");
            assertThrows(SQLException.class, c::commit);
            assertThrows(SQLException.class, c::rollback);

            // POSITIVE CONTROL: with auto-commit off, both succeed and do their
            // job - so the refusal above is about the MODE, not about the calls
            // being unimplemented.
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE ac_t (id INT PRIMARY KEY, v INT)");
            }
            c.setAutoCommit(false);
            try (Statement st = c.createStatement()) {
                st.executeUpdate("INSERT INTO ac_t VALUES (1, 10)");
            }
            assertDoesNotThrow(() -> c.rollback());
            assertEquals(0, count(c, "SELECT id FROM ac_t WHERE id = 1"),
                    "rollback() did not undo the insert");
            try (Statement st = c.createStatement()) {
                st.executeUpdate("INSERT INTO ac_t VALUES (2, 20)");
            }
            assertDoesNotThrow(() -> c.commit());
            assertEquals(1, count(c, "SELECT id FROM ac_t WHERE id = 2"),
                    "commit() did not persist the insert");
            c.setAutoCommit(true);
        }
    }

    /**
     * ParticleDB cannot roll a DDL statement back, so with
     * {@code autoCommit=false} a DDL is refused (25001) inside the transaction
     * -- also as the first statement, which carries the BEGIN (see
     * {@code AutocommitOffScopeTest}). Whatever the DDL's outcome, the
     * statements after the caller's {@code rollback()} must still be
     * transactional: leaving the connection with {@code autoCommit == false}
     * and every later statement committing on its own means the caller's
     * {@code rollback()} has nothing to undo - a failed migration step turning
     * the rest of the migration into auto-commit.
     */
    @Test
    void failedDdlInsideTransaction_leavesTheTransactionOpen() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE ddlfail_t (id INT PRIMARY KEY, v INT)");
            }
            c.setAutoCommit(false);
            try {
                // A DDL that is guaranteed to fail: the table already exists.
                try (Statement st = c.createStatement()) {
                    st.executeUpdate("CREATE TABLE ddlfail_t (id INT PRIMARY KEY)");
                    fail("expected the duplicate CREATE TABLE to fail");
                } catch (SQLException expected) {
                    // the point of the test is what the CONNECTION looks like now
                }
                // The failure aborted the block, as in PostgreSQL.
                c.rollback();
                // Still transactional: this insert must be undoable.
                try (Statement st = c.createStatement()) {
                    st.executeUpdate("INSERT INTO ddlfail_t VALUES (7, 70)");
                }
                c.rollback();
                assertEquals(0, count(c, "SELECT id FROM ddlfail_t WHERE id = 7"),
                        "the statement after a FAILED in-transaction DDL was "
                        + "auto-committed — the driver did not reopen the "
                        + "transaction on the failure path");
            } finally {
                c.setAutoCommit(true);
            }
        }
    }

    /**
     * Companion to the test above with a DDL that WOULD succeed in autocommit
     * (a fresh index): it is refused all the same -- never run outside the
     * transaction -- and the statement after the rollback is transactional.
     */
    @Test
    void validDdlInsideTransaction_isRefusedAndLeavesTheTransactionOpen() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE ddlok_t (id INT PRIMARY KEY, v INT)");
            }
            c.setAutoCommit(false);
            try {
                try (Statement st = c.createStatement()) {
                    SQLException e = assertThrows(SQLException.class,
                            () -> st.executeUpdate("CREATE INDEX ddlok_v ON ddlok_t (v)"));
                    assertEquals("25001", e.getSQLState(), e.getMessage());
                }
                c.rollback();
                try (Statement st = c.createStatement()) {
                    st.executeUpdate("INSERT INTO ddlok_t VALUES (8, 80)");
                }
                c.rollback();
                assertEquals(0, count(c, "SELECT id FROM ddlok_t WHERE id = 8"),
                        "the statement after a refused in-transaction DDL was "
                        + "auto-committed");
            } finally {
                c.setAutoCommit(true);
            }
        }
    }

    // ── closed connection ──────────────────────────────────────────────────
    /**
     * Every entry point on a CLOSED connection must raise SQLSTATE {@code 08003}
     * (connection_does_not_exist). That is the class connection pools evict on;
     * a driver that quietly serves a closed connection hands a pool a
     * half-dead object it will keep re-using.
     */
    @Test
    void closedConnection_refusesEveryEntryPointWith08003() throws Exception {
        Connection c = DriverManager.getConnection(url());
        // POSITIVE CONTROL, before close: each of these works.
        assertNotNull(c.createStatement());
        assertNotNull(c.prepareStatement("SELECT 1"));
        assertTrue(c.isValid(2));
        assertDoesNotThrow(() -> c.setAutoCommit(false));
        assertDoesNotThrow(() -> c.setAutoCommit(true));

        c.close();
        assertTrue(c.isClosed());
        assertFalse(c.isValid(2), "isValid must be false on a closed connection");
        for (ThrowingCall call : new ThrowingCall[]{
                c::createStatement,
                () -> c.prepareStatement("SELECT 1"),
                () -> c.setAutoCommit(false),
                c::commit,
                c::rollback,
                () -> c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE),
                () -> c.setReadOnly(true),
                () -> c.setSchema("public"),
        }) {
            SQLException e = assertThrows(SQLException.class, call::call);
            assertEquals("08003", e.getSQLState(),
                    "closed-connection error must carry 08003, got "
                            + e.getSQLState() + " for: " + e.getMessage());
        }
        // close() is idempotent per the JDBC contract.
        assertDoesNotThrow(() -> c.close());
    }

    /** {@code abort(Executor)} must close the connection. */
    @Test
    void abort_closesTheConnection() throws Exception {
        Connection c = DriverManager.getConnection(url());
        assertTrue(c.isValid(2));            // POSITIVE CONTROL
        c.abort(Runnable::run);
        assertTrue(c.isClosed());
        assertFalse(c.isValid(2));
    }

    // ── connection-pool surfaces ───────────────────────────────────────────
    /**
     * The calls HikariCP makes on every checkout/return. A pool that gets a
     * wrong answer here silently reuses a connection in the wrong state.
     */
    @Test
    void poolLifecycleCalls_areHonoured() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            assertTrue(c.isValid(5));
            assertTrue(c.isWrapperFor(Connection.class));
            assertSame(c, c.unwrap(Connection.class));
            assertThrows(SQLException.class, () -> c.unwrap(String.class));

            // Hikari's resetConnectionState touches all of these.
            c.setAutoCommit(false);
            assertFalse(c.getAutoCommit());
            c.rollback();
            c.setAutoCommit(true);
            assertTrue(c.getAutoCommit());
            c.setReadOnly(false);
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            c.clearWarnings();
            assertNull(c.getWarnings());

            // Still usable after the reset dance.
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    // ── savepoints: the server's SAVEPOINT / ROLLBACK TO / RELEASE ─────────
    /**
     * Savepoints are implemented (they used to be refused). The metadata must
     * agree, a savepoint must do what PostgreSQL's does -- ROLLBACK TO undoes
     * only the work after it, and the transaction stays usable after an error
     * rolled back to it -- and auto-commit mode refuses them, as pgjdbc does.
     * Asserted on the SERVER's state (the rows that committed), not on the
     * driver's bookkeeping.
     */
    @Test
    void savepoints_workAndAgreeWithTheMetadata() throws Exception {
        try (Connection c = DriverManager.getConnection(url()); Statement st = c.createStatement()) {
            assertTrue(c.getMetaData().supportsSavepoints(), "metadata must advertise the savepoints the driver makes");
            assertThrows(SQLException.class, c::setSavepoint, "auto-commit mode must refuse a savepoint");
            st.execute("DROP TABLE IF EXISTS jcc_sp");
            st.execute("CREATE TABLE jcc_sp (id INT PRIMARY KEY)");
            c.setAutoCommit(false);
            try {
                st.execute("INSERT INTO jcc_sp VALUES (1)");
                java.sql.Savepoint named = c.setSavepoint("sp1");
                assertEquals("sp1", named.getSavepointName());
                st.execute("INSERT INTO jcc_sp VALUES (2)");
                SQLException dup = assertThrows(SQLException.class, () -> st.execute("INSERT INTO jcc_sp VALUES (1)"));
                assertEquals("23505", dup.getSQLState());
                c.rollback(named);
                java.sql.Savepoint anon = c.setSavepoint();
                assertTrue(anon.getSavepointId() > 0);
                st.execute("INSERT INTO jcc_sp VALUES (3)");
                c.releaseSavepoint(anon);
                c.commit();
            } finally {
                c.setAutoCommit(true);
            }
            try (ResultSet rs = st.executeQuery("SELECT id FROM jcc_sp ORDER BY id")) {
                assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(3, rs.getInt(1), "row 2 must be gone: it followed the savepoint");
                assertFalse(rs.next());
            }
        }
    }

    // ── CallableStatement / LOB factories: refusal + positive control ──────
    /**
     * {@code prepareCall} and the LOB/SQLXML/Struct factories are not
     * implemented. They must refuse loudly - a null return or a silent stub is
     * how a framework ends up calling methods on nothing - and the refusal must
     * agree with the metadata that advertises them.
     */
    @Test
    void unimplementedFactories_refuse_andThePreparedPathStillWorks() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            assertThrows(SQLFeatureNotSupportedException.class,
                    () -> c.prepareCall("{call foo()}"));
            assertThrows(SQLFeatureNotSupportedException.class, c::createBlob);
            assertThrows(SQLFeatureNotSupportedException.class, c::createClob);
            assertThrows(SQLFeatureNotSupportedException.class, c::createNClob);
            assertThrows(SQLFeatureNotSupportedException.class, c::createSQLXML);
            assertThrows(SQLFeatureNotSupportedException.class,
                    () -> c.createStruct("t", new Object[0]));
            assertFalse(c.getMetaData().supportsStoredFunctionsUsingCallSyntax(),
                    "metadata must not advertise call syntax the driver refuses");

            // POSITIVE CONTROL: the factory that IS implemented works, and the
            // ordinary prepared path works, so the refusals are specific.
            assertNotNull(c.createArrayOf("text", new Object[]{"a", "b"}));
            try (PreparedStatement ps = c.prepareStatement("SELECT ?")) {
                ps.setInt(1, 7);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(7, rs.getInt(1));
                }
            }
        }
    }

    // ── helpers ────────────────────────────────────────────────────────────
    private interface ThrowingCall { void call() throws Exception; }

    private static int count(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = 0;
            while (rs.next()) n++;
            return n;
        }
    }
}
