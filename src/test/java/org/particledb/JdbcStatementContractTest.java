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
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link Statement} / {@link PreparedStatement} CONTRACT conformance against a
 * live server.
 *
 * <p>The theme here is the mirror of {@code JdbcConnectionContractTest}: methods
 * that ACCEPTED work and then did not do it. {@code ps.addBatch("…")} appended
 * to a list nothing executed and {@code executeBatch()} reported success;
 * {@code setMaxRows(10)} returned every row; {@code setQueryTimeout(2)} waited
 * sixty seconds. None of these had a test, and none of them fails loudly, so
 * each would have survived any amount of regression testing that only checked
 * "did the call return".
 *
 * <p>Every refusal is paired with a POSITIVE CONTROL.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JdbcStatementContractTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping Statement contract suite");
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE stc_t (id INT PRIMARY KEY, v TEXT)");
            for (int i = 1; i <= 25; i++) {
                st.executeUpdate("INSERT INTO stc_t VALUES (" + i + ", 'v" + i + "')");
            }
        }
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private String url() {
        return "jdbc:particledb://127.0.0.1:" + server.port + "/test?preferUnixSocket=false";
    }

    // ── PreparedStatement rejects the String-taking Statement methods ──────
    /**
     * JDBC 4.3: the SQL-taking {@code Statement} methods "cannot be called on a
     * PreparedStatement object; a SQLException is thrown". This was not a
     * cosmetic deviation. {@code addBatch(String)} appended to the SUPERCLASS's
     * batch list, and the overridden {@code executeBatch()} reads a DIFFERENT
     * list - so the statements were accepted, never sent, and
     * {@code executeBatch()} answered with an empty {@code int[0]} that the
     * caller reads as "there was nothing to do".
     */
    @Test
    void preparedStatement_rejectsTheSqlTakingStatementMethods() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             PreparedStatement ps = c.prepareStatement("INSERT INTO stc_t VALUES (?, ?)")) {
            assertThrows(SQLException.class, () -> ps.addBatch("INSERT INTO stc_t VALUES (900,'x')"));
            assertThrows(SQLException.class, () -> ps.executeQuery("SELECT 1"));
            assertThrows(SQLException.class, () -> ps.executeUpdate("SELECT 1"));
            assertThrows(SQLException.class, () -> ps.execute("SELECT 1"));
            assertThrows(SQLException.class,
                    () -> ps.execute("SELECT 1", Statement.RETURN_GENERATED_KEYS));
            assertThrows(SQLException.class,
                    () -> ps.executeUpdate("SELECT 1", new String[]{"id"}));

            // The row the smuggled addBatch(String) would have inserted must not
            // exist - and neither must a silent "success" have been reported.
            assertEquals(0, count(c, "SELECT id FROM stc_t WHERE id = 900"));
        }
        // POSITIVE CONTROL: the no-argument forms on the same statement DO work,
        // so the refusals above are about the SQL-taking overloads specifically.
        try (Connection c = DriverManager.getConnection(url());
             PreparedStatement ps = c.prepareStatement("INSERT INTO stc_t VALUES (?, ?)")) {
            ps.setInt(1, 901); ps.setString(2, "a"); ps.addBatch();
            ps.setInt(1, 902); ps.setString(2, "b"); ps.addBatch();
            int[] rc = ps.executeBatch();
            assertEquals(2, rc.length);
            assertEquals(2, count(c, "SELECT id FROM stc_t WHERE id IN (901, 902)"),
                    "the no-argument addBatch()/executeBatch() path did not insert");
        }
    }

    // ── executeQuery()/executeUpdate() must record the current result ──────
    /**
     * {@code ps.executeQuery(); ps.getResultSet();} and
     * {@code ps.executeUpdate(); ps.getUpdateCount();} are the shapes
     * JdbcTemplate and MyBatis use. The PreparedStatement forms bypassed the
     * bookkeeping the String forms do, so {@code getResultSet()} was null,
     * {@code getUpdateCount()} was a stale value and
     * {@code rs.getStatement()} was null.
     */
    @Test
    void preparedStatement_recordsItsCurrentResult() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            try (PreparedStatement ps = c.prepareStatement("SELECT id FROM stc_t WHERE id = ?")) {
                ps.setInt(1, 3);
                ResultSet rs = ps.executeQuery();
                assertSame(rs, ps.getResultSet(),
                        "getResultSet() must return the ResultSet executeQuery() just produced");
                assertEquals(-1, ps.getUpdateCount(),
                        "getUpdateCount() must be -1 after a row-returning execution");
                assertSame(ps, rs.getStatement(),
                        "rs.getStatement() must point back at the statement that made it");
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO stc_t VALUES (?, ?)")) {
                ps.setInt(1, 910); ps.setString(2, "z");
                int n = ps.executeUpdate();
                assertEquals(n, ps.getUpdateCount(),
                        "getUpdateCount() must agree with executeUpdate()'s return");
                assertNull(ps.getResultSet(),
                        "getResultSet() must be null after an update");
            }
            // POSITIVE CONTROL: the plain Statement forms behave the same way.
            try (Statement st = c.createStatement()) {
                ResultSet rs = st.executeQuery("SELECT id FROM stc_t WHERE id = 4");
                assertSame(rs, st.getResultSet());
                assertSame(st, rs.getStatement());
                assertEquals(-1, st.getUpdateCount());
            }
        }
    }

    // ── setMaxRows is a limit, not a hint ─────────────────────────────────
    /**
     * JDBC: "if the limit is exceeded, the excess rows are silently dropped".
     * A no-op setter meant a caller that capped a query at 10 rows got all 25.
     */
    @Test
    void setMaxRows_dropsTheExcessRows() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            // POSITIVE CONTROL: uncapped, the table really does have 25 rows, so
            // the capped assertion below cannot pass merely because the query
            // returned few rows.
            assertEquals(0, st.getMaxRows());
            assertEquals(25, drain(st.executeQuery("SELECT id FROM stc_t WHERE id <= 25")));

            st.setMaxRows(10);
            assertEquals(10, st.getMaxRows());
            assertEquals(10, drain(st.executeQuery("SELECT id FROM stc_t WHERE id <= 25")),
                    "setMaxRows(10) did not drop the excess rows");

            st.setMaxRows(0);
            assertEquals(25, drain(st.executeQuery("SELECT id FROM stc_t WHERE id <= 25")),
                    "setMaxRows(0) must mean unlimited");
            assertThrows(SQLException.class, () -> st.setMaxRows(-1));
        }
    }

    // ── setQueryTimeout is honoured ───────────────────────────────────────
    /**
     * The value must actually become the wait - proven by making a statement
     * EXCEED it. Asserting only that the getter echoes the setter would pass on
     * the old code, which stored the number and let every statement wait a
     * hardcoded 60s; a caller asking for a 2-second cap blocked for sixty.
     *
     * <p>The failure has to arrive as {@link java.sql.SQLTimeoutException} with
     * PG's {@code 57014}: retry and circuit-breaker logic branches on that
     * class, and a bare SQLException whose message is "null" (which is what a
     * raw {@code TimeoutException} produces) is indistinguishable from a
     * transport error.
     */
    @Test
    void setQueryTimeout_isHonouredAndValidated() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            assertEquals(0, st.getQueryTimeout(), "JDBC defaults to no statement timeout");

            // POSITIVE CONTROL: a 2s query under a 30s cap SUCCEEDS, so the
            // refusal below is about the cap and not about pg_sleep failing.
            st.setQueryTimeout(30);
            assertEquals(1, drain(st.executeQuery("SELECT pg_sleep(2)")),
                    "a 2s query must complete under a 30s cap");

            st.setQueryTimeout(1);
            assertEquals(1, st.getQueryTimeout());
            long t0 = System.nanoTime();
            java.sql.SQLTimeoutException te = assertThrows(
                    java.sql.SQLTimeoutException.class,
                    () -> st.executeQuery("SELECT pg_sleep(20)"),
                    "setQueryTimeout(1) did not cap a 20s query");
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            assertEquals("57014", te.getSQLState(),
                    "a query timeout must carry PG's query_canceled SQLSTATE");
            assertTrue(elapsedMs < 10_000,
                    "the 1s cap took " + elapsedMs + "ms to fire — the value was "
                    + "not used as the wait");
            assertThrows(SQLException.class, () -> st.setQueryTimeout(-1));
            // The server cancelled the statement when the driver reset its
            // stream: the outcome is known, so the connection stays in service.
            assertFalse(c.isClosed(), "a server-cancelled timeout closed the connection");
            st.setQueryTimeout(30);
            assertEquals(1, drain(st.executeQuery("SELECT id FROM stc_t WHERE id = 2")));
        }
        // 0 == no limit, per the JDBC contract, and must not mean "fail at once".
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            st.setQueryTimeout(0);
            assertEquals(1, drain(st.executeQuery("SELECT pg_sleep(2)")),
                    "setQueryTimeout(0) must mean no limit");
        }
    }

    // ── closed Statement ──────────────────────────────────────────────────
    /**
     * A closed Statement must refuse every execution path. {@code close()} used
     * to set a flag nothing consulted, so a use-after-close kept issuing
     * statements on the connection - and never closed its ResultSet either.
     */
    @Test
    void closedStatement_refusesEveryExecutionPath() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            Statement st = c.createStatement();
            ResultSet rs = st.executeQuery("SELECT id FROM stc_t WHERE id = 1");
            assertTrue(rs.next());                       // POSITIVE CONTROL
            st.close();
            assertTrue(st.isClosed());
            assertTrue(rs.isClosed(),
                    "closing a Statement must close its current ResultSet");
            assertThrows(SQLException.class, () -> st.executeQuery("SELECT 1"));
            assertThrows(SQLException.class, () -> st.executeUpdate("SELECT 1"));
            assertThrows(SQLException.class, () -> st.execute("SELECT 1"));
            assertThrows(SQLException.class, () -> st.addBatch("SELECT 1"));
            assertThrows(SQLException.class, st::executeBatch);
            assertThrows(SQLException.class, st::getResultSet);
            assertThrows(SQLException.class, st::getUpdateCount);
            assertDoesNotThrow(() -> st.close());        // idempotent
        }
    }

    // ── executeUpdate must not swallow a result set ───────────────────────
    /**
     * {@code executeUpdate} on a row-returning statement is an error per JDBC.
     * Returning -1 and DISCARDING the rows let a caller read "-1 rows affected"
     * as a successful update that had in fact selected.
     */
    @Test
    void executeUpdateOnARowReturningStatement_isRefused() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            assertThrows(SQLException.class, () -> st.executeUpdate("SELECT 1"));
            // POSITIVE CONTROL: a real update through the same method works and
            // reports its count.
            assertEquals(1, st.executeUpdate("INSERT INTO stc_t VALUES (920, 'u')"));
            assertEquals(1, st.executeUpdate("UPDATE stc_t SET v = 'u2' WHERE id = 920"));
            assertEquals(1, st.executeUpdate("DELETE FROM stc_t WHERE id = 920"));
        }
    }

    // ── cancel() must not lie ─────────────────────────────────────────────
    /**
     * {@code cancel()} really cancels: the driver resets the statement's stream
     * and the server ends it with 57014 (the multiplexed wire's per-request
     * cancel). It once had an empty body that told JdbcTemplate's query-timeout
     * path a statement was cancelled when nothing was sent, and then refused,
     * because the wire had no way to cancel.
     */
    @Test
    void cancel_endsTheRunningStatementAndKeepsTheConnection() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            // Nothing running: a no-op, as in pgjdbc.
            st.cancel();
            Thread canceller = new Thread(() -> {
                try {
                    Thread.sleep(300);
                    st.cancel();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            long t0 = System.nanoTime();
            canceller.start();
            SQLException e = assertThrows(SQLException.class, () -> st.executeQuery("SELECT pg_sleep(20)"));
            canceller.join();
            assertEquals("57014", e.getSQLState(), e.getMessage());
            assertTrue((System.nanoTime() - t0) < 8_000_000_000L, "the cancel did not end the 20 s statement promptly");
            // POSITIVE CONTROL: the same statement and connection keep working.
            assertEquals(1, drain(st.executeQuery("SELECT id FROM stc_t WHERE id = 2")));
        }
    }

    // ── getMoreResults drain loop ─────────────────────────────────────────
    /**
     * The standard JDBC drain loop must TERMINATE. Its exit condition is
     * {@code getMoreResults() == false && getUpdateCount() == -1}; a
     * {@code getMoreResults()} that leaves the update count pinned at 0 spins
     * forever, accumulating a result per iteration until the client OOMs. This
     * is the regression test for that fix, which had none.
     */
    @Test
    void getMoreResultsDrainLoop_terminates() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            assertTrue(st.execute("SELECT id FROM stc_t WHERE id = 1"));
            int guard = 0;
            while (st.getMoreResults() || st.getUpdateCount() != -1) {
                if (++guard > 100) fail("getMoreResults() drain loop did not terminate");
            }
            assertEquals(-1, st.getUpdateCount());
            assertNull(st.getResultSet());
            // Same for the update shape.
            assertFalse(st.execute("INSERT INTO stc_t VALUES (930, 'm')"));
            assertEquals(1, st.getUpdateCount());
            guard = 0;
            while (st.getMoreResults() || st.getUpdateCount() != -1) {
                if (++guard > 100) fail("getMoreResults() drain loop did not terminate");
            }
        }
    }

    // ── parameter binding bounds ──────────────────────────────────────────
    @Test
    void parameterIndexOutOfRange_isRefused() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             PreparedStatement ps = c.prepareStatement("SELECT id FROM stc_t WHERE id = ?")) {
            assertThrows(SQLException.class, () -> ps.setInt(0, 1));
            assertThrows(SQLException.class, () -> ps.setInt(2, 1));
            assertThrows(SQLException.class, () -> ps.setInt(-1, 1));
            // POSITIVE CONTROL
            ps.setInt(1, 5);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
            }
        }
    }

    /**
     * {@code clearParameters()} must actually unbind, and a re-executed
     * statement must use the NEW binding rather than a stale one.
     */
    @Test
    void clearParametersAndRebind() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             PreparedStatement ps = c.prepareStatement("SELECT v FROM stc_t WHERE id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("v1", rs.getString(1));
            }
            ps.setInt(1, 2);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("v2", rs.getString(1),
                        "re-execution used a stale parameter binding");
            }
            ps.clearParameters();
            ps.setNull(1, Types.INTEGER);
            try (ResultSet rs = ps.executeQuery()) {
                assertFalse(rs.next(), "id = NULL must match nothing");
            }
        }
    }

    // ── generated keys: the answer must match the advertisement ───────────
    /**
     * {@code DatabaseMetaData.supportsGetGeneratedKeys()} is false, so
     * {@code getGeneratedKeys()} returns an EMPTY result rather than keys. What
     * must not happen is the two disagreeing - a framework that believes keys
     * are available will read column 1 of an empty result set and get an
     * exception at a point far from the cause.
     */
    @Test
    void generatedKeys_agreeWithTheMetadata() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            assertFalse(c.getMetaData().supportsGetGeneratedKeys());
            st.executeUpdate("INSERT INTO stc_t VALUES (940, 'k')",
                    Statement.RETURN_GENERATED_KEYS);
            try (ResultSet keys = st.getGeneratedKeys()) {
                assertNotNull(keys, "getGeneratedKeys() must never return null");
                assertFalse(keys.next(),
                        "no keys are available, so the result must be empty");
            }
            // POSITIVE CONTROL: the row itself really was inserted, so the empty
            // key set is about key RETRIEVAL and not about a failed insert.
            assertEquals(1, count(c, "SELECT id FROM stc_t WHERE id = 940"));
        }
    }

    // ── ResultSet type/concurrency advertisement must be honest ───────────
    /**
     * {@code createStatement(TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE)} returns
     * a forward-only read-only statement. That is legal ONLY if the metadata
     * says so and the statement reports what it actually is - otherwise an
     * application that asked for an updatable scrollable cursor gets a
     * forward-only one and finds out at {@code rs.updateRow()}.
     */
    @Test
    void resultSetTypeAndConcurrency_areReportedHonestly() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            assertTrue(c.getMetaData().supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
            assertFalse(c.getMetaData().supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
            assertTrue(c.getMetaData().supportsResultSetConcurrency(
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
            assertFalse(c.getMetaData().supportsResultSetConcurrency(
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
            try (Statement st = c.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
                assertEquals(ResultSet.TYPE_FORWARD_ONLY, st.getResultSetType());
                assertEquals(ResultSet.CONCUR_READ_ONLY, st.getResultSetConcurrency());
                try (ResultSet rs = st.executeQuery("SELECT id FROM stc_t WHERE id = 1")) {
                    assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
                    assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
                    assertTrue(rs.next());
                    assertThrows(SQLException.class, () -> rs.updateInt(1, 99));
                    assertThrows(SQLException.class, rs::updateRow);
                }
            }
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────
    private static int drain(ResultSet rs) throws SQLException {
        try (ResultSet r = rs) {
            int n = 0;
            while (r.next()) n++;
            return n;
        }
    }

    private static int count(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = 0;
            while (rs.next()) n++;
            return n;
        }
    }
}
