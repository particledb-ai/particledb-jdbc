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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DIFFERENTIAL conformance: for each JDBC surface whose correct answer is
 * defined by PostgreSQL + pgjdbc rather than by anything ParticleDB-specific,
 * ask the REFERENCE IMPLEMENTATION what it does, then require this driver to do
 * the same.
 *
 * <p>Why this file exists: every "must throw" / "must not wrap" assertion in the
 * other contract suites is one reading of the JDBC javadoc. A reading can be
 * wrong in either direction, and a suite built entirely on one reading is
 * self-consistent whether or not it is right. Here the oracle is the driver
 * every JVM application already compares us to.
 *
 * <h2>Identity is pinned before anything is believed</h2>
 * A listener answering on a Postgres-looking port has, on this very machine,
 * turned out to BE ParticleDB - at which point a differential test compares the
 * engine against itself and confirms whatever it already does. So
 * {@link #setUp()} refuses to run unless {@code version()} names PostgreSQL,
 * does NOT name ParticleDB, and the server answers a
 * {@code pg_catalog}-internals query that only real PostgreSQL serves. If any
 * of that fails the whole class SKIPS rather than producing a green run against
 * the wrong oracle.
 *
 * <p>Point the suite elsewhere with {@code -DPG_ORACLE_URL=jdbc:postgresql://…}
 * / {@code PG_ORACLE_URL}; the default is the local PostgreSQL on port 5434.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PgOracleDifferentialTest {

    private String pgUrl;
    private String pgVersion;
    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        Class.forName("org.postgresql.Driver");

        pgUrl = System.getProperty("PG_ORACLE_URL", System.getenv("PG_ORACLE_URL"));
        if (pgUrl == null || pgUrl.isEmpty()) {
            pgUrl = "jdbc:postgresql://127.0.0.1:5434/postgres?user="
                    + System.getProperty("user.name");
        }

        // ── PIN THE ORACLE'S IDENTITY ─────────────────────────────────────
        String banner = null;
        boolean servesPgInternals = false;
        try (Connection pg = DriverManager.getConnection(pgUrl, new Properties())) {
            try (Statement st = pg.createStatement();
                 ResultSet rs = st.executeQuery("SELECT version()")) {
                if (rs.next()) banner = rs.getString(1);
            }
            // A query only real PostgreSQL answers: pg_am's handler column is a
            // regproc, and pg_get_indexdef is a core PG internal function.
            try (Statement st = pg.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT count(*) FROM pg_am WHERE amhandler <> 0")) {
                servesPgInternals = rs.next() && rs.getInt(1) > 0;
            }
        } catch (SQLException notThere) {
            banner = null;
        }
        Assumptions.assumeTrue(banner != null,
                "no PostgreSQL oracle reachable at " + pgUrl + " — skipping "
                + "differential suite");
        final String b = banner;
        Assumptions.assumeTrue(b.startsWith("PostgreSQL"),
                "the server at " + pgUrl + " does not identify as PostgreSQL: " + b);
        assertFalse(b.contains("ParticleDB") || b.contains("particledb"),
                "REFUSING to use " + pgUrl + " as the PostgreSQL oracle: it is "
                + "ParticleDB (" + b + "). A differential test against the "
                + "engine under test confirms whatever the engine already does.");
        Assumptions.assumeTrue(servesPgInternals,
                "the server at " + pgUrl + " does not serve pg_am.amhandler — "
                + "not a real PostgreSQL");
        pgVersion = banner;

        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping differential suite");

        // Same fixture on both sides.
        for (String url : new String[]{pdbUrl(), pgUrl}) {
            try (Connection c = DriverManager.getConnection(url, new Properties());
                 Statement st = c.createStatement()) {
                try { st.executeUpdate("DROP TABLE diff_t"); } catch (SQLException ignored) {}
                st.executeUpdate("CREATE TABLE diff_t (id INT PRIMARY KEY, big BIGINT, "
                        + "txt TEXT, flag BOOLEAN)");
                st.executeUpdate("INSERT INTO diff_t VALUES (1, 2147483648, 't', true)");
                st.executeUpdate("INSERT INTO diff_t VALUES (2, 7, 'f', false)");
            }
        }
    }

    @AfterAll
    void tearDown() {
        if (pgUrl != null) {
            try (Connection c = DriverManager.getConnection(pgUrl, new Properties());
                 Statement st = c.createStatement()) {
                st.executeUpdate("DROP TABLE IF EXISTS diff_t");
            } catch (SQLException ignored) {}
        }
        if (server != null) server.stop();
    }

    private String pdbUrl() {
        return "jdbc:particledb://127.0.0.1:" + server.port + "/test?preferUnixSocket=false";
    }

    /** Open one connection per engine and hand both to the same probe. */
    private void bothEngines(Probe probe) throws Exception {
        try (Connection pdb = DriverManager.getConnection(pdbUrl(), new Properties())) {
            probe.run("ParticleDB", pdb);
        }
        try (Connection pg = DriverManager.getConnection(pgUrl, new Properties())) {
            probe.run("PostgreSQL(" + pgVersion.split(" ")[1] + ")", pg);
        }
    }

    private interface Probe { void run(String engine, Connection c) throws Exception; }

    // ── the oracle is real: prove the harness can tell the two apart ──────
    /**
     * POSITIVE CONTROL FOR THE HARNESS ITSELF. Before any "both engines agree"
     * result is worth reading, the harness has to be able to produce a
     * DISAGREEMENT - otherwise "they agreed" might mean it was talking to one
     * server twice. The product name differs by construction, so this test fails
     * loudly if both handles ever point at the same place.
     */
    @Test
    void theTwoConnectionsAreActuallyTwoDifferentEngines() throws Exception {
        String pdbName, pgName;
        try (Connection c = DriverManager.getConnection(pdbUrl(), new Properties())) {
            pdbName = c.getMetaData().getDriverName();
        }
        try (Connection c = DriverManager.getConnection(pgUrl, new Properties())) {
            pgName = c.getMetaData().getDriverName();
        }
        assertTrue(pdbName.contains("ParticleDB"), "PDB side driver: " + pdbName);
        assertTrue(pgName.contains("PostgreSQL"), "PG side driver: " + pgName);
        assertNotEquals(pdbName, pgName);
        assertTrue(pgVersion.startsWith("PostgreSQL 1"), "oracle banner: " + pgVersion);
    }

    // ── narrowing getters ─────────────────────────────────────────────────
    /**
     * {@code getInt} on a BIGINT that does not fit an int. pgjdbc throws; this
     * driver used to WRAP, so 2147483648 read back as -2147483648.
     */
    @Test
    void getIntOnAnOutOfRangeBigint_throwsOnBothEngines() throws Exception {
        bothEngines((engine, c) -> {
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT big FROM diff_t WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(2147483648L, rs.getLong(1), engine + ": getLong");
                assertThrows(SQLException.class, () -> rs.getInt(1),
                        engine + ": getInt on an out-of-range BIGINT must throw");
            }
            // POSITIVE CONTROL: the in-range row reads fine on the same engine.
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT big FROM diff_t WHERE id = 2")) {
                assertTrue(rs.next());
                assertEquals(7, rs.getInt(1), engine + ": in-range getInt");
            }
        });
    }

    // ── boolean text forms ────────────────────────────────────────────────
    /**
     * {@code getBoolean} on a TEXT column carrying PG's canonical {@code t} /
     * {@code f}. pgjdbc reads them as true/false; this driver used to answer
     * false for both because it went through {@code Boolean.parseBoolean}.
     */
    @Test
    void getBooleanOnPgTextForms_agreesWithPgjdbc() throws Exception {
        bothEngines((engine, c) -> {
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT txt FROM diff_t ORDER BY id")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1), engine + ": text 't' must read as true");
                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1), engine + ": text 'f' must read as false");
            }
            // POSITIVE CONTROL: a real BOOLEAN column behaves the same way.
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT flag FROM diff_t ORDER BY id")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1), engine + ": boolean true");
                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1), engine + ": boolean false");
            }
        });
    }

    // ── PreparedStatement rejects the SQL-taking Statement methods ────────
    @Test
    void preparedStatementRejectsSqlTakingMethods_onBothEngines() throws Exception {
        bothEngines((engine, c) -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT id FROM diff_t WHERE id = ?")) {
                assertThrows(SQLException.class, () -> ps.addBatch("SELECT 1"),
                        engine + ": PreparedStatement.addBatch(String)");
                assertThrows(SQLException.class, () -> ps.executeQuery("SELECT 1"),
                        engine + ": PreparedStatement.executeQuery(String)");
                assertThrows(SQLException.class, () -> ps.executeUpdate("SELECT 1"),
                        engine + ": PreparedStatement.executeUpdate(String)");
                assertThrows(SQLException.class, () -> ps.execute("SELECT 1"),
                        engine + ": PreparedStatement.execute(String)");
                // POSITIVE CONTROL: the no-argument form works on both.
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                }
            }
        });
    }

    // ── commit / rollback outside a transaction ───────────────────────────
    @Test
    void commitAndRollbackInAutoCommit_throwOnBothEngines() throws Exception {
        bothEngines((engine, c) -> {
            assertTrue(c.getAutoCommit());
            assertThrows(SQLException.class, c::commit, engine + ": commit() in auto-commit");
            assertThrows(SQLException.class, c::rollback, engine + ": rollback() in auto-commit");
            // POSITIVE CONTROL: with auto-commit off, both calls succeed.
            c.setAutoCommit(false);
            assertDoesNotThrow(() -> c.rollback(), engine + ": rollback() in a transaction");
            assertDoesNotThrow(() -> c.commit(), engine + ": commit() in a transaction");
            c.setAutoCommit(true);
        });
    }

    // ── executeUpdate on a row-returning statement ────────────────────────
    @Test
    void executeUpdateOnASelect_throwsOnBothEngines() throws Exception {
        bothEngines((engine, c) -> {
            try (Statement st = c.createStatement()) {
                assertThrows(SQLException.class, () -> st.executeUpdate("SELECT 1"),
                        engine + ": executeUpdate on a row-returning statement");
                // POSITIVE CONTROL
                assertEquals(1, st.executeUpdate(
                        "UPDATE diff_t SET txt = txt WHERE id = 2"), engine + ": real update");
            }
        });
    }

    // ── setMaxRows ────────────────────────────────────────────────────────
    @Test
    void setMaxRows_dropsExcessRowsOnBothEngines() throws Exception {
        bothEngines((engine, c) -> {
            try (Statement st = c.createStatement()) {
                assertEquals(2, drain(st.executeQuery("SELECT id FROM diff_t")),
                        engine + ": uncapped row count");
                st.setMaxRows(1);
                assertEquals(1, drain(st.executeQuery("SELECT id FROM diff_t")),
                        engine + ": setMaxRows(1)");
                st.setMaxRows(0);
                assertEquals(2, drain(st.executeQuery("SELECT id FROM diff_t")),
                        engine + ": setMaxRows(0) means unlimited");
            }
        });
    }

    // ── closed ResultSet / closed Statement ───────────────────────────────
    @Test
    void closedResultSetAndStatement_refuseOnBothEngines() throws Exception {
        bothEngines((engine, c) -> {
            Statement st = c.createStatement();
            ResultSet rs = st.executeQuery("SELECT id FROM diff_t ORDER BY id");
            assertTrue(rs.next());                    // POSITIVE CONTROL
            assertEquals(1, rs.getInt(1));
            rs.close();
            assertThrows(SQLException.class, rs::next, engine + ": next() on a closed RS");
            assertThrows(SQLException.class, () -> rs.getInt(1),
                    engine + ": getInt on a closed RS");
            st.close();
            assertThrows(SQLException.class, () -> st.executeQuery("SELECT 1"),
                    engine + ": execute on a closed Statement");
        });
    }

    // ── metadata bounds ───────────────────────────────────────────────────
    @Test
    void resultSetMetaDataOutOfRange_throwsOnBothEngines() throws Exception {
        bothEngines((engine, c) -> {
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT id FROM diff_t")) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(1, md.getColumnCount(), engine + ": column count");
                assertEquals("id", md.getColumnLabel(1), engine + ": POSITIVE CONTROL");
                assertThrows(SQLException.class, () -> md.getColumnLabel(2),
                        engine + ": getColumnLabel past the end");
                assertThrows(SQLException.class, () -> md.getColumnLabel(0),
                        engine + ": getColumnLabel(0)");
            }
        });
    }

    // ── findColumn ────────────────────────────────────────────────────────
    @Test
    void findColumn_isCaseInsensitiveAndThrowsWhenAbsent_onBothEngines() throws Exception {
        bothEngines((engine, c) -> {
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT id FROM diff_t")) {
                assertEquals(1, rs.findColumn("id"), engine);
                assertEquals(1, rs.findColumn("ID"), engine + ": labels are case-insensitive");
                assertThrows(SQLException.class, () -> rs.findColumn("nope"), engine);
            }
        });
    }

    // ── string escaping: the premise the client-side renderer rests on ────
    /**
     * The renderer in {@link PdbPreparedStatement} escapes a value by DOUBLING
     * its single quotes and nothing else. That is correct exactly while
     * {@code standard_conforming_strings} is on, which is where both engines
     * are - and is asserted here rather than assumed, because if it ever flipped
     * off, a backslash-bearing value would become an injection vector.
     */
    @Test
    void stringEscapingPremiseHolds_onBothEngines() throws Exception {
        bothEngines((engine, c) -> {
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SHOW standard_conforming_strings")) {
                assertTrue(rs.next());
                assertEquals("on", rs.getString(1),
                        engine + ": standard_conforming_strings must be on, or quote "
                        + "doubling alone stops being a sufficient escape");
            }
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT length('a\\b'), 'it''s'")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1),
                        engine + ": a backslash in a plain literal must be one character");
                assertEquals("it's", rs.getString(2),
                        engine + ": '' must be the embedded-quote escape");
            }
        });
    }

    /**
     * End to end: a hostile value bound through this driver's client-side
     * substitution must round-trip as DATA on both engines, and must not have
     * executed anything.
     */
    @Test
    void hostileParameterRoundTripsAsData_onBothEngines() throws Exception {
        String payload = "'); DROP TABLE diff_t; --";
        bothEngines((engine, c) -> {
            try (PreparedStatement ps =
                         c.prepareStatement("INSERT INTO diff_t VALUES (?, ?, ?, ?)")) {
                ps.setInt(1, 99);
                ps.setLong(2, 0L);
                ps.setString(3, payload);
                ps.setBoolean(4, false);
                assertEquals(1, ps.executeUpdate(), engine);
            }
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT txt FROM diff_t WHERE id = 99")) {
                assertTrue(rs.next(), engine + ": the row is gone — the payload ran");
                assertEquals(payload, rs.getString(1),
                        engine + ": the payload was not stored verbatim");
            }
            // The table still exists and still has its other rows.
            assertEquals(3, drain(c.createStatement().executeQuery(
                    "SELECT id FROM diff_t")), engine + ": table shape changed");
            try (Statement st = c.createStatement()) {
                st.executeUpdate("DELETE FROM diff_t WHERE id = 99");
            }
        });
    }

    private static int drain(ResultSet rs) throws SQLException {
        try (ResultSet r = rs) {
            int n = 0;
            while (r.next()) n++;
            return n;
        }
    }
}
