package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@code set_config('search_path', …)} over the NATIVE wire, end to end.
 *
 * <h2>Why this class had to be written before the bug could be seen</h2>
 *
 * No JDBC test in this repo had ever created a non-{@code public} schema
 * ({@code git grep 'CREATE SCHEMA' src/test} was 0 hits), so
 * every assertion about {@code search_path} was made in a database with exactly
 * one schema - where "the pin took" and "the pin was silently dropped" produce
 * byte-identical answers. That is the entire reason a GUC writer that wrote
 * nothing could sit in the driver being echoed back to its caller.
 *
 * <h2>The defect</h2>
 *
 * {@link PgCatalogAdapter} rewrote every {@code set_config(name, value, local)}
 * call into its second argument - the VALUE - on the theory (stated in a code
 * comment) that "ParticleDB is single-schema, so the write is a semantic
 * no-op". That has not been true since the relation resolver started walking
 * {@code search_path}: the server's session-settings layer files
 * {@code search_path} as {@code Honoured} and every wire mirrors it into the
 * engine per message. So the call returned the caller's own string, the caller
 * concluded the pin had taken, and {@code current_schema()} went on answering
 * {@code public}.
 *
 * <p>Flyway's {@code changeCurrentSchemaTo} is exactly this call. Under
 * {@code flyway.schemas=fw} it pins the path, then issues unqualified
 * {@code CREATE TABLE} for every migration - so the pin being dropped is not a
 * cosmetic GUC bug, it is every migration landing in the wrong schema.
 *
 * <h2>What the assertions ask</h2>
 *
 * NEVER the return value alone. The old behaviour returned the right value and
 * did nothing, so a test that only checked the echo passed against the bug.
 * Every case here asks the SERVER what it believes - {@code current_schema()},
 * and where a relation actually landed - and the echo is asserted only
 * alongside it.
 *
 * <p>Skipped via {@link Assumptions} when the {@code particledb} binary is absent.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SearchPathSetConfigTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping set_config search_path validation");
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private String url() {
        return "jdbc:particledb://127.0.0.1:" + server.port + "/test?preferUnixSocket=false";
    }

    private static String scalar(Statement st, String sql) throws SQLException {
        try (ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), sql + " returned no row");
            return rs.getString(1);
        }
    }

    // ── the defect, as Flyway issues it ─────────────────────────────────────

    /**
     * {@code SELECT set_config('search_path', 'fw', false)} must MOVE
     * {@code current_schema()}, not merely echo its argument.
     *
     * <p>PREDICTION before the fix: FAIL - the echo is {@code fw} and
     * {@code current_schema()} is {@code public}.
     */
    @Test
    void set_config_search_path_is_honoured_not_just_echoed() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS fw");
            assertEquals("public", scalar(st, "SELECT current_schema()"),
                    "precondition: a fresh session starts on public");

            String echoed = scalar(st, "SELECT set_config('search_path','fw',false)");
            assertEquals("fw", echoed,
                    "set_config must still RETURN the value it set — that half was "
                    + "never broken and must not regress");
            assertEquals("fw", scalar(st, "SELECT current_schema()"),
                    "set_config('search_path', …) did not move current_schema(): the "
                    + "driver dropped the write and handed back the caller's own string");
        }
    }

    /**
     * The write is real enough to place a RELATION - the property Flyway
     * actually depends on.
     *
     * <p>An unqualified {@code CREATE TABLE} after the pin must land in the
     * pinned schema, and {@code public} must not acquire a relation of that
     * name. Asserted by QUALIFIED reachability in both directions, never by an
     * unqualified {@code SELECT}: the resolver falls back to {@code public}, so
     * a table wrongly created there is still readable by the bare name, and an
     * unqualified read passes against the bug.
     *
     * <p>It is deliberately NOT asserted through
     * {@code information_schema.tables}. MEASURED on this server (2026-09-04,
     * PG wire): a relation placed in a non-{@code public} schema does not
     * appear there at all - {@code information_schema.tables} enumerated only
     * {@code public}. That is a separate, still-open gap in the catalog
     * enumerations; using it here would make this case fail for a reason that
     * has nothing to do with the GUC write.
     *
     * <p>PREDICTION before the fix: FAIL - the table is in {@code public}.
     */
    @Test
    void a_pinned_session_creates_its_tables_in_the_pinned_schema() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS fwplace");
            st.executeQuery("SELECT set_config('search_path','fwplace',false)").close();
            st.execute("CREATE TABLE fw_migration_marker (id INT)");

            assertEquals("0", scalar(st, "SELECT count(*) FROM fwplace.fw_migration_marker"),
                    "the migration's table did not land in the pinned schema");
            assertThrows(SQLException.class,
                    () -> st.executeQuery("SELECT count(*) FROM public.fw_migration_marker"),
                    "the migration's table landed in public — two tenants' migrations "
                    + "commingle there and nothing says so");
        }
    }

    /**
     * A bound parameter is the shape Flyway actually sends
     * ({@code "SELECT set_config('search_path', ?, false)"}), so the rendered
     * form the driver produces must be honoured too - not only a hand-written
     * literal.
     *
     * <p>PREDICTION before the fix: FAIL.
     */
    @Test
    void the_prepared_parameterised_form_flyway_sends_is_honoured() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS fwparam");
        }
        try (Connection c = DriverManager.getConnection(url());
             java.sql.PreparedStatement ps =
                     c.prepareStatement("SELECT set_config('search_path', ?, false)")) {
            ps.setString(1, "fwparam");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "set_config returned no row");
                assertEquals("fwparam", rs.getString(1));
            }
            try (Statement st = c.createStatement()) {
                assertEquals("fwparam", scalar(st, "SELECT current_schema()"),
                        "the parameterised form was dropped");
            }
        }
    }

    /**
     * The pin is PER CONNECTION. A GUC write that leaked across connections
     * would be a worse bug than the one being fixed - one tenant's migration
     * silently retargeting another's session.
     */
    @Test
    void the_pin_does_not_leak_to_another_connection() throws Exception {
        try (Connection a = DriverManager.getConnection(url());
             Connection b = DriverManager.getConnection(url());
             Statement sa = a.createStatement();
             Statement sb = b.createStatement()) {
            sa.execute("CREATE SCHEMA IF NOT EXISTS fwleak");
            sa.executeQuery("SELECT set_config('search_path','fwleak',false)").close();
            assertEquals("fwleak", scalar(sa, "SELECT current_schema()"));
            assertEquals("public", scalar(sb, "SELECT current_schema()"),
                    "connection A's search_path decided connection B's answer");
        }
    }

    /**
     * {@link PdbConnection#setSchema(String)} - which emits a plain {@code SET}
     * and was ALREADY correct - and {@code set_config} must agree. They are two
     * spellings of one request; the whole defect was that they disagreed.
     */
    @Test
    void set_schema_and_set_config_agree() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS fwboth");
            c.setSchema("fwboth");
            String viaSetSchema = scalar(st, "SELECT current_schema()");
            assertEquals("fwboth", viaSetSchema);

            // Back to public, then the same request through the other spelling.
            st.executeQuery("SELECT set_config('search_path','public',false)").close();
            assertEquals("public", scalar(st, "SELECT current_schema()"));
            st.executeQuery("SELECT set_config('search_path','fwboth',false)").close();
            assertEquals(viaSetSchema, scalar(st, "SELECT current_schema()"),
                    "setSchema() and set_config() must reach the same session state");
        }
    }

    /**
     * {@code set_config(…, true)} - the {@code SET LOCAL} spelling - reaches
     * the server's session store rather than being swallowed.
     *
     * <h2>Why this asserts {@code SHOW} and not {@code current_schema()}</h2>
     *
     * {@code SET LOCAL search_path} is broken SERVER-side, on both wires, and
     * that is a different defect from the one this class is about. MEASURED
     * 2026-09-04 over the PG wire (which has had its own {@code set_config}
     * intercept all along), so the driver is not in the picture at all:
     *
     * <pre>{@code
     * BEGIN;
     * SET LOCAL search_path TO 'fwlocal';
     * SELECT current_schema();   -- public     <-- wrong
     * SHOW search_path;          -- fwlocal    <-- right
     * COMMIT;
     * }</pre>
     *
     * The session store takes the {@code LOCAL} write ({@code SessionGucs::get}
     * prefers {@code local_vars}) but the per-message mirror into the engine
     * does not carry it, so the resolver and {@code current_schema()} keep
     * answering from the SESSION value. Asserting {@code current_schema()} here
     * would make this case fail for that reason instead of for the driver's,
     * so it asserts the surface the driver's half is observable on and the
     * server defect is recorded above rather than silently worked around.
     *
     * <p>The assertion is still discriminating: before the fix the driver
     * replaced the whole call with the literal {@code 'fwlocal'} and wrote
     * nothing, so {@code SHOW search_path} answered the default.
     */
    @Test
    void the_is_local_shape_is_translated_too() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE SCHEMA IF NOT EXISTS fwlocal");
            }
            c.setAutoCommit(false);
            try (Statement st = c.createStatement()) {
                assertEquals("fwlocal",
                        scalar(st, "SELECT set_config('search_path','fwlocal',true)"),
                        "set_config must return the value now in effect");
                assertEquals("fwlocal", scalar(st, "SHOW search_path"),
                        "the is_local=true form never reached the session store");
            }
            c.commit();
            c.setAutoCommit(true);
        }
    }

    /**
     * {@code set_config(name, NULL, …)} is PostgreSQL's spelling of
     * {@code RESET name} - not "set it to the empty string" - and it must
     * return the value that is now in effect, which after a reset is the
     * DEFAULT, not the null the caller passed.
     *
     * <p>Measured on this server's PG wire, which already had the intercept:
     * {@code SELECT set_config('search_path', NULL, false)} returns
     * {@code "$user", public}. The native wire must agree.
     */
    @Test
    void the_null_reset_shape_resets_and_reports_the_default() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS fwreset");
            st.executeQuery("SELECT set_config('search_path','fwreset',false)").close();
            assertEquals("fwreset", scalar(st, "SELECT current_schema()"));

            String afterReset = scalar(st, "SELECT set_config('search_path', NULL, false)");
            assertEquals("\"$user\", public", afterReset,
                    "the NULL form must report the value now in effect (the default), "
                    + "not the null it was handed");
            assertEquals("public", scalar(st, "SELECT current_schema()"),
                    "set_config(name, NULL, …) must RESET the parameter");
        }
    }
}
