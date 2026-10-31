package org.particledb;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Orkes Conductor OSS boots by running its own Flyway migrations through this
 * driver (its {@code flywayForPrimaryDb} bean). This runs THOSE migrations --
 * {@code src/test/resources/db/conductor}, copied verbatim from Conductor's
 * {@code postgres-persistence} -- with Flyway at Conductor's pinned version and
 * with Conductor's configuration, over connections shaped the way Conductor's
 * Hikari pool hands them out ({@code auto-commit=false}, READ COMMITTED).
 *
 * <p>Two transactions are involved, and only one can be configured away. Each
 * MIGRATION can be made non-transactional with Flyway's per-script
 * {@code <script>.conf} ({@code executeInTransaction=false}). Flyway's OWN schema-history table, however, is always
 * created inside a transaction ({@code setAutoCommit(false)}; CREATE TABLE;
 * ALTER TABLE ... ADD CONSTRAINT; CREATE INDEX; {@code commit()}), and no
 * setting changes that.
 *
 * <p>The regression this pins ({@code flywayForPrimaryDb}
 * failed {@code 25001} on {@code CREATE TABLE "public"."flyway_schema_history"}):
 * the driver used to run a DDL issued while no transaction was open outside
 * one, with a SQLWarning, so those three history-table statements ran. A later
 * driver change made every such DDL open the transaction and be refused, and
 * Conductor could no longer boot even with non-transactional migrations. {@code allowDdlOutsideTransaction=true} restores that behaviour as
 * an explicit choice; without it the strict refusal stays the default. Every
 * outcome is judged from a SECOND, autocommit connection.
 */
public class ConductorFlywayMigrationTest {

    /** What Conductor's migrate() applies by default (13 schema + 1 data). */
    private static final int CONDUCTOR_MIGRATIONS = 14;

    private PdbTestServer server;
    private Path migrations;

    @BeforeAll
    static void loadDriver() throws Exception {
        Class.forName("org.particledb.Driver");
    }

    @BeforeEach
    void start() throws Exception {
        // A fresh server per test: each one migrates an empty database.
        // Pinned to `--transactional-ddl off`: this suite asserts the refusal of
        // DDL inside a transaction (25001), the server default in earlier releases.
        server = PdbTestServer.tryStart("--transactional-ddl", "off");
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping Conductor Flyway migration test");
    }

    @AfterEach
    void stop() throws IOException {
        if (server != null) server.stop();
        if (migrations != null) {
            try (Stream<Path> w = Files.walk(migrations)) {
                w.sorted(Comparator.reverseOrder()).forEach(f -> f.toFile().delete());
            }
        }
    }

    private String url(String extra) {
        return "jdbc:particledb://127.0.0.1:" + server.port + "/test?preferUnixSocket=false" + extra;
    }

    /** Connections as Conductor's Hikari pool configures them. */
    private static final class ConductorPoolLike implements DataSource {
        private final String url;
        ConductorPoolLike(String url) { this.url = url; }
        @Override public Connection getConnection() throws SQLException {
            Connection c = DriverManager.getConnection(url);
            c.setAutoCommit(false);                                   // hikari.auto-commit=false
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            return c;
        }
        @Override public Connection getConnection(String u, String p) throws SQLException {
            return getConnection();
        }
        @Override public PrintWriter getLogWriter() { return null; }
        @Override public void setLogWriter(PrintWriter out) { }
        @Override public void setLoginTimeout(int seconds) { }
        @Override public int getLoginTimeout() { return 0; }
        @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
        @Override public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("not a wrapper");
        }
        @Override public boolean isWrapperFor(Class<?> iface) { return false; }
    }

    /**
     * Conductor's PostgresConfiguration#flywayForPrimaryDb with default
     * properties, over Conductor's migrations copied to a scratch directory --
     * with the rig's overlay when asked: a {@code <script>.conf} holding
     * {@code executeInTransaction=false} next to every script, as
     * {@code flyway_overlay.sh} writes it.
     */
    private Flyway conductorFlyway(DataSource ds, boolean nonTransactionalOverlay)
            throws IOException, URISyntaxException {
        Path src = Paths.get(ConductorFlywayMigrationTest.class.getClassLoader()
                .getResource("db/conductor").toURI());
        migrations = Files.createTempDirectory("conductor-flyway");
        String[] dirs = {"migration_postgres", "migration_postgres_data"};
        String[] locations = new String[dirs.length];
        for (int i = 0; i < dirs.length; i++) {
            Path to = Files.createDirectories(migrations.resolve(dirs[i]));
            List<Path> scripts;
            try (Stream<Path> files = Files.list(src.resolve(dirs[i]))) {
                scripts = files.filter(f -> f.toString().endsWith(".sql")).collect(Collectors.toList());
            }
            for (Path f : scripts) {
                Files.copy(f, to.resolve(f.getFileName()));
                if (nonTransactionalOverlay) {
                    Files.writeString(to.resolve(f.getFileName() + ".conf"),
                            "executeInTransaction=false\n");
                }
            }
            locations[i] = "filesystem:" + to;
        }
        return Flyway.configure(ConductorFlywayMigrationTest.class.getClassLoader())
                .locations(locations)
                .configuration(Map.of("flyway.postgresql.transactional.lock", "false"))
                .schemas("public")
                .dataSource(ds)
                .outOfOrder(true)
                .baselineOnMigrate(true)
                .load();
    }

    private Connection observer() throws SQLException {
        return DriverManager.getConnection(url(""));
    }

    private static long scalar(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getLong(1);
        }
    }

    private static boolean tableExists(Connection c, String table) {
        try (Statement st = c.createStatement()) {
            st.executeQuery("SELECT count(*) FROM " + table).close();
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

    private static SQLException sqlCause(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof SQLException) return (SQLException) c;
        }
        return null;
    }

    /**
     * The rig's bring-up, end to end: Conductor's migrations with the rig's
     * non-transactional overlay, Conductor's Flyway configuration,
     * auto-commit-off connections, and the option. Every migration applies and
     * is recorded as successful, the schema the later ones depend on is there,
     * and a second migrate() -- a Conductor restart -- finds nothing to do. On
     * a driver without the option this fails exactly as Conductor's boot did:
     * 25001 on the history-table CREATE, before any migration runs.
     */
    @Test
    void conductorBootsWithTheOverlayAndAllowDdlOutsideTransaction() throws Exception {
        DataSource ds = new ConductorPoolLike(url("&allowDdlOutsideTransaction=true"));
        Flyway flyway = conductorFlyway(ds, true);
        MigrateResult r = flyway.migrate();
        assertTrue(r.success, "Flyway migrate() did not succeed: " + r.warnings);
        assertEquals(CONDUCTOR_MIGRATIONS, r.migrationsExecuted,
                "every Conductor migration (schema + data) must apply");

        try (Connection o = observer()) {
            assertEquals(CONDUCTOR_MIGRATIONS, scalar(o,
                    "SELECT count(*) FROM flyway_schema_history WHERE success = true"
                    + " AND version IS NOT NULL"));
            assertEquals(0, scalar(o,
                    "SELECT count(*) FROM flyway_schema_history WHERE success = false"));
            // Tables from the first and the last migrations, and a column a
            // late migration ADDs to a table an early one CREATEs.
            for (String t : new String[]{"meta_event_handler", "meta_task_def", "workflow",
                    "task", "queue", "queue_message", "workflow_index", "task_index", "locks"}) {
                assertTrue(tableExists(o, t), "Conductor table missing after migrate(): " + t);
            }
            assertTrue(columnExists(o, "workflow_index", "update_time"),
                    "V13.1 column missing: the migrations did not all apply in order");
        }

        // Conductor restart: validate against the recorded history, apply nothing.
        MigrateResult again = flyway.migrate();
        assertTrue(again.success);
        assertEquals(0, again.migrationsExecuted, "a re-run must find every migration applied");
    }

    /**
     * Without the option the strict default stands, overlay or not: the first
     * DDL Flyway sends (its history table) is refused 25001 and nothing is
     * created. This is the rig's failure; it is pinned so that a change of
     * default is a deliberate, visible decision.
     */
    @Test
    void withoutTheOptionTheHistoryTableIsRefused25001AndNothingIsCreated() throws Exception {
        DataSource ds = new ConductorPoolLike(url(""));
        Flyway flyway = conductorFlyway(ds, true);
        Exception e = assertThrows(Exception.class, flyway::migrate);
        SQLException se = sqlCause(e);
        assertNotNull(se, "expected an SQLException cause: " + e);
        assertEquals("25001", se.getSQLState(), se.getMessage());
        // Flyway names the statement only at debug level; that it was the
        // history-table CREATE is proven by the table's absence below (every
        // migration's failure would come after it exists).
        try (Connection o = observer()) {
            assertFalse(tableExists(o, "flyway_schema_history"));
            assertFalse(tableExists(o, "meta_event_handler"));
        }
    }

    /**
     * The option is not transactional DDL. Without the overlay Flyway runs V1
     * inside a transaction whose first statements are Flyway's own (SET ROLE,
     * a pg_namespace probe, set_config): the transaction is already open when
     * V1's first CREATE arrives, so it is refused 25001 -- never run after an
     * early commit, never silently half-applied. Only the history table,
     * created before, exists.
     */
    @Test
    void theOptionDoesNotRunATransactionalMigrationItIsRefused() throws Exception {
        DataSource ds = new ConductorPoolLike(url("&allowDdlOutsideTransaction=true"));
        Flyway flyway = conductorFlyway(ds, false);
        Exception e = assertThrows(Exception.class, flyway::migrate);
        SQLException se = sqlCause(e);
        assertNotNull(se, "expected an SQLException cause: " + e);
        assertEquals("25001", se.getSQLState(), se.getMessage());
        try (Connection o = observer()) {
            assertTrue(tableExists(o, "flyway_schema_history"));
            assertFalse(tableExists(o, "meta_event_handler"),
                    "V1's first CREATE ran although its transaction was open");
            assertEquals(0, scalar(o, "SELECT count(*) FROM flyway_schema_history"
                    + " WHERE version IS NOT NULL AND success = true"));
        }
    }

    /**
     * The option's exact contract, with plain JDBC: a DDL with no transaction
     * open runs on its own and the connection carries a SQLWarning saying a
     * rollback() will not undo it (it does not); the next statement opens the
     * transaction; a DDL inside that open transaction is STILL refused 25001
     * and the transaction's DML rolls back -- the caller's work is never
     * committed to make room for a DDL.
     */
    @Test
    void theOptionRunsOnlyANoTransactionDdlOutsideAndSaysSo() throws Exception {
        try (Connection c = DriverManager.getConnection(url("&allowDdlOutsideTransaction=true"));
             Connection o = observer()) {
            c.setAutoCommit(false);
            c.clearWarnings();
            exec(c, "CREATE TABLE ado_a (id BIGINT PRIMARY KEY)");
            SQLWarning w = c.getWarnings();
            assertNotNull(w, "a DDL run outside the transaction must leave a SQLWarning");
            assertEquals("01000", w.getSQLState());
            assertTrue(w.getMessage().contains("rollback() will not undo it"), w.getMessage());
            assertFalse(c.unwrap(PdbConnection.class).inServerTransaction(),
                    "the DDL must not have opened a transaction");
            assertTrue(tableExists(o, "ado_a"), "the DDL took effect at once");

            exec(c, "INSERT INTO ado_a VALUES (1)");
            assertTrue(c.unwrap(PdbConnection.class).inServerTransaction(),
                    "the statement after the DDL must open the transaction");
            SQLException refused = assertThrows(SQLException.class,
                    () -> exec(c, "CREATE TABLE ado_b (id INT)"));
            assertEquals("25001", refused.getSQLState(), refused.getMessage());
            c.rollback();

            assertTrue(tableExists(o, "ado_a"), "documented: rollback() cannot undo the DDL");
            assertEquals(0, scalar(o, "SELECT count(*) FROM ado_a"),
                    "the transaction's DML must roll back");
            assertFalse(tableExists(o, "ado_b"), "a DDL inside an open transaction ran");

            // After rollback() the next first-statement DDL again runs outside.
            exec(c, "CREATE TABLE ado_c (id INT)");
            assertTrue(tableExists(o, "ado_c"));
        }
    }
}
