package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * LIVE: Conductor's {@code poll_data} BEFORE UPDATE trigger, raised over the
 * native wire, reaches the caller exactly as pgjdbc reports it from PostgreSQL  -
 * so Conductor's {@code startsWith("ERROR: lastPollTime …")} suppression works.
 *
 * <p>Table, function, trigger and UPDATE are Conductor's verbatim
 * ({@code V1__initial_schema.sql} + {@code V6__update_pk.sql} +
 * {@code V10__poll_data_check.sql}; {@code PostgresPollDataDAO}'s UPDATE text).
 * The decreasing update is deterministic - no concurrency needed to raise it;
 * concurrency only decides how OFTEN Conductor's wall-clock poll times arrive
 * out of order, and PostgreSQL 17 raises it on 33-64% of concurrent polls.
 *
 * <p>Server-free half: {@link ServerErrorMessageShapeTest}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PollDataTriggerWireTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping poll_data trigger wire test");
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE poll_data (created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                    + "modified_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP, queue_name varchar(255) NOT NULL, "
                    + "domain varchar(255) NOT NULL, json_data TEXT NOT NULL, PRIMARY KEY (queue_name, domain))");
            st.execute("CREATE OR REPLACE FUNCTION poll_data_update_check ()\n"
                    + "    RETURNS TRIGGER\n    AS $$\nBEGIN\n"
                    + "    IF(NEW.json_data::json ->> 'lastPollTime')::BIGINT < (OLD.json_data::json ->> 'lastPollTime')::BIGINT THEN\n"
                    + "        RAISE EXCEPTION 'lastPollTime cannot be set to a lower value';\n"
                    + "    END IF;\n    RETURN NEW;\nEND;\n$$\nLANGUAGE plpgsql");
            st.execute("CREATE TRIGGER poll_data_update_check_trigger BEFORE UPDATE ON poll_data "
                    + "FOR EACH ROW EXECUTE FUNCTION poll_data_update_check ()");
            st.executeUpdate("INSERT INTO poll_data (queue_name, domain, json_data) "
                    + "VALUES ('cbench_task', 'DEFAULT', '{\"lastPollTime\":2000}')");
        }
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private Connection conn() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:particledb://127.0.0.1:" + server.port + "/test?preferUnixSocket=false");
    }

    private static int update(Connection c, long lastPollTime) throws SQLException {
        try (PreparedStatement p = c.prepareStatement(
                "UPDATE poll_data SET json_data=?, modified_on=CURRENT_TIMESTAMP WHERE queue_name=? AND domain=?")) {
            p.setString(1, "{\"lastPollTime\":" + lastPollTime + "}");
            p.setString(2, "cbench_task");
            p.setString(3, "DEFAULT");
            return p.executeUpdate();
        }
    }

    @Test
    void decreasingPollTimeRaisesInPgjdbcShape() throws Exception {
        try (Connection c = conn()) {
            c.setAutoCommit(false);
            SQLException e = assertThrows(SQLException.class, () -> update(c, 1000));
            c.rollback();
            // Conductor's PostgresPollDataDAO.insertOrUpdatePollData predicate, verbatim.
            assertTrue(e.getMessage().startsWith("ERROR: lastPollTime cannot be set to a lower value"),
                    "Conductor would re-throw and log this: " + e.getMessage());
            assertEquals("P0001", e.getSQLState(), e.getMessage());

            // Positive control: the trigger passes a non-decreasing update, so the
            // raise above is the trigger's comparison, not a broken statement.
            assertEquals(1, update(c, 3000));
            c.commit();
        }
    }
}
