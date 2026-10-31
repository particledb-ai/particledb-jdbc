package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/** Verify {@code addBatch}/{@code executeBatch} round-trips correctly. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BatchExecutorTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping batch test");
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private String url() {
        return "jdbc:particledb://127.0.0.1:" + server.port + "/test"
                + "?preferUnixSocket=false";
    }

    @Test
    void prepared_statement_batch_round_trip() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE batched (id INT PRIMARY KEY, v INT)");
            }
            try (PreparedStatement ps =
                         c.prepareStatement("INSERT INTO batched VALUES (?, ?)")) {
                for (int i = 0; i < 10; i++) {
                    ps.setInt(1, i);
                    ps.setInt(2, i * 100);
                    ps.addBatch();
                }
                int[] r = ps.executeBatch();
                assertEquals(10, r.length);
            }
        }
    }

    @Test
    void low_level_BatchExecutor_works() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE low_batch (id INT PRIMARY KEY)");
            }
            BatchExecutor be = new BatchExecutor((PdbConnection) c);
            be.add("INSERT INTO low_batch VALUES (1)");
            be.add("INSERT INTO low_batch VALUES (2)");
            be.add("INSERT INTO low_batch VALUES (3)");
            assertEquals(3, be.size());
            be.execute();
            assertEquals(0, be.size());
        }
    }
}
