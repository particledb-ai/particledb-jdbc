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
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Batch insert followed by a full read-back: every batched row must be
 * present with the correct values (not just "executeBatch returned an array").
 *
 * <p>Skipped via {@link Assumptions} when the {@code particledb} binary is
 * absent.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BatchReadBackTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping batch read-back test");
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
    void batchInsertThenSelectAllRows() throws Exception {
        final int n = 25;
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE batch_rb (id INT PRIMARY KEY, v INT)");
            }
            try (PreparedStatement ps =
                         c.prepareStatement("INSERT INTO batch_rb VALUES (?, ?)")) {
                for (int i = 0; i < n; i++) {
                    ps.setInt(1, i);
                    ps.setInt(2, i * 3);
                    ps.addBatch();
                }
                int[] r = ps.executeBatch();
                assertEquals(n, r.length, "executeBatch() returned wrong array length");
            }

            // Read back ALL rows and assert every batched value is present.
            boolean[] seen = new boolean[n];
            int count = 0;
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT id, v FROM batch_rb ORDER BY id")) {
                while (rs.next()) {
                    int id = rs.getInt("id");
                    int v = rs.getInt("v");
                    assertTrue(id >= 0 && id < n, "unexpected id " + id);
                    assertEquals(id * 3, v, "wrong value for id " + id);
                    assertFalse(seen[id], "duplicate id " + id);
                    seen[id] = true;
                    count++;
                }
            }
            assertEquals(n, count, "batch read-back returned wrong row count");
            for (int i = 0; i < n; i++) {
                assertTrue(seen[i], "batched row id=" + i + " was LOST");
            }
        }
    }

    @Test
    void statementLevelBatchInsertThenSelectAllRows() throws Exception {
        final int n = 20;
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE batch_st (id INT PRIMARY KEY, v INT)");
            }
            try (Statement st = c.createStatement()) {
                for (int i = 0; i < n; i++) {
                    st.addBatch("INSERT INTO batch_st VALUES (" + i + ", " + (i * 5) + ")");
                }
                int[] r = st.executeBatch();
                assertEquals(n, r.length, "executeBatch() returned wrong array length");
            }

            boolean[] seen = new boolean[n];
            int count = 0;
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT id, v FROM batch_st ORDER BY id")) {
                while (rs.next()) {
                    int id = rs.getInt("id");
                    assertEquals(id * 5, rs.getInt("v"), "wrong value for id " + id);
                    seen[id] = true;
                    count++;
                }
            }
            assertEquals(n, count, "statement-batch read-back returned wrong row count");
            for (int i = 0; i < n; i++) {
                assertTrue(seen[i], "statement-batched row id=" + i + " was LOST");
            }
        }
    }
}
