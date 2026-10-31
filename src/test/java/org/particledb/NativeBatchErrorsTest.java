package org.particledb;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Live-server check that a JDBC batch never reports a failed statement as
 * success: {@code Statement.executeBatch}, {@code PreparedStatement.executeBatch}
 * and {@link BatchExecutor#execute} raise the server's error (as a
 * {@link BatchUpdateException} carrying the update counts of the statements
 * that ran, for the JDBC calls), and a failed batch inside a manual-commit
 * transaction leaves the connection recoverable by {@code rollback()}.
 *
 * <p>Server: {@code PDB_NATIVE_PORT} (host 127.0.0.1, a {@code --no-auth}
 * server already running - what the step runner sets, using the binary it
 * started). Without it, the suite's own {@link PdbTestServer} starts the
 * binary named by {@code PDB_BIN} (and refuses to skip under
 * {@code -Dpdb.requireServer=true}); with neither, the test is skipped.
 * Prints {@code NBE_PASS jdbc} on success so the runner can prove it ran.
 */
public class NativeBatchErrorsTest {

    private static final String T = "nbe_jdbc";

    @Test
    void batchErrorsAreRaised() throws Exception {
        Class.forName("org.particledb.Driver");
        String portEnv = System.getenv("PDB_NATIVE_PORT");
        PdbTestServer server = null;
        int port;
        if (portEnv != null && !portEnv.isEmpty()) {
            port = Integer.parseInt(portEnv);
        } else {
            server = PdbTestServer.tryStart();
            Assumptions.assumeTrue(server != null,
                    "neither PDB_NATIVE_PORT nor a particledb binary -- skipping");
            port = server.port;
        }
        try {
            run("jdbc:particledb://127.0.0.1:" + port + "/test?preferUnixSocket=false");
        } finally {
            if (server != null) server.stop();
        }
        System.out.println("NBE_PASS jdbc");
    }

    private interface Case { void run() throws Exception; }

    private static void check(List<String> failures, String name, Case r) {
        try {
            r.run();
        } catch (Throwable t) {
            String line = "case " + name + ": " + t;
            System.out.println("NBE FAIL " + line);
            failures.add(line);
        }
    }

    private static void assertDuplicate(String label, SQLException e) {
        assertTrue(e.getMessage() != null && e.getMessage().contains("duplicate key"),
                label + ": message lacks 'duplicate key': " + e.getMessage());
    }

    private static long count(Connection c, String where) throws SQLException {
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + T + " WHERE " + where)) {
            assertTrue(rs.next(), "COUNT(*) returned no row");
            return rs.getLong(1);
        }
    }

    private void run(String url) throws Exception {
        List<String> failures = new ArrayList<>();
        try (Connection c = DriverManager.getConnection(url)) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("DROP TABLE IF EXISTS " + T);
                st.executeUpdate("CREATE TABLE " + T + " (id INT PRIMARY KEY)");
            }

            // (a) auto-commit batches with a duplicate key must raise.
            check(failures, "a/statement", () -> {
                try (Statement st = c.createStatement()) {
                    st.addBatch("INSERT INTO " + T + " VALUES (1)");
                    st.addBatch("INSERT INTO " + T + " VALUES (1)");
                    st.addBatch("INSERT INTO " + T + " VALUES (2)");
                    BatchUpdateException e = assertThrows(BatchUpdateException.class,
                            st::executeBatch, "Statement batch with a duplicate key returned success");
                    assertDuplicate("a/statement", e);
                    assertArrayEquals(new int[]{1}, e.getUpdateCounts(),
                            "update counts of the statements that ran: "
                                    + Arrays.toString(e.getUpdateCounts()));
                    System.out.println("NBE case a/statement raised: " + e.getMessage()
                            + " counts=" + Arrays.toString(e.getUpdateCounts())
                            + " sqlstate=" + e.getSQLState());
                }
            });
            check(failures, "a/prepared", () -> {
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + T + " VALUES (?)")) {
                    for (int id : new int[]{5, 5, 6}) {
                        ps.setInt(1, id);
                        ps.addBatch();
                    }
                    BatchUpdateException e = assertThrows(BatchUpdateException.class,
                            ps::executeBatch, "PreparedStatement batch with a duplicate key returned success");
                    assertDuplicate("a/prepared", e);
                    if (c.unwrap(PdbConnection.class).bulkBatchEnabled()) {
                        // A plain INSERT batch is ONE atomic bulk ingest: nothing ran.
                        assertArrayEquals(new int[0], e.getUpdateCounts());
                        assertEquals("23505", e.getSQLState());
                        assertEquals(0L, count(c, "id IN (5, 6)"), "a failed bulk batch loaded rows");
                    } else {
                        assertArrayEquals(new int[]{1}, e.getUpdateCounts());
                    }
                    System.out.println("NBE case a/prepared raised: " + e.getMessage());
                }
            });
            check(failures, "a/executor", () -> {
                BatchExecutor be = new BatchExecutor((PdbConnection) c);
                be.add("INSERT INTO " + T + " VALUES (7)");
                be.add("INSERT INTO " + T + " VALUES (7)");
                SQLException e = assertThrows(SQLException.class, be::execute,
                        "BatchExecutor with a duplicate key returned success");
                assertDuplicate("a/executor", e);
                System.out.println("NBE case a/executor raised: " + e.getMessage());
            });

            // (b) a duplicate inside a manual-commit transaction must raise.
            check(failures, "b", () -> {
                c.setAutoCommit(false);
                try (Statement st = c.createStatement()) {
                    st.addBatch("INSERT INTO " + T + " VALUES (3)");
                    st.addBatch("INSERT INTO " + T + " VALUES (3)");
                    BatchUpdateException e = assertThrows(BatchUpdateException.class,
                            st::executeBatch, "transaction batch with a duplicate key returned success");
                    assertDuplicate("b", e);
                    System.out.println("NBE case b raised: " + e.getMessage()
                            + " counts=" + Arrays.toString(e.getUpdateCounts()));
                }
            });

            // (c) rollback(), then a plain query on the SAME connection works
            // and the failed transaction's row is absent.
            check(failures, "c", () -> {
                if (!c.getAutoCommit()) c.rollback();
                assertEquals(0L, count(c, "id = 3"), "failed transaction's row is visible");
                c.setAutoCommit(true);
                assertEquals(0L, count(c, "id = 3"));
                System.out.println("NBE case c ok");
            });

            // (c2) commit() of a transaction whose statement failed behaves as
            // pgjdbc's does against PostgreSQL: the failing STATEMENT raised
            // (that is where the application learns the transaction is lost),
            // and commit() then returns normally -- PostgreSQL answers that
            // COMMIT with the tag ROLLBACK and no error, and pgjdbc 42.7.x
            // raises nothing (measured against PostgreSQL 17.9). The rollback
            // itself is proven by the row being absent.
            check(failures, "c2", () -> {
                c.setAutoCommit(false);
                try (Statement st = c.createStatement()) {
                    st.executeUpdate("INSERT INTO " + T + " VALUES (20)");
                    assertThrows(SQLException.class,
                            () -> st.executeUpdate("INSERT INTO " + T + " VALUES (20)"),
                            "duplicate inside the transaction returned success");
                }
                assertDoesNotThrow(c::commit,
                        "commit() of a failed transaction must return like pgjdbc's");
                c.setAutoCommit(true);
                assertEquals(0L, count(c, "id = 20"), "the failed transaction's row is visible");
                System.out.println("NBE case c2 ok: commit() returned, transaction rolled back");
            });

            // (d) all-success batches and a successful transaction still work.
            check(failures, "d", () -> {
                if (!c.getAutoCommit()) c.setAutoCommit(true);
                try (Statement st = c.createStatement()) {
                    st.addBatch("INSERT INTO " + T + " VALUES (10)");
                    st.addBatch("INSERT INTO " + T + " VALUES (11)");
                    assertArrayEquals(new int[]{1, 1}, st.executeBatch());
                }
                c.setAutoCommit(false);
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + T + " VALUES (?)")) {
                    for (int id : new int[]{12, 13}) {
                        ps.setInt(1, id);
                        ps.addBatch();
                    }
                    assertArrayEquals(new int[]{1, 1}, ps.executeBatch());
                }
                c.commit();
                c.setAutoCommit(true);
                BatchExecutor be = new BatchExecutor((PdbConnection) c);
                be.add("INSERT INTO " + T + " VALUES (14)");
                be.add("INSERT INTO " + T + " VALUES (15)");
                assertEquals(2L, be.execute());
                assertEquals(6L, count(c, "id IN (10, 11, 12, 13, 14, 15)"),
                        "successful batch/transaction rows not all visible");
                System.out.println("NBE case d ok");
            });

            if (failures.isEmpty()) {
                if (!c.getAutoCommit()) c.setAutoCommit(true);
                try (Statement st = c.createStatement()) {
                    st.executeUpdate("DROP TABLE " + T);
                }
            }
        }
        if (!failures.isEmpty()) {
            fail("native batch error cases failed:\n  " + String.join("\n  ", failures));
        }
    }
}
