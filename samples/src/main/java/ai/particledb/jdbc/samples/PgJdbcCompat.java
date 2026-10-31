package ai.particledb.jdbc.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Demonstrates that stock pgjdbc (org.postgresql.Driver) speaks directly
 * to the ParticleDB engine on the PG wire port. Exercises pgjdbc's
 * default {@code prepareThreshold=5} promotion to a server-side prepared
 * statement - the path that surfaced the 5-SELECT regression that
 * required engine fix at the server's PostgreSQL-wire layer to refresh
 * {@code use_binary} on every Bind.
 *
 * If this main runs to completion without
 * {@code java.lang.ArrayIndexOutOfBoundsException} or
 * {@code NoSuchElementException}, the engine fix is intact.
 *
 * Defaults to {@code jdbc:postgresql://127.0.0.1:5432/test} (PG wire,
 * not PDB wire). Override with {@code PG_URL}.
 */
public final class PgJdbcCompat {
    public static void main(String[] args) throws Exception {
        String url = System.getenv().getOrDefault(
                "PG_URL", "jdbc:postgresql://127.0.0.1:5432/test");
        Class.forName("org.postgresql.Driver");

        try (Connection c = open(url, /* prepareThreshold */ 5)) {
            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS pgjdbc_compat");
                s.execute("CREATE TABLE pgjdbc_compat (id INT PRIMARY KEY, val INT)");
                s.executeUpdate("INSERT INTO pgjdbc_compat (id, val) VALUES (1153, 42)");
            }
        }

        runLoop(url, /* prepareThreshold */ 5, /* iterations */ 10);
        runLoop(url, /* prepareThreshold */ 1, /* iterations */ 6);
        System.out.println("[done] pgjdbc + PDB engine compat verified.");
    }

    private static Connection open(String url, int prepareThreshold) throws Exception {
        Properties p = new Properties();
        p.setProperty("user", "particledb");
        p.setProperty("password", "");
        p.setProperty("prepareThreshold", Integer.toString(prepareThreshold));
        return DriverManager.getConnection(url, p);
    }

    private static void runLoop(String url, int prepareThreshold, int iterations)
            throws Exception {
        System.out.printf("[loop] prepareThreshold=%d iterations=%d%n",
                prepareThreshold, iterations);
        try (Connection c = open(url, prepareThreshold);
             PreparedStatement ps = c.prepareStatement(
                     "SELECT val FROM pgjdbc_compat WHERE id = ?")) {
            for (int i = 1; i <= iterations; i++) {
                ps.setInt(1, 1153);
                int got = -1;
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) got = rs.getInt(1);
                }
                System.out.printf("  iter %2d/%d → %d%n", i, iterations, got);
                if (got != 42) {
                    throw new AssertionError("iter " + i + " returned " + got);
                }
            }
        }
    }
}
