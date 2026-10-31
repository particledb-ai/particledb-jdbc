package ai.particledb.jdbc.samples;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * HikariCP pool integration sample. ParticleDB ships no built-in pool;
 * standard practice is to wrap the driver in HikariCP for fan-out
 * workloads. The PDB wire v2 transport already multiplexes streams per
 * TCP connection, so a small pool (≤10) is typically enough.
 */
public final class HikariCpDemo {
    public static void main(String[] args) throws Exception {
        String url = System.getenv().getOrDefault(
                "PDB_URL", "jdbc:particledb://127.0.0.1:7173/default");
        Class.forName("org.particledb.Driver");

        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(url);
        cfg.setMaximumPoolSize(10);
        cfg.setMinimumIdle(1);
        cfg.setConnectionTimeout(30_000);
        cfg.setPoolName("pdb-jdbc-sample");

        try (HikariDataSource ds = new HikariDataSource(cfg)) {
            for (int i = 0; i < 5; i++) {
                try (Connection c = ds.getConnection();
                     Statement s = c.createStatement();
                     ResultSet rs = s.executeQuery("SELECT " + (i + 1))) {
                    rs.next();
                    System.out.printf("  pool checkout %d → SELECT %d%n",
                            i, rs.getInt(1));
                }
            }
        }
        System.out.println("[done] pool closed cleanly.");
    }
}
