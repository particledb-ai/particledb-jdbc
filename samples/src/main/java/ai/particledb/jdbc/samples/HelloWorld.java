package ai.particledb.jdbc.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Smallest possible ParticleDB JDBC sample: connect, run SELECT 1, print.
 *
 * Run with:
 *   mvn -q exec:exec -Dexec.args="-cp %classpath ai.particledb.jdbc.samples.HelloWorld"
 *
 * Defaults to {@code jdbc:particledb://127.0.0.1:7173/default}. Set
 * {@code PDB_URL} env var to override.
 */
public final class HelloWorld {
    public static void main(String[] args) throws Exception {
        String url = System.getenv().getOrDefault(
                "PDB_URL", "jdbc:particledb://127.0.0.1:7173/default");
        Class.forName("org.particledb.Driver");
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT 1")) {
            while (rs.next()) {
                System.out.println("hello, particledb -> " + rs.getInt(1));
            }
        }
    }
}
