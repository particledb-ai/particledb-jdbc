package ai.particledb.jdbc.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Full CRUD round-trip on a temporary table: CREATE, INSERT, SELECT, UPDATE,
 * DELETE, DROP. Demonstrates {@link PreparedStatement} parameter binding
 * (which goes through Parse/Bind/Execute on the PDB native wire).
 *
 * Run with:
 *   mvn -q compile exec:exec -Dexec.executable=java \
 *       -Dexec.args="-cp %classpath ai.particledb.jdbc.samples.CrudRoundtrip"
 */
public final class CrudRoundtrip {
    public static void main(String[] args) throws Exception {
        String url = System.getenv().getOrDefault(
                "PDB_URL", "jdbc:particledb://127.0.0.1:7173/default");
        Class.forName("org.particledb.Driver");
        try (Connection c = DriverManager.getConnection(url)) {
            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS jdbc_sample");
                s.execute("CREATE TABLE jdbc_sample ("
                        + "id INT PRIMARY KEY, name TEXT NOT NULL, score DOUBLE)");
            }

            try (PreparedStatement ins = c.prepareStatement(
                    "INSERT INTO jdbc_sample (id, name, score) VALUES (?,?,?)")) {
                Object[][] rows = {
                        {1, "alice",  92.5},
                        {2, "bob",    77.0},
                        {3, "carol",  88.25},
                };
                for (Object[] r : rows) {
                    ins.setInt(1, (Integer) r[0]);
                    ins.setString(2, (String) r[1]);
                    ins.setDouble(3, (Double) r[2]);
                    int affected = ins.executeUpdate();
                    System.out.printf("  inserted id=%s rows=%d%n", r[0], affected);
                }
            }

            System.out.println("-- after inserts --");
            dump(c);

            try (PreparedStatement upd = c.prepareStatement(
                    "UPDATE jdbc_sample SET score = score + 5 WHERE name = ?")) {
                upd.setString(1, "alice");
                int n = upd.executeUpdate();
                System.out.printf("  updated rows=%d%n", n);
            }

            try (PreparedStatement del = c.prepareStatement(
                    "DELETE FROM jdbc_sample WHERE id = ?")) {
                del.setInt(1, 2);
                int n = del.executeUpdate();
                System.out.printf("  deleted rows=%d%n", n);
            }

            System.out.println("-- after update + delete --");
            dump(c);

            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE jdbc_sample");
            }
        }
    }

    private static void dump(Connection c) throws Exception {
        try (Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, name, score FROM jdbc_sample ORDER BY id")) {
            while (rs.next()) {
                System.out.printf("  id=%d name=%s score=%.2f%n",
                        rs.getInt("id"), rs.getString("name"), rs.getDouble("score"));
            }
        }
    }
}
