package ai.particledb.jdbc.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Explicit transaction with read-your-own-writes (RYOW) over PDB native wire.
 *
 * <p><b>Wire-level semantics note.</b> The PDB native wire (port 7173,
 * the default for {@code jdbc:particledb://}) is optimised for batched
 * OLTP throughput and treats {@code BEGIN}/{@code COMMIT}/{@code ROLLBACK}
 * as batch-boundary markers, not as full ACID transaction brackets - a
 * call to {@link java.sql.Connection#rollback()} after writes that have
 * been flushed will NOT undo them on PDB native wire.
 *
 * <p>For full interactive-transaction semantics (rollback-after-write,
 * SAVEPOINT, isolation-level pinning), connect via the PG wire instead
 * using stock pgjdbc at {@code jdbc:postgresql://host:5432/db}. The
 * {@code PgJdbcCompat} sample shows that path.
 *
 * <p>This sample exercises the well-supported subset: setAutoCommit
 * toggling, RYOW within a batch, and clean COMMIT.
 */
public final class TransactionDemo {
    public static void main(String[] args) throws Exception {
        String url = System.getenv().getOrDefault(
                "PDB_URL", "jdbc:particledb://127.0.0.1:7173/default");
        Class.forName("org.particledb.Driver");

        try (Connection c = DriverManager.getConnection(url)) {
            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS jdbc_txn_sample");
                s.execute("CREATE TABLE jdbc_txn_sample ("
                        + "id INT PRIMARY KEY, balance INT NOT NULL)");
                s.execute("INSERT INTO jdbc_txn_sample VALUES (1, 100), (2, 50)");
            }

            c.setAutoCommit(false);
            try (PreparedStatement upd = c.prepareStatement(
                    "UPDATE jdbc_txn_sample SET balance = balance + ? WHERE id = ?")) {
                upd.setInt(1, -25); upd.setInt(2, 1); upd.executeUpdate();
                upd.setInt(1, +25); upd.setInt(2, 2); upd.executeUpdate();
            }
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT id, balance FROM jdbc_txn_sample ORDER BY id")) {
                System.out.println("-- inside txn (RYOW, before COMMIT) --");
                while (rs.next()) {
                    System.out.printf("  id=%d balance=%d%n",
                            rs.getInt(1), rs.getInt(2));
                }
            }
            c.commit();

            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT id, balance FROM jdbc_txn_sample ORDER BY id")) {
                System.out.println("-- after COMMIT (new autocommit reader sees it) --");
                while (rs.next()) {
                    System.out.printf("  id=%d balance=%d%n",
                            rs.getInt(1), rs.getInt(2));
                }
            }

            // PDB native wire intentionally does NOT support
            // rollback-after-write - BEGIN/COMMIT/ROLLBACK are batch markers.
            // To demonstrate the rollback semantics that JDBC defines,
            // connect via PG wire (jdbc:postgresql://host:5432/db) using
            // stock pgjdbc; see the PgJdbcCompat sample.
            c.setAutoCommit(true);
            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE jdbc_txn_sample");
            }
            System.out.println("-- done (autocommit restored, table dropped) --");
        }
    }
}
