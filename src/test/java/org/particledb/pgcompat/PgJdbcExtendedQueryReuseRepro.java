// Regression repro for the pgjdbc 5-SELECT prepared-statement promotion bug.
// Reported externally on 2026-05-18 against a pre-release build; fixed in
// the server's PostgreSQL-wire layer by refreshing the PointSelect batch's
// use_binary flag from each Bind's result_format_codes, not just on batch
// creation.
//
// Symptom before the fix: pgjdbc default prepareThreshold=5 means iter 1..5
// use the unnamed prepared statement (text result format) and work, then
// iter 6 reuses a named statement ("S_1") with a binary result format. The
// engine's fast-path PointSelect batch was created on iter 5 with
// use_binary=false and reused on iter 6 without refresh, so it kept emitting
// 2-byte text "42" while the client read it as binary int8 → OOB index 2.
//
// Run as a single-file source (no JUnit needed):
//   java -cp ~/.m2/repository/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
//        --source 17 \
//        src/test/java/org/particledb/pgcompat/PgJdbcExtendedQueryReuseRepro.java \
//        <pg-port>

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class PgJdbcExtendedQueryReuseRepro {

    public static void main(String[] args) throws Exception {
        int port = (args.length >= 1) ? Integer.parseInt(args[0]) : 5432;
        String url = "jdbc:postgresql://127.0.0.1:" + port + "/test";

        // 1) Setup table + insert one row via simple query.
        try (Connection c = open(url, 5)) {
            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS txn0");
                s.execute("CREATE TABLE txn0 (id INT PRIMARY KEY, val INT)");
                s.executeUpdate("INSERT INTO txn0 (id, val) VALUES (1153, 42)");
            }
            System.out.println("[setup] table + row created");
        }

        // 2) Default-threshold case: 5 simple-query passes, then promotion.
        //    Expect the 6th iteration to throw NoSuchElementException.
        runReuseLoop(url, /* prepareThreshold */ 5, /* iterations */ 10);

        // 3) Force-extended case (threshold=1): every call goes Parse/Bind/Execute.
        runReuseLoop(url, /* prepareThreshold */ 1, /* iterations */ 6);

        System.out.println("[done] all loops completed without NoSuchElementException");
    }

    private static Connection open(String url, int prepareThreshold) throws Exception {
        Properties p = new Properties();
        p.setProperty("user", "particledb");
        p.setProperty("password", "");
        p.setProperty("prepareThreshold", Integer.toString(prepareThreshold));
        p.setProperty("preferQueryMode", "extendedForPrepared"); // be explicit
        return DriverManager.getConnection(url, p);
    }

    private static void runReuseLoop(String url, int prepareThreshold, int iterations)
            throws Exception {
        System.out.printf("[loop] prepareThreshold=%d iterations=%d%n",
                prepareThreshold, iterations);
        try (Connection c = open(url, prepareThreshold);
             PreparedStatement ps = c.prepareStatement(
                     "SELECT val FROM txn0 WHERE id = ?")) {
            for (int i = 1; i <= iterations; i++) {
                ps.setInt(1, 1153);
                int got = -1;
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        got = rs.getInt(1);
                    }
                }
                System.out.printf("  [iter %2d/%d] got=%d  (expect 42)%n",
                        i, iterations, got);
                if (got != 42) {
                    throw new AssertionError(
                            "iter " + i + " returned " + got + " (expected 42)");
                }
            }
        }
    }
}
