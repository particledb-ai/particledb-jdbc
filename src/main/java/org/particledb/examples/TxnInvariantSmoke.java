// SPDX-License-Identifier: MIT
// ParticleDB JDBC - transaction-invariant smoke client.
//
// Demonstrates the transaction shape that bank / counter / register style
// consistency checks use:
//
//   1. Open JDBC connection (PG wire :5432 via pgjdbc, OR the native wire
//      :7173 via this particledb-jdbc driver).
//   2. Auto-commit OFF; explicit BEGIN ... COMMIT/ROLLBACK on a single
//      Connection per worker.
//   3. Tight inner loop of UPDATE ... balance = balance +/- N statements with
//      WHERE id = ?, plus a final SELECT sum(balance) to check the invariant.
//   4. Clean close.
//
// This file is consciously dependency-free (only java.sql + the JDBC URL the
// user picks), so it can serve as a reference for the transaction shape in
// your own consistency tests.
//
// Build:
//   mvn -q -DskipTests package
//
// Run (PG wire via pgjdbc):
//   java -cp particledb-jdbc-1.0.11.jar:postgresql-42.7.4.jar \
//        org.particledb.examples.TxnInvariantSmoke \
//        jdbc:postgresql://node1:5432/postgres admin '' bank
//
// Run (native wire via this driver, multiplexed):
//   java -cp particledb-jdbc-1.0.11.jar \
//        org.particledb.examples.TxnInvariantSmoke \
//        jdbc:particledb://node1:7173/postgres admin '' bank

package org.particledb.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public final class TxnInvariantSmoke {

    private static final int ACCOUNTS = 10;
    private static final int STARTING_BALANCE = 100;
    private static final int TRANSFER = 5;

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("usage: TxnInvariantSmoke <jdbc-url> <user> <password> <workload>");
            System.err.println("  workload ∈ { bank, register, lost-update }");
            System.exit(2);
        }
        final String url      = args[0];
        final String user     = args[1];
        final String password = args[2];
        final String workload = args[3];

        // Register both drivers so the URL discriminator picks the right one.
        try { Class.forName("org.postgresql.Driver");    } catch (ClassNotFoundException ignored) {}
        try { Class.forName("org.particledb.Driver");    } catch (ClassNotFoundException ignored) {}

        final Properties props = new Properties();
        props.setProperty("user", user);
        if (password != null && !password.isEmpty()) {
            props.setProperty("password", password);
        }

        switch (workload) {
            case "bank":         runBank(url, props);         break;
            case "register":     runRegister(url, props);     break;
            case "lost-update":  runLostUpdate(url, props);   break;
            default:
                System.err.println("unknown workload: " + workload);
                System.exit(2);
        }
    }

    // Bank: N accounts of $STARTING_BALANCE each. K concurrent transfers
    // (balance -= T from src; balance += T into dst) under SSI/SI. Final
    // invariant: SUM(balance) = N * STARTING_BALANCE.
    private static void runBank(String url, Properties props) throws Exception {
        try (Connection setup = DriverManager.getConnection(url, props)) {
            setup.setAutoCommit(true);
            try (Statement s = setup.createStatement()) {
                s.execute("DROP TABLE IF EXISTS smoke_bank");
                s.execute("CREATE TABLE smoke_bank (id INT PRIMARY KEY, balance INT NOT NULL)");
                for (int i = 1; i <= ACCOUNTS; i++) {
                    s.execute("INSERT INTO smoke_bank VALUES (" + i + ", " + STARTING_BALANCE + ")");
                }
            }
        }

        final int expected = ACCOUNTS * STARTING_BALANCE;
        final int processes = 8;
        final int transfersPerProcess = 250;
        final AtomicInteger committed = new AtomicInteger();
        final AtomicInteger aborted   = new AtomicInteger();
        final Thread[] threads = new Thread[processes];
        final Random shared = new Random(42);

        long t0 = System.nanoTime();
        for (int p = 0; p < processes; p++) {
            final int pid = p;
            threads[p] = new Thread(() -> {
                Random rng = new Random(shared.nextLong());
                try (Connection c = DriverManager.getConnection(url, props)) {
                    c.setAutoCommit(false);
                    try (PreparedStatement sub = c.prepareStatement(
                                "UPDATE smoke_bank SET balance = balance - ? WHERE id = ?");
                         PreparedStatement add = c.prepareStatement(
                                "UPDATE smoke_bank SET balance = balance + ? WHERE id = ?")) {
                        for (int i = 0; i < transfersPerProcess; i++) {
                            int from = 1 + rng.nextInt(ACCOUNTS);
                            int to;
                            do { to = 1 + rng.nextInt(ACCOUNTS); } while (to == from);
                            sub.setInt(1, TRANSFER); sub.setInt(2, from); sub.executeUpdate();
                            add.setInt(1, TRANSFER); add.setInt(2, to);   add.executeUpdate();
                            try {
                                c.commit();
                                committed.incrementAndGet();
                            } catch (SQLException retryable) {
                                c.rollback();
                                aborted.incrementAndGet();
                            }
                        }
                    }
                } catch (SQLException e) {
                    System.err.printf("[p%d] FAIL %s%n", pid, e.getMessage());
                }
            }, "smoke-bank-p" + p);
            threads[p].start();
        }
        for (Thread t : threads) t.join();
        long dt = System.nanoTime() - t0;

        try (Connection c = DriverManager.getConnection(url, props);
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT SUM(balance) FROM smoke_bank")) {
            rs.next();
            int actual = rs.getInt(1);
            boolean ok = actual == expected;
            System.out.printf("bank: %s  expected=%d  actual=%d  committed=%d  aborted=%d  wall=%.2fs%n",
                    ok ? "PASS" : "FAIL", expected, actual,
                    committed.get(), aborted.get(), dt / 1e9);
            try (Statement drop = c.createStatement()) {
                drop.execute("DROP TABLE smoke_bank");
            }
            System.exit(ok ? 0 : 1);
        }
    }

    // Lost-update: a single counter is incremented 100 times across 4 concurrent
    // connections. Final value must equal 100; if writes interleave without
    // commit-time validation, it will be lower.
    private static void runLostUpdate(String url, Properties props) throws Exception {
        try (Connection setup = DriverManager.getConnection(url, props)) {
            setup.setAutoCommit(true);
            try (Statement s = setup.createStatement()) {
                s.execute("DROP TABLE IF EXISTS smoke_counter");
                s.execute("CREATE TABLE smoke_counter (id INT PRIMARY KEY, v INT NOT NULL)");
                s.execute("INSERT INTO smoke_counter VALUES (1, 0)");
            }
        }
        final int processes = 4;
        final int incPerProcess = 25;
        final Thread[] threads = new Thread[processes];
        for (int p = 0; p < processes; p++) {
            threads[p] = new Thread(() -> {
                try (Connection c = DriverManager.getConnection(url, props)) {
                    c.setAutoCommit(true);
                    try (Statement s = c.createStatement()) {
                        for (int i = 0; i < incPerProcess; i++) {
                            s.executeUpdate("UPDATE smoke_counter SET v = v + 1 WHERE id = 1");
                        }
                    }
                } catch (SQLException ignored) {}
            }, "lost-p" + p);
            threads[p].start();
        }
        for (Thread t : threads) t.join();
        try (Connection c = DriverManager.getConnection(url, props);
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT v FROM smoke_counter WHERE id = 1")) {
            rs.next();
            int v = rs.getInt(1);
            boolean ok = v == processes * incPerProcess;
            System.out.printf("lost-update: %s  expected=%d  actual=%d%n",
                    ok ? "PASS" : "FAIL", processes * incPerProcess, v);
            try (Statement drop = c.createStatement()) {
                drop.execute("DROP TABLE smoke_counter");
            }
            System.exit(ok ? 0 : 1);
        }
    }

    // Register: a CAS-style register check; one process writes a fresh value,
    // others read; the final read must equal the last write.
    private static void runRegister(String url, Properties props) throws Exception {
        try (Connection setup = DriverManager.getConnection(url, props)) {
            setup.setAutoCommit(true);
            try (Statement s = setup.createStatement()) {
                s.execute("DROP TABLE IF EXISTS smoke_register");
                s.execute("CREATE TABLE smoke_register (id INT PRIMARY KEY, v INT)");
                s.execute("INSERT INTO smoke_register VALUES (1, 0)");
            }
        }
        try (Connection c = DriverManager.getConnection(url, props)) {
            c.setAutoCommit(true);
            try (Statement s = c.createStatement()) {
                for (int i = 1; i <= 50; i++) {
                    s.executeUpdate("UPDATE smoke_register SET v = " + i + " WHERE id = 1");
                }
                try (ResultSet rs = s.executeQuery("SELECT v FROM smoke_register WHERE id = 1")) {
                    rs.next();
                    int v = rs.getInt(1);
                    boolean ok = v == 50;
                    System.out.printf("register: %s  expected=50  actual=%d%n",
                            ok ? "PASS" : "FAIL", v);
                    s.execute("DROP TABLE smoke_register");
                    System.exit(ok ? 0 : 1);
                }
            }
        }
    }
}
