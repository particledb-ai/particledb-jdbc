package org.particledb.bench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Is it worth sharing ONE socket between threads? The evidence for the
 * driver's "one socket per Connection" decision (conformance/SPEC.md §6).
 *
 * <p>T threads each run N point SELECTs (prepared, server-side) either all
 * over ONE Connection (one multiplexed socket: the shape a shared-socket pool
 * would give every caller) or each over its OWN Connection (the default).
 * Alternating rounds on one server; prints throughput and p50/p99 latency.
 *
 * <pre>java -cp &lt;driver classpath&gt; SocketSharingBench.java PORT [THREADS] [PER_THREAD] [ROUNDS]</pre>
 */
public final class SocketSharingBench {
    public static void main(String[] a) throws Exception {
        int port = Integer.parseInt(a[0]);
        int threads = a.length > 1 ? Integer.parseInt(a[1]) : 32;
        int per = a.length > 2 ? Integer.parseInt(a[2]) : 5000;
        int rounds = a.length > 3 ? Integer.parseInt(a[3]) : 3;
        String url = "jdbc:particledb://127.0.0.1:" + port + "/";
        try (Connection c = DriverManager.getConnection(url)) {
            c.createStatement().execute("DROP TABLE IF EXISTS ssb");
            c.createStatement().execute("CREATE TABLE ssb (id INT8 PRIMARY KEY, v TEXT)");
            c.createStatement().execute("INSERT INTO ssb SELECT g, 'v' || g FROM generate_series(1, 10000) g");
        }
        for (int r = 0; r < rounds; r++) {
            for (boolean shared : new boolean[] {true, false}) {
                run(url, threads, per, shared);
            }
        }
    }

    static void run(String url, int threads, int per, boolean shared) throws Exception {
        Connection one = shared ? DriverManager.getConnection(url) : null;
        List<Connection> own = new ArrayList<>();
        for (int t = 0; t < threads && !shared; t++) own.add(DriverManager.getConnection(url));
        long[][] lat = new long[threads][per];
        CountDownLatch go = new CountDownLatch(1), done = new CountDownLatch(threads);
        List<Thread> ts = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int ti = t;
            Connection c = shared ? one : own.get(t);
            Thread th = new Thread(() -> {
                try (PreparedStatement ps = c.prepareStatement("SELECT v FROM ssb WHERE id = ?")) {
                    go.await();
                    for (int i = 0; i < per; i++) {
                        long s0 = System.nanoTime();
                        ps.setLong(1, 1 + (ti * 7919L + i) % 10000);
                        try (ResultSet rs = ps.executeQuery()) { rs.next(); }
                        lat[ti][i] = System.nanoTime() - s0;
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            });
            th.start();
            ts.add(th);
        }
        long t0 = System.nanoTime();
        go.countDown();
        done.await();
        long wall = System.nanoTime() - t0;
        long[] all = Arrays.stream(lat).flatMapToLong(Arrays::stream).sorted().toArray();
        System.out.printf("%-14s threads=%d ops=%d  %,.0f ops/s  p50=%.0fus p99=%.0fus%n",
                shared ? "ONE socket" : "socket/thread", threads, all.length,
                all.length / (wall / 1e9), all[all.length / 2] / 1e3, all[(int) (all.length * 0.99)] / 1e3);
        if (one != null) one.close();
        for (Connection c : own) c.close();
    }
}
