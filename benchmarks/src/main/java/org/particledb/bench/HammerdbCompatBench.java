package org.particledb.bench;

import org.particledb.BatchExecutor;
import org.particledb.PdbConnection;
import org.particledb.Pgcompat;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Minimal TPC-C NEWORD benchmark driver against PDB-wire JDBC.
 *
 * <p>This harness is intentionally tiny - it's a baseline for the post-#277
 * "JVM apps on PDB-wire" measurement. Run pattern:
 *
 * <pre>
 *   java -cp particledb-jdbc-1.0.0.jar:hammerdb-bench.jar \
 *        org.particledb.bench.HammerdbCompatBench \
 *        --threads 32 --duration 60 \
 *        --url jdbc:particledb://localhost:7173/tpcc
 * </pre>
 *
 * <p>Measures TPS by issuing batched NEWORD-shape inserts. Each thread owns
 * its own {@code PdbConnection} (contrary to the multiplexing happy path)
 * to give a like-for-like comparison vs pgjdbc - the multiplexed mode is
 * benchmarked separately by reusing one shared connection across threads.
 */
public final class HammerdbCompatBench {

    public static void main(String[] args) throws Exception {
        Args a = parse(args);
        System.out.println("[bench] url=" + a.url + " threads=" + a.threads
                + " duration=" + a.durationSec + "s sharedConn=" + a.sharedConn);

        Pgcompat.ParsedUrl parsed = Pgcompat.parse(a.url, null);
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(a.durationSec);
        AtomicLong txns = new AtomicLong();
        ExecutorService pool = Executors.newFixedThreadPool(a.threads);

        PdbConnection sharedConn = a.sharedConn ? new PdbConnection(parsed) : null;

        for (int t = 0; t < a.threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try {
                    PdbConnection c = sharedConn != null
                            ? sharedConn
                            : new PdbConnection(parsed);
                    while (System.nanoTime() < deadlineNanos) {
                        BatchExecutor be = new BatchExecutor(c);
                        be.add("BEGIN");
                        // 5 inserts - rough NEWORD shape (district, order, ...)
                        for (int i = 0; i < 5; i++) {
                            be.add("INSERT INTO bench_t VALUES ("
                                    + (tid * 1_000_000 + (int)(Math.random() * 1_000_000))
                                    + "," + i + ")");
                        }
                        be.add("COMMIT");
                        be.execute();
                        txns.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(a.durationSec + 30, TimeUnit.SECONDS);
        long total = txns.get();
        double tps = total / (double) a.durationSec;
        System.out.printf("[bench] total=%d tps=%.0f%n", total, tps);
        if (sharedConn != null) sharedConn.close();
    }

    private static Args parse(String[] argv) {
        Args a = new Args();
        Map<String, String> kv = new LinkedHashMap<>();
        for (int i = 0; i + 1 < argv.length; i += 2) kv.put(argv[i], argv[i + 1]);
        a.url = kv.getOrDefault("--url", "jdbc:particledb://localhost:7173/bench");
        a.threads = Integer.parseInt(kv.getOrDefault("--threads", "16"));
        a.durationSec = Integer.parseInt(kv.getOrDefault("--duration", "30"));
        a.sharedConn = "true".equalsIgnoreCase(kv.getOrDefault("--shared-conn", "false"));
        return a;
    }

    private static final class Args {
        String url;
        int threads;
        int durationSec;
        boolean sharedConn;
    }
}
