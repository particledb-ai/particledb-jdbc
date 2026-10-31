package org.particledb.bench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Latency of a Conductor task upsert (its {@code index_data}
 * bound with {@code setArray}) over particledb-jdbc, two ways on ONE server:
 *
 * <ul>
 *   <li>{@code text}: {@code serverPrepare=false} -- every value inlined into
 *       the SQL text, the array as {@code '{..}'::text[]}: what the driver did
 *       for ANY statement with an array parameter through 1.0.8;</li>
 *   <li>{@code bound}: the default -- PREPARE once, then EXECUTE with typed
 *       values, the array as a typed native-wire array (tag 17).</li>
 * </ul>
 *
 * Closed loop, one connection per thread, over TCP ({@code preferUnixSocket=false}
 * unless the URL says otherwise). Each upsert hits one of {@code KEYS} task
 * ids, so after the first pass it is the ON CONFLICT DO UPDATE path, as
 * Conductor's three updates per task are. Rounds alternate text / bound;
 * prints each round's median / p99 per-statement latency, then the median of
 * the rounds' medians per mode, and the server's CPU per statement when
 * {@code PDB_PID} is set.
 *
 * <pre>
 *   ArrayUpsertLatencyBench URL [THREADS=1] [ROUNDS=6] [OPS_PER_ROUND=2000] [KEYS=500] [SHAPE=full]
 * </pre>
 *
 * {@code SHAPE=full} is the statement verbatim. {@code SHAPE=plain} drops the
 * three {@code ?::jsonb} casts and the {@code DO UPDATE ... WHERE} (the values
 * still land in the jsonb columns by assignment): a shape the server's bound
 * plans cover today, to price what binding buys once the plan is served
 * without a parse.
 */
public final class ArrayUpsertLatencyBench {

    static final String SHARD = "task_archive_shard_bench";
    static final String DDL = "CREATE TABLE IF NOT EXISTS " + SHARD + " (tenant_id varchar(255) NOT NULL, task_id varchar(255) NOT NULL,"
            + " task_reference_name varchar(255) NOT NULL, workflow_id varchar(255) NOT NULL, task_type varchar(255),"
            + " workflow_name varchar(255), created_on bigint, scheduled_on bigint, modified_on bigint, end_on bigint,"
            + " status varchar(255) NOT NULL, task_input jsonb, task_output jsonb, json_data jsonb, index_data text[],"
            + " PRIMARY KEY (tenant_id, task_id))";
    static final String UPSERT = "INSERT INTO " + SHARD + " (tenant_id, task_id, task_reference_name, workflow_id, task_type, workflow_name, "
            + "created_on, scheduled_on, modified_on, end_on, status, task_input, task_output, json_data, index_data) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?::jsonb, ?) "
            + "ON CONFLICT (tenant_id, task_id) DO UPDATE SET "
            + "json_data = EXCLUDED.json_data, modified_on = EXCLUDED.modified_on, end_on = EXCLUDED.end_on, "
            + "status = EXCLUDED.status, task_output = EXCLUDED.task_output, index_data = EXCLUDED.index_data "
            + "WHERE " + SHARD + ".modified_on <= EXCLUDED.modified_on "
            + "OR EXCLUDED.status IN ('COMPLETED','FAILED','CANCELED','TIMED_OUT','FAILED_WITH_TERMINAL_ERROR','SKIPPED')";

    private ArrayUpsertLatencyBench() {}

    static String rnd(int n) {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) sb.append((char) ('a' + r.nextInt(26)));
        return sb.toString();
    }

    /** A Conductor-sized TaskModel document (~1.6 KB) and a 1 KB output. */
    static String taskJson(String tid, String status, long now) {
        return "{\"taskType\":\"simulatedTask\",\"status\":\"" + status + "\",\"inputData\":{\"delayType\":\"fixed\","
                + "\"minDelay\":100,\"maxDelay\":100,\"outputSize\":1024},\"referenceTaskName\":\"simple_1\",\"retryCount\":0,"
                + "\"seq\":3,\"pollCount\":1,\"taskDefName\":\"simulatedTask\",\"scheduledTime\":" + now + ",\"startTime\":" + now
                + ",\"endTime\":0,\"updateTime\":" + now + ",\"workflowInstanceId\":\"wf-1\",\"workflowType\":\"loadtest_parallel\","
                + "\"taskId\":\"" + tid + "\",\"callbackAfterSeconds\":0,\"workerId\":\"loadtest-workers-local-0\","
                + "\"outputData\":{\"data\":\"" + rnd(1024) + "\"},\"workflowTask\":{\"name\":\"simulatedTask\","
                + "\"taskReferenceName\":\"simple_1\",\"type\":\"SIMPLE\",\"taskDefinition\":{\"name\":\"simulatedTask\","
                + "\"retryCount\":3,\"timeoutSeconds\":3600,\"timeoutPolicy\":\"TIME_OUT_WF\",\"retryLogic\":\"FIXED\","
                + "\"ownerEmail\":\"loadtest@example.com\"}},\"iteration\":0}";
    }

    static String sql = UPSERT;

    static void upsert(Connection c, PreparedStatement ps, String tid, String status, long now) throws Exception {
        ps.setString(1, "0000");
        ps.setString(2, tid);
        ps.setString(3, "simple_1");
        ps.setString(4, "wf-1");
        ps.setString(5, "simulatedTask");
        ps.setString(6, "loadtest_parallel");
        ps.setLong(7, now);
        ps.setLong(8, now);
        ps.setLong(9, now);
        ps.setLong(10, 0);
        ps.setString(11, status);
        ps.setString(12, "{\"delayType\":\"fixed\",\"minDelay\":100,\"maxDelay\":100,\"outputSize\":1024}");
        ps.setString(13, "{\"data\":\"" + rnd(1024) + "\"}");
        ps.setString(14, taskJson(tid, status, now));
        ps.setArray(15, c.createArrayOf("text", new String[] {"simulatedTask", status, "simple_1", tid, "wf-1",
                "loadtest_parallel", "SIMPLE", "fixed", "100", "1024", "1", "loadtest", "a", "c", "completed", "0000"}));
        if (ps.executeUpdate() != 1) throw new IllegalStateException("upsert did not report 1 row");
    }

    /** One round in {@code mode}: per-statement latencies (ns) of every thread. */
    static long[] round(String url, String mode, int threads, int ops, int keys) throws Exception {
        String u = url + (url.contains("?") ? "&" : "?") + "serverPrepare=" + mode.equals("bound");
        long[][] lat = new long[threads][ops];
        Thread[] ts = new Thread[threads];
        Exception[] err = new Exception[1];
        for (int t = 0; t < threads; t++) {
            final int tt = t;
            ts[t] = new Thread(() -> {
                try (Connection c = DriverManager.getConnection(u)) {
                    String[] statuses = {"SCHEDULED", "IN_PROGRESS", "COMPLETED"};
                    long now = System.currentTimeMillis() * 1000;
                    try (PreparedStatement ps = c.prepareStatement(sql)) {
                        for (int i = 0; i < 50; i++) upsert(c, ps, "w" + tt + "-" + i, statuses[i % 3], ++now);
                        for (int i = 0; i < ops; i++) {
                            String tid = "t" + tt + "-" + (i % keys);
                            long t0 = System.nanoTime();
                            upsert(c, ps, tid, statuses[i % 3], ++now);
                            lat[tt][i] = System.nanoTime() - t0;
                        }
                    }
                } catch (Exception e) {
                    err[0] = e;
                }
            });
            ts[t].start();
        }
        for (Thread t : ts) t.join();
        if (err[0] != null) throw err[0];
        long[] all = new long[threads * ops];
        for (int t = 0; t < threads; t++) System.arraycopy(lat[t], 0, all, t * ops, ops);
        Arrays.sort(all);
        return all;
    }

    static double serverCpu() {
        String pid = System.getenv("PDB_PID");
        if (pid == null) return -1;
        try {
            Process p = new ProcessBuilder("ps", "-o", "cputime=", "-p", pid).start();
            String s = new String(p.getInputStream().readAllBytes()).trim();
            p.waitFor();
            double t = 0;
            for (String part : s.split(":")) t = t * 60 + Double.parseDouble(part);
            return t;
        } catch (Exception e) {
            return -1;
        }
    }

    public static void main(String[] a) throws Exception {
        String url = a[0];
        int threads = a.length > 1 ? Integer.parseInt(a[1]) : 1;
        int rounds = a.length > 2 ? Integer.parseInt(a[2]) : 6;
        int ops = a.length > 3 ? Integer.parseInt(a[3]) : 2000;
        int keys = a.length > 4 ? Integer.parseInt(a[4]) : 500;
        if (a.length > 5 && a[5].equals("plain")) {
            sql = UPSERT.replace("?::jsonb, ?::jsonb, ?::jsonb", "?, ?, ?");
            sql = sql.substring(0, sql.indexOf(" WHERE " + SHARD));
        }
        System.out.println("shape=" + (sql.equals(UPSERT) ? "full" : "plain"));
        try (Connection c = DriverManager.getConnection(url); Statement st = c.createStatement()) {
            st.execute(DDL);
        }
        String[] modes = {"text", "bound"};
        double[][] med = new double[2][rounds];
        double[] cpu = new double[2];
        for (int r = 0; r < rounds; r++) {
            for (int m = 0; m < 2; m++) {
                double c0 = serverCpu();
                long[] l = round(url, modes[m], threads, ops, keys);
                double c1 = serverCpu();
                if (c0 >= 0 && c1 >= 0) cpu[m] += c1 - c0;
                med[m][r] = l[l.length / 2] / 1000.0;
                System.out.printf("round %d %-5s threads=%d median=%.1fus p99=%.1fus%n", r, modes[m], threads,
                        l[l.length / 2] / 1000.0, l[(int) (l.length * 0.99)] / 1000.0);
            }
        }
        for (int m = 0; m < 2; m++) {
            double[] s = med[m].clone();
            Arrays.sort(s);
            System.out.printf("RESULT %-5s threads=%d median_of_round_medians=%.1fus", modes[m], threads, s[s.length / 2]);
            if (cpu[m] > 0) System.out.printf(" server_cpu_us_per_stmt=%.1f", 1e6 * cpu[m] / ((double) rounds * threads * (ops + 50)));
            System.out.println();
        }
    }
}
