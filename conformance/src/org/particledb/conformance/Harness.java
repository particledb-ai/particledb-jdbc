package org.particledb.conformance;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The language-neutral half of a Java conformance adapter: the runner contract
 * (env vars in, one results file out), the case files, the {@code script}-case
 * interpreter, and the canonical value comparison of
 * {@code conformance/SPEC.md} §4. An adapter supplies a {@link Driver}
 * (how this SDK opens a session and runs a statement) and its {@code custom}
 * cases.
 *
 * <p>Contract (set by {@code run_conformance.sh}): {@code PDB_CONF_HOST},
 * {@code PDB_CONF_PORT}, {@code PDB_CONF_DEFAULT_PORT}, {@code PDB_CONF_RESP_PORT},
 * {@code PDB_CONF_MODE}, {@code PDB_CONF_CASES_DIR}, {@code PDB_CONF_IDS} (file:
 * the case ids that run in this mode), {@code PDB_CONF_RESULTS} (output),
 * {@code PDB_CONF_CONTROL} (executable: {@code kill9} / {@code restart}).
 */
public final class Harness {

    // ── outcome signals a case body throws ─────────────────────────────────

    /** The case does not apply to this SDK; the reason is mandatory. */
    public static final class Skip extends RuntimeException {
        public Skip(String reason) { super(reason); }
    }

    /** An expectation did not hold. */
    public static final class Fail extends RuntimeException {
        public Fail(String detail) { super(detail); }
    }

    public static void check(boolean ok, String detail) {
        if (!ok) throw new Fail(detail);
    }

    public static void eq(Object want, Object got, String what) {
        if (want == null ? got != null : !want.equals(got)) {
            throw new Fail(what + ": expected " + show(want) + ", got " + show(got));
        }
    }

    static String show(Object o) {
        if (o == null) return "null";
        String s = o instanceof String ? "'" + o + "'" : String.valueOf(o);
        return s.length() > 200 ? s.substring(0, 200) + "...(" + s.length() + " chars)" : s;
    }

    // ── what an adapter implements ─────────────────────────────────────────

    /** A typed parameter from a case file: {@code type} and canonical {@code value} (null = typed NULL). */
    public static final class Param {
        public final String type;
        public final Object value; // String, List<String> (arrays), or null
        Param(String type, Object value) { this.type = type; this.value = value; }
    }

    /** One SDK session (a connection) the interpreter drives. */
    public interface Session extends AutoCloseable {
        void exec(String sql, List<Param> params) throws Exception;
        /** Rows of canonical cells (String / List of String-or-null / null), per {@code types}. */
        List<List<Object>> query(String sql, List<Param> params, List<String> types) throws Exception;
        void begin() throws Exception;
        void commit() throws Exception;
        void rollback() throws Exception;
        void isolation(String level) throws Exception;
        void savepoint(String name) throws Exception;
        void rollbackTo(String name) throws Exception;
        @Override void close() throws Exception;
    }

    public interface Driver {
        String sdk();
        String version();
        Session open() throws Exception;
        /** The SQLSTATE an SDK exception carries, or null. */
        String sqlState(Throwable t);
    }

    public interface CustomCase {
        void run() throws Exception;
    }

    // ── env ────────────────────────────────────────────────────────────────

    public static String env(String k) {
        String v = System.getenv(k);
        if (v == null || v.isEmpty()) throw new IllegalStateException(k + " is not set (run via run_conformance.sh)");
        return v;
    }

    public static final String HOST = System.getenv().getOrDefault("PDB_CONF_HOST", "127.0.0.1");
    public static int port() { return Integer.parseInt(env("PDB_CONF_PORT")); }
    public static int defaultPort() { return Integer.parseInt(env("PDB_CONF_DEFAULT_PORT")); }
    public static int respPort() { return Integer.parseInt(env("PDB_CONF_RESP_PORT")); }
    public static String mode() { return env("PDB_CONF_MODE"); }

    /** Run the server control hook ({@code kill9} / {@code restart}); throws unless it exits 0. */
    public static void control(String action) throws Exception {
        Process p = new ProcessBuilder(env("PDB_CONF_CONTROL"), action).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!p.waitFor(120, TimeUnit.SECONDS) || p.exitValue() != 0) {
            throw new IllegalStateException("control " + action + " failed: " + out);
        }
    }

    // ── case files ─────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    public static Map<String, Map<String, Object>> loadCases(Path dir) throws IOException {
        Map<String, Map<String, Object>> all = new LinkedHashMap<>();
        List<Path> files = new ArrayList<>();
        try (var s = Files.list(dir)) { s.filter(p -> p.toString().endsWith(".json")).sorted().forEach(files::add); }
        for (Path f : files) {
            Map<String, Object> doc = (Map<String, Object>) Json.parse(Files.readString(f));
            for (Object c : (List<Object>) doc.get("cases")) {
                Map<String, Object> m = (Map<String, Object>) c;
                all.put((String) m.get("id"), m);
            }
        }
        return all;
    }

    // ── the run loop ───────────────────────────────────────────────────────

    public static int run(Driver driver, Map<String, CustomCase> customs) throws Exception {
        Map<String, Map<String, Object>> cases = loadCases(Paths.get(env("PDB_CONF_CASES_DIR")));
        List<String> ids = new ArrayList<>();
        for (String l : Files.readAllLines(Paths.get(env("PDB_CONF_IDS")))) if (!l.isBlank()) ids.add(l.trim());
        String only = System.getenv("PDB_CONF_ONLY"); // debugging: comma-separated ids
        Map<String, Object> results = new LinkedHashMap<>();
        int pass = 0, fail = 0, skip = 0;
        ExecutorService ex = Executors.newCachedThreadPool(r -> { Thread t = new Thread(r); t.setDaemon(true); return t; });
        for (String id : ids) {
            Map<String, Object> c = cases.get(id);
            Map<String, Object> r = new LinkedHashMap<>();
            if (c == null) {
                r.put("status", "fail");
                r.put("detail", "the runner listed a case id the case files do not define");
            } else if (only != null && !List.of(only.split(",")).contains(id)) {
                r.put("status", "skip");
                r.put("reason", "PDB_CONF_ONLY excluded it (debug run)");
            } else {
                long t0 = System.nanoTime();
                Future<?> f = ex.submit(() -> {
                    if ("script".equals(c.get("kind"))) { runScript(driver, c); return null; }
                    CustomCase cc = customs.get(id);
                    if (cc == null) throw new Fail("adapter has no implementation of this custom case");
                    cc.run();
                    return null;
                });
                try {
                    f.get(180, TimeUnit.SECONDS);
                    r.put("status", "pass");
                } catch (TimeoutException te) {
                    f.cancel(true);
                    r.put("status", "fail");
                    r.put("detail", "case did not finish within 180 s");
                } catch (java.util.concurrent.ExecutionException ee) {
                    Throwable t = ee.getCause();
                    if (t instanceof Skip) {
                        r.put("status", "skip");
                        r.put("reason", t.getMessage());
                    } else {
                        r.put("status", "fail");
                        String st = driver.sqlState(t);
                        r.put("detail", (t instanceof Fail ? "" : t.getClass().getSimpleName() + ": ")
                                + t.getMessage() + (st != null && !(t instanceof Fail) ? " [SQLSTATE " + st + "]" : ""));
                    }
                }
                r.put("ms", (System.nanoTime() - t0) / 1_000_000);
            }
            String st = (String) r.get("status");
            if (st.equals("pass")) pass++; else if (st.equals("fail")) fail++; else skip++;
            System.out.printf("%-16s %-4s %s%n", id, st, r.containsKey("detail") ? r.get("detail") : r.getOrDefault("reason", ""));
            results.put(id, r);
        }
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("sdk", driver.sdk());
        doc.put("sdk_version", driver.version());
        doc.put("mode", mode());
        doc.put("results", results);
        Files.writeString(Paths.get(env("PDB_CONF_RESULTS")), Json.write(doc) + "\n");
        System.out.printf("%s: %d pass, %d fail, %d skip%n", driver.sdk(), pass, fail, skip);
        return fail == 0 ? 0 : 1;
    }

    // ── the script interpreter ─────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    static void runScript(Driver driver, Map<String, Object> c) throws Exception {
        Map<String, Session> sessions = new HashMap<>();
        try {
            int n = 0;
            for (Object o : (List<Object>) c.get("steps")) {
                n++;
                Map<String, Object> st = (Map<String, Object>) o;
                String conn = (String) st.getOrDefault("conn", "a");
                Session s = sessions.get(conn);
                if (s == null) { s = driver.open(); sessions.put(conn, s); }
                String op = (String) st.get("op");
                String sql = (String) st.get("sql");
                List<Param> params = params((List<Object>) st.get("params"));
                String where = "step " + n + " (" + op + (sql != null ? " " + abbreviate(sql) : "") + ")";
                try {
                    switch (op) {
                        case "exec": s.exec(sql, params); break;
                        case "query": {
                            List<String> types = (List<String>) st.get("types");
                            List<List<Object>> got = s.query(sql, params, types);
                            compareRows(types, (List<Object>) st.get("expect_rows"), got, where);
                            break;
                        }
                        case "error": {
                            String want = (String) st.get("sqlstate");
                            try {
                                s.exec(sql, params);
                            } catch (Fail f) {
                                throw f;
                            } catch (Exception e) {
                                eq(want, driver.sqlState(e), where + " SQLSTATE (" + e.getMessage() + ")");
                                break;
                            }
                            throw new Fail(where + ": expected SQLSTATE " + want + ", statement succeeded");
                        }
                        case "begin": s.begin(); break;
                        case "commit": s.commit(); break;
                        case "rollback": s.rollback(); break;
                        case "isolation": s.isolation((String) st.get("level")); break;
                        case "savepoint": s.savepoint((String) st.get("name")); break;
                        case "rollback_to": s.rollbackTo((String) st.get("name")); break;
                        case "commit_error": {
                            String want = (String) st.get("sqlstate");
                            if (skipNext.remove(conn) != null) break; // already raised `want` at a statement
                            try {
                                s.commit();
                            } catch (Exception e) {
                                eq(want, driver.sqlState(e), where + " SQLSTATE (" + e.getMessage() + ")");
                                try { s.rollback(); } catch (Exception ignored) { /* already ended */ }
                                break;
                            }
                            throw new Fail(where + ": expected the commit to fail " + want + ", it succeeded");
                        }
                        default: throw new Fail("unknown script op " + op);
                    }
                } catch (Fail | Skip f) {
                    throw f;
                } catch (Exception e) {
                    String stt = driver.sqlState(e);
                    // A statement failure inside a step that awaits a commit
                    // error (write skew may surface at the UPDATE): accept it
                    // when the next step for this conn is commit_error with
                    // the same SQLSTATE.
                    Map<String, Object> next = nextFor((List<Object>) c.get("steps"), n, conn);
                    if (next != null && "commit_error".equals(next.get("op")) && stt != null
                            && stt.equals(next.get("sqlstate"))) {
                        try { s.rollback(); } catch (Exception ignored) { /* ended */ }
                        skipNext.put(conn, n);
                        continue;
                    }
                    throw new Fail(where + ": " + e.getClass().getSimpleName() + ": " + e.getMessage()
                            + (stt != null ? " [SQLSTATE " + stt + "]" : ""));
                }
            }
        } finally {
            for (Session s : sessions.values()) { try { s.close(); } catch (Exception ignored) { /* best effort */ } }
            skipNext.clear();
        }
    }

    // A conn whose statement already raised the awaited commit_error SQLSTATE.
    private static final Map<String, Integer> skipNext = new java.util.concurrent.ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    private static Map<String, Object> nextFor(List<Object> steps, int done, String conn) {
        for (int i = done; i < steps.size(); i++) {
            Map<String, Object> m = (Map<String, Object>) steps.get(i);
            if (conn.equals(m.getOrDefault("conn", "a"))) return m;
        }
        return null;
    }

    static String abbreviate(String s) {
        return s.length() > 60 ? s.substring(0, 57) + "..." : s;
    }

    @SuppressWarnings("unchecked")
    static List<Param> params(List<Object> raw) {
        List<Param> out = new ArrayList<>();
        if (raw == null) return out;
        for (Object o : raw) {
            Map<String, Object> m = (Map<String, Object>) o;
            Object v = m.containsKey("gen") ? gen((Map<String, Object>) m.get("gen")) : m.get("value");
            out.add(new Param((String) m.get("type"), v));
        }
        return out;
    }

    /** Expand a generated value: {@code {"repeat": s, "count": n}} or {@code {"bytes": "all256", "count": n}} (bytea hex). */
    static String gen(Map<String, Object> g) {
        int count = Integer.parseInt(String.valueOf(g.get("count")));
        if (g.containsKey("repeat")) return String.valueOf(g.get("repeat")).repeat(count);
        StringBuilder b = new StringBuilder("\\x");
        for (int k = 0; k < count; k++) for (int i = 0; i < 256; i++) b.append(String.format("%02x", i));
        return b.toString();
    }

    // ── canonical comparison (CONFORMANCE.md §4) ───────────────────────────

    @SuppressWarnings("unchecked")
    static void compareRows(List<String> types, List<Object> want, List<List<Object>> got, String where) {
        if (want.size() != got.size()) {
            throw new Fail(where + ": expected " + want.size() + " rows, got " + got.size() + " " + show(got));
        }
        for (int r = 0; r < want.size(); r++) {
            List<Object> wr = (List<Object>) want.get(r);
            List<Object> gr = got.get(r);
            if (wr.size() != gr.size()) throw new Fail(where + ": row " + r + " width " + gr.size() + " != " + wr.size());
            for (int col = 0; col < wr.size(); col++) {
                Object w = wr.get(col);
                if (w instanceof Map) w = gen((Map<String, Object>) ((Map<String, Object>) w).get("gen"));
                if (!cellEquals(types.get(col), w, gr.get(col))) {
                    throw new Fail(where + ": row " + r + " col " + (col + 1) + " (" + types.get(col) + "): expected "
                            + show(w) + ", got " + show(gr.get(col)));
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    static boolean cellEquals(String type, Object want, Object got) {
        if (want == null || got == null) return want == null && got == null;
        if (type.endsWith("[]") || type.equals("vector")) {
            if (!(want instanceof List) || !(got instanceof List)) return false;
            List<Object> w = (List<Object>) want, g = (List<Object>) got;
            if (w.size() != g.size()) return false;
            String et = type.equals("vector") ? "float4" : type.substring(0, type.length() - 2);
            for (int i = 0; i < w.size(); i++) if (!cellEquals(et, w.get(i), g.get(i))) return false;
            return true;
        }
        String ws = String.valueOf(want), gs = String.valueOf(got);
        switch (type) {
            case "float4": return Float.compare(Float.parseFloat(ws), Float.parseFloat(gs)) == 0;
            case "float8": return Double.compare(Double.parseDouble(ws), Double.parseDouble(gs)) == 0;
            default: return ws.equals(gs);
        }
    }

    /** Canonical float text (§4): NaN / Infinity / -Infinity, else the shortest round-trip decimal. */
    public static String canonFloat(double d) {
        if (Double.isNaN(d)) return "NaN";
        if (Double.isInfinite(d)) return d > 0 ? "Infinity" : "-Infinity";
        return new java.math.BigDecimal(Double.toString(d)).stripTrailingZeros().toPlainString();
    }

    public static String canonFloat4(float f) {
        if (Float.isNaN(f)) return "NaN";
        if (Float.isInfinite(f)) return f > 0 ? "Infinity" : "-Infinity";
        return new java.math.BigDecimal(Float.toString(f)).stripTrailingZeros().toPlainString();
    }

    public static String hex(byte[] b) {
        StringBuilder s = new StringBuilder(2 + b.length * 2).append("\\x");
        for (byte x : b) s.append(Character.forDigit((x >> 4) & 0xF, 16)).append(Character.forDigit(x & 0xF, 16));
        return s.toString();
    }

    public static byte[] unhex(String s) {
        if (!s.startsWith("\\x")) throw new IllegalArgumentException("bytea canonical form is \\x<hex>: " + s);
        int n = (s.length() - 2) / 2;
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) b[i] = (byte) Integer.parseInt(s.substring(2 + 2 * i, 4 + 2 * i), 16);
        return b;
    }

    /** {@code YYYY-MM-DD HH:MM:SS[.ffffff]} with the fraction's trailing zeros trimmed (PostgreSQL's text form). */
    public static String canonTimestamp(java.time.LocalDateTime t) {
        String base = String.format("%04d-%02d-%02d %02d:%02d:%02d", t.getYear(), t.getMonthValue(), t.getDayOfMonth(),
                t.getHour(), t.getMinute(), t.getSecond());
        int nanos = t.getNano();
        if (nanos == 0) return base;
        String f = String.format("%09d", nanos).replaceAll("0+$", "");
        return base + "." + f;
    }

    private Harness() {}
}
