package org.particledb;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The driver must need NO JVM flag on any JDK 11+, for any result the server
 * can send, and the old {@code --add-opens=java.base/java.nio=ALL-UNNAMED}
 * must be a harmless no-op. Each check runs a CHILD JVM whose command line and
 * environment carry exactly the flags under test (surefire's own options never
 * reach it):
 *
 * <ul>
 *   <li>no flags, Apache Arrow Java removed from the classpath: every golden
 *       stream decodes, and a live session reads numeric SUM/AVG, timestamp,
 *       timestamptz, date, time, decimal and array results and runs a bulk
 *       {@code executeBatch};</li>
 *   <li>the same with the flag;</li>
 *   <li>{@code resultDecoder=arrow}: works with the flag; on JDK 16+ without
 *       it fails naming the flag; without Arrow Java on the classpath fails
 *       saying so.</li>
 * </ul>
 */
class ArrowModuleFlagsTest {

    /** label, SQL. The child prints {@code OK <label> <value>} or {@code ERR <label> <message>}. */
    static final String[][] LIVE = {
        {"sum", "SELECT SUM(v) FROM module_flags"},
        {"avg", "SELECT AVG(v) FROM module_flags"},
        {"timestamp", "SELECT CAST('2024-01-01 01:02:03.456789' AS TIMESTAMP)"},
        {"timestamptz", "SELECT CAST('2024-01-01 01:02:03+05' AS TIMESTAMPTZ)"},
        {"date", "SELECT CAST('2024-02-29' AS DATE)"},
        {"time", "SELECT CAST('12:34:56.789' AS TIME)"},
        {"decimal", "SELECT CAST(12345.678 AS NUMERIC(8,3))"},
        {"array", "SELECT ARRAY[1,2,3]"},
        {"textarray", "SELECT ARRAY['a','b c']"},
    };

    static final Map<String, String> EXPECTED = new LinkedHashMap<>();
    static {
        EXPECTED.put("plain", "1:first;2:second;");
        EXPECTED.put("sum", "30");
        EXPECTED.put("avg", "15");
        EXPECTED.put("timestamp", "2024-01-01T01:02:03.456789");
        EXPECTED.put("timestamptz", "2023-12-31T20:02:03Z");
        EXPECTED.put("date", "2024-02-29");
        EXPECTED.put("time", "12:34:56.789");
        EXPECTED.put("decimal", "12345.678");
        EXPECTED.put("array", "[1, 2, 3]");
        EXPECTED.put("textarray", "[a, b c]");
        EXPECTED.put("bulk", "100 true");
        EXPECTED.put("hikari", "30");
    }

    /** Child-JVM entry point. */
    public static class Child {
        public static void main(String[] args) throws Exception {
            boolean arrowPresent;
            try {
                Class.forName("org.apache.arrow.vector.ipc.ArrowStreamReader");
                arrowPresent = true;
            } catch (ClassNotFoundException e) {
                arrowPresent = false;
            }
            System.out.println("ARROW " + (arrowPresent ? "present" : "absent"));
            if ("decode".equals(args[0])) {
                ArrowDecoder.ResultDecoder mode = ArrowDecoder.ResultDecoder.parse(args[1]);
                for (int i = 2; i < args.length; i++) {
                    byte[] payload = Files.readAllBytes(Path.of(args[i]));
                    try {
                        ArrowDecoder.Decoded d = ArrowDecoder.decodePositional(payload, mode);
                        System.out.println("OK decode " + d.rows.size());
                    } catch (RuntimeException e) {
                        System.out.println("ERR decode " + e.getMessage());
                    }
                }
                return;
            }
            // args[2]: a per-run suffix, so runs against one server use their own tables.
            String flags = "module_flags_" + args[2];
            try (Connection c = DriverManager.getConnection(args[1]); Statement s = c.createStatement()) {
                s.execute("CREATE TABLE " + flags + " (id BIGINT PRIMARY KEY, name TEXT, v BIGINT)");
                s.execute("INSERT INTO " + flags + " VALUES (1, 'first', 10), (2, 'second', 20)");
                try {
                    StringBuilder plain = new StringBuilder();
                    try (ResultSet r = s.executeQuery("SELECT id, name FROM " + flags + " ORDER BY id")) {
                        while (r.next()) plain.append(r.getLong(1)).append(':').append(r.getString(2)).append(';');
                    }
                    System.out.println("OK plain " + plain);
                } catch (Exception e) {
                    System.out.println("ERR plain " + e.getMessage());
                }
                for (String[] q : LIVE) {
                    // Twice: the attempt after a failure is checked too.
                    for (int i = 0; i < 2; i++) {
                        try (ResultSet r = s.executeQuery(q[1].replace("module_flags", flags))) {
                            r.next();
                            System.out.println("OK " + q[0] + " " + render(q[0], r));
                        } catch (Exception e) {
                            System.out.println("ERR " + q[0] + " " + e.getMessage());
                        }
                    }
                }
                // Through a connection pool (HikariCP, Conductor's pool): the
                // pool must load the driver's classes without Arrow Java too.
                try {
                    com.zaxxer.hikari.HikariConfig hc = new com.zaxxer.hikari.HikariConfig();
                    hc.setJdbcUrl(args[1]);
                    hc.setMaximumPoolSize(2);
                    try (com.zaxxer.hikari.HikariDataSource ds = new com.zaxxer.hikari.HikariDataSource(hc);
                         Connection pc = ds.getConnection(); Statement ps = pc.createStatement();
                         ResultSet r = ps.executeQuery("SELECT SUM(v) FROM " + flags)) {
                        r.next();
                        System.out.println("OK hikari " + r.getBigDecimal(1).stripTrailingZeros().toPlainString());
                    }
                } catch (Exception e) {
                    System.out.println("ERR hikari " + e);
                }
                try {
                    String bulkTable = "module_bulk_" + args[2];
                    s.execute("CREATE TABLE " + bulkTable + " (id BIGINT PRIMARY KEY, t TEXT, n NUMERIC(10,2))");
                    try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + bulkTable + " VALUES (?, ?, ?)")) {
                        for (int i = 0; i < 100; i++) {
                            ps.setLong(1, i);
                            ps.setString(2, "t" + i);
                            ps.setBigDecimal(3, new java.math.BigDecimal(i + ".25"));
                            ps.addBatch();
                        }
                        ps.executeBatch();
                    }
                    try (ResultSet r = s.executeQuery("SELECT COUNT(*), SUM(n) FROM " + bulkTable)) {
                        r.next();
                        boolean bulk = c.unwrap(PdbConnection.class).bulkBatchesLoaded() > 0;
                        boolean sum = r.getBigDecimal(2).compareTo(new java.math.BigDecimal("4975.00")) == 0;
                        System.out.println("OK bulk " + r.getLong(1) + " " + (bulk && sum));
                    }
                } catch (Exception e) {
                    System.out.println("ERR bulk " + e.getMessage());
                }
            }
        }

        static String render(String label, ResultSet r) throws Exception {
            switch (label) {
                case "sum": case "avg": case "decimal":
                    return r.getBigDecimal(1).stripTrailingZeros().toPlainString();
                case "timestamp": return r.getTimestamp(1).toLocalDateTime().toString();
                case "timestamptz": return r.getObject(1, java.time.OffsetDateTime.class).toInstant().toString();
                case "date": return r.getDate(1).toLocalDate().toString();
                case "time": return r.getString(1);
                default: return Arrays.toString((Object[]) r.getArray(1).getArray());
            }
        }
    }

    private static String classpath(boolean withArrow) {
        List<String> keep = new ArrayList<>();
        for (String e : System.getProperty("java.class.path").split(File.pathSeparator)) {
            if (!withArrow && (e.contains("/org/apache/arrow/") || e.contains(File.separator + "arrow-"))) continue;
            keep.add(e);
        }
        return String.join(File.pathSeparator, keep);
    }

    private static List<String> runChild(boolean withArrow, List<String> jvmFlags, String... args) throws Exception {
        Path log = Files.createTempFile("pdb-module-flags-", ".log");
        List<String> command = new ArrayList<>();
        command.add(System.getProperty("java.home") + "/bin/java");
        command.addAll(jvmFlags);
        command.addAll(List.of("-cp", classpath(withArrow), Child.class.getName()));
        command.addAll(List.of(args));
        ProcessBuilder pb = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());
        // No module flags may reach the child from the environment either.
        pb.environment().remove("JDK_JAVA_OPTIONS");
        pb.environment().remove("JAVA_TOOL_OPTIONS");
        pb.environment().remove("_JAVA_OPTIONS");
        Process p = pb.start();
        try {
            assertTrue(p.waitFor(120, TimeUnit.SECONDS), "child JVM timed out");
            List<String> lines = Files.readAllLines(log);
            assertEquals(0, p.exitValue(), String.join("\n", lines));
            return lines;
        } finally {
            if (p.isAlive()) p.destroyForcibly();
            Files.deleteIfExists(log);
        }
    }

    private static final List<String> NO_FLAGS = List.of();
    private static final List<String> FLAG = List.of(ArrowDecoder.ADD_OPENS_FLAG);

    private static List<String> goldenFiles(Path dir) throws Exception {
        List<String> files = new ArrayList<>();
        for (GoldenCases.Case c : GoldenCases.ALL) {
            Path f = dir.resolve(c.name + ".arrow");
            Files.write(f, ArrowIpcReaderGoldenTest.golden(c.name));
            files.add(f.toString());
        }
        return files;
    }

    private static void assertAllDecoded(List<String> lines, int n, String arrow) {
        String all = String.join("\n", lines);
        assertTrue(lines.contains("ARROW " + arrow), all);
        long ok = lines.stream().filter(l -> l.startsWith("OK decode ")).count();
        assertEquals(n, ok, all);
    }

    @Test
    void everyGoldenDecodesOnAPlainJvmWithoutArrowJava() throws Exception {
        Path dir = Files.createTempDirectory("pdb-golden-");
        try {
            List<String> files = goldenFiles(dir);
            List<String> args = new ArrayList<>(List.of("decode", "auto"));
            args.addAll(files);
            assertAllDecoded(runChild(false, NO_FLAGS, args.toArray(new String[0])), files.size(), "absent");
            args.set(1, "builtin");
            assertAllDecoded(runChild(false, FLAG, args.toArray(new String[0])), files.size(), "absent");
            assertAllDecoded(runChild(true, FLAG, args.toArray(new String[0])), files.size(), "present");
            // resultDecoder=arrow without Arrow Java: a clear error per payload.
            args.set(1, "arrow");
            List<String> lines = runChild(false, NO_FLAGS, args.toArray(new String[0]));
            String all = String.join("\n", lines);
            long err = lines.stream().filter(l -> l.startsWith("ERR decode ")
                    && l.contains("Arrow Java is not on the classpath")).count();
            assertEquals(files.size(), err, all);
        } finally {
            for (File f : dir.toFile().listFiles()) f.delete();
            Files.deleteIfExists(dir);
        }
    }

    private static void assertLive(List<String> lines, String arrow) {
        String all = String.join("\n", lines);
        assertTrue(lines.contains("ARROW " + arrow), all);
        for (Map.Entry<String, String> e : EXPECTED.entrySet()) {
            long hits = lines.stream().filter(l -> l.equals("OK " + e.getKey() + " " + e.getValue())).count();
            long want = e.getKey().equals("plain") || e.getKey().equals("bulk") || e.getKey().equals("hikari") ? 1 : 2;
            assertEquals(want, hits, e.getKey() + ":\n" + all);
        }
        assertFalse(all.contains("ERR "), all);
    }

    @Test
    void liveResultsNeedNoFlagAndTheFlagIsHarmless() throws Exception {
        PdbTestServer server = PdbTestServer.tryStart();
        assumeTrue(server != null, "PDB_BIN required");
        try {
            String url = "jdbc:particledb://127.0.0.1:" + server.port + "/";
            assertLive(runChild(false, NO_FLAGS, "live", url + "test", "noflag_noarrow"), "absent");
            assertLive(runChild(true, NO_FLAGS, "live", url + "test", "noflag_arrow"), "present");
            assertLive(runChild(true, FLAG, "live", url + "test", "flag_arrow"), "present");
            assertLive(runChild(false, FLAG, "live", url + "test?resultDecoder=builtin", "flag_noarrow"), "absent");
        } finally {
            server.stop();
        }
    }

    @Test
    void arrowDecoderNeedsTheFlagOnJdk16AndArrowOnTheClasspath() throws Exception {
        PdbTestServer server = PdbTestServer.tryStart();
        assumeTrue(server != null, "PDB_BIN required");
        try {
            String url = "jdbc:particledb://127.0.0.1:" + server.port + "/";
            // With Arrow Java and the flag: identical answers.
            assertLive(runChild(true, FLAG, "live", url + "test?resultDecoder=ARROW", "arrow_flag"), "present");
            // Without the flag: JDK 11-15 permit the access; 16+ fail naming the flag.
            List<String> lines = runChild(true, NO_FLAGS, "live", url + "test?resultDecoder=arrow", "arrow_noflag");
            String all = String.join("\n", lines);
            if (Runtime.version().feature() >= 16) {
                assertTrue(lines.stream().anyMatch(l -> l.startsWith("ERR sum ")
                        && l.contains(ArrowDecoder.ADD_OPENS_FLAG + " ")), all);
            } else {
                assertLive(lines, "present");
            }
            // Without Arrow Java: every row-returning query says so.
            lines = runChild(false, FLAG, "live", url + "test?resultDecoder=arrow", "arrow_absent");
            all = String.join("\n", lines);
            assertTrue(lines.contains("ARROW absent"), all);
            assertTrue(lines.stream().anyMatch(l -> l.startsWith("ERR sum ")
                    && l.contains("Arrow Java is not on the classpath")), all);
        } finally {
            server.stop();
        }
    }
}
