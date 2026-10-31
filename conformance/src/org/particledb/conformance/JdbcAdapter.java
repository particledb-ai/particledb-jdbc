package org.particledb.conformance;

import static org.particledb.conformance.Harness.check;
import static org.particledb.conformance.Harness.eq;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.particledb.PdbConnection;
import org.particledb.PdbPreparedStatement;

/**
 * The conformance adapter for the JDBC driver ({@code org.particledb.Driver}):
 * the REFERENCE implementation every other SDK's adapter is measured against.
 *
 * <p>Every connection is opened the documented zero-option way
 * ({@code DriverManager.getConnection("jdbc:particledb://HOST:PORT/")}, no
 * properties). CONC-005 disables the optional client rejection cap to isolate
 * server admission; the other cases test connection defaults.
 */
public final class JdbcAdapter implements Harness.Driver {

    static String url() { return "jdbc:particledb://" + Harness.HOST + ":" + Harness.port() + "/"; }

    static Connection connect() throws SQLException { return DriverManager.getConnection(url()); }

    @Override public String sdk() { return "java-jdbc"; }

    @Override public String version() {
        Package p = org.particledb.Driver.class.getPackage();
        String v = p == null ? null : p.getImplementationVersion();
        return v == null ? "source" : v;
    }

    @Override public String sqlState(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof SQLException && ((SQLException) c).getSQLState() != null) return ((SQLException) c).getSQLState();
        }
        return null;
    }

    @Override public Harness.Session open() throws Exception { return new JdbcSession(connect()); }

    // ── script sessions ────────────────────────────────────────────────────

    static final class JdbcSession implements Harness.Session {
        final Connection c;
        final Map<String, Savepoint> sps = new HashMap<>();

        JdbcSession(Connection c) { this.c = c; }

        @Override public void exec(String sql, List<Harness.Param> params) throws Exception {
            if (params.isEmpty()) {
                try (Statement s = c.createStatement()) { s.execute(sql); }
            } else {
                try (PreparedStatement ps = c.prepareStatement(toJdbc(sql))) { bind(c, ps, params); ps.execute(); }
            }
        }

        @Override public List<List<Object>> query(String sql, List<Harness.Param> params, List<String> types) throws Exception {
            if (params.isEmpty()) {
                try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) { return rows(rs, types); }
            }
            try (PreparedStatement ps = c.prepareStatement(toJdbc(sql))) {
                bind(c, ps, params);
                try (ResultSet rs = ps.executeQuery()) { return rows(rs, types); }
            }
        }

        @Override public void begin() throws Exception { c.setAutoCommit(false); }
        @Override public void commit() throws Exception { try { c.commit(); } finally { c.setAutoCommit(true); } }
        @Override public void rollback() throws Exception { try { c.rollback(); } finally { c.setAutoCommit(true); } }

        @Override public void isolation(String level) throws Exception {
            switch (level) {
                case "read committed": c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED); break;
                case "repeatable read": c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ); break;
                case "serializable": c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE); break;
                default: throw new IllegalArgumentException(level);
            }
        }

        @Override public void savepoint(String name) throws Exception { sps.put(name, c.setSavepoint(name)); }
        @Override public void rollbackTo(String name) throws Exception { c.rollback(sps.get(name)); }
        @Override public void close() throws Exception { c.close(); }
    }

    /** {@code $1..$N} (case-file spelling) to JDBC {@code ?}, in order. */
    static String toJdbc(String sql) { return sql.replaceAll("\\$\\d+", "?"); }

    @SuppressWarnings("unchecked")
    static void bind(Connection c, PreparedStatement ps, List<Harness.Param> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            Harness.Param p = params.get(i);
            int k = i + 1;
            if (p.value == null) { ps.setNull(k, sqlType(p.type)); continue; }
            if (p.type.endsWith("[]")) {
                String et = p.type.substring(0, p.type.length() - 2);
                List<Object> vs = (List<Object>) p.value;
                Object[] arr = new Object[vs.size()];
                for (int j = 0; j < arr.length; j++) arr[j] = vs.get(j) == null ? null : scalar(et, (String) vs.get(j));
                ps.setArray(k, c.createArrayOf(et, arr));
                continue;
            }
            String v = (String) p.value;
            switch (p.type) {
                case "int2": ps.setShort(k, Short.parseShort(v)); break;
                case "int4": ps.setInt(k, Integer.parseInt(v)); break;
                case "int8": ps.setLong(k, Long.parseLong(v)); break;
                case "float4": ps.setFloat(k, Float.parseFloat(v)); break;
                case "float8": ps.setDouble(k, Double.parseDouble(v)); break;
                case "numeric": ps.setBigDecimal(k, new BigDecimal(v)); break;
                case "bool": ps.setBoolean(k, Boolean.parseBoolean(v)); break;
                case "bytea": ps.setBytes(k, Harness.unhex(v)); break;
                case "date": ps.setObject(k, LocalDate.parse(v)); break;
                case "timestamp": ps.setObject(k, LocalDateTime.parse(v.replace(' ', 'T'))); break;
                case "timestamptz": ps.setObject(k, parseTz(v)); break;
                case "uuid": ps.setObject(k, UUID.fromString(v)); break;
                default: ps.setString(k, v); // text, varchar, json, jsonb
            }
        }
    }

    static Object scalar(String t, String v) {
        switch (t) {
            case "int2": return Short.parseShort(v);
            case "int4": return Integer.parseInt(v);
            case "int8": return Long.parseLong(v);
            case "float4": return Float.parseFloat(v);
            case "float8": return Double.parseDouble(v);
            case "numeric": return new BigDecimal(v);
            case "bool": return Boolean.parseBoolean(v);
            default: return v;
        }
    }

    static OffsetDateTime parseTz(String v) {
        // "YYYY-MM-DD HH:MM:SS[.f]+00"
        String iso = v.replace(' ', 'T');
        if (iso.matches(".*[+-]\\d\\d$")) iso = iso + ":00";
        return OffsetDateTime.parse(iso);
    }

    static int sqlType(String t) {
        switch (t) {
            case "int2": return Types.SMALLINT;
            case "int4": return Types.INTEGER;
            case "int8": return Types.BIGINT;
            case "float4": return Types.REAL;
            case "float8": return Types.DOUBLE;
            case "numeric": return Types.NUMERIC;
            case "bool": return Types.BOOLEAN;
            case "bytea": return Types.BINARY;
            case "date": return Types.DATE;
            case "timestamp": return Types.TIMESTAMP;
            case "timestamptz": return Types.TIMESTAMP_WITH_TIMEZONE;
            case "uuid": return Types.OTHER;
            default: return t.endsWith("[]") ? Types.ARRAY : Types.VARCHAR;
        }
    }

    static List<List<Object>> rows(ResultSet rs, List<String> types) throws SQLException {
        List<List<Object>> out = new ArrayList<>();
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 0; i < types.size(); i++) row.add(cell(rs, i + 1, types.get(i)));
            out.add(row);
        }
        return out;
    }

    /** Read column {@code i} with the typed getter an application would use, and render it canonically. */
    static Object cell(ResultSet rs, int i, String type) throws SQLException {
        if (type.endsWith("[]")) {
            Array a = rs.getArray(i);
            if (a == null) return null;
            Object[] els = (Object[]) a.getArray();
            List<Object> l = new ArrayList<>();
            String et = type.substring(0, type.length() - 2);
            for (Object e : els) l.add(e == null ? null : canonScalar(et, e));
            return l;
        }
        switch (type) {
            case "int2": case "int4": case "int8": { long v = rs.getLong(i); return rs.wasNull() ? null : Long.toString(v); }
            case "float4": { float v = rs.getFloat(i); return rs.wasNull() ? null : Harness.canonFloat4(v); }
            case "float8": { double v = rs.getDouble(i); return rs.wasNull() ? null : Harness.canonFloat(v); }
            case "numeric": { BigDecimal v = rs.getBigDecimal(i); return v == null ? null : v.toPlainString(); }
            case "bool": { boolean v = rs.getBoolean(i); return rs.wasNull() ? null : Boolean.toString(v); }
            case "bytea": { byte[] v = rs.getBytes(i); return v == null ? null : Harness.hex(v); }
            case "date": { LocalDate v = rs.getObject(i, LocalDate.class); return v == null ? null : v.toString(); }
            case "timestamp": { LocalDateTime v = rs.getObject(i, LocalDateTime.class); return v == null ? null : Harness.canonTimestamp(v); }
            case "timestamptz": {
                OffsetDateTime v = rs.getObject(i, OffsetDateTime.class);
                return v == null ? null : Harness.canonTimestamp(v.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime()) + "+00";
            }
            case "uuid": { Object v = rs.getObject(i); return v == null ? null : v.toString().toLowerCase(); }
            case "vector": {
                Object v = rs.getObject(i);
                if (v == null) return null;
                return vectorCells(v);
            }
            default: return rs.getString(i);
        }
    }

    static List<Object> vectorCells(Object v) throws SQLException {
        List<Object> l = new ArrayList<>();
        if (v instanceof org.particledb.PdbVector) v = ((org.particledb.PdbVector) v).toArray();
        if (v instanceof float[]) { for (float f : (float[]) v) l.add(Harness.canonFloat4(f)); return l; }
        if (v instanceof Array) v = ((Array) v).getArray();
        if (v instanceof Object[]) {
            for (Object o : (Object[]) v) l.add(o == null ? null : Harness.canonFloat4(((Number) o).floatValue()));
            return l;
        }
        String s = v.toString().trim();
        if (s.startsWith("[") && s.endsWith("]")) {
            for (String p : s.substring(1, s.length() - 1).split(",")) if (!p.isBlank()) l.add(Harness.canonFloat4(Float.parseFloat(p.trim())));
            return l;
        }
        throw new Harness.Fail("vector column decoded as " + v.getClass().getName() + ": " + v);
    }

    static Object canonScalar(String t, Object e) {
        switch (t) {
            case "float4": return Harness.canonFloat4(((Number) e).floatValue());
            case "float8": return Harness.canonFloat(((Number) e).doubleValue());
            case "numeric": return (e instanceof BigDecimal ? (BigDecimal) e : new BigDecimal(e.toString())).toPlainString();
            default: return e.toString();
        }
    }

    // ── helpers for custom cases ───────────────────────────────────────────

    static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) { s.execute(sql); }
    }

    static long scalarLong(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            check(rs.next(), "no row from " + sql);
            return rs.getLong(1);
        }
    }

    static String scalarString(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            check(rs.next(), "no row from " + sql);
            return rs.getString(1);
        }
    }

    static Set<String> caps(String csv) {
        Set<String> s = new TreeSet<>();
        for (String p : csv.split(",")) if (!p.isBlank()) s.add(p.trim());
        return s;
    }

    static PdbConnection pdb(Connection c) throws SQLException { return c.unwrap(PdbConnection.class); }

    static Resp resp() throws Exception { return new Resp(Harness.HOST, Harness.respPort()); }

    // ── custom cases ───────────────────────────────────────────────────────

    final Map<String, Harness.CustomCase> cases = new LinkedHashMap<>();

    @SuppressWarnings("unchecked")
    JdbcAdapter(Map<String, Map<String, Object>> caseDefs) {
        List<String> required = (List<String>) (Object) caseDefs.get("DEF-002").get("required_capabilities");

        cases.put("DEF-001", () -> {
            try (Connection c = connect()) {
                eq("pdb-native-v2", scalarString(c, "SHOW pdb.wire_protocol"), "server-recorded protocol");
            }
        });
        cases.put("DEF-002", () -> {
            try (Connection c = connect()) {
                Set<String> got = caps(scalarString(c, "SHOW pdb.wire_capabilities"));
                List<String> missing = new ArrayList<>();
                for (String r : required) if (!got.contains(r)) missing.add(r);
                check(missing.isEmpty(), "server recorded " + got + "; missing required " + missing);
            }
        });
        cases.put("DEF-003", () -> {
            try (Connection c = connect()) {
                eq("arrow_ipc", scalarString(c, "SHOW pdb.wire_result_format"), "server-recorded result format");
            }
        });
        cases.put("DEF-004", () -> {
            try (Connection c = connect()) {
                PdbConnection p = pdb(c);
                eq(scalarString(c, "SHOW pdb.wire_protocol"), p.negotiatedProtocol(), "client protocol vs server record");
                eq(caps(scalarString(c, "SHOW pdb.wire_capabilities")), new TreeSet<>(p.negotiatedCapabilities()),
                        "client capability set vs server record");
            }
        });
        cases.put("DEF-005", () -> {
            eq(Harness.defaultPort(), org.particledb.Driver.DEFAULT_PORT, "driver default port vs server default port");
            if (Harness.port() != Harness.defaultPort()) {
                throw new Harness.Skip("default-port constant checked; the port-less connect needs the runner's server on "
                        + Harness.defaultPort() + " but that port was busy (server on " + Harness.port() + ")");
            }
            try (Connection c = DriverManager.getConnection("jdbc:particledb://" + Harness.HOST + "/")) {
                eq("pdb-native-v2", scalarString(c, "SHOW pdb.wire_protocol"), "port-less connect protocol");
            }
        });
        cases.put("DEF-006", () -> {
            String hostile = "o'clock; DROP TABLE x -- ";
            try (Connection c = connect(); PreparedStatement ps = c.prepareStatement("SELECT ?::text || ''")) {
                for (int i = 0; i < 10; i++) {
                    ps.setString(1, hostile);
                    try (ResultSet rs = ps.executeQuery()) {
                        check(rs.next(), "no row");
                        eq(hostile, rs.getString(1), "bound text param, execution " + i);
                    }
                }
                check(ps.unwrap(PdbPreparedStatement.class).isServerPrepared(),
                        "statement still not server-prepared after 10 executions");
            }
        });

        // ── SQL custom ──
        cases.put("SQL-PREP-001", () -> {
            try (Connection c = connect()) {
                exec(c, "DROP TABLE IF EXISTS conf_prep1");
                exec(c, "CREATE TABLE conf_prep1 (id int8 PRIMARY KEY, v text)");
                exec(c, "INSERT INTO conf_prep1 SELECT g, 'v' || g FROM generate_series(1, 50) g");
                try (PreparedStatement ps = c.prepareStatement("SELECT v FROM conf_prep1 WHERE id = ?")) {
                    for (int pass = 0; pass < 2; pass++) {
                        for (int k = 1; k <= 50; k++) {
                            int id = pass == 0 ? k : 51 - k;
                            ps.setLong(1, id);
                            try (ResultSet rs = ps.executeQuery()) {
                                check(rs.next(), "no row for id " + id);
                                eq("v" + id, rs.getString(1), "id " + id);
                                check(!rs.next(), "more than one row for id " + id);
                            }
                        }
                    }
                }
            }
        });
        cases.put("SQL-PREP-002", () -> {
            try (Connection c = connect()) {
                exec(c, "DROP TABLE IF EXISTS conf_prep2");
                exec(c, "CREATE TABLE conf_prep2 (id int8 PRIMARY KEY, a text)");
                exec(c, "INSERT INTO conf_prep2 VALUES (1, 'x')");
                try (PreparedStatement ps = c.prepareStatement("SELECT * FROM conf_prep2 WHERE id = ?")) {
                    for (int i = 0; i < 6; i++) {
                        ps.setLong(1, 1);
                        try (ResultSet rs = ps.executeQuery()) { check(rs.next(), "row"); eq(2, rs.getMetaData().getColumnCount(), "width before ALTER"); }
                    }
                    exec(c, "ALTER TABLE conf_prep2 ADD COLUMN b int8");
                    exec(c, "UPDATE conf_prep2 SET b = 5");
                    ps.setLong(1, 1);
                    try (ResultSet rs = ps.executeQuery()) {
                        check(rs.next(), "row after ALTER");
                        eq(3, rs.getMetaData().getColumnCount(), "width after ALTER (stale describe?)");
                        eq(5L, rs.getLong(3), "column b after ALTER");
                    } catch (SQLException e) {
                        String st = e.getSQLState();
                        check("0A000".equals(st) || "26000".equals(st), "after ALTER: unexpected " + e + " [" + st + "]");
                    }
                }
            }
        });
        cases.put("SQL-BATCH-001", () -> {
            try (Connection c = connect()) {
                exec(c, "DROP TABLE IF EXISTS conf_b1");
                exec(c, "CREATE TABLE conf_b1 (id int8 PRIMARY KEY, v text)");
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO conf_b1 VALUES (?, ?)")) {
                    for (int i = 1; i <= 1000; i++) { ps.setLong(1, i); ps.setString(2, "r" + i); ps.addBatch(); }
                    int[] counts = ps.executeBatch();
                    eq(1000, counts.length, "per-row count entries");
                    for (int i = 0; i < counts.length; i++) {
                        check(counts[i] == 1, "count[" + i + "] = " + counts[i] + " (expected 1)");
                    }
                }
                eq(1000L, scalarLong(c, "SELECT count(*) FROM conf_b1"), "rows");
                eq(500500L, scalarLong(c, "SELECT sum(id) FROM conf_b1"), "sum");
            }
        });
        cases.put("SQL-BATCH-002", () -> {
            try (Connection c = connect()) {
                exec(c, "DROP TABLE IF EXISTS conf_b2");
                exec(c, "CREATE TABLE conf_b2 (id int8 PRIMARY KEY)");
                c.setAutoCommit(false);
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO conf_b2 VALUES (?)")) {
                    for (long id : new long[] {1, 2, 1, 3}) { ps.setLong(1, id); ps.addBatch(); }
                    try {
                        ps.executeBatch();
                        throw new Harness.Fail("batch with a duplicate key succeeded");
                    } catch (BatchUpdateException e) {
                        eq("23505", e.getSQLState(), "batch failure SQLSTATE (" + e.getMessage() + ")");
                    }
                }
                c.rollback();
                c.setAutoCommit(true);
                eq(0L, scalarLong(c, "SELECT count(*) FROM conf_b2"), "rows after rollback");
            }
        });
        cases.put("SQL-STREAM-001", () -> {
            long max = Runtime.getRuntime().maxMemory();
            check(max <= 512L * 1024 * 1024, "streaming case must run with a bounded heap (-Xmx<=512m), have " + (max >> 20) + " MiB");
            try (Connection c = connect(); Statement s = c.createStatement()) {
                s.setFetchSize(10000);
                long t0 = System.nanoTime(), first = -1, n = 0, sum = 0;
                try (ResultSet rs = s.executeQuery("SELECT g, repeat('x', 100) FROM generate_series(1, 3000000) g")) {
                    while (rs.next()) {
                        if (first < 0) first = System.nanoTime() - t0;
                        n++;
                        sum += rs.getLong(1);
                    }
                }
                long total = System.nanoTime() - t0;
                eq(3000000L, n, "rows");
                eq(4500001500000L, sum, "sum");
                check(first < total / 2, "first row only after " + first / 1_000_000 + " ms of " + total / 1_000_000
                        + " ms total: the result was buffered, not streamed");
            }
        });
        cases.put("SQL-MULTI-001", () -> {
            try (Connection c = connect()) {
                exec(c, "DROP TABLE IF EXISTS conf_m1");
                exec(c, "CREATE TABLE conf_m1 (id int8 PRIMARY KEY)");
                try (Statement s = c.createStatement()) {
                    s.execute("INSERT INTO conf_m1 VALUES (1); INSERT INTO conf_m1 VALUES (1)");
                    throw new Harness.Fail("multi-statement with a duplicate key succeeded");
                } catch (SQLException e) {
                    eq("23505", e.getSQLState(), "multi-statement failure SQLSTATE (" + e.getMessage() + ")");
                }
                eq(0L, scalarLong(c, "SELECT count(*) FROM conf_m1"), "rows (implicit transaction must roll back statement 1)");
            }
        });
        cases.put("SQL-MULTI-002", () -> {
            try (Connection c = connect()) {
                exec(c, "DROP TABLE IF EXISTS conf_m2");
                exec(c, "CREATE TABLE conf_m2 (id int8 PRIMARY KEY)");
                try (Statement s = c.createStatement()) {
                    boolean isRs = s.execute("INSERT INTO conf_m2 VALUES (1); INSERT INTO conf_m2 VALUES (2); SELECT count(*) FROM conf_m2");
                    Long seen = null;
                    for (int guard = 0; guard < 10; guard++) {
                        if (isRs) {
                            try (ResultSet rs = s.getResultSet()) { if (rs.next()) seen = rs.getLong(1); }
                        } else if (s.getUpdateCount() == -1) {
                            break;
                        }
                        isRs = s.getMoreResults();
                    }
                    eq(2L, seen, "final SELECT result of the multi-statement string");
                }
            }
        });
        cases.put("SQL-TXN-005", () -> {
            try (Connection a = connect(); Connection b = connect()) {
                exec(a, "DROP TABLE IF EXISTS conf_x5");
                exec(a, "CREATE TABLE conf_x5 (id int8 PRIMARY KEY, v int8)");
                exec(a, "INSERT INTO conf_x5 VALUES (1, 0)");
                try (PreparedStatement up = a.prepareStatement("UPDATE conf_x5 SET v = ? WHERE id = 1");
                     PreparedStatement ra = a.prepareStatement("SELECT v FROM conf_x5 WHERE id = 1");
                     PreparedStatement rb = b.prepareStatement("SELECT v FROM conf_x5 WHERE id = 1")) {
                    for (int i = 1; i <= 200; i++) {
                        up.setLong(1, i);
                        eq(1, up.executeUpdate(), "update count " + i);
                        try (ResultSet rs = ra.executeQuery()) { rs.next(); eq((long) i, rs.getLong(1), "same-connection read " + i); }
                        try (ResultSet rs = rb.executeQuery()) { rs.next(); eq((long) i, rs.getLong(1), "other-connection read " + i); }
                    }
                }
            }
        });
        cases.put("SQL-CANCEL-001", () -> {
            try (Connection c = connect(); Statement s = c.createStatement()) {
                AtomicLong cancelledAt = new AtomicLong();
                Thread t = new Thread(() -> {
                    try { Thread.sleep(300); cancelledAt.set(System.nanoTime()); s.cancel(); }
                    catch (Exception e) { cancelError.set(e); }
                });
                cancelError.set(null);
                t.start();
                try {
                    s.executeQuery("SELECT pg_sleep(30)").close();
                    throw new Harness.Fail("long statement completed despite cancel"
                            + (cancelError.get() != null ? " (cancel() threw " + cancelError.get() + ")" : ""));
                } catch (SQLException e) {
                    if (cancelError.get() != null) throw new Harness.Fail("cancel() threw " + cancelError.get());
                    eq("57014", e.getSQLState(), "cancelled statement SQLSTATE (" + e.getMessage() + ")");
                    check(System.nanoTime() - cancelledAt.get() < 5_000_000_000L, "cancel took more than 5 s");
                }
                t.join();
                eq(1L, scalarLong(c, "SELECT 1"), "connection usable after cancel");
            }
        });
        cases.put("SQL-TIMEOUT-001", () -> {
            try (Connection c = connect()) {
                try (Statement s = c.createStatement()) {
                    s.setQueryTimeout(1);
                    long t0 = System.nanoTime();
                    try {
                        s.executeQuery("SELECT pg_sleep(30)").close();
                        throw new Harness.Fail("setQueryTimeout(1) statement completed");
                    } catch (SQLException e) {
                        eq("57014", e.getSQLState(), "setQueryTimeout SQLSTATE (" + e.getMessage() + ")");
                        check(System.nanoTime() - t0 < 6_000_000_000L, "setQueryTimeout(1) took > 6 s");
                    }
                }
                check(!c.isClosed() && c.isValid(2), "connection unusable after setQueryTimeout expiry");
                exec(c, "SET statement_timeout = '500ms'");
                try {
                    scalarString(c, "SELECT pg_sleep(30)");
                    throw new Harness.Fail("statement_timeout statement completed");
                } catch (SQLException e) {
                    eq("57014", e.getSQLState(), "statement_timeout SQLSTATE (" + e.getMessage() + ")");
                }
                exec(c, "RESET statement_timeout");
                eq(1L, scalarLong(c, "SELECT 1"), "connection usable after statement_timeout");
            }
        });

        // ── concurrency ──
        cases.put("CONC-001", () -> {
            try (Connection c = connect()) {
                exec(c, "DROP TABLE IF EXISTS conf_c1");
                exec(c, "CREATE TABLE conf_c1 (id int8 PRIMARY KEY, v text)");
                exec(c, "INSERT INTO conf_c1 SELECT g, 'row-' || g FROM generate_series(1, 1000) g");
                ExecutorService pool = Executors.newFixedThreadPool(32);
                ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();
                AtomicInteger ok = new AtomicInteger();
                List<Future<?>> fs = new ArrayList<>();
                for (int t = 0; t < 32; t++) {
                    final int seed = t;
                    fs.add(pool.submit(() -> {
                        Random r = new Random(seed);
                        for (int i = 0; i < 200; i++) {
                            int id = 1 + r.nextInt(1000);
                            try {
                                if ((i & 1) == 0) {
                                    try (PreparedStatement ps = c.prepareStatement("SELECT id, v FROM conf_c1 WHERE id = ?")) {
                                        ps.setLong(1, id);
                                        try (ResultSet rs = ps.executeQuery()) { verifyRow(rs, id, errors); }
                                    }
                                } else {
                                    try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT id, v FROM conf_c1 WHERE id = " + id)) {
                                        verifyRow(rs, id, errors);
                                    }
                                }
                                ok.incrementAndGet();
                            } catch (Exception e) {
                                errors.add(e.toString());
                            }
                        }
                    }));
                }
                for (Future<?> f : fs) f.get(120, TimeUnit.SECONDS);
                pool.shutdown();
                check(errors.isEmpty(), errors.size() + " errors, first: " + errors.peek());
                eq(6400, ok.get(), "successful statements");
                eq("pdb-native-v2", scalarString(c, "SHOW pdb.wire_protocol"), "protocol of the shared connection");
            }
        });
        cases.put("CONC-002", () -> {
            try (Connection c = connect()) {
                ExecutorService pool = Executors.newFixedThreadPool(9);
                long t0 = System.nanoTime();
                Future<Long> slow = pool.submit(() -> {
                    try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT pg_sleep(2)")) { rs.next(); }
                    return System.nanoTime();
                });
                Thread.sleep(100);
                List<Future<Long>> fast = new ArrayList<>();
                ConcurrentLinkedQueue<String> slowOnes = new ConcurrentLinkedQueue<>();
                for (int t = 0; t < 8; t++) {
                    fast.add(pool.submit(() -> {
                        long last = 0;
                        for (int i = 0; i < 20; i++) {
                            long s0 = System.nanoTime();
                            try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT 1")) { rs.next(); }
                            long d = System.nanoTime() - s0;
                            if (d > 1_000_000_000L) slowOnes.add((d / 1_000_000) + " ms");
                            last = System.nanoTime();
                        }
                        return last;
                    }));
                }
                long lastFast = 0;
                for (Future<Long> f : fast) lastFast = Math.max(lastFast, f.get(30, TimeUnit.SECONDS));
                long slowDone = slow.get(30, TimeUnit.SECONDS);
                pool.shutdown();
                check(slowOnes.isEmpty(), "fast statements blocked behind the slow one: " + slowOnes);
                check(lastFast < slowDone, "fast statements finished " + (lastFast - slowDone) / 1_000_000
                        + " ms AFTER the slow one (head-of-line blocking); slow took " + (slowDone - t0) / 1_000_000 + " ms");
            }
        });
        cases.put("CONC-003", () -> {
            try (Connection c = connect()) {
                ExecutorService pool = Executors.newFixedThreadPool(2);
                Future<long[]> a = pool.submit(() -> streamSum(c, "SELECT g FROM generate_series(1, 200000) g", 1));
                Future<long[]> b = pool.submit(() -> streamSum(c, "SELECT -g FROM generate_series(1, 200000) g", -1));
                long[] ra = a.get(120, TimeUnit.SECONDS), rb = b.get(120, TimeUnit.SECONDS);
                pool.shutdown();
                eq(200000L, ra[0], "stream A rows");
                eq(20000100000L, ra[1], "stream A sum");
                eq(0L, ra[2], "stream A out-of-order values");
                eq(200000L, rb[0], "stream B rows");
                eq(-20000100000L, rb[1], "stream B sum");
                eq(0L, rb[2], "stream B out-of-order values");
            }
        });
        cases.put("CONC-004", () -> {
            try (Connection a = connect(); Connection b = connect(); Connection z = connect()) {
                exec(z, "DROP TABLE IF EXISTS conf_c4");
                exec(z, "CREATE TABLE conf_c4 (id int8 PRIMARY KEY, owner text)");
                a.setAutoCommit(false);
                b.setAutoCommit(false);
                exec(a, "INSERT INTO conf_c4 VALUES (1, 'a')");
                exec(b, "INSERT INTO conf_c4 VALUES (2, 'b')");
                ExecutorService pool = Executors.newFixedThreadPool(16);
                List<Future<String>> fs = new ArrayList<>();
                for (int t = 0; t < 16; t++) {
                    Connection c = t % 2 == 0 ? a : b;
                    fs.add(pool.submit(() -> {
                        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT owner FROM conf_c4 ORDER BY id")) {
                            StringBuilder sb = new StringBuilder();
                            while (rs.next()) sb.append(rs.getString(1));
                            return sb.toString();
                        }
                    }));
                }
                for (int t = 0; t < 16; t++) eq(t % 2 == 0 ? "a" : "b", fs.get(t).get(60, TimeUnit.SECONDS), "thread " + t + " view inside its connection's transaction");
                pool.shutdown();
                a.commit();
                b.commit();
                a.setAutoCommit(true);
                b.setAutoCommit(true);
                eq("ab", scalarString(z, "SELECT string_agg(owner, '' ORDER BY id) FROM conf_c4"), "after both commits");
            }
        });
        cases.put("CONC-005", () -> {
            // Test server admission independently of the optional client rejection limit.
            try (Connection c = DriverManager.getConnection(url() + "?maxStreamsPerConn=0")) {
                int cap = pdb(c).maxConcurrentStreams();
                check(cap > 0 && cap < 4096, "implausible stream cap " + cap);
                int n = cap * 3;
                ExecutorService pool = Executors.newFixedThreadPool(n);
                CountDownLatch go = new CountDownLatch(1);
                List<Future<Long>> fs = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    fs.add(pool.submit(() -> {
                        go.await();
                        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT pg_sleep(0.05), 1")) {
                            rs.next();
                            return rs.getLong(2);
                        }
                    }));
                }
                go.countDown();
                for (Future<Long> f : fs) eq(1L, f.get(120, TimeUnit.SECONDS), "result past the stream cap (" + cap + ")");
                pool.shutdown();
            }
        });

        // ── key-value parity ──
        cases.put("KV-001", () -> {
            try (Connection c = connect(); Resp r = resp()) {
                PdbConnection p = pdb(c);
                p.kvSet(utf8("conf:kv1"), utf8("hello wörld"));
                eq("hello wörld", r.str("GET", "conf:kv1"), "RESP GET of SDK SET");
                r.cmd("SET", "conf:kv1b", "from resp");
                eq("from resp", str(p.kvGet(utf8("conf:kv1b"))), "SDK GET of RESP SET");
            }
        });
        cases.put("KV-002", () -> {
            try (Connection c = connect(); Resp r = resp()) {
                PdbConnection p = pdb(c);
                eq(null, p.kvGet(utf8("conf:kv2-absent")), "GET of an absent key");
                p.kvSet(utf8("conf:kv2"), utf8("x"));
                check(p.kvDel(utf8("conf:kv2")), "DEL of a present key reported not-deleted");
                eq(0L, r.num("EXISTS", "conf:kv2"), "RESP EXISTS after SDK DEL");
            }
        });
        cases.put("KV-003", () -> {
            try (Connection c = connect(); Resp r = resp()) {
                PdbConnection p = pdb(c);
                r.cmd("DEL", "conf:kv3");
                eq(1L, p.kvIncr(utf8("conf:kv3")), "SDK INCR of absent");
                eq(11L, r.num("INCRBY", "conf:kv3", "10"), "RESP INCRBY");
                eq(12L, p.kvIncr(utf8("conf:kv3")), "SDK INCR");
                eq("12", r.str("GET", "conf:kv3"), "RESP GET");
            }
        });
        cases.put("KV-004", () -> {
            try (Connection c = connect(); Resp r = resp()) {
                PdbConnection p = pdb(c);
                p.kvSet(utf8("conf:kv4"), utf8("v"));
                eq(1L, sqlLong(c, "SELECT KV_EXPIRE(?, ?)", "conf:kv4", 100L), "KV_EXPIRE");
                long ttl = r.num("TTL", "conf:kv4");
                check(ttl >= 99 && ttl <= 100, "RESP TTL after SDK expire 100: " + ttl);
                r.cmd("SET", "conf:kv4b", "w", "EX", "200");
                long t2 = sqlLong(c, "SELECT KV_TTL(?)", "conf:kv4b");
                check(t2 >= 199 && t2 <= 200, "SDK TTL after RESP EX 200: " + t2);
            }
        });
        cases.put("KV-005", () -> {
            try (Connection c = connect(); Resp r = resp()) {
                pdb(c).kvSet(utf8("conf:kv5"), utf8("v"));
                eq(-1L, sqlLong(c, "SELECT KV_TTL(?)", "conf:kv5"), "SDK TTL of a key with none");
                eq(-1L, r.num("TTL", "conf:kv5"), "RESP TTL of a key with none");
                eq(-2L, sqlLong(c, "SELECT KV_TTL(?)", "conf:kv5-absent"), "SDK TTL of a missing key");
                eq(-2L, r.num("TTL", "conf:kv5-absent"), "RESP TTL of a missing key");
            }
        });
        cases.put("KV-006", () -> {
            try (Connection c = connect(); Resp r = resp()) {
                r.cmd("SET", "conf:kv6", "a", "EX", "300");
                pdb(c).kvSet(utf8("conf:kv6"), utf8("b"));
                eq(-1L, r.num("TTL", "conf:kv6"), "RESP TTL after SDK plain SET");
                eq("b", r.str("GET", "conf:kv6"), "RESP GET");
            }
        });
        cases.put("KV-007", () -> {
            try (Connection c = connect(); Resp r = resp()) {
                PdbConnection p = pdb(c);
                p.kvSet(utf8("conf:kv7"), utf8("v"));
                sqlLong(c, "SELECT KV_EXPIRE(?, ?)", "conf:kv7", 1L);
                Thread.sleep(2200);
                eq(null, p.kvGet(utf8("conf:kv7")), "SDK GET of an expired key");
                eq(0L, r.num("EXISTS", "conf:kv7"), "RESP EXISTS of an expired key");
            }
        });
        cases.put("KV-008", () -> {
            byte[] all = new byte[256];
            for (int i = 0; i < 256; i++) all[i] = (byte) i;
            try (Connection c = connect(); Resp r = resp()) {
                pdb(c).kvSet(utf8("conf:kv8"), all);
                Object got = r.cmd("GET", "conf:kv8");
                check(got instanceof byte[] && Arrays.equals(all, (byte[]) got), "RESP GET returned different bytes");
                r.cmd("SET", utf8("conf:kv8b"), all);
                check(Arrays.equals(all, pdb(c).kvGet(utf8("conf:kv8b"))), "SDK GET of RESP-written binary differs");
            }
        });
        cases.put("KV-009", () -> {
            String big = "x".repeat(1 << 20);
            try (Connection c = connect(); Resp r = resp()) {
                pdb(c).kvSet(utf8("conf:kv9"), utf8(big));
                eq(1048576L, r.num("STRLEN", "conf:kv9"), "RESP STRLEN");
                eq(big, r.str("GET", "conf:kv9"), "RESP GET of 1 MiB");
            }
        });
        cases.put("KV-010", () -> {
            try (Connection c = connect(); Resp r = resp()) {
                r.cmd("DEL", "conf:kv10-absent");
                try (PreparedStatement ps = c.prepareStatement("SELECT KV_MSET(?, ?, ?, ?)")) {
                    ps.setString(1, "conf:kv10a"); ps.setString(2, "1"); ps.setString(3, "conf:kv10b"); ps.setString(4, "2");
                    ps.executeQuery().close();
                }
                Object m = r.cmd("MGET", "conf:kv10a", "conf:kv10b", "conf:kv10-absent");
                List<Object> l = (List<Object>) m;
                eq("1", new String((byte[]) l.get(0), StandardCharsets.UTF_8), "RESP MGET[0]");
                eq("2", new String((byte[]) l.get(1), StandardCharsets.UTF_8), "RESP MGET[1]");
                eq(null, l.get(2), "RESP MGET[2]");
                try (PreparedStatement ps = c.prepareStatement("SELECT KV_MGET(?, ?, ?)")) {
                    ps.setString(1, "conf:kv10a"); ps.setString(2, "conf:kv10-absent"); ps.setString(3, "conf:kv10b");
                    try (ResultSet rs = ps.executeQuery()) {
                        check(rs.next(), "no row");
                        Object[] vs = (Object[]) rs.getArray(1).getArray();
                        eq(Arrays.asList("1", null, "2"), Arrays.asList(vs), "SDK KV_MGET");
                    }
                }
            }
        });
        cases.put("KV-011", () -> {
            try (Connection c = connect(); Resp r = resp()) {
                r.cmd("DEL", "conf:h", "conf:l", "conf:s", "conf:z");
                sqlLong(c, "SELECT HASH_SET(?, ?, ?)", "conf:h", "f", "v");
                eq("v", r.str("HGET", "conf:h", "f"), "RESP HGET of SDK HASH_SET");
                sqlLong(c, "SELECT LIST_PUSH(?, ?)", "conf:l", "a");
                sqlLong(c, "SELECT LIST_PUSH(?, ?)", "conf:l", "b");
                List<Object> lr = (List<Object>) r.cmd("LRANGE", "conf:l", "0", "-1");
                eq(2, lr.size(), "RESP LRANGE length");
                sqlLong(c, "SELECT SET_ADD(?, ?)", "conf:s", "m");
                eq(1L, r.num("SISMEMBER", "conf:s", "m"), "RESP SISMEMBER of SDK SET_ADD");
                try (PreparedStatement ps = c.prepareStatement("SELECT ZSET_ADD(?, ?, ?)")) {
                    ps.setString(1, "conf:z"); ps.setDouble(2, 1.5); ps.setString(3, "m");
                    ps.executeQuery().close();
                }
                eq("1.5", r.str("ZSCORE", "conf:z", "m"), "RESP ZSCORE of SDK ZSET_ADD");
            }
        });
        cases.put("KV-012", () -> {
            try (Connection c = connect(); Resp r = resp()) {
                r.cmd("DEL", "conf:kv12");
                c.setAutoCommit(false);
                pdb(c).kvSet(utf8("conf:kv12"), utf8("kept"));
                c.rollback();
                c.setAutoCommit(true);
                eq("kept", r.str("GET", "conf:kv12"), "KV write after rollback");
            }
        });

        // ── durability ──
        cases.put("DUR-001", () -> {
            try (Connection c = connect()) {
                exec(c, "DROP TABLE IF EXISTS conf_dur1");
                exec(c, "CREATE TABLE conf_dur1 (id int8 PRIMARY KEY, v text)");
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO conf_dur1 VALUES (?, 'v')")) {
                    for (int i = 1; i <= 500; i++) { ps.setLong(1, i); ps.executeUpdate(); }
                }
            }
            crashRestart();
            try (Connection c = connect()) {
                eq(500L, scalarLong(c, "SELECT count(*) FROM conf_dur1"), "acked rows after kill -9");
                eq(125250L, scalarLong(c, "SELECT sum(id) FROM conf_dur1"), "acked ids after kill -9");
            }
        });
        cases.put("DUR-002", () -> {
            try (Connection c = connect()) {
                exec(c, "DROP TABLE IF EXISTS conf_dur2");
                exec(c, "CREATE TABLE conf_dur2 (id int8 PRIMARY KEY, v int8)");
                c.setAutoCommit(false);
                exec(c, "INSERT INTO conf_dur2 SELECT g, 1 FROM generate_series(1, 100) g");
                exec(c, "UPDATE conf_dur2 SET v = 101 WHERE id = 7");
                c.commit();
            }
            crashRestart();
            try (Connection c = connect()) {
                eq(100L, scalarLong(c, "SELECT count(*) FROM conf_dur2"), "rows of an acked COMMIT after kill -9");
                eq(200L, scalarLong(c, "SELECT sum(v) FROM conf_dur2"), "the COMMIT's UPDATE after kill -9");
            }
        });
        cases.put("DUR-003", () -> {
            try (Connection c = connect()) {
                exec(c, "DROP TABLE IF EXISTS conf_dur3");
                exec(c, "CREATE TABLE conf_dur3 (id int8 PRIMARY KEY)");
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO conf_dur3 VALUES (?)")) {
                    for (int i = 1; i <= 1000; i++) { ps.setLong(1, i); ps.addBatch(); }
                    ps.executeBatch();
                }
            }
            crashRestart();
            try (Connection c = connect()) {
                eq(1000L, scalarLong(c, "SELECT count(*) FROM conf_dur3"), "acked batch rows after kill -9");
            }
        });
        cases.put("DUR-004", () -> {
            try (Connection c = connect()) {
                PdbConnection p = pdb(c);
                p.kvSet(utf8("conf:dur4"), utf8("durable"));
                p.kvDel(utf8("conf:dur4n"));
                for (int i = 0; i < 3; i++) p.kvIncr(utf8("conf:dur4n"));
            }
            crashRestart();
            try (Connection c = connect(); Resp r = resp()) {
                eq("durable", str(pdb(c).kvGet(utf8("conf:dur4"))), "SDK GET after kill -9");
                eq("3", r.str("GET", "conf:dur4n"), "RESP GET of the counter after kill -9");
            }
        });
        cases.put("DUR-005", () -> {
            Connection a = connect();
            try {
                eq(1L, scalarLong(a, "SELECT 1"), "before the kill");
                Harness.control("kill9");
                long t0 = System.nanoTime();
                try {
                    scalarLong(a, "SELECT 1");
                    throw new Harness.Fail("statement on a connection to a killed server succeeded");
                } catch (SQLException e) {
                    String st = e.getSQLState();
                    check(st != null && st.startsWith("08"), "connection-loss SQLSTATE " + st + " is not class 08 (" + e.getMessage() + ")");
                    check(System.nanoTime() - t0 < 10_000_000_000L, "connection loss took > 10 s to surface");
                }
            } finally {
                try { a.close(); } catch (Exception ignored) { /* dead */ }
                Harness.control("restart");
            }
            try (Connection b = connect()) { eq(1L, scalarLong(b, "SELECT 1"), "new connection after restart"); }
        });
    }

    private static final java.util.concurrent.atomic.AtomicReference<Exception> cancelError = new java.util.concurrent.atomic.AtomicReference<>();

    static void crashRestart() throws Exception {
        Harness.control("kill9");
        Harness.control("restart");
    }

    static long sqlLong(Connection c, String sql, Object... args) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) ps.setObject(i + 1, args[i]);
            try (ResultSet rs = ps.executeQuery()) {
                check(rs.next(), "no row from " + sql);
                Object o = rs.getObject(1);
                if (o instanceof Number) return ((Number) o).longValue();
                if (o instanceof Boolean) return ((Boolean) o) ? 1 : 0;
                if (o instanceof String && "OK".equals(o)) return 1;
                return Long.parseLong(String.valueOf(o));
            }
        }
    }

    static long[] streamSum(Connection c, String sql, int sign) throws SQLException {
        long n = 0, sum = 0, disorder = 0, prev = 0;
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                long v = rs.getLong(1);
                if (n > 0 && sign * v != sign * prev + 1) disorder++;
                prev = v;
                n++;
                sum += v;
            }
        }
        return new long[] {n, sum, disorder};
    }

    static void verifyRow(ResultSet rs, int id, ConcurrentLinkedQueue<String> errors) throws SQLException {
        if (!rs.next()) { errors.add("no row for " + id); return; }
        long got = rs.getLong(1);
        String v = rs.getString(2);
        if (got != id || !("row-" + id).equals(v)) errors.add("asked " + id + ", got [" + got + ", " + v + "]");
        if (rs.next()) errors.add("extra row for " + id);
    }

    static byte[] utf8(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    static String str(byte[] b) { return b == null ? null : new String(b, StandardCharsets.UTF_8); }

    public static void main(String[] args) throws Exception {
        Class.forName("org.particledb.Driver");
        Map<String, Map<String, Object>> defs = Harness.loadCases(java.nio.file.Paths.get(Harness.env("PDB_CONF_CASES_DIR")));
        JdbcAdapter a = new JdbcAdapter(defs);
        System.exit(Harness.run(a, a.cases));
    }
}
