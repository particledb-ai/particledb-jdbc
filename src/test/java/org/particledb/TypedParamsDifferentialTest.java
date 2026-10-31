package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.particledb.wire.WireV2Codec;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.TimeZone;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Typed EXECUTE parameters against a live server: every Java value the driver
 * now sends typed (Double, Float, BigDecimal, Timestamp, Date, Time,
 * OffsetDateTime, byte[], UUID) must give EXACTLY what the same statement gives
 * with {@code serverPrepare=false}, where the driver inlines the value as SQL
 * text and sends QUERY -- the behaviour before typed parameters existed.
 *
 * <p>For every value: {@code SELECT ?} (value and column type),
 * {@code SELECT (?)::text}, and an INSERT into a column of the natural type
 * plus a TEXT column, read back. Then mixed parameter sets beside the original
 * kinds (negative integers, booleans, null, strings) in casts, operators and
 * predicates. Each comparison also asserts the handle path was REALLY taken
 * ({@link PdbConnection#serverPreparedExecutions}) -- otherwise both sides
 * would be the text path and the test would prove nothing.
 *
 * <p>The JVM zone is America/New_York so Timestamps on both sides of the 2024
 * DST changes exercise the zone-dependent {@code Timestamp.toString}.
 *
 * <p>Skipped via {@link Assumptions} when the {@code particledb} binary is absent.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TypedParamsDifferentialTest {

    private PdbTestServer server;
    private TimeZone savedZone;
    private Connection typed;
    private Connection inline;
    private int table;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null, "particledb binary not available -- skipping");
        savedZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
        typed = open(true);
        inline = open(false);
        assertTrue(typed.unwrap(PdbConnection.class).typedParamsEnabled(),
                "the server must echo CAP_EXT_TYPED_PARAMS");
    }

    @AfterAll
    void tearDown() throws Exception {
        if (savedZone != null) TimeZone.setDefault(savedZone);
        if (typed != null) typed.close();
        if (inline != null) inline.close();
        if (server != null) server.stop();
    }

    private Connection open(boolean serverPrepare) throws SQLException {
        return DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + server.port
                + "/test?preferUnixSocket=false&serverPrepare=" + serverPrepare);
    }

    private static String dump(Connection c, String sql, Object... params) {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) ps.setObject(i + 1, params[i]);
            if (!ps.execute()) return "UPDATE " + ps.getUpdateCount();
            try (ResultSet rs = ps.getResultSet()) {
                StringBuilder sb = new StringBuilder();
                ResultSetMetaData md = rs.getMetaData();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    sb.append(md.getColumnTypeName(i)).append('/').append(md.getColumnType(i)).append(' ');
                }
                while (rs.next()) {
                    sb.append("| ");
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        Object o = rs.getObject(i);
                        String text = o instanceof byte[] ? hex((byte[]) o) : rs.getString(i);
                        sb.append(text).append(':')
                                .append(o == null ? "null" : o.getClass().getSimpleName()).append(' ');
                    }
                }
                return sb.toString();
            }
        } catch (SQLException e) {
            return "ERROR " + e.getSQLState() + " " + e.getMessage();
        }
    }

    private static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder("0x");
        for (byte x : b) sb.append(String.format("%02x", x));
        return sb.toString();
    }

    private long handleExecs() throws SQLException {
        return typed.unwrap(PdbConnection.class).serverPreparedExecutions();
    }

    /** Run on both connections; record a divergence; assert the typed side took the handle path. */
    private void compare(List<String> bad, String label, String sql, Object... params) throws SQLException {
        long before = handleExecs();
        String t = dump(typed, sql, params);
        boolean viaHandle = handleExecs() == before + 1;
        String i = dump(inline, sql, params);
        // A refused statement does not count as a handle execution (the
        // counter records answered executions). The only refusal here is a
        // value past the server's 76-digit numeric (Double.MIN_VALUE /
        // MAX_VALUE and a 120-digit decimal, exact NUMERIC on the inline
        // side): both paths must refuse it with 22003, compared below.
        if (t.startsWith("ERROR")) {
            if (!t.startsWith("ERROR 22003 ")) bad.add("[" + label + "] refused other than 22003: " + t);
        } else if (!viaHandle) {
            bad.add("[" + label + "] NOT sent typed: " + sql + " " + Arrays.toString(params));
        }
        if (!t.equals(i)) bad.add("[" + label + "] " + sql + "\n    typed:  " + t + "\n    inline: " + i);
    }

    /**
     * {@code pg_typeof(?)} is not probed here: the driver's pg_catalog adapter
     * keeps any text naming a {@code pg_*} object on the text path (so both
     * sides would be inline). The column TYPE of {@code SELECT ?} carries the
     * same information, and {@code zz_wire_typed_params_differential.rs}
     * compares {@code pg_typeof} over the wire for every tag.
     */
    private void perValue(List<String> bad, String label, Object v, String column) throws SQLException {
        compare(bad, label, "SELECT ? AS v", v);
        compare(bad, label, "SELECT (?)::text AS t", v);
        compare(bad, label, "SELECT ? AS v, ? IS NULL AS n, (?)::text || '|' AS c", v, v, v);
        String tbl = "tpd" + (table++);
        try (Statement st = typed.createStatement()) {
            st.executeUpdate("CREATE TABLE " + tbl + " (id BIGINT PRIMARY KEY, c " + column + ", t TEXT)");
        }
        String ins = "INSERT INTO " + tbl + " (id, c, t) VALUES (?, ?, ?)";
        long before = handleExecs();
        String wt = dump(typed, ins, 1L, v, v);
        // A statement the server refuses does not count as a handle execution
        // (the counter records answered executions); the answers are still
        // compared below, so an identical refusal on both paths passes.
        if (wt.startsWith("ERROR")) {
            System.err.println("[TypedParamsDifferentialTest] " + label + ": INSERT refused: " + wt);
        } else if (handleExecs() != before + 1) {
            bad.add("[" + label + "] INSERT NOT sent typed: " + wt);
        }
        String wi = dump(inline, ins, 2L, v, v);
        if (!wt.equals(wi)) bad.add("[" + label + "] INSERT\n    typed:  " + wt + "\n    inline: " + wi);
        String rt = dump(inline, "SELECT c, t FROM " + tbl + " WHERE id = 1");
        String ri = dump(inline, "SELECT c, t FROM " + tbl + " WHERE id = 2");
        if (!rt.equals(ri)) bad.add("[" + label + "] read back\n    typed:  " + rt + "\n    inline: " + ri);
    }

    @Test
    void everyTypedJavaValueAnswersLikeTheInlinedStatement() throws Exception {
        List<String> bad = new ArrayList<>();
        Object[][] cases = {
                {"double 1.5", 1.5d, "double precision"},
                {"double 1e10", 1e10, "double precision"},
                {"double -0.0", -0.0d, "double precision"},
                {"double 2^63", 9.223372036854775807E18, "double precision"},
                {"double MIN_VALUE", Double.MIN_VALUE, "double precision"},
                {"double MAX_VALUE", Double.MAX_VALUE, "double precision"},
                {"double -1/3", -1.0 / 3.0, "double precision"},
                {"double NaN", Double.NaN, "double precision"},
                {"double +Inf", Double.POSITIVE_INFINITY, "double precision"},
                {"double -Inf", Double.NEGATIVE_INFINITY, "double precision"},
                {"float 1.1", 1.1f, "real"},
                {"float MAX", Float.MAX_VALUE, "real"},
                {"float NaN", Float.NaN, "real"},
                {"decimal 123.4500", new BigDecimal("123.4500"), "numeric"},
                {"decimal 0", BigDecimal.ZERO, "numeric"},
                {"decimal -0.000001", new BigDecimal("-0.000001"), "numeric"},
                {"decimal 1000 (scale 0)", new BigDecimal("1000"), "numeric"},
                {"decimal 76 digits", new BigDecimal("1234567890123456789012345678901234567890123456789012345678901234567890.123456"), "numeric"},
                {"decimal 120 digits", new BigDecimal("9".repeat(100) + "." + "1".repeat(20)), "numeric"},
                {"timestamp", java.sql.Timestamp.valueOf("2024-01-01 12:34:56.789"), "timestamp"},
                {"timestamp whole second", java.sql.Timestamp.valueOf("2024-01-01 12:34:56"), "timestamp"},
                {"timestamp micros", java.sql.Timestamp.valueOf("2024-07-04 00:00:00.123456"), "timestamp"},
                // Instants either side of the New York DST changes: toString is
                // the zone's wall clock, and so is the typed value.
                {"timestamp before spring DST", new java.sql.Timestamp(1_710_053_940_000L), "timestamp"},
                {"timestamp after spring DST", new java.sql.Timestamp(1_710_054_000_000L), "timestamp"},
                {"timestamp fall overlap", new java.sql.Timestamp(1_730_615_400_000L), "timestamp"},
                {"timestamp into timestamptz", java.sql.Timestamp.valueOf("2024-01-01 12:34:56.5"), "timestamptz"},
                {"timestamp 0001", java.sql.Timestamp.valueOf("0001-01-01 00:00:00"), "timestamp"},
                {"date", java.sql.Date.valueOf("2024-02-29"), "date"},
                {"date 1582 (Julian in java.sql.Date)", java.sql.Date.valueOf("1582-10-04"), "date"},
                {"date 0001", java.sql.Date.valueOf("0001-01-01"), "date"},
                {"time", java.sql.Time.valueOf("23:59:59"), "text"},
                {"time with millis (dropped by toString)", new java.sql.Time(45_296_789L), "text"},
                {"offset +02:00 DST edge", OffsetDateTime.of(2024, 3, 10, 2, 30, 0, 0, ZoneOffset.ofHours(2)), "timestamptz"},
                {"offset into timestamp col", OffsetDateTime.of(2024, 3, 10, 2, 30, 0, 0, ZoneOffset.ofHours(2)), "timestamp"},
                {"offset -05:00 millis", OffsetDateTime.of(2024, 11, 3, 1, 30, 0, 500_000_000, ZoneOffset.ofHours(-5)), "timestamptz"},
                {"offset Z micros", OffsetDateTime.of(2024, 1, 1, 0, 0, 0, 1_000, ZoneOffset.UTC), "timestamptz"},
                {"uuid nil", new UUID(0, 0), "uuid"},
                {"uuid", UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"), "uuid"},
                {"bytes empty", new byte[0], "bytea"},
                {"bytes", new byte[] {1, 2, (byte) 0xab, '\''}, "bytea"},
        };
        for (Object[] c : cases) perValue(bad, (String) c[0], c[1], (String) c[2]);
        assertTrue(bad.isEmpty(), bad.size() + " divergence(s):\n" + String.join("\n", bad));
    }

    /**
     * Random doubles and floats, each through {@code SELECT (?)::text, ?}:
     * the inline text is exact NUMERIC, so its text shows every
     * digit and the scale -- a single digit of disagreement between this
     * driver's canonical spelling and the server's renderer shows here. Values
     * this JDK prints non-canonically must NOT go typed.
     */
    @Test
    void randomFloatsSpellIdenticallyOnBothPaths() throws Exception {
        List<String> bad = new ArrayList<>();
        SplittableRandom r = new SplittableRandom(7);
        int typedCount = 0;
        for (int i = 0; i < 400; i++) {
            Object v;
            if (i % 4 == 0) v = Double.longBitsToDouble(r.nextLong());
            else if (i % 4 == 1) v = Math.round(r.nextDouble() * 1e9) / Math.pow(10, r.nextInt(12));
            else if (i % 4 == 2) v = Float.intBitsToFloat(r.nextInt());
            else v = (float) (r.nextDouble() * Math.pow(10, r.nextInt(-10, 10)));
            boolean bindable = WireV2Codec.bindable(new Object[] {v}, 1, true);
            long before = handleExecs();
            String t = dump(typed, "SELECT (?)::text AS a, ? AS b", v, v);
            String in = dump(inline, "SELECT (?)::text AS a, ? AS b", v, v);
            // A value past the server's 76-digit numeric is refused 22003 on
            // both paths (inline it is exact NUMERIC); a refusal is not an
            // answered handle execution, so it does not move the counter.
            boolean refused = t.startsWith("ERROR");
            if (refused) assertTrue(t.startsWith("ERROR 22003 "), "refusal must be 22003: " + v + " -> " + t);
            assertEquals(bindable && !refused ? before + 1 : before, handleExecs(), "typed iff bindable: " + v);
            if (bindable) typedCount++;
            if (!t.equals(in)) bad.add(v + "\n    typed:  " + t + "\n    inline: " + in);
        }
        assertTrue(typedCount > 300, "most random values go typed: " + typedCount);
        assertTrue(bad.isEmpty(), String.join("\n", bad));
    }

    /**
     * Mixed parameter sets: the new kinds beside negative integers, booleans,
     * null and strings, in casts, operators and predicates. Every statement must
     * answer identically -- including a negative number next to an operator or
     * a cast ({@code ?::text}, {@code 10-?}, {@code -?}): the inline renderer
     * parenthesises negatives exactly as the server's typed rendering does (it
     * used to write {@code 10--2.5}, a comment that answered 10).
     */
    @Test
    void mixedParameterSetsWithServerPrepareOnAndOff() throws Exception {
        try (Statement st = typed.createStatement()) {
            st.executeUpdate("CREATE TABLE mixd (id BIGINT PRIMARY KEY, ts TIMESTAMP, d DOUBLE PRECISION, "
                    + "n NUMERIC, name TEXT, ok BOOLEAN, b BYTEA, u UUID)");
        }
        java.sql.Timestamp ts = java.sql.Timestamp.valueOf("2024-03-10 03:15:00.25");
        UUID u = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        List<String> bad = new ArrayList<>();
        // The Conductor shape: a timestamp, a double and text in one INSERT.
        long before = handleExecs();
        assertEquals("UPDATE 1", dump(typed, "INSERT INTO mixd VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                1L, ts, 0.25d, new BigDecimal("-10.50"), "o'k", true, new byte[] {9}, u));
        assertEquals(before + 1, handleExecs(), "the Conductor-shaped INSERT goes typed");
        assertEquals("UPDATE 1", dump(inline, "INSERT INTO mixd VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                2L, ts, 0.25d, new BigDecimal("-10.50"), "o'k", true, new byte[] {9}, u));
        compare(bad, "readback", "SELECT ts, d, n, name, ok, b, u FROM mixd WHERE id = ?", 1L);
        assertEquals(dump(inline, "SELECT ts, d, n, name, ok, b, u FROM mixd WHERE id = 1"),
                dump(inline, "SELECT ts, d, n, name, ok, b, u FROM mixd WHERE id = 2"), "stored rows");
        Object[][] same = {
                {"SELECT id FROM mixd WHERE ts = ? AND d < ? AND n = ? AND ok = ? AND name <> ? ORDER BY id",
                        ts, 1.0d, new BigDecimal("-10.50"), true, "x"},
                {"SELECT ?::text, ? + 1, ? * 2, ? IS NULL, ? || 'z'", ts, 0.5d, new BigDecimal("0.7"), null, "y"},
                {"SELECT ts + interval '1 day', d / ? FROM mixd WHERE id = ?", 4.0d, 1L},
                {"SELECT count(*) FROM mixd WHERE ts BETWEEN ? AND ? AND u = ? AND b = ?",
                        java.sql.Timestamp.valueOf("2024-03-10 00:00:00"),
                        OffsetDateTime.of(2024, 3, 11, 0, 0, 0, 0, ZoneOffset.UTC), u, new byte[] {9}},
                {"SELECT ? = true, ? > 2, ?::date, ?::numeric(10,1)", true, 3.5d,
                        java.sql.Date.valueOf("2024-03-10"), new BigDecimal("1.25")},
                {"SELECT ? + ?, ? - ?", -5L, -1.5d, 2.5d, -3L},
                {"UPDATE mixd SET d = ?, ts = ? WHERE id = ? AND n = ?", -0.5d, ts, 1L, new BigDecimal("-10.50")},
        };
        for (Object[] row : same) {
            compare(bad, "mixed", (String) row[0], Arrays.copyOfRange(row, 1, row.length));
        }
        assertTrue(bad.isEmpty(), bad.size() + " divergence(s):\n" + String.join("\n", bad));

        Object[][] negatives = {
                {"SELECT ?::text AS v", -1.5d},
                {"SELECT ?::text AS v", new BigDecimal("-2.50")},
                {"SELECT 10-? AS v", -2.5d},
                {"SELECT -? AS v", new BigDecimal("-0.5")},
        };
        for (Object[] row : negatives) {
            compare(bad, "negative", (String) row[0], Arrays.copyOfRange(row, 1, row.length));
        }
        assertTrue(bad.isEmpty(), bad.size() + " divergence(s):\n" + String.join("\n", bad));
        assertTrue(dump(inline, "SELECT 10-? AS v", -2.5d).contains("12.5"), "10-(-2.5) is 12.5 inline too");
    }

    /** {@code serverPrepare=false} still never uses the handle path for any value. */
    @Test
    void serverPrepareOffNeverSendsTyped() throws Exception {
        long before = inline.unwrap(PdbConnection.class).serverPreparedExecutions();
        dump(inline, "SELECT ?, ?, ?", 1.5d, java.sql.Timestamp.valueOf("2024-01-01 00:00:00"), new byte[] {1});
        assertEquals(before, inline.unwrap(PdbConnection.class).serverPreparedExecutions());
    }
}
