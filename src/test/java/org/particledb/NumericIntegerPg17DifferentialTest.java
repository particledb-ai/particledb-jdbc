package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * getLong / getInt / getShort / getByte on numeric, float and bigint values,
 * differential against pgjdbc on PostgreSQL 17: values at and past
 * Long.MAX_VALUE / Long.MIN_VALUE, the int, short and byte bounds, and
 * fractions (pgjdbc truncates toward zero), as literals and from
 * {@code numeric}, {@code numeric(30,2)}, {@code double precision} and
 * {@code real} table columns, including a {@code numeric(30,2)} batch with a
 * NaN in it (sent as text) and one without (sent as Decimal). An error
 * counts as its SQLSTATE and message. Each statement runs as a Statement and
 * as a PreparedStatement.
 *
 * <p>The oracle runs with {@code prepareThreshold=0}: pgjdbc then reads
 * text-format results, its behaviour for every Statement and for a
 * PreparedStatement's first executions. (After the threshold it switches to
 * binary results, where its messages print the value with digit grouping
 * and report an int overflow past the long range as "type long".)
 *
 * <p>The oracle is {@code PG_ORACLE_URL}, checked to be PostgreSQL 17;
 * skipped when it or the {@code particledb} binary is absent.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class NumericIntegerPg17DifferentialTest {

    private static final String TABLE = "numeric_integer_diff";

    private static final String[] VALUES = {
        "9223372036854775807", "9223372036854775808", "-9223372036854775808", "-9223372036854775809",
        "2147483647", "2147483648", "-2147483648", "-2147483649",
        "32767", "32768", "-32768", "-32769", "127", "128", "-128", "-129",
        "1.7", "-1.7", "1.5", "-1.5", "2.5", "0.5", "-0.5", "0.9999",
        "2147483647.9", "2147483648.1", "-2147483648.9", "9223372036854775807.5", "1e19", "33554448",
    };

    private String pgUrl;
    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        Class.forName("org.postgresql.Driver");
        pgUrl = System.getProperty("PG_ORACLE_URL", System.getenv("PG_ORACLE_URL"));
        Assumptions.assumeTrue(pgUrl != null && !pgUrl.isEmpty(), "PG_ORACLE_URL not set");
        String banner = null;
        try (Connection pg = pg(); Statement st = pg.createStatement();
             ResultSet rs = st.executeQuery("SELECT version()")) {
            if (rs.next()) banner = rs.getString(1);
        } catch (SQLException e) {
            banner = null;
        }
        Assumptions.assumeTrue(banner != null && banner.startsWith("PostgreSQL 17")
                && !banner.contains("ParticleDB"), "not a PostgreSQL 17 oracle: " + banner);
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null, "particledb binary not available");
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private Connection pg() throws SQLException {
        Properties p = new Properties();
        p.setProperty("prepareThreshold", "0");
        return DriverManager.getConnection(pgUrl, p);
    }

    private Connection nat() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:particledb://127.0.0.1:" + server.port + "/test?preferUnixSocket=false");
    }

    @FunctionalInterface
    private interface Getter { Object get() throws SQLException; }

    private static String read(Getter g) {
        try {
            return String.valueOf(g.get());
        } catch (SQLException e) {
            return "ERR " + e.getSQLState() + " " + e.getMessage();
        }
    }

    private static List<String> queries() {
        List<String> q = new ArrayList<>();
        for (String type : new String[]{"numeric", "float8", "float4", "int8"}) {
            for (String v : VALUES) {
                // bigint refuses a fractional or out-of-range literal.
                if (type.equals("int8") && (v.contains(".") || v.contains("e")
                        || v.equals("9223372036854775808") || v.equals("-9223372036854775809"))) continue;
                q.add("SELECT CAST('" + v + "' AS " + type + ") AS v");
            }
        }
        q.add("SELECT n, d, r FROM " + TABLE + " ORDER BY id");
        q.add("SELECT n2 FROM " + TABLE + " ORDER BY id");                 // batch with a NaN
        q.add("SELECT n2 FROM " + TABLE + " WHERE id > 0 ORDER BY id");    // Decimal batch
        q.add("SELECT n2 + 0 AS e FROM " + TABLE + " WHERE id > 0 ORDER BY id");
        q.add("SELECT sum(n) AS s FROM " + TABLE + " WHERE id > 0");
        return q;
    }

    private static void fixture(Connection c) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + TABLE);
            st.execute("CREATE TABLE " + TABLE
                    + " (id int PRIMARY KEY, n numeric, n2 numeric(30,2), d double precision, r real)");
            st.execute("INSERT INTO " + TABLE + " VALUES (0, 1, 'NaN', 1, 1)");
            int id = 1;
            for (String v : new String[]{"9223372036854775807", "9223372036854775808", "-9223372036854775808",
                    "-9223372036854775809", "2147483647.99", "2147483648.10", "-2147483649", "32768", "-129",
                    "1.70", "-1.70", "0.50", "18446744073709551616"}) {
                st.execute("INSERT INTO " + TABLE + " VALUES (" + id++ + ", " + v + ", " + v + ", " + v + ", " + v + ")");
            }
        }
    }

    private static void dump(List<String> out, String label, ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int row = 0;
        while (rs.next()) {
            row++;
            for (int c = 1; c <= md.getColumnCount(); c++) {
                final int col = c;
                String key = label + " | row " + row + " | " + md.getColumnLabel(c) + " | ";
                out.add(key + "getLong = " + read(() -> rs.getLong(col)));
                out.add(key + "getInt = " + read(() -> rs.getInt(col)));
                out.add(key + "getShort = " + read(() -> rs.getShort(col)));
                out.add(key + "getByte = " + read(() -> rs.getByte(col)));
            }
        }
    }

    private static List<String> matrix(Connection c) throws SQLException {
        List<String> out = new ArrayList<>();
        for (String sql : queries()) {
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                dump(out, "S " + sql, rs);
            }
            try (PreparedStatement ps = c.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
                dump(out, "P " + sql, rs);
            }
        }
        return out;
    }

    @Test
    void integerGettersMatchPgjdbcOnPostgres17() throws Exception {
        List<String> want;
        List<String> got;
        try (Connection pg = pg()) {
            fixture(pg);
            want = matrix(pg);
        }
        try (Connection n = nat()) {
            fixture(n);
            got = matrix(n);
        }
        assertTrue(want.size() > 1000, "matrix too small to mean anything: " + want.size());
        assertTrue(want.contains("S SELECT CAST('1.7' AS numeric) AS v | row 1 | v | getInt = 1"));
        assertTrue(want.contains("S SELECT CAST('9223372036854775808' AS numeric) AS v | row 1 | v | getLong = "
                + "ERR 22003 Bad value for type long : 9223372036854775808"));
        StringBuilder diff = new StringBuilder();
        for (int i = 0; i < Math.max(want.size(), got.size()); i++) {
            String w = i < want.size() ? want.get(i) : "<missing>";
            String g = i < got.size() ? got.get(i) : "<missing>";
            if (!w.equals(g)) diff.append("\n  pg17:   ").append(w).append("\n  native: ").append(g);
        }
        assertEquals("", diff.toString(), "native driver differs from pgjdbc on PostgreSQL 17");
    }

    /**
     * {@link PgNumeric#pgFloatText}, which the integer getters convert from,
     * against PostgreSQL 17's own text for 4,000 seeded random {@code real}
     * and {@code double precision} values (random bit patterns, and whole and
     * three-decimal numbers).
     */
    @Test
    void floatTextMatchesPostgres17() throws Exception {
        java.util.Random r = new java.util.Random(20261026L);
        StringBuilder diff = new StringBuilder();
        int checked = 0;
        try (Connection pg = pg(); Statement st = pg.createStatement()) {
            for (int batch = 0; batch < 8; batch++) {
                boolean single = batch % 2 == 0;
                List<Number> vals = new ArrayList<>();
                StringBuilder q = new StringBuilder("SELECT v FROM (VALUES ");
                for (int i = 0; i < 500; i++) {
                    double d;
                    if (i % 3 == 0) {
                        d = Math.rint(r.nextGaussian() * Math.pow(10, r.nextInt(single ? 9 : 18))) / (i % 2 == 0 ? 1 : 1000);
                    } else if (single) {
                        float f;
                        do { f = Float.intBitsToFloat(r.nextInt()); } while (!Float.isFinite(f));
                        d = f;
                    } else {
                        do { d = Double.longBitsToDouble(r.nextLong()); } while (!Double.isFinite(d));
                    }
                    Number v = single ? (Number) (float) d : (Number) d;
                    double exact = v.doubleValue();
                    vals.add(v);
                    String lit = exact == 0 && 1 / exact < 0 ? "-0" : new java.math.BigDecimal(exact).toString();
                    q.append(i == 0 ? "" : ",").append("(").append(i).append(", CAST('").append(lit)
                            .append("' AS ").append(single ? "float4" : "float8").append("))");
                }
                q.append(") t(i, v) ORDER BY i");
                try (ResultSet rs = st.executeQuery(q.toString())) {
                    for (int i = 0; rs.next(); i++) {
                        checked++;
                        String want = rs.getString(1);
                        String got = PgNumeric.pgFloatText(vals.get(i));
                        if (!want.equals(got)) diff.append("\n  ").append(vals.get(i)).append(": pg17 ").append(want).append(", driver ").append(got);
                    }
                }
            }
        }
        assertEquals(4000, checked);
        assertEquals("", diff.toString(), "float text differs from PostgreSQL 17");
    }
}
