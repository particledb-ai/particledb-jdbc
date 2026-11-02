package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * NaN, Infinity and -Infinity, differential against PostgreSQL 17 through
 * pgjdbc: the same statements over pgjdbc to PostgreSQL 17 and over this
 * driver to the server must read identically through getObject (class and
 * value), getDouble, getFloat, getBigDecimal, getString, getLong, getInt,
 * getShort and getBoolean, an error counting as its SQLSTATE and message.
 * Covered: {@code real}, {@code double precision}, {@code numeric} and
 * {@code numeric(10,2)} table columns, float literals and expressions, and
 * numeric expressions and aggregates over a table column, each through a
 * Statement and a PreparedStatement.
 *
 * <p>The oracle is {@code PG_ORACLE_URL} (system property or env); its
 * identity is checked ({@code version()} names PostgreSQL 17, not ParticleDB).
 * Skipped when it or the {@code particledb} binary is absent.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class NumericSpecialPg17DifferentialTest {

    private String pgUrl;
    private PdbTestServer server;

    private static final String TABLE = "numeric_special_diff";

    private static final String[] QUERIES = {
        "SELECT r, d, n FROM " + TABLE + " ORDER BY id",
        "SELECT n2 FROM " + TABLE + " WHERE id IN (1, 4) ORDER BY id",
        "SELECT 'NaN'::float4 AS v", "SELECT 'Infinity'::float4 AS v", "SELECT '-Infinity'::float4 AS v",
        "SELECT 'NaN'::float8 AS v", "SELECT 'Infinity'::float8 AS v", "SELECT '-Infinity'::float8 AS v",
        "SELECT 'Infinity'::float8 * 2 AS v", "SELECT -('Infinity'::float8) AS v", "SELECT 'NaN'::float8 + 1 AS v",
        "SELECT n + 1 AS v FROM " + TABLE + " ORDER BY id",
        "SELECT -n AS v FROM " + TABLE + " ORDER BY id",
        "SELECT max(n) AS v FROM " + TABLE,
        "SELECT max(d) AS v FROM " + TABLE,
        "SELECT n::float8 AS v FROM " + TABLE + " ORDER BY id",
        "SELECT 'NaN'::text AS v",
        // Numeric specials typed only by a literal or a cast of one. Through
        // rc.16 the server sent these as untyped text over the native wire;
        // rc.17 stamps them numeric, so they are held to the oracle as well.
        "SELECT 'NaN'::numeric AS v", "SELECT 'Infinity'::numeric AS v", "SELECT '-Infinity'::numeric AS v",
        "SELECT 'NaN'::numeric(10,2) AS v", "SELECT 'Infinity'::numeric + 1 AS v",
        "SELECT CAST('NaN'::float8 AS numeric) AS v",
    };

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        Class.forName("org.postgresql.Driver");
        pgUrl = System.getProperty("PG_ORACLE_URL", System.getenv("PG_ORACLE_URL"));
        Assumptions.assumeTrue(pgUrl != null && !pgUrl.isEmpty(), "PG_ORACLE_URL not set");
        String banner = null;
        try (Connection pg = DriverManager.getConnection(pgUrl);
             Statement st = pg.createStatement();
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
        return DriverManager.getConnection(pgUrl);
    }

    private Connection nat() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:particledb://127.0.0.1:" + server.port + "/test?preferUnixSocket=false");
    }

    @FunctionalInterface
    private interface Getter { Object get() throws SQLException; }

    private static String read(Getter g) {
        try {
            Object o = g.get();
            return o == null ? "null" : o.getClass().getName() + ":" + o;
        } catch (SQLException e) {
            return "ERR " + e.getSQLState() + " " + e.getMessage();
        }
    }

    private static void fixture(Connection c) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + TABLE);
            st.execute("CREATE TABLE " + TABLE
                    + " (id int PRIMARY KEY, r real, d double precision, n numeric, n2 numeric(10,2))");
            st.execute("INSERT INTO " + TABLE + " VALUES"
                    + " (1, 'NaN', 'NaN', 'NaN', 'NaN'),"
                    + " (2, 'Infinity', 'Infinity', 'Infinity', NULL),"
                    + " (3, '-Infinity', '-Infinity', '-Infinity', NULL),"
                    + " (4, 1.5, 1.5, 1.5, 1.5)");
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
                out.add(key + "getObject = " + read(() -> rs.getObject(col)));
                out.add(key + "getDouble = " + read(() -> rs.getDouble(col)));
                out.add(key + "getFloat = " + read(() -> rs.getFloat(col)));
                out.add(key + "getBigDecimal = " + read(() -> rs.getBigDecimal(col)));
                if ("numeric".equals(md.getColumnTypeName(c))) {
                    // pgjdbc supports this conversion from numeric only.
                    out.add(key + "getObject(BigDecimal) = " + read(() -> rs.getObject(col, BigDecimal.class)));
                }
                out.add(key + "getString = " + read(() -> rs.getString(col)));
                out.add(key + "getLong = " + read(() -> isSpecial(rs, col) ? rs.getLong(col) : "skipped"));
                out.add(key + "getInt = " + read(() -> isSpecial(rs, col) ? rs.getInt(col) : "skipped"));
                out.add(key + "getShort = " + read(() -> isSpecial(rs, col) ? rs.getShort(col) : "skipped"));
                out.add(key + "getBoolean = " + read(() -> isSpecial(rs, col) ? rs.getBoolean(col) : "skipped"));
            }
        }
    }

    /**
     * The integer and boolean getters are compared on numeric specials only:
     * on an ordinary 1.5, and on text that does not parse, the two drivers
     * already differ (pgjdbc refuses getBoolean on 1.5, and its message for an
     * unparseable text differs), which is outside this test.
     */
    private static boolean isSpecial(ResultSet rs, int col) throws SQLException {
        if ("text".equals(rs.getMetaData().getColumnTypeName(col))) return false;
        String s = rs.getString(col);
        return "NaN".equals(s) || "Infinity".equals(s) || "-Infinity".equals(s);
    }

    private static List<String> matrix(Connection c, String[] queries) throws SQLException {
        List<String> out = new ArrayList<>();
        for (String sql : queries) {
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                dump(out, "S " + sql, rs);
            }
            try (PreparedStatement ps = c.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
                dump(out, "P " + sql, rs);
            }
        }
        return out;
    }

    private static void assertSameLines(List<String> want, List<String> got) {
        StringBuilder diff = new StringBuilder();
        for (int i = 0; i < Math.max(want.size(), got.size()); i++) {
            String w = i < want.size() ? want.get(i) : "<missing>";
            String g = i < got.size() ? got.get(i) : "<missing>";
            if (!w.equals(g)) diff.append("\n  pg17:   ").append(w).append("\n  native: ").append(g);
        }
        assertEquals("", diff.toString(), "native driver differs from pgjdbc on PostgreSQL 17");
    }

    @Test
    void specialValuesReadAsPgjdbcOnPostgres17() throws Exception {
        List<String> want;
        List<String> got;
        try (Connection pg = pg()) {
            fixture(pg);
            want = matrix(pg, QUERIES);
        }
        try (Connection n = nat()) {
            fixture(n);
            got = matrix(n, QUERIES);
        }
        assertTrue(want.size() > 500, "matrix too small to mean anything: " + want.size());
        // The oracle is what the expectations in NumericSpecialValuesTest were
        // read from; spot-check it so a changed pgjdbc is noticed here.
        assertTrue(want.contains("S SELECT 'NaN'::float8 AS v | row 1 | v | getObject = java.lang.Double:NaN"));
        assertTrue(want.contains("S SELECT r, d, n FROM " + TABLE
                + " ORDER BY id | row 1 | n | getBigDecimal = ERR 22003 Bad value for type BigDecimal : NaN"));
        assertSameLines(want, got);
    }
}
