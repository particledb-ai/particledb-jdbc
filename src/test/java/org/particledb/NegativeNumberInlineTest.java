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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A negative number bound into the INLINE (text) path used to be spelled bare,
 * so a preceding minus formed a comment: {@code 10-?} with -2.5 became
 * {@code 10--2.5} and answered 10 -- a silent wrong result in every released
 * driver. And {@code ?::text} with -5 became {@code -5::text} (minus applied to
 * TEXT). The inline renderer now parenthesises every negative number, the
 * spelling the server uses for typed EXECUTE parameters, so both paths answer
 * the same, and the right, value.
 *
 * <p>Unit part (no server): the rendered text. Live part: each shape, for
 * negative int / long / double / BigDecimal, answered by the text path
 * ({@code serverPrepare=false}) AND by EXECUTE, against the value computed in
 * Java. Plus string escaping: a backslash and a quote survive literally (the
 * server refuses {@code standard_conforming_strings=off}), and a placeholder
 * glued to {@code E} cannot open an escape string.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class NegativeNumberInlineTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    @Test
    void negativeNumbersRenderParenthesised() {
        assertEquals("SELECT 10-('-2.5'::float8)", PdbPreparedStatement.interpolate("SELECT 10-?", new Object[] {-2.5d}));
        assertEquals("SELECT (-5)::text", PdbPreparedStatement.interpolate("SELECT ?::text", new Object[] {-5}));
        assertEquals("SELECT -(-5)", PdbPreparedStatement.interpolate("SELECT -?", new Object[] {-5L}));
        assertEquals("SELECT c - -('-1.50'::numeric) FROM t",
                PdbPreparedStatement.interpolate("SELECT c - -? FROM t", new Object[] {new BigDecimal("-1.50")}));
        // `col--?` written in the TEMPLATE is a comment: no placeholder at all.
        assertEquals(0, PdbPreparedStatement.placeholderPositions("SELECT col--? FROM t").size());
        assertEquals("SELECT ('-0.0'::float8), ('-1.0E10'::float8), ('-1E+3'::numeric), 7, ('0.5'::float4)",
                PdbPreparedStatement.interpolate("SELECT ?, ?, ?, ?, ?",
                        new Object[] {-0.0d, -1e10, new BigDecimal("-1E+3"), (short) 7, 0.5f}));
        // A placeholder glued to an identifier character is separated from it.
        assertEquals("SELECT E 'a\\' x", PdbPreparedStatement.interpolate("SELECT E? x", new Object[] {"a\\"}));
    }

    private Connection open(boolean serverPrepare) throws SQLException {
        return DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + server.port
                + "/test?preferUnixSocket=false&serverPrepare=" + serverPrepare);
    }

    private static String one(Connection c, String sql, Object p) {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setObject(1, p);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), sql);
                return rs.getString(1);
            }
        } catch (SQLException e) {
            return "ERROR " + e.getSQLState() + " " + e.getMessage();
        }
    }

    @Test
    void everyShapeAnswersTheRightValueOnBothPaths() throws Exception {
        Assumptions.assumeTrue(server != null, "particledb binary not available");
        try (Connection text = open(false); Connection exec = open(true); Statement st = text.createStatement()) {
            st.executeUpdate("CREATE TABLE negc (id BIGINT PRIMARY KEY, col BIGINT)");
            st.executeUpdate("INSERT INTO negc VALUES (1, 100)");
            Object[] values = {-5, -7L, -2.5d, new BigDecimal("-1.50")};
            List<String> bad = new ArrayList<>();
            for (Object v : values) {
                BigDecimal bv = new BigDecimal(v.toString());
                // shape -> expected numeric value (null = compare text exactly)
                Object[][] shapes = {
                        {"SELECT 10-? AS v", BigDecimal.TEN.subtract(bv)},
                        {"SELECT -? AS v", bv.negate()},
                        {"SELECT col - -? AS v FROM negc WHERE id = 1", BigDecimal.valueOf(100).add(bv)},
                        {"SELECT ?::text AS v", null},
                };
                for (Object[] s : shapes) {
                    String sql = (String) s[0];
                    for (Connection c : new Connection[] {text, exec}) {
                        String got = one(c, sql, v);
                        String where = (c == text ? "inline" : "execute") + " " + sql + " [" + v + "]";
                        if (s[1] == null) {
                            if (got.startsWith("ERROR") || new BigDecimal(got).compareTo(bv) != 0) {
                                bad.add(where + " -> " + got);
                            }
                        } else if (got.startsWith("ERROR")
                                || new BigDecimal(got).compareTo((BigDecimal) s[1]) != 0) {
                            bad.add(where + " -> " + got + ", expected " + s[1]);
                        }
                    }
                }
            }
            assertTrue(bad.isEmpty(), String.join("\n", bad));
            // `col--?` in the template is a SQL comment: the `?` is no
            // placeholder, so binding a value is refused on both paths.
            for (Connection c : new Connection[] {text, exec}) {
                String sql = "SELECT col--?\n AS v FROM negc WHERE id = 1";
                assertTrue(one(c, sql, -5).startsWith("ERROR 07009"));
                try (PreparedStatement p = c.prepareStatement(sql); ResultSet result = p.executeQuery()) {
                    assertTrue(result.next());
                    assertEquals(100, result.getInt(1));
                }
            }
        }
    }

    /**
     * Strings: a backslash and a quote are stored literally on both paths
     * (standard-conforming strings, which the server will not let a session
     * turn off), and a value placed right after {@code E} cannot escape its
     * quotes.
     */
    @Test
    void stringEscapingIsStandardConforming() throws Exception {
        Assumptions.assumeTrue(server != null, "particledb binary not available");
        try (Connection text = open(false); Connection exec = open(true); Statement st = text.createStatement()) {
            assertThrows(SQLException.class, () -> st.execute("SET standard_conforming_strings = off"),
                    "the server must refuse to read backslashes as escapes");
            String[] values = {"a\\b", "it's", "\\'", "\\' OR 1=1 --", "tail\\", "''", "\\x41"};
            for (String v : values) {
                assertEquals(v, one(text, "SELECT ? AS v", v), "inline " + v);
                assertEquals(v, one(exec, "SELECT ? AS v", v), "execute " + v);
            }
            // `E?` would have been the escape string E'\' OR 1=1 --': now it
            // is the identifier E followed by a string, which fails to parse
            // instead of running anything.
            String got = one(text, "SELECT E? AS v", "\\' OR 1=1 --");
            assertTrue(got.startsWith("ERROR"), got);
        }
    }
}
