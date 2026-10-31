package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression coverage for the 1.0.2 driver fixes:
 * <ul>
 *   <li><b>F2</b> - by-name getters ({@code getObject/getString(String)}) are
 *       case-insensitive per the JDBC contract, so a present value is never
 *       read as {@code null} due to identifier case.</li>
 *   <li><b>F3</b> - {@code Statement.execute()} exposes a ResultSet for any
 *       row-returning statement (WITH / VALUES / RETURNING), classifying by the
 *       real wire frame kind instead of a SELECT-prefix guess.</li>
 *   <li><b>F11</b> - {@code getObject(i, Class)} coerces Arrow's carrier type to
 *       the requested JDBC type instead of throwing ClassCastException.</li>
 * </ul>
 * Skipped via {@link Assumptions} when the {@code particledb} binary is absent.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DriverFixCoverageTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping driver-fix coverage test");
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private String url() {
        return "jdbc:particledb://127.0.0.1:" + server.port + "/test?preferUnixSocket=false";
    }

    /** F2: getObject/getString by column name must be case-insensitive. */
    @Test
    void byNameGettersAreCaseInsensitive() throws Exception {
        try (Connection c = DriverManager.getConnection(url());
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT 1 AS \"MixedCase\"")) {
            assertTrue(rs.next());
            String col = rs.getMetaData().getColumnName(1);
            assertNotNull(rs.getObject(col), "exact-name getObject must return the value");
            // JDBC column labels are case-insensitive: a case-variant lookup of
            // a present column must never read null (the F2 bug).
            String variant = col.equals(col.toLowerCase())
                    ? col.toUpperCase() : col.toLowerCase();
            assertNotNull(rs.getObject(variant),
                    "case-variant getObject must find the column");
            assertNotNull(rs.getString(variant),
                    "case-variant getString must find the column");
            assertEquals(1, rs.getInt(variant));
        }
    }

    /** F3: execute() must expose rows for WITH / VALUES / RETURNING. */
    @Test
    void executeReturnsResultSetForNonSelectRowSources() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE fix_exec (id INT PRIMARY KEY, v INT)");
            }
            try (Statement st = c.createStatement()) {
                assertTrue(st.execute("WITH t AS (SELECT 7 AS x) SELECT x FROM t"),
                        "execute() on WITH…SELECT must report a ResultSet");
                try (ResultSet rs = st.getResultSet()) {
                    assertNotNull(rs, "getResultSet() must not be null after a row-returning execute()");
                    assertTrue(rs.next());
                    assertEquals(7, rs.getInt(1));
                }
            }
            try (Statement st = c.createStatement()) {
                assertTrue(st.execute("VALUES (1),(2),(3)"),
                        "execute() on VALUES must report a ResultSet");
                int count = 0;
                try (ResultSet rs = st.getResultSet()) {
                    while (rs.next()) count++;
                }
                assertEquals(3, count);
            }
            try (Statement st = c.createStatement()) {
                assertTrue(st.execute("INSERT INTO fix_exec VALUES (10, 100) RETURNING id, v"),
                        "execute() on INSERT…RETURNING must report a ResultSet");
                try (ResultSet rs = st.getResultSet()) {
                    assertTrue(rs.next());
                    assertEquals(10, rs.getInt("id"));
                    assertEquals(100, rs.getInt("v"));
                }
            }
        }
    }

    /** F11: getObject(i, Class) must coerce, not ClassCastException. */
    @Test
    void getObjectWithClassCoerces() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE fix_obj (id INT PRIMARY KEY, big BIGINT)");
                st.executeUpdate("INSERT INTO fix_obj VALUES (1, 9000000000)");
            }
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT id, big FROM fix_obj")) {
                assertTrue(rs.next());
                assertEquals(Integer.valueOf(1), rs.getObject(1, Integer.class));
                assertEquals(Long.valueOf(9000000000L), rs.getObject(2, Long.class));
                assertEquals("9000000000", rs.getObject("big", String.class));
                assertEquals(Integer.valueOf(1), rs.getObject("id", Integer.class));
            }
        }
    }
}
