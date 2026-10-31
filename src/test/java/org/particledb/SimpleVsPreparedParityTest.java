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
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Parity between a literal {@link Statement} and a bound
 * {@link PreparedStatement} of the same logical query. The driver does
 * client-side parameter substitution, so the two paths must yield identical
 * result sets - including a null parameter, a string containing a quote, and
 * a numeric.
 *
 * <p>Skipped via {@link Assumptions} when the {@code particledb} binary is
 * absent.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SimpleVsPreparedParityTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping simple/prepared parity test");
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private String url() {
        return "jdbc:particledb://127.0.0.1:" + server.port + "/test"
                + "?preferUnixSocket=false";
    }

    @Test
    void sameQuery_simpleAndPrepared_identicalResults() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE parity (id INT PRIMARY KEY, name TEXT)");
                st.executeUpdate("INSERT INTO parity VALUES (1, 'alice')");
                st.executeUpdate("INSERT INTO parity VALUES (2, 'O''Brien')");
                st.executeUpdate("INSERT INTO parity VALUES (3, 'bob')");
            }

            // ── Case 1: numeric parameter ────────────────────────────────
            List<List<String>> literalNum;
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT id, name FROM parity WHERE id = 3 ORDER BY id")) {
                literalNum = dump(rs);
            }
            List<List<String>> preparedNum;
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT id, name FROM parity WHERE id = ? ORDER BY id")) {
                ps.setInt(1, 3);
                try (ResultSet rs = ps.executeQuery()) {
                    preparedNum = dump(rs);
                }
            }
            assertEquals(1, literalNum.size(), "numeric-literal query should match one row");
            assertEquals(literalNum, preparedNum, "numeric param parity mismatch");

            // ── Case 2: string containing a single quote ─────────────────
            List<List<String>> literalStr;
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT id, name FROM parity WHERE name = 'O''Brien'")) {
                literalStr = dump(rs);
            }
            List<List<String>> preparedStr;
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT id, name FROM parity WHERE name = ?")) {
                ps.setString(1, "O'Brien");
                try (ResultSet rs = ps.executeQuery()) {
                    preparedStr = dump(rs);
                }
            }
            assertEquals(1, literalStr.size(), "quoted-string query should match one row");
            assertEquals(literalStr, preparedStr, "quoted-string param parity mismatch");

            // ── Case 3: null parameter ───────────────────────────────────
            List<List<String>> literalNull;
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT NULL AS x, 1 AS y")) {
                literalNull = dump(rs);
            }
            List<List<String>> preparedNull;
            try (PreparedStatement ps = c.prepareStatement("SELECT ? AS x, 1 AS y")) {
                ps.setNull(1, Types.INTEGER);
                try (ResultSet rs = ps.executeQuery()) {
                    preparedNull = dump(rs);
                }
            }
            assertEquals(1, literalNull.size());
            assertEquals(literalNull, preparedNull, "null param parity mismatch");
        }
    }

    /** Materialize a result set as rows-of-string-cells, null-safe. */
    private static List<List<String>> dump(ResultSet rs) throws Exception {
        int cols = rs.getMetaData().getColumnCount();
        List<List<String>> out = new ArrayList<>();
        while (rs.next()) {
            List<String> row = new ArrayList<>();
            for (int i = 1; i <= cols; i++) {
                Object o = rs.getObject(i);
                row.add(o == null ? "<null>" : o.toString());
            }
            out.add(row);
        }
        return out;
    }
}
