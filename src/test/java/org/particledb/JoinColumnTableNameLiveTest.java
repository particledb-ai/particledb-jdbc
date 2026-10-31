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
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A reported join-metadata issue over the native wire: {@code SELECT *} over an OUTER
 * join whose sides share a column name. pgjdbc against PostgreSQL 17 answers,
 * for {@code SELECT * FROM t FULL OUTER JOIN u ON t.a = u.a} with
 * {@code t(a int, b text)} and {@code u(a int, c text)}:
 *
 * <pre>
 *   getColumnName  a  b  a  c
 *   getTableName   t  t  u  u        (and "" for a computed column)
 * </pre>
 *
 * Before the fix this driver answered {@code ""} for every getTableName, and
 * {@code SELECT *, 1 AS x} over the outer join came back with three columns
 * instead of five (the server's star passthrough lost the non-colliding ones).
 *
 * <p>Skipped via {@link Assumptions} when the {@code particledb} binary is absent.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JoinColumnTableNameLiveTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null, "particledb binary not available");
        try (Connection c = open(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE jt54 (a int, b text)");
            s.execute("CREATE TABLE ju54 (a int, c text)");
            s.execute("INSERT INTO jt54 VALUES (1, 'x')");
            s.execute("INSERT INTO ju54 VALUES (2, 'y')");
        }
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private Connection open() throws SQLException {
        return DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + server.port + "/test?preferUnixSocket=false");
    }

    private static List<String> describe(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        List<String> out = new ArrayList<>();
        for (int i = 1; i <= md.getColumnCount(); i++) out.add(md.getColumnName(i) + "@" + md.getTableName(i));
        return out;
    }

    private static int rows(ResultSet rs) throws SQLException {
        int n = 0;
        while (rs.next()) n++;
        return n;
    }

    @Test
    void outerJoinStarNamesEachColumnAndItsTable() throws SQLException {
        String[][] cases = {
            {"SELECT * FROM jt54 FULL OUTER JOIN ju54 ON jt54.a = ju54.a", "a@jt54,b@jt54,a@ju54,c@ju54", "2"},
            {"SELECT * FROM jt54 LEFT JOIN ju54 ON jt54.a = ju54.a", "a@jt54,b@jt54,a@ju54,c@ju54", "1"},
            {"SELECT *, 1 AS x FROM jt54 RIGHT JOIN ju54 ON jt54.a = ju54.a", "a@jt54,b@jt54,a@ju54,c@ju54,x@", "1"},
            {"SELECT ju54.a, jt54.a AS ta, 1 AS one FROM jt54 FULL JOIN ju54 ON jt54.a = ju54.a", "a@ju54,ta@jt54,one@", "2"},
        };
        try (Connection c = open(); Statement s = c.createStatement()) {
            for (String[] k : cases) {
                List<String> want = Arrays.asList(k[1].split(",", -1));
                try (ResultSet rs = s.executeQuery(k[0])) {
                    assertEquals(want, describe(rs), "statement: " + k[0]);
                    assertEquals(Integer.parseInt(k[2]), rows(rs), "rows: " + k[0]);
                }
                try (PreparedStatement ps = c.prepareStatement(k[0]); ResultSet rs = ps.executeQuery()) {
                    assertEquals(want, describe(rs), "prepared: " + k[0]);
                    assertEquals(Integer.parseInt(k[2]), rows(rs), "prepared rows: " + k[0]);
                }
            }
        }
    }

    /** A result with no connection to ask (a detached metadata) answers "" rather than failing. */
    @Test
    void detachedMetadataHasNoTableName() throws SQLException {
        java.util.Map<String, String> m = new java.util.HashMap<>();
        m.put("pdb:src_table_oid", "16384");
        PdbResultSet.SimpleMeta md = new PdbResultSet.SimpleMeta(
                Arrays.asList("a"), Arrays.asList("int64"), Arrays.asList(m));
        assertEquals("", md.getTableName(1));
    }
}
