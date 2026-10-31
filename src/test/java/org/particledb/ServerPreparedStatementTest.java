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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link PdbPreparedStatement} executing by server-side handle (native wire
 * {@code PREPARE} / {@code EXECUTE}, {@code CAP_PREPARED}).
 *
 * <p>Pinned: the handle path is really taken ({@link
 * PdbConnection#serverPreparedExecutions}) and answers exactly what the text
 * path answers (same connection with {@code serverPrepare=false}); a schema
 * change between prepare and execute answers the NEW schema; values the wire
 * cannot carry typed (a double) keep the text path; and many threads racing
 * the FIRST execution of one statement on a shared connection all succeed
 * (an EXECUTE must not overtake its PREPARE - the server runs streams
 * concurrently).
 *
 * <p>Skipped via {@link Assumptions} when the {@code particledb} binary is absent.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ServerPreparedStatementTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping server-prepared test");
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private Connection open(boolean serverPrepare) throws SQLException {
        return DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + server.port
                + "/test?preferUnixSocket=false&serverPrepare=" + serverPrepare);
    }

    private static long handleExecs(Connection c) throws SQLException {
        return c.unwrap(PdbConnection.class).serverPreparedExecutions();
    }

    private static List<List<String>> dump(ResultSet rs) throws SQLException {
        List<List<String>> out = new ArrayList<>();
        ResultSetMetaData md = rs.getMetaData();
        List<String> header = new ArrayList<>();
        for (int i = 1; i <= md.getColumnCount(); i++) {
            header.add(md.getColumnLabel(i) + ":" + md.getColumnType(i));
        }
        out.add(header);
        while (rs.next()) {
            List<String> row = new ArrayList<>();
            for (int i = 1; i <= md.getColumnCount(); i++) row.add(rs.getString(i));
            out.add(row);
        }
        return out;
    }

    private static List<List<String>> query(Connection c, String sql, Object... params)
            throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) ps.setObject(i + 1, params[i]);
            try (ResultSet rs = ps.executeQuery()) {
                return dump(rs);
            }
        }
    }

    @Test
    void handlePathIsTakenAndAnswersLikeTheTextPath() throws Exception {
        try (Connection h = open(true); Connection t = open(false)) {
            assertTrue(h.unwrap(PdbConnection.class).serverPrepareEnabled(),
                    "the server must negotiate CAP_PREPARED");
            assertFalse(t.unwrap(PdbConnection.class).serverPrepareEnabled());
            try (Statement st = h.createStatement()) {
                st.executeUpdate("CREATE TABLE sp1 (id BIGINT PRIMARY KEY, name TEXT, n INT, flag BOOLEAN)");
                st.executeUpdate("INSERT INTO sp1 VALUES (1, 'plain', 10, true), "
                        + "(2, 'it''s \\ $1 -- x', -3, false), (3, NULL, NULL, NULL)");
            }
            Object[][] cases = {
                    {"SELECT id, name, n, flag FROM sp1 WHERE id = ?", new Object[] {1L}},
                    {"SELECT id, name, n, flag FROM sp1 WHERE id = ?", new Object[] {2}},
                    {"SELECT id, name, n, flag FROM sp1 WHERE id = ?", new Object[] {999L}},
                    {"SELECT id FROM sp1 WHERE name = ?", new Object[] {"it's \\ $1 -- x"}},
                    {"SELECT id FROM sp1 WHERE n = ?", new Object[] {-3}},
                    {"SELECT id FROM sp1 WHERE flag = ? ORDER BY id", new Object[] {Boolean.TRUE}},
                    {"SELECT id FROM sp1 WHERE name IS NOT DISTINCT FROM ? ORDER BY id", new Object[] {null}},
                    {"SELECT id, name FROM sp1 WHERE id > ? ORDER BY id LIMIT ?", new Object[] {0L, 2}},
            };
            long before = handleExecs(h);
            for (Object[] c : cases) {
                String sql = (String) c[0];
                Object[] p = (Object[]) c[1];
                assertEquals(query(t, sql, p), query(h, sql, p), "handle vs text: " + sql);
            }
            assertEquals(before + cases.length, handleExecs(h),
                    "every case must have run by server-side handle");
            assertEquals(0, handleExecs(t), "serverPrepare=false must never use the handle path");

            // Writes by handle, read back by text.
            try (PreparedStatement ps = h.prepareStatement("INSERT INTO sp1 (id, name, n) VALUES (?, ?, ?)")) {
                ps.setLong(1, 10);
                ps.setString(2, "O'Brien \\n");
                ps.setInt(3, Integer.MIN_VALUE);
                assertEquals(1, ps.executeUpdate());
            }
            List<List<String>> back = query(t, "SELECT name, n FROM sp1 WHERE id = 10");
            assertEquals(List.of("O'Brien \\n", String.valueOf(Integer.MIN_VALUE)), back.get(1));
        }
    }

    @Test
    void aValueWithoutATypedEncodingKeepsTheTextPath() throws Exception {
        try (Connection h = open(true)) {
            try (Statement st = h.createStatement()) {
                st.executeUpdate("CREATE TABLE sp2 (id BIGINT PRIMARY KEY, x DOUBLE PRECISION)");
                st.executeUpdate("INSERT INTO sp2 VALUES (1, 1.5)");
            }
            // A java.time.LocalDateTime has no typed encoding: text path.
            long before = handleExecs(h);
            List<List<String>> r = query(h, "SELECT id FROM sp2 WHERE x = ? AND ? IS NOT NULL",
                    1.5d, java.time.LocalDateTime.of(2024, 1, 1, 0, 0));
            assertEquals(2, r.size(), "one row expected: " + r);
            assertEquals(before, handleExecs(h), "a LocalDateTime must not be sent by handle");
            // A double now travels typed (CAP_EXT_TYPED_PARAMS), and answers alike.
            r = query(h, "SELECT id FROM sp2 WHERE x = ?", 1.5d);
            assertEquals(2, r.size(), "one row expected: " + r);
            assertEquals(before + 1, handleExecs(h), "a double is sent by handle when the server types it");
        }
    }

    @Test
    void aSchemaChangeBetweenPrepareAndExecuteAnswersTheNewSchema() throws Exception {
        try (Connection h = open(true); Statement st = h.createStatement()) {
            st.executeUpdate("CREATE TABLE sp3 (id BIGINT PRIMARY KEY, v BIGINT)");
            st.executeUpdate("INSERT INTO sp3 VALUES (1, 10)");
            try (PreparedStatement star = h.prepareStatement("SELECT * FROM sp3 WHERE id = ?");
                 PreparedStatement v = h.prepareStatement("SELECT id, v FROM sp3 WHERE id = ?")) {
                star.setLong(1, 1);
                v.setLong(1, 1);
                try (ResultSet rs = star.executeQuery()) {
                    assertEquals(2, rs.getMetaData().getColumnCount());
                }
                try (ResultSet rs = v.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(10L, rs.getLong(2));
                }
                st.executeUpdate("ALTER TABLE sp3 ADD COLUMN w TEXT");
                st.executeUpdate("UPDATE sp3 SET w = 'new' WHERE id = 1");
                try (ResultSet rs = star.executeQuery()) {
                    assertEquals(3, rs.getMetaData().getColumnCount(), "SELECT * after ADD COLUMN");
                    assertTrue(rs.next());
                    assertEquals("new", rs.getString(3));
                }
                st.executeUpdate("DROP TABLE sp3");
                st.executeUpdate("CREATE TABLE sp3 (id BIGINT PRIMARY KEY, v TEXT)");
                st.executeUpdate("INSERT INTO sp3 VALUES (1, 'text now')");
                try (ResultSet rs = v.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("text now", rs.getString(2), "re-created relation, new type");
                }
                st.executeUpdate("ALTER TABLE sp3 DROP COLUMN v");
                assertThrows(SQLException.class, v::executeQuery,
                        "a dropped column must be an error, never a stale value");
            }
        }
    }

    @Test
    void manyThreadsRacingTheFirstExecutionOnOneConnectionAllSucceed() throws Exception {
        try (Connection h = open(true)) {
            try (Statement st = h.createStatement()) {
                st.executeUpdate("CREATE TABLE sp4 (id BIGINT PRIMARY KEY, v BIGINT)");
                st.executeUpdate("INSERT INTO sp4 VALUES (1, 100), (2, 200)");
            }
            ExecutorService pool = Executors.newFixedThreadPool(32);
            try {
                for (int round = 0; round < 5; round++) {
                    // A fresh text each round, so every round races a new PREPARE.
                    String sql = "SELECT v FROM sp4 WHERE id = ? AND " + round + " = " + round;
                    List<Future<Long>> fs = new ArrayList<>();
                    for (int i = 0; i < 64; i++) {
                        final long id = 1 + (i % 2);
                        fs.add(pool.submit(() -> {
                            try (PreparedStatement ps = h.prepareStatement(sql)) {
                                ps.setLong(1, id);
                                try (ResultSet rs = ps.executeQuery()) {
                                    assertTrue(rs.next());
                                    return rs.getLong(1);
                                }
                            }
                        }));
                    }
                    for (int i = 0; i < fs.size(); i++) {
                        assertEquals(100L * (1 + (i % 2)), fs.get(i).get());
                    }
                }
            } finally {
                pool.shutdownNow();
            }
        }
    }
}
