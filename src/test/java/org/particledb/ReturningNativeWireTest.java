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
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RETURNING over the NATIVE wire never answers a silent wrong value (the PG-wire half is
 * covered by the server's RETURNING wire tests).
 *
 * <p>An earlier pre-release server answered {@code UPDATE … RETURNING old.v, new.v} with the
 * POST-image under both names, and a RETURNING expression that raised with a
 * NULL while the statement "succeeded". PostgreSQL 17 refuses the qualifiers
 * ({@code 42P01}); PostgreSQL 18 gives them the pre-/post-image meaning, which
 * this server does not implement - so it must refuse, never answer. Each
 * refusal is checked from a SECOND connection to have written nothing, on the
 * statement and the prepared routes, in autocommit and in a transaction.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReturningNativeWireTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null,
                "particledb binary not available — skipping native-wire RETURNING test");
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    private Connection open() throws SQLException {
        return DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + server.port
                + "/test?preferUnixSocket=false");
    }

    private static String state(Connection c) throws SQLException {
        StringBuilder sb = new StringBuilder();
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT id, v FROM rn ORDER BY id")) {
            while (rs.next()) sb.append(rs.getInt(1)).append('|').append(rs.getInt(2)).append(' ');
        }
        return sb.toString().trim();
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    @Test
    void returningOldNewAndRaisingExpressionsAreRefusedAndWriteNothing() throws Exception {
        try (Connection c = open(); Connection other = open()) {
            exec(c, "CREATE TABLE rn (id INT PRIMARY KEY, v INT)");
            exec(c, "INSERT INTO rn VALUES (1, 10), (2, 20)");
            String before = state(other);
            String[] refused = {
                    "UPDATE rn SET v = 99 WHERE id = 1 RETURNING old.v AS old_v, new.v AS new_v",
                    "DELETE FROM rn WHERE id = 2 RETURNING new.v",
                    "INSERT INTO rn VALUES (3, 0) RETURNING old.v",
                    "UPDATE rn SET v = 0 WHERE id = 1 RETURNING 1 / v",
                    "INSERT INTO rn VALUES (4, 0) RETURNING 10 / v",
            };
            for (boolean inTxn : new boolean[] {false, true}) {
                c.setAutoCommit(!inTxn);
                for (String sql : refused) {
                    assertThrows(SQLException.class, () -> exec(c, sql),
                            (inTxn ? "[txn] " : "[autocommit] ") + sql);
                    if (inTxn) c.rollback();
                    try (PreparedStatement ps = c.prepareStatement(sql)) {
                        assertThrows(SQLException.class, ps::execute,
                                (inTxn ? "[txn, prepared] " : "[prepared] ") + sql);
                    }
                    if (inTxn) c.rollback();
                    assertEquals(before, state(other), "a refused statement wrote: " + sql);
                }
                c.setAutoCommit(true);
            }
            // Control: the target's own name resolves, to the written row.
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("UPDATE rn SET v = 11 WHERE id = 1 RETURNING rn.v, v * 2")) {
                assertTrue(rs.next());
                assertEquals(11, rs.getInt(1));
                assertEquals(22, rs.getInt(2));
            }
            assertEquals("1|11 2|20", state(other));
        }
    }
}
