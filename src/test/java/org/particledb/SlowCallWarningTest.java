package org.particledb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.particledb.UnresponsiveServerTimeoutTest.Mode;
import org.particledb.UnresponsiveServerTimeoutTest.StoppedServer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * With {@code socketTimeout=0} (the default) a call against a server that
 * stopped answering waits without limit, so the driver ALERTS instead: once a
 * call has waited {@code slowCallWarnMillis} (default 60000; 0 disables) with
 * no response for its stream, one WARNING goes to the driver's logger
 * ({@code org.particledb.jdbc}) naming the connection id, stream id, request
 * kind and elapsed time -- never SQL text or parameter values -- and repeats
 * at most once a minute for that call. {@code getClientInfo("pdb.slowCallWarnings")}
 * counts them.
 */
class SlowCallWarningTest {

    private static final Logger LOG = Logger.getLogger("org.particledb.jdbc");
    private final List<LogRecord> records = Collections.synchronizedList(new ArrayList<>());
    private final Handler capture = new Handler() {
        @Override public void publish(LogRecord r) { records.add(r); }
        @Override public void flush() { }
        @Override public void close() { }
    };

    @BeforeEach
    void attach() {
        capture.setLevel(Level.ALL);
        LOG.addHandler(capture);
    }

    @AfterEach
    void detach() {
        LOG.removeHandler(capture);
    }

    private List<LogRecord> slowCallWarnings() {
        synchronized (records) {
            List<LogRecord> out = new ArrayList<>();
            for (LogRecord r : records) {
                if (r.getLevel() == Level.WARNING && String.valueOf(r.getMessage()).contains("slow call")) out.add(r);
            }
            return out;
        }
    }

    private static Connection connect(StoppedServer server, String extra) throws SQLException {
        return DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + server.port()
                + "/test?preferUnixSocket=false" + extra);
    }

    @Test
    void aCallWithNoResponseWarnsOnceNamingIdsNotSql() throws Exception {
        try (StoppedServer server = new StoppedServer(Mode.SILENT)) {
            Connection c = connect(server, "&slowCallWarnMillis=200&socketTimeout=1500");
            try (Statement st = c.createStatement()) {
                String connId = c.getClientInfo("pdb.connectionId");
                assertNotNull(connId, "no pdb.connectionId in getClientInfo");
                assertEquals("0", c.getClientInfo("pdb.slowCallWarnings"));
                assertTimeoutPreemptively(Duration.ofSeconds(20), () -> assertThrows(SQLException.class,
                        () -> st.executeUpdate("UPDATE secret_table SET secret_col = 'hunter2'")));
                List<LogRecord> w = slowCallWarnings();
                // 1.3 s past a 200 ms threshold: once, not once per threshold.
                assertEquals(1, w.size(), "slow-call warnings: " + messages(w));
                String m = w.get(0).getMessage();
                assertTrue(m.contains("connection " + connId), m);
                assertTrue(m.matches(".*stream \\d+.*"), m);
                assertTrue(m.contains("QUERY"), m);
                assertTrue(m.matches(".*waited \\d+ ms.*"), m);
                assertFalse(m.contains("secret"), "SQL text in the warning: " + m);
                assertFalse(m.contains("hunter2"), "a value in the warning: " + m);
                assertEquals("1", c.getClientInfo("pdb.slowCallWarnings"), "the counter survives the close");
            } finally {
                c.abort(Runnable::run);
            }
        }
    }

    @Test
    void aServerPreparedExecuteNamesItsKindAndNoParameters() throws Exception {
        try (StoppedServer server = new StoppedServer(Mode.PARTIAL_PAYLOAD)) {
            Connection c = connect(server, "&slowCallWarnMillis=200&socketTimeout=1200");
            try (PreparedStatement ps = c.prepareStatement("UPDATE secret_table SET x = ? WHERE id = ?")) {
                ps.setInt(1, 424242);
                ps.setInt(2, 1);
                assertTimeoutPreemptively(Duration.ofSeconds(20), () -> assertThrows(SQLException.class, ps::executeUpdate));
                List<LogRecord> w = slowCallWarnings();
                assertEquals(1, w.size(), messages(w));
                String m = w.get(0).getMessage();
                assertTrue(m.contains("EXECUTE"), m);
                assertFalse(m.contains("424242"), "a parameter in the warning: " + m);
                assertFalse(m.contains("secret"), "SQL text in the warning: " + m);
            } finally {
                c.abort(Runnable::run);
            }
        }
    }

    @Test
    void zeroDisablesAndTheDefaultIsAdvertised() throws Exception {
        DriverPropertyInfo[] info = new Driver().getPropertyInfo("jdbc:particledb://h/db", new java.util.Properties());
        DriverPropertyInfo p = Arrays.stream(info).filter(i -> "slowCallWarnMillis".equals(i.name)).findFirst()
                .orElseThrow(() -> new AssertionError("slowCallWarnMillis not in getPropertyInfo"));
        assertEquals("60000", p.value);
        try (StoppedServer server = new StoppedServer(Mode.SILENT)) {
            assertEquals("22023", assertThrows(SQLException.class,
                    () -> connect(server, "&slowCallWarnMillis=-1")).getSQLState());
            Connection c = connect(server, "&slowCallWarnMillis=0&socketTimeout=800");
            try (Statement st = c.createStatement()) {
                assertTimeoutPreemptively(Duration.ofSeconds(20), () -> assertThrows(SQLException.class,
                        () -> st.executeUpdate("UPDATE t SET x = 1")));
                assertEquals(0, slowCallWarnings().size(), messages(slowCallWarnings()));
                assertEquals("0", c.getClientInfo("pdb.slowCallWarnings"));
            } finally {
                c.abort(Runnable::run);
            }
        }
    }

    @Test
    void anAnsweredCallDoesNotWarn() throws Exception {
        try (var server = new AmbiguousFailureTest.FakeServer(AmbiguousFailureTest.Act.OK);
             Connection c = DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + server.port()
                     + "/test?preferUnixSocket=false&slowCallWarnMillis=50");
             Statement st = c.createStatement()) {
            for (int i = 0; i < 20; i++) assertEquals(1, st.executeUpdate("INSERT INTO t VALUES (1)"));
            assertEquals(0, slowCallWarnings().size(), messages(slowCallWarnings()));
            assertEquals("0", c.getClientInfo("pdb.slowCallWarnings"));
        }
    }

    private static String messages(List<LogRecord> w) {
        StringBuilder b = new StringBuilder();
        for (LogRecord r : w) b.append('[').append(r.getMessage()).append("] ");
        return b.toString();
    }
}
