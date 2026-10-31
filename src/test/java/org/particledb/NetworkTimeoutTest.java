package org.particledb;

import org.junit.jupiter.api.Test;
import org.particledb.ds.PdbDataSource;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class NetworkTimeoutTest {
    private static Connection connect(AmbiguousFailureTest.FakeServer server) throws SQLException {
        return DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + server.port()
                + "/test?preferUnixSocket=false");
    }

    @Test
    void networkTimeoutClosesTheConnectionWithoutRetrying() throws Exception {
        try (var server = new AmbiguousFailureTest.FakeServer(AmbiguousFailureTest.Act.LATE);
             Connection c = connect(server); Statement s = c.createStatement()) {
            c.setNetworkTimeout(Runnable::run, 150);
            s.setQueryTimeout(0);
            long start = System.nanoTime();
            SQLException failure = assertThrows(SQLNonTransientConnectionException.class,
                    () -> s.executeUpdate("INSERT INTO t VALUES (1)"));
            assertEquals("08006", failure.getSQLState());
            assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 2000);
            assertTrue(c.isClosed());
            assertEquals(1, server.requests.get());
            assertEquals(1, server.accepts.get());
        }
    }

    @Test
    void networkTimeoutAlsoBoundsABlockedSocketWrite() throws Exception {
        try (var server = new AmbiguousFailureTest.FakeServer();
             Connection c = connect(server); Statement s = c.createStatement()) {
            c.setNetworkTimeout(Runnable::run, 150);
            server.stopReading = true;
            String sql = "INSERT INTO t VALUES ('" + "x".repeat(12 * 1024 * 1024) + "')";
            long start = System.nanoTime();
            SQLException error = assertThrows(SQLException.class, () -> s.executeUpdate(sql));
            assertEquals("08006", error.getSQLState());
            assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 5000);
            assertTrue(c.isClosed());
            assertEquals(1, server.accepts.get());
        }
    }

    @Test
    void zeroDisablesTheNetworkLimitAndInvalidSettingsAreRefused() throws Exception {
        try (var server = new AmbiguousFailureTest.FakeServer(AmbiguousFailureTest.Act.OK);
             Connection c = connect(server); Statement s = c.createStatement()) {
            assertEquals(0, c.getNetworkTimeout());
            assertThrows(SQLException.class, () -> c.setNetworkTimeout(null, 1));
            assertThrows(SQLException.class, () -> c.setNetworkTimeout(Runnable::run, -1));
            c.setNetworkTimeout(Runnable::run, 1);
            c.setNetworkTimeout(Runnable::run, 0);
            assertEquals(0, c.getNetworkTimeout());
            assertEquals(1, s.executeUpdate("INSERT INTO t VALUES (1)"));
        }
    }

    @Test
    void queryTimeoutRemainsIndependentAndCanLeaveTheConnectionUsable() throws Exception {
        try (var server = new AmbiguousFailureTest.FakeServer(AmbiguousFailureTest.Act.LATE_IN_GRACE);
             Connection c = connect(server); Statement s = c.createStatement()) {
            c.setNetworkTimeout(Runnable::run, 3000);
            s.setQueryTimeout(1);
            assertEquals(1, s.executeUpdate("INSERT INTO t VALUES (1)"));
            assertFalse(c.isClosed());
            assertEquals(3000, c.getNetworkTimeout());
        }
    }

    @Test
    void catalogFailureIsNotReportedAsAnEmptyCatalog() throws Exception {
        try (var server = new AmbiguousFailureTest.FakeServer(); Connection c = connect(server)) {
            var metadata = c.getMetaData();
            c.close();
            SQLException failure = assertThrows(SQLException.class,
                    () -> metadata.getTables(null, null, "%", null));
            assertEquals("08003", failure.getSQLState());
        }
    }

    @Test
    void validationAndAbortValidateTheirArguments() throws Exception {
        try (var server = new AmbiguousFailureTest.FakeServer(AmbiguousFailureTest.Act.OK);
             Connection c = connect(server)) {
            assertEquals("22023", assertThrows(SQLException.class, () -> c.isValid(-1)).getSQLState());
            assertEquals("22023", assertThrows(SQLException.class, () -> c.abort(null)).getSQLState());
            c.abort(Runnable::run);
            assertFalse(c.isValid(0));
        }
    }

    @Test
    void dataSourceLoginTimeoutBoundsAnUnansweredHandshake() throws Exception {
        PdbDataSource source = new PdbDataSource();
        assertThrows(SQLException.class, () -> source.setLoginTimeout(-1));
        try (ServerSocket listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            source.setHost(listener.getInetAddress().getHostAddress());
            source.setPort(listener.getLocalPort());
            source.setPreferUnixSocket(false);
            source.setLoginTimeout(1);
            source.setConnectTimeout(10000);
            var accepted = CompletableFuture.supplyAsync(() -> {
                try { return listener.accept(); }
                catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
            });
            long start = System.nanoTime();
            try {
                assertThrows(SQLTimeoutException.class, source::getConnection);
                assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 3000);
            } finally {
                accepted.get(2, TimeUnit.SECONDS).close();
            }
        }
    }
}
