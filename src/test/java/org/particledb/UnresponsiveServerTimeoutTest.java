package org.particledb;

import org.junit.jupiter.api.Test;
import org.particledb.ds.PdbDataSource;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A server that accepts the connection, completes the handshake and then stops
 * answering -- what a SIGSTOPped server (Jepsen's pause nemesis) looks like to
 * a client: the socket stays open and no byte, or only part of a frame,
 * arrives. Every blocking wait must end at the caller's bound with an 08xxx
 * error and a closed connection; none may block on the socket forever.
 *
 * <p>Found on a Jepsen run: a thread sat in {@code StreamMux.readUntil ->
 * FrameReader.fill -> Socket.read} for more than 3.5 hours with
 * {@code socketTimeout=15000} in the URL. The driver never read that property,
 * so with no {@code setQueryTimeout} and no {@code setNetworkTimeout} the
 * frame reader read with SO_TIMEOUT 0.
 *
 * <p>The scripted server (no ParticleDB) answers the connect-time
 * {@code SET TimeZone} and PREPARE, and then, for every QUERY / BATCH /
 * EXECUTE, writes nothing ({@link Mode#SILENT}), the first 5 of an OK frame's
 * 11 header bytes ({@link Mode#PARTIAL_HEADER}), or the full header and 3 of
 * its 8 payload bytes ({@link Mode#PARTIAL_PAYLOAD}).
 */
class UnresponsiveServerTimeoutTest {

    enum Mode { SILENT, PARTIAL_HEADER, PARTIAL_PAYLOAD }

    /** Far longer than any bound under test; a hang fails the test instead of hanging the build. */
    private static final Duration HANG_GUARD = Duration.ofSeconds(20);
    private static final int SOCKET_TIMEOUT_MS = 500;
    /** Scheduling slack on a loaded machine; still far below "forever". */
    private static final long SLACK_MS = 3000;

    static final class StoppedServer implements AutoCloseable {
        final ServerSocket ss;
        final Mode mode;
        final AtomicInteger accepts = new AtomicInteger();
        /** QUERY / BATCH / EXECUTE frames read (the requests left unanswered). */
        final AtomicInteger requests = new AtomicInteger();
        final AtomicInteger prepares = new AtomicInteger();
        final AtomicInteger resets = new AtomicInteger();
        final CopyOnWriteArrayList<Socket> sockets = new CopyOnWriteArrayList<>();

        StoppedServer(Mode mode) throws IOException {
            this.mode = mode;
            this.ss = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
            Thread t = new Thread(this::acceptLoop, "stopped-server-accept");
            t.setDaemon(true);
            t.start();
        }

        int port() { return ss.getLocalPort(); }

        private void acceptLoop() {
            while (true) {
                try {
                    Socket s = ss.accept();
                    accepts.incrementAndGet();
                    sockets.add(s);
                    Thread t = new Thread(() -> serve(s), "stopped-server-conn");
                    t.setDaemon(true);
                    t.start();
                } catch (IOException e) {
                    return;
                }
            }
        }

        private static void readN(InputStream in, byte[] b) throws IOException {
            int off = 0;
            while (off < b.length) {
                int r = in.read(b, off, b.length - off);
                if (r < 0) throw new EOFException();
                off += r;
            }
        }

        private static byte[] v2Frame(byte type, long sid, byte[] payload) {
            byte[] f = new byte[11 + payload.length];
            f[0] = 2; f[1] = type; f[2] = 0x01; // END_STREAM
            for (int i = 0; i < 4; i++) f[3 + i] = (byte) (sid >> (8 * i));
            for (int i = 0; i < 4; i++) f[7 + i] = (byte) (payload.length >> (8 * i));
            System.arraycopy(payload, 0, f, 11, payload.length);
            return f;
        }

        private void serve(Socket s) {
            try {
                s.setTcpNoDelay(true);
                InputStream in = s.getInputStream();
                OutputStream out = s.getOutputStream();
                byte[] hdr = new byte[8];
                readN(in, hdr);
                readN(in, new byte[(hdr[0] & 0xFF) | ((hdr[1] & 0xFF) << 8)]);
                // HELLO_OK (v1 framed): v2 mux + prepared, Arrow, 16 streams.
                byte[] hello = {2, (byte) (0x02 | 0x80), 0x01, 16, 0, 0, 0, 0, 0, 0, 0};
                byte[] hf = new byte[8 + hello.length];
                hf[0] = (byte) hello.length;
                hf[6] = (byte) 0x80;
                System.arraycopy(hello, 0, hf, 8, hello.length);
                out.write(hf);
                out.flush();
                while (true) {
                    byte[] qh = new byte[11];
                    readN(in, qh);
                    int plen = (qh[7] & 0xFF) | ((qh[8] & 0xFF) << 8)
                            | ((qh[9] & 0xFF) << 16) | ((qh[10] & 0xFF) << 24);
                    byte[] body = new byte[plen];
                    readN(in, body);
                    byte type = qh[1];
                    long sid = (qh[3] & 0xFFL) | ((qh[4] & 0xFFL) << 8)
                            | ((qh[5] & 0xFFL) << 16) | ((qh[6] & 0xFFL) << 24);
                    if (type == 0x06) { resets.incrementAndGet(); continue; } // RST_STREAM: ignored (stopped)
                    if (type == 0x01 && new String(body, java.nio.charset.StandardCharsets.UTF_8)
                            .startsWith("SET TimeZone TO ")) {
                        out.write(v2Frame((byte) 0x82, sid, new byte[8]));
                        out.flush();
                        continue;
                    }
                    if (type == 0x20) { // PREPARE -> PREPARE_OK
                        prepares.incrementAndGet();
                        out.write(v2Frame((byte) 0x8d, sid, new byte[0]));
                        out.flush();
                        continue;
                    }
                    if (type != 0x01 && type != 0x02 && type != 0x21) continue;
                    int seen = requests.incrementAndGet();
                    // Only the first request gets the partial bytes: anything
                    // after them on this socket would be a different frame.
                    if (seen == 1 && mode != Mode.SILENT) {
                        byte[] ok = v2Frame((byte) 0x82, sid, new byte[] {1, 0, 0, 0, 0, 0, 0, 0});
                        int n = mode == Mode.PARTIAL_HEADER ? 5 : 11 + 3;
                        out.write(Arrays.copyOf(ok, n));
                        out.flush();
                    }
                    // ...and then nothing more: the process is stopped.
                }
            } catch (IOException e) {
                // the client closed the socket
            }
        }

        void awaitRequests(int n) throws InterruptedException {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (requests.get() < n) {
                if (System.nanoTime() > until) fail("server saw " + requests.get() + " of " + n + " requests");
                Thread.sleep(5);
            }
        }

        @Override
        public void close() throws IOException {
            ss.close();
            for (Socket s : sockets) {
                try { s.close(); } catch (IOException ignored) { }
            }
        }
    }

    private static Connection connect(StoppedServer server, String extra) throws SQLException {
        return DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + server.port()
                + "/test?preferUnixSocket=false" + extra);
    }

    private static void assertBrokenConnectionError(SQLException e) {
        assertNotNull(e.getSQLState(), "no SQLSTATE: " + e);
        assertTrue(e.getSQLState().startsWith("08"), "SQLSTATE " + e.getSQLState() + ": " + e);
    }

    private static long elapsedMs(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    /** One statement against a stopped server, bounded only by the URL's socketTimeout. */
    private static void assertSocketTimeoutBoundsAStatement(Mode mode) throws Exception {
        try (StoppedServer server = new StoppedServer(mode)) {
            Connection c = connect(server, "&socketTimeout=" + SOCKET_TIMEOUT_MS);
            try (Statement st = c.createStatement()) {
                assertEquals(SOCKET_TIMEOUT_MS, c.getNetworkTimeout(),
                        "socketTimeout in the URL did not set the connection's network timeout");
                long start = System.nanoTime();
                SQLException e = assertTimeoutPreemptively(HANG_GUARD,
                        () -> assertThrows(SQLException.class, () -> st.executeUpdate("UPDATE t SET x = 1")),
                        "the call blocked on the socket past socketTimeout");
                long ms = elapsedMs(start);
                assertBrokenConnectionError(e);
                assertTrue(ms >= SOCKET_TIMEOUT_MS - 50, "failed before the timeout: " + ms + " ms");
                assertTrue(ms < SOCKET_TIMEOUT_MS + SLACK_MS, "took " + ms + " ms");
                assertTrue(c.isClosed(), "a timed-out connection was left open");
                assertEquals(1, server.requests.get(), "the statement was resent");
                assertEquals(1, server.accepts.get(), "the driver reconnected on its own");
                SQLException next = assertThrows(SQLException.class, () -> st.executeUpdate("SELECT 1"));
                assertBrokenConnectionError(next);
                assertEquals(1, server.requests.get(), "a request was sent on the broken connection");
            } finally {
                c.abort(Runnable::run);
            }
        }
    }

    @Test
    void socketTimeoutBoundsAReadThatGetsNoBytes() throws Exception {
        assertSocketTimeoutBoundsAStatement(Mode.SILENT);
    }

    @Test
    void socketTimeoutBoundsAReadStoppedMidHeader() throws Exception {
        assertSocketTimeoutBoundsAStatement(Mode.PARTIAL_HEADER);
    }

    @Test
    void socketTimeoutBoundsAReadStoppedMidPayload() throws Exception {
        assertSocketTimeoutBoundsAStatement(Mode.PARTIAL_PAYLOAD);
    }

    /** The reported stack: PdbConnection.executeServerPrepared -> StreamMux.readUntil. */
    @Test
    void socketTimeoutBoundsAServerPreparedExecution() throws Exception {
        try (StoppedServer server = new StoppedServer(Mode.SILENT)) {
            Connection c = connect(server, "&socketTimeout=" + SOCKET_TIMEOUT_MS);
            try (PreparedStatement ps = c.prepareStatement("UPDATE t SET x = ? WHERE id = ?")) {
                ps.setInt(1, 7);
                ps.setInt(2, 1);
                long start = System.nanoTime();
                SQLException e = assertTimeoutPreemptively(HANG_GUARD,
                        () -> assertThrows(SQLException.class, ps::executeUpdate),
                        "a server-prepared EXECUTE blocked past socketTimeout");
                long ms = elapsedMs(start);
                assertEquals(1, server.prepares.get(), "the statement did not take the server-prepared path");
                assertBrokenConnectionError(e);
                assertTrue(ms < SOCKET_TIMEOUT_MS + SLACK_MS, "took " + ms + " ms");
                assertTrue(c.isClosed());
                assertEquals(1, server.requests.get(), "the EXECUTE was resent (or rendered as text)");
            } finally {
                c.abort(Runnable::run);
            }
        }
    }

    /**
     * Two statements in flight on one multiplexed connection: one thread holds
     * the read role (blocked on the socket), the other is parked waiting for
     * its stream. Both must fail at the bound, and the connection is closed.
     */
    @Test
    void socketTimeoutFailsEveryStreamWaitingOnTheConnection() throws Exception {
        for (Mode mode : new Mode[] {Mode.SILENT, Mode.PARTIAL_PAYLOAD}) {
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try (StoppedServer server = new StoppedServer(mode)) {
                Connection c = connect(server, "&socketTimeout=" + SOCKET_TIMEOUT_MS);
                try {
                    long start = System.nanoTime();
                    CompletableFuture<SQLException> a = CompletableFuture.supplyAsync(() -> run(c, "UPDATE t SET x = 1"), pool);
                    server.awaitRequests(1);
                    CompletableFuture<SQLException> b = CompletableFuture.supplyAsync(() -> run(c, "UPDATE t SET x = 2"), pool);
                    SQLException ea = a.get(HANG_GUARD.toMillis(), TimeUnit.MILLISECONDS);
                    SQLException eb = b.get(HANG_GUARD.toMillis(), TimeUnit.MILLISECONDS);
                    long ms = elapsedMs(start);
                    assertNotNull(ea, mode + ": first statement succeeded against a stopped server");
                    assertNotNull(eb, mode + ": second statement succeeded against a stopped server");
                    assertBrokenConnectionError(ea);
                    assertBrokenConnectionError(eb);
                    assertTrue(ms < 2L * SOCKET_TIMEOUT_MS + SLACK_MS, mode + ": took " + ms + " ms");
                    assertTrue(c.isClosed(), mode + ": connection left open");
                    assertTrue(server.requests.get() <= 2, mode + ": a statement was resent");
                } catch (java.util.concurrent.TimeoutException te) {
                    fail(mode + ": a statement on the multiplexed connection blocked past socketTimeout");
                } finally {
                    c.abort(Runnable::run);
                }
            } finally {
                pool.shutdownNow();
            }
        }
    }

    private static SQLException run(Connection c, String sql) {
        try (Statement st = c.createStatement()) {
            st.executeUpdate(sql);
            return null;
        } catch (SQLException e) {
            return e;
        }
    }

    /** No socketTimeout: setQueryTimeout alone still bounds the call (cancel, grace, close). */
    @Test
    void queryTimeoutBoundsAStatementOnAStoppedServer() throws Exception {
        try (StoppedServer server = new StoppedServer(Mode.PARTIAL_PAYLOAD)) {
            Connection c = connect(server, "");
            try (Statement st = c.createStatement()) {
                st.setQueryTimeout(1);
                long start = System.nanoTime();
                SQLException e = assertTimeoutPreemptively(HANG_GUARD,
                        () -> assertThrows(SQLException.class, () -> st.executeUpdate("UPDATE t SET x = 1")));
                long ms = elapsedMs(start);
                assertInstanceOf(SQLTimeoutException.class, e, "got " + e);
                assertTrue(ms < 1000L + 1000L * PdbConnection.CANCEL_GRACE_SECONDS + SLACK_MS, "took " + ms + " ms");
                assertEquals(1, server.resets.get(), "the timed-out statement was not reset");
                assertTrue(c.isClosed(), "an unanswered timed-out statement left the connection open");
                assertEquals(1, server.requests.get());
            } finally {
                c.abort(Runnable::run);
            }
        }
    }

    /**
     * No timeout of any kind: Statement.cancel() from another thread must still
     * end the call. The stopped server never answers the reset, so after the
     * cancel grace the outcome is unknown and the connection is closed.
     */
    @Test
    void cancelEndsAStatementBlockedOnAStoppedServer() throws Exception {
        for (Mode mode : new Mode[] {Mode.SILENT, Mode.PARTIAL_HEADER}) {
            try (StoppedServer server = new StoppedServer(mode)) {
                Connection c = connect(server, "");
                try (Statement st = c.createStatement()) {
                    CompletableFuture<SQLException> running = CompletableFuture.supplyAsync(() -> {
                        try {
                            st.executeUpdate("UPDATE t SET x = 1");
                            return null;
                        } catch (SQLException e) {
                            return e;
                        }
                    });
                    server.awaitRequests(1);
                    long start = System.nanoTime();
                    st.cancel();
                    SQLException e;
                    try {
                        e = running.get(HANG_GUARD.toMillis(), TimeUnit.MILLISECONDS);
                    } catch (java.util.concurrent.TimeoutException te) {
                        fail(mode + ": Statement.cancel() did not end a statement blocked on a stopped server");
                        return;
                    }
                    long ms = elapsedMs(start);
                    assertNotNull(e, mode + ": a cancelled statement against a stopped server returned normally");
                    assertBrokenConnectionError(e);
                    assertTrue(ms < 1000L * PdbConnection.CANCEL_GRACE_SECONDS + SLACK_MS, mode + ": took " + ms + " ms");
                    assertEquals(1, server.resets.get(), mode + ": cancel did not reset the stream");
                    assertTrue(c.isClosed(), mode + ": connection left open after an unanswered cancel");
                } finally {
                    c.abort(Runnable::run);
                }
            }
        }
    }

    @Test
    void socketTimeoutIsAdvertisedValidatedAndSettableOnTheDataSource() throws Exception {
        DriverPropertyInfo[] info = new Driver().getPropertyInfo("jdbc:particledb://h/db", new java.util.Properties());
        DriverPropertyInfo st = Arrays.stream(info).filter(p -> "socketTimeout".equals(p.name)).findFirst()
                .orElseThrow(() -> new AssertionError("socketTimeout not in getPropertyInfo"));
        assertEquals("0", st.value);
        try (StoppedServer server = new StoppedServer(Mode.SILENT)) {
            SQLException bad = assertThrows(SQLException.class, () -> connect(server, "&socketTimeout=-1"));
            assertEquals("22023", bad.getSQLState());
            try (Connection c = connect(server, "")) {
                assertEquals(0, c.getNetworkTimeout(), "the default must stay unbounded (pgjdbc's default)");
            }
            PdbDataSource ds = new PdbDataSource();
            ds.setHost("127.0.0.1");
            ds.setPort(server.port());
            ds.setPreferUnixSocket(false);
            ds.setSocketTimeout(750);
            assertEquals(750, ds.getSocketTimeout());
            try (Connection c = ds.getConnection()) {
                assertEquals(750, c.getNetworkTimeout());
            }
        }
    }
}
