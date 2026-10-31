package org.particledb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A request that ends in a client-side timeout or a transport failure is
 * AMBIGUOUS -- the server may still execute and commit it -- so the driver
 * surfaces it, never resends it, and CLOSES the connection: its socket may
 * still deliver the late reply, and the statement may still be running inside
 * the connection's transaction. Before this, a {@code setQueryTimeout} expiry
 * left the Connection open and usable on the same socket with the timed-out
 * statement still in flight, so a pool (HikariCP) recycled it.
 *
 * <p>A timed-out request is first RESET ({@code RST_STREAM(CANCEL)}; a server
 * that cancels answers 57014 at once): only a request that still has not
 * answered within {@code PdbConnection.CANCEL_GRACE_SECONDS} is ambiguous. The
 * scripted server here never cancels, so its LATE answer comes after the
 * grace ({@link Act#LATE}), or inside it ({@link Act#LATE_IN_GRACE}: the
 * outcome is then known and the connection stays in service).
 *
 * <p>A scripted v2 server (no ParticleDB) counts accepted connections and the
 * requests it read; each request is answered per a plan.
 */
public class AmbiguousFailureTest {

    enum Act { LATE, LATE_IN_GRACE, CLOSE, OK }

    static final class FakeServer implements AutoCloseable {
        final ServerSocket ss;
        final AtomicInteger accepts = new AtomicInteger();
        final AtomicInteger requests = new AtomicInteger();
        final Deque<Act> plan;
        volatile boolean stopReading;
        final java.util.concurrent.CountDownLatch resumeReading = new java.util.concurrent.CountDownLatch(1);

        FakeServer(Act... plan) throws IOException {
            this.ss = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
            this.plan = new ArrayDeque<>(List.of(plan));
            Thread t = new Thread(this::acceptLoop, "fake-v2-accept");
            t.setDaemon(true);
            t.start();
        }

        int port() { return ss.getLocalPort(); }

        private void acceptLoop() {
            while (true) {
                try {
                    Socket s = ss.accept();
                    accepts.incrementAndGet();
                    Thread t = new Thread(() -> serve(s), "fake-v2-conn");
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

        private static byte[] v1Frame(byte type, byte[] payload) {
            byte[] f = new byte[8 + payload.length];
            f[0] = (byte) payload.length;
            f[6] = type;
            System.arraycopy(payload, 0, f, 8, payload.length);
            return f;
        }

        private static byte[] v2Ok(long sid, long affected) {
            byte[] f = new byte[11 + 8];
            f[0] = 2; f[1] = (byte) 0x82; f[2] = 0x01; // OK, END_STREAM
            for (int i = 0; i < 4; i++) f[3 + i] = (byte) (sid >> (8 * i));
            f[7] = 8;
            for (int i = 0; i < 8; i++) f[11 + i] = (byte) (affected >> (8 * i));
            return f;
        }

        private void serve(Socket s) {
            try (s) {
                s.setTcpNoDelay(true);
                InputStream in = s.getInputStream();
                OutputStream out = s.getOutputStream();
                byte[] hdr = new byte[8];
                readN(in, hdr);
                readN(in, new byte[(hdr[0] & 0xFF) | ((hdr[1] & 0xFF) << 8)]);
                // HELLO_OK: v2 mux, Arrow, 16 streams.
                out.write(v1Frame((byte) 0x80, new byte[] {2, 0x02, 0x01, 16, 0, 0, 0, 0, 0, 0, 0}));
                out.flush();
                while (true) {
                    byte[] qh = new byte[11];
                    readN(in, qh);
                    if (stopReading) {
                        s.setReceiveBufferSize(1024);
                        resumeReading.await();
                        return;
                    }
                    int plen = (qh[7] & 0xFF) | ((qh[8] & 0xFF) << 8) | ((qh[9] & 0xFF) << 16);
                    byte[] body = new byte[plen];
                    readN(in, body);
                    byte type = qh[1];
                    long sid = (qh[3] & 0xFFL) | ((qh[4] & 0xFFL) << 8)
                            | ((qh[5] & 0xFFL) << 16) | ((qh[6] & 0xFFL) << 24);
                    if (type != 0x01 && type != 0x02 && type != 0x21) continue; // not a request
                    // The session TimeZone the driver sets at connect is
                    // session setup, not one of the requests under test.
                    if (type == 0x01 && new String(body, java.nio.charset.StandardCharsets.UTF_8)
                            .startsWith("SET TimeZone TO ")) {
                        out.write(v2Ok(sid, 0));
                        out.flush();
                        continue;
                    }
                    requests.incrementAndGet();
                    Act act;
                    synchronized (plan) {
                        act = plan.isEmpty() ? Act.OK : plan.poll();
                    }
                    if (act == Act.CLOSE) return;
                    // Past the 1 s queryTimeout AND the driver's cancel grace.
                    if (act == Act.LATE) Thread.sleep(1000L * (PdbConnection.CANCEL_GRACE_SECONDS + 2));
                    // Past the 1 s queryTimeout, inside the grace.
                    if (act == Act.LATE_IN_GRACE) Thread.sleep(1500);
                    out.write(v2Ok(sid, 1));
                    out.flush();
                }
            } catch (IOException | InterruptedException e) {
                // connection ended
            }
        }

        @Override
        public void close() throws IOException { resumeReading.countDown(); ss.close(); }
    }

    private FakeServer srv;

    @AfterEach
    void stop() throws IOException {
        if (srv != null) srv.close();
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + srv.port()
                + "/test?preferUnixSocket=false");
    }

    @Test
    void queryTimeoutClosesTheConnectionAndIsNotResent() throws Exception {
        srv = new FakeServer(Act.LATE);
        Connection c = connect();
        try (Statement st = c.createStatement()) {
            st.setQueryTimeout(1);
            assertThrows(SQLTimeoutException.class, () -> st.executeUpdate("UPDATE t SET x = x + 1"));
        }
        assertEquals(1, srv.requests.get(), "the timed-out statement was resent");
        // The late reply is still in flight on this socket, and the statement
        // may still commit: the connection must be closed, not reusable.
        SQLException e = null;
        try {
            c.createStatement().executeUpdate("SELECT 1");
        } catch (SQLException x) {
            e = x;
        }
        assertEquals(1, srv.requests.get(), "the next statement was sent on the timed-out"
                + " socket, with the late reply still in flight");
        assertNotNull(e);
        assertTrue(e.getSQLState().startsWith("08"), "SQLSTATE " + e.getSQLState());
        assertTrue(c.isClosed(), "a timed-out connection was left open");
        // A new connection opens a new socket and works.
        try (Connection c2 = connect(); Statement st = c2.createStatement()) {
            assertEquals(1, st.executeUpdate("UPDATE t SET x = 1"));
        }
        assertEquals(2, srv.accepts.get());
    }

    /**
     * The server answers after the query timeout but inside the cancel grace:
     * its answer is the outcome (the statement ran), it is returned, and the
     * connection stays usable -- nothing about it is ambiguous any more.
     */
    @Test
    void anAnswerInsideTheCancelGraceIsTheOutcome() throws Exception {
        srv = new FakeServer(Act.LATE_IN_GRACE);
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.setQueryTimeout(1);
            assertEquals(1, st.executeUpdate("UPDATE t SET x = x + 1"));
            assertFalse(c.isClosed(), "a request whose outcome is known closed the connection");
            assertEquals(1, st.executeUpdate("UPDATE t SET x = 1"));
        }
        assertEquals(2, srv.requests.get());
        assertEquals(1, srv.accepts.get());
    }

    @Test
    void preparedQueryTimeoutClosesTheConnection() throws Exception {
        srv = new FakeServer(Act.LATE);
        Connection c = connect();
        try (java.sql.PreparedStatement ps = c.prepareStatement("UPDATE t SET x = ?")) {
            ps.setQueryTimeout(1);
            ps.setInt(1, 7);
            assertThrows(SQLTimeoutException.class, ps::executeUpdate);
        }
        assertEquals(1, srv.requests.get(), "the timed-out statement was resent");
        assertTrue(c.isClosed(), "a timed-out connection was left open");
    }

    @Test
    void transportLossIsRaisedOnceAndTheConnectionIsUnusable() throws Exception {
        srv = new FakeServer(Act.CLOSE);
        Connection c = connect();
        SQLException e = assertThrows(SQLException.class,
                () -> c.createStatement().executeUpdate("SELECT KV_SET('k', 'v')"));
        assertTrue(e.getSQLState().startsWith("08"), "SQLSTATE " + e.getSQLState());
        assertEquals(1, srv.requests.get(), "the statement was resent");
        assertTrue(c.isClosed(), "a connection whose socket died reports itself open");
        assertThrows(SQLException.class, () -> c.createStatement().executeUpdate("SELECT 1"));
        assertEquals(1, srv.requests.get());
        assertEquals(1, srv.accepts.get(), "the driver reconnected on its own");
    }
}
