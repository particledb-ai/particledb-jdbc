package org.particledb;

import org.junit.jupiter.api.Test;
import org.particledb.wire.WireV2Codec;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The driver's real connect path, driven against a protocol-level stand-in for
 * the server.
 *
 * <p>{@link WireStub} is a plain {@link ServerSocket} that speaks the frame
 * layout read out of the server's native-wire codec and
 * the server's native-wire multiplexer: it answers HELLO with a HELLO_OK
 * carrying whatever {@code chosen_caps} the test wants, then records the very
 * next frame the driver sends. No database is involved - this asserts what the
 * driver PUTS ON THE WIRE and how it reacts to what comes back, which is
 * exactly the half that was missing.
 *
 * <p>Both framings are covered: a stub advertising {@code CAP_V2_MUX} makes the
 * driver upgrade and send {@code F_AUTH} in the 11-byte v2 header, and a stub
 * that withholds it keeps the driver on v1 {@code MSG_AUTH} with the 8-byte
 * header.
 */
public class NativeWireAuthConnectTest {

    static {
        try { Class.forName("org.particledb.Driver"); }
        catch (ClassNotFoundException e) { throw new ExceptionInInitializerError(e); }
    }

    // ── Protocol stand-in ─────────────────────────────────────────────────

    /** One frame as the stub observed it. */
    static final class Frame {
        final int type;
        final long streamId;
        final byte[] payload;
        Frame(int type, long streamId, byte[] payload) {
            this.type = type; this.streamId = streamId; this.payload = payload;
        }
    }

    static final class WireStub implements AutoCloseable {
        private final ServerSocket server;
        private final Thread thread;
        private final byte caps;
        private final Integer rejectCode;      // null ⇒ accept the credentials
        private final String rejectMessage;
        /** Frames the driver sent AFTER the HELLO handshake, in order. */
        final LinkedBlockingQueue<Frame> received = new LinkedBlockingQueue<>();

        WireStub(byte caps, Integer rejectCode, String rejectMessage) throws IOException {
            this.caps = caps;
            this.rejectCode = rejectCode;
            this.rejectMessage = rejectMessage;
            this.server = new ServerSocket(0, 1, java.net.InetAddress.getByName("127.0.0.1"));
            this.thread = new Thread(this::serve, "pdb-wire-stub");
            this.thread.setDaemon(true);
            this.thread.start();
        }

        int port() { return server.getLocalPort(); }
        boolean v2() { return (caps & WireV2Codec.CAP_V2_MUX) != 0; }

        private void serve() {
            try (Socket s = server.accept()) {
                s.setTcpNoDelay(true);
                DataInputStream in = new DataInputStream(s.getInputStream());
                OutputStream out = s.getOutputStream();

                // 1. HELLO - always v1-framed, whatever mode follows.
                Frame hello = readV1(in);
                if (hello == null || hello.type != WireV2Codec.MSG_HELLO) return;

                // 2. HELLO_OK - also v1-framed; it is what decides the framing
                //    of everything after it.
                //    [u8 ver][u8 caps][u8 fmt][u32_le max_streams][u32_le window]
                byte[] helloOk = new byte[]{
                        2, caps, WireV2Codec.FMT_ARROW_IPC,
                        100, 0, 0, 0,
                        0, 0, 1, 0,
                };
                writeV1(out, WireV2Codec.MSG_HELLO_OK, 0, helloOk);

                // 3. Everything the driver sends next.
                boolean authenticated = false;
                while (true) {
                    Frame f = v2() ? readV2(in) : readV1(in);
                    if (f == null) return;
                    received.add(f);
                    if (f.type == WireV2Codec.MSG_AUTH) {
                        if (rejectCode == null) {
                            // OK carries [u64_le affected = 1].
                            byte[] ok = new byte[8];
                            ok[0] = 1;
                            write(out, WireV2Codec.MSG_OK, f.streamId, ok);
                            authenticated = true;
                        } else {
                            // ERROR carries [u16_le code][utf8 message], then
                            // the server tears the connection down.
                            byte[] msg = rejectMessage.getBytes(StandardCharsets.UTF_8);
                            byte[] p = new byte[2 + msg.length];
                            p[0] = (byte) (rejectCode & 0xFF);
                            p[1] = (byte) ((rejectCode >> 8) & 0xFF);
                            System.arraycopy(msg, 0, p, 2, msg.length);
                            write(out, WireV2Codec.MSG_ERROR, f.streamId, p);
                            out.flush();
                            return;
                        }
                    } else if (f.type == WireV2Codec.MSG_QUERY) {
                        // The session TimeZone the driver sets at connect
                        // (PdbConnection.applySessionTimeZone): OK, 0 rows.
                        write(out, WireV2Codec.MSG_OK, f.streamId, new byte[8]);
                    } else if (f.type == WireV2Codec.MSG_PING) {
                        // Only answered once authenticated - mirrors the real
                        // server, whose gate refuses PING before the credential
                        // frame. Lets a test prove ordering: auth, then traffic.
                        if (authenticated || (caps & WireV2Codec.CAP_AUTH_REQUIRED) == 0) {
                            write(out, WireV2Codec.MSG_PONG, f.streamId, new byte[0]);
                        }
                    }
                    out.flush();
                }
            } catch (IOException ignored) {
                // client hung up - normal at end of test
            }
        }

        private void write(OutputStream out, int type, long streamId, byte[] payload)
                throws IOException {
            if (v2()) writeV2(out, type, streamId, payload);
            else writeV1(out, type, (int) streamId, payload);
        }

        private static Frame readV1(DataInputStream in) throws IOException {
            byte[] h = new byte[8];
            if (!readFully(in, h)) return null;
            int len = (int) u32(h, 0);
            long sid = (h[4] & 0xFFL) | ((h[5] & 0xFFL) << 8);
            int type = h[6] & 0xFF;
            byte[] payload = new byte[len];
            if (len > 0 && !readFully(in, payload)) return null;
            return new Frame(type, sid, payload);
        }

        private static Frame readV2(DataInputStream in) throws IOException {
            byte[] h = new byte[11];
            if (!readFully(in, h)) return null;
            if (h[0] != 2) throw new IOException("expected a v2 frame, got version " + h[0]);
            int type = h[1] & 0xFF;
            long sid = u32(h, 3);
            int len = (int) u32(h, 7);
            byte[] payload = new byte[len];
            if (len > 0 && !readFully(in, payload)) return null;
            return new Frame(type, sid, payload);
        }

        private static void writeV1(OutputStream out, int type, int sid, byte[] payload)
                throws IOException {
            byte[] h = new byte[8];
            putU32(h, 0, payload.length);
            h[4] = (byte) (sid & 0xFF);
            h[5] = (byte) ((sid >> 8) & 0xFF);
            h[6] = (byte) type;
            out.write(h);
            out.write(payload);
            out.flush();
        }

        private static void writeV2(OutputStream out, int type, long sid, byte[] payload)
                throws IOException {
            byte[] h = new byte[11];
            h[0] = 2;
            h[1] = (byte) type;
            h[2] = WireV2Codec.FLAG_END_STREAM;
            putU32(h, 3, (int) sid);
            putU32(h, 7, payload.length);
            out.write(h);
            out.write(payload);
            out.flush();
        }

        private static boolean readFully(DataInputStream in, byte[] b) throws IOException {
            int off = 0;
            while (off < b.length) {
                int n = in.read(b, off, b.length - off);
                if (n < 0) return false;
                off += n;
            }
            return true;
        }

        private static long u32(byte[] b, int off) {
            return (b[off] & 0xFFL) | ((b[off + 1] & 0xFFL) << 8)
                 | ((b[off + 2] & 0xFFL) << 16) | ((b[off + 3] & 0xFFL) << 24);
        }

        private static void putU32(byte[] b, int off, int v) {
            b[off] = (byte) (v & 0xFF);
            b[off + 1] = (byte) ((v >> 8) & 0xFF);
            b[off + 2] = (byte) ((v >> 16) & 0xFF);
            b[off + 3] = (byte) ((v >> 24) & 0xFF);
        }

        @Override public void close() throws IOException {
            server.close();
            thread.interrupt();
        }
    }

    private static String url(WireStub stub, String query) {
        return "jdbc:particledb://127.0.0.1:" + stub.port() + "/test?preferUnixSocket=false"
                + "&connectTimeout=10000" + (query == null ? "" : "&" + query);
    }

    private static final byte AUTH_V2 =
            (byte) (WireV2Codec.CAP_V2_MUX | WireV2Codec.CAP_AUTH_REQUIRED);
    private static final byte AUTH_V1 = WireV2Codec.CAP_AUTH_REQUIRED;
    private static final byte NO_AUTH_V2 = WireV2Codec.CAP_V2_MUX;

    /**
     * Assert that a refused connect put nothing on the wire but teardown.
     *
     * <p>A rejected connection still closes politely - {@code mux.close()}
     * writes GOAWAY (v2) / CLOSE (v1) - so the queue is not necessarily empty.
     * What must never appear is a credential frame or any request frame.
     */
    /**
     * After a successful connect the only frame is the session setup every
     * connection makes - the JVM zone as the session {@code TimeZone}, as
     * pgjdbc's startup packet carries it - and never a credential frame.
     */
    private static void assertOnlySessionSetup(WireStub stub) throws InterruptedException {
        Frame f = stub.received.poll(5, TimeUnit.SECONDS);
        assertNotNull(f, "the driver sets the session TimeZone at connect");
        assertEquals(WireV2Codec.MSG_QUERY, f.type, "frame type 0x" + Integer.toHexString(f.type));
        assertEquals("SET TimeZone TO '"
                        + Timestamptz.sessionSetting(java.time.ZoneId.systemDefault()) + "'",
                new String(f.payload, StandardCharsets.UTF_8));
        assertNull(stub.received.poll(500, TimeUnit.MILLISECONDS),
                "nothing else — in particular no credential frame");
    }

    private static void assertOnlyTeardown(WireStub stub) throws InterruptedException {
        Frame f;
        while ((f = stub.received.poll(500, TimeUnit.MILLISECONDS)) != null) {
            // Frame.type is the unsigned opcode byte; the codec constants are
            // signed bytes, so mask before comparing (GOAWAY is 0x8a).
            assertTrue(f.type == (WireV2Codec.MSG_GOAWAY & 0xFF)
                            || f.type == (WireV2Codec.MSG_CLOSE & 0xFF),
                    "a refused connection may only send teardown, but sent frame type 0x"
                    + Integer.toHexString(f.type));
        }
    }

    // ── Tests ─────────────────────────────────────────────────────────────

    @Test
    void authRequiredV2_sendsTheCredentialFrameBeforeAnythingElse() throws Exception {
        try (WireStub stub = new WireStub(AUTH_V2, null, null)) {
            try (Connection c = DriverManager.getConnection(
                    url(stub, "user=alice&password=s3cret"))) {

                Frame f = stub.received.poll(5, TimeUnit.SECONDS);
                assertNotNull(f, "driver must send a frame after HELLO_OK");
                assertEquals(WireV2Codec.MSG_AUTH, f.type,
                        "the FIRST frame after the handshake must be the credential frame");
                assertArrayEquals(WireV2Codec.encodeAuthPayload("alice", "s3cret"), f.payload);

                // Ordering: normal traffic only flows AFTER the credential
                // frame. The stub refuses to PONG until it has seen the AUTH,
                // so a true here proves the driver got the sequence right.
                assertTrue(c.isValid(5), "connection must be usable once authenticated");
                // Then the session setup every connection makes (the JVM zone
                // as the session TimeZone), then the caller's traffic.
                Frame setup = stub.received.poll(5, TimeUnit.SECONDS);
                assertNotNull(setup);
                assertEquals(WireV2Codec.MSG_QUERY, setup.type,
                        "the session TimeZone follows the credential frame");
                Frame next = stub.received.poll(5, TimeUnit.SECONDS);
                assertNotNull(next);
                assertEquals(WireV2Codec.MSG_PING, next.type,
                        "the credential frame must precede ordinary traffic");
            }
        }
    }

    @Test
    void authRequiredV1_sendsTheCredentialFrameInV1Framing() throws Exception {
        // No CAP_V2_MUX ⇒ the driver stays on the 8-byte v1 header; the stub's
        // readV1 would blow up on a v2 header, so this asserts the framing too.
        //
        // requireMux=false is MANDATORY here and is the point of the test: the
        // driver now REFUSES a server that does not negotiate v2, because
        // falling back silently meant an operator could believe they had
        // multiplexing while getting none. This test is the one place that
        // deliberately wants v1, so it opts out explicitly. If this parameter
        // ever stops being needed here, the fail-closed default has regressed.
        try (WireStub stub = new WireStub(AUTH_V1, null, null)) {
            try (Connection c = DriverManager.getConnection(
                    url(stub, "user=bob&password=hunter2&requireMux=false"))) {
                assertNotNull(c);

                Frame f = stub.received.poll(5, TimeUnit.SECONDS);
                assertNotNull(f, "driver must send a frame after HELLO_OK");
                assertEquals(WireV2Codec.MSG_AUTH, f.type);
                assertArrayEquals(WireV2Codec.encodeAuthPayload("bob", "hunter2"), f.payload);
            }
        }
    }

    @Test
    void credentialsFromProperties_reachTheWire() throws Exception {
        try (WireStub stub = new WireStub(AUTH_V2, null, null)) {
            Properties p = new Properties();
            p.setProperty("user", "svc-acct");
            p.setProperty("password", "p@ss&w=rd? ünïcode");
            try (Connection c = DriverManager.getConnection(url(stub, null), p)) {
                assertNotNull(c);
                Frame f = stub.received.poll(5, TimeUnit.SECONDS);
                assertNotNull(f);
                assertEquals(WireV2Codec.MSG_AUTH, f.type);
                assertArrayEquals(
                        WireV2Codec.encodeAuthPayload("svc-acct", "p@ss&w=rd? ünïcode"),
                        f.payload);
            }
        }
    }

    @Test
    void rejectedCredentials_surfaceTheServersMessage() throws Exception {
        String serverMsg = "password authentication failed for user \"alice\"";
        try (WireStub stub = new WireStub(AUTH_V2, WireV2Codec.ERR_AUTH_FAILED, serverMsg)) {
            SQLException e = assertThrows(SQLException.class, () ->
                    DriverManager.getConnection(url(stub, "user=alice&password=wrong")));

            assertTrue(e.getMessage().contains(serverMsg),
                    "the server's own message must survive: " + e.getMessage());
            assertEquals("28P01", e.getSQLState());
            assertEquals(WireV2Codec.ERR_AUTH_FAILED, e.getErrorCode());
        }
    }

    @Test
    void authRequiredButNoCredentials_failsAtConnectNotAtFirstQuery() throws Exception {
        try (WireStub stub = new WireStub(AUTH_V2, null, null)) {
            SQLException e = assertThrows(SQLException.class, () ->
                    DriverManager.getConnection(url(stub, null)));

            assertEquals("28000", e.getSQLState());
            assertTrue(e.getMessage().contains("server requires authentication"),
                    e.getMessage());

            // …and it did not blurt credentials or a query at a server that
            // told it credentials were required.
            assertOnlyTeardown(stub);
        }
    }

    @Test
    void noAuthServer_sendsNoCredentialFrameEvenWhenCredentialsAreConfigured()
            throws Exception {
        // The load-bearing case: a --no-auth server withholds CAP_AUTH_REQUIRED,
        // so the driver must put NOTHING extra on the wire - the handshake stays
        // byte-identical to a driver with no auth support at all.
        try (WireStub stub = new WireStub(NO_AUTH_V2, null, null)) {
            try (Connection c = DriverManager.getConnection(
                    url(stub, "user=alice&password=s3cret"))) {
                assertNotNull(c);
                // A --no-auth server must receive no credential frame.
                assertOnlySessionSetup(stub);
            }
        }
    }

    @Test
    void noAuthServer_withNoCredentialsIsUnchanged() throws Exception {
        try (WireStub stub = new WireStub(NO_AUTH_V2, null, null)) {
            try (Connection c = DriverManager.getConnection(url(stub, null))) {
                assertNotNull(c);
                assertOnlySessionSetup(stub);
            }
        }
    }

    @Test
    void blankUserOnAnAuthRequiredServer_failsWithTheMissingCredentialError()
            throws Exception {
        // `?user=` is not a username the server would accept (its decoder
        // rejects an empty one), so the driver must report the real problem
        // rather than send a frame that comes back as "malformed payload".
        try (WireStub stub = new WireStub(AUTH_V2, null, null)) {
            SQLException e = assertThrows(SQLException.class, () ->
                    DriverManager.getConnection(url(stub, "user=&password=pw")));
            assertEquals("28000", e.getSQLState());
            assertOnlyTeardown(stub);
        }
    }
}
