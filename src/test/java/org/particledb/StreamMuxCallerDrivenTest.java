package org.particledb;

import org.junit.jupiter.api.Test;
import org.particledb.wire.StreamMux;
import org.particledb.wire.StreamState;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The caller-driven StreamMux: the thread blocked on a result reads the
 * socket itself. Two properties a server-backed test cannot pin down:
 *
 * <ul>
 *   <li>a deadline that expires with a response frame HALF read must not
 *       desynchronise the connection -- the next {@code get} resumes the same
 *       frame and returns it intact;</li>
 *   <li>no thread other than the caller is needed: the response is delivered
 *       with no I/O thread in the JVM.</li>
 * </ul>
 *
 * A scripted server sends HELLO_OK, then the OK frame for stream 1 in two
 * pieces separated by a pause longer than the first wait's timeout.
 */
public class StreamMuxCallerDrivenTest {

    private static byte[] v1Frame(byte type, byte[] payload) {
        byte[] f = new byte[8 + payload.length];
        f[0] = (byte) payload.length; f[1] = (byte) (payload.length >> 8);
        f[2] = (byte) (payload.length >> 16); f[3] = (byte) (payload.length >> 24);
        f[6] = type;
        System.arraycopy(payload, 0, f, 8, payload.length);
        return f;
    }

    private static byte[] v2Ok(long sid, long affected) {
        byte[] f = new byte[11 + 8];
        f[0] = 2; f[1] = (byte) 0x82; f[2] = 0x01;
        for (int i = 0; i < 4; i++) f[3 + i] = (byte) (sid >> (8 * i));
        f[7] = 8;
        for (int i = 0; i < 8; i++) f[11 + i] = (byte) (affected >> (8 * i));
        return f;
    }

    private static void readN(InputStream in, int n) throws Exception {
        byte[] b = new byte[n];
        int off = 0;
        while (off < n) {
            int r = in.read(b, off, n - off);
            if (r < 0) throw new java.io.EOFException();
            off += r;
        }
    }

    /**
     * The server allows 2 streams. Two requests time out on the client (their
     * streams and slots stay live), then the server answers both. Nobody is
     * reading. A third request must get a slot by DRAINING those buffered
     * responses -- not wait out the 60 s slot timeout with its slots' answers
     * already sitting in the socket.
     */
    @Test
    void waitingForASlotDrainsResponsesThatWouldFreeOne() throws Exception {
        try (ServerSocket ss = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            Thread server = new Thread(() -> {
                try (Socket s = ss.accept()) {
                    s.setTcpNoDelay(true);
                    InputStream in = s.getInputStream();
                    OutputStream out = s.getOutputStream();
                    byte[] hdr = new byte[8];
                    int off = 0;
                    while (off < 8) off += in.read(hdr, off, 8 - off);
                    readN(in, (hdr[0] & 0xFF) | ((hdr[1] & 0xFF) << 8));
                    // HELLO_OK: v2 mux, arrow, TWO streams.
                    out.write(v1Frame((byte) 0x80,
                            new byte[] { 2, 0x02, 0x01, 2, 0, 0, 0, 0, 0, 0, 0 }));
                    out.flush();
                    long[] sids = new long[3];
                    for (int q = 0; q < 3; q++) {
                        byte[] qh = new byte[11];
                        off = 0;
                        while (off < 11) off += in.read(qh, off, 11 - off);
                        readN(in, (qh[7] & 0xFF) | ((qh[8] & 0xFF) << 8));
                        sids[q] = (qh[3] & 0xFFL) | ((qh[4] & 0xFFL) << 8)
                                | ((qh[5] & 0xFFL) << 16) | ((qh[6] & 0xFFL) << 24);
                        if (q == 1) {
                            // Both earlier requests have timed out on the client
                            // by now; answer them.
                            Thread.sleep(300);
                            out.write(v2Ok(sids[0], 1));
                            out.write(v2Ok(sids[1], 2));
                            out.flush();
                        }
                    }
                    out.write(v2Ok(sids[2], 3));
                    out.flush();
                    Thread.sleep(2000);
                } catch (Exception ignored) {
                }
            }, "scripted-server");
            server.setDaemon(true);
            server.start();

            StreamMux mux = new StreamMux(null, false, 2000);
            try {
                assertTrue(mux.connect("127.0.0.1", ss.getLocalPort())
                        .get(2, TimeUnit.SECONDS).isV2());
                CompletableFuture<StreamState.StreamResult> f1 = mux.query("SELECT 1");
                CompletableFuture<StreamState.StreamResult> f2 = mux.query("SELECT 2");
                assertThrows(TimeoutException.class, () -> f1.get(50, TimeUnit.MILLISECONDS));
                assertThrows(TimeoutException.class, () -> f2.get(50, TimeUnit.MILLISECONDS));
                Thread.sleep(600); // the answers are now buffered, unread
                long t0 = System.nanoTime();
                StreamState.StreamResult r = assertTimeoutPreemptively(
                        java.time.Duration.ofSeconds(5),
                        () -> mux.query("SELECT 3").get(5, TimeUnit.SECONDS),
                        "a slot wait must drain the buffered responses, not sit out the slot timeout");
                assertEquals(3L, r.affectedRows());
                assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) < 2000);
                assertTrue(f1.isDone() && f2.isDone(), "the drained responses completed their streams");
            } finally {
                mux.close();
            }
        }
    }

    @Test
    void deadlineMidFrameDoesNotDesynchroniseAndNeedsNoIoThread() throws Exception {
        try (ServerSocket ss = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            Thread server = new Thread(() -> {
                try (Socket s = ss.accept()) {
                    s.setTcpNoDelay(true);
                    InputStream in = s.getInputStream();
                    OutputStream out = s.getOutputStream();
                    // HELLO (v1 framing): header + payload.
                    byte[] hdr = new byte[8];
                    int off = 0;
                    while (off < 8) off += in.read(hdr, off, 8 - off);
                    readN(in, (hdr[0] & 0xFF) | ((hdr[1] & 0xFF) << 8));
                    // HELLO_OK: v2 mux, arrow, 16 streams.
                    out.write(v1Frame((byte) 0x80,
                            new byte[] { 2, 0x02, 0x01, 16, 0, 0, 0, 0, 0, 0, 0 }));
                    out.flush();
                    // The client's QUERY on stream 1 (v2 framing).
                    byte[] qh = new byte[11];
                    off = 0;
                    while (off < 11) off += in.read(qh, off, 11 - off);
                    readN(in, (qh[7] & 0xFF) | ((qh[8] & 0xFF) << 8));
                    // Answer in two pieces, the pause outlasting the client's
                    // first (short) wait.
                    byte[] ok = v2Ok(1, 42);
                    out.write(ok, 0, 7);
                    out.flush();
                    Thread.sleep(400);
                    out.write(ok, 7, ok.length - 7);
                    out.flush();
                    Thread.sleep(2000);
                } catch (Exception ignored) {
                }
            }, "scripted-server");
            server.setDaemon(true);
            server.start();

            StreamMux mux = new StreamMux(null, false, 2000);
            try {
                assertTrue(mux.connect("127.0.0.1", ss.getLocalPort())
                        .get(2, TimeUnit.SECONDS).isV2());
                int threadsBefore = Thread.activeCount();
                CompletableFuture<StreamState.StreamResult> f = mux.query("SELECT 1");
                assertThrows(TimeoutException.class, () -> f.get(100, TimeUnit.MILLISECONDS),
                        "the second half of the frame is 400 ms away");
                StreamState.StreamResult r = f.get(3, TimeUnit.SECONDS);
                assertEquals(StreamState.StreamResult.Kind.OK, r.kind);
                assertEquals(42L, r.affectedRows(),
                        "the frame resumed after the timeout must decode intact");
                assertTrue(Thread.activeCount() <= threadsBefore,
                        "the response must be read by the caller, not an I/O thread");
            } finally {
                mux.close();
            }
        }
    }
}
