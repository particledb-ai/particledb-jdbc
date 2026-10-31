package org.particledb;

import io.netty.channel.EventLoopGroup;
import org.junit.jupiter.api.Test;
import org.particledb.wire.SharedEventLoop;
import org.particledb.wire.StreamMux;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression test for a reported thread leak - the per-connection Netty
 * {@code NioEventLoopGroup} leak.
 *
 * <p>Before the fix, every {@link PdbConnection} did
 * {@code new StreamMux(null, ...)}, and {@code StreamMux} responded to a null
 * group by allocating its own {@code NioEventLoopGroup(1)} - i.e. one OS thread
 * per connection. Connections that errored / aborted / were GC'd without an
 * explicit {@code close()} leaked that event-loop thread forever. Over a long
 * run with many reconnects the client JVM exhausted OS threads and crashed.
 *
 * <p>The fix shares a single process-wide {@link SharedEventLoop} group across
 * all connections ({@code ownGroup=false}); {@link PdbConnection#close()} only
 * releases the channel/streams and never shuts the shared group down.
 *
 * <p>This test needs <b>no</b> ParticleDB server: it stands up a tiny raw TCP
 * server that completes the v1 HELLO handshake, so real {@code PdbConnection}s
 * connect end-to-end. It then proves:
 * <ol>
 *   <li>every connection's {@code StreamMux} uses the SAME shared group instance
 *       and does NOT own it,</li>
 *   <li>opening + closing many connections does not grow the event-loop thread
 *       count unboundedly (the leak is gone), and</li>
 *   <li>{@code close()} is idempotent and leaves the shared group alive.</li>
 * </ol>
 */
public class OpenCloseThreadLeakTest {

    /** Counts threads that belong to a Netty event-loop group. */
    private static int eventLoopThreadCount() {
        int n = 0;
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            String name = t.getName();
            if (name == null) continue;
            if (name.startsWith("nioEventLoopGroup")
                    || name.startsWith("pdb-shared-eventloop")
                    || name.startsWith("globalEventExecutor")) {
                n++;
            }
        }
        return n;
    }

    @Test
    void openCloseManyConnections_doesNotLeakEventLoopThreads() throws Exception {
        try (FakeHelloServer server = FakeHelloServer.start()) {
            // requireMux=false: FakeHelloServer answers HELLO_OK without
            // CAP_V2_MUX, and the driver now refuses a non-multiplexed native
            // connection by default. This test is about event-loop thread
            // lifetime, not about protocol negotiation, so it opts out rather
            // than teaching the stub to speak v2.
            String url = "jdbc:particledb://127.0.0.1:" + server.port()
                    + "/test?preferUnixSocket=false&connectTimeout=5000"
                    + "&requireMux=false";

            EventLoopGroup shared = SharedEventLoop.group();
            assertNotNull(shared, "shared event-loop group must exist");

            // NioEventLoopGroup starts each of its worker threads lazily, the
            // first time a channel is assigned to that worker (round-robin). So
            // the live thread count climbs as more connections touch more
            // workers - but it is CAPPED at the shared group's fixed thread
            // count, no matter how many connections we open. The pre-fix code
            // would instead spawn one NioEventLoopGroup (one thread) PER
            // connection, so the count would track N.
            //
            // Warm up by opening several connections so every shared worker
            // thread is started, then capture the plateau.
            final int WARMUP = 32;
            for (int i = 0; i < WARMUP; i++) {
                PdbConnection w = new PdbConnection(org.particledb.Pgcompat.parse(url, null));
                assertSame(shared, w.mux().eventLoopGroup(),
                        "PdbConnection must use the shared event-loop group");
                assertFalse(w.mux().ownsEventLoopGroup(),
                        "PdbConnection's mux must NOT own the event-loop group");
                w.close();
            }
            int plateau = eventLoopThreadCount();

            final int N = 300; // far more than the OS-thread budget the leak ate
            for (int i = 0; i < N; i++) {
                PdbConnection c = new PdbConnection(org.particledb.Pgcompat.parse(url, null));
                // Same shared group every time - no per-connection group spawned.
                assertSame(shared, c.mux().eventLoopGroup(),
                        "connection " + i + " spawned a non-shared event-loop group");
                assertFalse(c.mux().ownsEventLoopGroup());
                c.close();
                // close() is idempotent.
                c.close();
            }

            // Give any lingering close() listeners a moment to settle.
            Thread.sleep(200);

            int after = eventLoopThreadCount();
            // After warm-up the shared group's threads are all started, so 300
            // more open/close cycles must add (essentially) ZERO new threads.
            // Tiny slack for JIT/finalizer noise. The OLD code would add ~N=300.
            assertTrue(after <= plateau + 2,
                    "event-loop threads leaked: warmed-up plateau=" + plateau
                            + " after " + N + " more open/close cycles=" + after
                            + " (per-connection group leak is back)");
            // Sanity: the plateau itself is bounded by the shared group size,
            // NOT by the number of connections we opened.
            assertTrue(plateau <= 8 + 2,
                    "shared group thread count unexpectedly large: " + plateau);

            // The shared group survives connection closes.
            assertFalse(shared.isShuttingDown(),
                    "shared group must NOT be shut down by PdbConnection.close()");
            assertFalse(shared.isShutdown(),
                    "shared group must NOT be terminated by PdbConnection.close()");
        }
    }

    @Test
    void sharedGroup_isSingleton() {
        assertSame(SharedEventLoop.group(), SharedEventLoop.group(),
                "SharedEventLoop.group() must return the same instance");
    }

    @Test
    void streamMuxClose_onSharedGroup_leavesGroupAlive() throws Exception {
        EventLoopGroup shared = SharedEventLoop.group();
        // A mux constructed with a borrowed group must never shut it down.
        StreamMux mux = new StreamMux(shared, false, 1000);
        assertFalse(mux.ownsEventLoopGroup());
        mux.close(); // no channel connected — must be a clean no-op for the group
        assertFalse(shared.isShutdown(),
                "close() on a borrowed group must not terminate it");
    }

    // ── Minimal in-process server that just completes the v1 HELLO handshake ──

    /**
     * Reads the client's v1 HELLO frame and replies with a v1-framed HELLO_OK
     * advertising caps=0x01 (v1 only) so no Arrow decoding is needed. Enough for
     * {@code StreamMux.connect(...)} to complete its {@code helloOkFuture}.
     */
    private static final class FakeHelloServer implements AutoCloseable {
        private final ServerSocket ss;
        private final Thread acceptor;
        private final AtomicBoolean running = new AtomicBoolean(true);

        private FakeHelloServer(ServerSocket ss) {
            this.ss = ss;
            this.acceptor = new Thread(this::acceptLoop, "fake-hello-server");
            this.acceptor.setDaemon(true);
            this.acceptor.start();
        }

        static FakeHelloServer start() throws IOException {
            ServerSocket ss = new ServerSocket(0, 128, java.net.InetAddress.getByName("127.0.0.1"));
            return new FakeHelloServer(ss);
        }

        int port() { return ss.getLocalPort(); }

        private void acceptLoop() {
            while (running.get()) {
                try {
                    Socket s = ss.accept();
                    Thread t = new Thread(() -> handle(s), "fake-hello-conn");
                    t.setDaemon(true);
                    t.start();
                } catch (IOException e) {
                    if (running.get()) {
                        // transient; keep going
                    } else {
                        return;
                    }
                }
            }
        }

        private void handle(Socket s) {
            try (Socket sock = s) {
                sock.setTcpNoDelay(true);
                InputStream in = sock.getInputStream();
                OutputStream out = sock.getOutputStream();

                // Read the client HELLO (v1 framing): [u32_le len][u16 stream][u8 type][u8 flags][payload]
                byte[] hdr = readN(in, 8);
                if (hdr == null) return;
                int len = (hdr[0] & 0xFF) | ((hdr[1] & 0xFF) << 8)
                        | ((hdr[2] & 0xFF) << 16) | ((hdr[3] & 0xFF) << 24);
                if (len > 0) readN(in, len); // discard HELLO payload

                // Reply HELLO_OK (v1 framing), payload:
                //   [u8 server_ver=2][u8 chosen_caps=0x01 (v1 only)][u8 chosen_format=ARROW_IPC]
                //   [u32_le max_streams=1][u32_le init_window=0]
                byte[] payload = new byte[]{
                        0x02,        // server proto version
                        0x01,        // chosen caps: v1 legacy only (no v2 mux → no Arrow needed)
                        0x01,        // chosen format (FMT_ARROW_IPC) — unused in v1 path here
                        1, 0, 0, 0,  // max_concurrent_streams = 1 (LE)
                        0, 0, 0, 0   // initial_window_size = 0 (LE)
                };
                byte MSG_HELLO_OK = (byte) 0x80;
                byte[] frame = new byte[8 + payload.length];
                int pl = payload.length;
                frame[0] = (byte) (pl & 0xFF);
                frame[1] = (byte) ((pl >> 8) & 0xFF);
                frame[2] = (byte) ((pl >> 16) & 0xFF);
                frame[3] = (byte) ((pl >> 24) & 0xFF);
                frame[4] = 0; frame[5] = 0;      // stream id = 0
                frame[6] = MSG_HELLO_OK;          // msg type
                frame[7] = 0;                     // flags
                System.arraycopy(payload, 0, frame, 8, pl);
                out.write(frame);
                out.flush();

                // Keep the socket open until the client disconnects: answer
                // the session TimeZone it sets at connect (a v1 QUERY) with
                // OK, and drain everything else (GOAWAY/CLOSE on close()).
                while (running.get()) {
                    byte[] h = readN(in, 8);
                    if (h == null) break;
                    int n = (h[0] & 0xFF) | ((h[1] & 0xFF) << 8)
                            | ((h[2] & 0xFF) << 16) | ((h[3] & 0xFF) << 24);
                    if (n > 0 && readN(in, n) == null) break;
                    if (h[6] == 0x01) { // MSG_QUERY -> MSG_OK [u64 affected = 0]
                        byte[] ok = new byte[8 + 8];
                        ok[0] = 8;
                        ok[4] = h[4]; ok[5] = h[5];   // same stream id
                        ok[6] = (byte) 0x82;          // MSG_OK
                        out.write(ok);
                        out.flush();
                    }
                }
            } catch (IOException ignored) {
                // client closed - fine
            }
        }

        private static byte[] readN(InputStream in, int n) throws IOException {
            byte[] b = new byte[n];
            int off = 0;
            while (off < n) {
                int r = in.read(b, off, n - off);
                if (r < 0) return null;
                off += r;
            }
            return b;
        }

        @Override
        public void close() {
            running.set(false);
            try { ss.close(); } catch (IOException ignored) {}
        }
    }
}
