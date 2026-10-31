package org.particledb;

import org.particledb.wire.WireV2Codec;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Test helper: a TCP proxy in front of a live native-wire server that clears
 * capability bits in the server's HELLO_OK before the driver reads it, so the
 * driver negotiates as it would against an older server -- without one.
 * Every other byte is forwarded unchanged, both ways.
 *
 * <p>HELLO_OK is v1-framed ({@code [u32 len][u16 stream][u8 type][u8 flags]}),
 * payload {@code [u8 ver][u8 chosen_caps][u8 fmt][u32 streams][u32 window]
 * [u8 num_ext][ext0...]} ({@link WireV2Codec#parseHelloOk}).
 */
final class HelloCapStrippingProxy implements AutoCloseable {

    final int port;
    private final ServerSocket listener;
    private final List<Socket> sockets = new CopyOnWriteArrayList<>();
    /** HELLO_OK frames rewritten (proof the proxy saw the handshake it claims to strip). */
    volatile int rewrittenHellos;

    /**
     * @param targetPort the real server's native-wire port (127.0.0.1)
     * @param clearCaps  bits cleared from {@code chosen_caps} (e.g. {@link WireV2Codec#CAP_PREPARED})
     * @param clearExt   bits cleared from ext byte 0 (e.g. {@link WireV2Codec#CAP_EXT_ARRAY_PARAMS})
     */
    HelloCapStrippingProxy(int targetPort, byte clearCaps, byte clearExt) throws IOException {
        listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        port = listener.getLocalPort();
        Thread t = new Thread(() -> {
            while (!listener.isClosed()) {
                try {
                    Socket client = listener.accept();
                    Socket server = new Socket(InetAddress.getLoopbackAddress(), targetPort);
                    sockets.add(client);
                    sockets.add(server);
                    Socket[] pair = {client, server};
                    pump(client.getInputStream(), server.getOutputStream(), null, pair);
                    pump(server.getInputStream(), client.getOutputStream(), new byte[] {clearCaps, clearExt}, pair);
                } catch (IOException e) {
                    return;
                }
            }
        }, "hello-cap-proxy-accept");
        t.setDaemon(true);
        t.start();
    }

    private void pump(InputStream in, OutputStream out, byte[] strip, Socket[] pair) {
        Thread t = new Thread(() -> {
            try {
                if (strip != null) {
                    DataInputStream d = new DataInputStream(in);
                    byte[] hdr = new byte[8];
                    d.readFully(hdr);
                    int len = (hdr[0] & 0xFF) | (hdr[1] & 0xFF) << 8 | (hdr[2] & 0xFF) << 16 | (hdr[3] & 0xFF) << 24;
                    byte[] payload = new byte[len];
                    d.readFully(payload);
                    if (hdr[6] == WireV2Codec.MSG_HELLO_OK) {
                        if (payload.length > 1) payload[1] &= (byte) ~strip[0];
                        if (payload.length >= 13 && (payload[11] & 0xFF) >= 1) payload[12] &= (byte) ~strip[1];
                        rewrittenHellos++;
                    }
                    out.write(hdr);
                    out.write(payload);
                    out.flush();
                }
                byte[] buf = new byte[64 * 1024];
                for (int n; (n = in.read(buf)) >= 0; ) {
                    out.write(buf, 0, n);
                    out.flush();
                }
            } catch (IOException ignored) {
                // a side closed
            } finally {
                // One side closed: close the pair, so the other pump ends too.
                for (Socket s : pair) {
                    try { s.close(); } catch (IOException ignored) { }
                }
            }
        }, "hello-cap-proxy-pump");
        t.setDaemon(true);
        t.start();
    }

    @Override
    public void close() throws IOException {
        listener.close();
        for (Socket s : sockets) {
            try { s.close(); } catch (IOException ignored) { }
        }
    }
}
