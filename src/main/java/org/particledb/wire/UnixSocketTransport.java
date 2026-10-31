package org.particledb.wire;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ConnectException;
import java.net.ProtocolFamily;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.StandardProtocolFamily;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * {@link WireTransport} over the server's Unix-domain socket
 * ({@code <unix_socket_dir>/.s.PDB.<port>}, see {@code PdbWireServer::start}).
 *
 * <p><b>Java 16+ only, reached by reflection.</b> The driver is compiled for
 * Java 11 ({@code maven.compiler.release}); {@code UnixDomainSocketAddress} and
 * {@code StandardProtocolFamily.UNIX} arrived in Java 16. {@link #AVAILABLE}
 * is false on an older JVM and the caller uses TCP.
 *
 * <p><b>Timed reads without SO_TIMEOUT.</b> A Unix {@link SocketChannel} has no
 * socket adaptor, so it cannot honour {@code setSoTimeout}. The channel runs
 * non-blocking and a read that finds no data waits on a per-connection
 * {@link Selector} for at most the read timeout, then throws
 * {@link SocketTimeoutException} -- the same contract the mux's frame reader
 * gets from a TCP socket. Writes that hit a full socket buffer wait on a
 * second selector (created on first need), so the reading and the writing
 * thread never share one selector.
 *
 * <p><b>Thread interrupts.</b> An interrupt closes an
 * {@code InterruptibleChannel} only during a BLOCKING operation; this channel
 * is non-blocking, so an interrupted caller cannot close it (checked by
 * {@code UnixSocketTransportTest.interruptedCallerDoesNotKillTheConnection}).
 * But a pending interrupt makes {@link Selector#select} return at once, so a
 * wait loop would spin until data arrived. Each read/write therefore clears
 * the caller's interrupt status while it waits and restores it before
 * returning; the mux's own {@code drive} loop is what reacts to interrupts.
 */
final class UnixSocketTransport implements WireTransport {

    /** True when this JVM supports Unix-domain {@link SocketChannel}s (Java 16+). */
    static final boolean AVAILABLE;
    private static final Method ADDRESS_OF;   // UnixDomainSocketAddress.of(Path)
    private static final Method OPEN_FAMILY;  // SocketChannel.open(ProtocolFamily)
    private static final ProtocolFamily UNIX;

    static {
        Method of = null;
        Method open = null;
        ProtocolFamily unix = null;
        try {
            Class<?> addr = Class.forName("java.net.UnixDomainSocketAddress");
            of = addr.getMethod("of", Path.class);
            open = SocketChannel.class.getMethod("open", ProtocolFamily.class);
            unix = StandardProtocolFamily.valueOf("UNIX");
        } catch (ReflectiveOperationException | IllegalArgumentException | LinkageError e) {
            of = null;
            open = null;
            unix = null;
        }
        ADDRESS_OF = of;
        OPEN_FAMILY = open;
        UNIX = unix;
        AVAILABLE = of != null && open != null && unix != null;
    }

    /**
     * Connecting found no server behind the path: the file does not exist, or
     * it exists and nobody accepts on it (ECONNREFUSED -- the socket file a
     * crashed server left behind). The only outcome a caller may answer by
     * trying TCP; every other failure is reported.
     */
    static final class NoListenerException extends IOException {
        NoListenerException(String msg, Throwable cause) { super(msg, cause); }
    }

    private final SocketChannel ch;
    private final Selector readSelector;
    private volatile Selector writeSelector;
    private final Object writeSelectorInit = new Object();
    private final Path path;
    private volatile int readTimeoutMillis;
    private final InputStream in = new In();
    private final OutputStream out = new Out();

    private UnixSocketTransport(SocketChannel ch, Selector readSelector, Path path) {
        this.ch = ch;
        this.readSelector = readSelector;
        this.path = path;
    }

    /**
     * Connect to the Unix socket at {@code path}.
     *
     * @throws NoListenerException no server there (absent file / refused)
     * @throws IOException any other failure (permission denied, timeout, ...)
     */
    static UnixSocketTransport connect(Path path, int connectTimeoutMillis) throws IOException {
        if (!AVAILABLE) {
            throw new IOException("Unix-domain sockets need Java 16+; this JVM is "
                    + System.getProperty("java.version"));
        }
        if (!Files.exists(path)) {
            throw new NoListenerException("no Unix socket at " + path, null);
        }
        SocketChannel ch;
        SocketAddress addr;
        try {
            addr = (SocketAddress) ADDRESS_OF.invoke(null, path);
            ch = (SocketChannel) OPEN_FAMILY.invoke(null, UNIX);
        } catch (InvocationTargetException e) {
            Throwable c = e.getCause();
            if (c instanceof IOException) throw (IOException) c;
            throw new IOException("cannot open a Unix socket for " + path, c);
        } catch (IllegalAccessException e) {
            throw new IOException("cannot open a Unix socket for " + path, e);
        }
        Selector sel = null;
        boolean ok = false;
        boolean interrupted = Thread.interrupted();
        try {
            ch.configureBlocking(false);
            sel = Selector.open();
            boolean connected;
            try {
                connected = ch.connect(addr);
                if (!connected) {
                    SelectionKey k = ch.register(sel, SelectionKey.OP_CONNECT);
                    long deadline = System.nanoTime()
                            + TimeUnit.MILLISECONDS.toNanos(Math.max(1, connectTimeoutMillis));
                    while (!ch.finishConnect()) {
                        long rem = deadline - System.nanoTime();
                        if (rem <= 0) {
                            throw new SocketTimeoutException("connect to " + path + " timed out after "
                                    + connectTimeoutMillis + " ms");
                        }
                        sel.select(Math.max(1, TimeUnit.NANOSECONDS.toMillis(rem)));
                        sel.selectedKeys().clear();
                        if (Thread.interrupted()) interrupted = true;
                    }
                    k.interestOps(SelectionKey.OP_READ);
                } else {
                    ch.register(sel, SelectionKey.OP_READ);
                }
            } catch (ConnectException refused) {
                // ECONNREFUSED: a socket file with no listener (stale). Or the
                // file vanished between the check above and connect().
                throw new NoListenerException("no server accepting on " + path
                        + " (" + refused.getMessage() + ")", refused);
            } catch (SocketException se) {
                if (!Files.exists(path)) {
                    throw new NoListenerException("Unix socket " + path + " disappeared", se);
                }
                throw se;
            }
            ok = true;
            return new UnixSocketTransport(ch, sel, path);
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
            if (!ok) {
                try { ch.close(); } catch (IOException ignored) { /* best-effort */ }
                if (sel != null) {
                    try { sel.close(); } catch (IOException ignored) { /* best-effort */ }
                }
            }
        }
    }

    @Override public InputStream in() { return in; }
    @Override public OutputStream out() { return out; }
    @Override public void setReadTimeout(int millis) { readTimeoutMillis = Math.max(0, millis); }
    @Override public boolean isClosed() { return !ch.isOpen(); }
    @Override public boolean isUnixSocket() { return true; }
    @Override public String describe() { return "unix:" + path; }

    @Override
    public void close() throws IOException {
        // Selector.close() wakes a thread blocked in select() on it.
        IOException first = null;
        try { ch.close(); } catch (IOException e) { first = e; }
        try { readSelector.close(); } catch (IOException e) { if (first == null) first = e; }
        Selector ws = writeSelector;
        if (ws != null) {
            try { ws.close(); } catch (IOException e) { if (first == null) first = e; }
        }
        if (first != null) throw first;
    }

    private Selector writeSelector() throws IOException {
        Selector ws = writeSelector;
        if (ws != null) return ws;
        synchronized (writeSelectorInit) {
            ws = writeSelector;
            if (ws == null) {
                ws = Selector.open();
                ch.register(ws, SelectionKey.OP_WRITE);
                writeSelector = ws;
                if (!ch.isOpen()) {
                    ws.close();
                    throw new SocketException("Socket closed");
                }
            }
            return ws;
        }
    }

    private int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) return 0;
        ByteBuffer bb = ByteBuffer.wrap(b, off, len);
        int timeout = readTimeoutMillis;
        long deadline = timeout > 0 ? System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout) : 0L;
        boolean interrupted = Thread.interrupted();
        try {
            while (true) {
                int n = ch.read(bb);
                if (n != 0) return n; // bytes, or -1 at EOF
                long waitMillis = 0L; // 0 = block in select until ready
                if (timeout > 0) {
                    long rem = deadline - System.nanoTime();
                    if (rem <= 0) throw new SocketTimeoutException("Read timed out");
                    waitMillis = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(rem));
                }
                readSelector.select(waitMillis);
                readSelector.selectedKeys().clear();
                if (Thread.interrupted()) interrupted = true;
                if (!ch.isOpen()) throw new SocketException("Socket closed");
            }
        } catch (ClosedSelectorException e) {
            throw new SocketException("Socket closed");
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    private void write(byte[] b, int off, int len) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(b, off, len);
        boolean interrupted = Thread.interrupted();
        try {
            while (bb.hasRemaining()) {
                if (ch.write(bb) == 0) {
                    Selector ws = writeSelector();
                    ws.select();
                    ws.selectedKeys().clear();
                    if (Thread.interrupted()) interrupted = true;
                    if (!ch.isOpen()) throw new SocketException("Socket closed");
                }
            }
        } catch (ClosedSelectorException e) {
            throw new SocketException("Socket closed");
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    private final class In extends InputStream {
        @Override public int read() throws IOException {
            byte[] one = new byte[1];
            int n = UnixSocketTransport.this.read(one, 0, 1);
            return n <= 0 ? -1 : (one[0] & 0xFF);
        }
        @Override public int read(byte[] b, int off, int len) throws IOException {
            java.util.Objects.checkFromIndexSize(off, len, b.length);
            return UnixSocketTransport.this.read(b, off, len);
        }
        @Override public void close() throws IOException { UnixSocketTransport.this.close(); }
    }

    private final class Out extends OutputStream {
        @Override public void write(int b) throws IOException {
            UnixSocketTransport.this.write(new byte[] { (byte) b }, 0, 1);
        }
        @Override public void write(byte[] b, int off, int len) throws IOException {
            java.util.Objects.checkFromIndexSize(off, len, b.length);
            UnixSocketTransport.this.write(b, off, len);
        }
        @Override public void close() throws IOException { UnixSocketTransport.this.close(); }
    }
}
