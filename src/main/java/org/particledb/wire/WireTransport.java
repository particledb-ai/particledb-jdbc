package org.particledb.wire;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * The byte pipe a {@link StreamMux} runs the native wire over: a TCP socket
 * ({@link TcpTransport}) or the server's Unix-domain socket
 * ({@link UnixSocketTransport}).
 *
 * <p>Contract the mux relies on, identical for both:
 * <ul>
 *   <li>{@link #in()} {@code read} honours {@link #setReadTimeout}: with a
 *       timeout {@code > 0} it throws {@link java.net.SocketTimeoutException}
 *       when nothing arrives in time, and buffered bytes are never lost;
 *       {@code 0} blocks until data, EOF or close.</li>
 *   <li>{@link #out()} {@code write} writes every byte or throws.</li>
 *   <li>One thread reads while one other thread writes (the mux's read role
 *       and write lock); {@link #close} from any thread wakes both.</li>
 * </ul>
 */
interface WireTransport extends Closeable {
    InputStream in();

    OutputStream out();

    /** Read timeout in milliseconds; {@code 0} = block indefinitely. */
    void setReadTimeout(int millis) throws IOException;

    boolean isClosed();

    /** {@code true} for the Unix-domain transport. */
    boolean isUnixSocket();

    /** Human-readable peer, e.g. {@code tcp:localhost:7173} or {@code unix:/tmp/.s.PDB.7173}. */
    String describe();

    /** Close immediately, without a protocol or TLS shutdown exchange. */
    default void abort() throws IOException { close(); }

    @Override
    void close() throws IOException;
}
