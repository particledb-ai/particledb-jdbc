package org.particledb.wire;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Multiplexed PDB-wire connection: one socket, N concurrent streams.
 *
 * <h2>Transport</h2>
 *
 * <p>The socket is the server's Unix-domain socket
 * ({@code <unixSocketDir>/.s.PDB.<port>}) when {@code preferUnixSocket} (the
 * default) and the host is loopback, else TCP -- see {@link #openTransport}.
 * Both sit behind {@link WireTransport}; everything above it is identical.
 * Through 1.0.8 {@code preferUnixSocket} was stored here and never read: every
 * connection used TCP.
 *
 * <h2>I/O model (2026-09-25): caller-driven, no I/O thread</h2>
 *
 * <p>Requests are written by the calling thread, under a write lock, straight
 * to the socket. Responses are read by a calling thread too: a thread blocked
 * on a stream's future ({@link StreamState.DrivenFuture#get}) takes the
 * connection's <em>read role</em> when it is free and reads frames until its
 * own stream completes, completing (and waking) the waiters of any other
 * streams whose frames it reads on the way. When it returns it hands the role
 * to another waiter. With one request in flight per socket -- the JDBC
 * contract for a {@code Connection} -- the caller writes its request and reads
 * its own response, exactly like a synchronous driver: no thread hop in either
 * direction.
 *
 * <p>This replaced a Netty event loop. There, every request was a task handed
 * to the event-loop thread (a cross-thread wakeup) and every response a future
 * completed on it (a second wakeup to unpark the caller). Measured on the
 * native wire against pgjdbc on the same server, those two wakeups were most
 * of the per-statement overhead.
 *
 * <p>The {@link EventLoopGroup} constructor parameter is retained for source
 * and binary compatibility and reported by {@link #eventLoopGroup()}; no I/O
 * runs on it, and this class never shuts it down.
 *
 * <p>If the server returns {@code chosen_caps & 0x02 == 0} during HELLO_OK,
 * the mux degrades to v1 single-stream behaviour (only with
 * {@code requireMux=false}): at most one stream is open at a time and stream
 * IDs use the v1 u16 wrap-around.
 */
public final class StreamMux implements AutoCloseable, StreamState.Driver {

    /** Allocator: client-initiated streams use odd ids per spec. */
    private final AtomicLong nextStreamId = new AtomicLong(1);

    private final ConcurrentHashMap<Long, StreamState> streams = new ConcurrentHashMap<>();

    private final EventLoopGroup group;

    private static final Logger LOGGER = Logger.getLogger("org.particledb.jdbc");

    /** TCP or Unix-domain; null until {@link #connect} opens it. */
    private volatile WireTransport transport;
    private InputStream in;
    private OutputStream out;
    private volatile int networkTimeoutMillis;
    private static final java.util.concurrent.ScheduledThreadPoolExecutor DEADLINES = deadlines();
    private static java.util.concurrent.ScheduledThreadPoolExecutor deadlines() {
        var scheduler = new java.util.concurrent.ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "pdb-network-deadlines");
            thread.setDaemon(true);
            return thread;
        });
        scheduler.setRemoveOnCancelPolicy(true);
        return scheduler;
    }

    public void setNetworkTimeout(int millis) { networkTimeoutMillis = millis; }

    /** Close the transport without waiting for a peer to read a final frame. */
    public void abort() {
        closed = true;
        WireTransport current = transport;
        if (current != null) {
            try { current.abort(); } catch (IOException ignored) { /* already closed */ }
        }
        failAll(new StreamState.PdbWireException(0, "connection closed: network operation aborted"));
        ByteBuf leftover;
        while ((leftover = pendingWrites.poll()) != null) leftover.release();
    }

    private final ReentrantLock writeLock = new ReentrantLock();
    /** The read role: held by at most one thread, which alone reads the socket. */
    private final AtomicBoolean reading = new AtomicBoolean(false);
    private volatile boolean closed = false;
    private final FrameReader frames = new FrameReader();

    // Negotiated parameters
    private volatile boolean v2Active = false;
    private volatile long maxConcurrentStreams = 1;
    /**
     * One permit per stream the server will accept at once (its advertised
     * {@code max_concurrent_streams}); null = no server cap known. Held from
     * {@link #newStream} until the stream's terminal frame (or connection
     * failure). With the Netty event loop, requests trickled out one at a
     * time and a 64-thread burst on one connection rarely had more than the
     * cap open on the server at once; written directly by 64 callers they
     * arrive together, and the server resets everything past its cap.
     */
    private volatile java.util.concurrent.Semaphore serverSlots;
    /** Longest a caller waits for a server stream slot. */
    private static final long SLOT_WAIT_SECONDS = 60;
    private volatile byte chosenFormat = WireV2Codec.FMT_ARROW_IPC;
    /** The negotiated HELLO_OK; null until the handshake completes. */
    private volatile WireV2Codec.HelloOk helloOk;

    private final boolean preferUnixSocket;
    private final int connectTimeoutMillis;
    /** Directory holding the server's {@code .s.PDB.<port>}; the server's default is /tmp. */
    private final String unixSocketDir;
    /** Explicit socket path; when set it is used and MUST work (no TCP fallback). */
    private final String unixSocketPath;
    /** The server's default {@code unix_socket_dir} ({@code UnixSocketPolicy}). */
    public static final String DEFAULT_UNIX_SOCKET_DIR = "/tmp";

    /**
     * Default cap on CONCURRENTLY OPEN client streams for one socket. Matches
     * the value the driver has always ADVERTISED via
     * {@code Driver.getPropertyInfo} and {@code PdbDataSource}.
     */
    public static final int DEFAULT_MAX_STREAMS = 64;

    /**
     * Cap on concurrently open client streams; {@code 0} means no limit, the
     * same convention the server uses for {@code max_concurrent_streams}.
     *
     * <p>HISTORY, because the previous state was worse than a wrong value:
     * {@code maxStreamsPerConn} was advertised through
     * {@code Driver.getPropertyInfo} (default "64"), settable via
     * {@code PdbDataSource.setMaxStreamsPerConn}, and documented in the URL
     * grammar -- and NOTHING read it. Setting it did nothing and reported
     * nothing. It is now enforced here, at the single point where a stream
     * enters the live table.
     */
    private final int maxStreams;

    /**
     * Whether a connection must negotiate the v2 mux, or may fall back to v1.
     *
     * DEFAULT IS REQUIRE. The native wire is the path first-party software
     * uses, and on it every connection is meant to be multiplexed -- so a
     * server that does not confirm v2 is a misconfiguration to report, not a
     * condition to paper over. Before this, {@code v2Active} simply stayed
     * false and every send site quietly used v1 framing, so an operator could
     * believe they had multiplexing while getting none. That is the same shape
     * as a setting that is accepted and ignored.
     *
     * Set {@code requireMux=false} in the URL to talk to an older server that
     * genuinely only speaks v1. That is an explicit downgrade, chosen and
     * visible, rather than a silent one.
     */
    private final boolean requireMux;

    /**
     * Back-compatible 3-arg form: applies {@link #DEFAULT_MAX_STREAMS}. Kept
     * because this constructor is called from {@code PdbConnection} and is
     * reachable by anyone who linked against an earlier driver build.
     */
    public StreamMux(EventLoopGroup group, boolean preferUnixSocket,
                     int connectTimeoutMillis) {
        this(group, preferUnixSocket, connectTimeoutMillis, DEFAULT_MAX_STREAMS);
    }

    public StreamMux(EventLoopGroup group, boolean preferUnixSocket,
                     int connectTimeoutMillis, int maxStreams) {
        this(group, preferUnixSocket, connectTimeoutMillis, maxStreams, true);
    }

    public StreamMux(EventLoopGroup group, boolean preferUnixSocket,
                     int connectTimeoutMillis, int maxStreams,
                     boolean requireMux) {
        this(group, preferUnixSocket, connectTimeoutMillis, maxStreams, requireMux, null, null);
    }

    /**
     * Full form. {@code unixSocketDir} (null = {@link #DEFAULT_UNIX_SOCKET_DIR})
     * is where the server's {@code .s.PDB.<port>} is looked for when
     * {@code preferUnixSocket} and the host is loopback; {@code unixSocketPath}
     * names the socket outright -- see {@link #connect(String, int, String)}.
     */
    public StreamMux(EventLoopGroup group, boolean preferUnixSocket,
                     int connectTimeoutMillis, int maxStreams,
                     boolean requireMux, String unixSocketDir, String unixSocketPath) {
        this.unixSocketDir = (unixSocketDir == null || unixSocketDir.isEmpty())
                ? DEFAULT_UNIX_SOCKET_DIR : unixSocketDir;
        this.unixSocketPath = (unixSocketPath == null || unixSocketPath.isEmpty())
                ? null : unixSocketPath;
        this.requireMux = requireMux;
        if (maxStreams < 0) {
            throw new IllegalArgumentException(
                    "maxStreamsPerConn must be >= 0 (0 = unlimited), got " + maxStreams);
        }
        this.maxStreams = maxStreams;
        this.group = group;
        this.preferUnixSocket = preferUnixSocket;
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    /**
     * Connect to (host, port) and run the HELLO handshake on the calling
     * thread. Returns a future that is already complete: with the HELLO_OK, or
     * exceptionally with the connect/handshake failure.
     */
    public CompletableFuture<WireV2Codec.HelloOk> connect(String host, int port) {
        return connect(host, port, null);
    }

    private TlsOptions tlsOptions = TlsOptions.DISABLED;

    public void configureTls(TlsOptions options) {
        if (transport != null) throw new IllegalStateException("connection already opened");
        tlsOptions = java.util.Objects.requireNonNull(options, "options");
    }

    /**
     * {@link #connect(String, int)} sending {@code sessionTimeZone} (when non-null)
     * in the HELLO's session-parameter block
     * ({@link WireV2Codec#CAP_EXT_SESSION_PARAMS}): the session STARTS in that
     * zone, as a pgjdbc session starts in the zone its startup packet names.
     * {@link WireV2Codec.HelloOk#appliedSessionParams()} says whether the server
     * applied it. A server that refuses the value answers an error frame in
     * place of HELLO_OK; the future then fails with that
     * {@link StreamState.PdbWireException} ({@link WireV2Codec#ERR_STARTUP_PARAMETER}).
     */
    public CompletableFuture<WireV2Codec.HelloOk> connect(String host, int port, String sessionTimeZone) {
        CompletableFuture<WireV2Codec.HelloOk> f = new CompletableFuture<>();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(connectTimeoutMillis);
        try {
            WireTransport t = openTransport(host, port);
            transport = t;
            frames.timeoutTarget = t;
            in = t.in();
            out = t.out();
            // Every capability the driver implements is asked for by DEFAULT:
            // the native wire's most efficient and most faithful mode is the
            // mode a plain connect gets (the conformance suite's DEF-002 holds
            // the server's record of this to the list).
            byte ext = (byte) (WireV2Codec.CAP_EXT_TYPED_PARAMS | WireV2Codec.CAP_EXT_BULK_INGEST
                    | WireV2Codec.CAP_EXT_ARRAY_PARAMS | WireV2Codec.CAP_EXT_BATCH_SQLSTATE);
            String[][] sessionParams = null;
            if (sessionTimeZone != null) {
                ext |= WireV2Codec.CAP_EXT_SESSION_PARAMS;
                sessionParams = new String[][] { { "TimeZone", sessionTimeZone } };
            }
            writeFrame(WireV2Codec.encodeHelloV1(WireV2Codec.PROTOCOL_V2,
                    new byte[] { (byte) (WireV2Codec.CAP_V1_LEGACY | WireV2Codec.CAP_V2_MUX
                            | WireV2Codec.CAP_PREPARED | WireV2Codec.CAP_NUMERIC_TEXT
                            | (tlsOptions.isEnabled() ? WireV2Codec.CAP_TLS_NEXT_PROTO : 0)
                            | WireV2Codec.CAP_STATEMENT_NOTICE) },
                    new byte[] { WireV2Codec.FMT_ARROW_IPC },
                    new byte[] { ext },
                    sessionParams));
            // The HELLO_OK is v1-framed. Bound the wait by the connect timeout.
            WireV2Codec.WireFrame hf = frames.next(in, false,
                    deadline);
            if (hf.msgType == WireV2Codec.MSG_ERROR && hf.payload.length >= 2) {
                // The server refused the handshake (a session parameter it
                // cannot apply, TLS required, …): its own code and text.
                int code = (hf.payload[0] & 0xFF) | ((hf.payload[1] & 0xFF) << 8);
                String text = new String(hf.payload, 2, hf.payload.length - 2,
                        java.nio.charset.StandardCharsets.UTF_8);
                throw StreamState.PdbWireException.fromServerFrame(code, text);
            }
            if (hf.msgType != WireV2Codec.MSG_HELLO_OK) {
                throw new IOException("PDB native wire: expected HELLO_OK, got 0x"
                        + Integer.toHexString(hf.msgType & 0xFF));
            }
            WireV2Codec.HelloOk hi = WireV2Codec.parseHelloOk(hf.payload);
            if (tlsOptions.isEnabled()) {
                if (!hi.isTlsAlpn()) {
                    throw new javax.net.ssl.SSLHandshakeException("server did not accept TLS; refusing plaintext");
                }
                long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                if (remaining <= 0) throw new TimeoutException("TLS connect deadline exceeded");
                TcpTransport secured = ((TcpTransport) t).secure(host, port,
                        (int) Math.min(Integer.MAX_VALUE, Math.max(1, remaining)), tlsOptions);
                transport = secured;
                frames.timeoutTarget = secured;
                in = secured.in();
                out = secured.out();
            }
            v2Active = hi.isV2();
            chosenFormat = hi.chosenFormat;
            maxConcurrentStreams = Math.max(1, hi.maxConcurrentStreams);
            // The server REFUSES a stream past the cap it advertised
            // (RST_STREAM, ERR_STREAM_LIMIT). Callers sharing this connection
            // wait for a slot instead of racing into that refusal. 0 = no limit.
            if (v2Active && hi.maxConcurrentStreams > 0) {
                serverSlots = new java.util.concurrent.Semaphore(
                        (int) Math.min(hi.maxConcurrentStreams, Integer.MAX_VALUE));
            }
            helloOk = hi;
            if (!v2Active && requireMux) {
                // FAIL CLOSED. Falling through here would leave v2Active false,
                // and every send site below branches on it, so the connection
                // would work -- unmultiplexed, silently. The caller asked for the
                // native wire; give it an error it can act on instead of a
                // downgrade it cannot see.
                closeSocketQuietly();
                f.completeExceptionally(new IOException(
                        "PDB native wire: server did not negotiate the v2"
                        + " multiplexed protocol (CAP_V2_MUX). This driver"
                        + " requires it. If the server genuinely only"
                        + " speaks v1, pass requireMux=false in the JDBC"
                        + " URL to accept an UNMULTIPLEXED connection."));
                return f;
            }
            f.complete(hi);
        } catch (TimeoutException te) {
            closeSocketQuietly();
            f.completeExceptionally(new StreamState.PdbWireException(0,
                    "connection closed before HELLO_OK (timed out)"));
        } catch (Exception e) {
            closeSocketQuietly();
            f.completeExceptionally(e);
        }
        return f;
    }

    /**
     * Open the byte pipe for {@code (host, port)}.
     *
     * <p>Unix socket first when {@code preferUnixSocket} and either
     * {@code unixSocketPath} is set, or {@code host} is a loopback NAME OR
     * LITERAL ({@code localhost}, {@code 127.0.0.1}, {@code ::1}; no DNS
     * lookup). The derived path is {@code <unixSocketDir>/.s.PDB.<port>}, the
     * name {@code PdbWireServer::start} binds.
     *
     * <p>Fallback to TCP happens ONLY when there is no server behind the
     * derived path (file absent, or a stale file refusing connections), or the
     * JVM predates Unix-domain channels (Java &lt; 16); it is logged at FINE.
     * Any other connect failure (permission denied, timeout) is thrown. An
     * explicit {@code unixSocketPath} never falls back. Errors after the pipe
     * is open (handshake, authentication, protocol) are never retried on TCP:
     * this method has returned by then.
     */
    private WireTransport openTransport(String host, int port) throws IOException {
        if (tlsOptions.isEnabled() && unixSocketPath != null) {
            throw new IOException("TLS requires TCP; remove unixSocketPath");
        }
        if (preferUnixSocket && !tlsOptions.isEnabled()) {
            boolean explicit = unixSocketPath != null;
            if (explicit || isLoopbackHost(host)) {
                Path path = explicit
                        ? Paths.get(unixSocketPath)
                        : Paths.get(unixSocketDir, ".s.PDB." + port);
                if (!UnixSocketTransport.AVAILABLE) {
                    if (explicit) {
                        throw new IOException("unixSocketPath=" + unixSocketPath
                                + " needs Java 16+ (Unix-domain SocketChannel); this JVM is "
                                + System.getProperty("java.version"));
                    }
                    LOGGER.fine(() -> "PDB native wire: Java " + System.getProperty("java.version")
                            + " has no Unix-domain sockets; using TCP " + host + ":" + port);
                } else {
                    try {
                        WireTransport t = UnixSocketTransport.connect(path, connectTimeoutMillis);
                        LOGGER.fine(() -> "PDB native wire: connected over Unix socket " + path);
                        return t;
                    } catch (UnixSocketTransport.NoListenerException absent) {
                        if (explicit) {
                            throw new IOException("unixSocketPath: " + absent.getMessage(), absent);
                        }
                        LOGGER.fine(() -> "PDB native wire: " + absent.getMessage()
                                + "; falling back to TCP " + host + ":" + port);
                    } catch (IOException e) {
                        throw new IOException("PDB native wire: Unix socket " + path
                                + " exists but connecting failed (" + e + "). Not falling back"
                                + " to TCP; pass preferUnixSocket=false to use TCP.", e);
                    }
                }
            }
        }
        return TcpTransport.connect(host, port, connectTimeoutMillis);
    }

    /** Loopback by name or literal only: resolving here would cost a DNS lookup per connect. */
    public static boolean isLoopbackHost(String host) {
        if (host == null) return false;
        String h = host.trim().toLowerCase(java.util.Locale.ROOT);
        if (h.startsWith("[") && h.endsWith("]")) h = h.substring(1, h.length() - 1);
        return h.equals("localhost") || h.equals("127.0.0.1") || h.equals("::1")
                || h.equals("0:0:0:0:0:0:0:1");
    }

    /** True when this connection runs over the server's Unix-domain socket. */
    public boolean isUnixSocket() {
        WireTransport t = transport;
        return t != null && t.isUnixSocket();
    }

    /** The open transport, e.g. {@code unix:/tmp/.s.PDB.7173} or {@code tcp:localhost:7173}; null before connect. */
    public String transportDescription() {
        WireTransport t = transport;
        return t == null ? null : t.describe();
    }

    /**
     * Open a new client stream and return the stream object. Frames sent
     * for this stream go via {@link #send(StreamState, ByteBuf)}.
     */
    public StreamState newStream() {
        // ENFORCE the advertised cap. `streams` is the single source of truth
        // for liveness (InboundHandler removes on stream end), so the check
        // reads it directly rather than mirroring the count in a second
        // counter that could drift away from the map.
        //
        // Semantics are REFUSE, not block, and the reasoning is NOT that the cap
        // is unreachable -- an earlier version of this comment claimed that and
        // it was WRONG. Independent review corrected it:
        //
        //   * `PdbConnection.sendOne` is NOT synchronized, so the one-request-
        //     per-connection property is a JDBC CONTRACT, not something this
        //     code enforces. A caller that shares a Connection across threads
        //     (misuse, but possible) creates real concurrent streams.
        //   * More importantly, a statement TIMEOUT does not cancel the request:
        //     PdbConnection catches the TimeoutException and throws
        //     SQLTimeoutException, leaving the stream LIVE in `streams` until
        //     the server's late response is read (`dispatch` removes it
        //     then) or the connection fails. So live depth exceeds 1
        //     transiently with NO misuse at all, and a connection that
        //     accumulates enough simultaneously-outstanding timed-out requests
        //     can genuinely reach this cap.
        //
        // REFUSE is chosen because it is a BOUNDED, VISIBLE failure that names
        // the cap, whereas a bounded-wait backpressure path would need its own
        // design and its own tests (and blocking here could interact badly with
        // the un-cancelled timed-out requests above). If a shared-mux design
        // lands -- one socket behind many logical connections -- that change
        // owns choosing backpressure and proving it.
        //
        // KNOWN, ACCEPTED: this check is not an atomic admission reservation.
        // `streams.size()` is read and `putIfAbsent` happens below, so N
        // concurrent callers can overshoot the cap by up to N-1. Closing that
        // needs a permit the completion path releases, which is the same design
        // the backpressure question turns on -- deliberately deferred, not
        // overlooked.
        if (maxStreams > 0 && streams.size() >= maxStreams) {
            throw new IllegalStateException(
                    "PDB wire: refusing to open another stream; maxStreamsPerConn="
                    + maxStreams + " already open on this connection."
                    + " Raise maxStreamsPerConn, or use more connections.");
        }
        java.util.concurrent.Semaphore slots = serverSlots;
        if (slots != null) {
            boolean got;
            try {
                got = acquireSlotDraining(slots,
                        System.nanoTime() + (networkTimeoutMillis > 0
                                ? Math.min(TimeUnit.SECONDS.toNanos(SLOT_WAIT_SECONDS), TimeUnit.MILLISECONDS.toNanos(networkTimeoutMillis))
                                : TimeUnit.SECONDS.toNanos(SLOT_WAIT_SECONDS)));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "PDB wire: interrupted waiting for a stream slot", ie);
            }
            if (!got) {
                if (networkTimeoutMillis > 0) {
                    abort();
                    throw new StreamState.PdbWireException(0, "connection closed: network timeout waiting for a stream slot");
                }
                throw new IllegalStateException(
                        "PDB wire: no stream slot for " + SLOT_WAIT_SECONDS + "s; the server"
                        + " allows " + maxConcurrentStreams + " concurrent streams per"
                        + " connection and all are busy. Use more connections.");
            }
        }
        long id;
        if (v2Active) {
            id = nextStreamId.getAndAdd(2); // odd ids
        } else {
            // v1: u16 stream id (wraps), allocator still safe
            id = nextStreamId.getAndAdd(2) & 0xFFFF;
            if (id == 0) id = 1;
        }
        StreamState s = new StreamState(id, this);
        // Guard against stream-id COLLISION. In v2 the ids are monotonic
        // 64-bit and never repeat, so this can only fire on a genuine bug.
        // In the v1 fallback the wire id is a u16 (masked above) and WILL
        // wrap after ~32768 statements on a long-lived connection; a plain
        // put() would clobber a still-live stream (last-write-wins) and that
        // stream's server response would then be misrouted to the new stream
        // or silently dropped in `dispatch` (s==null)  -
        // surfacing as a SELECT that returns ZERO rows. Fail loudly instead
        // of silently losing a result. (Full v1 fix: enforce the documented
        // single-stream invariant or allocate a provably-free u16.)
        if (slots != null) {
            s.slot.set(true);
        }
        StreamState prev = streams.putIfAbsent(id, s);
        if (prev != null) {
            releaseSlot(s);
            throw new IllegalStateException(
                    "PDB wire: stream id " + id + " collided with a live stream"
                    + (v2Active
                            ? " (v2 monotonic id — should be impossible)"
                            : " (v1 u16 stream-id wrap — too many concurrent"
                              + " statements on one connection)"));
        }
        s.open();
        return s;
    }

    /**
     * Wait for a server stream slot WHILE DRIVING THE READER.
     *
     * <p>A slot is released only when its stream's response is dispatched,
     * and responses are read only by a thread that holds the read role. A
     * caller that times out stops reading but leaves its stream (and slot)
     * live, so once every slot belongs to a timed-out request the responses
     * that would free them sit unread in the socket -- and a caller that
     * merely waited on the semaphore waited out the whole slot timeout with
     * the answers already buffered. So the waiter takes the read role
     * whenever it is free and dispatches frames itself; when another thread
     * holds the role, that thread is reading, and the waiter waits briefly
     * on the semaphore instead.
     */
    private boolean acquireSlotDraining(java.util.concurrent.Semaphore slots, long deadlineNanos)
            throws InterruptedException {
        while (true) {
            if (slots.tryAcquire()) return true;
            if (closed) return false;
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) return false;
            long slice = Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(5));
            if (reading.compareAndSet(false, true)) {
                try {
                    readOneFrame(System.nanoTime() + slice);
                } finally {
                    reading.set(false);
                    wakeOneWaiter();
                }
            } else if (slots.tryAcquire(slice, TimeUnit.NANOSECONDS)) {
                return true;
            }
        }
    }

    /** Read and dispatch at most one frame before the deadline. Holds the role. */
    private void readOneFrame(long deadlineNanos) {
        WireV2Codec.WireFrame f;
        try {
            f = frames.next(in, v2Active, deadlineNanos);
        } catch (TimeoutException te) {
            return; // nothing complete arrived; a partial frame stays buffered
        } catch (IOException e) {
            closed = true;
            closeSocketQuietly();
            failAll(new StreamState.PdbWireException(0, "connection closed: " + e.getMessage()));
            return;
        }
        dispatch(f);
    }

    /**
     * Send a frame on a stream and return its future. The frame is written by
     * the calling thread; a write failure fails this stream's future (and, as
     * the connection is then unusable, every other open stream's).
     */
    public CompletableFuture<StreamState.StreamResult> send(StreamState s, ByteBuf frame) {
        try {
            writeFrame(frame);
        } catch (IOException e) {
            // The connection's write side is gone: nothing queued on it will be
            // answered, so fail every open stream rather than just this one.
            closed = true;
            closeSocketQuietly();
            failAll(new StreamState.PdbWireException(0, "connection closed: " + e.getMessage()));
        }
        return s.future;
    }

    /**
     * Cancel the request {@code f} answers: send {@code RST_STREAM(CANCEL)} on
     * its stream. A server that cancels (the multiplexed wire, since the
     * per-request cancel identity) ends the statement at its next interrupt
     * check and answers the stream with 57014, so {@code f} completes and the
     * connection stays usable; an older server lets the statement finish and
     * answers normally. Either way {@code f} still completes -- the reset does
     * not abandon the stream on the client. Returns false when there is nothing
     * to reset (not one of this mux's futures, already done, v1, closed).
     */
    public boolean cancel(java.util.concurrent.Future<StreamState.StreamResult> f) {
        if (!(f instanceof StreamState.DrivenFuture) || f.isDone() || !v2Active || closed) return false;
        long sid = ((StreamState.DrivenFuture) f).streamId();
        try {
            writeFrame(WireV2Codec.encodeRstStream(sid, WireV2Codec.ERR_CANCEL));
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Write one encoded frame. Concurrent senders on one socket COMBINE: each
     * queues its frame, and whichever sender takes the write lock drains every
     * queued frame into one buffer and writes it with one system call. A sender
     * that finds the lock taken returns at once, leaving its frame for the
     * holder, which re-checks the queue after unlocking (so a frame queued just
     * as the holder finished is written by the holder's retry or by its own
     * sender's {@code tryLock} -- never stranded). Without this, a shared
     * connection's senders queued on the lock and paid a park/unpark each.
     *
     * <p>Past {@link #WRITE_QUEUE_BOUND} queued frames a sender waits for the
     * lock instead of returning, so a peer that stops reading applies
     * backpressure rather than growing the queue without bound.
     */
    private void writeFrame(ByteBuf frame) throws IOException {
        int timeout = networkTimeoutMillis;
        java.util.concurrent.ScheduledFuture<?> deadline = timeout > 0
                ? DEADLINES.schedule(this::abort, timeout, TimeUnit.MILLISECONDS) : null;
        try { writeFrameUnbounded(frame); }
        finally { if (deadline != null) deadline.cancel(false); }
    }

    private void writeFrameUnbounded(ByteBuf frame) throws IOException {
        if (closed || out == null) {
            frame.release();
            throw new IOException("connection closed");
        }
        // Uncontended fast path (the one-statement-per-Connection case): nobody
        // is queued and the lock is free, so write this frame directly without
        // a queue node. Anything queued meanwhile is drained before unlocking.
        if (pendingCount.get() == 0 && writeLock.tryLock()) {
            boolean more;
            try {
                try {
                    frame.getBytes(frame.readerIndex(), out, frame.readableBytes());
                } finally {
                    frame.release();
                }
                drainWritesLocked();
            } finally {
                writeLock.unlock();
            }
            more = !pendingWrites.isEmpty();
            if (!more) return;
            frame = null;
        }
        boolean mustWait;
        if (frame != null) {
            pendingWrites.add(frame);
            mustWait = pendingCount.incrementAndGet() > WRITE_QUEUE_BOUND;
        } else {
            mustWait = false;
        }
        while (true) {
            if (mustWait) {
                writeLock.lock();
            } else if (!writeLock.tryLock()) {
                return;
            }
            try {
                drainWritesLocked();
            } finally {
                writeLock.unlock();
            }
            if (pendingWrites.isEmpty()) {
                return;
            }
            mustWait = false;
        }
    }

    private static final int WRITE_QUEUE_BOUND = 1024;
    private final java.util.concurrent.ConcurrentLinkedQueue<ByteBuf> pendingWrites =
            new java.util.concurrent.ConcurrentLinkedQueue<>();
    private final java.util.concurrent.atomic.AtomicInteger pendingCount =
            new java.util.concurrent.atomic.AtomicInteger();
    /** Reused coalescing buffer; only touched under {@link #writeLock}. */
    private byte[] writeBuf = new byte[16 * 1024];

    private void drainWritesLocked() throws IOException {
        IOException failure = null;
        while (true) {
            int len = 0;
            ByteBuf f;
            while ((f = pendingWrites.poll()) != null) {
                pendingCount.decrementAndGet();
                try {
                    int n = f.readableBytes();
                    if (failure == null) {
                        if (len + n > writeBuf.length) {
                            if (len > 0) {
                                try {
                                    out.write(writeBuf, 0, len);
                                } catch (IOException e) {
                                    failure = e;
                                }
                                len = 0;
                            }
                            if (n > writeBuf.length) {
                                writeBuf = new byte[Math.max(n, writeBuf.length * 2)];
                            }
                        }
                        if (failure == null) {
                            f.getBytes(f.readerIndex(), writeBuf, len, n);
                            len += n;
                        }
                    }
                } finally {
                    f.release();
                }
            }
            if (len == 0) break;
            if (failure == null) {
                try {
                    out.write(writeBuf, 0, len);
                } catch (IOException e) {
                    failure = e;
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * QUERY with {@link WireV2Codec#FLAG_TXN_BEGIN}: the server opens a
     * transaction and runs {@code sql} in it, in one round trip. Only valid
     * when {@link #supportsPrepared()}.
     */
    public CompletableFuture<StreamState.StreamResult> queryBeginning(String sql) {
        StreamState s = newStream();
        return send(s, WireV2Codec.encodeV2(WireV2Codec.MSG_QUERY,
                (byte) (WireV2Codec.FLAG_END_STREAM | WireV2Codec.FLAG_TXN_BEGIN), s.streamId,
                sql.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    /** Convenience: open a new stream, send a QUERY, return the future. */
    public CompletableFuture<StreamState.StreamResult> query(String sql) {
        StreamState s = newStream();
        ByteBuf buf = v2Active
                ? WireV2Codec.encodeQueryV2(s.streamId, sql)
                : WireV2Codec.encodeQueryV1((int) (s.streamId & 0xFFFF), sql);
        return send(s, buf);
    }

    /**
     * The server's transaction status after the most recent response on this
     * connection: 0 idle, {@link WireV2Codec#FLAG_TXN_OPEN} or
     * {@link WireV2Codec#FLAG_TXN_FAILED}. Meaningful only when
     * {@link #supportsPrepared()}. Written BEFORE the response completes its
     * future, so a caller that has its result sees the status that came with it.
     */
    private volatile int txnStatus = 0;

    public int txnStatus() { return txnStatus; }

    /** The server serves bound-parameter statements on this connection. */
    public boolean supportsPrepared() {
        return v2Active && helloOk != null && helloOk.supportsPrepared();
    }

    /**
     * The server accepts typed EXECUTE parameters (tags 4..16) on this
     * connection: it echoed {@link WireV2Codec#CAP_EXT_TYPED_PARAMS}.
     */
    public boolean supportsTypedParams() {
        return v2Active && helloOk != null && helloOk.supportsTypedParams();
    }

    /**
     * The server accepts the EXECUTE array parameter (tag 17) on this
     * connection: it echoed {@link WireV2Codec#CAP_EXT_ARRAY_PARAMS}.
     */
    public boolean supportsArrayParams() {
        return v2Active && helloOk != null && helloOk.supportsArrayParams();
    }

    /** The server serves native bulk ingest ({@link WireV2Codec#MSG_BULK}) on this connection. */
    public boolean supportsBulkIngest() {
        return v2Active && helloOk != null && helloOk.supportsBulkIngest();
    }

    /** Send one native bulk-ingest request ({@link WireV2Codec#MSG_BULK}) on a new stream. v2 only. */
    public CompletableFuture<StreamState.StreamResult> bulk(byte[] payload) {
        StreamState s = newStream();
        return send(s, WireV2Codec.encodeV2(WireV2Codec.MSG_BULK, WireV2Codec.FLAG_END_STREAM, s.streamId, payload));
    }

    /** Send PREPARE on a new stream; completes with PREPARE_OK's payload. */
    public CompletableFuture<StreamState.StreamResult> prepare(String name, String sql) {
        StreamState s = newStream();
        return send(s, WireV2Codec.encodeV2(WireV2Codec.MSG_PREPARE, WireV2Codec.FLAG_END_STREAM,
                s.streamId, WireV2Codec.encodePreparePayload(name, sql)));
    }

    /** Send EXECUTE of a prepared statement (payload from {@link WireV2Codec#encodeExecutePayload}). */
    public CompletableFuture<StreamState.StreamResult> execute(byte[] payload, boolean beginTxn) {
        StreamState s = newStream();
        byte flags = beginTxn
                ? (byte) (WireV2Codec.FLAG_END_STREAM | WireV2Codec.FLAG_TXN_BEGIN)
                : WireV2Codec.FLAG_END_STREAM;
        return send(s, WireV2Codec.encodeV2(WireV2Codec.MSG_EXECUTE, flags, s.streamId, payload));
    }

    /**
     * Send a native queue op ({@link WireV2Codec#MSG_QUEUE}); {@code beginTxn}
     * carries the lazy BEGIN exactly as {@link #execute} does. v2 only.
     */
    public CompletableFuture<StreamState.StreamResult> queue(byte[] payload, boolean beginTxn) {
        StreamState s = newStream();
        byte flags = beginTxn
                ? (byte) (WireV2Codec.FLAG_END_STREAM | WireV2Codec.FLAG_TXN_BEGIN)
                : WireV2Codec.FLAG_END_STREAM;
        return send(s, WireV2Codec.encodeV2(WireV2Codec.MSG_QUEUE, flags, s.streamId, payload));
    }

    /**
     * Send one native KV frame ({@link WireV2Codec#MSG_KV_SET} / {@code GET} /
     * {@code DEL} / {@code INCR}) on a new stream, in whichever framing the
     * handshake negotiated. The server answers OK, KV_BULK, KV_NIL or ERROR.
     */
    public CompletableFuture<StreamState.StreamResult> kv(byte msgType, byte[] payload) {
        StreamState s = newStream();
        ByteBuf buf = v2Active
                ? WireV2Codec.encodeV2(msgType, WireV2Codec.FLAG_END_STREAM, s.streamId, payload)
                : WireV2Codec.encodeV1(msgType, (byte) 0, (int) (s.streamId & 0xFFFF), payload);
        return send(s, buf);
    }

    /** Convenience: open a new stream, send a BATCH, return the future. */
    public CompletableFuture<StreamState.StreamResult> batch(java.util.List<String> sqls) {
        StreamState s = newStream();
        s.expectBatch(sqls.size());
        ByteBuf buf = v2Active
                ? WireV2Codec.encodeBatchV2(s.streamId, sqls)
                : WireV2Codec.encodeBatchV1((int) (s.streamId & 0xFFFF), sqls);
        return send(s, buf);
    }

    /**
     * Send the connection-level credential frame (v1 {@code MSG_AUTH} / v2
     * {@code F_AUTH}) and return the future for the server's answer.
     *
     * <p>Must be sent BEFORE any QUERY/BATCH/KV frame on an auth-required
     * server: the server refuses everything else until it succeeds. On success
     * the server answers OK ({@code [u64 affected = 1]}) on the same stream id;
     * on failure it answers ERROR with {@link WireV2Codec#ERR_AUTH_FAILED} and
     * then tears the connection down, so the future completes exceptionally
     * with the server's message rather than hanging.
     *
     * <p>The frame goes out in whatever framing HELLO_OK negotiated - the
     * server's two dispatch loops accept the credential frame in their own
     * framing and the payload is identical in both.
     */
    public CompletableFuture<StreamState.StreamResult> authenticate(String user,
                                                                    String password) {
        // Validate + encode BEFORE allocating the stream: a rejected credential
        // (empty user, oversized field) must not leave an orphan entry in the
        // stream table that nothing will ever complete.
        byte[] payload = WireV2Codec.encodeAuthPayload(user, password);
        StreamState s = newStream();
        ByteBuf buf = v2Active
                ? WireV2Codec.encodeV2(WireV2Codec.MSG_AUTH, WireV2Codec.FLAG_END_STREAM,
                                       s.streamId, payload)
                : WireV2Codec.encodeV1(WireV2Codec.MSG_AUTH, (byte) 0,
                                       (int) (s.streamId & 0xFFFF), payload);
        return send(s, buf);
    }

    /** Send a PING (stream 0, connection-level). */
    public CompletableFuture<StreamState.StreamResult> ping() {
        StreamState s = new StreamState(0, this);
        // We can't keep stream 0 in the map with multiple in-flight pings;
        // keep last-write-wins for simplicity (PINGs are diagnostic).
        streams.put(0L, s);
        ByteBuf buf = v2Active
                ? WireV2Codec.encodePingV2()
                : WireV2Codec.encodePingV1();
        return send(s, buf);
    }

    /**
     * The event-loop group this mux was constructed with (possibly null). No
     * I/O runs on it; kept so callers that share one group keep compiling and
     * so tests can assert no per-connection group is ever created.
     */
    public EventLoopGroup eventLoopGroup() { return group; }

    /** Always false: this mux never creates, and so never shuts down, a group. */
    public boolean ownsEventLoopGroup() { return false; }

    public boolean isV2() { return v2Active; }
    /** The negotiated HELLO_OK, or null if the handshake has not completed. */
    public WireV2Codec.HelloOk helloOk() { return helloOk; }
    /** True iff HELLO_OK advertised {@link WireV2Codec#CAP_AUTH_REQUIRED}. */
    public boolean serverRequiresAuth() {
        WireV2Codec.HelloOk h = helloOk;
        return h != null && h.isAuthRequired();
    }
    public byte chosenFormat() { return chosenFormat; }
    public long maxConcurrentStreams() { return maxConcurrentStreams; }
    public int activeStreams() { return streams.size(); }
    /** True once the socket is gone (closed, transport failure, or GOAWAY). */
    public boolean isClosed() { return closed; }

    @Override
    public void close() {
        if (closed) return;
        java.util.concurrent.ScheduledFuture<?> deadline = DEADLINES.schedule(this::abort, 500, TimeUnit.MILLISECONDS);
        try {
            WireTransport s = transport;
            if (s != null && !s.isClosed()) {
                // Best-effort polite GOAWAY, bounded so a peer that stopped reading
                // cannot hold close() hostage; the socket is closed regardless.
                try {
                    if (writeLock.tryLock(500, TimeUnit.MILLISECONDS)) {
                        try {
                            ByteBuf goaway = v2Active
                                    ? WireV2Codec.encodeGoaway(nextStreamId.get(), WireV2Codec.ERR_NO_ERROR)
                                    : WireV2Codec.encodeV1(WireV2Codec.MSG_CLOSE, (byte) 0, 0, new byte[0]);
                            try {
                                goaway.getBytes(goaway.readerIndex(), out, goaway.readableBytes());
                                out.flush();
                            } finally {
                                goaway.release();
                            }
                        } finally {
                            writeLock.unlock();
                        }
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } catch (Throwable ignored) {
                    // fall through to the socket close regardless
                }
            }
            closed = true;
            closeSocketQuietly();
            ByteBuf leftover;
            while ((leftover = pendingWrites.poll()) != null) {
                leftover.release();
            }
            failAll(new StreamState.PdbWireException(0, "connection closed"));
        } finally { deadline.cancel(false); }
    }

    private void closeSocketQuietly() {
        WireTransport s = transport;
        if (s != null) {
            try { s.close(); } catch (IOException ignored) { /* best-effort */ }
        }
    }

    // ── Caller-driven reading ─────────────────────────────────────────────

    /**
     * Block until {@code target} completes, reading the socket while this
     * thread holds the read role and parking while another thread does.
     *
     * <p>Hand-off is lossless: a waiter publishes itself ({@code target.waiter})
     * BEFORE trying to take the role, and a leader releases the role BEFORE it
     * scans for a waiter to wake. So a waiter that failed to take the role is
     * either seen by the releasing leader's scan (and unparked), or retries
     * after the release and takes the role itself. Parks are also bounded, as
     * a backstop.
     */
    @Override
    public void drive(StreamState target, long deadlineNanos)
            throws InterruptedException, TimeoutException {
        driveUntil(target, target.future::isDone, deadlineNanos);
    }

    /**
     * {@link #drive} until {@code done} holds rather than until the stream's
     * future completes: a STREAMED result's consumer waits for its next chunk
     * ({@link StreamState#chunkReady}) while the future completes only with the
     * last one.
     */
    public void driveUntil(StreamState target, java.util.function.BooleanSupplier done, long deadlineNanos)
            throws InterruptedException, TimeoutException {
        target.waiter = Thread.currentThread();
        try {
            while (!done.getAsBoolean()) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                if (reading.compareAndSet(false, true)) {
                    try {
                        readUntil(done, deadlineNanos);
                    } finally {
                        reading.set(false);
                        wakeOneWaiter();
                    }
                } else {
                    long remaining = deadlineNanos - System.nanoTime();
                    if (remaining <= 0) {
                        throw new TimeoutException();
                    }
                    LockSupport.parkNanos(this, Math.min(remaining, PARK_BACKSTOP_NANOS));
                }
            }
        } finally {
            target.waiter = null;
        }
    }

    /** Upper bound on one park; wake-ups are explicit, this only bounds a bug. */
    private static final long PARK_BACKSTOP_NANOS = TimeUnit.MILLISECONDS.toNanos(50);

    /** Read and dispatch frames until {@code target} completes. Holds the role. */
    private void readUntil(java.util.function.BooleanSupplier done, long deadlineNanos) throws TimeoutException {
        while (!done.getAsBoolean()) {
            WireV2Codec.WireFrame f;
            try {
                f = frames.next(in, v2Active, deadlineNanos);
            } catch (TimeoutException te) {
                throw te;
            } catch (IOException e) {
                closed = true;
                closeSocketQuietly();
                failAll(new StreamState.PdbWireException(0, "connection closed: " + e.getMessage()));
                return;
            }
            dispatch(f);
        }
    }

    /** Route one inbound frame to its stream; wake that stream's waiter. */
    private void dispatch(WireV2Codec.WireFrame f) {
        if (f.msgType == WireV2Codec.MSG_GOAWAY) {
            // Connection going away - fail any open streams.
            closed = true;
            failAll(new StreamState.PdbWireException(0, "GOAWAY"));
            closeSocketQuietly();
            return;
        }
        StreamState s = streams.get(f.streamId);
        if (s == null) {
            // Frame for an unknown stream - silently drop per spec.
            return;
        }
        if (v2Active && helloOk != null && helloOk.supportsPrepared() && !f.hasMoreChunks()) {
            txnStatus = f.flags & (WireV2Codec.FLAG_TXN_OPEN | WireV2Codec.FLAG_TXN_FAILED);
        }
        boolean terminal = s.onInbound(f);
        if (!terminal && s.isAttached()) {
            // A streamed result's consumer waits for each chunk, not only the
            // last: wake it (it may be parked while another thread reads).
            Thread w = s.waiter;
            if (w != null && w != Thread.currentThread()) LockSupport.unpark(w);
        }
        if (terminal) {
            streams.remove(f.streamId, s);
            releaseSlot(s);
            Thread w = s.waiter;
            if (w != null && w != Thread.currentThread()) {
                LockSupport.unpark(w);
            }
        }
    }

    /** Give the read role's next turn to one thread still waiting on a result. */
    private void wakeOneWaiter() {
        if (streams.isEmpty()) return;
        for (StreamState s : streams.values()) {
            Thread w = s.waiter;
            if (w != null && w != Thread.currentThread() && !s.future.isDone()) {
                LockSupport.unpark(w);
                return;
            }
        }
    }

    /** Return a stream's server slot, exactly once. */
    private void releaseSlot(StreamState s) {
        java.util.concurrent.Semaphore slots = serverSlots;
        if (slots != null && s.slot.compareAndSet(true, false)) {
            slots.release();
        }
    }

    private void failAll(Throwable cause) {
        for (StreamState s : streams.values()) {
            releaseSlot(s);
            s.future.completeExceptionally(cause);
            Thread w = s.waiter;
            if (w != null && w != Thread.currentThread()) {
                LockSupport.unpark(w);
            }
        }
        streams.clear();
    }

    /**
     * Resumable frame parser over the socket's input stream. Bytes are
     * buffered and a frame is returned only once it is complete, so a read
     * that times out (the caller's deadline) never leaves the stream positioned
     * mid-frame: the next reader resumes exactly where this one stopped.
     */
    private static final class FrameReader {
        private byte[] buf = new byte[64 * 1024];
        private int start = 0;
        private int end = 0;

        WireV2Codec.WireFrame next(InputStream in, boolean v2, long deadlineNanos)
                throws IOException, TimeoutException {
            int header = v2 ? WireV2Codec.V2_HEADER_SIZE : WireV2Codec.V1_HEADER_SIZE;
            fill(in, header, deadlineNanos);
            int p = start;
            long streamId;
            byte msgType;
            byte flags;
            int payloadLen;
            if (v2) {
                byte version = buf[p];
                if (version != WireV2Codec.PROTOCOL_V2) {
                    throw new IOException("v2 frame with version=" + version);
                }
                msgType = buf[p + 1];
                flags = buf[p + 2];
                streamId = readIntLE(buf, p + 3) & 0xFFFF_FFFFL;
                payloadLen = readIntLE(buf, p + 7);
            } else {
                payloadLen = readIntLE(buf, p);
                streamId = ((buf[p + 4] & 0xFF) | ((buf[p + 5] & 0xFF) << 8));
                msgType = buf[p + 6];
                flags = buf[p + 7];
            }
            if (payloadLen < 0 || payloadLen > WireV2Codec.MAX_PAYLOAD) {
                throw new IOException("invalid payload length: " + payloadLen);
            }
            fill(in, header + payloadLen, deadlineNanos);
            byte[] payload = new byte[payloadLen];
            System.arraycopy(buf, start + header, payload, 0, payloadLen);
            start += header + payloadLen;
            if (start == end) {
                start = 0;
                end = 0;
            }
            return new WireV2Codec.WireFrame(streamId, msgType, flags, payload);
        }

        /** Ensure at least {@code need} buffered bytes from {@code start}. */
        private void fill(InputStream in, int need, long deadlineNanos)
                throws IOException, TimeoutException {
            if (end - start >= need) return;
            if (buf.length - start < need) {
                // Compact, growing if the frame is larger than the buffer.
                byte[] dst = need > buf.length ? new byte[Math.max(need, buf.length * 2)] : buf;
                System.arraycopy(buf, start, dst, 0, end - start);
                end -= start;
                start = 0;
                buf = dst;
            }
            while (end - start < need) {
                if (deadlineNanos != Long.MAX_VALUE) {
                    long remaining = deadlineNanos - System.nanoTime();
                    if (remaining <= 0) throw new TimeoutException();
                    setTimeout(in, (int) Math.max(1, Math.min(Integer.MAX_VALUE,
                            TimeUnit.NANOSECONDS.toMillis(remaining))));
                } else {
                    setTimeout(in, 0);
                }
                int n;
                try {
                    n = in.read(buf, end, buf.length - end);
                } catch (SocketTimeoutException ste) {
                    continue; // re-check the deadline; buffered bytes are kept
                }
                if (n < 0) throw new IOException("connection closed by server");
                end += n;
            }
        }

        private WireTransport timeoutTarget;
        private int currentTimeout = -1;

        private void setTimeout(InputStream in, int millis) throws IOException {
            if (millis != currentTimeout && timeoutTarget != null) {
                timeoutTarget.setReadTimeout(millis);
                currentTimeout = millis;
            }
        }

        private static int readIntLE(byte[] b, int off) {
            return (b[off] & 0xFF)
                 | ((b[off + 1] & 0xFF) << 8)
                 | ((b[off + 2] & 0xFF) << 16)
                 | ((b[off + 3] & 0xFF) << 24);
        }
    }
}
