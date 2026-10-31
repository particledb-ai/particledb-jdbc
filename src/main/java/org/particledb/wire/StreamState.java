package org.particledb.wire;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Per-stream state and result accumulator.
 *
 * <p>A stream lives from the first client frame (entering {@link State#OPEN})
 * to the terminal server frame ({@link State#CLOSED}). Intermediate frames
 * (e.g. chunked Arrow IPC) are accumulated here.
 *
 * <p>State transitions follow the spec:
 * <pre>
 *   IDLE -> OPEN -> HALF_CLOSED -> CLOSED
 *   any  -> CLOSED  (on RST_STREAM)
 * </pre>
 */
public final class StreamState {

    public enum State { IDLE, OPEN, HALF_CLOSED_LOCAL, HALF_CLOSED_REMOTE, CLOSED }

    public final long streamId;
    private volatile State state = State.IDLE;

    // Result accumulator
    private final List<byte[]> arrowChunks = new ArrayList<>();
    private byte resultType = 0;          // first server msgType seen
    private byte[] singleFramePayload;     // for OK/ERROR/PONG/etc.

    /**
     * Statements sent on this stream as one MSG_BATCH, or -1 when the stream
     * did not carry a batch. Set by {@link StreamMux#batch} before the frame is
     * written, so a short batch result can be told apart from a complete one.
     */
    private volatile int batchStatements = -1;

    /** Record that this stream carries a MSG_BATCH of {@code n} statements. */
    void expectBatch(int n) { this.batchStatements = n; }

    /**
     * Future completed when the terminal frame arrives. On a stream opened by
     * {@link StreamMux} it is a {@link DrivenFuture}: a thread blocked in
     * {@code get} reads the socket itself when nobody else is, so a response
     * reaches the caller without a hop through another thread.
     */
    public final CompletableFuture<StreamResult> future;

    /** The thread blocked on {@link #future}, for {@link StreamMux} to wake. */
    volatile Thread waiter;

    /** Whether this stream holds one of the server's stream slots. */
    final java.util.concurrent.atomic.AtomicBoolean slot =
            new java.util.concurrent.atomic.AtomicBoolean();

    public StreamState(long streamId) {
        this.streamId = streamId;
        this.future = new CompletableFuture<>();
    }

    StreamState(long streamId, Driver driver) {
        this.streamId = streamId;
        this.future = new DrivenFuture(this, driver);
    }

    /** Reads the connection on behalf of a blocked caller (see {@link StreamMux}). */
    interface Driver {
        /** Return once {@code s.future} is done; TimeoutException at the deadline. */
        void drive(StreamState s, long deadlineNanos)
                throws InterruptedException, java.util.concurrent.TimeoutException;
    }

    /**
     * A future whose blocking {@code get} drives the connection: the waiting
     * thread takes the connection's read role when it is free and reads until
     * its own frame arrives, instead of parking until another thread delivers it.
     */
    public static final class DrivenFuture extends CompletableFuture<StreamResult> {
        /** The stream this future answers (what a cancel resets). */
        long streamId() { return stream.streamId; }

        /** The stream this future answers. */
        public StreamState streamState() { return stream; }

        private final StreamState stream;
        private final Driver driver;

        DrivenFuture(StreamState stream, Driver driver) {
            this.stream = stream;
            this.driver = driver;
        }

        @Override
        public StreamResult get() throws InterruptedException,
                java.util.concurrent.ExecutionException {
            if (!isDone()) {
                try {
                    driver.drive(stream, Long.MAX_VALUE);
                } catch (java.util.concurrent.TimeoutException impossible) {
                    throw new IllegalStateException(impossible);
                }
            }
            return super.get();
        }

        @Override
        public StreamResult get(long timeout, java.util.concurrent.TimeUnit unit)
                throws InterruptedException, java.util.concurrent.ExecutionException,
                       java.util.concurrent.TimeoutException {
            if (!isDone()) {
                long now = System.nanoTime();
                long nanos = unit.toNanos(timeout);
                long deadline = nanos >= Long.MAX_VALUE - now ? Long.MAX_VALUE : now + nanos;
                driver.drive(stream, deadline);
            }
            return super.get();
        }

        @Override
        public StreamResult join() {
            try {
                return get();
            } catch (java.util.concurrent.ExecutionException e) {
                throw new java.util.concurrent.CompletionException(e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new java.util.concurrent.CompletionException(e);
            }
        }
    }

    public State state() { return state; }
    public synchronized void open() { if (state == State.IDLE) state = State.OPEN; }
    public synchronized void halfCloseLocal() {
        if (state == State.OPEN) state = State.HALF_CLOSED_LOCAL;
    }
    public synchronized void halfCloseRemote() {
        if (state == State.OPEN) state = State.HALF_CLOSED_REMOTE;
    }
    public synchronized void close() { state = State.CLOSED; }

    /**
     * Process an inbound server frame for this stream. Returns true if the
     * stream is now terminal (caller should remove it from the mux table).
     */
    private static boolean rolledBack(WireV2Codec.WireFrame f) {
        return (f.flags & WireV2Codec.FLAG_TXN_ROLLED_BACK) != 0;
    }

    public synchronized boolean onInbound(WireV2Codec.WireFrame f) {
        // First-frame: capture the type
        if (resultType == 0) {
            resultType = f.msgType;
        }
        switch (f.msgType) {
            case WireV2Codec.MSG_NOTICE:
                // A statement advisory, before its terminal frame (only sent
                // when CAP_STATEMENT_NOTICE was negotiated). Not terminal.
                // A malformed frame is dropped whole, as the server's own
                // decoder does.
                java.util.List<String[]> n = decodeNotices(f.payload);
                if (n != null) {
                    if (notices.isEmpty()) notices = new java.util.ArrayList<>();
                    notices.addAll(n);
                }
                return false;
            case WireV2Codec.MSG_ARROW_STREAM:
                if (attached) {
                    // A streaming consumer owns the chunks: queue them for it
                    // (or drop them once it closed its result set early).
                    if (!discard) chunkQueue.add(f.payload);
                    if (!f.hasMoreChunks()) {
                        future.complete(attach(StreamResult.ok(new byte[0])));
                        close();
                        return true;
                    }
                    return false;
                }
                arrowChunks.add(f.payload);
                if (!f.hasMoreChunks()) {
                    completeArrowStream();
                    close();
                    return true;
                }
                return false;
            case WireV2Codec.MSG_ARROW_RESULT:
                singleFramePayload = f.payload;
                future.complete(attach(StreamResult.arrow(f.payload)));
                close();
                return true;
            case WireV2Codec.MSG_BATCH_RESULT: {
                // A batch answers with ONE frame even when a statement fails:
                // the failure is an ERROR entry inside it, not a MSG_ERROR.
                // Decode every entry here so no caller can mistake a failed
                // batch for success.
                WireV2Codec.BatchResult br;
                try {
                    br = WireV2Codec.decodeBatchResult(f.payload);
                } catch (IllegalArgumentException e) {
                    future.completeExceptionally(new PdbWireException(
                            WireV2Codec.ERR_PROTOCOL_ERROR, "protocol error: " + e.getMessage()));
                    close();
                    return true;
                }
                if (br.errorMessage != null) {
                    future.completeExceptionally(br.errorSqlState != null
                            ? new PdbBatchException(br.counts, br.errorMessage, br.errorCode, br.errorSqlState)
                            : new PdbBatchException(br.counts, br.errorMessage));
                } else if (batchStatements >= 0 && br.entries != batchStatements) {
                    future.completeExceptionally(new PdbWireException(
                            WireV2Codec.ERR_PROTOCOL_ERROR, "protocol error: batch of "
                            + batchStatements + " statements answered with "
                            + br.entries + " results and no error"));
                } else {
                    singleFramePayload = f.payload;
                    future.complete(attach(StreamResult.ok(f.payload, rolledBack(f))));
                }
                close();
                return true;
            }
            case WireV2Codec.MSG_OK:
            case WireV2Codec.MSG_PREPARE_OK:
                singleFramePayload = f.payload;
                future.complete(attach(StreamResult.ok(f.payload, rolledBack(f))));
                close();
                return true;
            case WireV2Codec.MSG_ERROR: {
                WireV2Codec.WireError err = f.parseError();
                future.completeExceptionally(PdbWireException.fromServerFrame(err.code, err.message));
                close();
                return true;
            }
            case WireV2Codec.MSG_PONG:
                future.complete(attach(StreamResult.pong()));
                close();
                return true;
            case WireV2Codec.MSG_KV_BULK:
                future.complete(attach(StreamResult.kvBulk(f.payload)));
                close();
                return true;
            case WireV2Codec.MSG_KV_NIL:
                future.complete(attach(StreamResult.kvNil()));
                close();
                return true;
            case WireV2Codec.MSG_QUEUE_RESULT:
                future.complete(attach(StreamResult.queue(f.payload)));
                close();
                return true;
            case WireV2Codec.MSG_RST_STREAM_S: {
                int code = (f.payload.length >= 2)
                        ? ((f.payload[0] & 0xFF) | ((f.payload[1] & 0xFF) << 8))
                        : 0;
                future.completeExceptionally(
                        new PdbWireException(code, "stream reset by server"));
                close();
                return true;
            }
            default:
                future.completeExceptionally(
                        new PdbWireException(0,
                                "unexpected frame type 0x"
                                        + Integer.toHexString(f.msgType & 0xFF)));
                close();
                return true;
        }
    }

    // ── Streamed results (JDBC setFetchSize) ────────────────────────────────
    //
    // By default a chunked Arrow result (MSG_ARROW_STREAM ... last chunk) is
    // accumulated and the future completes with the whole IPC stream. A
    // consumer that wants rows as they arrive ATTACHES once the first chunk is
    // in: the chunks so far move to chunkQueue, later chunks go there instead
    // of arrowChunks, and the future completes (with an empty OK) only at the
    // terminal frame. Guarded by this object's monitor, as onInbound is.

    private boolean attached;
    private boolean discard;
    private final java.util.ArrayDeque<byte[]> chunkQueue = new java.util.ArrayDeque<>();

    /** True once the first chunk of a chunked result, or the whole answer, is in. */
    public synchronized boolean firstChunkOrDone() {
        return future.isDone() || !arrowChunks.isEmpty();
    }

    /**
     * Take over the chunks of a chunked result. False when there is nothing to
     * stream: the answer already completed (a one-frame result, an error, or a
     * chunked result whose last chunk is in) -- the caller uses the future.
     */
    public synchronized boolean attachStreaming() {
        if (future.isDone() || arrowChunks.isEmpty()) return false;
        chunkQueue.addAll(arrowChunks);
        arrowChunks.clear();
        attached = true;
        return true;
    }

    public synchronized boolean isAttached() { return attached; }

    /** A chunk is queued, or no more will come (ended / failed). */
    public synchronized boolean chunkReady() {
        return !chunkQueue.isEmpty() || future.isDone();
    }

    /** The next queued chunk, or null when none is queued. */
    public synchronized byte[] pollChunk() {
        return chunkQueue.poll();
    }

    /** The streaming consumer stopped early: drop the rest as it arrives. */
    public synchronized void discardRest() {
        discard = true;
        chunkQueue.clear();
    }

    /** Advisories received on this stream so far ({@link WireV2Codec#MSG_NOTICE}). */
    private java.util.List<String[]> notices = java.util.Collections.emptyList();

    private StreamResult attach(StreamResult r) {
        if (!notices.isEmpty()) r.notices = java.util.Collections.unmodifiableList(notices);
        return r;
    }

    /**
     * Decode a notice frame: {@code [u16 count] count x ([u8 len][severity]
     * [u8 len][sqlstate][u32 len][message])}, little-endian. Each entry is
     * {@code {severity, sqlstate, message}}. {@code null} when malformed.
     */
    static java.util.List<String[]> decodeNotices(byte[] p) {
        try {
            int off = 0;
            int count = (p[off] & 0xFF) | ((p[off + 1] & 0xFF) << 8);
            off += 2;
            java.util.List<String[]> out = new java.util.ArrayList<>(count);
            java.nio.charset.Charset u = java.nio.charset.StandardCharsets.UTF_8;
            for (int i = 0; i < count; i++) {
                int sl = p[off++] & 0xFF;
                String sev = new String(p, off, sl, u);
                off += sl;
                int tl = p[off++] & 0xFF;
                String st = new String(p, off, tl, u);
                off += tl;
                int ml = (p[off] & 0xFF) | ((p[off + 1] & 0xFF) << 8) | ((p[off + 2] & 0xFF) << 16) | ((p[off + 3] & 0xFF) << 24);
                off += 4;
                if (ml < 0 || off + ml > p.length) return null;
                out.add(new String[] {sev, st, new String(p, off, ml, u)});
                off += ml;
            }
            return off == p.length ? out : null;
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
    }

    private void completeArrowStream() {
        int total = 0;
        for (byte[] c : arrowChunks) total += c.length;
        byte[] combined = new byte[total];
        int pos = 0;
        for (byte[] c : arrowChunks) {
            System.arraycopy(c, 0, combined, pos, c.length);
            pos += c.length;
        }
        future.complete(attach(StreamResult.arrow(combined)));
    }

    /** Result returned through the per-stream future. */
    public static final class StreamResult {
        public enum Kind { OK, ARROW, PONG, KV_BULK, KV_NIL, QUEUE }
        public final Kind kind;
        public final byte[] payload;
        /**
         * The server marked this response {@link WireV2Codec#FLAG_TXN_ROLLED_BACK}:
         * a COMMIT in it ended a FAILED transaction by rolling it back (the
         * native stand-in for PostgreSQL's {@code ROLLBACK} command tag).
         */
        public final boolean rolledBack;
        /**
         * The statement's advisories ({@code {severity, sqlstate, message}}),
         * delivered on its stream before this result; empty when none.
         */
        public java.util.List<String[]> notices = java.util.Collections.emptyList();

        private StreamResult(Kind kind, byte[] payload) {
            this(kind, payload, false);
        }

        private StreamResult(Kind kind, byte[] payload, boolean rolledBack) {
            this.kind = kind;
            this.payload = payload;
            this.rolledBack = rolledBack;
        }

        public static StreamResult ok(byte[] p)        { return new StreamResult(Kind.OK,       p); }
        public static StreamResult ok(byte[] p, boolean rolledBack) {
            return new StreamResult(Kind.OK, p, rolledBack);
        }
        public static StreamResult arrow(byte[] p)     { return new StreamResult(Kind.ARROW,    p); }
        public static StreamResult pong()              { return new StreamResult(Kind.PONG,     new byte[0]); }
        public static StreamResult kvBulk(byte[] p)    { return new StreamResult(Kind.KV_BULK,  p); }
        public static StreamResult kvNil()             { return new StreamResult(Kind.KV_NIL,   new byte[0]); }
        public static StreamResult queue(byte[] p)     { return new StreamResult(Kind.QUEUE,    p); }
        /**
         * A row-returning answer whose chunks a consumer reads as they arrive
         * ({@code payload} is empty; {@link #streamed} is the driver's reader).
         */
        public static StreamResult arrowStreamed(Object reader) {
            StreamResult r = new StreamResult(Kind.ARROW, new byte[0]);
            r.streamed = reader;
            return r;
        }
        /** The driver's streamed-rows reader for {@link #arrowStreamed}; null otherwise. */
        public Object streamed;

        public long affectedRows() {
            if (kind != Kind.OK) return 0L;
            return WireV2Codec.decodeOkAffected(payload);
        }
    }

    /**
     * A statement inside a MSG_BATCH failed. The server stopped there:
     * {@link #completedCounts} holds the results of the statements before it
     * (affected rows, {@code -1} for a row-returning one). A batch ERROR entry
     * carries no code, so {@link #code} is the server's generic SQL error code
     * ({@code ERR_GENERIC_SQL}, SQLSTATE {@code HY000}).
     */
    public static final class PdbBatchException extends PdbWireException {
        public final long[] completedCounts;
        /**
         * The failed entry's SQLSTATE, when the server sent it
         * ({@code CAP_EXT_BATCH_SQLSTATE}); null for a legacy entry.
         */
        public final String sqlState;
        public PdbBatchException(long[] completedCounts, String msg) {
            super(1064, msg);
            this.completedCounts = completedCounts;
            this.sqlState = null;
        }
        public PdbBatchException(long[] completedCounts, String msg, int code, String sqlState) {
            super(code, msg, true);
            this.completedCounts = completedCounts;
            this.sqlState = sqlState;
        }
    }

    /** Wire-level exception from server. */
    public static class PdbWireException extends RuntimeException {
        public final int code;
        /**
         * The server's message WITHOUT the {@code "PDB wire error N: "}
         * prefix that {@link #getMessage()} adds. Callers that surface the
         * server's own wording (the connect-time authentication failure, which
         * must read as PostgreSQL's "password authentication failed for user
         * …" rather than a generic wire error) use this.
         */
        public final String serverMessage;
        /**
         * {@code true} when this exception is the server's own error frame
         * ({@code MSG_ERROR}) - an error the SERVER raised while executing the
         * request - and {@code false} when the driver synthesized it (closed
         * connection, GOAWAY, stream reset, unexpected frame). Only a server
         * error is rendered in PostgreSQL's {@code "ERROR: <message>"} shape
         * (see {@code PdbSqlState.serverErrorToSqlException}): pgjdbc's own
         * client-side failures do not carry a severity either.
         */
        public final boolean fromServer;
        public PdbWireException(int code, String msg) {
            this(code, msg, false);
        }
        protected PdbWireException(int code, String msg, boolean fromServer) {
            super("PDB wire error " + code + ": " + msg);
            this.code = code;
            this.serverMessage = msg == null ? "" : msg;
            this.fromServer = fromServer;
        }
        /** The exception for a server {@code MSG_ERROR} frame. */
        public static PdbWireException fromServerFrame(int code, String msg) {
            return new PdbWireException(code, msg, true);
        }
    }
}
