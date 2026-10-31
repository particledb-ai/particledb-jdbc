package org.particledb;

import org.particledb.wire.StreamMux;
import org.particledb.wire.StreamState;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Rows of a chunked Arrow result, decoded one record batch at a time as the
 * chunks arrive -- the driver's memory-bounded result path, used when the
 * statement set {@code setFetchSize(n > 0)} (the JDBC knob pgjdbc also streams
 * on).
 *
 * <p>The server answers a large result as {@code MSG_ARROW_STREAM} chunks that
 * split ONE Arrow IPC stream at arbitrary byte offsets. This reader reassembles
 * whole IPC messages from the chunks, keeps the schema (and each
 * dictionary id's latest batch plus the delta batches after it), and decodes each record batch on its own by handing
 * the existing decoder a minimal stream: schema, dictionaries, that batch,
 * end-of-stream. Memory is one partial message plus one decoded batch, instead
 * of the whole IPC stream plus every decoded row.
 *
 * <p>A chunk is waited for by driving the connection
 * ({@link StreamMux#driveUntil}), so other statements on the same multiplexed
 * connection keep being served between batches.
 */
final class StreamedRows {
    private static final byte[] EOS = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0, 0, 0, 0};

    private final StreamMux mux;
    private final PdbConnection connection;
    private final StreamState stream;
    private byte[] buf = new byte[0];
    private int start;
    private int end;
    private byte[] schemaMsg;
    private final Map<Long, List<byte[]>> dictionaries = new LinkedHashMap<>();
    private boolean done;

    StreamedRows(PdbConnection connection, StreamMux mux, StreamState stream) {
        this.connection = connection;
        this.mux = mux;
        this.stream = stream;
    }

    private ArrowDecoder.Decoded schema;

    /**
     * Read up to and including the schema message (called once, by the
     * driver, before the result set exists); a 0-row decode carrying the
     * columns.
     */
    StreamedRows readSchema() throws SQLException {
        while (schemaMsg == null) {
            byte[] m = nextMessage();
            if (m == null) throw new SQLException("streamed result ended before its schema", "08P01");
        }
        schema = ArrowDecoder.decodePositional(concat(schemaMsg, EOS), connection.resultDecoder());
        return this;
    }

    /** The columns ({@link #readSchema} must have run). */
    ArrowDecoder.Decoded schema() {
        return schema;
    }

    /** The next record batch's rows (possibly empty), or null when the result is exhausted. */
    ArrowDecoder.Decoded nextBatch() throws SQLException {
        while (true) {
            byte[] m = nextMessage();
            if (m == null) return null;
            if (lastType != ArrowIpcReader.H_RECORD_BATCH) continue;
            int n = schemaMsg.length + EOS.length + m.length;
            for (List<byte[]> ds : dictionaries.values()) for (byte[] d : ds) n += d.length;
            byte[] payload = new byte[n];
            int p = 0;
            System.arraycopy(schemaMsg, 0, payload, p, schemaMsg.length);
            p += schemaMsg.length;
            for (List<byte[]> ds : dictionaries.values()) {
                for (byte[] d : ds) {
                    System.arraycopy(d, 0, payload, p, d.length);
                    p += d.length;
                }
            }
            System.arraycopy(m, 0, payload, p, m.length);
            p += m.length;
            System.arraycopy(EOS, 0, payload, p, EOS.length);
            return ArrowDecoder.decodePositional(payload, connection.resultDecoder());
        }
    }

    /** The consumer stopped early: drop what is queued and what is still to come. */
    void discard() {
        done = true;
        buf = new byte[0];
        start = end = 0;
        stream.discardRest();
    }

    private byte lastType;

    /**
     * The next whole IPC message (continuation marker, length, metadata,
     * body), or null at end of stream. Schema and dictionary messages are
     * recorded as a side effect; {@link #lastType} says which kind was read.
     */
    private byte[] nextMessage() throws SQLException {
        if (done) return null;
        if (!need(4)) return endOfChunks();
        int prefix = 4;
        int len = readIntLE(start);
        if (len == -1) {
            if (!need(8)) return endOfChunks();
            len = readIntLE(start + 4);
            prefix = 8;
        }
        if (len == 0) { // end-of-stream marker
            done = true;
            return null;
        }
        if (len < 0) throw new SQLException("streamed result: bad IPC message length " + len, "08P01");
        if (!need(prefix + len)) return endOfChunks();
        ArrowIpcReader.MessageInfo msg;
        try {
            msg = ArrowIpcReader.messageInfo(buf, start + prefix, len);
        } catch (RuntimeException e) {
            throw new SQLException("streamed result: unreadable IPC message: " + e.getMessage(), "08P01", e);
        }
        long body = msg.bodyLength;
        if (body < 0 || body > Integer.MAX_VALUE - prefix - len) {
            throw new SQLException("streamed result: bad IPC body length " + body, "08P01");
        }
        int total = prefix + len + (int) body;
        if (!need(total)) return endOfChunks();
        byte[] m = java.util.Arrays.copyOfRange(buf, start, start + total);
        start += total;
        lastType = msg.headerType;
        if (lastType == ArrowIpcReader.H_SCHEMA) {
            schemaMsg = m;
        } else if (lastType == ArrowIpcReader.H_DICTIONARY) {
            // A delta batch appends to the id's dictionary; any other batch
            // replaces it.
            List<byte[]> ds = msg.delta ? dictionaries.get(msg.dictionaryId) : null;
            if (ds == null) {
                ds = new ArrayList<>();
                dictionaries.put(msg.dictionaryId, ds);
            }
            if (!msg.delta) ds.clear();
            ds.add(m);
        }
        return m;
    }

    /** The chunks ran out without an end-of-stream marker: the result is complete. */
    private byte[] endOfChunks() {
        done = true;
        return null;
    }

    /** Make {@code n} bytes available from {@code start}; false when the result ended first. */
    private boolean need(int n) throws SQLException {
        long deadline = connection.networkDeadline();
        while (end - start < n) {
            byte[] c = stream.pollChunk();
            if (c == null) {
                if (stream.future.isDone()) {
                    try {
                        stream.future.getNow(null);
                    } catch (java.util.concurrent.CompletionException ce) {
                        Throwable cause = ce.getCause() instanceof Exception ? ce.getCause() : ce;
                        throw PdbConnection.mapException((Exception) cause);
                    }
                    // Completed: drain anything queued in between.
                    c = stream.pollChunk();
                    if (c == null) return false;
                } else {
                    try {
                        mux.driveUntil(stream, stream::chunkReady, deadline);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new SQLException("interrupted while streaming a result", "57014", ie);
                    } catch (java.util.concurrent.TimeoutException te) {
                        throw connection.networkTimeout(te);
                    }
                    continue;
                }
            }
            append(c);
            deadline = connection.networkDeadline();
        }
        return true;
    }

    private void append(byte[] c) {
        int live = end - start;
        if (buf.length - end < c.length) {
            byte[] nb = new byte[Math.max(live + c.length, Math.min(buf.length * 2, live + c.length + (1 << 20)))];
            System.arraycopy(buf, start, nb, 0, live);
            buf = nb;
            start = 0;
            end = live;
        }
        System.arraycopy(c, 0, buf, end, c.length);
        end += c.length;
    }

    private int readIntLE(int at) {
        return (buf[at] & 0xFF) | (buf[at + 1] & 0xFF) << 8 | (buf[at + 2] & 0xFF) << 16 | (buf[at + 3] & 0xFF) << 24;
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] o = new byte[a.length + b.length];
        System.arraycopy(a, 0, o, 0, a.length);
        System.arraycopy(b, 0, o, a.length, b.length);
        return o;
    }
}
