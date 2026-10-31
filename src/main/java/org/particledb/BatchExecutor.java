package org.particledb;

import org.particledb.wire.StreamMux;
import org.particledb.wire.StreamState;
import org.particledb.wire.WireV2Codec;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Wraps a {@link PdbConnection}'s batch path so callers can ship N
 * statements in one {@code MSG_BATCH} frame on a single stream.
 *
 * <p>Equivalent to {@link java.sql.Statement#executeBatch} but exposed as a
 * standalone helper for benchmark code that wants to drive batches without
 * going through the full JDBC API surface.
 */
public final class BatchExecutor {

    private final PdbConnection conn;
    private final StreamMux mux;
    private final List<String> sqls = new ArrayList<>();

    public BatchExecutor(PdbConnection conn) {
        this.conn = conn;
        this.mux = conn.mux();
    }

    public BatchExecutor add(String sql) {
        sqls.add(sql);
        return this;
    }

    public BatchExecutor addAll(List<String> ss) {
        sqls.addAll(ss);
        return this;
    }

    /** Execute the queued statements as a single MSG_BATCH frame. */
    public long execute() throws SQLException {
        if (sqls.isEmpty()) return 0;
        try {
            StreamState.StreamResult r = mux.batch(new ArrayList<>(sqls))
                    .get(120, TimeUnit.SECONDS);
            sqls.clear();
            // A MSG_BATCH_RESULT arrives as a Kind.OK StreamResult only when
            // every statement succeeded: StreamState decodes each entry and
            // completes the future exceptionally on an ERROR entry (or a short
            // count), which lands in the catch below. The payload is
            // [u32_le count][per-stmt entries...] - decode the leading u32
            // statement count, NOT the u64 that affectedRows() would read
            // (which returns count | (firstEntry << 32) garbage - finding F6).
            return r.kind == StreamState.StreamResult.Kind.OK
                    ? WireV2Codec.decodeBatchCount(r.payload)
                    : 0L;
        } catch (Exception e) {
            sqls.clear();
            if (e instanceof java.util.concurrent.TimeoutException) {
                // Ambiguous: the batch may still commit and its late reply is
                // still in flight. Never resent; the connection is closed.
                conn.closeAfterTimeout();
            }
            // Route through the same mapper as the JDBC path so a batch failure
            // carries its SQLSTATE and vendor code instead of a bare message.
            throw PdbConnection.mapException(e);
        }
    }

    public int size() { return sqls.size(); }
    public void clear() { sqls.clear(); }
}
