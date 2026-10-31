package org.particledb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * ParticleDB's native queue ,
 * one {@code F_QUEUE} frame (one round trip) per operation.
 *
 * <p>Obtain it from a connection: {@code conn.unwrap(PdbConnection.class).queue()}.
 * Operations run in the connection's transaction exactly as statements do: with
 * {@code autoCommit=false} they join the open transaction (and commit or roll
 * back with the SQL in it); with {@code autoCommit=true} each one is its own
 * transaction on the server.
 *
 * <p>Semantics (server side): a message is due when its {@code deliverAt} (epoch
 * ms) has passed. {@link #pop} claims due messages in order priority DESC, then
 * FIFO, and leases each for {@code leaseMs}: it is invisible to other claimers
 * until it is acked, or the lease expires and it is redelivered (no sweeper).
 *
 * <p>Wire layout, mirrored byte for byte from
 * the server's queue frame codec (all integers little-endian):
 * <pre>
 * request  := u8 fmt_v(=1) | u8 op | str queue | op fields
 * response := u8 fmt_v(=1) | u8 op | body
 * str      := u32 len | utf8 ;  ostr := u8 present | [str]
 * </pre>
 */
public final class PdbQueue {

    static final byte FMT_V = 1;
    static final byte OP_PUSH = 1, OP_PUSH_IF_ABSENT = 2, OP_POP = 3, OP_ACK = 4, OP_SET_LEASE = 5,
            OP_RESET_OFFSET = 6, OP_POSTPONE = 7, OP_CONTAINS = 8, OP_FLUSH = 9, OP_SIZE = 10,
            OP_DETAIL = 11, OP_INIT = 12, OP_PUSH_BATCH = 13;

    /** A claimed message. */
    public static final class Claim {
        public final String id;
        public final long priority;
        public final long attempts;
        /** Lease expiry, epoch ms. */
        public final long deliverAt;
        public final String payload;

        Claim(String id, long priority, long attempts, long deliverAt, String payload) {
            this.id = id;
            this.priority = priority;
            this.attempts = attempts;
            this.deliverAt = deliverAt;
            this.payload = payload;
        }
    }

    /** One message of {@link #pushBatch}. */
    public static final class Item {
        public final String id;
        public final long priority;
        public final long delayMs;
        public final String payload;

        public Item(String id, long priority, long delayMs, String payload) {
            this.id = id;
            this.priority = priority;
            this.delayMs = delayMs;
            this.payload = payload;
        }
    }

    /** Committed counts of one queue. */
    public static final class Size {
        public final long total;
        public final long inFlight;

        Size(long total, long inFlight) {
            this.total = total;
            this.inFlight = inFlight;
        }
    }

    /** Counts of one queue in {@link #detail}. */
    public static final class QueueDetail {
        public final String queue;
        public final long total;
        public final long inFlight;

        QueueDetail(String queue, long total, long inFlight) {
            this.queue = queue;
            this.total = total;
            this.inFlight = inFlight;
        }
    }

    private final PdbConnection conn;

    PdbQueue(PdbConnection conn) {
        this.conn = conn;
    }

    // ── operations ────────────────────────────────────────────────────────

    /** Create the queue tables (idempotent DDL). Needs {@code autoCommit=true}. */
    public void init() throws SQLException {
        call(new Enc(OP_INIT, ""));
    }

    /**
     * Push (PostgresQueueDAO semantics): insert, or for an existing id update
     * the payload and the due time - a message already in flight stays in flight,
     * due at now + delay + {@code unackWindowMs}. Returns true when inserted.
     */
    public boolean push(String queue, String id, long priority, long delayMs, String payload,
                        long unackWindowMs) throws SQLException {
        Enc e = new Enc(OP_PUSH, queue).item(id, priority, delayMs, payload).i64(unackWindowMs);
        return decode(e, PdbQueue::bool);
    }

    /** Insert only if absent. Returns true when inserted. */
    public boolean pushIfAbsent(String queue, String id, long priority, long delayMs, String payload)
            throws SQLException {
        return decode(new Enc(OP_PUSH_IF_ABSENT, queue).item(id, priority, delayMs, payload), PdbQueue::bool);
    }

    /** Push many in one frame (one transaction). Returns how many were inserted. */
    public int pushBatch(String queue, List<Item> items, long unackWindowMs) throws SQLException {
        Enc e = new Enc(OP_PUSH_BATCH, queue).u32(items.size());
        for (Item i : items) e.item(i.id, i.priority, i.delayMs, i.payload);
        e.i64(unackWindowMs);
        return decode(e, Dec::u32);
    }

    /** Claim up to {@code n} due messages, each leased for {@code leaseMs}. */
    public List<Claim> pop(String queue, int n, long leaseMs) throws SQLException {
        Dec d = call(new Enc(OP_POP, queue).u32(n).i64(leaseMs));
        try {
            // Each claim is at least 29 bytes, so a count beyond that bound is
            // malformed - never pre-allocate from an unchecked count.
            int count = d.count(29);
            if (count == 0) return Collections.emptyList();
            List<Claim> out = new ArrayList<>(count);
            for (int k = 0; k < count; k++) {
                out.add(new Claim(d.str(), d.i64(), d.i64(), d.i64(), d.ostr()));
            }
            return out;
        } catch (RuntimeException re) {
            throw Dec.malformed(re);
        }
    }

    /** Ack (delete). Returns true when the message existed. */
    public boolean ack(String queue, String id) throws SQLException {
        return decode(new Enc(OP_ACK, queue).str(id), PdbQueue::bool);
    }

    /** Set the message due at now + ms (Conductor's setUnackTimeout). True if it existed. */
    public boolean setLease(String queue, String id, long ms) throws SQLException {
        return decode(new Enc(OP_SET_LEASE, queue).str(id).i64(ms), PdbQueue::bool);
    }

    /** Make the message due now. True if it existed. */
    public boolean resetOffset(String queue, String id) throws SQLException {
        return decode(new Enc(OP_RESET_OFFSET, queue).str(id), PdbQueue::bool);
    }

    /** QueueDAO.postpone: remove, then push queued with the given priority and delay. */
    public boolean postpone(String queue, String id, long priority, long delayMs) throws SQLException {
        return decode(new Enc(OP_POSTPONE, queue).str(id).i64(priority).i64(delayMs), PdbQueue::bool);
    }

    public boolean contains(String queue, String id) throws SQLException {
        return decode(new Enc(OP_CONTAINS, queue).str(id), PdbQueue::bool);
    }

    /** Delete every message of the queue; returns how many. */
    public long flush(String queue) throws SQLException {
        return decode(new Enc(OP_FLUSH, queue), Dec::i64);
    }

    public Size size(String queue) throws SQLException {
        return decode(new Enc(OP_SIZE, queue), d -> new Size(d.i64(), d.i64()));
    }

    public List<QueueDetail> detail() throws SQLException {
        Dec d = call(new Enc(OP_DETAIL, ""));
        try {
            int n = d.count(20);
            List<QueueDetail> out = new ArrayList<>(n);
            for (int k = 0; k < n; k++) out.add(new QueueDetail(d.str(), d.i64(), d.i64()));
            return out;
        } catch (RuntimeException re) {
            throw Dec.malformed(re);
        }
    }

    // ── framing ───────────────────────────────────────────────────────────

    /**
     * Largest request payload the native wire accepts (its 16 MiB frame cap).
     * A larger request is refused HERE with 54000: the server would otherwise
     * have to drop the connection mid-frame.
     */
    static final int MAX_REQUEST_BYTES = 16 * 1024 * 1024;

    private Dec call(Enc e) throws SQLException {
        byte[] req = e.bytes();
        if (req.length > MAX_REQUEST_BYTES) {
            throw new SQLException("native queue: request of " + req.length + " bytes exceeds the "
                    + MAX_REQUEST_BYTES + "-byte frame limit; send fewer or smaller messages", "54000");
        }
        byte[] resp;
        try {
            resp = conn.executeQueue(req);
        } catch (SQLException se) {
            throw withSqlState(se);
        }
        Dec d = new Dec(resp);
        try {
            if (d.u8() != FMT_V) throw new SQLException("native queue: response fmt_v unknown", "08P01");
            if (d.u8() != e.op) throw new SQLException("native queue: response for another op", "08P01");
        } catch (RuntimeException re) {
            throw Dec.malformed(re);
        }
        return d;
    }

    /** Decode a response body; any malformed content is an 08P01 SQLException, never unchecked. */
    private <T> T decode(Enc e, java.util.function.Function<Dec, T> f) throws SQLException {
        Dec d = call(e);
        try {
            return f.apply(d);
        } catch (RuntimeException re) {
            throw Dec.malformed(re);
        }
    }

    private static boolean bool(Dec d) {
        return d.u8() != 0;
    }

    private static final java.util.regex.Pattern STATE =
            java.util.regex.Pattern.compile("(?:^|\\s)([0-9A-Z]{5}): ");

    /**
     * The server's message carries the SQLSTATE ({@code "25P01: …"}); the
     * frame's numeric code can only carry numeric states, so a non-numeric one
     * (25P01, 0A000, 42P01, 08P01) is taken from the text.
     */
    static SQLException withSqlState(SQLException se) {
        String m = se.getMessage();
        if (m == null) return se;
        java.util.regex.Matcher mt = STATE.matcher(m);
        if (!mt.find()) return se;
        String state = mt.group(1);
        if (state.equals(se.getSQLState())) return se;
        if (state.startsWith("40")) {
            return new java.sql.SQLTransactionRollbackException(m, state, se.getErrorCode(), se);
        }
        return new SQLException(m, state, se.getErrorCode(), se);
    }

    static final class Enc {
        final byte op;
        private ByteBuffer b = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);

        Enc(byte op, String queue) {
            this.op = op;
            u8(FMT_V);
            u8(op);
            str(queue);
        }

        private void room(int n) {
            if (b.remaining() < n) {
                ByteBuffer nb = ByteBuffer.allocate(Math.max(b.capacity() * 2, b.position() + n))
                        .order(ByteOrder.LITTLE_ENDIAN);
                b.flip();
                nb.put(b);
                b = nb;
            }
        }

        Enc u8(int v) { room(1); b.put((byte) v); return this; }
        Enc u32(int v) { room(4); b.putInt(v); return this; }
        Enc i64(long v) { room(8); b.putLong(v); return this; }

        Enc str(String s) {
            byte[] u = s.getBytes(StandardCharsets.UTF_8);
            room(4 + u.length);
            b.putInt(u.length);
            b.put(u);
            return this;
        }

        Enc ostr(String s) {
            if (s == null) return u8(0);
            u8(1);
            return str(s);
        }

        Enc item(String id, long priority, long delayMs, String payload) {
            return str(id).i64(priority).i64(delayMs).ostr(payload);
        }

        byte[] bytes() {
            byte[] out = new byte[b.position()];
            System.arraycopy(b.array(), 0, out, 0, out.length);
            return out;
        }
    }

    static final class Dec {
        private final ByteBuffer b;

        Dec(byte[] p) {
            b = ByteBuffer.wrap(p).order(ByteOrder.LITTLE_ENDIAN);
        }

        int u8() { return b.get() & 0xFF; }
        int u32() { return b.getInt(); }
        long i64() { return b.getLong(); }

        /** A count whose items are at least {@code minItemBytes} each, bounded by what remains. */
        int count(int minItemBytes) {
            int n = b.getInt();
            if (n < 0 || (long) n * minItemBytes > b.remaining()) {
                throw new IllegalArgumentException("count " + n + " exceeds the response");
            }
            return n;
        }

        static SQLException malformed(RuntimeException re) {
            return new SQLException("native queue: malformed response: " + re, "08P01", re);
        }

        String str() {
            int n = b.getInt();
            if (n < 0 || n > b.remaining()) {
                throw new IllegalArgumentException("string length " + n + " exceeds the response");
            }
            byte[] u = new byte[n];
            b.get(u);
            return new String(u, StandardCharsets.UTF_8);
        }

        String ostr() {
            return u8() == 0 ? null : str();
        }
    }
}
