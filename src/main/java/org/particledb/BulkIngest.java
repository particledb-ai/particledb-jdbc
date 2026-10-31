package org.particledb;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Future;

import org.particledb.wire.StreamState;
import org.particledb.wire.WireV2Codec;

/**
 * One native bulk ingest ({@code F_BULK}, {@code PROTOCOL.md}
 * §5) on one connection: BEGIN (awaited), DATA frames (each ONE complete Arrow
 * IPC stream of at most {@link #MAX_DATA_BYTES}, pipelined up to
 * {@link #WINDOW} in flight), END carrying the DATA count. Any DATA failure
 * drains the in-flight frames, sends ABORT and raises the server's error.
 *
 * <p>Also home of the INSERT-shape detector behind
 * {@code PdbPreparedStatement.executeBatch}'s bulk path ({@link Insert#parse})
 * and the row conversion it uses ({@link #rowsToText}, sent through the
 * driver's own heap-only {@link ArrowIpcWriter}, so that path needs neither
 * Apache Arrow Java nor a JVM flag). Loads of caller-supplied Arrow Java
 * vectors are serialized by {@link ArrowBulk}, the only bulk code that links
 * Arrow Java (an optional dependency the caller already has).
 */
final class BulkIngest {
    /** Largest DATA IPC payload this driver sends (the frame cap is 16 MiB). */
    static final int MAX_DATA_BYTES = 12 * 1024 * 1024;
    /** Buffer bytes a DATA frame is sized for before serialization (re-split if larger). */
    static final long TARGET_DATA_BYTES = 8L * 1024 * 1024;
    /** DATA frames in flight. */
    static final int WINDOW = 4;
    /** Per-request wait (seconds); a load may run long, END waits for every DATA. */
    static final int TIMEOUT_SECONDS = 900;

    /** Thrown by BEGIN when the server does not know {@code F_BULK} (ERROR code 2). */
    static final class Unsupported extends SQLException {
        Unsupported(String msg, Throwable cause) {
            super(msg, "0A000", cause);
        }
    }

    /** The data a load sends once BEGIN was answered. */
    interface Body {
        void run(BulkIngest ingest) throws Exception;
    }

    private final PdbConnection conn;
    private final Deque<Future<StreamState.StreamResult>> inflight = new ArrayDeque<>();
    private long dataSent;

    private BulkIngest(PdbConnection conn) {
        this.conn = conn;
    }

    /**
     * Run one load: BEGIN, {@code body} (which calls {@link #send}), END.
     * Returns the rows the server loaded.
     */
    static long run(PdbConnection conn, String relation, List<String> columns, long commitEveryRows,
                     Body body) throws SQLException {
        BulkIngest b = new BulkIngest(conn);
        byte[] begin;
        try {
            begin = WireV2Codec.encodeBulkBegin(relation, columns, commitEveryRows);
        } catch (IllegalArgumentException e) {
            throw new SQLException("bulk ingest: " + e.getMessage(), "22023", e);
        }
        try {
            conn.awaitBulk(conn.mux().bulk(begin), TIMEOUT_SECONDS);
        } catch (Exception e) {
            StreamState.PdbWireException w = wireCause(e);
            if (w != null && w.code == 2) {
                throw new Unsupported("this server has no native bulk ingest (F_BULK unsupported)", w);
            }
            throw toSql(e);
        }
        try {
            body.run(b);
        } catch (Exception e) {
            throw b.abort(e);
        }
        // END is pipelined behind the DATA frames: the server waits for every
        // DATA it counts before finishing.
        Future<StreamState.StreamResult> end;
        try {
            end = conn.mux().bulk(WireV2Codec.encodeBulkEnd(b.dataSent));
        } catch (Exception e) {
            throw b.abort(e);
        }
        Exception dataFailure = null;
        while (!b.inflight.isEmpty()) {
            try {
                conn.awaitBulk(b.inflight.poll(), TIMEOUT_SECONDS);
            } catch (Exception e) {
                if (dataFailure == null) dataFailure = e;
                if (!serverAnswered(e)) break;
            }
        }
        if (dataFailure != null) {
            b.inflight.clear();
            try {
                conn.awaitBulk(end, TIMEOUT_SECONDS);
            } catch (Exception ignored) {
                // the END answers the same failure; the DATA error is the one raised
            }
            throw b.abort(dataFailure);
        }
        try {
            return conn.awaitBulk(end, TIMEOUT_SECONDS).affectedRows();
        } catch (Exception e) {
            throw toSql(e);
        }
    }

    /**
     * Send text rows (column-major UTF-8, null for SQL NULL) as Utf8 columns
     * named {@code names}, split so every DATA payload stays within
     * {@link #MAX_DATA_BYTES}.
     */
    void sendText(List<String> names, byte[][][] cols, int rows) throws Exception {
        if (rows == 0) return;
        long bytes = 0;
        for (byte[][] col : cols) {
            bytes += 8L * rows;
            for (int r = 0; r < rows; r++) if (col[r] != null) bytes += col[r].length;
        }
        int per = bytes <= TARGET_DATA_BYTES ? rows : (int) Math.max(1L, rows * TARGET_DATA_BYTES / bytes);
        for (int at = 0; at < rows; at += per) {
            sendTextRange(names, cols, at, Math.min(per, rows - at));
        }
    }

    private void sendTextRange(List<String> names, byte[][][] cols, int start, int len) throws Exception {
        byte[] payload = serializeText(names, cols, start, len);
        if (payload.length > MAX_DATA_BYTES) {
            if (len > 1) {
                int h = len / 2;
                sendTextRange(names, cols, start, h);
                sendTextRange(names, cols, start + h, len - h);
                return;
            }
            throw new SQLException("bulk ingest: one row encodes to " + payload.length
                    + " bytes, over the " + MAX_DATA_BYTES + "-byte DATA limit", "54000");
        }
        sendData(payload);
    }

    /** {@code [BULK_DATA]} + one Arrow IPC stream (schema, one batch, EOS) of text rows [start, start+len). */
    static byte[] serializeText(List<String> names, byte[][][] cols, int start, int len) {
        return ArrowIpcWriter.utf8Stream(new byte[] {WireV2Codec.BULK_DATA}, names, cols, start, len);
    }

    void sendData(byte[] payload) throws Exception {
        while (inflight.size() >= WINDOW) {
            conn.awaitBulk(inflight.poll(), TIMEOUT_SECONDS);
        }
        dataSent++;
        inflight.add(conn.mux().bulk(payload));
    }

    /**
     * The load failed with {@code cause}: drain the DATA still in flight,
     * send ABORT (the server drops the session; a no-op when END already took
     * it), and return the exception to raise.
     */
    private SQLException abort(Exception cause) {
        SQLException se = toSql(cause);
        if (conn.isClosed()) return se;
        boolean usable = serverAnswered(cause) || cause instanceof SQLException;
        while (usable && !inflight.isEmpty()) {
            try {
                conn.awaitBulk(inflight.poll(), TIMEOUT_SECONDS);
            } catch (Exception e) {
                if (!serverAnswered(e)) usable = false;
            }
        }
        inflight.clear();
        if (usable) {
            try {
                conn.awaitBulk(conn.mux().bulk(new byte[] { WireV2Codec.BULK_ABORT }), TIMEOUT_SECONDS);
            } catch (Exception e) {
                se.addSuppressed(e);
            }
        }
        return se;
    }

    private static boolean serverAnswered(Exception e) {
        return PdbConnection.serverAnswered(e);
    }

    static StreamState.PdbWireException wireCause(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof StreamState.PdbWireException) return (StreamState.PdbWireException) t;
            if (t.getCause() == t) break;
        }
        return null;
    }

    static SQLException toSql(Exception e) {
        if (e instanceof SQLException) return (SQLException) e;
        if (e instanceof java.util.concurrent.TimeoutException) {
            return new java.sql.SQLTimeoutException("bulk ingest request timed out after "
                    + TIMEOUT_SECONDS + "s", "57014", e);
        }
        return PdbConnection.mapException(e);
    }

    // ── executeBatch: the INSERT shape and the row conversion ─────────────

    /**
     * A plain {@code INSERT INTO <table> [(<cols>)] VALUES (?, ..., ?)}: one
     * VALUES tuple, every value a bare {@code ?}, nothing after it but an
     * optional {@code ;}. Anything else (comments, ON CONFLICT, RETURNING,
     * SELECT, expressions, DEFAULT, several tuples) is not this shape.
     */
    static final class Insert {
        /** The relation as written (quoted parts kept), for BEGIN and the metadata query. */
        final String relation;
        /** Target columns (unquoted folded to lower case), or null when none were listed. */
        final List<String> columns;
        /** Number of {@code ?} values. */
        final int values;

        Insert(String relation, List<String> columns, int values) {
            this.relation = relation;
            this.columns = columns;
            this.values = values;
        }

        static Insert parse(String sql) {
            if (sql == null) return null;
            Lexer lx = new Lexer(sql);
            if (!lx.keyword("insert") || !lx.keyword("into")) return null;
            int relStart = lx.skipWs();
            List<String> parts = new ArrayList<>();
            String p = lx.ident();
            if (p == null) return null;
            parts.add(p);
            while (lx.peek() == '.') {
                lx.pos++;
                p = lx.ident();
                if (p == null) return null;
                parts.add(p);
            }
            String relation = sql.substring(relStart, lx.pos);
            if (parts.size() > 3) return null;
            List<String> cols = null;
            lx.skipWs();
            if (lx.peek() == '(') {
                lx.pos++;
                cols = new ArrayList<>();
                while (true) {
                    lx.skipWs();
                    String c = lx.ident();
                    if (c == null) return null;
                    cols.add(c);
                    lx.skipWs();
                    char ch = lx.peek();
                    lx.pos++;
                    if (ch == ')') break;
                    if (ch != ',') return null;
                }
            }
            if (!lx.keyword("values")) return null;
            lx.skipWs();
            if (lx.peek() != '(') return null;
            lx.pos++;
            int n = 0;
            while (true) {
                lx.skipWs();
                if (lx.peek() != '?') return null;
                lx.pos++;
                // `??` is the escaped operator, not a placeholder.
                if (lx.peek() == '?') return null;
                n++;
                lx.skipWs();
                char ch = lx.peek();
                lx.pos++;
                if (ch == ')') break;
                if (ch != ',') return null;
            }
            lx.skipWs();
            while (lx.peek() == ';') {
                lx.pos++;
                lx.skipWs();
            }
            if (lx.pos < sql.length()) return null;
            if (cols != null && cols.size() != n) return null;
            if (cols != null) {
                for (int i = 0; i < cols.size(); i++) {
                    for (int j = i + 1; j < cols.size(); j++) {
                        if (cols.get(i).equals(cols.get(j))) return null;
                    }
                }
            }
            return new Insert(relation.trim(), cols, n);
        }
    }

    private static final class Lexer {
        final String s;
        int pos;

        Lexer(String s) {
            this.s = s;
        }

        char peek() {
            return pos < s.length() ? s.charAt(pos) : '\0';
        }

        int skipWs() {
            while (pos < s.length() && Character.isWhitespace(s.charAt(pos))) pos++;
            return pos;
        }

        /** Consume keyword {@code kw} (case-insensitive) followed by a non-identifier char. */
        boolean keyword(String kw) {
            skipWs();
            int end = pos + kw.length();
            if (end > s.length() || !s.regionMatches(true, pos, kw, 0, kw.length())) return false;
            if (end < s.length() && isIdentPart(s.charAt(end))) return false;
            pos = end;
            return true;
        }

        /** An identifier: unquoted (folded to lower case) or {@code "quoted"} (kept, {@code ""} unescaped). */
        String ident() {
            if (pos >= s.length()) return null;
            char c = s.charAt(pos);
            if (c == '"') {
                StringBuilder sb = new StringBuilder();
                int i = pos + 1;
                while (i < s.length()) {
                    char d = s.charAt(i);
                    if (d == '"') {
                        if (i + 1 < s.length() && s.charAt(i + 1) == '"') {
                            sb.append('"');
                            i += 2;
                            continue;
                        }
                        if (sb.length() == 0) return null;
                        pos = i + 1;
                        return sb.toString();
                    }
                    sb.append(d);
                    i++;
                }
                return null;
            }
            if (!(Character.isLetter(c) || c == '_')) return null;
            int start = pos;
            while (pos < s.length() && (isIdentPart(s.charAt(pos)) || s.charAt(pos) == '$')) pos++;
            String id = s.substring(start, pos).toLowerCase(Locale.ROOT);
            // A reserved word where a name belongs is a different statement shape.
            switch (id) {
                case "values": case "select": case "default": case "on": case "returning":
                case "overriding": case "as": case "with":
                    return null;
                default:
                    return id;
            }
        }

        private static boolean isIdentPart(char c) {
            return Character.isLetterOrDigit(c) || c == '_';
        }
    }

    /** Value not representable as text for a bulk load: the batch takes the text path. */
    static final Object UNSUPPORTED = new Object();

    /**
     * The PostgreSQL text form of a bound value, as the column's input
     * function reads it; null for SQL NULL; {@link #UNSUPPORTED} for a value
     * the bulk path does not carry (arrays, unknown objects).
     */
    static Object textOf(Object v) {
        if (v == null) return null;
        if (v instanceof String) return v;
        if (v instanceof BigDecimal) return ((BigDecimal) v).toPlainString();
        if (v instanceof Number || v instanceof Boolean || v instanceof Character) return v.toString();
        if (v instanceof byte[]) {
            byte[] b = (byte[]) v;
            StringBuilder sb = new StringBuilder(2 + 2 * b.length).append("\\x");
            for (byte x : b) {
                sb.append(Character.forDigit((x >> 4) & 0xF, 16)).append(Character.forDigit(x & 0xF, 16));
            }
            return sb.toString();
        }
        if (v instanceof java.sql.Timestamp || v instanceof java.sql.Date || v instanceof java.sql.Time
                || v instanceof java.util.UUID || v instanceof java.time.temporal.TemporalAccessor) {
            return v.toString();
        }
        return UNSUPPORTED;
    }

    /**
     * Rows of bound values as column-major UTF-8 text ({@link #textOf}), one
     * array per column of {@code cols}; null when some value is unsupported.
     */
    static byte[][][] rowsToText(List<String> cols, List<Object[]> rows) {
        int nc = cols.size();
        int nr = rows.size();
        byte[][][] text = new byte[nc][nr][];
        for (int r = 0; r < nr; r++) {
            Object[] row = rows.get(r);
            for (int c = 0; c < nc; c++) {
                Object t = textOf(c < row.length ? row[c] : null);
                if (t == UNSUPPORTED) return null;
                text[c][r] = t == null ? null : ((String) t).getBytes(StandardCharsets.UTF_8);
            }
        }
        return text;
    }
}
