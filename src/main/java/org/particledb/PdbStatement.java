package org.particledb;

import org.particledb.wire.StreamState;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Plain-text {@link Statement} - each call ships a string query on a fresh
 * stream. Use {@link PdbPreparedStatement} for parameter binding.
 */
public class PdbStatement implements Statement {

    final PdbConnection conn;
    final List<String> batch = new ArrayList<>();
    private PdbResultSet currentRs;
    private long updateCount = -1L;
    private boolean closed = false;
    private int queryTimeout;
    private int maxRows = 0;
    private boolean closeOnCompletion;

    public PdbStatement(PdbConnection conn) {
        this.conn = conn;
    }

    /**
     * Every execution path on a CLOSED Statement must raise SQLException (JDBC
     * contract). {@code close()} used to set a flag that nothing consulted, so
     * a use-after-close kept issuing statements on the connection.
     */
    final void ensureOpen() throws SQLException {
        if (closed) throw new SQLException("statement is closed", "HY010");
        if (conn.isClosed()) throw new java.sql.SQLNonTransientConnectionException("connection closed", "08003");
    }

    /** JDBC: a new execution implicitly closes the previous ResultSet. */
    private void closeCurrentResult() {
        PdbResultSet result = currentRs;
        currentRs = null;
        if (result != null) result.close();
    }

    void resultClosed(PdbResultSet result) {
        if (closeOnCompletion && currentRs == result) close();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return executeQueryRendered(sql);
    }

    /**
     * The ONE {@code executeQuery} body. {@link PdbPreparedStatement} routes its
     * no-argument {@code executeQuery()} here instead of calling the connection
     * directly, so a prepared statement records its current result exactly like
     * a plain one - {@code ps.executeQuery(); ps.getResultSet();} used to hand
     * back null, and {@code rs.getStatement()} used to hand back null too.
     */
    /**
     * Set by {@link PdbPreparedStatement} when its template can never be touched
     * by the pg_catalog adapter ({@link PgCatalogAdapter#mayRewrite}), so the
     * connection skips re-checking every rendered execution.
     */
    boolean adaptFree = false;

    /** How one execution reaches the server (text, or a prepared handle). */
    interface Send {
        StreamState.StreamResult run(int timeoutSeconds) throws SQLException;
    }

    /**
     * Run one execution and record the advisories the server sent with it
     * ({@code MSG_NOTICE}, negotiated by default) as this statement's
     * {@link #getWarnings() warnings} -- PostgreSQL's NoticeResponse, which
     * pgjdbc surfaces the same way. A new execution replaces them (JDBC:
     * warnings are cleared when a statement is re-executed).
     */
    private StreamState.StreamResult runNoting(Send send) throws SQLException {
        warnings = null;
        PdbStatement prev = PdbConnection.EXECUTING.get();
        PdbConnection.EXECUTING.set(this);
        StreamState.StreamResult r;
        try {
            r = send.run(queryTimeout);
        } finally {
            PdbConnection.EXECUTING.set(prev);
        }
        for (String[] n : r.notices) {
            SQLWarning w = new SQLWarning(n[0] + ": " + n[2], n[1]);
            if (warnings == null) warnings = w; else warnings.setNextWarning(w);
        }
        return r;
    }

    /** This statement's warnings from its last execution (see {@link #runNoting}). */
    private SQLWarning warnings;

    final ResultSet executeQueryRendered(String sql) throws SQLException {
        return executeQueryWith(t -> conn.executeRaw(sql, t, adaptFree));
    }

    final ResultSet executeQueryWith(Send send) throws SQLException {
        ensureOpen();
        closeCurrentResult();
        currentRs = new PdbResultSet(runNoting(send), conn.resultDecoder());
        currentRs.setOwner(this);
        currentRs.setMaxRows(maxRows);
        updateCount = -1L;
        return currentRs;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return executeUpdateRendered(sql);
    }

    /** The ONE {@code executeUpdate} body - see {@link #executeQueryRendered}. */
    final int executeUpdateRendered(String sql) throws SQLException {
        return executeUpdateWith(t -> conn.executeRaw(sql, t, adaptFree));
    }

    final int executeUpdateWith(Send send) throws SQLException {
        ensureOpen();
        closeCurrentResult();
        StreamState.StreamResult r = runNoting(send);
        if (r.kind == StreamState.StreamResult.Kind.ARROW) {
            // JDBC: executeUpdate must not be used for a row-returning
            // statement. Returning -1 let the rows be silently discarded and
            // the caller read "-1 rows affected" as a successful update.
            currentRs = null;
            updateCount = -1L;
            throw new SQLException(
                    "a result set was returned when an update count was expected"
                    + " — use executeQuery() or execute()", "0100E");
        }
        currentRs = null;
        updateCount = (r.kind == StreamState.StreamResult.Kind.OK) ? r.affectedRows() : 0L;
        return (int) updateCount;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return executeRendered(sql);
    }

    /** The ONE {@code execute} body - see {@link #executeQueryRendered}. */
    final boolean executeRendered(String sql) throws SQLException {
        return executeWith(t -> conn.executeRaw(sql, t, adaptFree));
    }

    final boolean executeWith(Send send) throws SQLException {
        // Classify by the ACTUAL result the server returns, not by a fragile
        // SELECT-prefix guess. The old heuristic routed WITH…SELECT / VALUES /
        // SHOW / EXPLAIN / TABLE / `… RETURNING` (and leading-comment) queries
        // to executeUpdate(), whose row-less update path DISCARDED the Arrow
        // rows (conn.execute returns -1 for an ARROW frame). Running once and
        // switching on the frame kind returns a ResultSet whenever the server
        // sent rows, and an update count otherwise.
        ensureOpen();
        closeCurrentResult();
        StreamState.StreamResult r = runNoting(send);
        if (r.kind == StreamState.StreamResult.Kind.ARROW) {
            currentRs = new PdbResultSet(r, conn.resultDecoder());
            currentRs.setOwner(this);
            currentRs.setMaxRows(maxRows);
            updateCount = -1L;
            return true;
        }
        currentRs = null;
        updateCount = (r.kind == StreamState.StreamResult.Kind.OK) ? r.affectedRows() : 0L;
        return false;
    }

    /**
     * Best-effort "is this SQL a row-returning statement" prefix check. No
     * longer used to route {@link #execute(String)} (that now switches on the
     * real frame kind); retained for callers/tests that still consult it and
     * broadened beyond a bare leading {@code SELECT}.
     */
    static boolean looksLikeSelect(String sql) {
        String s = stripLeadingNoise(sql);
        if (s.isEmpty()) return false;
        for (String kw : ROW_RETURNING_PREFIXES) {
            if (s.regionMatches(true, 0, kw, 0, kw.length())) return true;
        }
        return false;
    }

    private static final String[] ROW_RETURNING_PREFIXES = {
            "SELECT", "WITH", "VALUES", "TABLE", "SHOW", "EXPLAIN", "("
    };

    /** Strip leading whitespace and SQL comments so the keyword check sees code. */
    private static String stripLeadingNoise(String sql) {
        int i = 0, n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);
            if (Character.isWhitespace(c)) { i++; continue; }
            if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                i += 2; while (i < n && sql.charAt(i) != '\n') i++; continue;
            }
            if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                i += 2; while (i + 1 < n && !(sql.charAt(i) == '*' && sql.charAt(i + 1) == '/')) i++;
                i += 2; continue;
            }
            break;
        }
        return i >= n ? "" : sql.substring(i);
    }

    @Override public void addBatch(String sql) throws SQLException {
        ensureOpen();
        batch.add(sql);
    }
    @Override public void clearBatch() { batch.clear(); }
    @Override public int[] executeBatch() throws SQLException {
        ensureOpen();
        if (batch.isEmpty()) return new int[0];
        PdbStatement previous = PdbConnection.EXECUTING.get();
        PdbConnection.EXECUTING.set(this);
        try {
            // Per-statement update counts; a failed statement raises
            // BatchUpdateException (see PdbConnection#executeBatch).
            return conn.executeBatch(batch);
        } finally {
            PdbConnection.EXECUTING.set(previous);
            batch.clear();
        }
    }

    /** Closing a Statement closes its current ResultSet (JDBC contract). */
    @Override public void close() {
        closed = true;
        closeCurrentResult();
    }
    @Override public boolean isClosed() { return closed; }

    @Override public ResultSet getResultSet() throws SQLException {
        ensureOpen();
        return currentRs;
    }
    @Override public int getUpdateCount() throws SQLException {
        ensureOpen();
        return (int) updateCount;
    }
    @Override public boolean getMoreResults() throws SQLException {
        return getMoreResults(CLOSE_CURRENT_RESULT);
    }

    @Override public int getMaxFieldSize() { return 0; }
    @Override public void setMaxFieldSize(int max) throws SQLException {
        ensureOpen();
        if (max < 0) throw new SQLException("maxFieldSize must be >= 0", "22023");
        if (max != 0) throw new java.sql.SQLFeatureNotSupportedException(
                "field truncation is not supported", "0A000");
    }
    /**
     * {@code setMaxRows} is NOT a hint - the JDBC contract is that any row past
     * the limit is silently DROPPED. A no-op setter meant a caller that capped a
     * query at 10 rows received every row the table had.
     */
    @Override public int getMaxRows() { return maxRows; }
    @Override public void setMaxRows(int max) throws SQLException {
        if (max < 0) throw new SQLException("maxRows must be >= 0");
        this.maxRows = max;
    }
    @Override public void setEscapeProcessing(boolean enable) {}

    @Override public int getQueryTimeout() { return queryTimeout; }
    /**
     * Honoured: the value is the per-statement wire wait, and 0 means "no
     * limit" per the JDBC contract. It used to be stored and ignored while
     * every statement waited a hardcoded 60s, so a caller that asked for a
     * 2-second cap blocked for 60.
     */
    @Override public void setQueryTimeout(int seconds) throws SQLException {
        if (seconds < 0) throw new SQLException("queryTimeout must be >= 0");
        this.queryTimeout = seconds;
    }

    /**
     * Cancel the statement this object is running: the driver resets its
     * stream ({@code RST_STREAM(CANCEL)}), the server ends it at its next
     * interrupt check, and the waiting {@code execute*} call fails with
     * SQLSTATE 57014 while the connection stays usable -- PostgreSQL's
     * CancelRequest, scoped to one statement of a multiplexed connection.
     * (This used to refuse: the native wire had no way to cancel.)
     */
    @Override public void cancel() throws SQLException {
        ensureOpen();
        java.util.concurrent.Future<StreamState.StreamResult> f = inflight;
        // Nothing running: nothing to cancel (pgjdbc: a no-op too).
        if (f != null) conn.cancelRequest(f);
    }

    /**
     * The request this statement is waiting on, published by
     * {@code PdbConnection.await} for {@link #cancel()} (called from another
     * thread); null between executions.
     */
    volatile java.util.concurrent.Future<StreamState.StreamResult> inflight;
    @Override public SQLWarning getWarnings() throws SQLException { ensureOpen(); return warnings; }
    @Override public void clearWarnings() { warnings = null; }
    @Override public void setCursorName(String name) {}
    @Override public int getFetchDirection() { return ResultSet.FETCH_FORWARD; }
    @Override public void setFetchDirection(int dir) {}
    /**
     * JDBC's streaming knob (pgjdbc streams only when it is set): with
     * {@code rows > 0} a chunked result is handed over as its chunks arrive and
     * decoded one record batch at a time ({@link StreamedRows}), so client
     * memory is bounded by a batch rather than by the result. The value is a
     * switch, not a row count: the server's record batches are the unit. The
     * result set is then forward-only. 0 (the default) keeps the whole result,
     * fully scrollable.
     */
    private volatile int fetchSize;
    @Override public int getFetchSize() { return fetchSize; }
    @Override public void setFetchSize(int rows) throws SQLException {
        if (rows < 0) throw new SQLException("fetch size must be >= 0", "22023");
        fetchSize = rows;
    }

    /** Hand row-returning results over as they stream ({@link #setFetchSize}). */
    boolean streamsResults() { return fetchSize > 0; }
    @Override public int getResultSetConcurrency() { return ResultSet.CONCUR_READ_ONLY; }
    @Override public int getResultSetType() { return ResultSet.TYPE_FORWARD_ONLY; }
    @Override public Connection getConnection() { return conn; }
    @Override public boolean getMoreResults(int current) throws SQLException {
        ensureOpen();
        // A PdbStatement produces exactly one result per execute() (one Arrow
        // frame OR one command-complete). "Move to the next result" therefore
        // always advances into the exhausted state: no next ResultSet, and a
        // subsequent getUpdateCount() must return -1 per the JDBC contract
        //   "there are no more results when
        //    ((getMoreResults() == false) && (getUpdateCount() == -1))".
        // Leaving updateCount pinned at 0 (the old no-op stub) makes the
        // standard drain loop used by Flyway/JdbcTemplate spin forever,
        // accumulating a Result per iteration until the client OOMs.
        if (current != KEEP_CURRENT_RESULT && currentRs != null) {
            currentRs.close();
        }
        currentRs = null;
        updateCount = -1L;
        return false;
    }
    @Override public ResultSet getGeneratedKeys() throws SQLException {
        ensureOpen();
        return PdbResultSet.empty();
    }
    @Override public int executeUpdate(String sql, int autoGen) throws SQLException { return executeUpdate(sql); }
    @Override public int executeUpdate(String sql, int[] colIdx) throws SQLException { return executeUpdate(sql); }
    @Override public int executeUpdate(String sql, String[] colNames) throws SQLException { return executeUpdate(sql); }
    @Override public boolean execute(String sql, int autoGen) throws SQLException { return execute(sql); }
    @Override public boolean execute(String sql, int[] colIdx) throws SQLException { return execute(sql); }
    @Override public boolean execute(String sql, String[] colNames) throws SQLException { return execute(sql); }
    @Override public int getResultSetHoldability() { return 1; }
    @Override public void setPoolable(boolean b) {}
    @Override public boolean isPoolable() { return false; }
    @Override public void closeOnCompletion() throws SQLException { ensureOpen(); closeOnCompletion = true; }
    @Override public boolean isCloseOnCompletion() throws SQLException { ensureOpen(); return closeOnCompletion; }

    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("not a wrapper for " + iface);
    }
    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }
}
