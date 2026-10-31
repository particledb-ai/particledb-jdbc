package org.particledb;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.particledb.wire.ArrayParams;
import org.particledb.wire.StreamState;
import org.particledb.wire.WireV2Codec;

/**
 * JDBC {@link PreparedStatement}. Each execution runs on its own multiplexed
 * stream, so simultaneous executions on the same {@code Connection} do not
 * block each other.
 *
 * <p>When the server serves {@code CAP_PREPARED}, an execution whose values the
 * native wire carries is sent by server-side handle (PREPARE once per
 * connection, then EXECUTE with typed values); otherwise the values are
 * rendered into the SQL text client-side. An execution with an ARRAY
 * parameter ({@link #setArray}, or {@code setObject} of a Java array /
 * {@code Collection}) is only ever sent by handle, the array as a typed
 * EXECUTE value ({@link ArrayParams}). When the URL set
 * {@code serverPrepare=false}, or the server does not accept bound arrays (no
 * server-side prepare, or it did not echo {@code CAP_EXT_ARRAY_PARAMS}), the
 * array is rendered as its typed PG literal ({@code '{1,2,3}'::int4[]});
 * every other reason it cannot be bound raises
 * {@link SQLFeatureNotSupportedException}.
 */
public class PdbPreparedStatement extends PdbStatement implements PreparedStatement {

    private final String sqlTemplate;
    private final Object[] params;
    /**
     * Offsets of the template's placeholders and {@code ??} escapes, found ONCE.
     * The template never changes, and re-scanning it on every execute (a
     * character-by-character lexer plus two boxed lists) was the single largest
     * item in the driver's per-statement profile (JFR, 32 threads).
     */
    private final int[] placeholderAt;
    private final int[] escapeAt;
    private final List<String> pendingBatch = new ArrayList<>();
    /**
     * Bound values of batched rows kept for a native bulk ingest
     * ({@link #bulkInsert}); rendered to {@link #pendingBatch} text only if
     * the batch falls back to the text path.
     */
    private final List<Object[]> pendingRows = new ArrayList<>();
    private final BulkIngest.Insert bulkInsert;
    /**
     * The template with each placeholder spelled {@code $1..$N}, when it may
     * run by server-side handle ({@link PdbConnection#executeServerPrepared});
     * null when it must stay on the text path: a {@code ??} escape, a template
     * the pg_catalog adapter may rewrite, DDL (the driver's autocommit
     * bracketing applies to it), or text that already contains {@code $<digit>}
     * (which the server would read as a placeholder).
     */
    private final String serverSql;

    /**
     * Everything derived from the SQL text alone, computed once per text per
     * connection ({@link PdbConnection#template}): Conductor-style DAOs call
     * {@code prepareStatement(sql)} for EVERY execution, so deriving this per
     * statement object re-scanned the same text on every call.
     */
    static final class Template {
        final int[] placeholderAt;
        final int[] escapeAt;
        final boolean adaptFree;
        final String serverSql;
        /** Why {@link #serverSql} is null (for the array-parameter refusal), else null. */
        final String noServerSqlReason;
        /** The plain-INSERT shape {@code executeBatch} may bulk-load, or null. */
        final BulkIngest.Insert bulkInsert;

        Template(String sql) {
            List<Integer> pos = new ArrayList<>();
            List<Integer> esc = new ArrayList<>();
            scan(sql, pos, esc);
            this.placeholderAt = toArray(pos);
            this.escapeAt = toArray(esc);
            this.adaptFree = !PgCatalogAdapter.mayRewrite(sql);
            String why = null;
            if (!adaptFree) why = "it names a pg_catalog object the driver rewrites";
            else if (escapeAt.length > 0) why = "it contains a ?? escape";
            else if (PdbConnection.looksLikeDdl(sql)) why = "it is DDL";
            else if (hasDollarDigit(sql)) why = "its text contains $<digit>";
            this.noServerSqlReason = why;
            this.serverSql = why == null ? toDollarPlaceholders(sql, placeholderAt) : null;
            BulkIngest.Insert ins = (adaptFree && escapeAt.length == 0) ? BulkIngest.Insert.parse(sql) : null;
            this.bulkInsert = (ins != null && ins.values == placeholderAt.length) ? ins : null;
        }
    }

    /** The last execution ran by server-side handle (PREPARE/EXECUTE with bound values). */
    private volatile boolean lastServerPrepared;
    private final java.util.BitSet assigned = new java.util.BitSet();

    /**
     * True when this statement's most recent execution ran as a server-side
     * prepared statement -- values bound on the wire, never rendered into SQL
     * text. False before the first execution, and for an execution that fell
     * back to text (a value the native wire cannot carry, {@code serverPrepare=false},
     * DDL, ...).
     */
    public boolean isServerPrepared() {
        return lastServerPrepared;
    }

    public PdbPreparedStatement(PdbConnection conn, String sqlTemplate) {
        super(conn);
        this.sqlTemplate = sqlTemplate;
        Template t = conn.template(sqlTemplate);
        this.placeholderAt = t.placeholderAt;
        this.escapeAt = t.escapeAt;
        // Size the parameter array to the count of REAL bind placeholders - a
        // '?' that sits inside a string/identifier/dollar-quoted body or a SQL
        // comment is not a placeholder. The count MUST agree with what
        // interpolate() substitutes, else setXxx() beyond the count throws or a
        // real parameter is left unbound. Both come from the same scan.
        this.params = new Object[Math.max(placeholderAt.length, 1)];
        this.adaptFree = t.adaptFree;
        this.serverSql = t.serverSql;
        this.noServerSqlReason = t.noServerSqlReason;
        this.bulkInsert = t.bulkInsert;
    }

    private final String noServerSqlReason;

    private static boolean hasDollarDigit(String sql) {
        for (int i = sql.indexOf('$'); i >= 0 && i + 1 < sql.length(); i = sql.indexOf('$', i + 1)) {
            if (Character.isDigit(sql.charAt(i + 1))) return true;
        }
        return false;
    }

    static String toDollarPlaceholders(String sql, int[] positions) {
        if (positions.length == 0) return sql;
        StringBuilder sb = new StringBuilder(sql.length() + 2 * positions.length);
        int prev = 0;
        for (int k = 0; k < positions.length; k++) {
            sb.append(sql, prev, positions[k]);
            // `E?` / `x?`: the server reads `E$1` as text, not a placeholder.
            if (positions[k] > 0 && isIdentPart(sql.charAt(positions[k] - 1))) sb.append(' ');
            sb.append('$').append(k + 1);
            prev = positions[k] + 1;
        }
        return sb.append(sql, prev, sql.length()).toString();
    }

    /**
     * Send one execution: by server-side handle when the template and every
     * bound value allow it, else as rendered text (the path every statement
     * used before).
     */
    private StreamState.StreamResult send(int timeoutSeconds) throws SQLException {
        requireParameters();
        int n = placeholderAt.length;
        lastServerPrepared = false;
        if (hasArray(params, n)) {
            // Text when the URL said serverPrepare=false, or when the server
            // cannot take a bound array (no CAP_PREPARED, or no
            // CAP_EXT_ARRAY_PARAMS echoed): the typed PG literal
            // ('{1,2,3}'::int4[]) stores the same value the bound array would.
            if (!arrayBindingAvailable()) return conn.executeRaw(renderSql(), timeoutSeconds, adaptFree);
            return sendBound(wireParams(params), timeoutSeconds);
        }
        if (serverSql != null && conn.serverPrepareEnabled()
                && WireV2Codec.bindable(params, n, conn.typedParamsEnabled())) {
            StreamState.StreamResult r = conn.executeServerPrepared(serverSql, params, n, timeoutSeconds);
            if (r != null) {
                conn.preparedCache().recordExecution(sqlTemplate);
                lastServerPrepared = true;
                return r;
            }
        }
        return conn.executeRaw(renderSql(), timeoutSeconds, adaptFree);
    }

    /**
     * An array parameter goes by handle (tag 17): the URL left server-side
     * prepare on AND the server accepts bound arrays. Otherwise it is
     * rendered as its typed literal -- a server that lacks the capability is
     * the one refusal that falls back; every other reason an array cannot be
     * bound still raises (see {@link #wireParams}).
     */
    private boolean arrayBindingAvailable() {
        return conn.serverPrepareRequested() && conn.arrayParamsEnabled();
    }

    /** Any bound value among the first {@code n} an array ({@link ArrayParams.Shape})? */
    static boolean hasArray(Object[] row, int n) {
        for (int i = 0; i < n && i < row.length; i++) if (row[i] instanceof ArrayParams.Shape) return true;
        return false;
    }

    /**
     * {@code row} as EXECUTE values: each array in its tag-17 form. Called
     * only when {@link #arrayBindingAvailable} (a server without the
     * capability, or {@code serverPrepare=false}, renders the typed literal
     * instead); every other reason it cannot be bound raises
     * {@link SQLFeatureNotSupportedException} naming that reason.
     */
    private Object[] wireParams(Object[] row) throws SQLException {
        int n = placeholderAt.length;
        if (!arrayBindingAvailable()) {
            // Callers render the literal first; reaching here is a driver bug.
            throw new IllegalStateException("array parameters bound on a connection that cannot bind them");
        }
        if (serverSql == null) {
            throw new SQLFeatureNotSupportedException("this statement cannot be executed by server-side handle ("
                    + noServerSqlReason + "), so its array parameter cannot be bound; set serverPrepare=false"
                    + " to render arrays as SQL text", "0A000");
        }
        Object[] wire = new Object[n];
        for (int i = 0; i < n; i++) {
            Object v = row[i];
            wire[i] = v instanceof ArrayParams.Shape ? ((ArrayParams.Shape) v).encode() : v;
        }
        boolean typed = conn.typedParamsEnabled();
        for (int i = 0; i < n; i++) {
            if (!WireV2Codec.bindable(new Object[] {wire[i]}, 1, typed, true)) {
                Object v = wire[i];
                throw new SQLFeatureNotSupportedException("parameter " + (i + 1) + " ("
                        + v.getClass().getName() + " " + v + ") cannot be bound by the native wire, and the"
                        + " statement carries an array parameter, which is never rendered as text; bind it as"
                        + " another type (java.sql.Timestamp for a LocalDateTime, BigDecimal without an exponent,"
                        + " ...), or set serverPrepare=false to render the statement as SQL text", "0A000");
            }
        }
        return wire;
    }

    private StreamState.StreamResult sendBound(Object[] wire, int timeoutSeconds) throws SQLException {
        StreamState.StreamResult r = conn.executeServerPrepared(serverSql, wire, placeholderAt.length,
                timeoutSeconds, true);
        conn.preparedCache().recordExecution(sqlTemplate);
        lastServerPrepared = true;
        return r;
    }

    private String renderSql() {
        // record execution for cache analytics
        conn.preparedCache().recordExecution(sqlTemplate);
        return interpolate(sqlTemplate, placeholderAt, escapeAt, params);
    }

    private static int[] toArray(List<Integer> l) {
        int[] a = new int[l.size()];
        for (int i = 0; i < a.length; i++) a[i] = l.get(i);
        return a;
    }

    static String interpolate(String sql, Object[] params) {
        List<Integer> positions = new ArrayList<>();
        List<Integer> escapes = new ArrayList<>();
        scan(sql, positions, escapes);
        return interpolate(sql, toArray(positions), toArray(escapes), params);
    }

    static String interpolate(String sql, int[] positions, int[] escapes, Object[] params) {
        // Escapes must still be rewritten even with no params bound, otherwise a
        // literal-'?' operator would reach the server as '??'.
        if (positions.length == 0 && escapes.length == 0) return sql;
        int extra = 0;
        if (params != null) {
            for (int k = 0; k < params.length && k < positions.length; k++) {
                Object v = params[k];
                extra += v instanceof String ? ((String) v).length() + 8 : 24;
            }
        }
        StringBuilder sb = new StringBuilder(sql.length() + extra);
        int prev = 0, pi = 0, ip = 0, ie = 0;
        while (ip < positions.length || ie < escapes.length) {
            boolean placeholderNext;
            if (ip >= positions.length) placeholderNext = false;
            else if (ie >= escapes.length) placeholderNext = true;
            else placeholderNext = positions[ip] < escapes[ie];

            if (placeholderNext) {
                int p = positions[ip++];
                sb.append(sql, prev, p);
                // A placeholder glued to an identifier character gets a space:
                // `E?` must not become the ESCAPE string `E'…'`, where a
                // backslash in the value would escape the closing quote.
                if (p > 0 && isIdentPart(sql.charAt(p - 1))) sb.append(' ');
                if (params != null && pi < params.length) appendLiteral(sb, params[pi++]);
                else sb.append('?'); // more placeholders than params — leave literal
                prev = p + 1;
            } else {
                int e = escapes[ie++];
                sb.append(sql, prev, e);
                sb.append('?'); // '??' collapses to the literal operator '?'
                prev = e + 2;
            }
        }
        sb.append(sql, prev, sql.length());
        return sb.toString();
    }

    /**
     * Char offsets of top-level {@code ?} bind placeholders - i.e. those NOT
     * inside a single-quoted string, a double-quoted identifier, a
     * dollar-quoted body ({@code $tag$ … $tag$}), a {@code --} line comment, or
     * a {@code /* *}{@code /} block comment (PG block comments nest). Shared by
     * the constructor's slot count and interpolate() so they can never drift.
     */
    static List<Integer> placeholderPositions(String sql) {
        List<Integer> pos = new ArrayList<>();
        scan(sql, pos, null);
        return pos;
    }

    /**
     * The single scanner behind both {@link #placeholderPositions} and
     * {@link #interpolate}. Appends real bind-placeholder offsets to
     * {@code placeholders}, and the offset of the first {@code ?} of each
     * {@code ??} escape to {@code escapes} (either list may be null). One
     * scanner means the slot count and the substitution can never disagree
     * about what a given {@code ?} means.
     */
    private static void scan(String sql, List<Integer> placeholders, List<Integer> escapes) {
        scan(sql, placeholders, escapes, null);
    }

    /**
     * True when {@code sql} holds more than one statement: a {@code ;} outside
     * every literal, quoted identifier, dollar-quoted body and comment, followed
     * by anything but whitespace, comments and further {@code ;}. Uses the same
     * scanner as placeholder detection, so a {@code ;} inside a string or a
     * function body never counts.
     */
    static boolean isMultiStatement(String sql) {
        if (sql == null || sql.indexOf(';') < 0) return false;
        List<Integer> semis = new ArrayList<>();
        scan(sql, null, null, semis);
        int n = sql.length();
        for (int at : semis) {
            int i = at + 1;
            while (i < n) {
                char c = sql.charAt(i);
                if (Character.isWhitespace(c) || c == ';') { i++; continue; }
                if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                    while (i < n && sql.charAt(i) != '\n') i++;
                    continue;
                }
                if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                    int depth = 1; i += 2;
                    while (i < n && depth > 0) {
                        if (i + 1 < n && sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') { depth++; i += 2; }
                        else if (i + 1 < n && sql.charAt(i) == '*' && sql.charAt(i + 1) == '/') { depth--; i += 2; }
                        else i++;
                    }
                    continue;
                }
                return true;
            }
        }
        return false;
    }

    private static void scan(String sql, List<Integer> placeholders, List<Integer> escapes,
                             List<Integer> semicolons) {
        int i = 0, n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);
            // line comment: -- … <eol>
            if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                i += 2;
                while (i < n && sql.charAt(i) != '\n') i++;
                continue;
            }
            // block comment: /* … */ (nesting)
            if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                int depth = 1; i += 2;
                while (i < n && depth > 0) {
                    if (i + 1 < n && sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') { depth++; i += 2; }
                    else if (i + 1 < n && sql.charAt(i) == '*' && sql.charAt(i + 1) == '/') { depth--; i += 2; }
                    else i++;
                }
                continue;
            }
            // single-quoted string ('' escapes an embedded quote). If it is a
            // PostgreSQL escape string E'…' (an `E`/`e` token immediately before
            // the quote), a backslash also escapes the next char - so E'a\'b?'
            // keeps the ? inside the literal instead of ending the string early.
            if (c == '\'') {
                boolean eString = i > 0 && (sql.charAt(i - 1) == 'E' || sql.charAt(i - 1) == 'e')
                        && (i < 2 || !isIdentPart(sql.charAt(i - 2)));
                i++;
                while (i < n) {
                    char d = sql.charAt(i);
                    if (eString && d == '\\' && i + 1 < n) { i += 2; continue; }
                    if (d == '\'') {
                        if (i + 1 < n && sql.charAt(i + 1) == '\'') { i += 2; continue; }
                        i++; break;
                    }
                    i++;
                }
                continue;
            }
            // double-quoted identifier ("" escapes an embedded quote)
            if (c == '"') {
                i++;
                while (i < n) {
                    if (sql.charAt(i) == '"') {
                        if (i + 1 < n && sql.charAt(i + 1) == '"') { i += 2; continue; }
                        i++; break;
                    }
                    i++;
                }
                continue;
            }
            // dollar-quoted string: $tag$ … $tag$ (tag empty or an identifier)
            if (c == '$') {
                int tagClose = dollarTagClose(sql, i);
                if (tagClose > i) {
                    String tag = sql.substring(i, tagClose + 1); // e.g. "$$" or "$q$"
                    int end = sql.indexOf(tag, tagClose + 1);
                    i = (end < 0) ? n : end + tag.length();
                    continue;
                }
                i++;
                continue;
            }
            if (c == '?') {
                // '??' is the JDBC escape for a LITERAL '?' operator - not two
                // placeholders. PostgreSQL spells jsonb/hstore key-existence
                // '?', '?|' and '?&', so an app that needs those writes '??',
                // '??|', '??&'. Counting the pair as binds both mis-aligns every
                // later parameter and emits "NULLNULL" for the unbound slots.
                if (i + 1 < n && sql.charAt(i + 1) == '?') {
                    if (escapes != null) escapes.add(i);
                    i += 2;
                    continue;
                }
                if (placeholders != null) placeholders.add(i);
                i++;
                continue;
            }
            if (c == ';' && semicolons != null) semicolons.add(i);
            i++;
        }
    }

    /**
     * If a dollar-quote tag opens at {@code start} ({@code '$'}), return the
     * index of its closing {@code '$'}; otherwise -1. A tag is {@code $$} or
     * {@code $ident$} where ident starts with a letter/underscore.
     */
    private static boolean isIdentPart(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    private static int dollarTagClose(String sql, int start) {
        int n = sql.length();
        int j = start + 1;
        if (j < n && sql.charAt(j) == '$') return j;           // "$$"
        if (j < n && (Character.isLetter(sql.charAt(j)) || sql.charAt(j) == '_')) {
            j++;
            while (j < n && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_')) j++;
            if (j < n && sql.charAt(j) == '$') return j;        // "$ident$"
        }
        return -1;
    }

    /**
     * A number's text, parenthesised when negative. Bare, {@code 10-?} with
     * -2.5 rendered {@code 10--2.5}: a {@code --} COMMENT, so the statement
     * silently answered 10. And {@code ?::text} with -5 rendered
     * {@code -5::text}, which casts 5 and then negates TEXT. Parenthesised it
     * is one value in every position -- the spelling the server's typed
     * parameters use, so the inline and EXECUTE paths agree.
     */
    private static void appendNumber(StringBuilder sb, String text) {
        if (text.startsWith("-")) sb.append('(').append(text).append(')');
        else sb.append(text);
    }

    private static void appendLiteral(StringBuilder sb, Object v) {
        if (v instanceof TargetValue) {
            TargetValue target = (TargetValue) v;
            sb.append("CAST(");
            appendLiteral(sb, target.value);
            sb.append(" AS ").append(target.type).append(')');
        } else if (v == null) {
            sb.append("NULL");
        } else if (v instanceof ArrayParams.Shape) {
            // An array, rendered only under serverPrepare=false: the PG array
            // literal ('{"a","b"}'::text[]) the server renders for the same
            // bound array. Fully quoted/escaped + cast, so append verbatim.
            sb.append(((ArrayParams.Shape) v).sqlLiteral());
        } else if (v instanceof Double || v instanceof Float) {
            // Quote before casting so large finite floats never parse as limited-precision numeric.
            sb.append("('").append(v).append("'::")
                    .append(v instanceof Float ? "float4" : "float8").append(')');
        } else if (v instanceof BigDecimal) {
            sb.append("('").append(v).append("'::numeric)");
        } else if (v instanceof Number) {
            appendNumber(sb, v.toString());
        } else if (v instanceof Boolean) {
            sb.append(v);
        } else if (v instanceof byte[]) {
            sb.append("'\\x");
            for (byte b : (byte[]) v) {
                sb.append(String.format("%02x", b));
            }
            sb.append("'::bytea");
        } else {
            // Bulk copy between quotes, doubling each embedded quote: byte-
            // identical to the per-char loop it replaces, without the per-char
            // appends on multi-KB JSON values.
            String s = v.toString();
            sb.append('\'');
            int from = 0;
            for (int q = s.indexOf('\''); q >= 0; q = s.indexOf('\'', q + 1)) {
                sb.append(s, from, q + 1).append('\'');
                from = q + 1;
            }
            sb.append(s, from, s.length());
            sb.append('\'');
            String type = null;
            if (v instanceof Timestamp || v instanceof java.time.LocalDateTime) type = "timestamp";
            else if (v instanceof Date || v instanceof java.time.LocalDate) type = "date";
            else if (v instanceof Time || v instanceof java.time.LocalTime) type = "time";
            else if (v instanceof java.time.OffsetDateTime || v instanceof java.time.Instant
                    || v instanceof java.time.ZonedDateTime) type = "timestamptz";
            else if (v instanceof java.util.UUID) type = "uuid";
            if (type != null) sb.append("::").append(type);
        }
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return executeQueryWith(this::send);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return executeUpdateWith(this::send);
    }

    @Override
    public boolean execute() throws SQLException {
        return executeWith(this::send);
    }

    // ── the String-taking Statement methods are ILLEGAL on a PreparedStatement ──
    //
    // JDBC 4.3 §PreparedStatement: "…this method cannot be called on a
    // PreparedStatement object; a SQLException is thrown if it is." Inheriting
    // them from PdbStatement was not merely a spec deviation - addBatch(String)
    // appended to the SUPERCLASS's batch list, which the overridden
    // executeBatch() never reads, so the statements were accepted, never sent,
    // and executeBatch() reported success for a batch it had silently dropped.
    @Override public ResultSet executeQuery(String sql) throws SQLException { throw onPrepared(); }
    @Override public int executeUpdate(String sql) throws SQLException { throw onPrepared(); }
    @Override public boolean execute(String sql) throws SQLException { throw onPrepared(); }
    @Override public void addBatch(String sql) throws SQLException { throw onPrepared(); }
    @Override public int executeUpdate(String sql, int a) throws SQLException { throw onPrepared(); }
    @Override public int executeUpdate(String sql, int[] a) throws SQLException { throw onPrepared(); }
    @Override public int executeUpdate(String sql, String[] a) throws SQLException { throw onPrepared(); }
    @Override public boolean execute(String sql, int a) throws SQLException { throw onPrepared(); }
    @Override public boolean execute(String sql, int[] a) throws SQLException { throw onPrepared(); }
    @Override public boolean execute(String sql, String[] a) throws SQLException { throw onPrepared(); }

    private static SQLException onPrepared() {
        return new SQLException(
                "this method takes a SQL string and cannot be called on a "
                + "PreparedStatement — use the no-argument form", "07003");
    }

    @Override
    public void addBatch() throws SQLException {
        ensureOpen();
        requireParameters();
        int n = placeholderAt.length;
        if (!arrayRows.isEmpty() || (hasArray(params, n) && arrayBindingAvailable())) {
            // A batch with an array parameter runs every row by server-side
            // handle, in order (executeBatch): it is never rendered as text.
            if (arrayRows.isEmpty()) {
                // Earlier rows (no array: NULL or none bound) join it, in order.
                arrayRows.addAll(batchRows);
                pendingBatch.clear();
                pendingRows.clear();
                batchRows.clear();
            }
            arrayRows.add(params.clone());
            return;
        }
        if (conn.serverPrepareRequested()) batchRows.add(params.clone());
        if (bulkInsert != null && pendingBatch.isEmpty() && conn.bulkBatchEnabled()) {
            pendingRows.add(params.clone());
            return;
        }
        pendingBatch.add(renderSql());
    }

    /**
     * Rows of a batch that carries an array parameter, run by
     * {@link #executeArrayBatch}. Every row after the first array row lands
     * here too, so the batch keeps its order.
     */
    private final List<Object[]> arrayRows = new ArrayList<>();
    /** The bound values of every row added before the batch's first array row. */
    private final List<Object[]> batchRows = new ArrayList<>();

    /**
     * A batch with an array parameter: each row by server-side handle, in
     * order, as the text {@code BATCH} frame runs its statements -- the first
     * failing row stops it and raises {@link java.sql.BatchUpdateException}
     * with the counts of the rows before it; in auto-commit mode each row
     * commits on its own, in a transaction they join it. Every row is
     * converted before the first is sent, so a row that cannot be bound fails
     * the batch before anything runs.
     */
    private int[] executeArrayBatch() throws SQLException {
        List<Object[]> wire = new ArrayList<>(arrayRows.size());
        try {
            for (Object[] row : arrayRows) wire.add(wireParams(row));
        } catch (SQLException e) {
            throw new java.sql.BatchUpdateException(e.getMessage(), e.getSQLState(), e.getErrorCode(),
                    new int[0], e);
        }
        int[] counts = new int[wire.size()];
        for (int i = 0; i < counts.length; i++) {
            StreamState.StreamResult r;
            try {
                r = sendBound(wire.get(i), getQueryTimeout());
            } catch (SQLException e) {
                throw new java.sql.BatchUpdateException(e.getMessage(), e.getSQLState(), e.getErrorCode(),
                        java.util.Arrays.copyOf(counts, i), e);
            }
            long c = r.kind == StreamState.StreamResult.Kind.OK ? r.affectedRows() : -1L;
            counts[i] = (c < 0 || c > Integer.MAX_VALUE) ? SUCCESS_NO_INFO : (int) c;
        }
        return counts;
    }

    @Override
    public int[] executeBatch() throws SQLException {
        ensureOpen();
        PdbStatement previous = PdbConnection.EXECUTING.get();
        PdbConnection.EXECUTING.set(this);
        try {
            return executePendingBatch();
        } finally {
            PdbConnection.EXECUTING.set(previous);
        }
    }

    private int[] executePendingBatch() throws SQLException {
        batchRows.clear();
        if (!arrayRows.isEmpty()) {
            try {
                return executeArrayBatch();
            } finally {
                arrayRows.clear();
            }
        }
        if (!pendingRows.isEmpty()) {
            // A plain INSERT ... VALUES (?, ...): ONE native bulk ingest of
            // every batched row (atomic: a failing row loads nothing).
            try {
                boolean loaded;
                try {
                    loaded = conn.bulkInsertRows(bulkInsert, pendingRows);
                } catch (java.sql.BatchUpdateException e) {
                    throw e;
                } catch (SQLException e) {
                    throw new java.sql.BatchUpdateException(e.getMessage(), e.getSQLState(),
                            e.getErrorCode(), new int[0], e);
                }
                if (loaded) {
                    int[] counts = new int[pendingRows.size()];
                    java.util.Arrays.fill(counts, 1);
                    return counts;
                }
                // Not bulk-loadable after all: the text batch, rows in order.
                List<String> text = new ArrayList<>(pendingRows.size() + pendingBatch.size());
                for (Object[] row : pendingRows) {
                    conn.preparedCache().recordExecution(sqlTemplate);
                    text.add(interpolate(sqlTemplate, placeholderAt, escapeAt, row));
                }
                text.addAll(pendingBatch);
                pendingBatch.clear();
                pendingBatch.addAll(text);
            } finally {
                pendingRows.clear();
            }
        }
        if (pendingBatch.isEmpty()) return new int[0];
        try {
            // Per-statement update counts; a failed statement raises
            // BatchUpdateException (see PdbConnection#executeBatch).
            return conn.executeBatch(pendingBatch);
        } finally {
            pendingBatch.clear();
        }
    }

    @Override public void clearBatch() {
        pendingBatch.clear();
        pendingRows.clear();
        arrayRows.clear();
        batchRows.clear();
        super.clearBatch();
    }

    @Override
    public void clearParameters() throws SQLException {
        ensureOpen();
        for (int i = 0; i < params.length; i++) params[i] = null;
        assigned.clear();
    }

    private void requireParameters() throws SQLException {
        int missing = assigned.nextClearBit(0);
        if (missing < placeholderAt.length) {
            throw new SQLException("no value supplied for parameter " + (missing + 1), "07001");
        }
    }

    private void setParam(int idx, Object v) throws SQLException {
        ensureOpen();
        if (idx < 1) throw new SQLException("parameter index must be >= 1", "07009");
        if (idx > placeholderAt.length) {
            throw new SQLException(
                    "parameter index " + idx + " exceeds placeholder count " + placeholderAt.length, "07009");
        }
        params[idx - 1] = v;
        assigned.set(idx - 1);
    }

    // ── setX() ────────────────────────────────────────────────────────────
    @Override public void setNull(int idx, int sqlType) throws SQLException { setParam(idx, null); }
    @Override public void setBoolean(int idx, boolean v) throws SQLException { setParam(idx, v); }
    @Override public void setByte(int idx, byte v) throws SQLException { setParam(idx, v); }
    @Override public void setShort(int idx, short v) throws SQLException { setParam(idx, v); }
    @Override public void setInt(int idx, int v) throws SQLException { setParam(idx, v); }
    @Override public void setLong(int idx, long v) throws SQLException { setParam(idx, v); }
    @Override public void setFloat(int idx, float v) throws SQLException { setParam(idx, v); }
    @Override public void setDouble(int idx, double v) throws SQLException { setParam(idx, v); }
    @Override public void setBigDecimal(int idx, BigDecimal v) throws SQLException { setParam(idx, v); }
    @Override public void setString(int idx, String v) throws SQLException { setParam(idx, v); }
    @Override public void setBytes(int idx, byte[] v) throws SQLException { setParam(idx, v); }
    @Override public void setDate(int idx, Date v) throws SQLException { setParam(idx, v); }
    @Override public void setTime(int idx, Time v) throws SQLException { setParam(idx, v); }
    @Override public void setTimestamp(int idx, Timestamp v) throws SQLException { setParam(idx, v); }
    @Override public void setObject(int idx, Object v) throws SQLException { setParam(idx, arrayOrValue(v)); }
    @Override public void setObject(int idx, Object v, int sqlType) throws SQLException {
        if (sqlType == java.sql.Types.ARRAY) {
            Object array = arrayOrValue(v);
            if (array != null && !(array instanceof ArrayParams.Shape)) {
                throw new java.sql.SQLDataException("ARRAY requires an array value", "22023");
            }
            setParam(idx, array);
        } else {
            setParam(idx, new TargetValue(v, targetType(sqlType)));
        }
    }
    @Override public void setObject(int idx, Object v, int sqlType, int scale) throws SQLException {
        if (v != null && (sqlType == java.sql.Types.DECIMAL || sqlType == java.sql.Types.NUMERIC)) {
            if (scale < 0 || scale > 16383) throw new java.sql.SQLDataException("numeric scale out of range", "22003");
            try {
                v = new BigDecimal(v.toString()).setScale(scale, java.math.RoundingMode.HALF_UP);
            } catch (NumberFormatException e) {
                throw new java.sql.SQLDataException("invalid numeric value", "22018", e);
            }
        }
        setObject(idx, v, sqlType);
    }
    @Override public void setObject(int idx, Object v, java.sql.SQLType type) throws SQLException {
        if (type == null || type.getVendorTypeNumber() == null) throw new SQLException("SQLType is required", "22023");
        setObject(idx, v, type.getVendorTypeNumber());
    }
    @Override public void setObject(int idx, Object v, java.sql.SQLType type, int scale) throws SQLException {
        if (type == null || type.getVendorTypeNumber() == null) throw new SQLException("SQLType is required", "22023");
        setObject(idx, v, type.getVendorTypeNumber(), scale);
    }

    private static final class TargetValue {
        final Object value;
        final String type;
        TargetValue(Object value, String type) { this.value = value; this.type = type; }
    }

    private static String targetType(int type) throws SQLException {
        switch (type) {
            case java.sql.Types.BOOLEAN: case java.sql.Types.BIT: return "boolean";
            case java.sql.Types.SMALLINT: case java.sql.Types.TINYINT: return "smallint";
            case java.sql.Types.INTEGER: return "integer";
            case java.sql.Types.BIGINT: return "bigint";
            case java.sql.Types.REAL: return "real";
            case java.sql.Types.FLOAT: case java.sql.Types.DOUBLE: return "double precision";
            case java.sql.Types.NUMERIC: case java.sql.Types.DECIMAL: return "numeric";
            case java.sql.Types.CHAR: case java.sql.Types.VARCHAR: case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.NCHAR: case java.sql.Types.NVARCHAR: case java.sql.Types.LONGNVARCHAR: return "text";
            case java.sql.Types.BINARY: case java.sql.Types.VARBINARY: case java.sql.Types.LONGVARBINARY: return "bytea";
            case java.sql.Types.DATE: return "date";
            case java.sql.Types.TIME: return "time";
            case java.sql.Types.TIMESTAMP: return "timestamp";
            case java.sql.Types.TIMESTAMP_WITH_TIMEZONE: return "timestamptz";
            case java.sql.Types.NULL: return "text";
            default: throw new SQLFeatureNotSupportedException("unsupported target SQL type: " + type, "0A000");
        }
    }

    /**
     * {@code setObject}'s value: a {@code java.sql.Array}, a Java array (other
     * than {@code byte[]}, which is a bytea) or a {@code Collection} is bound
     * as an SQL array, its element type taken from the Java component type
     * ({@code String[]} text[], {@code int[]}/{@code Integer[]} int4[],
     * {@code long[]} int8[], {@code UUID[]} uuid[], ...; {@code Integer[][]}
     * a 2-D int4[]) or, for an {@code Object[]} / {@code Collection}, from its
     * first non-null element. Anything else is bound as before.
     */
    private Object arrayOrValue(Object v) throws SQLException {
        if (v == null || v instanceof byte[] || v instanceof String) return v;
        if (v instanceof Array) return arrayShape((Array) v);
        if (v instanceof java.util.Collection || v.getClass().isArray()) {
            String type = inferElementType(v);
            Object[] elems = v instanceof java.util.Collection ? ((java.util.Collection<?>) v).toArray()
                    : boxPrimitiveArray(v);
            return new PdbArray(type, elems).shape();
        }
        return v;
    }

    /** The SQL element type of a Java array / collection value (see {@link #arrayOrValue}). */
    static String inferElementType(Object v) throws SQLException {
        Class<?> c = v.getClass();
        while (c.isArray() && c.getComponentType().isArray() && c.getComponentType() != byte[].class) {
            c = c.getComponentType();
        }
        Class<?> comp = c.isArray() ? c.getComponentType() : Object.class;
        String t = elementTypeOfClass(comp);
        if (t != null) return t;
        Object leaf = firstLeaf(v);
        if (leaf == null) {
            throw new SQLException("cannot infer the SQL element type of an empty or all-NULL "
                    + v.getClass().getSimpleName() + "; bind it with setArray(i,"
                    + " connection.createArrayOf(\"<type>\", elements))", "42P18");
        }
        t = elementTypeOfClass(leaf.getClass());
        if (t == null) throw ArrayParams.unsupportedType(leaf.getClass().getName());
        // Inferred from the values, so every value must name the same type:
        // Object[]{1, "a"} or {1, 2L} is refused (pgjdbc refuses every
        // Object[] with 07006), never coerced to the first element's type.
        String other = otherLeafType(v, t);
        if (other != null) {
            throw new SQLException("cannot infer one SQL element type for a " + v.getClass().getSimpleName()
                    + " holding both " + t + " and " + other + " values; bind it with setArray(i,"
                    + " connection.createArrayOf(\"<type>\", elements))", "07006");
        }
        return t;
    }

    /** The element type of the first non-null leaf of {@code v} whose type is not {@code t}, or null. */
    private static String otherLeafType(Object v, String t) {
        if (v == null) return null;
        if (!(v instanceof byte[]) && (v instanceof java.util.Collection || v.getClass().isArray())) {
            Object[] level = v instanceof java.util.Collection ? ((java.util.Collection<?>) v).toArray()
                    : v instanceof Object[] ? (Object[]) v : null;
            if (level == null) { // a primitive array: one type, its component's
                String lt = elementTypeOfClass(v.getClass().getComponentType());
                return java.lang.reflect.Array.getLength(v) == 0 || t.equals(lt) ? null : lt;
            }
            for (Object e : level) { String o = otherLeafType(e, t); if (o != null) return o; }
            return null;
        }
        String lt = elementTypeOfClass(v.getClass());
        return t.equals(lt) ? null : (lt == null ? v.getClass().getName() : lt);
    }

    private static Object firstLeaf(Object v) {
        if (v == null) return null;
        if (v instanceof byte[]) return v;
        if (v instanceof java.util.Collection) {
            for (Object e : (java.util.Collection<?>) v) { Object l = firstLeaf(e); if (l != null) return l; }
            return null;
        }
        if (v.getClass().isArray()) {
            int n = java.lang.reflect.Array.getLength(v);
            for (int i = 0; i < n; i++) { Object l = firstLeaf(java.lang.reflect.Array.get(v, i)); if (l != null) return l; }
            return null;
        }
        return v;
    }

    private static String elementTypeOfClass(Class<?> c) {
        if (c == String.class) return "text";
        if (c == Integer.class || c == int.class) return "int4";
        if (c == Long.class || c == long.class) return "int8";
        if (c == Short.class || c == short.class) return "int2";
        if (c == Double.class || c == double.class) return "float8";
        if (c == Float.class || c == float.class) return "float4";
        if (c == Boolean.class || c == boolean.class) return "bool";
        if (c == BigDecimal.class || c == java.math.BigInteger.class) return "numeric";
        if (c == java.util.UUID.class) return "uuid";
        if (c == java.sql.Date.class || c == java.time.LocalDate.class) return "date";
        if (c == java.sql.Timestamp.class || c == java.time.LocalDateTime.class) return "timestamp";
        if (c == java.time.OffsetDateTime.class || c == java.time.ZonedDateTime.class
                || c == java.time.Instant.class) return "timestamptz";
        if (c == byte[].class) return "bytea";
        return null;
    }

    /** The bind-side shape of any {@code java.sql.Array} (a foreign one is adapted to a {@link PdbArray}). */
    private static ArrayParams.Shape arrayShape(Array v) throws SQLException {
        if (v instanceof PdbArray) return ((PdbArray) v).shape();
        Object raw = v.getArray();
        Object[] elems = (raw instanceof Object[]) ? (Object[]) raw : boxPrimitiveArray(raw);
        return new PdbArray(v.getBaseTypeName(), elems).shape();
    }
    @Override public void setDate(int idx, Date v, Calendar cal) throws SQLException {
        setParam(idx, v == null || cal == null ? v
                : java.time.Instant.ofEpochMilli(v.getTime()).atZone(cal.getTimeZone().toZoneId())
                        .toLocalDate().toString());
    }
    @Override public void setTime(int idx, Time v, Calendar cal) throws SQLException {
        setParam(idx, v == null || cal == null ? v
                : java.time.Instant.ofEpochMilli(v.getTime()).atZone(cal.getTimeZone().toZoneId())
                        .toOffsetDateTime().toOffsetTime().toString());
    }
    @Override public void setTimestamp(int idx, Timestamp v, Calendar cal) throws SQLException {
        // The offset preserves the instant for timestamptz; timestamp uses the wall clock.
        setParam(idx, v == null || cal == null ? v
                : v.toInstant().atZone(cal.getTimeZone().toZoneId()).toOffsetDateTime()
                        .toString().replace('T', ' '));
    }
    @Override public void setNull(int idx, int sqlType, String typeName) throws SQLException { setParam(idx, null); }
    @Override public void setURL(int idx, URL v) throws SQLException { setParam(idx, v == null ? null : v.toString()); }

    // ── Stream/large-object setters: not yet supported ────────────────────
    @Override public void setAsciiStream(int idx, InputStream v, int len) throws SQLException { throw nys(); }
    @Override public void setUnicodeStream(int idx, InputStream v, int len) throws SQLException { throw nys(); }
    @Override public void setBinaryStream(int idx, InputStream v, int len) throws SQLException { throw nys(); }
    @Override public void setCharacterStream(int idx, Reader v, int len) throws SQLException { throw nys(); }
    @Override public void setRef(int idx, Ref v) throws SQLException { throw nys(); }
    @Override public void setBlob(int idx, Blob v) throws SQLException { throw nys(); }
    @Override public void setClob(int idx, Clob v) throws SQLException { throw nys(); }
    /**
     * Bind an SQL array. Its element type must be one an array parameter
     * carries ({@link ArrayParams#SUPPORTED_TYPES}), else
     * {@link SQLFeatureNotSupportedException}; elements are converted here
     * (a bad element raises {@link java.sql.SQLDataException}). The execution
     * sends it as a typed native-wire array (EXECUTE tag 17), never as SQL
     * text, unless the URL set {@code serverPrepare=false}.
     */
    @Override public void setArray(int idx, Array v) throws SQLException {
        if (v == null) { setParam(idx, null); return; }
        setParam(idx, arrayShape(v));
    }

    /** Box a primitive[] into an Object[] (for a foreign Array.getArray()). */
    private static Object[] boxPrimitiveArray(Object raw) throws SQLException {
        if (raw == null) return new Object[0];
        if (raw instanceof Object[]) return (Object[]) raw;
        if (!raw.getClass().isArray()) {
            throw new SQLException("setArray: unexpected array payload " + raw.getClass());
        }
        int len = java.lang.reflect.Array.getLength(raw);
        Object[] out = new Object[len];
        for (int i = 0; i < len; i++) out[i] = java.lang.reflect.Array.get(raw, i);
        return out;
    }
    @Override public ResultSetMetaData getMetaData() { return null; }
    @Override public ParameterMetaData getParameterMetaData() throws SQLException { throw nys(); }
    @Override public void setRowId(int idx, RowId v) throws SQLException { throw nys(); }
    @Override public void setNString(int idx, String v) throws SQLException { setString(idx, v); }
    @Override public void setNCharacterStream(int idx, Reader v, long len) throws SQLException { throw nys(); }
    @Override public void setNClob(int idx, NClob v) throws SQLException { throw nys(); }
    @Override public void setClob(int idx, Reader v, long len) throws SQLException { throw nys(); }
    @Override public void setBlob(int idx, InputStream v, long len) throws SQLException { throw nys(); }
    @Override public void setNClob(int idx, Reader v, long len) throws SQLException { throw nys(); }
    @Override public void setSQLXML(int idx, SQLXML v) throws SQLException { throw nys(); }
    @Override public void setAsciiStream(int idx, InputStream v, long len) throws SQLException { throw nys(); }
    @Override public void setBinaryStream(int idx, InputStream v, long len) throws SQLException { throw nys(); }
    @Override public void setCharacterStream(int idx, Reader v, long len) throws SQLException { throw nys(); }
    @Override public void setAsciiStream(int idx, InputStream v) throws SQLException { throw nys(); }
    @Override public void setBinaryStream(int idx, InputStream v) throws SQLException { throw nys(); }
    @Override public void setCharacterStream(int idx, Reader v) throws SQLException { throw nys(); }
    @Override public void setNCharacterStream(int idx, Reader v) throws SQLException { throw nys(); }
    @Override public void setClob(int idx, Reader v) throws SQLException { throw nys(); }
    @Override public void setBlob(int idx, InputStream v) throws SQLException { throw nys(); }
    @Override public void setNClob(int idx, Reader v) throws SQLException { throw nys(); }

    private static SQLFeatureNotSupportedException nys() {
        return new SQLFeatureNotSupportedException("setter not supported in v1; lands in v1.1");
    }
}
