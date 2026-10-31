package org.particledb;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * pg_catalog introspection adapter - the "native way" for PG-dialect tooling.
 *
 * <p>When the driver advertises itself as PostgreSQL ({@code pgCompat=true},
 * the default), frameworks such as Flyway 9's PostgreSQL dialect and
 * Conductor's partition-manager DAOs emit raw SQL against {@code pg_catalog}
 * relations. ParticleDB serves {@code pg_class}, {@code pg_namespace},
 * {@code pg_attribute}, {@code pg_type}, {@code pg_operator},
 * {@code pg_constraint}, {@code pg_index} and the full
 * {@code information_schema}, but does NOT serve {@code pg_depend},
 * {@code pg_proc}, {@code pg_extension}, {@code pg_roles},
 * {@code pg_inherits} or {@code pg_tables}. Queries that reference the
 * missing relations fail with a {@code table not found} error.
 *
 * <p>Rather than have the engine emulate these tables, this adapter rewrites
 * the SQL in the driver, on its way to the wire: each reference to a missing
 * relation is substituted with an inline sub-select that yields the expected
 * column set. For a fresh database (no extensions, no inheritance, no
 * user-defined functions catalogued in {@code pg_proc}) the correct answer for
 * these relations is the empty set, so most substitutions are empty relations
 * with the right column names/types. {@code pg_tables} is backed by
 * {@code information_schema.tables} so partition-existence checks return real
 * answers.
 *
 * <p>The transform is a pure {@code String -> String} function with no
 * database round-trip. It:
 * <ul>
 *   <li>matches both {@code pg_catalog.pg_X} and the bare {@code pg_X} form;</li>
 *   <li>only matches a relation reference at a word boundary - never inside a
 *       longer identifier ({@code pg_tablespace}, {@code my_pg_tables}) or
 *       inside a single-/double-quoted/dollar-quoted string literal;</li>
 *   <li>preserves a trailing alias ({@code pg_depend d} &rarr; {@code (...) d});</li>
 *   <li>is case-insensitive on the relation name;</li>
 *   <li>additionally normalises the PG-legal but PDB-unparsed empty
 *       target-list form {@code EXISTS(SELECT FROM t ...)} to
 *       {@code EXISTS(SELECT 1 FROM t ...)} - semantically identical inside
 *       {@code EXISTS} - because Conductor's {@code checkShardExists} emits it
 *       and PDB's parser rejects an empty {@code SELECT} target list.</li>
 * </ul>
 *
 * <p>Empty-relation form: {@code (SELECT NULL::T AS col, ... WHERE false) alias}.
 * ParticleDB accepts {@code SELECT <exprs> WHERE false} with no {@code FROM}.
 * Char-typed catalog columns ({@code deptype}, {@code prokind}) use {@code text}
 * instead of the pseudo-type {@code "char"}, which PDB does not define; every
 * caller only ever compares them for equality (e.g. {@code deptype = 'e'}),
 * so {@code text} is behaviour-preserving.
 */
public final class PgCatalogAdapter {

    private PgCatalogAdapter() {}

    /**
     * Ordered map: catalog relation name (lower-case, no schema) &rarr; the
     * inline sub-select body (WITHOUT the surrounding alias) that replaces it.
     *
     * <p>Column sets are taken from the actual PostgreSQL catalog definitions
     * and the specific columns the Flyway 9.0.4 / Conductor queries SELECT or
     * join on. The parenthesised sub-select is emitted verbatim; the matcher
     * re-attaches any alias that followed the original table token.
     */
    static final Map<String, String> SUBSTITUTIONS = new LinkedHashMap<>();
    static {
        // pg_depend: Flyway LEFT JOINs on objid / deptype to exclude
        // extension-owned objects. Empty ⇒ nothing is extension-owned.
        SUBSTITUTIONS.put("pg_depend",
            "(SELECT NULL::int4 AS classid, NULL::int4 AS objid, NULL::int4 AS objsubid, "
          + "NULL::int4 AS refclassid, NULL::int4 AS refobjid, NULL::int4 AS refobjsubid, "
          + "NULL::text AS deptype WHERE false)");

        // pg_proc: Flyway isSchemaEmpty joins on pronamespace; the "clean"
        // path SELECTs proname/proargtypes/prokind/proisagg. Empty ⇒ no
        // user procedures/functions. prokind is char in PG ⇒ text here.
        SUBSTITUTIONS.put("pg_proc",
            "(SELECT NULL::oid AS oid, NULL::name AS proname, NULL::oid AS pronamespace, "
          + "NULL::bool AS proisagg, NULL::text AS prokind, NULL::oid AS prorettype, "
          + "NULL::text AS proargtypes, NULL::text AS prosrc WHERE false)");

        // pg_extension: Flyway getCurrentExtensions selects extname and joins
        // extnamespace/extowner. Empty ⇒ no installed extensions.
        SUBSTITUTIONS.put("pg_extension",
            "(SELECT NULL::oid AS oid, NULL::name AS extname, NULL::oid AS extnamespace, "
          + "NULL::oid AS extowner WHERE false)");

        // pg_roles: Flyway getCurrentExtensions joins extowner -> rolname.
        // Empty ⇒ role filter matches nothing (extension list is empty anyway).
        SUBSTITUTIONS.put("pg_roles",
            "(SELECT NULL::oid AS oid, NULL::name AS rolname WHERE false)");

        // pg_inherits: Flyway doAllTables excludes partition children via a
        // NOT EXISTS on inhrelid. Empty ⇒ no table inherits ⇒ all base tables
        // are returned (correct for a non-partitioned schema).
        SUBSTITUTIONS.put("pg_inherits",
            "(SELECT NULL::oid AS inhrelid, NULL::oid AS inhparent, NULL::int4 AS inhseqno WHERE false)");

        // pg_tables: backed by information_schema.tables so Conductor's
        // partition existence / enumeration checks return REAL answers.
        // Columns match PG's pg_tables view; owner/tablespace are unknown
        // (NULL) and the boolean flags are conservatively false - none of the
        // known callers read them (they filter on schemaname/tablename only).
        SUBSTITUTIONS.put("pg_tables",
            "(SELECT table_schema AS schemaname, table_name AS tablename, "
          + "NULL::name AS tableowner, NULL::name AS tablespace, "
          + "false AS hasindexes, false AS hasrules, false AS hastriggers, "
          + "false AS rowsecurity FROM information_schema.tables)");
    }

    /**
     * Rewrite {@code sql} so every reference to a missing pg_catalog relation
     * is replaced by its inline sub-select, and PG-legal empty {@code SELECT}
     * target lists are normalised. Returns the input unchanged when it
     * references none of the handled relations and has no empty target list  -
     * the common case (fast path).
     */
    /**
     * Whether {@link #rewrite} (or the whole-statement {@code set_config}
     * translation) could change {@code sql} at all. False means every one of
     * their cheap pre-filters rejects it.
     *
     * <p>Used on a PreparedStatement's TEMPLATE, once: a rendered parameter is a
     * quoted literal or a number, and every rewrite either skips literals
     * (relation substitution, empty-target-list normalisation) or requires the
     * statement's own shape to be a {@code set_config} call, so a template for
     * which this is false renders to statements for which it is false too.
     */
    static boolean mayRewrite(String sql) {
        if (sql == null || sql.isEmpty()) return false;
        return containsCandidate(sql) || mayHaveEmptySelectList(sql) || containsSetConfig(sql);
    }

    public static String rewrite(String sql) {
        if (sql == null || sql.isEmpty()) return sql;

        String out = sql;
        // Only walk the (relatively expensive) tokenizer if a candidate token
        // could be present. A cheap case-insensitive contains check avoids the
        // per-character scan for the overwhelming majority of app queries.
        if (containsCandidate(out)) {
            out = substituteRelations(out);
        }
        // Empty target-list normalisation is likewise gated on a cheap check.
        if (mayHaveEmptySelectList(out)) {
            out = normaliseEmptySelectLists(out);
        }
        // set_config(setting, value, is_local) is a PostgreSQL GUC-writer that
        // Flyway uses to pin search_path ("SELECT set_config('search_path', ?,
        // false)"). It is a WRITE, and this driver must perform it: see
        // setConfigAsSessionStatement below for the whole-statement form, which
        // becomes the SET/RESET the native wire honours. Gated on a cheap
        // case-insensitive scan.
        if (containsSetConfig(out)) {
            String asSet = setConfigAsSessionStatement(out);
            out = asSet != null ? asSet : unwrapSetConfig(out);
        }
        return out;
    }

    // ── set_config: a GUC WRITE, translated into one the wire honours ───────

    /**
     * The whole-statement {@code SELECT set_config(name, value, is_local)}
     * shape, translated into the {@code SET} / {@code SET LOCAL} / {@code RESET}
     * statement the ParticleDB native wire actually applies - or {@code null}
     * when {@code sql} is not that shape.
     *
     * <h2>Why a translation and not a pass-through</h2>
     *
     * The engine has no {@code set_config} function ({@code grep} it: zero hits
     * in the server's SQL and execution layers). It is a WIRE intercept,
     * and only the PG wire has one - the server's PostgreSQL-wire layer's
     * {@code parse_set_config}. Sending the call through unchanged on the
     * native wire therefore does not write the GUC; it earns
     * {@code 42883 function set_config does not exist}. The native wire DOES
     * honour {@code SET}/{@code SET LOCAL}/{@code RESET}: they reach
     * {@code session_guc::apply_session_stmt}, which is the per-connection
     * store every wire mirrors into the engine before each statement. So the
     * faithful thing a driver can do on its own is emit the equivalent
     * statement, which is what this does.
     *
     * <h2>What was here before, and why it was wrong</h2>
     *
     * This call used to be REWRITTEN AWAY - replaced by its second argument  -
     * under a comment asserting "ParticleDB is single-schema, so the write is a
     * semantic no-op". That has not been true since the relation resolver
     * started walking {@code search_path}: {@code session_guc.rs} files
     * {@code search_path} as {@code Honoured}, and an unqualified
     * {@code CREATE TABLE} is placed in the path's schema. The rewrite returned
     * the caller's own string, so Flyway saw its value echoed back and
     * concluded the pin had taken while {@code current_schema()} went on
     * answering {@code public} - and every migration landed in the wrong
     * schema. It is deleted rather than narrowed to non-{@code search_path}
     * names, because the same argument applies to every honoured parameter.
     *
     * <h2>Deliberately conservative</h2>
     *
     * Only the exact shape PostgreSQL's own wire intercept models is
     * translated: a lone {@code SELECT set_config(<literal>, <literal|NULL>,
     * <boolean literal>)} with nothing else in the statement, and a parameter
     * name that is a plain (optionally dotted) identifier. The driver renders
     * bound parameters into the SQL before this runs - this IS the chokepoint
     * every statement flows through, {@code PdbConnection#adapt} - so Flyway's
     * {@code '…', ?, false} arrives here already literal. Anything else falls
     * back to the previous unwrap rather than being turned into a statement
     * this method guessed at.
     */
    static String setConfigAsSessionStatement(String sql) {
        SetConfigCall c = parseWholeStatementSetConfig(sql);
        return c == null ? null : c.toSessionStatement();
    }

    /** A parsed whole-statement {@code SELECT set_config(...)} call. */
    static final class SetConfigCall {
        /** The parameter name (arg 1), already unquoted. */
        final String name;
        /** The value (arg 2), already unquoted; {@code null} for the NULL form. */
        final String value;
        /** arg 3 - {@code SET LOCAL} rather than {@code SET}. */
        final boolean isLocal;

        SetConfigCall(String name, String value, boolean isLocal) {
            this.name = name;
            this.value = value;
            this.isLocal = isLocal;
        }

        /**
         * The equivalent session statement.
         *
         * <p>{@code set_config(name, NULL, …)} is PostgreSQL's spelling of
         * {@code RESET name} - the null is not "set it to the empty string".
         */
        String toSessionStatement() {
            // A NULL value is RESET - but `is_local` SCOPES it, and dropping the
            // flag here destroyed the caller's SESSION value permanently.
            //
            // PostgreSQL 17.9, measured:
            //
            //   SELECT set_config('search_path','session_val',false);
            //   BEGIN;
            //   SELECT set_config('search_path',NULL,true);   -- default, in-txn
            //   COMMIT;
            //   SHOW search_path;                             -- session_val BACK
            //
            // Emitting a plain `RESET` for the LOCAL form made that last line
            // answer the default instead: the session entry was gone, so there
            // was nothing for end-of-transaction to restore. `SET LOCAL <name>
            // TO DEFAULT` is PostgreSQL's spelling for the txn-scoped reset and
            // is what the wire honours.
            if (value == null) {
                return isLocal ? "SET LOCAL " + name + " TO DEFAULT" : "RESET " + name;
            }
            return "SET " + (isLocal ? "LOCAL " : "") + name + " TO '"
                    + value.replace("'", "''") + "'";
        }

        /**
         * The statement that reads the parameter back, so the call can still
         * RETURN a row the way {@code set_config} does.
         *
         * <p>{@code SHOW} rather than an echo of the argument, deliberately:
         * the argument is what the CALLER asked for and the old rewrite already
         * proved a client cannot tell that apart from what the server did. This
         * asks the server.
         */
        String readBackStatement() {
            return "SHOW " + name;
        }
    }

    /**
     * Parse {@code sql} as a lone {@code SELECT [pg_catalog.]set_config(a, b, c)}
     * call. Returns {@code null} unless the entire statement is that call with
     * three literal arguments and an identifier-shaped parameter name  -
     * mirroring {@code pg_wire.rs::parse_set_config}, which declines the same
     * shapes for the same reason (anything larger is a query, not a GUC write).
     */
    static SetConfigCall parseWholeStatementSetConfig(String sql) {
        if (sql == null) return null;
        int i = skipTrivia(sql, 0);
        int n = sql.length();
        if (!sql.regionMatches(true, i, "select", 0, 6)) return null;
        i = skipTrivia(sql, i + 6);
        // `pg_catalog.set_config(...)` is the same function, and it is the form
        // pg_dump emits ("SELECT pg_catalog.set_config('search_path', '',
        // false)" before every restore). Accepting only the bare spelling left
        // that one falling through to a wire that has no such function.
        if (sql.regionMatches(true, i, "pg_catalog", 0, 10)) {
            int j = skipTrivia(sql, i + 10);
            if (j < sql.length() && sql.charAt(j) == '.') {
                i = skipTrivia(sql, j + 1);
            }
        }
        if (!sql.regionMatches(true, i, "set_config", 0, 10)) return null;
        int afterName = i + 10;
        if (afterName < n && isIdentPart(sql.charAt(afterName))) return null;
        i = skipTrivia(sql, afterName);
        if (i >= n || sql.charAt(i) != '(') return null;

        int[] end = new int[1];
        List<String> args = parseCallArgs(sql, i, end);
        if (args == null || args.size() != 3) return null;
        // Nothing may follow the call but trivia and an optional ';'.
        int j = skipTrivia(sql, end[0]);
        if (j < n && sql.charAt(j) == ';') j = skipTrivia(sql, j + 1);
        if (j != n) return null;

        String name = unquoteLiteral(args.get(0).trim());
        if (name == null || !isPlainParameterName(name)) return null;

        String rawValue = args.get(1).trim();
        String value;
        if (rawValue.equalsIgnoreCase("null")) {
            value = null;
        } else {
            value = unquoteLiteral(rawValue);
            if (value == null) return null;
        }

        String rawLocal = args.get(2).trim().toLowerCase();
        boolean isLocal;
        if (rawLocal.equals("true") || rawLocal.equals("'t'") || rawLocal.equals("'true'")) {
            isLocal = true;
        } else if (rawLocal.equals("false") || rawLocal.equals("'f'") || rawLocal.equals("'false'")) {
            isLocal = false;
        } else {
            // A bound param, an expression, anything else - not this shape.
            return null;
        }
        return new SetConfigCall(name, value, isLocal);
    }

    /** A single-quoted SQL string literal → its value, or {@code null}. */
    private static String unquoteLiteral(String s) {
        if (s.length() < 2 || s.charAt(0) != '\'' || s.charAt(s.length() - 1) != '\'') return null;
        String inner = s.substring(1, s.length() - 1);
        // An unescaped quote in the middle means this was two literals, not one.
        if (inner.replace("''", "").indexOf('\'') >= 0) return null;
        return inner.replace("''", "'");
    }

    /**
     * Is {@code name} safe to splice into a {@code SET}/{@code RESET} statement
     * unquoted - a plain identifier, or the dotted {@code a.b} spelling of a
     * PostgreSQL customized option? Anything else (whitespace, quotes,
     * semicolons) declines the translation rather than building a statement out
     * of it.
     */
    private static boolean isPlainParameterName(String name) {
        if (name.isEmpty() || name.length() > 128) return false;
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (!(Character.isLetterOrDigit(c) || c == '_' || c == '.')) return false;
        }
        return Character.isLetter(name.charAt(0)) || name.charAt(0) == '_';
    }

    /** Advance past whitespace and SQL comments starting at {@code i}. */
    private static int skipTrivia(String sql, int i) {
        int n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);
            if (Character.isWhitespace(c)) { i++; continue; }
            if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                i += 2;
                while (i < n && sql.charAt(i) != '\n') i++;
                continue;
            }
            if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                int d = 1;
                i += 2;
                while (i < n && d > 0) {
                    if (i + 1 < n && sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') { d++; i += 2; }
                    else if (i + 1 < n && sql.charAt(i) == '*' && sql.charAt(i + 1) == '/') { d--; i += 2; }
                    else i++;
                }
                continue;
            }
            break;
        }
        return i;
    }

    private static boolean containsSetConfig(String sql) {
        final int m = 10; // "set_config".length()
        for (int i = 0, n = sql.length(); i + m <= n; i++) {
            char c = sql.charAt(i);
            if ((c == 's' || c == 'S') && sql.regionMatches(true, i, "set_config", 0, m)) return true;
        }
        return false;
    }

    private static boolean containsCandidate(String sql) {
        // "pg_" is the shared prefix of every handled relation. If it is not
        // present at all (case-insensitively) there is nothing to do.
        int n = sql.length();
        for (int i = 0; i + 2 < n; i++) {
            char a = sql.charAt(i);
            if ((a == 'p' || a == 'P')
                    && (sql.charAt(i + 1) == 'g' || sql.charAt(i + 1) == 'G')
                    && sql.charAt(i + 2) == '_') {
                return true;
            }
        }
        return false;
    }

    private static boolean mayHaveEmptySelectList(String sql) {
        // Only the "SELECT FROM" shape (empty target list) needs fixing. Cheap
        // pre-filter: the substring "select" followed shortly by "from".
        // Case-insensitive in place: this runs on EVERY statement, and
        // lower-casing a copy of the whole statement first was a visible cost
        // in the per-statement profile.
        int n = sql.length();
        int idx = indexOfIgnoreCase(sql, "select", 0);
        while (idx >= 0) {
            int j = idx + 6;
            // skip whitespace/comments crudely - a full scan happens later
            while (j < n && Character.isWhitespace(sql.charAt(j))) j++;
            if (sql.regionMatches(true, j, "from", 0, 4)
                    && (j + 4 >= n || !isIdentPart(Character.toLowerCase(sql.charAt(j + 4))))) {
                return true;
            }
            idx = indexOfIgnoreCase(sql, "select", idx + 6);
        }
        return false;
    }

    /** {@code s.toLowerCase().indexOf(lowerNeedle, from)} without the copy (ASCII needle). */
    private static int indexOfIgnoreCase(String s, String lowerNeedle, int from) {
        int m = lowerNeedle.length();
        char first = lowerNeedle.charAt(0);
        for (int i = Math.max(from, 0), last = s.length() - m; i <= last; i++) {
            char c = s.charAt(i);
            if ((c == first || Character.toLowerCase(c) == first)
                    && s.regionMatches(true, i, lowerNeedle, 0, m)) {
                return i;
            }
        }
        return -1;
    }

    // ── relation substitution ───────────────────────────────────────────────

    /**
     * Scan {@code sql} character by character, skipping string/quoted-identifier
     * literals and comments, and replace each bare {@code pg_X} /
     * {@code pg_catalog.pg_X} token (X in {@link #SUBSTITUTIONS}) with its
     * sub-select, re-attaching any alias that follows.
     */
    static String substituteRelations(String sql) {
        StringBuilder out = new StringBuilder(sql.length() + 256);
        int i = 0, n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);

            // line comment --...
            if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                int start = i;
                i += 2;
                while (i < n && sql.charAt(i) != '\n') i++;
                out.append(sql, start, i);
                continue;
            }
            // block comment /* ... */ (nesting per SQL standard)
            if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                int start = i;
                int depth = 1;
                i += 2;
                while (i < n && depth > 0) {
                    if (i + 1 < n && sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') { depth++; i += 2; }
                    else if (i + 1 < n && sql.charAt(i) == '*' && sql.charAt(i + 1) == '/') { depth--; i += 2; }
                    else i++;
                }
                out.append(sql, start, i);
                continue;
            }
            // single-quoted string literal (handles '' escape and E'\'' escape)
            if (c == '\'') {
                boolean eString = i > 0 && (sql.charAt(i - 1) == 'E' || sql.charAt(i - 1) == 'e')
                        && (i < 2 || !isIdentPart(sql.charAt(i - 2)));
                int start = i;
                i++;
                while (i < n) {
                    char d = sql.charAt(i);
                    if (eString && d == '\\' && i + 1 < n) { i += 2; continue; }
                    if (d == '\'') {
                        if (i + 1 < n && sql.charAt(i + 1) == '\'') { i += 2; continue; }
                        i++;
                        break;
                    }
                    i++;
                }
                out.append(sql, start, i);
                continue;
            }
            // double-quoted identifier ("" escape) - a quoted identifier is
            // NEVER one of our unquoted catalog tokens, so copy verbatim.
            if (c == '"') {
                int start = i;
                i++;
                while (i < n) {
                    if (sql.charAt(i) == '"') {
                        if (i + 1 < n && sql.charAt(i + 1) == '"') { i += 2; continue; }
                        i++;
                        break;
                    }
                    i++;
                }
                out.append(sql, start, i);
                continue;
            }
            // dollar-quoted string $tag$ ... $tag$
            if (c == '$') {
                int tagEnd = dollarTagEnd(sql, i);
                if (tagEnd > i) {
                    String tag = sql.substring(i, tagEnd); // includes both $
                    int close = sql.indexOf(tag, tagEnd);
                    int end = close < 0 ? n : close + tag.length();
                    out.append(sql, i, end);
                    i = end;
                    continue;
                }
            }

            // identifier / keyword run
            if (isIdentStart(c)) {
                int start = i;
                i++;
                while (i < n && isIdentPart(sql.charAt(i))) i++;
                String word = sql.substring(start, i);

                // optional "pg_catalog." schema qualifier
                String bare = word;
                int tokenStart = start;
                if (word.equalsIgnoreCase("pg_catalog") && i < n && sql.charAt(i) == '.') {
                    int dot = i;
                    int j = dot + 1;
                    if (j < n && isIdentStart(sql.charAt(j))) {
                        int ws = j;
                        j++;
                        while (j < n && isIdentPart(sql.charAt(j))) j++;
                        bare = sql.substring(ws, j);
                        // advance i past the qualified name
                        i = j;
                    }
                }

                String sub = SUBSTITUTIONS.get(bare.toLowerCase());
                if (sub != null && !precededByDot(sql, tokenStart)) {
                    out.append(sub);
                    // preserve/attach an alias if one follows
                    i = appendAlias(sql, i, out);
                } else {
                    out.append(sql, tokenStart, i);
                }
                continue;
            }

            out.append(c);
            i++;
        }
        return out.toString();
    }

    /**
     * The RESIDUE path: replace a {@code set_config(setting, value, is_local)}
     * call that is EMBEDDED in a larger expression with its {@code value}
     * argument.
     *
     * <p>Reached only when {@link #parseWholeStatementSetConfig} declined  -
     * i.e. the call is not the whole statement, or an argument is not a
     * literal. Those shapes cannot be turned into a {@code SET} without
     * guessing at a value the driver does not have, and the engine has no
     * {@code set_config} function to send them to, so the substitution is kept
     * as the least-bad answer: the caller gets the value back and the GUC is
     * NOT written.
     *
     * <p><b>That is a known, narrowed lie.</b> It used to be the ONLY path, and
     * it is why {@code set_config('search_path', …)} silently did nothing while
     * reporting success. The shape Flyway and every ordinary client send - the
     * lone statement - no longer comes here.
     *
     * <p>Literal/comment/dollar-quote aware; leaves a {@code set_config} token
     * that is a column reference (preceded by '.') or not immediately followed
     * by '(' untouched, and falls back to copying verbatim on any malformed
     * call.
     */
    static String unwrapSetConfig(String sql) {
        StringBuilder out = new StringBuilder(sql.length());
        int i = 0, n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);
            if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                int s = i; i += 2; while (i < n && sql.charAt(i) != '\n') i++;
                out.append(sql, s, i); continue;
            }
            if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                int s = i, d = 1; i += 2;
                while (i < n && d > 0) {
                    if (i + 1 < n && sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') { d++; i += 2; }
                    else if (i + 1 < n && sql.charAt(i) == '*' && sql.charAt(i + 1) == '/') { d--; i += 2; }
                    else i++;
                }
                out.append(sql, s, i); continue;
            }
            if (c == '\'') {
                int s = i; i++;
                while (i < n) {
                    char d = sql.charAt(i);
                    if (d == '\'') { if (i + 1 < n && sql.charAt(i + 1) == '\'') { i += 2; continue; } i++; break; }
                    i++;
                }
                out.append(sql, s, i); continue;
            }
            if (c == '"') {
                int s = i; i++;
                while (i < n) {
                    if (sql.charAt(i) == '"') { if (i + 1 < n && sql.charAt(i + 1) == '"') { i += 2; continue; } i++; break; }
                    i++;
                }
                out.append(sql, s, i); continue;
            }
            if (c == '$') {
                int te = dollarTagEnd(sql, i);
                if (te > i) {
                    String tag = sql.substring(i, te);
                    int close = sql.indexOf(tag, te);
                    int end = close < 0 ? n : close + tag.length();
                    out.append(sql, i, end); i = end; continue;
                }
            }
            if (isIdentStart(c)) {
                int start = i; i++;
                while (i < n && isIdentPart(sql.charAt(i))) i++;
                String word = sql.substring(start, i);
                if (word.equalsIgnoreCase("set_config") && !precededByDot(sql, start)) {
                    int j = i;
                    while (j < n && Character.isWhitespace(sql.charAt(j))) j++;
                    if (j < n && sql.charAt(j) == '(') {
                        int[] endHolder = new int[1];
                        List<String> args = parseCallArgs(sql, j, endHolder);
                        if (args != null && args.size() >= 2) {
                            out.append(args.get(1).trim());
                            i = endHolder[0];
                            continue;
                        }
                    }
                }
                out.append(word); continue;
            }
            out.append(c); i++;
        }
        return out.toString();
    }

    /**
     * Parse the comma-separated top-level arguments of a call whose opening
     * '(' is at {@code open}. Returns the raw argument substrings (literal /
     * comment / nested-paren aware) and, via {@code endHolder[0]}, the index
     * just past the matching ')'. Returns {@code null} on an unbalanced call.
     */
    private static List<String> parseCallArgs(String sql, int open, int[] endHolder) {
        int n = sql.length();
        if (open >= n || sql.charAt(open) != '(') return null;
        List<String> args = new ArrayList<>();
        int depth = 0, i = open + 1, argStart = open + 1;
        while (i < n) {
            char c = sql.charAt(i);
            if (c == '\'') {
                i++;
                while (i < n) {
                    char d = sql.charAt(i);
                    if (d == '\'') { if (i + 1 < n && sql.charAt(i + 1) == '\'') { i += 2; continue; } i++; break; }
                    i++;
                }
                continue;
            }
            if (c == '"') {
                i++;
                while (i < n) {
                    if (sql.charAt(i) == '"') { if (i + 1 < n && sql.charAt(i + 1) == '"') { i += 2; continue; } i++; break; }
                    i++;
                }
                continue;
            }
            if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') { i += 2; while (i < n && sql.charAt(i) != '\n') i++; continue; }
            if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                int d = 1; i += 2;
                while (i < n && d > 0) {
                    if (i + 1 < n && sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') { d++; i += 2; }
                    else if (i + 1 < n && sql.charAt(i) == '*' && sql.charAt(i + 1) == '/') { d--; i += 2; }
                    else i++;
                }
                continue;
            }
            if (c == '$') {
                int te = dollarTagEnd(sql, i);
                if (te > i) { String tag = sql.substring(i, te); int close = sql.indexOf(tag, te); i = close < 0 ? n : close + tag.length(); continue; }
            }
            if (c == '(') { depth++; i++; continue; }
            if (c == ')') {
                if (depth == 0) { args.add(sql.substring(argStart, i)); endHolder[0] = i + 1; return args; }
                depth--; i++; continue;
            }
            if (c == ',' && depth == 0) { args.add(sql.substring(argStart, i)); argStart = i + 1; i++; continue; }
            i++;
        }
        return null;
    }

    /**
     * If the token that ended at {@code tokenStart} was preceded (ignoring no
     * whitespace - a qualifier dot is adjacent) by a '.', it is a column/schema
     * qualifier like {@code x.pg_tables}, NOT a top-level relation reference.
     * We must not rewrite those. ({@code pg_catalog.pg_tables} is handled
     * separately and does not reach here as a lone {@code pg_tables}.)
     */
    private static boolean precededByDot(String sql, int tokenStart) {
        int k = tokenStart - 1;
        // A dot directly before the identifier means it is qualified. We do not
        // skip whitespace: PG identifiers qualify with an adjacent dot.
        return k >= 0 && sql.charAt(k) == '.';
    }

    /**
     * After emitting a substitution, consume and re-emit a following table
     * alias so {@code pg_depend d}, {@code pg_depend AS d}, {@code pg_depend
     * dep} are preserved as {@code (...) d} etc. Does NOT consume a SQL keyword
     * that legitimately follows a table reference (WHERE, ON, JOIN, LEFT, ...),
     * a comma, a closing paren, or a semicolon.
     *
     * @return the index to continue scanning from
     */
    private static int appendAlias(String sql, int i, StringBuilder out) {
        int n = sql.length();
        int save = i;
        // skip whitespace
        while (i < n && Character.isWhitespace(sql.charAt(i))) i++;
        if (i >= n) return i;

        // explicit AS
        if (matchesKeyword(sql, i, "as")) {
            int afterAs = i + 2;
            int a2 = afterAs;
            while (a2 < n && Character.isWhitespace(sql.charAt(a2))) a2++;
            if (a2 < n && isIdentStart(sql.charAt(a2))) {
                int aliasStart = a2;
                a2++;
                while (a2 < n && isIdentPart(sql.charAt(a2))) a2++;
                out.append(' ').append(sql, aliasStart, a2);
                return a2;
            }
            // AS with no following identifier - leave untouched
            return save;
        }

        // bare alias: an identifier that is not a reserved follow-keyword
        if (isIdentStart(sql.charAt(i))) {
            int aliasStart = i;
            int a2 = i + 1;
            while (a2 < n && isIdentPart(sql.charAt(a2))) a2++;
            String cand = sql.substring(aliasStart, a2);
            if (!isFollowKeyword(cand)) {
                out.append(' ').append(cand);
                return a2;
            }
        }
        // no alias to attach - resume at the original position (keeps the
        // whitespace/keyword for the outer scanner)
        return save;
    }

    private static boolean matchesKeyword(String sql, int i, String kw) {
        int n = sql.length();
        if (i + kw.length() > n) return false;
        for (int k = 0; k < kw.length(); k++) {
            if (Character.toLowerCase(sql.charAt(i + k)) != kw.charAt(k)) return false;
        }
        int after = i + kw.length();
        return after >= n || !isIdentPart(sql.charAt(after));
    }

    /**
     * Keywords that may legitimately follow a table reference and therefore
     * must NOT be swallowed as an alias.
     */
    private static boolean isFollowKeyword(String w) {
        switch (w.toLowerCase()) {
            case "where": case "on": case "using": case "join": case "inner":
            case "left": case "right": case "full": case "cross": case "natural":
            case "group": case "order": case "having": case "limit": case "offset":
            case "union": case "intersect": case "except": case "for": case "as":
            case "and": case "or": case "fetch": case "window": case "returning":
            case "with": case "lateral": case "tablesample":
                return true;
            default:
                return false;
        }
    }

    // ── empty target-list normalisation ──────────────────────────────────────

    /**
     * Rewrite {@code SELECT FROM} (an empty target list, legal in PG but
     * rejected by PDB's parser) to {@code SELECT 1 FROM}. Only touches a
     * {@code SELECT} immediately followed by {@code FROM} at a token boundary;
     * skips literals/comments so a string containing "select from" is safe.
     */
    static String normaliseEmptySelectLists(String sql) {
        StringBuilder out = new StringBuilder(sql.length() + 16);
        int i = 0, n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);
            // comments and literals: copy verbatim (reuse the same skips)
            if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                int s = i; i += 2; while (i < n && sql.charAt(i) != '\n') i++;
                out.append(sql, s, i); continue;
            }
            if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                int s = i, depth = 1; i += 2;
                while (i < n && depth > 0) {
                    if (i + 1 < n && sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') { depth++; i += 2; }
                    else if (i + 1 < n && sql.charAt(i) == '*' && sql.charAt(i + 1) == '/') { depth--; i += 2; }
                    else i++;
                }
                out.append(sql, s, i); continue;
            }
            if (c == '\'') {
                int s = i; i++;
                while (i < n) {
                    if (sql.charAt(i) == '\'') {
                        if (i + 1 < n && sql.charAt(i + 1) == '\'') { i += 2; continue; }
                        i++; break;
                    }
                    i++;
                }
                out.append(sql, s, i); continue;
            }
            if (c == '"') {
                int s = i; i++;
                while (i < n) {
                    if (sql.charAt(i) == '"') {
                        if (i + 1 < n && sql.charAt(i + 1) == '"') { i += 2; continue; }
                        i++; break;
                    }
                    i++;
                }
                out.append(sql, s, i); continue;
            }
            if (c == '$') {
                int tagEnd = dollarTagEnd(sql, i);
                if (tagEnd > i) {
                    String tag = sql.substring(i, tagEnd);
                    int close = sql.indexOf(tag, tagEnd);
                    int end = close < 0 ? n : close + tag.length();
                    out.append(sql, i, end); i = end; continue;
                }
            }

            if (isIdentStart(c)) {
                int start = i; i++;
                while (i < n && isIdentPart(sql.charAt(i))) i++;
                String word = sql.substring(start, i);
                if (word.equalsIgnoreCase("select")) {
                    // look ahead across whitespace/comments for FROM
                    int j = skipWsAndComments(sql, i);
                    if (matchesKeyword(sql, j, "from")) {
                        out.append(word).append(" 1");
                        out.append(sql, i, j == i ? i : j); // preserve inter-token whitespace
                        i = j;
                        continue;
                    }
                }
                out.append(word);
                continue;
            }
            out.append(c);
            i++;
        }
        return out.toString();
    }

    private static int skipWsAndComments(String sql, int i) {
        int n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);
            if (Character.isWhitespace(c)) { i++; continue; }
            if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                i += 2; while (i < n && sql.charAt(i) != '\n') i++; continue;
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
            break;
        }
        return i;
    }

    // ── character helpers ────────────────────────────────────────────────────

    private static boolean isIdentStart(char c) {
        return Character.isLetter(c) || c == '_';
    }

    private static boolean isIdentPart(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$';
    }

    /**
     * If {@code sql[i] == '$'} begins a dollar-quote tag ({@code $$} or
     * {@code $tag$}), return the index just past the opening tag; otherwise
     * return {@code i} (not a dollar quote - e.g. a positional parameter $1).
     */
    private static int dollarTagEnd(String sql, int i) {
        int n = sql.length();
        if (i >= n || sql.charAt(i) != '$') return i;
        int j = i + 1;
        while (j < n) {
            char c = sql.charAt(j);
            if (c == '$') return j + 1;           // closing $ of the tag
            if (!(Character.isLetterOrDigit(c) || c == '_')) return i; // e.g. $1 param
            j++;
        }
        return i;
    }
}
