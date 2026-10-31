package org.particledb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Pure string-transform unit tests for {@link PgCatalogAdapter}. No server  -
 * every case asserts the exact shape of the rewritten SQL.
 *
 * <p>The inputs marked "Flyway 9.0.4" / "Conductor" are the LITERAL SQL those
 * tools emit (captured by decompiling flyway-core-9.0.4.jar's
 * PostgreSQLSchema/PostgreSQLTable and grepping the Conductor
 * postgres-persistence DAOs).
 */
public class PgCatalogAdapterTest {

    // ── no-op / passthrough ──────────────────────────────────────────────────

    @Test
    void plainQueryUnchanged() {
        String sql = "SELECT id, name FROM users WHERE id = 42";
        assertEquals(sql, PgCatalogAdapter.rewrite(sql));
    }

    @Test
    void servedCatalogTablesUnchanged() {
        // pg_class / pg_namespace / pg_attribute / pg_type ARE served by PDB  -
        // must not be rewritten.
        String sql = "SELECT relname FROM pg_catalog.pg_class c "
                   + "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace";
        assertEquals(sql, PgCatalogAdapter.rewrite(sql));
    }

    @Test
    void nullAndEmptyUnchanged() {
        assertNull(PgCatalogAdapter.rewrite(null));
        assertEquals("", PgCatalogAdapter.rewrite(""));
    }

    // ── each of the six relations gets substituted ───────────────────────────

    @Test
    void pgDependSubstituted() {
        String out = PgCatalogAdapter.rewrite("SELECT * FROM pg_depend");
        assertFalse(referencesRelation(out, "pg_depend"),
                "pg_depend should be gone: " + out);
        assertTrue(out.contains("AS deptype"), out);
        assertTrue(out.contains("WHERE false"), out);
    }

    @Test
    void pgProcSubstituted() {
        String out = PgCatalogAdapter.rewrite("SELECT proname FROM pg_proc");
        assertFalse(referencesRelation(out, "pg_proc"), out);
        assertTrue(out.contains("AS proname"), out);
        assertTrue(out.contains("AS prokind"), out);
        // char pseudo-type is NOT emitted - PDB has no "char"
        assertFalse(out.contains("\"char\""), out);
    }

    @Test
    void pgExtensionSubstituted() {
        String out = PgCatalogAdapter.rewrite("SELECT extname FROM pg_extension");
        assertFalse(referencesRelation(out, "pg_extension"), out);
        assertTrue(out.contains("AS extname"), out);
        assertTrue(out.contains("AS extnamespace"), out);
        assertTrue(out.contains("AS extowner"), out);
    }

    @Test
    void pgRolesSubstituted() {
        String out = PgCatalogAdapter.rewrite("SELECT rolname FROM pg_roles");
        assertFalse(referencesRelation(out, "pg_roles"), out);
        assertTrue(out.contains("AS rolname"), out);
    }

    @Test
    void pgInheritsSubstituted() {
        String out = PgCatalogAdapter.rewrite("SELECT inhrelid FROM pg_inherits");
        assertFalse(referencesRelation(out, "pg_inherits"), out);
        assertTrue(out.contains("AS inhrelid"), out);
        assertTrue(out.contains("AS inhparent"), out);
        assertTrue(out.contains("AS inhseqno"), out);
    }

    @Test
    void pgTablesBackedByInformationSchema() {
        String out = PgCatalogAdapter.rewrite("SELECT tablename FROM pg_tables");
        assertFalse(referencesRelation(out, "pg_tables"), out);
        assertTrue(out.contains("information_schema.tables"),
                "pg_tables must be information_schema-backed: " + out);
        assertTrue(out.contains("table_name AS tablename"), out);
        assertTrue(out.contains("table_schema AS schemaname"), out);
    }

    // ── qualifier + case-insensitivity ───────────────────────────────────────

    @Test
    void pgCatalogQualifierRewritten() {
        String out = PgCatalogAdapter.rewrite("SELECT * FROM pg_catalog.pg_depend");
        assertFalse(out.toLowerCase().contains("pg_catalog.pg_depend"), out);
        assertFalse(out.toLowerCase().contains("pg_depend "), out);
        assertTrue(out.contains("WHERE false"), out);
    }

    @Test
    void caseInsensitiveRelationName() {
        String out = PgCatalogAdapter.rewrite("SELECT * FROM PG_TABLES");
        assertFalse(out.toUpperCase().matches(".*\\bPG_TABLES\\b(?!\\s*AS).*")
                        && referencesRelation(out.toLowerCase(), "pg_tables"),
                out);
        assertTrue(out.contains("information_schema.tables"), out);
    }

    // ── alias preservation ───────────────────────────────────────────────────

    @Test
    void bareAliasPreserved() {
        String out = PgCatalogAdapter.rewrite(
                "SELECT d.objid FROM pg_depend d WHERE d.deptype = 'e'");
        assertTrue(out.matches(".*\\) d\\b.*"),
                "alias 'd' must be re-attached: " + out);
        // the WHERE clause referencing the alias survives
        assertTrue(out.contains("d.deptype = 'e'"), out);
    }

    @Test
    void asAliasPreserved() {
        String out = PgCatalogAdapter.rewrite("SELECT * FROM pg_proc AS p");
        assertTrue(out.matches(".*\\) p\\b.*"), "alias 'p' via AS: " + out);
    }

    @Test
    void multiCharAliasPreserved() {
        String out = PgCatalogAdapter.rewrite(
                "SELECT dep.objid FROM pg_depend dep WHERE dep.deptype='e'");
        assertTrue(out.matches(".*\\) dep\\b.*"), "alias 'dep': " + out);
    }

    @Test
    void followKeywordNotSwallowedAsAlias() {
        // WHERE must NOT be consumed as an alias.
        String out = PgCatalogAdapter.rewrite("SELECT * FROM pg_roles WHERE rolname='x'");
        assertTrue(out.contains("WHERE rolname='x'"),
                "WHERE must survive, not become an alias: " + out);
        assertFalse(out.matches(".*\\) WHERE\\b.*\\bWHERE\\b.*"), out);
    }

    @Test
    void joinKeywordNotSwallowed() {
        String out = PgCatalogAdapter.rewrite(
                "SELECT * FROM pg_extension e LEFT JOIN pg_roles ON true");
        // both substituted; LEFT survives as a join keyword, not an alias
        assertFalse(referencesRelation(out, "pg_extension"), out);
        assertFalse(referencesRelation(out, "pg_roles"), out);
        assertTrue(out.contains("LEFT JOIN"), out);
    }

    // ── word-boundary / no over-match ────────────────────────────────────────

    @Test
    void longerIdentifierNotMatched() {
        // pg_tablespace, pg_proc_oid_index, my_pg_tables must NOT be touched.
        String sql = "SELECT * FROM pg_tablespace, pg_proc_backup, my_pg_tables";
        assertEquals(sql, PgCatalogAdapter.rewrite(sql));
    }

    @Test
    void columnQualifierNotMatched() {
        // x.pg_tables is a column ref (contrived), must not be rewritten.
        String sql = "SELECT x.pg_tables FROM x";
        assertEquals(sql, PgCatalogAdapter.rewrite(sql));
    }

    // ── string-literal safety ────────────────────────────────────────────────

    @Test
    void relationNameInsideStringLiteralUntouched() {
        String sql = "SELECT 'querying pg_depend and pg_tables' AS msg";
        assertEquals(sql, PgCatalogAdapter.rewrite(sql));
    }

    @Test
    void relationNameInDoubleQuotedIdentifierUntouched() {
        String sql = "SELECT \"pg_tables\" FROM \"my table\"";
        assertEquals(sql, PgCatalogAdapter.rewrite(sql));
    }

    @Test
    void relationNameInDollarQuotedBodyUntouched() {
        String sql = "SELECT $tag$ pg_depend pg_tables $tag$ AS body";
        assertEquals(sql, PgCatalogAdapter.rewrite(sql));
    }

    @Test
    void positionalParamNotTreatedAsDollarQuote() {
        // $1 must NOT swallow the rest as a dollar-quoted string.
        String out = PgCatalogAdapter.rewrite(
                "SELECT * FROM pg_tables WHERE tablename = $1");
        assertTrue(out.contains("information_schema.tables"), out);
        assertTrue(out.contains("$1"), out);
    }

    // ── empty target-list normalisation ──────────────────────────────────────

    @Test
    void selectFromNormalised() {
        String out = PgCatalogAdapter.rewrite(
                "select exists(select from pg_tables where tablename = 'x')");
        assertTrue(out.toLowerCase().contains("select 1 from"),
                "empty SELECT list must become SELECT 1: " + out);
        assertFalse(out.toLowerCase().matches(".*select\\s+from.*"), out);
    }

    @Test
    void selectWithColumnsNotAltered() {
        // SELECT that already has a target list must not gain a spurious "1".
        String out = PgCatalogAdapter.rewrite("SELECT a, b FROM t");
        assertEquals("SELECT a, b FROM t", out);
    }

    @Test
    void selectFromInStringLiteralUntouched() {
        String sql = "SELECT 'select from x' AS lit";
        assertEquals(sql, PgCatalogAdapter.rewrite(sql));
    }

    // ── full real-world queries ──────────────────────────────────────────────

    @Test
    void flywayIsSchemaEmptyFullyRewritten() {
        // The LITERAL Flyway 9.0.4 PostgreSQLSchema.doEmpty() query.
        String sql = "SELECT EXISTS (\n"
            + "    SELECT c.oid FROM pg_catalog.pg_class c\n"
            + "    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
            + "    LEFT JOIN pg_catalog.pg_depend d ON d.objid = c.oid AND d.deptype = 'e'\n"
            + "    WHERE  n.nspname = ? AND d.objid IS NULL AND c.relkind IN ('r', 'v', 'S', 't')\n"
            + "  UNION ALL\n"
            + "    SELECT t.oid FROM pg_catalog.pg_type t\n"
            + "    JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace\n"
            + "    LEFT JOIN pg_catalog.pg_depend d ON d.objid = t.oid AND d.deptype = 'e'\n"
            + "    WHERE n.nspname = ? AND d.objid IS NULL AND t.typcategory NOT IN ('A', 'C')\n"
            + "  UNION ALL\n"
            + "    SELECT p.oid FROM pg_catalog.pg_proc p\n"
            + "    JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace\n"
            + "    LEFT JOIN pg_catalog.pg_depend d ON d.objid = p.oid AND d.deptype = 'e'\n"
            + "    WHERE n.nspname = ? AND d.objid IS NULL\n"
            + ")";
        String out = PgCatalogAdapter.rewrite(sql);
        // No missing relation survives.
        assertFalse(out.toLowerCase().contains("pg_depend"), out);
        assertFalse(out.toLowerCase().contains("pg_proc"), out);
        // The served relations remain.
        assertTrue(out.contains("pg_catalog.pg_class"), out);
        assertTrue(out.contains("pg_catalog.pg_type"), out);
        assertTrue(out.contains("pg_catalog.pg_namespace"), out);
        // The pg_depend alias 'd' is preserved so d.objid / d.deptype resolve.
        assertTrue(out.matches("(?s).*\\) d\\b.*"), out);
        // The pg_proc alias 'p' is preserved.
        assertTrue(out.matches("(?s).*\\) p\\b.*"), out);
    }

    @Test
    void flywayGetCurrentExtensionsRewritten() {
        String sql = "SELECT e.extname FROM pg_extension e "
            + "LEFT JOIN pg_namespace n ON n.oid = e.extnamespace "
            + "LEFT JOIN pg_roles r ON r.oid = e.extowner "
            + "WHERE n.nspname=? AND r.rolname=?";
        String out = PgCatalogAdapter.rewrite(sql);
        assertFalse(referencesRelation(out, "pg_extension"), out);
        assertFalse(referencesRelation(out, "pg_roles"), out);
        // pg_namespace is served ⇒ untouched.
        assertTrue(out.contains("pg_namespace n"), out);
        // aliases e and r preserved
        assertTrue(out.matches("(?s).*\\) e\\b.*"), out);
        assertTrue(out.matches("(?s).*\\) r\\b.*"), out);
    }

    @Test
    void flywayDoAllTablesRewritten() {
        String sql = "SELECT t.table_name FROM information_schema.tables t "
            + "LEFT JOIN pg_depend dep ON dep.objid = "
            + "(quote_ident(t.table_schema)||'.'||quote_ident(t.table_name))::regclass::oid "
            + "AND dep.deptype = 'e' "
            + "WHERE table_schema=? AND table_type='BASE TABLE' AND dep.objid IS NULL "
            + "AND NOT (SELECT EXISTS (SELECT inhrelid FROM pg_catalog.pg_inherits "
            + "WHERE inhrelid = (quote_ident(t.table_schema)||'.'||quote_ident(t.table_name))::regclass::oid))";
        String out = PgCatalogAdapter.rewrite(sql);
        assertFalse(out.toLowerCase().contains("pg_depend"), out);
        assertFalse(out.toLowerCase().contains("pg_inherits"), out);
        assertTrue(out.contains("information_schema.tables t"), out);
        assertTrue(out.matches("(?s).*\\) dep\\b.*"), out);
    }

    @Test
    void conductorGetNextPartitionIdRewritten() {
        // PostgresPartitionManagerDAO.getNextPartitionId
        String sql = "SELECT tablename FROM pg_tables WHERE tablename LIKE '%workflow%'";
        String out = PgCatalogAdapter.rewrite(sql);
        assertFalse(referencesRelation(out, "pg_tables"), out);
        assertTrue(out.contains("information_schema.tables"), out);
        assertTrue(out.contains("WHERE tablename LIKE '%workflow%'"), out);
    }

    @Test
    void conductorCheckShardExistsRewritten() {
        // PostgresPartitionManagerDAO / PostgresAuditPartitionManagerDAO.checkShardExists
        // - note the empty SELECT target list AND the pg_tables reference.
        String sql = "select exists(select from pg_tables where schemaname = 'public' and tablename = ?)";
        String out = PgCatalogAdapter.rewrite(sql);
        assertFalse(referencesRelation(out, "pg_tables"), out);
        assertTrue(out.contains("information_schema.tables"), out);
        assertTrue(out.toLowerCase().contains("select 1 from"),
                "empty target-list must be normalised: " + out);
        assertFalse(out.toLowerCase().matches(".*select\\s+from\\s+\\(.*"), out);
    }

    // ── idempotence ──────────────────────────────────────────────────────────

    @Test
    void rewriteIsIdempotent() {
        String sql = "SELECT tablename FROM pg_tables WHERE tablename LIKE 'x%'";
        String once = PgCatalogAdapter.rewrite(sql);
        String twice = PgCatalogAdapter.rewrite(once);
        assertEquals(once, twice, "rewrite must be idempotent");
    }

    // ── set_config: a GUC WRITE, not a value to be substituted away ──────────

    /**
     * The exact call Flyway's {@code changeCurrentSchemaTo} emits (after the
     * driver renders its bound parameter) becomes the {@code SET} the native
     * wire honours.
     *
     * <p>This is the regression pin for a rewrite that used to replace the
     * whole call with its second argument - so the caller got its own string
     * back, believed the schema pin had taken, and every subsequent unqualified
     * {@code CREATE TABLE} went to {@code public}.
     */
    @Test
    void flywaysSetConfigBecomesARealSet() {
        assertEquals("SET search_path TO 'fw'",
                PgCatalogAdapter.rewrite("SELECT set_config('search_path','fw',false)"));
    }

    @Test
    void setConfigTrailingSemicolonAndWhitespaceAndCaseAreAccepted() {
        assertEquals("SET search_path TO 'fw'",
                PgCatalogAdapter.rewrite("  /*c*/ SELECT  SET_CONFIG( 'search_path' , 'fw' , FALSE ) ; "));
    }

    /** {@code is_local=true} is {@code SET LOCAL}, not a plain {@code SET}. */
    @Test
    void setConfigLocalBecomesSetLocal() {
        assertEquals("SET LOCAL search_path TO 'fw'",
                PgCatalogAdapter.rewrite("SELECT set_config('search_path','fw',true)"));
    }

    /** {@code NULL} is PostgreSQL's spelling of {@code RESET}, not "set to ''". */
    @Test
    void setConfigNullBecomesReset() {
        assertEquals("RESET search_path",
                PgCatalogAdapter.rewrite("SELECT set_config('search_path', NULL, false)"));
    }

    /** A value containing a quote must survive the round trip escaped. */
    @Test
    void setConfigEscapesQuotesInTheValue() {
        assertEquals("SET search_path TO 'a''b'",
                PgCatalogAdapter.rewrite("SELECT set_config('search_path','a''b',false)"));
    }

    /**
     * The schema-qualified spelling pg_dump emits before every restore.
     * Declining it left that exact statement going to a wire with no
     * {@code set_config} function.
     */
    @Test
    void pgCatalogQualifiedSetConfigIsTranslatedToo() {
        assertEquals("SET search_path TO ''",
                PgCatalogAdapter.rewrite("SELECT pg_catalog.set_config('search_path', '', false)"));
    }

    /** It is not only about search_path - any parameter is a real write now. */
    @Test
    void setConfigWorksForAnyParameterName() {
        assertEquals("SET myapp.tenant TO '7'",
                PgCatalogAdapter.rewrite("SELECT set_config('myapp.tenant','7',false)"));
    }

    /**
     * Shapes the translation must DECLINE - it only models the lone-statement
     * call with literal arguments, exactly as {@code pg_wire.rs}'s own
     * {@code parse_set_config} does. Anything else falls back to the old
     * value-substitution rather than becoming a statement built from a guess.
     */
    @Test
    void nonWholeStatementSetConfigIsNotTurnedIntoASet() {
        // Embedded in a larger expression.
        assertNull(PgCatalogAdapter.parseWholeStatementSetConfig(
                "SELECT 'x' || set_config('search_path','fw',false)"));
        // Something follows the call.
        assertNull(PgCatalogAdapter.parseWholeStatementSetConfig(
                "SELECT set_config('search_path','fw',false) FROM t"));
        // Wrong arity.
        assertNull(PgCatalogAdapter.parseWholeStatementSetConfig(
                "SELECT set_config('search_path','fw')"));
        // A non-literal is_local (an unrendered bound parameter).
        assertNull(PgCatalogAdapter.parseWholeStatementSetConfig(
                "SELECT set_config('search_path','fw',?)"));
        // A parameter name that is not identifier-shaped must never be spliced
        // into a SET statement.
        assertNull(PgCatalogAdapter.parseWholeStatementSetConfig(
                "SELECT set_config('a; DROP TABLE t','fw',false)"));
        // A column reference named set_config.
        assertNull(PgCatalogAdapter.parseWholeStatementSetConfig(
                "SELECT t.set_config('search_path','fw',false)"));
    }

    /**
     * The declined shapes still take the old unwrap, so a client that had one
     * working is not newly broken by a {@code 42883}.
     */
    @Test
    void declinedShapesStillFallBackToTheUnwrap() {
        assertEquals("SELECT 'x' || 'fw'",
                PgCatalogAdapter.rewrite("SELECT 'x' || set_config('search_path','fw',false)"));
    }

    /**
     * {@code set_config(name, NULL, is_local)} - the RESET form - must keep its
     * SCOPE. The translation dropped {@code is_local} and emitted a plain
     * session-wide {@code RESET} for both, which destroyed the caller's session
     * value permanently: there was nothing left for end-of-transaction to
     * restore.
     *
     * <p>This intersection had NO test. The suite covered {@code is_local}
     * (a SET LOCAL case) and covered {@code NULL} (the session form), but never
     * both at once - which is exactly how the defect survived.
     *
     * <p>PostgreSQL 17.9, measured:
     * <pre>
     *   SELECT set_config('search_path','session_val',false);
     *   BEGIN;
     *   SELECT set_config('search_path',NULL,true);   -- reads as the default
     *   COMMIT;
     *   SHOW search_path;                             -- session_val is BACK
     * </pre>
     */
    @Test
    void the_null_form_keeps_its_scope() {
        assertEquals(
                "SET LOCAL search_path TO DEFAULT",
                PgCatalogAdapter.setConfigAsSessionStatement(
                        "SELECT set_config('search_path',NULL,true)"),
                "NULL + is_local=true is a TRANSACTION-scoped reset");
        assertEquals(
                "RESET search_path",
                PgCatalogAdapter.setConfigAsSessionStatement(
                        "SELECT set_config('search_path',NULL,false)"),
                "NULL + is_local=false is the session-wide reset");
        // The two must not be the same statement - that identity WAS the bug.
        assertNotEquals(
                PgCatalogAdapter.setConfigAsSessionStatement(
                        "SELECT set_config('search_path',NULL,true)"),
                PgCatalogAdapter.setConfigAsSessionStatement(
                        "SELECT set_config('search_path',NULL,false)"),
                "the local and session RESET forms must differ");
        // A non-null value already carried the scope; pinned so a future edit to
        // the null branch cannot regress the value branch with it.
        assertEquals(
                "SET LOCAL search_path TO 'x'",
                PgCatalogAdapter.setConfigAsSessionStatement(
                        "SELECT set_config('search_path','x',true)"));
        assertEquals(
                "SET search_path TO 'x'",
                PgCatalogAdapter.setConfigAsSessionStatement(
                        "SELECT set_config('search_path','x',false)"));
    }

    // ── helper ───────────────────────────────────────────────────────────────

    /**
     * True if {@code sql} still references {@code rel} as a bare relation token
     * (word boundary, case-insensitive) - i.e. the substitution did NOT catch
     * it. Substituted output no longer contains the token because every
     * inline sub-select uses only the column aliases, never the relation name.
     */
    private static boolean referencesRelation(String sql, String rel) {
        return sql.toLowerCase().matches("(?s).*\\b" + rel.toLowerCase() + "\\b.*");
    }
}
