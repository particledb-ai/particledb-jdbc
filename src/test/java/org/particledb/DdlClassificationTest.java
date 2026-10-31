package org.particledb;

import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link PdbConnection#looksLikeDdl} and the two small SQL-text helpers around
 * it. All three had NO TEST, and {@code looksLikeDdl} is load-bearing: it is
 * what decides, for every statement issued while {@code autoCommit == false}
 * and no transaction is open yet, whether the driver runs the statement on its
 * own instead of opening a transaction with it (inside an OPEN transaction a
 * DDL is sent as-is and the server refuses it with 25001).
 *
 * <p>Both directions of that decision matter, so both are asserted here:
 * <ul>
 *   <li>a FALSE NEGATIVE (a DDL not recognised) opens a transaction with the
 *       DDL, which the server then refuses (25001) - a Flyway migration's
 *       first statement would fail;</li>
 *   <li>a FALSE POSITIVE (ordinary DML mistaken for DDL) runs that DML
 *       OUTSIDE the caller's transaction, so the caller's later
 *       {@code rollback()} cannot undo it.</li>
 * </ul>
 */
public class DdlClassificationTest {

    // ── recognised: the statement kinds ParticleDB auto-commits ───────────
    @Test
    void ddlAndDclKeywordsAreRecognised() {
        String[] ddl = {
                "CREATE TABLE t (id INT)",
                "create table t (id int)",
                "CrEaTe InDeX i ON t (id)",
                "ALTER TABLE t ADD COLUMN c INT",
                "DROP TABLE t",
                "TRUNCATE t",
                "COMMENT ON TABLE t IS 'x'",
                "REINDEX TABLE t",
                "GRANT SELECT ON t TO app",
                "REVOKE SELECT ON t FROM app",
        };
        for (String s : ddl) {
            assertTrue(PdbConnection.looksLikeDdl(s), "not recognised as DDL: " + s);
        }
    }

    /**
     * Leading whitespace and SQL comments must not hide the keyword - a Flyway
     * migration file routinely opens with a comment header, and every statement
     * in it is DDL.
     */
    @Test
    void leadingWhitespaceAndCommentsDoNotHideTheKeyword() {
        assertTrue(PdbConnection.looksLikeDdl("   \n\t CREATE TABLE t (id INT)"));
        assertTrue(PdbConnection.looksLikeDdl("-- V1__init.sql\nCREATE TABLE t (id INT)"));
        assertTrue(PdbConnection.looksLikeDdl("/* header */ CREATE TABLE t (id INT)"));
        assertTrue(PdbConnection.looksLikeDdl(
                "-- one\n-- two\n/* three */\n  ALTER TABLE t ADD c INT"));
    }

    // ── NOT recognised: ordinary DML must never leave the transaction ────────
    /**
     * A false positive here runs DML outside the caller's transaction. These
     * are the near misses: statements that begin with, contain, or merely
     * resemble a DDL keyword but are DML or transaction control.
     */
    @Test
    void dmlAndTransactionControlAreNotDdl() {
        String[] notDdl = {
                "SELECT 1",
                "SELECT * FROM created_at",
                "INSERT INTO t VALUES (1)",
                "INSERT INTO t SELECT * FROM u",
                "UPDATE t SET c = 1",
                "DELETE FROM t",
                "MERGE INTO t USING u ON (t.id = u.id)",
                "BEGIN",
                "COMMIT",
                "ROLLBACK",
                "SET search_path TO public",
                "SHOW transaction_isolation",
                "WITH x AS (SELECT 1) SELECT * FROM x",
                "VALUES (1)",
                "EXPLAIN SELECT 1",
                // A DDL keyword that is NOT the leading token.
                "SELECT 'CREATE TABLE t' AS s",
                "UPDATE t SET note = 'DROP TABLE u'",
                "INSERT INTO grants VALUES (1)",
                // Identifier prefixes that merely START with a DDL keyword.
                "CREATED_AT_FN()",
                "DROPPED FROM t",
                "",
                "   ",
        };
        for (String s : notDdl) {
            assertFalse(PdbConnection.looksLikeDdl(s),
                    "wrongly classified as DDL (would run outside the caller's "
                    + "transaction): " + s);
        }
        assertFalse(PdbConnection.looksLikeDdl(null), "null must not classify as DDL");
    }

    /**
     * KNOWN LIMITATION, pinned deliberately rather than hidden: the check is a
     * FIRST-KEYWORD check, so a longer identifier that begins with a DDL keyword
     * and is followed by a word boundary is still read as DDL. {@code CREATED}
     * is not a statement anyone writes, so this costs nothing today - but the
     * assertion exists so that if the classifier is ever rewritten, the change
     * in this behaviour is a deliberate one and not a silent swing.
     */
    @Test
    void firstKeywordCheckIsPrefixExact_notWordExact() {
        // The scanner stops at the first non-letter, so "CREATED" is one token
        // and does NOT match "CREATE".
        assertFalse(PdbConnection.looksLikeDdl("CREATED something"));
        // But punctuation ends the token, so this DOES match.
        assertTrue(PdbConnection.looksLikeDdl("CREATE(x)"));
    }

    // ── isolation-level spelling ──────────────────────────────────────────
    /**
     * The four JDBC isolation constants must each map to their SQL spelling, and
     * everything else - including {@code TRANSACTION_NONE}, which the JDBC
     * javadoc excludes as an argument to {@code setTransactionIsolation} - must
     * map to nothing so the setter can refuse instead of storing a level it
     * never asked the server for.
     */
    @Test
    void isolationLevelSpellings() {
        assertEquals("READ UNCOMMITTED",
                PdbConnection.isolationLevelSql(Connection.TRANSACTION_READ_UNCOMMITTED));
        assertEquals("READ COMMITTED",
                PdbConnection.isolationLevelSql(Connection.TRANSACTION_READ_COMMITTED));
        assertEquals("REPEATABLE READ",
                PdbConnection.isolationLevelSql(Connection.TRANSACTION_REPEATABLE_READ));
        assertEquals("SERIALIZABLE",
                PdbConnection.isolationLevelSql(Connection.TRANSACTION_SERIALIZABLE));
        assertNull(PdbConnection.isolationLevelSql(Connection.TRANSACTION_NONE));
        assertNull(PdbConnection.isolationLevelSql(3));   // between REPEATABLE_READ(4)
        assertNull(PdbConnection.isolationLevelSql(-1));
        assertNull(PdbConnection.isolationLevelSql(999));
    }

    // ── identifier quoting ────────────────────────────────────────────────
    /**
     * {@code setSchema} interpolates the schema name into
     * {@code SET search_path TO …}, so the quoting is the only thing between a
     * caller-supplied name and injected SQL.
     */
    @Test
    void identifierQuotingIsInjectionSafe() {
        assertEquals("\"public\"", PdbConnection.quoteIdentifier("public"));
        assertEquals("\"MixedCase\"", PdbConnection.quoteIdentifier("MixedCase"));
        assertEquals("\"a\"\"b\"", PdbConnection.quoteIdentifier("a\"b"));
        assertEquals("\"x\"\"; DROP TABLE t; --\"",
                PdbConnection.quoteIdentifier("x\"; DROP TABLE t; --"));
        // A single quote is NOT special inside a double-quoted identifier and
        // must be left alone rather than doubled.
        assertEquals("\"o'brien\"", PdbConnection.quoteIdentifier("o'brien"));
    }

    /**
     * allowDdlOutsideTransaction applies to ONE statement only: a top-level
     * {@code ;} followed by anything but trivia makes a string multi-statement,
     * and a {@code ;} inside a literal, identifier, dollar body or comment
     * never does.
     */
    @Test
    void multiStatementDetection() {
        assertTrue(PdbPreparedStatement.isMultiStatement("CREATE TABLE a (id INT); INSERT INTO a VALUES (1)"));
        assertTrue(PdbPreparedStatement.isMultiStatement("CREATE TABLE a (id INT);\n-- c\n/* d */ SELECT 1"));
        assertTrue(PdbPreparedStatement.isMultiStatement("ALTER TABLE a ADD b INT;;DROP TABLE c"));
        assertFalse(PdbPreparedStatement.isMultiStatement("CREATE TABLE a (id INT)"));
        assertFalse(PdbPreparedStatement.isMultiStatement("CREATE TABLE a (id INT);"));
        assertFalse(PdbPreparedStatement.isMultiStatement("CREATE TABLE a (id INT);  -- tail\n /* x; y */ ;"));
        assertFalse(PdbPreparedStatement.isMultiStatement("COMMENT ON TABLE a IS 'x; DROP TABLE a'"));
        assertFalse(PdbPreparedStatement.isMultiStatement("CREATE TABLE \"a;b\" (id INT)"));
        assertFalse(PdbPreparedStatement.isMultiStatement(
                "CREATE FUNCTION f() RETURNS int AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql"));
        assertFalse(PdbPreparedStatement.isMultiStatement("-- a; b\nCREATE TABLE a (id INT)"));
        assertFalse(PdbPreparedStatement.isMultiStatement(null));
    }
}
