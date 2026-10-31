package org.particledb;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit coverage for F8 - the bind-placeholder scanner
 * ({@link PdbPreparedStatement#placeholderPositions}). A {@code ?} inside a
 * string, quoted identifier, dollar-quoted body, or comment is NOT a bind
 * parameter; the count must match what interpolate() substitutes so setXxx()
 * never over/under-runs. No server required.
 */
public class PlaceholderScanTest {

    @Test
    void countsOnlyRealPlaceholders() {
        assertEquals(0, PdbPreparedStatement.placeholderPositions("SELECT 1").size());
        assertEquals(2, PdbPreparedStatement.placeholderPositions(
                "SELECT * FROM t WHERE a = ? AND b = ?").size());
    }

    @Test
    void ignoresQuestionMarksInStringsIdentifiersCommentsAndDollarQuotes() {
        // Exactly ONE real placeholder; every other '?' is inside a
        // string / block comment / line comment / dollar-quoted body.
        String sql = "SELECT ? /* ? */, '$$ ? $$', \"c?l\", $tag$ a?b $tag$ -- trailing ?\n";
        List<Integer> pos = PdbPreparedStatement.placeholderPositions(sql);
        assertEquals(1, pos.size(), "only the first ? is a real placeholder");
        assertEquals('?', sql.charAt(pos.get(0)));
    }

    @Test
    void escapedQuotesInsideStringsDoNotEndTheString() {
        // The '' is an escaped quote - the ? stays inside the string literal.
        assertEquals(0, PdbPreparedStatement.placeholderPositions(
                "SELECT 'it''s a ? here'").size());
    }

    @Test
    void interpolateSubstitutesOnlyRealPlaceholders() {
        String out = PdbPreparedStatement.interpolate(
                "SELECT ?, '? literal', ?", new Object[]{1, 2});
        assertEquals("SELECT 1, '? literal', 2", out);
    }

    // ---- GH #40: '??' is the JDBC escape for a LITERAL '?' operator ----------
    // PostgreSQL spells jsonb/hstore key-existence '?', '?|', '?&'. Treating the
    // escaped pair as two binds mis-aligned every later parameter and rendered
    // the unbound slots as "NULL"+"NULL" - the NULLNULL the server choked on.

    @Test
    void escapedQuestionMarkIsNotABindPlaceholder() {
        assertEquals(0, PdbPreparedStatement.placeholderPositions(
                "SELECT count(*) FROM j WHERE d ?? 'k'").size());
        assertEquals(0, PdbPreparedStatement.placeholderPositions(
                "SELECT count(*) FROM j WHERE d ??| array['a','b']").size());
        assertEquals(0, PdbPreparedStatement.placeholderPositions(
                "SELECT count(*) FROM j WHERE d ??& array['a','b']").size());
    }

    @Test
    void escapedQuestionMarkRendersAsTheLiteralOperator() {
        // Must collapse even with nothing bound, or '??' reaches the server.
        assertEquals("SELECT count(*) FROM j WHERE d ? 'k'",
                PdbPreparedStatement.interpolate(
                        "SELECT count(*) FROM j WHERE d ?? 'k'", new Object[0]));
        assertEquals("SELECT count(*) FROM j WHERE d ?| array['a']",
                PdbPreparedStatement.interpolate(
                        "SELECT count(*) FROM j WHERE d ??| array['a']", new Object[0]));
        assertEquals("SELECT count(*) FROM j WHERE d ?& array['a']",
                PdbPreparedStatement.interpolate(
                        "SELECT count(*) FROM j WHERE d ??& array['a']", new Object[0]));
    }

    @Test
    void escapeDoesNotShiftLaterParameters() {
        // The reported Conductor shape: containment bind + a key-existence
        // operator. The bind must stay on '@>' and NOT slide into the operator.
        String sql = "SELECT count(*) FROM archived_workflow "
                + "WHERE index_data @> ? AND json_data ?? 'workflowId'";
        assertEquals(1, PdbPreparedStatement.placeholderPositions(sql).size());
        String out = PdbPreparedStatement.interpolate(sql, new Object[]{"{a,b}"});
        assertEquals("SELECT count(*) FROM archived_workflow "
                + "WHERE index_data @> '{a,b}' AND json_data ? 'workflowId'", out);
        assertFalse(out.contains("NULLNULL"));
    }

    @Test
    void escapeBeforeAndAfterRealPlaceholdersKeepsOrder() {
        String sql = "SELECT * FROM t WHERE d ?? 'k' AND w = ? AND e ??| a AND x = ?";
        assertEquals(2, PdbPreparedStatement.placeholderPositions(sql).size());
        assertEquals("SELECT * FROM t WHERE d ? 'k' AND w = 'w1' AND e ?| a AND x = 9",
                PdbPreparedStatement.interpolate(sql, new Object[]{"w1", 9}));
    }

    @Test
    void escapeInsideStringsAndCommentsIsLeftAlone() {
        // Not a placeholder and not an escape - quoted/commented text is verbatim.
        String sql = "SELECT 'a??b' /* c??d */, ? -- e??f\n";
        assertEquals(1, PdbPreparedStatement.placeholderPositions(sql).size());
        assertEquals("SELECT 'a??b' /* c??d */, 1 -- e??f\n",
                PdbPreparedStatement.interpolate(sql, new Object[]{1}));
    }

    @Test
    void tripleQuestionMarkIsEscapePlusPlaceholder() {
        // '??' escapes, the trailing lone '?' still binds (matches PgJDBC).
        String sql = "SELECT * FROM t WHERE d ??? ";
        assertEquals(1, PdbPreparedStatement.placeholderPositions(sql).size());
        assertEquals("SELECT * FROM t WHERE d ?7 ",
                PdbPreparedStatement.interpolate(sql, new Object[]{7}));
    }
}
