package org.particledb;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * How a bound parameter becomes SQL TEXT.
 *
 * <p>This driver does CLIENT-SIDE substitution: {@code setString(1, v)} does not
 * ship a typed bind, it renders {@code v} into the statement. That makes the
 * renderer the driver's ONLY defence against SQL injection, and it had no test
 * of its own - {@code PlaceholderScanTest} covers which {@code ?} is a
 * placeholder, not what replaces it.
 *
 * <p>The escaping contract this relies on was checked against the real
 * PostgreSQL 17.9 on this box and against a live ParticleDB: both report
 * {@code standard_conforming_strings = on}, both treat a backslash inside a
 * plain {@code '…'} literal as an ordinary character
 * ({@code length('a\b') = 3}), and both take {@code ''} as the embedded-quote
 * escape. Doubling the quote is therefore sufficient AND necessary - if the
 * server ever flipped {@code standard_conforming_strings} off, the
 * backslash cases below would become live injection vectors, which is exactly
 * why they are asserted here rather than assumed.
 */
public class LiteralRenderingTest {

    private static String render(String sql, Object... params) {
        return PdbPreparedStatement.interpolate(sql, params);
    }

    // ── injection ─────────────────────────────────────────────────────────
    /**
     * The classic payloads. Each must come out as ONE quoted literal, with the
     * statement's own structure untouched - no unbalanced quote, no second
     * statement, no comment that swallows the rest of the query.
     */
    @Test
    void quoteBearingValuesCannotEscapeTheirLiteral() {
        String[] payloads = {
                "'; DROP TABLE users; --",
                "' OR '1'='1",
                "\\'; DROP TABLE users; --",
                "\\\\'; DROP TABLE users; --",
                "admin'--",
                "'/*",
                "*/'",
                "$$; DROP TABLE users; --",
                "a'b'c",
                "''",
        };
        for (String p : payloads) {
            String out = render("SELECT * FROM t WHERE name = ?", p);
            assertTrue(out.startsWith("SELECT * FROM t WHERE name = '"),
                    "payload broke the prefix: " + out);
            assertTrue(out.endsWith("'"), "payload broke the suffix: " + out);
            String body = out.substring("SELECT * FROM t WHERE name = '".length(),
                    out.length() - 1);
            // Inside the literal every quote must be DOUBLED - that is what
            // makes the closing quote unreachable from the payload.
            assertEquals(p.replace("'", "''"), body,
                    "quote doubling wrong for payload: " + p);
            assertEquals(evenQuotes(out), true,
                    "unbalanced quotes after rendering: " + out);
            assertFalse(out.contains("DROP TABLE users; --\n"),
                    "payload escaped the literal: " + out);
        }
    }

    /**
     * POSITIVE CONTROL for the above: an ordinary value renders EXACTLY as
     * itself. Without this, "every payload came out quoted" could be satisfied
     * by a renderer that mangles or drops everything.
     */
    @Test
    void ordinaryValuesRenderVerbatimInsideTheirQuotes() {
        assertEquals("SELECT * FROM t WHERE name = 'alice'",
                render("SELECT * FROM t WHERE name = ?", "alice"));
        assertEquals("SELECT * FROM t WHERE name = 'Ünïcödé ☃'",
                render("SELECT * FROM t WHERE name = ?", "Ünïcödé ☃"));
        assertEquals("SELECT * FROM t WHERE name = ''",
                render("SELECT * FROM t WHERE name = ?", ""));
    }

    /**
     * A backslash inside the VALUE must survive as a backslash, not be
     * re-escaped. Both engines run with {@code standard_conforming_strings=on},
     * so {@code 'a\b'} is three characters; doubling the backslash would store
     * four and silently corrupt every Windows path and regex an application
     * writes.
     */
    @Test
    void backslashInAValueIsNotRewritten() {
        assertEquals("SELECT ? FROM t".replace("?", "'C:\\Users\\bob'"),
                render("SELECT ? FROM t", "C:\\Users\\bob"));
        assertEquals("SELECT '\\'", render("SELECT ?", "\\"));
    }

    // ── numeric literals ──────────────────────────────────────────────────
    /**
     * A {@code Double}/{@code Float} NaN or infinity has no bare SQL literal.
     * {@code sb.append(v)} emitted the tokens {@code NaN} / {@code Infinity} /
     * {@code -Infinity}, which reach the parser as IDENTIFIERS: the statement
     * fails at parse, or - if a column of that name exists - binds the WRONG
     * COLUMN with no error at all.
     */
    @Test
    void specialFloatingPointValuesGetTheirQuotedSqlForm() {
        assertEquals("SELECT ('NaN'::float8)", render("SELECT ?", Double.NaN));
        assertEquals("SELECT ('Infinity'::float8)", render("SELECT ?", Double.POSITIVE_INFINITY));
        assertEquals("SELECT ('-Infinity'::float8)", render("SELECT ?", Double.NEGATIVE_INFINITY));
        assertEquals("SELECT ('NaN'::float4)", render("SELECT ?", Float.NaN));
        assertEquals("SELECT ('Infinity'::float4)", render("SELECT ?", Float.POSITIVE_INFINITY));
        assertEquals("SELECT ('-Infinity'::float4)", render("SELECT ?", Float.NEGATIVE_INFINITY));
        for (Object v : new Object[]{Double.NaN, Float.POSITIVE_INFINITY}) {
            assertFalse(render("SELECT ?", v).matches(".*[^']\\b(NaN|Infinity)\\b.*"),
                    "special value leaked as a bare identifier for " + v);
        }
    }

    /** Integers, floats and decimals retain their types on the text path. */
    @Test
    void numericValuesKeepTheirType() {
        assertEquals("SELECT 42", render("SELECT ?", 42));
        // Negative: parenthesised, so a preceding minus cannot form a comment.
        assertEquals("SELECT (-7)", render("SELECT ?", -7L));
        assertEquals("SELECT ('1.5'::float8)", render("SELECT ?", 1.5d));
        assertEquals("SELECT ('0.0'::float8)", render("SELECT ?", 0.0d));
        assertEquals("SELECT ('3.10'::numeric)", render("SELECT ?", new BigDecimal("3.10")));
        assertEquals("SELECT true", render("SELECT ?", Boolean.TRUE));
        assertEquals("SELECT false", render("SELECT ?", Boolean.FALSE));
        assertEquals("SELECT NULL", render("SELECT ?", new Object[]{null}));
    }

    // ── temporal + misc literals ──────────────────────────────────────────
    /**
     * The temporal types render through their {@code toString()} - which is
     * PG-parseable - and, critically, are QUOTED. An unquoted
     * {@code 2026-03-04} is an arithmetic expression that evaluates to 2019.
     */
    @Test
    void temporalValuesAreQuotedAndPgParseable() {
        assertEquals("SELECT '2026-03-04'::date", render("SELECT ?", Date.valueOf("2026-03-04")));
        assertEquals("SELECT '05:06:07'::time", render("SELECT ?", Time.valueOf("05:06:07")));
        assertEquals("SELECT '2026-03-04 05:06:07.123456789'::timestamp",
                render("SELECT ?", Timestamp.valueOf("2026-03-04 05:06:07.123456789")));
        UUID u = UUID.fromString("0b3f1d0e-1111-2222-3333-444455556666");
        assertEquals("SELECT '0b3f1d0e-1111-2222-3333-444455556666'::uuid", render("SELECT ?", u));
    }

    /** {@code byte[]} renders as PG's hex bytea form, lowercase and quoted. */
    @Test
    void byteArrayRendersAsHexBytea() {
        assertEquals("SELECT '\\xdeadbeef'::bytea",
                render("SELECT ?", (Object) new byte[]{(byte) 0xDE, (byte) 0xAD,
                        (byte) 0xBE, (byte) 0xEF}));
        assertEquals("SELECT '\\x00ff'::bytea",
                render("SELECT ?", (Object) new byte[]{0x00, (byte) 0xFF}));
        assertEquals("SELECT '\\x'::bytea", render("SELECT ?", (Object) new byte[0]));
    }

    // ── multi-parameter ordering ──────────────────────────────────────────
    /**
     * Parameters must land in their own slots. A payload in one position must
     * not shift the others - the class of defect the {@code ??} escape fix was
     * about, re-asserted here from the VALUE side.
     */
    @Test
    void parametersLandInTheirOwnSlots() {
        assertEquals("INSERT INTO t VALUES ('a''b', 2, NULL, 'd')",
                render("INSERT INTO t VALUES (?, ?, ?, ?)", "a'b", 2, null, "d"));
        // A value containing a '?' must not be rescanned as a placeholder.
        assertEquals("SELECT * FROM t WHERE a = 'x?y' AND b = 'z'",
                render("SELECT * FROM t WHERE a = ? AND b = ?", "x?y", "z"));
        // A value containing the literal-operator escape likewise.
        assertEquals("SELECT * FROM t WHERE a = '??' AND b = 1",
                render("SELECT * FROM t WHERE a = ? AND b = ?", "??", 1));
    }

    /** Quotes are balanced iff every {@code '} is matched. */
    private static boolean evenQuotes(String s) {
        int n = 0;
        for (int i = 0; i < s.length(); i++) if (s.charAt(i) == '\'') n++;
        return n % 2 == 0;
    }
}
