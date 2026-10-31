package org.particledb;

import org.junit.jupiter.api.Test;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransactionRollbackException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Pins the wire-code → SQLSTATE table. No server needed, so this runs
 * everywhere and cannot be skipped; {@link SqlStateMappingTest} proves the same
 * table over a live wire.
 *
 * <p>The expected values are transcribed from the server's own encoder,
 * {@code sqlstate_to_wire_code} / {@code mod pdb_err} in
 * the server's native-wire codec.
 */
public class PdbSqlStateTest {

    /** All-digit SQLSTATEs are the wire code - surfaced verbatim. */
    @Test
    void allDigitCodesAreTheSqlState() {
        assertEquals("23505", PdbSqlState.forWireCode(23505)); // unique_violation
        assertEquals("23502", PdbSqlState.forWireCode(23502)); // not_null_violation
        assertEquals("23503", PdbSqlState.forWireCode(23503)); // foreign_key_violation
        assertEquals("23514", PdbSqlState.forWireCode(23514)); // check_violation
        assertEquals("22012", PdbSqlState.forWireCode(22012)); // division_by_zero
        assertEquals("42601", PdbSqlState.forWireCode(42601)); // syntax_error
        assertEquals("42703", PdbSqlState.forWireCode(42703)); // undefined_column
        assertEquals("42501", PdbSqlState.forWireCode(42501)); // insufficient_privilege
        assertEquals("42804", PdbSqlState.forWireCode(42804)); // datatype_mismatch
        assertEquals("25006", PdbSqlState.forWireCode(25006)); // read_only_sql_transaction
        assertEquals("40001", PdbSqlState.forWireCode(40001)); // serialization_failure
    }

    /** Retry loops key on 40001 - it must survive the mapping exactly. */
    @Test
    void serializationFailureIsPreservedExactly() {
        assertEquals("40001", PdbSqlState.forWireCode(40001));
        SQLException e = PdbSqlState.toSqlException("conflict", 40001, null);
        assertEquals("40001", e.getSQLState());
        assertEquals(40001, e.getErrorCode());
        assertTrue(e instanceof SQLTransactionRollbackException);
    }

    /**
     * The 6xxxx band maps BACK to the letter-bearing SQLSTATE it stands for.
     * The server's {@code SQLSTATE_WIRE_BAND} and {@link PdbSqlState}'s table
     * are the same set of pairs (the server's
     * {@code native_sqlstate_band_matches_the_jdbc_driver} test diffs them), so
     * the encoding is a bijection.
     */
    @Test
    void sixtyThousandBandReversesToLetterBearingSqlStates() {
        assertEquals("XX000", PdbSqlState.forWireCode(60000)); // internal_error
        assertEquals("42P01", PdbSqlState.forWireCode(60001)); // undefined_table
        assertEquals("42P02", PdbSqlState.forWireCode(60002)); // undefined_parameter
        assertEquals("25P02", PdbSqlState.forWireCode(60003)); // in_failed_sql_transaction
        assertEquals("25P01", PdbSqlState.forWireCode(60004)); // no_active_sql_transaction
        assertEquals("40P01", PdbSqlState.forWireCode(60005)); // deadlock_detected
        assertEquals("55P03", PdbSqlState.forWireCode(60006)); // lock_not_available
        assertEquals("0A000", PdbSqlState.forWireCode(60007)); // feature_not_supported
        assertEquals("22P02", PdbSqlState.forWireCode(60008)); // invalid_text_representation
        assertEquals("3B001", PdbSqlState.forWireCode(60009)); // invalid_savepoint_specification
        assertEquals("28P01", PdbSqlState.forWireCode(60010)); // invalid_password
        assertEquals("22P04", PdbSqlState.forWireCode(60011)); // bad_copy_file_format
        assertEquals("2201B", PdbSqlState.forWireCode(60012)); // invalid_regular_expression
        assertEquals("2202E", PdbSqlState.forWireCode(60016)); // array_subscript_error
        assertEquals("2BP01", PdbSqlState.forWireCode(60019)); // dependent_objects_still_exist
        assertEquals("3F000", PdbSqlState.forWireCode(60021)); // invalid_schema_name
        assertEquals("42P07", PdbSqlState.forWireCode(60024)); // duplicate_table
        assertEquals("428C9", PdbSqlState.forWireCode(60032)); // generated_always
        assertEquals("57P01", PdbSqlState.forWireCode(60034)); // admin_shutdown
        assertEquals("08P01", PdbSqlState.forWireCode(60036)); // protocol_violation
        assertEquals("P0001", PdbSqlState.forWireCode(60037)); // raise_exception
        assertEquals("HY000", PdbSqlState.forWireCode(60038),
                "the first value past the band is unassigned");
    }

    /** 40P01 (deadlock) is retriable too and must not be flattened. */
    @Test
    void deadlockBandEntryIsRetriable() {
        SQLException e = PdbSqlState.toSqlException("deadlock", 60005, null);
        assertEquals("40P01", e.getSQLState());
        assertEquals(60005, e.getErrorCode(), "vendor code keeps the wire value");
        assertTrue(e instanceof SQLTransactionRollbackException);
    }

    /** A leading-zero SQLSTATE loses its zero to the u16 - pad it back. */
    @Test
    void leadingZeroSqlStatesArePaddedBack() {
        assertEquals("02000", PdbSqlState.forWireCode(2000));  // no_data
        assertEquals("08006", PdbSqlState.forWireCode(8006));  // connection_failure
        assertEquals("01000", PdbSqlState.forWireCode(1000));  // warning
    }

    /** The generic code, and anything unrecognized, is HY000 - never null. */
    @Test
    void unknownCodesGetTheGeneralErrorNeverNull() {
        assertEquals("HY000", PdbSqlState.forWireCode(PdbSqlState.ERR_GENERIC_SQL));
        assertEquals("HY000", PdbSqlState.forWireCode(999));
        assertEquals("HY000", PdbSqlState.forWireCode(60999),
                "an unknown band value cannot be reconstructed — general error");
        assertEquals("HY000", PdbSqlState.forWireCode(65535));
        for (int code = 0; code <= 65535; code++) {
            String state = PdbSqlState.forWireCode(code);
            assertNotNull(state, "code " + code + " produced a null SQLSTATE");
            assertEquals(5, state.length(),
                    "code " + code + " produced a non-5-char SQLSTATE: " + state);
        }
    }

    /** Pre-SQLSTATE protocol codes get a connection/protocol class, not HY000. */
    @Test
    void protocolCodesGetAConnectionClass() {
        assertEquals("08006", PdbSqlState.forWireCode(0x00)); // GOAWAY / closed
        assertEquals("08P01", PdbSqlState.forWireCode(0x01)); // protocol error
        assertEquals("08P01", PdbSqlState.forWireCode(0x02)); // unknown message type
        assertEquals("53300", PdbSqlState.forWireCode(0x11)); // too many connections
        assertEquals("28000", PdbSqlState.forWireCode(0x13)); // auth required
        assertEquals("28P01", PdbSqlState.forWireCode(0x14)); // auth failed
        assertEquals("26000", PdbSqlState.forWireCode(26));   // prepared stmt missing
    }

    /** JDBC 4 exception subclasses for the classes frameworks branch on. */
    @Test
    void standardJdbcSubclassesArePicked() {
        assertTrue(PdbSqlState.toSqlException("dup", 23505, null)
                instanceof SQLIntegrityConstraintViolationException);
        assertTrue(PdbSqlState.toSqlException("syntax", 42601, null)
                instanceof SQLSyntaxErrorException);
        assertTrue(PdbSqlState.toSqlException("missing table", 60001, null)
                instanceof SQLSyntaxErrorException);
        assertTrue(PdbSqlState.toSqlException("div0", 22012, null)
                instanceof SQLDataException);
        assertTrue(PdbSqlState.toSqlException("closed", 0, null)
                instanceof SQLNonTransientConnectionException);
        // No standard subclass for these classes - plain SQLException.
        assertEquals(SQLException.class,
                PdbSqlState.toSqlException("aborted", 60003, null).getClass());
        assertEquals(SQLException.class,
                PdbSqlState.toSqlException("generic", 1064, null).getClass());
    }

    /** The cause is preserved so the wire exception stays reachable. */
    @Test
    void causeIsPreserved() {
        RuntimeException cause = new RuntimeException("wire");
        SQLException e = PdbSqlState.toSqlException("boom", 23505, cause);
        assertSame(cause, e.getCause());
    }
}
