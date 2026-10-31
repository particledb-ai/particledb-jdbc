package org.particledb;

import org.particledb.wire.StreamState;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLTransactionRollbackException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A server-raised error reaches the caller in pgjdbc's shape:
 * {@code getMessage() == "ERROR: <message>"} and the exact SQLSTATE.
 *
 * <p>Why it matters: applications written against pgjdbc branch on that text.
 * Conductor's {@code PostgresPollDataDAO.insertOrUpdatePollData} ignores its own
 * {@code poll_data_update_check} trigger only when the message
 * {@code startsWith("ERROR: lastPollTime cannot be set to a lower value")}. On
 * PostgreSQL 17 that trigger fires on 33-64% of concurrent poll updates and every
 * one is ignored; through this driver the identical raise used to read
 * {@code "PDB wire error 1064: SQL execution error: lastPollTime … [P0001]"} with
 * SQLSTATE {@code HY000}, so Conductor re-threw and logged each one as a failed
 * poll. The inputs below are the server's frame text verbatim.
 *
 * <p>No server needed: {@link PollDataTriggerWireTest} proves the same over a
 * live wire.
 */
public class ServerErrorMessageShapeTest {

    /** The predicate Conductor applies, verbatim. */
    private static final String CONDUCTOR_SWALLOW = "ERROR: lastPollTime cannot be set to a lower value";

    private static SQLException fromServer(int code, String frameText) {
        return PdbConnection.mapException(StreamState.PdbWireException.fromServerFrame(code, frameText));
    }

    @Test
    void plpgsqlRaiseIsPgjdbcShapedWithItsSqlState() {
        SQLException e = fromServer(1064,
                "SQL execution error: lastPollTime cannot be set to a lower value [P0001]");
        assertEquals("ERROR: lastPollTime cannot be set to a lower value", e.getMessage());
        assertTrue(e.getMessage().startsWith(CONDUCTOR_SWALLOW), e.getMessage());
        assertEquals("P0001", e.getSQLState());
        assertEquals(1064, e.getErrorCode(), "the wire code stays the vendor code");
    }

    @Test
    void markerClassStillSelectsTheJdbcSubclass() {
        SQLException e = fromServer(1064,
                "SQL execution error: duplicate key value violates unique constraint \"t_pkey\" [23505]");
        assertInstanceOf(SQLIntegrityConstraintViolationException.class, e);
        assertEquals("23505", e.getSQLState());
        assertEquals("ERROR: duplicate key value violates unique constraint \"t_pkey\"", e.getMessage());
    }

    @Test
    void withoutAMarkerTheWireCodeDecides() {
        SQLException e = fromServer(40001, "could not serialize access due to concurrent update");
        assertInstanceOf(SQLTransactionRollbackException.class, e);
        assertEquals("40001", e.getSQLState());
        assertEquals("ERROR: could not serialize access due to concurrent update", e.getMessage());

        SQLException g = fromServer(1064, "SQL plan error: table not found: nope");
        assertEquals("HY000", g.getSQLState());
        assertEquals("ERROR: table not found: nope", g.getMessage());
    }

    @Test
    void bracketsThatAreNotASqlStateAreMessageText() {
        // "[abc]" is not five SQLSTATE characters: it stays in the message.
        SQLException e = fromServer(1064, "SQL execution error: bad value [abc]");
        assertEquals("HY000", e.getSQLState());
        assertEquals("ERROR: bad value [abc]", e.getMessage());
    }

    @Test
    void driverSynthesizedFailuresKeepTheWireForm() {
        // Not a server ErrorResponse: pgjdbc's own client-side failures carry no
        // severity, so neither does this one.
        SQLException e = PdbConnection.mapException(new StreamState.PdbWireException(0, "connection closed"));
        assertEquals("PDB wire error 0: connection closed", e.getMessage());
        assertEquals("08006", e.getSQLState());
    }
}
