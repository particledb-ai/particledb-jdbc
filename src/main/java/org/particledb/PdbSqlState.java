package org.particledb;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransactionRollbackException;
import java.util.HashMap;
import java.util.Map;

/**
 * Native-wire error code → PostgreSQL SQLSTATE.
 *
 * <p>JDBC error handling keys on {@link SQLException#getSQLState()}. Without it
 * a caller cannot tell a duplicate key (23505 - routinely handled, often
 * retried) from a syntax error (42601 - never retried), and every framework
 * that branches on the class (Spring's {@code SQLErrorCodeSQLExceptionTranslator},
 * Hibernate, Flyway) degrades to "uncategorized". This class is the driver's
 * single place where a wire code becomes a class, so no error path can leave it
 * null.
 *
 * <h2>The wire encoding</h2>
 *
 * The v1 error frame carries a {@code u16} code, so the server encodes SQLSTATEs
 * two ways (see {@code sqlstate_to_wire_code} / {@code SQLSTATE_WIRE_BAND} in
 * the server's native-wire codec):
 *
 * <ul>
 *   <li><b>All-digit SQLSTATEs ARE the code.</b> {@code 23505}, {@code 23502},
 *       {@code 23503}, {@code 23514}, {@code 22012}, {@code 42601}, {@code 42703},
 *       {@code 42501}, {@code 40001}, {@code 25006} … arrive as their own numeric
 *       value and are surfaced verbatim. A SQLSTATE with a leading zero
 *       ({@code 02000} key-not-found, {@code 08006}) parses to a value below
 *       10000 and is zero-padded back to five characters.</li>
 *   <li><b>Letter-bearing SQLSTATEs use the 6xxxx band.</b> {@code 42P01} has no
 *       u16 form, so the server maps each letter-bearing SQLSTATE it can raise
 *       into {@code 60000+} - a band no real SQLSTATE can occupy (PostgreSQL's
 *       numeric classes stop at 58; every class above it is letter-bearing).
 *       {@link #BAND} is the exact inverse of the server's table.</li>
 * </ul>
 *
 * <h2>Reversibility</h2>
 *
 * The band is a bijection: the server's {@code SQLSTATE_WIRE_BAND} and
 * {@link #BAND} are the same set of pairs, so every banded code recovers its
 * SQLSTATE exactly. The <em>numeric</em> half is
 * ambiguous by construction below 10000, where a zero-padded SQLSTATE
 * ({@code 08006} → 8006) shares the number space with the pre-SQLSTATE protocol
 * codes ({@code 0}–{@code 8} stream errors, {@code 0x10}–{@code 0x14} handshake,
 * {@code 26}, {@code 1064}). No real error SQLSTATE collides with one of those
 * values today - the collisions would need classes {@code 00}/{@code 01} that
 * are "success"/"warning" and never reach an error frame - so the decode below
 * carves out the known protocol codes first and treats the rest as zero-padded
 * SQLSTATEs. A future protocol code in the {@code 1000}–{@code 9999} range would
 * be indistinguishable from a leading-zero SQLSTATE; new codes belong in the
 * 6xxxx band or above.
 *
 * <h2>Never null</h2>
 *
 * An unrecognized code yields {@link #GENERAL_ERROR} ({@code HY000}, the SQL
 * standard's general error) rather than {@code null}. {@code 1064} - the
 * server's own "class genuinely unknown" fallback, and the code an older server
 * stamps on <em>every</em> SQL error - maps there too. The numeric code is
 * always preserved as the JDBC vendor code, so nothing is lost.
 */
public final class PdbSqlState {

    private PdbSqlState() {}

    /** SQL standard general error - used when the wire code carries no class. */
    public static final String GENERAL_ERROR = "HY000";

    /** {@code 08006} connection_failure. */
    public static final String CONNECTION_FAILURE = "08006";
    /** {@code 08P01} protocol_violation. */
    public static final String PROTOCOL_VIOLATION = "08P01";

    /**
     * The server's legacy catch-all SQL error code
     * ({@code ERR_GENERIC_SQL}). Also what a pre-SQLSTATE server sends
     * for every SQL error.
     */
    public static final int ERR_GENERIC_SQL = 1064;

    /** Lowest value of the reserved letter-bearing SQLSTATE band. */
    private static final int BAND_BASE = 60000;

    /**
     * Inverse of the server's {@code SQLSTATE_WIRE_BAND}
     * (the server's native-wire codec), one entry per letter-bearing
     * SQLSTATE the server can raise. The server's test
     * {@code native_sqlstate_band_matches_the_jdbc_driver} reads this file and
     * fails if the two tables differ.
     */
    private static final Map<Integer, String> BAND = new HashMap<>();
    static {
        BAND.put(60000, "XX000"); // internal_error
        BAND.put(60001, "42P01"); // undefined_table
        BAND.put(60002, "42P02"); // undefined_parameter
        BAND.put(60003, "25P02"); // in_failed_sql_transaction
        BAND.put(60004, "25P01"); // no_active_sql_transaction
        BAND.put(60005, "40P01"); // deadlock_detected  (RETRIABLE)
        BAND.put(60006, "55P03"); // lock_not_available (RETRIABLE)
        BAND.put(60007, "0A000"); // feature_not_supported
        BAND.put(60008, "22P02"); // invalid_text_representation
        BAND.put(60009, "3B001"); // invalid_savepoint_specification
        BAND.put(60010, "28P01"); // invalid_password
        BAND.put(60011, "22P04"); // bad_copy_file_format
        BAND.put(60012, "2201B"); // invalid_regular_expression
        BAND.put(60013, "2201E"); // invalid_argument_for_logarithm
        BAND.put(60014, "2201F"); // invalid_argument_for_power_function
        BAND.put(60015, "2201G"); // invalid_argument_for_width_bucket_function
        BAND.put(60016, "2202E"); // array_subscript_error
        BAND.put(60017, "2200H"); // sequence_generator_limit_exceeded
        BAND.put(60018, "22P05"); // untranslatable_character
        BAND.put(60019, "2BP01"); // dependent_objects_still_exist
        BAND.put(60020, "3D000"); // invalid_catalog_name
        BAND.put(60021, "3F000"); // invalid_schema_name
        BAND.put(60022, "42P05"); // duplicate_prepared_statement
        BAND.put(60023, "42P06"); // duplicate_schema
        BAND.put(60024, "42P07"); // duplicate_table
        BAND.put(60025, "42P10"); // invalid_column_reference
        BAND.put(60026, "42P16"); // invalid_table_definition
        BAND.put(60027, "42P17"); // invalid_object_definition
        BAND.put(60028, "42P18"); // indeterminate_datatype
        BAND.put(60029, "42P19"); // invalid_recursion
        BAND.put(60030, "42P20"); // windowing_error
        BAND.put(60031, "42P21"); // collation_mismatch
        BAND.put(60032, "428C9"); // generated_always
        BAND.put(60033, "55P02"); // cant_change_runtime_param
        BAND.put(60034, "57P01"); // admin_shutdown
        BAND.put(60035, "57P03"); // cannot_connect_now
        BAND.put(60036, "08P01"); // protocol_violation
        BAND.put(60037, "P0001"); // raise_exception (PL/pgSQL RAISE)
    }

    /**
     * Protocol-level codes that predate SQLSTATE propagation and are NOT
     * numeric SQLSTATEs. Sources: the server's v2 stream error codes
     * (0x00–0x08), its handshake codes (0x10–0x14), the v1
     * framing guardrails that write code 1 or 2 ("SQL too long", "invalid batch
     * header", "unknown message type"), and code 26 for a missing prepared
     * statement. The driver also raises code 0 itself for GOAWAY / a closed
     * connection / an unexpected frame.
     */
    private static final Map<Integer, String> PROTOCOL = new HashMap<>();
    static {
        PROTOCOL.put(0x00, CONNECTION_FAILURE);  // NO_ERROR / GOAWAY / closed
        PROTOCOL.put(0x01, PROTOCOL_VIOLATION);  // PROTOCOL_ERROR / framing
        PROTOCOL.put(0x02, PROTOCOL_VIOLATION);  // INTERNAL_ERROR / unknown msg
        PROTOCOL.put(0x03, PROTOCOL_VIOLATION);  // FLOW_CONTROL_ERROR
        PROTOCOL.put(0x04, PROTOCOL_VIOLATION);  // STREAM_LIMIT
        PROTOCOL.put(0x05, PROTOCOL_VIOLATION);  // FRAME_SIZE_ERROR
        PROTOCOL.put(0x06, PROTOCOL_VIOLATION);  // REFUSED_STREAM
        PROTOCOL.put(0x07, "57014");             // CANCEL → query_canceled
        PROTOCOL.put(0x08, CONNECTION_FAILURE);  // IDLE_TIMEOUT
        PROTOCOL.put(0x10, "08004");             // TLS_REQUIRED → rejected
        PROTOCOL.put(0x11, "53300");             // TOO_MANY_CONNECTIONS
        PROTOCOL.put(0x12, "08004");             // RATE_LIMITED → rejected
        PROTOCOL.put(0x13, "28000");             // AUTH_REQUIRED
        PROTOCOL.put(0x14, "28P01");             // AUTH_FAILED → invalid_password
        PROTOCOL.put(26, "26000");               // prepared statement not found
        PROTOCOL.put(ERR_GENERIC_SQL, GENERAL_ERROR);
    }

    /**
     * The SQLSTATE for a native-wire error code. Never returns {@code null}.
     *
     * @param wireCode the {@code u16} code from the server's error frame
     */
    public static String forWireCode(int wireCode) {
        int code = wireCode & 0xFFFF;
        if (code >= BAND_BASE) {
            String banded = BAND.get(code);
            // A band value this driver does not know: the letter-bearing
            // SQLSTATE it stands for cannot be reconstructed, so report the
            // general error rather than invent a class. The vendor code still
            // carries the exact value.
            return banded != null ? banded : GENERAL_ERROR;
        }
        String protocol = PROTOCOL.get(code);
        if (protocol != null) {
            return protocol;
        }
        if (code >= 10000) {
            return Integer.toString(code);
        }
        if (code >= 1000) {
            // A SQLSTATE with a leading zero (02000, 08006, …) parsed to a
            // value below 10000 on the way out; pad it back to five chars.
            return "0" + code;
        }
        return GENERAL_ERROR;
    }

    /**
     * Build the {@link SQLException} for a native-wire error: the real SQLSTATE,
     * the numeric wire code as the JDBC vendor code, and the JDBC 4 exception
     * subclass for the class when there is a standard one (so
     * {@code SQLIntegrityConstraintViolationException} /
     * {@code SQLTransactionRollbackException} - the two every framework and
     * retry loop keys on - are thrown as themselves).
     */
    public static SQLException toSqlException(String message, int wireCode, Throwable cause) {
        String state = forWireCode(wireCode);
        int vendor = wireCode & 0xFFFF;
        SQLException e = newTyped(message, state, vendor);
        if (cause != null) {
            e.initCause(cause);
        }
        return e;
    }

    /**
     * The engine's {@code Display} framing for the error variants whose payload
     * IS the user-facing message ({@code Error::SqlExec} / {@code SqlPlan} /
     * {@code SqlParse} in the server's error types). The pg-wire
     * encoder (in the server's PostgreSQL-wire layer)
     * sends those payloads WITHOUT this prefix, so a PostgreSQL client never
     * sees it; the native wire sends the {@code Display} text, and this driver
     * removes the framing so both wires present one message.
     */
    private static final String[] ENGINE_FRAMING = {
        "SQL execution error: ",
        "SQL plan error: ",
        "SQL parse error: ",
    };

    /**
     * Build the {@link SQLException} for an error the SERVER raised (a
     * {@code MSG_ERROR} frame), in the shape pgjdbc gives a PostgreSQL
     * {@code ErrorResponse}:
     *
     * <ul>
     *   <li><b>SQLSTATE</b>: the trailing {@code " [XXXXX]"} marker the engine
     *       appends to errors that carry an exact class (a PL/pgSQL
     *       {@code RAISE EXCEPTION} → {@code P0001}, a constraint violation →
     *       {@code 23505}, …) - the same marker, recovered by the same rule, as
     *       the pg-wire's {@code txn_fastpath_error_to_wire}. Without it the
     *       frame's {@code u16} code decides ({@link #forWireCode}); that code
     *       cannot carry a letter-bearing class such as {@code P0001}, which
     *       arrived as the generic {@code 1064} → {@code HY000}.</li>
     *   <li><b>Message</b>: {@code "ERROR: " + <primary message>}, with the
     *       marker and the engine's {@code Display} framing removed. pgjdbc's
     *       {@code PSQLException.getMessage()} is {@code "ERROR: <message>"}, and
     *       applications match on it: Conductor's {@code PostgresPollDataDAO}
     *       ignores its own {@code poll_data} trigger's
     *       {@code "lastPollTime cannot be set to a lower value"} only when the
     *       message {@code startsWith("ERROR: lastPollTime …")}. With the
     *       previous {@code "PDB wire error 1064: SQL execution error: …"} text
     *       every such (expected, PostgreSQL-identical) trigger raise was
     *       re-thrown and logged as a failed poll.</li>
     *   <li><b>Vendor code</b>: the wire code, unchanged.</li>
     * </ul>
     */
    public static SQLException serverErrorToSqlException(String serverMessage, int wireCode, Throwable cause) {
        String text = serverMessage == null ? "" : serverMessage.trim();
        String state = null;
        if (text.endsWith("]")) {
            int open = text.lastIndexOf('[');
            if (open >= 0 && text.length() - open - 2 == 5) {
                String code = text.substring(open + 1, text.length() - 1);
                if (isSqlStateMarker(code)) {
                    state = code;
                    text = text.substring(0, open).trim();
                }
            }
        }
        if (state == null) {
            state = forWireCode(wireCode);
        }
        for (String framing : ENGINE_FRAMING) {
            if (text.startsWith(framing)) {
                text = text.substring(framing.length());
                break;
            }
        }
        // The server spells a session-statement / SHOW refusal
        // "<SQLSTATE>: <message>" and also carries that SQLSTATE as the frame
        // code. pgjdbc's message never repeats the code, so drop the prefix
        // when it is the SQLSTATE being reported.
        if (text.length() > 7 && text.startsWith(state) && text.startsWith(": ", 5)) {
            text = text.substring(7);
        }
        SQLException e = newTyped("ERROR: " + text, state, wireCode & 0xFFFF);
        if (cause != null) {
            e.initCause(cause);
        }
        return e;
    }

    /** Five characters of the SQLSTATE alphabet: digits and uppercase letters. */
    private static boolean isSqlStateMarker(String code) {
        if (code.length() != 5) return false;
        for (int i = 0; i < 5; i++) {
            char ch = code.charAt(i);
            if (!((ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z'))) return false;
        }
        return true;
    }

    private static SQLException newTyped(String message, String state, int vendor) {
        // query_canceled: a statement_timeout, a setQueryTimeout the server
        // enforced after the driver reset the stream, or a Statement.cancel().
        // JDBC names the timeout case SQLTimeoutException; retry and circuit
        // breaker logic keys on the class.
        if ("57014".equals(state)) return new java.sql.SQLTimeoutException(message, state, vendor);
        if (state.length() >= 2) {
            switch (state.substring(0, 2)) {
                case "23": // integrity_constraint_violation
                    return new SQLIntegrityConstraintViolationException(message, state, vendor);
                case "40": // transaction_rollback — 40001 / 40P01, RETRIABLE
                    return new SQLTransactionRollbackException(message, state, vendor);
                case "42": // syntax_error_or_access_rule_violation
                    return new SQLSyntaxErrorException(message, state, vendor);
                case "22": // data_exception
                    return new SQLDataException(message, state, vendor);
                case "28": // invalid_authorization_specification
                    return new SQLInvalidAuthorizationSpecException(message, state, vendor);
                case "08": // connection_exception
                    return new SQLNonTransientConnectionException(message, state, vendor);
                default:
                    break;
            }
        }
        return new SQLException(message, state, vendor);
    }
}
