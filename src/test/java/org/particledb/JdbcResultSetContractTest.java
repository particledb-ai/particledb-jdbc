package org.particledb;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link java.sql.ResultSet} CONTRACT conformance, driven off a REAL Arrow IPC
 * stream (see {@link ArrowIpc}) so the whole decode path runs - no live server.
 *
 * <p>Everything here was NO TEST before this file existed. The array getters
 * (covered by {@code ArrayElementTypeTest}) and the metadata type mapping
 * (covered by {@code MetadataAndArrayUnitTest}) are deliberately NOT repeated;
 * what is asserted here is the scalar getter contract, cursor positioning, and
 * the exception type/class every getter is required to raise - the surfaces a
 * regression could silently pass through today.
 *
 * <p>Every refusal assertion is paired with a POSITIVE CONTROL: the same call
 * made valid must succeed, so "it threw" can never be a harness that throws on
 * everything.
 */
@org.junit.jupiter.api.Tag("arrow-java")
public class JdbcResultSetContractTest {

    // ── boolean: PG's text forms 't' / 'f' ─────────────────────────────────
    /**
     * PDB serves a boolean-valued expression as Arrow {@code Utf8} in several
     * shapes, and PG's canonical external form for a boolean is the single
     * character {@code t} / {@code f}. {@code getBoolean} MUST read those, and
     * must agree with {@code getObject(i, Boolean.class)} - which has always
     * gone through the PG-aware {@code coerceBool}. A driver where
     * {@code getBoolean("flag")} says false and
     * {@code getObject(1, Boolean.class)} says true is silently wrong on
     * whichever half the application happens to call.
     */
    @Test
    void getBoolean_readsPostgresTextForms_andAgreesWithGetObject() throws Exception {
        String[] truthy = {"t", "true", "TRUE", "y", "yes", "on", "1"};
        String[] falsy  = {"f", "false", "FALSE", "n", "no", "off", "0"};
        for (String s : truthy) {
            try (PdbResultSet rs = ArrowIpc.build().utf8("flag", s).rs()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1), "getBoolean should read PG true form: " + s);
                assertTrue(rs.getBoolean("flag"), "by-name getBoolean disagrees for: " + s);
                assertEquals(Boolean.TRUE, rs.getObject(1, Boolean.class),
                        "getBoolean and getObject(Boolean.class) disagree for: " + s);
            }
        }
        for (String s : falsy) {
            try (PdbResultSet rs = ArrowIpc.build().utf8("flag", s).rs()) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1), "getBoolean should read PG false form: " + s);
                assertEquals(Boolean.FALSE, rs.getObject(1, Boolean.class),
                        "getBoolean and getObject(Boolean.class) disagree for: " + s);
            }
        }
    }

    /** POSITIVE CONTROL for the above: a real Arrow Bool column still works. */
    @Test
    void getBoolean_nativeArrowBoolColumn_stillWorks() throws Exception {
        try (PdbResultSet rs = ArrowIpc.build().bool("flag", true, false, null).rs()) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
            assertFalse(rs.wasNull());
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1), "SQL NULL boolean reads as false per JDBC");
            assertTrue(rs.wasNull(), "wasNull must flag the SQL NULL");
        }
    }

    // ── integral narrowing must not silently wrap ──────────────────────────
    /**
     * {@code getInt} on a BIGINT that does not fit an int is data corruption if
     * it silently wraps: {@code 2^31} came back as {@code -2147483648}. pgjdbc
     * throws ("Bad value for type int"), and this driver's own
     * {@code getObject(i, Integer.class)} already range-checks - so the two
     * halves of the same driver disagreed about the same cell.
     */
    @Test
    void getInt_outOfRangeThrows_ratherThanWrapping() throws Exception {
        long tooBig = Integer.MAX_VALUE + 1L;
        try (PdbResultSet rs = ArrowIpc.build().int64("n", tooBig).rs()) {
            assertTrue(rs.next());
            SQLException e = assertThrows(SQLException.class, () -> rs.getInt(1),
                    "getInt on " + tooBig + " must throw, not wrap to "
                            + (int) tooBig);
            // pgjdbc's error: SQLSTATE 22003 and its message.
            assertEquals("22003", e.getSQLState(), e.getMessage());
            assertEquals("Bad value for type int : " + tooBig, e.getMessage());
        }
        try (PdbResultSet rs = ArrowIpc.build().int64("n", (long) Short.MAX_VALUE + 1).rs()) {
            assertTrue(rs.next());
            assertThrows(SQLException.class, () -> rs.getShort(1));
        }
        try (PdbResultSet rs = ArrowIpc.build().int64("n", 200L).rs()) {
            assertTrue(rs.next());
            assertThrows(SQLException.class, () -> rs.getByte(1));
        }
    }

    /** POSITIVE CONTROL: in-range values still read, including the boundaries. */
    @Test
    void getInt_inRangeValuesStillRead() throws Exception {
        try (PdbResultSet rs = ArrowIpc.build()
                .int64("n", (long) Integer.MAX_VALUE, (long) Integer.MIN_VALUE, 0L, null)
                .rs()) {
            assertTrue(rs.next());
            assertEquals(Integer.MAX_VALUE, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(Integer.MIN_VALUE, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1), "SQL NULL reads as 0 per JDBC");
            assertTrue(rs.wasNull());
        }
        try (PdbResultSet rs = ArrowIpc.build()
                .int64("n", (long) Short.MAX_VALUE, (long) Byte.MIN_VALUE).rs()) {
            assertTrue(rs.next());
            assertEquals(Short.MAX_VALUE, rs.getShort(1));
            assertTrue(rs.next());
            assertEquals(Byte.MIN_VALUE, rs.getByte(1));
        }
    }

    /**
     * A numeric getter over a non-numeric text cell must raise
     * {@link SQLException} - NOT the unchecked {@link NumberFormatException}
     * that escapes a {@code Long.parseLong} / {@code new BigDecimal(...)}. An
     * unchecked throw here bypasses every {@code catch (SQLException)} an
     * application or framework has, so a bad cell kills the caller's thread
     * instead of surfacing as a query error.
     */
    @Test
    void numericGettersOverTextRaiseSqlException_notNumberFormatException() throws Exception {
        try (PdbResultSet rs = ArrowIpc.build().utf8("s", "not-a-number").rs()) {
            assertTrue(rs.next());
            assertThrows(SQLException.class, () -> rs.getLong(1));
            assertThrows(SQLException.class, () -> rs.getInt(1));
            assertThrows(SQLException.class, () -> rs.getDouble(1));
            assertThrows(SQLException.class, () -> rs.getBigDecimal(1));
        }
    }

    /** POSITIVE CONTROL: numeric text still parses through the same path. */
    @Test
    void numericGettersOverNumericTextStillParse() throws Exception {
        try (PdbResultSet rs = ArrowIpc.build().utf8("s", "42").rs()) {
            assertTrue(rs.next());
            assertEquals(42L, rs.getLong(1));
            assertEquals(42, rs.getInt(1));
            assertEquals(42.0, rs.getDouble(1), 0.0);
            assertEquals(new BigDecimal("42"), rs.getBigDecimal(1));
        }
    }

    // ── TIME: micros-of-day must not be read as epoch millis ───────────────
    /**
     * PDB ships a {@code TIME} column as Arrow {@code Time(MICROSECOND)}, whose
     * value is MICROSECONDS OF DAY - a wall-clock offset with no date and no
     * zone. Feeding that to {@code new java.sql.Time(millis)} interprets it as
     * epoch millis, which the JVM then renders in the DEFAULT time zone: the
     * same stored 12:00:00 reads back as 12:00:00 in UTC and 07:00:00 in
     * America/New_York. The value must be zone-INDEPENDENT.
     */
    @Test
    void getTime_microsOfDayIsWallClock_notEpochMillis() throws Exception {
        long noonMicros = 12L * 3600 * 1_000_000L;
        byte[] ipc = ArrowIpc.build().timeMicros("t", noonMicros).ipc();
        TimeZone saved = TimeZone.getDefault();
        try {
            for (String zone : new String[]{"UTC", "America/New_York", "Asia/Kolkata"}) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                try (PdbResultSet rs = new PdbResultSet(
                        org.particledb.wire.StreamState.StreamResult.arrow(ipc))) {
                    assertTrue(rs.next());
                    Time t = rs.getTime(1);
                    assertNotNull(t);
                    assertEquals("12:00:00", t.toString(),
                            "TIME 12:00:00 must read back as 12:00:00 under TZ=" + zone);
                }
            }
        } finally {
            TimeZone.setDefault(saved);
        }
    }

    /** POSITIVE CONTROL: a text TIME cell keeps working through the same getter. */
    @Test
    void getTime_textCarrierStillParses() throws Exception {
        try (PdbResultSet rs = ArrowIpc.build().utf8("t", "23:59:58").rs()) {
            assertTrue(rs.next());
            assertEquals("23:59:58", rs.getTime(1).toString());
        }
    }

    // ── TIMESTAMP: sub-second fidelity ────────────────────────────────────
    /**
     * Arrow {@code Timestamp(MICROSECOND)} → {@link Timestamp} must keep the
     * microsecond, not floor to the second, and must survive a negative
     * (pre-1970) epoch without the sign flip that integer division produces.
     */
    @Test
    void getTimestamp_keepsMicrosecondsIncludingBeforeEpoch() throws Exception {
        // A PG TIMESTAMP has no zone, so the property that must hold in EVERY
        // JVM zone is the WALL CLOCK, not the epoch. Asserting an epoch sign
        // here would pass in UTC and fail west of it - a zone-dependent test is
        // a false green waiting for a CI box in another region.
        byte[] ipc = ArrowIpc.build().timestampMicros("ts", 1_000_123L, -1_000_123L).ipc();
        TimeZone saved = TimeZone.getDefault();
        try {
            for (String zone : new String[]{"UTC", "America/New_York", "Asia/Kolkata"}) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                try (PdbResultSet rs = new PdbResultSet(
                        org.particledb.wire.StreamState.StreamResult.arrow(ipc))) {
                    assertTrue(rs.next());
                    assertEquals("1970-01-01 00:00:01.000123", rs.getTimestamp(1).toString(),
                            "microsecond component lost under TZ=" + zone);
                    assertTrue(rs.next());
                    assertEquals("1969-12-31 23:59:58.999877", rs.getTimestamp(1).toString(),
                            "pre-epoch sub-second component wrong under TZ=" + zone);
                }
            }
        } finally {
            TimeZone.setDefault(saved);
        }
    }

    /**
     * {@code getDate} / {@code getTime} on a TIMESTAMP column, and
     * {@code getTimestamp} on a DATE column, are ordinary JDBC - a framework
     * that maps a column to {@code java.util.Date} calls whichever its type
     * mapping picked. These used to reach {@code Date.valueOf(v.toString())}
     * with an ISO {@code T}-separated string and throw
     * {@link IllegalArgumentException}: UNCHECKED, so it escaped every
     * {@code catch (SQLException)} above it.
     */
    @Test
    void temporalGettersCrossColumnTypes_doNotThrowUnchecked() throws Exception {
        // TIMESTAMP column read through all three getters.
        try (PdbResultSet rs = ArrowIpc.build()
                .timestampMicros("ts", 1_700_000_000_123_456L).rs()) {
            assertTrue(rs.next());
            Timestamp ts = rs.getTimestamp(1);
            assertNotNull(ts);
            assertEquals(ts.toString().substring(0, 10), rs.getDate(1).toString(),
                    "getDate on a TIMESTAMP column must give its date part");
            assertEquals(ts.toString().substring(11, 19), rs.getTime(1).toString(),
                    "getTime on a TIMESTAMP column must give its time part");
        }
        // TEXT carrier in PG's space-separated spelling.
        try (PdbResultSet rs = ArrowIpc.build().utf8("ts", "2026-03-04 05:06:07.5").rs()) {
            assertTrue(rs.next());
            assertEquals("2026-03-04", rs.getDate(1).toString());
            assertEquals("05:06:07", rs.getTime(1).toString());
            assertEquals("2026-03-04 05:06:07.5", rs.getTimestamp(1).toString());
        }
        // A cell that is not temporal at all fails as SQLException, not as an
        // unchecked IllegalArgumentException.
        try (PdbResultSet rs = ArrowIpc.build().utf8("ts", "definitely not a date").rs()) {
            assertTrue(rs.next());
            assertThrows(SQLException.class, () -> rs.getDate(1));
            assertThrows(SQLException.class, () -> rs.getTime(1));
            assertThrows(SQLException.class, () -> rs.getTimestamp(1));
        }
    }

    // ── Calendar overloads must not be silently ignored ────────────────────
    /**
     * {@code getTimestamp(i, Calendar)} exists precisely so an application can
     * pin the zone used to interpret a zone-less {@code TIMESTAMP}. Ignoring
     * the {@link Calendar} makes the answer depend on the JVM's default zone  -
     * which is exactly the corruption the overload was added to prevent, and is
     * invisible in any test that runs in a single zone (Hibernate and Spring
     * both call the Calendar form).
     *
     * <p>Asserted as a RELATIVE property so it holds in every JVM zone: reading
     * the same cell through two Calendars whose offsets differ by N hours must
     * produce two Timestamps that differ by N hours.
     */
    @Test
    void getTimestamp_withCalendar_honoursTheCalendarsZone() throws Exception {
        byte[] ipc = ArrowIpc.build().timestampMicros("ts", 0L).ipc();
        Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        Calendar plus5 = Calendar.getInstance(TimeZone.getTimeZone("Etc/GMT-5")); // UTC+5
        try (PdbResultSet rs = new PdbResultSet(
                org.particledb.wire.StreamState.StreamResult.arrow(ipc))) {
            assertTrue(rs.next());
            Timestamp inUtc = rs.getTimestamp(1, utc);
            Timestamp inPlus5 = rs.getTimestamp(1, plus5);
            long deltaMillis = inUtc.getTime() - inPlus5.getTime();
            assertEquals(5 * 3600_000L, deltaMillis,
                    "getTimestamp(i, Calendar) ignored the Calendar: UTC and UTC+5 "
                            + "produced " + inUtc + " and " + inPlus5);
        }
    }

    /**
     * Same contract for {@code getDate}/{@code getTime}. The stored value is a
     * zone-less wall clock, so the CALENDAR DATE does not change - what the
     * Calendar decides is the INSTANT the returned {@code java.sql.Date} /
     * {@code java.sql.Time} carries (midnight, resp. 1970-01-01, in that zone).
     * Two Calendars five hours apart must therefore produce two values exactly
     * five hours apart.
     */
    @Test
    void getDateAndTime_withCalendar_honourTheCalendarsZone() throws Exception {
        byte[] ipc = ArrowIpc.build().timestampMicros("ts", 2L * 3600 * 1_000_000L).ipc();
        Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        Calendar minus5 = Calendar.getInstance(TimeZone.getTimeZone("Etc/GMT+5")); // UTC-5
        try (PdbResultSet rs = new PdbResultSet(
                org.particledb.wire.StreamState.StreamResult.arrow(ipc))) {
            assertTrue(rs.next());
            assertEquals(5 * 3600_000L,
                    rs.getDate(1, minus5).getTime() - rs.getDate(1, utc).getTime(),
                    "getDate(i, Calendar) ignored the Calendar");
            assertEquals(5 * 3600_000L,
                    rs.getTime(1, minus5).getTime() - rs.getTime(1, utc).getTime(),
                    "getTime(i, Calendar) ignored the Calendar");
        }
    }

    /**
     * POSITIVE CONTROL for both Calendar tests: the no-Calendar getters still
     * answer, and a null Calendar means "JVM default zone" per the JDBC
     * javadoc - i.e. identical to the no-arg form.
     */
    @Test
    void temporalGetters_nullCalendarEqualsNoArgForm() throws Exception {
        byte[] ipc = ArrowIpc.build().timestampMicros("ts", 1_700_000_000_000_000L).ipc();
        try (PdbResultSet rs = new PdbResultSet(
                org.particledb.wire.StreamState.StreamResult.arrow(ipc))) {
            assertTrue(rs.next());
            assertEquals(rs.getTimestamp(1), rs.getTimestamp(1, null));
            assertEquals(rs.getDate(1), rs.getDate(1, null));
            assertEquals(rs.getTime(1), rs.getTime(1, null));
        }
    }

    // ── cursor positioning must never report a position it cannot serve ────
    /**
     * The scroll methods answered a boolean that did not describe the cursor
     * they had just set. {@code absolute(-1)} put the cursor at index -2 and
     * returned TRUE, so the caller's very next getter threw "cursor out of
     * range". Whatever a forward-only ResultSet chooses to support, the return
     * value and the cursor must agree: TRUE iff a subsequent getter can serve
     * the row.
     */
    @Test
    void scrollMethods_returnValueMatchesTheCursorTheySet() throws Exception {
        try (PdbResultSet rs = ArrowIpc.build().int64("n", 10L, 20L, 30L).rs()) {
            // absolute(0) is "before the first row" per the JDBC javadoc.
            assertFalse(rs.absolute(0), "absolute(0) must report no row");
            assertTrue(rs.isBeforeFirst());

            // Negative index counts back from the end: -1 is the LAST row.
            assertTrue(rs.absolute(-1), "absolute(-1) must land on the last row");
            assertEquals(30L, rs.getLong(1));

            assertFalse(rs.absolute(99), "absolute past the end must report no row");
            assertTrue(rs.isAfterLast());
            assertThrows(SQLException.class, () -> rs.getLong(1),
                    "a getter after the last row must throw, not return a value");

            assertTrue(rs.absolute(2));
            assertEquals(20L, rs.getLong(1));
            assertFalse(rs.relative(-99), "relative past the start must report no row");
            assertTrue(rs.isBeforeFirst());
            assertFalse(rs.previous(), "previous() before the first row must report no row");
            assertTrue(rs.isBeforeFirst(),
                    "a failed previous() must leave the cursor before-first, not below it");
        }
    }

    /** POSITIVE CONTROL: plain forward iteration is untouched. */
    @Test
    void forwardIteration_isUnchanged() throws Exception {
        try (PdbResultSet rs = ArrowIpc.build().int64("n", 10L, 20L, 30L).rs()) {
            long sum = 0;
            int rows = 0;
            while (rs.next()) { sum += rs.getLong(1); rows++; }
            assertEquals(3, rows);
            assertEquals(60L, sum);
            assertFalse(rs.next(), "next() past the end must stay false");
            assertTrue(rs.isAfterLast());
        }
    }

    // ── metadata must raise SQLException, never IndexOutOfBounds ───────────
    /**
     * {@code ResultSetMetaData.getColumnLabel(99)} went straight into
     * {@code List.get} and threw {@link IndexOutOfBoundsException} - unchecked,
     * so it escapes every framework's {@code catch (SQLException)}. All of the
     * per-column accessors must fail as SQLException.
     */
    @Test
    void resultSetMetaData_outOfRangeColumnRaisesSqlException() throws Exception {
        try (PdbResultSet rs = ArrowIpc.build().int64("n", 1L).rs()) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(1, md.getColumnCount());
            for (int bad : new int[]{0, 2, -1, 99}) {
                assertThrows(SQLException.class, () -> md.getColumnLabel(bad),
                        "getColumnLabel(" + bad + ")");
                assertThrows(SQLException.class, () -> md.getColumnName(bad),
                        "getColumnName(" + bad + ")");
                assertThrows(SQLException.class, () -> md.getColumnType(bad),
                        "getColumnType(" + bad + ")");
                assertThrows(SQLException.class, () -> md.getColumnTypeName(bad),
                        "getColumnTypeName(" + bad + ")");
                assertThrows(SQLException.class, () -> md.getColumnClassName(bad),
                        "getColumnClassName(" + bad + ")");
            }
        }
    }

    /** POSITIVE CONTROL: the in-range column still answers its real type. */
    @Test
    void resultSetMetaData_inRangeColumnAnswers() throws Exception {
        try (PdbResultSet rs = ArrowIpc.build().int64("n", 1L).utf8("s", "x").rs()) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(2, md.getColumnCount());
            assertEquals("n", md.getColumnLabel(1));
            assertEquals(Types.BIGINT, md.getColumnType(1));
            assertEquals("s", md.getColumnLabel(2));
            assertEquals(Types.VARCHAR, md.getColumnType(2));
            assertEquals(String.class.getName(), md.getColumnClassName(2));
        }
    }

    // ── column-index bounds on the getters ────────────────────────────────
    @Test
    void getterWithBadColumnIndexRaisesSqlException() throws Exception {
        try (PdbResultSet rs = ArrowIpc.build().int64("n", 1L).rs()) {
            assertTrue(rs.next());
            assertThrows(SQLException.class, () -> rs.getLong(0));
            assertThrows(SQLException.class, () -> rs.getLong(2));
            assertThrows(SQLException.class, () -> rs.getString(-1));
            // POSITIVE CONTROL
            assertEquals(1L, rs.getLong(1));
        }
    }

    @Test
    void absentColumnLabelsThrowFromEveryGetter() throws Exception {
        try (PdbResultSet rs = ArrowIpc.build().int64("n", 1L).rs()) {
            assertTrue(rs.next());
            assertThrows(SQLException.class, () -> rs.findColumn("nope"));
            assertThrows(SQLException.class, () -> rs.getLong("nope"));
            assertThrows(SQLException.class, () -> rs.getString("nope"));
            assertThrows(SQLException.class, () -> rs.getObject("nope"));
            assertEquals(1L, rs.getLong("N"));
        }
    }

    // ── closed ResultSet ─────────────────────────────────────────────────
    /**
     * Every getter and {@code next()} on a CLOSED ResultSet must throw
     * {@link SQLException}. A closed cursor that keeps serving rows lets a
     * use-after-close bug run to completion and produce plausible-looking
     * output.
     */
    @Test
    void closedResultSet_refusesEveryRead() throws Exception {
        PdbResultSet rs = ArrowIpc.build().int64("n", 1L, 2L).rs();
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong(1));   // POSITIVE CONTROL, before close
        rs.close();
        assertTrue(rs.isClosed());
        assertThrows(SQLException.class, rs::next);
        assertThrows(SQLException.class, () -> rs.getLong(1));
        assertThrows(SQLException.class, () -> rs.getString(1));
        assertThrows(SQLException.class, () -> rs.getObject(1));
        assertThrows(SQLException.class, rs::getMetaData);
        // close() stays idempotent per the JDBC contract.
        assertDoesNotThrow(rs::close);
    }
}
