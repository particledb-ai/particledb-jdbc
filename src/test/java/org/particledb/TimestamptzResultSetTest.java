package org.particledb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A {@code timestamptz} result column is an INSTANT, whatever the JVM zone.
 *
 * <p>The engine ships it as the same zone-less Arrow {@code Timestamp(µs)} a
 * {@code timestamp} uses, holding the UTC microseconds, and says which it is
 * only in the field's {@code pdb:decl} stamp ({@link Timestamptz}). The getters
 * used to read every timestamp as a wall clock, so under
 * {@code America/Los_Angeles} {@code getTimestamp} on a timestamptz returned an
 * instant seven hours LATE, and {@code getString} printed the UTC wall clock
 * with no offset. The expected values are what pgjdbc answers for the same
 * column against PostgreSQL 17.9 with the JVM in that zone.
 *
 * <p>The JVM zone is pinned per test, so a UTC CI box cannot turn this green
 * by accident: in UTC the wall clock and the instant coincide.
 */
@org.junit.jupiter.api.Tag("arrow-java")
public class TimestamptzResultSetTest {

    /** 2024-07-01 12:00:00.5 UTC, i.e. 05:00:00.5 PDT. */
    private static final long JULY_NOON_UTC_MICROS =
            Instant.parse("2024-07-01T12:00:00.500Z").toEpochMilli() * 1000L;

    private TimeZone saved;

    @BeforeEach
    void pinZone() {
        saved = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
    }

    @AfterEach
    void restoreZone() {
        TimeZone.setDefault(saved);
    }

    private static PdbResultSet tz(String sessionZone, Long micros) throws Exception {
        PdbResultSet rs = ArrowIpc.build().timestamptzMicros("t", sessionZone, micros).rs();
        assertTrue(rs.next());
        return rs;
    }

    @Test
    void getTimestamp_onTimestamptz_isTheInstant_notTheUtcWallClock() throws Exception {
        try (PdbResultSet rs = tz("America/Los_Angeles", JULY_NOON_UTC_MICROS)) {
            Instant want = Instant.parse("2024-07-01T12:00:00.500Z");
            assertEquals(want, rs.getTimestamp(1).toInstant(),
                    "getTimestamp read the UTC micros as a Los Angeles wall clock");
            assertEquals(want, rs.getTimestamp("t").toInstant());
            Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            assertEquals(want, rs.getTimestamp(1, utc).toInstant(),
                    "a Calendar must not move an instant");
            assertEquals(want, ((Timestamp) rs.getObject(1)).toInstant());
            assertEquals(want.atOffset(ZoneOffset.UTC), rs.getObject(1, OffsetDateTime.class));
            assertEquals(want, rs.getObject(1, Instant.class));
            assertEquals(LocalDateTime.parse("2024-07-01T05:00:00.500"),
                    rs.getObject(1, LocalDateTime.class),
                    "a zone-less target is the JVM zone's wall clock");
            assertEquals("2024-07-01", rs.getDate(1).toString());
            assertEquals("05:00:00", rs.getTime(1).toString());
        }
    }

    @Test
    void getString_onTimestamptz_isPostgresTextInTheSessionZone() throws Exception {
        try (PdbResultSet rs = tz("America/Los_Angeles", JULY_NOON_UTC_MICROS)) {
            assertEquals("2024-07-01 05:00:00.5-07", rs.getString(1));
            assertEquals("2024-07-01 05:00:00.5-07", rs.getString("t"));
        }
        // The SESSION zone renders, not the JVM's.
        try (PdbResultSet rs = tz("Asia/Kolkata", JULY_NOON_UTC_MICROS)) {
            assertEquals("2024-07-01 17:30:00.5+05:30", rs.getString(1));
        }
        // `SET TIME ZONE 5.5` shows `<+05:30>-05:30`; a POSIX-signed `-05:30`
        // is the same zone.
        try (PdbResultSet rs = tz("<+05:30>-05:30", JULY_NOON_UTC_MICROS)) {
            assertEquals("2024-07-01 17:30:00.5+05:30", rs.getString(1));
        }
        try (PdbResultSet rs = tz("-05:30", JULY_NOON_UTC_MICROS)) {
            assertEquals("2024-07-01 17:30:00.5+05:30", rs.getString(1));
        }
        try (PdbResultSet rs = tz("UTC", JULY_NOON_UTC_MICROS - 500_000L)) {
            assertEquals("2024-07-01 12:00:00+00", rs.getString(1));
        }
    }

    /** POSITIVE CONTROL: a zone-less timestamp column is still its own wall clock. */
    @Test
    void timestampWithoutTimeZone_isUnchanged() throws Exception {
        try (PdbResultSet rs = ArrowIpc.build().timestampMicros("ts", JULY_NOON_UTC_MICROS).rs()) {
            assertTrue(rs.next());
            assertEquals("2024-07-01 12:00:00.5", rs.getTimestamp(1).toString());
            assertEquals(LocalDateTime.parse("2024-07-01T12:00:00.500"), rs.getObject(1));
        }
    }

    @Test
    void nullTimestamptz_isNull() throws Exception {
        try (PdbResultSet rs = tz("America/Los_Angeles", null)) {
            assertEquals(null, rs.getTimestamp(1));
            assertTrue(rs.wasNull());
            assertEquals(null, rs.getString(1));
            assertEquals(null, rs.getObject(1));
            assertEquals(null, rs.getObject(1, OffsetDateTime.class));
        }
    }

    /** The setting the driver SETs at connect, per JVM zone (POSIX-signed offsets). */
    @Test
    void sessionSetting_spellsTheJvmZoneForSetTimeZone() {
        assertEquals("America/Los_Angeles", Timestamptz.sessionSetting(ZoneId.of("America/Los_Angeles")));
        assertEquals("UTC", Timestamptz.sessionSetting(ZoneId.of("UTC")));
        assertEquals("UTC", Timestamptz.sessionSetting(ZoneOffset.UTC));
        assertEquals("-05:30", Timestamptz.sessionSetting(ZoneId.of("GMT+05:30")));
        assertEquals("+08:00", Timestamptz.sessionSetting(ZoneOffset.ofHours(-8)));
        assertEquals("-05:45:30", Timestamptz.sessionSetting(ZoneOffset.ofHoursMinutesSeconds(5, 45, 30)));
    }
}
