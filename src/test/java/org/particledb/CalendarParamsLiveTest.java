package org.particledb;

import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Calendar;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class CalendarParamsLiveTest {
    @Test
    void calendarBindingsPreserveWallClocksAndInstantsOnBothParameterPaths() throws Exception {
        PdbTestServer server = PdbTestServer.tryStart();
        assumeTrue(server != null, "PDB_BIN required");
        try {
            for (boolean nativePrepare : new boolean[]{true, false}) {
                try (var c = DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + server.port
                        + "/test?preferUnixSocket=false&serverPrepare=" + nativePrepare);
                     var s = c.createStatement()) {
                    s.execute("SET TIME ZONE 'UTC'");
                    s.execute("DROP TABLE IF EXISTS calendar_bindings");
                    s.execute("CREATE TABLE calendar_bindings (d DATE, t TIME, ts TIMESTAMP, tz TIMESTAMPTZ)");
                    Instant instant = Instant.parse("2024-01-02T03:04:05.123456Z");
                    Calendar ny = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
                    Calendar original = (Calendar) ny.clone();
                    try (var p = c.prepareStatement("INSERT INTO calendar_bindings VALUES (?, ?, ?, ?)")) {
                        p.setDate(1, new java.sql.Date(instant.toEpochMilli()), ny);
                        p.setTime(2, new java.sql.Time(instant.toEpochMilli()), ny);
                        p.setTimestamp(3, Timestamp.from(instant), ny);
                        p.setTimestamp(4, Timestamp.from(instant), ny);
                        assertEquals(1, p.executeUpdate());
                    }
                    assertEquals(original, ny, "binding must not mutate the caller's Calendar");
                    try (var r = s.executeQuery("SELECT d, t, ts, tz FROM calendar_bindings")) {
                        assertTrue(r.next());
                        assertEquals("2024-01-01", r.getString(1));
                        assertTrue(r.getString(2).startsWith("22:04:05"), r.getString(2));
                        assertTrue(r.getString(3).startsWith("2024-01-01 22:04:05.123456"), r.getString(3));
                        Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                        assertEquals(instant, r.getTimestamp(4, utc).toInstant());
                    }
                }
            }
        } finally {
            server.stop();
        }
    }
}
