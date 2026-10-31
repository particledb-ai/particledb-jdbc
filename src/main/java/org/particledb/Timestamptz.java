package org.particledb;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * The driver's ONE rule for {@code timestamp with time zone}, both directions.
 *
 * <p><b>Session zone.</b> PostgreSQL reads and writes a {@code timestamptz} in
 * the session {@code TimeZone}, and every comparison of a {@code timestamp}
 * with a {@code timestamptz} ({@code deliver_on <= current_timestamp}) runs
 * through it. pgjdbc sends the JVM's zone in its startup packet, so
 * {@code localtimestamp}, {@code now()::timestamp} and a bound
 * {@code java.sql.Timestamp} all agree with the application's wall clock.
 * {@link #sessionSetting(ZoneId)} is the value this driver sends in the native
 * HELLO's session-parameter block for the same effect (and {@code SET}s after
 * connect on a server that did not take it there).
 *
 * <p><b>Results.</b> The engine ships a {@code timestamptz} column as its UTC
 * instant (Arrow {@code Timestamp(µs)}) with the field stamped
 * {@code pdb:decl = timestamptz} and {@code pdb:tz = <session TimeZone>}
 * (the server's native-wire codec {@code stamp_timestamptz_zone}). Read as a
 * zone-less wall clock - what the getters did before - the instant moved by
 * the JVM's UTC offset.
 */
final class Timestamptz {
    private Timestamptz() {}

    /** Field-metadata key carrying the session TimeZone a timestamptz renders in. */
    static final String TZ_KEY = "pdb:tz";

    /**
     * Whether a {@code pdb:decl} stamp names {@code timestamptz} - the same
     * spellings as the engine's timestamptz type check.
     */
    static boolean isDecl(String decl) {
        if (decl == null) return false;
        String d = decl.trim().toLowerCase(java.util.Locale.ROOT);
        int paren = d.indexOf('(');
        String base = (paren >= 0 ? d.substring(0, paren) : d).trim();
        return base.equals("timestamptz") || base.equals("timestamp with time zone")
                || base.equals("timestampz")
                || (base.startsWith("timestamp") && d.endsWith("with time zone"));
    }

    /**
     * The session {@code TimeZone} setting for a JVM zone: a region id as is
     * ({@code America/Los_Angeles}); a fixed offset in the POSIX-signed
     * {@code ±HH:MM[:SS]} spelling PostgreSQL's {@code SET TIME ZONE} reads,
     * where the sign is INVERTED ({@code -05:30} is UTC+05:30); UTC as
     * {@code UTC}.
     */
    static String sessionSetting(ZoneId zone) {
        ZoneId n = zone.normalized();
        if (n instanceof ZoneOffset) {
            int east = ((ZoneOffset) n).getTotalSeconds();
            if (east == 0) return "UTC";
            int a = Math.abs(east);
            String s = String.format("%s%02d:%02d", east > 0 ? "-" : "+", a / 3600, a / 60 % 60);
            return a % 60 == 0 ? s : s + String.format(":%02d", a % 60);
        }
        return zone.getId();
    }

    /**
     * The instant a timestamptz cell holds, from the carriers the decoders
     * produce: a {@link LocalDateTime} that is the UTC wall clock of the
     * microseconds (Arrow's zone-less vector), a {@link Number} of microseconds
     * since the epoch, an {@link OffsetDateTime} / {@link Instant} /
     * {@link java.sql.Timestamp}, or PostgreSQL text with an offset.
     */
    static Instant instant(Object v) {
        if (v == null) return null;
        if (v instanceof Instant) return (Instant) v;
        if (v instanceof LocalDateTime) return ((LocalDateTime) v).toInstant(ZoneOffset.UTC);
        if (v instanceof OffsetDateTime) return ((OffsetDateTime) v).toInstant();
        if (v instanceof ZonedDateTime) return ((ZonedDateTime) v).toInstant();
        if (v instanceof java.sql.Timestamp) return ((java.sql.Timestamp) v).toInstant();
        if (v instanceof Number) {
            long us = ((Number) v).longValue();
            return Instant.ofEpochSecond(Math.floorDiv(us, 1_000_000L),
                    Math.floorMod(us, 1_000_000L) * 1000L);
        }
        return parseText(v.toString().trim());
    }

    /** PostgreSQL / ISO timestamptz text ({@code 2024-07-01 05:00:00-07}). */
    private static Instant parseText(String s) {
        String iso = s.length() > 10 && s.charAt(10) == ' '
                ? s.substring(0, 10) + 'T' + s.substring(11) : s;
        // PostgreSQL prints `-07` / `+05:30`; ISO wants `-07:00`.
        if (iso.matches(".*[+-]\\d{2}$")) iso = iso + ":00";
        try {
            return OffsetDateTime.parse(iso).toInstant();
        } catch (java.time.format.DateTimeParseException e) {
            // No offset: the engine's text for a timestamptz is always zoned, so
            // a bare wall clock here is UTC (the server default).
            return LocalDateTime.parse(iso).toInstant(ZoneOffset.UTC);
        }
    }

    /**
     * The zone a {@code pdb:tz} stamp names. PostgreSQL spellings Java cannot
     * name directly are translated: {@code <+05:30>-05:30} (what a numeric
     * {@code SET TIME ZONE} shows) by its POSIX offset, a POSIX-signed
     * {@code ±HH[:MM]}. Anything else unresolvable is UTC - the rendering
     * then still names the right instant, with a {@code +00} offset.
     */
    static ZoneId zoneOfSetting(String setting) {
        if (setting == null || setting.isEmpty()) return ZoneOffset.UTC;
        String s = setting.trim();
        try {
            if (s.startsWith("<")) {
                int close = s.indexOf('>');
                if (close > 0) return posixOffset(s.substring(close + 1));
            }
            if (s.startsWith("+") || s.startsWith("-")) return posixOffset(s);
            if (s.equalsIgnoreCase("UTC") || s.equalsIgnoreCase("Etc/UTC")) return ZoneOffset.UTC;
            if (ZoneId.getAvailableZoneIds().contains(s)) return ZoneId.of(s);
        } catch (RuntimeException ignored) {
            // fall through to UTC
        }
        return ZoneOffset.UTC;
    }

    /** A POSIX-signed offset ({@code -05:30} = UTC+05:30). */
    private static ZoneId posixOffset(String p) {
        String body = p.startsWith("+") || p.startsWith("-") ? p.substring(1) : p;
        int west = p.startsWith("-") ? -1 : 1;
        String[] f = body.split(":");
        int secs = Integer.parseInt(f[0]) * 3600
                + (f.length > 1 ? Integer.parseInt(f[1]) * 60 : 0)
                + (f.length > 2 ? Integer.parseInt(f[2]) : 0);
        return ZoneOffset.ofTotalSeconds(-west * secs);
    }

    /**
     * PostgreSQL's text for an instant in a zone, as {@code timestamptz_out}
     * writes it under {@code DateStyle = ISO}: {@code 2024-07-01 05:00:00-07},
     * fractional seconds without trailing zeros, the offset as {@code ±HH} plus
     * {@code :MM} / {@code :SS} only when non-zero, {@code BC} before year 1.
     */
    static String pgText(Instant i, ZoneId zone) {
        ZonedDateTime z = i.atZone(zone);
        StringBuilder b = wallClock(z.toLocalDateTime());
        boolean bc = z.getYear() <= 0;
        if (bc) b.setLength(b.length() - 3);
        int east = z.getOffset().getTotalSeconds();
        int a = Math.abs(east);
        b.append(east < 0 ? '-' : '+').append(String.format("%02d", a / 3600));
        if (a % 3600 != 0) {
            b.append(String.format(":%02d", a / 60 % 60));
            if (a % 60 != 0) b.append(String.format(":%02d", a % 60));
        }
        if (bc) b.append(" BC");
        return b.toString();
    }

    /**
     * PostgreSQL's text for a {@code timestamp} (no zone) under
     * {@code DateStyle = ISO}: {@code 2024-07-01 12:00:00}, fractional seconds
     * without trailing zeros, {@code BC} before year 1.
     */
    static String pgTextLocal(LocalDateTime t) {
        return wallClock(t).toString();
    }

    /** The wall-clock part both texts share (with a trailing " BC" when due). */
    private static StringBuilder wallClock(LocalDateTime z) {
        int year = z.getYear();
        boolean bc = year <= 0;
        StringBuilder b = new StringBuilder(32);
        b.append(String.format("%04d-%02d-%02d %02d:%02d:%02d", bc ? 1 - year : year,
                z.getMonthValue(), z.getDayOfMonth(), z.getHour(), z.getMinute(), z.getSecond()));
        int micros = z.getNano() / 1000;
        if (micros != 0) {
            String f = String.format("%06d", micros);
            int end = f.length();
            while (f.charAt(end - 1) == '0') end--;
            b.append('.').append(f, 0, end);
        }
        if (bc) b.append(" BC");
        return b;
    }
}
