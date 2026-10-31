package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The native driver with the JVM in {@code America/Los_Angeles} answers the
 * Conductor claim predicate ({@code deliver_on <= current_timestamp}) and every
 * {@code timestamp} / {@code timestamptz} comparison the way pgjdbc does, on
 * the SAME server's PostgreSQL wire, and the way PostgreSQL 17.9 does.
 *
 * <p>pgjdbc sends the JVM zone as the session {@code TimeZone}; this driver
 * never did, so its session ran in UTC while every bound
 * {@code java.sql.Timestamp} / {@code LocalDateTime} was a Los Angeles wall
 * clock: a {@code timestamptz} written from a bound timestamp landed seven
 * hours early, {@code now()::timestamp} disagreed with the application's clock,
 * and {@code getTimestamp} on a {@code timestamptz} came back seven hours late.
 *
 * <p>Every row is due one hour ago, written six ways (SQL clock readers, a
 * literal wall clock, three bound Java types); every predicate must hold for
 * every row, and the lower bound ({@code > now - 3h}) catches a shift either
 * way. Skipped via {@link Assumptions} when the {@code particledb} binary is absent.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SessionTimeZoneLiveTest {

    private static final String ZONE = "America/Los_Angeles";
    private PdbTestServer server;
    private TimeZone saved;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null, "particledb binary not available");
        saved = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone(ZONE));
    }

    @AfterAll
    void tearDown() {
        if (saved != null) TimeZone.setDefault(saved);
        if (server != null) server.stop();
    }

    private Connection nativeConn() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:particledb://127.0.0.1:" + server.port + "/test?preferUnixSocket=false");
    }

    private Connection pgjdbcConn() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:postgresql://127.0.0.1:" + server.pgPort + "/postgres?user=postgres");
    }

    private static String scalar(Statement st, String sql) throws SQLException {
        try (ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getString(1);
        }
    }

    @Test
    void session_runs_in_the_jvm_zone() throws Exception {
        try (Connection c = nativeConn(); Statement st = c.createStatement()) {
            assertEquals(ZONE, scalar(st, "SHOW TimeZone"),
                    "the native session must run in the JVM zone, as pgjdbc's does");
        }
    }

    /** The claim matrix on one wire: one line per (column, predicate) with its per-row verdicts. */
    private static List<String> claimMatrix(Connection c, String table) throws SQLException {
        Instant hourAgo = Instant.now().minusSeconds(3600);
        try (Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + table);
            st.execute("CREATE TABLE " + table + " (id int PRIMARY KEY, src text, "
                    + "deliver_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP, d2 timestamptz)");
            st.execute("INSERT INTO " + table + " VALUES (1, 'current_timestamp', "
                    + "current_timestamp - interval '1 hour', current_timestamp - interval '1 hour')");
            st.execute("INSERT INTO " + table + " VALUES (2, 'localtimestamp', "
                    + "localtimestamp - interval '1 hour', localtimestamp - interval '1 hour')");
            String wall = LocalDateTime.ofInstant(hourAgo, ZoneId.of(ZONE)).toString().replace('T', ' ');
            st.execute("INSERT INTO " + table + " VALUES (3, 'literal', '" + wall + "', '" + wall + "')");
        }
        try (PreparedStatement p = c.prepareStatement("INSERT INTO " + table + " VALUES (?, ?, ?, ?)")) {
            p.setInt(1, 4); p.setString(2, "setTimestamp");
            p.setTimestamp(3, Timestamp.from(hourAgo)); p.setTimestamp(4, Timestamp.from(hourAgo));
            p.executeUpdate();
            LocalDateTime ldt = LocalDateTime.ofInstant(hourAgo, ZoneId.systemDefault());
            p.setInt(1, 5); p.setString(2, "LocalDateTime");
            p.setObject(3, ldt); p.setObject(4, ldt);
            p.executeUpdate();
            OffsetDateTime odt = OffsetDateTime.ofInstant(hourAgo, ZoneId.systemDefault());
            p.setInt(1, 6); p.setString(2, "OffsetDateTime");
            p.setObject(3, odt); p.setObject(4, odt);
            p.executeUpdate();
        }
        List<String> out = new ArrayList<>();
        try (Statement st = c.createStatement()) {
            for (String col : new String[]{"deliver_on", "d2"}) {
                for (String now : new String[]{"current_timestamp", "now()", "localtimestamp"}) {
                    String sql = "SELECT id, (" + col + " <= " + now + " + interval '1000 microseconds')::text, ("
                            + col + " > " + now + " - interval '3 hours')::text FROM " + table + " ORDER BY id";
                    StringBuilder b = new StringBuilder(col + " vs " + now + ":");
                    try (ResultSet rs = st.executeQuery(sql)) {
                        while (rs.next()) {
                            b.append(' ').append(rs.getInt(1)).append('=')
                                    .append(rs.getString(2).charAt(0)).append(rs.getString(3).charAt(0));
                        }
                    }
                    out.add(b.toString());
                }
            }
        }
        for (String col : new String[]{"deliver_on", "d2"}) {
            try (PreparedStatement p = c.prepareStatement(
                    "SELECT count(*) FROM " + table + " WHERE " + col + " <= ? AND " + col + " >= ?")) {
                p.setTimestamp(1, Timestamp.from(Instant.now()));
                p.setTimestamp(2, Timestamp.from(Instant.now().minusSeconds(7200)));
                try (ResultSet rs = p.executeQuery()) {
                    rs.next();
                    out.add(col + " in [bound now-2h, bound now]: " + rs.getLong(1));
                }
            }
        }
        // What the driver hands back for the stored values: minutes from the
        // instant they were meant to be.
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT id, deliver_on, d2 FROM " + table + " ORDER BY id")) {
            while (rs.next()) {
                out.add("row " + rs.getInt(1) + " getTimestamp drift(min): deliver_on="
                        + (rs.getTimestamp(2).toInstant().getEpochSecond() - hourAgo.getEpochSecond()) / 60
                        + " d2=" + (rs.getTimestamp(3).toInstant().getEpochSecond() - hourAgo.getEpochSecond()) / 60);
            }
        }
        return out;
    }

    @Test
    void claim_predicate_and_bound_timestamps_match_pgjdbc_in_a_non_utc_zone() throws Exception {
        List<String> nat;
        try (Connection c = nativeConn()) {
            nat = claimMatrix(c, "tz_claim_native");
        }
        // The invariant on its own, so a failure names the broken row even if
        // pgjdbc is unavailable or itself wrong.
        for (String line : nat) {
            if (line.contains(" vs ")) {
                assertTrue(!line.contains("=f") && !line.contains("=tf") && !line.contains("=ft"),
                        "every row is due one hour ago, so every predicate holds: " + line);
            } else if (line.contains("in [bound")) {
                assertTrue(line.endsWith(": 6"), "all six rows are inside the bound window: " + line);
            } else {
                assertTrue(line.endsWith("deliver_on=0 d2=0"), "a stored value moved: " + line);
            }
        }
        try (Connection c = pgjdbcConn()) {
            List<String> pg = claimMatrix(c, "tz_claim_pgjdbc");
            assertEquals(pg, nat, "native driver and pgjdbc disagree on the same server");
        }
    }

    /**
     * The JVM zone is the session's STARTUP value (sent in the HELLO, as pgjdbc
     * sends it in the startup packet), not a SET after connect: RESET, RESET
     * ALL, DISCARD ALL, SET TIME ZONE LOCAL / DEFAULT return to it. With the
     * post-connect SET they returned to the server default, UTC, where pgjdbc
     * (and PostgreSQL 17.9) return to the JVM zone.
     */
    @Test
    void reset_returns_to_the_jvm_zone_like_pgjdbc() throws Exception {
        String[] resets = {"RESET TimeZone", "RESET TIME ZONE", "SET TIME ZONE LOCAL",
                "SET TIME ZONE DEFAULT", "SET TimeZone TO DEFAULT", "RESET ALL", "DISCARD ALL"};
        try (Connection n = nativeConn(); Connection p = pgjdbcConn();
             Statement sn = n.createStatement(); Statement sp = p.createStatement()) {
            for (String reset : resets) {
                for (Statement st : new Statement[]{sn, sp}) {
                    st.execute("SET TIME ZONE 'Asia/Tokyo'");
                    st.execute(reset);
                }
                assertEquals(scalar(sp, "SHOW TimeZone"), scalar(sn, "SHOW TimeZone"), reset);
                assertEquals(ZONE, scalar(sn, "SHOW TimeZone"), reset);
            }
        }
    }

    /**
     * Transaction scope of SET TimeZone, native vs pgjdbc on the same server:
     * SET LOCAL reverts at COMMIT and ROLLBACK, a SET in a rolled-back block
     * reverts (also after an error), a committed SET stays (PostgreSQL 17.9).
     */
    @Test
    void set_time_zone_follows_transaction_scope_like_pgjdbc() throws Exception {
        String[][] cases = {
                {"SET LOCAL TimeZone = 'Asia/Kolkata'", "commit", ZONE},
                {"SET LOCAL TimeZone = 'Asia/Kolkata'", "rollback", ZONE},
                {"SET TimeZone = 'Asia/Kolkata'", "rollback", ZONE},
                {"SET TimeZone = 'Asia/Kolkata'", "error+rollback", ZONE},
                {"SET TimeZone = 'Asia/Kolkata'", "commit", "Asia/Kolkata"},
        };
        for (String[] c : cases) {
            String[] got = new String[2];
            int i = 0;
            for (Connection conn : new Connection[]{nativeConn(), pgjdbcConn()}) {
                try (Connection cn = conn; Statement st = cn.createStatement()) {
                    cn.setAutoCommit(false);
                    st.execute(c[0]);
                    assertEquals(c[0].contains("Kolkata") ? "Asia/Kolkata" : ZONE, scalar(st, "SHOW TimeZone"));
                    if (c[1].startsWith("error")) {
                        try {
                            st.execute("SELECT 1/0");
                        } catch (SQLException expected) {
                            // the block is aborted
                        }
                    }
                    if (c[1].equals("commit")) cn.commit(); else cn.rollback();
                    cn.setAutoCommit(true);
                    got[i++] = scalar(st, "SHOW TimeZone");
                }
            }
            assertEquals(c[2], got[0], "native: " + String.join(" / ", c));
            assertEquals(got[1], got[0], "native vs pgjdbc: " + String.join(" / ", c));
        }
    }

    /**
     * The handshake itself: the zone rides in the HELLO's session-parameter
     * block and the server echoes {@code CAP_EXT_SESSION_PARAMS}; a zone the
     * server cannot apply is refused at connect with SQLSTATE 22023, as
     * PostgreSQL refuses a bad startup {@code TimeZone} (FATAL 22023).
     */
    @Test
    void the_zone_rides_in_the_hello_and_a_bad_one_is_refused() throws Exception {
        org.particledb.wire.StreamMux ok = new org.particledb.wire.StreamMux(
                org.particledb.wire.SharedEventLoop.group(), false, 10_000, 16, true);
        try {
            org.particledb.wire.WireV2Codec.HelloOk h =
                    ok.connect("127.0.0.1", server.port, ZONE).get(10, java.util.concurrent.TimeUnit.SECONDS);
            assertTrue(h.appliedSessionParams(), "the server must echo CAP_EXT_SESSION_PARAMS");
        } finally {
            ok.close();
        }
        org.particledb.wire.StreamMux bad = new org.particledb.wire.StreamMux(
                org.particledb.wire.SharedEventLoop.group(), false, 10_000, 16, true);
        try {
            bad.connect("127.0.0.1", server.port, "Not/AZone").get(10, java.util.concurrent.TimeUnit.SECONDS);
            throw new AssertionError("a zone PostgreSQL refuses at connect was accepted");
        } catch (java.util.concurrent.ExecutionException e) {
            SQLException refused = PdbConnection.startupRefusal(e);
            assertTrue(refused != null, "not a startup refusal: " + e.getCause());
            assertEquals("22023", refused.getSQLState());
            assertTrue(refused.getMessage().startsWith("FATAL: invalid value for parameter \"TimeZone\""),
                    refused.getMessage());
        } finally {
            bad.close();
        }
    }

    @Test
    void timestamptz_text_and_instant_match_pgjdbc() throws Exception {
        String sql = "SELECT TIMESTAMPTZ '2024-07-01 12:00:00.5+00', TIMESTAMPTZ '2024-01-15 08:00:00+00' + interval '1 hour'";
        try (Connection n = nativeConn(); Connection p = pgjdbcConn();
             Statement sn = n.createStatement(); Statement sp = p.createStatement();
             ResultSet rn = sn.executeQuery(sql); ResultSet rp = sp.executeQuery(sql)) {
            assertTrue(rn.next());
            assertTrue(rp.next());
            for (int i = 1; i <= 2; i++) {
                assertEquals(rp.getString(i), rn.getString(i), "getString col " + i);
                assertEquals(rp.getTimestamp(i).toInstant(), rn.getTimestamp(i).toInstant(), "getTimestamp col " + i);
                assertEquals(rp.getObject(i, OffsetDateTime.class), rn.getObject(i, OffsetDateTime.class), "OffsetDateTime col " + i);
            }
            assertEquals("2024-07-01 05:00:00.5-07", rn.getString(1));
        }
    }
}
