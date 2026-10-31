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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Session time zone, differential against PostgreSQL 17 through pgjdbc: with
 * the JVM in {@code America/Los_Angeles} (and the startup zone it implies),
 * the SAME statements over (a) pgjdbc to PostgreSQL 17, (b) pgjdbc to this
 * server's PG wire - text and binary transfer - and (c) this driver over the
 * native wire must bind and read {@code timestamptz} / {@code timestamp} /
 * {@code date} identically: {@code setTimestamp}, {@code setObject} of
 * {@code OffsetDateTime} / {@code LocalDateTime} / {@code LocalDate}, literals,
 * a DST-gap and a DST-fold wall clock; read back with {@code getString},
 * {@code getTimestamp}, {@code getObject(OffsetDateTime / LocalDateTime)}, and
 * zone-dependent SQL (casts, {@code date_trunc}, {@code extract}, {@code
 * to_char}, {@code AT TIME ZONE}, {@code generate_series}).
 *
 * <p>The oracle is {@code PG_ORACLE_URL} (system property or env); its
 * identity is checked ({@code version()} names PostgreSQL 17, not ParticleDB).
 * Skipped when it or the {@code particledb} binary is absent.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SessionTimeZonePg17DifferentialTest {

    private static final String ZONE = "America/Los_Angeles";
    private String pgUrl;
    private PdbTestServer server;
    private TimeZone saved;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        Class.forName("org.postgresql.Driver");
        pgUrl = System.getProperty("PG_ORACLE_URL", System.getenv("PG_ORACLE_URL"));
        Assumptions.assumeTrue(pgUrl != null && !pgUrl.isEmpty(), "PG_ORACLE_URL not set");
        String banner = null;
        try (Connection pg = DriverManager.getConnection(pgUrl);
             Statement st = pg.createStatement();
             ResultSet rs = st.executeQuery("SELECT version()")) {
            if (rs.next()) banner = rs.getString(1);
        } catch (SQLException e) {
            banner = null;
        }
        Assumptions.assumeTrue(banner != null && banner.startsWith("PostgreSQL 17")
                && !banner.contains("ParticleDB"), "not a PostgreSQL 17 oracle: " + banner);
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

    private Connection pg() throws SQLException {
        return DriverManager.getConnection(pgUrl);
    }

    private Connection pdbPg(boolean binary) throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:" + server.pgPort
                + "/postgres?user=postgres" + (binary ? "&prepareThreshold=-1&binaryTransfer=true" : "&prepareThreshold=0"));
    }

    private Connection nat() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:particledb://127.0.0.1:" + server.port + "/test?preferUnixSocket=false");
    }

    private static final Instant I1 = Instant.parse("2024-07-01T12:00:00.5Z");
    // 2024-03-10 02:30 does not exist in Los Angeles; 2024-11-03 01:30 happens twice.
    private static final LocalDateTime GAP = LocalDateTime.parse("2024-03-10T02:30:00");
    private static final LocalDateTime FOLD = LocalDateTime.parse("2024-11-03T01:30:00");

    private static List<String> matrix(Connection c, String table) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = c.createStatement()) {
            try { st.execute("DROP TABLE " + table); } catch (SQLException ignored) {}
            st.execute("CREATE TABLE " + table + " (id int PRIMARY KEY, tz timestamptz, ts timestamp, d date)");
            st.execute("INSERT INTO " + table + " VALUES (1, '2024-07-01 12:00:00', '2024-07-01 12:00:00', '2024-07-01')");
            st.execute("INSERT INTO " + table + " VALUES (2, '2024-03-10 02:30:00', '2024-03-10 02:30:00', '2024-03-10')");
            st.execute("INSERT INTO " + table + " VALUES (3, '2024-11-03 01:30:00', '2024-11-03 01:30:00', '2024-11-03')");
        }
        try (PreparedStatement p = c.prepareStatement("INSERT INTO " + table + " VALUES (?, ?, ?, ?)")) {
            p.setInt(1, 4);
            p.setTimestamp(2, Timestamp.from(I1));
            p.setTimestamp(3, Timestamp.from(I1));
            p.setDate(4, java.sql.Date.valueOf("2024-07-01"));
            p.executeUpdate();
            p.setInt(1, 5);
            p.setObject(2, OffsetDateTime.ofInstant(I1, ZoneOffset.ofHours(5)));
            p.setObject(3, GAP);
            p.setObject(4, LocalDate.parse("2024-03-10"));
            p.executeUpdate();
            p.setInt(1, 6);
            p.setObject(2, FOLD.atZone(java.time.ZoneId.of(ZONE)).toOffsetDateTime());
            p.setObject(3, FOLD);
            p.setObject(4, LocalDate.parse("2024-11-03"));
            p.executeUpdate();
        }
        String[] reads = {
            "SELECT id, tz, ts, d FROM %s ORDER BY id",
            "SELECT id, tz::text, ts::text, d::text, tz::timestamp::text, tz::date::text, ts::timestamptz::text, d::timestamptz::text FROM %s ORDER BY id",
            "SELECT id, date_trunc('day', tz)::text, extract(epoch FROM tz)::text, extract(hour FROM tz)::text, extract(timezone FROM tz)::text, to_char(tz, 'YYYY-MM-DD HH24:MI:SS TZ OF') FROM %s ORDER BY id",
            "SELECT id, (tz AT TIME ZONE 'Asia/Kolkata')::text, (ts AT TIME ZONE 'Asia/Kolkata')::text, (tz + interval '1 day')::text, date_trunc('day', tz, 'Asia/Kolkata')::text FROM %s ORDER BY id",
            "SELECT id FROM %s WHERE tz = ts ORDER BY id",
            "SELECT id FROM %s WHERE tz::date = d ORDER BY id",
        };
        for (String q : reads) {
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(String.format(q, table))) {
                int n = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    StringBuilder b = new StringBuilder();
                    for (int i = 1; i <= n; i++) b.append(rs.getString(i)).append(" | ");
                    out.add(b.toString());
                }
            }
        }
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT id, tz, ts FROM " + table + " ORDER BY id")) {
            while (rs.next()) {
                out.add(rs.getInt(1) + " getTimestamp tz=" + rs.getTimestamp(2).toInstant()
                        + " ts=" + rs.getTimestamp(3).toInstant()
                        + " odt=" + rs.getObject(2, OffsetDateTime.class).toInstant()
                        + " ldt=" + rs.getObject(3, LocalDateTime.class));
            }
        }
        try (PreparedStatement p = c.prepareStatement("SELECT count(*) FROM " + table + " WHERE tz >= ? AND tz < ?")) {
            p.setTimestamp(1, Timestamp.valueOf("2024-07-01 00:00:00"));
            p.setObject(2, OffsetDateTime.parse("2024-07-02T00:00:00-07:00"));
            try (ResultSet rs = p.executeQuery()) {
                rs.next();
                out.add("bound window count=" + rs.getLong(1));
            }
        }
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT g::text FROM generate_series(TIMESTAMPTZ '2024-11-02 00:00', TIMESTAMPTZ '2024-11-04 00:00', interval '1 day') g")) {
            while (rs.next()) out.add("series " + rs.getString(1));
        }
        try (Statement st = c.createStatement()) {
            st.execute("DROP TABLE " + table);
        }
        return out;
    }

    @Test
    void timestamps_bind_and_read_like_postgres_17_on_every_wire() throws Exception {
        List<String> oracle;
        try (Connection c = pg()) {
            oracle = matrix(c, "zt_oracle");
        }
        List<String> failures = new ArrayList<>();
        String[] names = {"pgjdbc->PDB PG wire (text)", "pgjdbc->PDB PG wire (binary)", "native JDBC"};
        for (int k = 0; k < 3; k++) {
            List<String> got;
            try (Connection c = k == 0 ? pdbPg(false) : k == 1 ? pdbPg(true) : nat()) {
                got = matrix(c, "zt_" + k);
            }
            for (int i = 0; i < Math.max(got.size(), oracle.size()); i++) {
                String w = i < oracle.size() ? oracle.get(i) : "<none>";
                String g = i < got.size() ? got.get(i) : "<none>";
                if (!w.equals(g)) failures.add(names[k] + " line " + i + "\n   PG17: " + w + "\n   PDB:  " + g);
            }
        }
        assertTrue(failures.isEmpty(), failures.size() + " divergences:\n" + String.join("\n", failures));
        assertEquals(oracle.size() > 30, true);
    }
}
