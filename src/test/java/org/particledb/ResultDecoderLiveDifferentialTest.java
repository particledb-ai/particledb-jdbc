package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The same live queries read through {@code resultDecoder=arrow} (Apache Arrow
 * Java, the decoder of driver 1.0.9 and earlier) and {@code resultDecoder=auto}
 * (the built-in {@link ArrowIpcReader}) must give identical JDBC answers:
 * every getter's value or exception, and every ResultSetMetaData answer, for
 * every column the server sends -- numeric aggregates, every temporal type,
 * arrays, NULLs, buffered, chunked and streamed ({@code setFetchSize}) results,
 * Statement and PreparedStatement.
 *
 * <p>{@code -Dpdb.captureGolden=<dir>} also writes the raw IPC payloads of
 * {@link ArrowIpcReaderGoldenTest#LIVE} queries as golden files.
 */
@Tag("arrow-java")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ResultDecoderLiveDifferentialTest {

    private PdbTestServer server;
    private Connection arrow;
    private Connection builtin;

    static final String TABLE_DDL = "CREATE TABLE dec_types (id BIGINT PRIMARY KEY, i INT, s SMALLINT, r REAL, "
            + "d DOUBLE PRECISION, n NUMERIC, n2 NUMERIC(10,2), n50 NUMERIC(50,5), tx TEXT, vc VARCHAR(20), "
            + "ch CHAR(5), b BYTEA, bo BOOLEAN, dt DATE, tm TIME, ts TIMESTAMP, tz TIMESTAMPTZ, iv INTERVAL, "
            + "u UUID, ia INT[], ta TEXT[])";

    static final String[] TABLE_ROWS = {
        "INSERT INTO dec_types VALUES (1, 10, 3, 1.5, 2.25, 0.52, 12.30, 123456789012345678901234567890123456789012.12345, "
            + "'héllo', 'vc', 'ab', '\\x00ff', true, '2024-02-29', '12:34:56.789', "
            + "'2024-01-01 01:02:03.456789', '2024-01-01 01:02:03+05', '1 day 02:00:00', "
            + "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', ARRAY[1,2,3], ARRAY['x','y'])",
        "INSERT INTO dec_types VALUES (2, -20, -4, -0.5, 1e300, -123.456, -0.01, -1.00001, '', '', 'z', '\\x', false, "
            + "'1969-12-31', '00:00:00', '1969-12-31 23:59:59.999999', '1970-01-01 00:00:00+00', '-3 months', "
            + "'00000000-0000-0000-0000-000000000000', ARRAY[]::INT[], ARRAY['a,b','c\"d'])",
        "INSERT INTO dec_types (id) VALUES (3)",
    };

    /** Queries compared on both decoders. */
    static final String[] QUERIES = {
        "SELECT * FROM dec_types ORDER BY id",
        "SELECT SUM(id), AVG(i), SUM(i), SUM(n), AVG(n2), SUM(n50), COUNT(*), MIN(dt), MAX(ts), MAX(tz), "
            + "SUM(r), AVG(d), MIN(tm) FROM dec_types",
        "SELECT id, n2 * 2, n / 3, n50 + 1, i * 1.5, CAST(i AS NUMERIC(12,4)) FROM dec_types ORDER BY id",
        "SELECT CAST('2024-02-29' AS DATE), CAST('12:34:56.789' AS TIME), "
            + "CAST('2024-01-01 01:02:03.456789' AS TIMESTAMP), CAST('2024-01-01 01:02:03+05' AS TIMESTAMPTZ), "
            + "INTERVAL '1 day 2 hours', CAST('0001-01-01' AS DATE), CAST('9999-12-31 23:59:59' AS TIMESTAMP)",
        "SELECT ARRAY[1,2,3], ARRAY['a','b'], ARRAY[1.5, 2.25], ARRAY[TRUE, NULL]",
        "SELECT id, dt + 1, ts + INTERVAL '1 hour', EXTRACT(YEAR FROM ts), date_trunc('month', ts) FROM dec_types ORDER BY id",
        "SELECT NULL, CAST(NULL AS INT), CAST(NULL AS NUMERIC), CAST(NULL AS TIMESTAMP), CAST(NULL AS DATE)",
        "SELECT CAST(1 AS NUMERIC(5,2)) / 3, CAST('1e50' AS NUMERIC), CAST('-0.000001' AS NUMERIC), "
            + "CAST(12345.678 AS NUMERIC(8,3))",
        "SELECT bo, COUNT(*), SUM(n), AVG(i) FROM dec_types GROUP BY bo ORDER BY bo",
        "SELECT g, CAST(g AS NUMERIC) / 7, CAST('2024-01-01' AS DATE) + g FROM generate_series(1, 2000) g",
        "SELECT 1 AS x, 2.5 AS x, 'dup' AS x",
        "SELECT now() - now(), current_date - current_date",
    };

    /** A result big enough to arrive as chunked MSG_ARROW_STREAM frames (over 1 MiB). */
    static final String BIG = "SELECT g, CAST(g AS NUMERIC) / 7, CAST('2024-01-01 00:00:00' AS TIMESTAMP) + g * INTERVAL "
            + "'1 second', 'row-' || g FROM generate_series(1, 120000) g";

    @BeforeAll
    void start() throws Exception {
        server = PdbTestServer.tryStart();
        assumeTrue(server != null, "PDB_BIN required");
        String url = "jdbc:particledb://127.0.0.1:" + server.port + "/test";
        arrow = DriverManager.getConnection(url + "?resultDecoder=arrow");
        builtin = DriverManager.getConnection(url + "?resultDecoder=auto");
        try (Statement s = builtin.createStatement()) {
            s.execute(TABLE_DDL);
            for (String row : TABLE_ROWS) s.execute(row);
        }
    }

    @AfterAll
    void stop() throws Exception {
        if (arrow != null) arrow.close();
        if (builtin != null) builtin.close();
        if (server != null) server.stop();
    }

    /** Every getter of every cell, and every metadata answer, as comparable text. */
    static List<String> snapshot(ResultSet rs) throws SQLException {
        List<String> out = new ArrayList<>();
        ResultSetMetaData md = rs.getMetaData();
        int n = md.getColumnCount();
        for (int c = 1; c <= n; c++) {
            out.add("meta " + c + ": " + md.getColumnLabel(c) + " type=" + md.getColumnType(c) + " name="
                    + md.getColumnTypeName(c) + " prec=" + md.getPrecision(c) + " scale=" + md.getScale(c)
                    + " class=" + md.getColumnClassName(c) + " signed=" + md.isSigned(c));
        }
        int row = 0;
        while (rs.next()) {
            row++;
            for (int col = 1; col <= n; col++) {
                final int c = col;
                String at = "r" + row + "c" + c + " ";
                out.add(at + "object " + describe(() -> rs.getObject(c)));
                out.add(at + "string " + describe(() -> rs.getString(c)));
                out.add(at + "wasNull " + rs.wasNull());
                out.add(at + "long " + describe(() -> rs.getLong(c)));
                out.add(at + "double " + describe(() -> rs.getDouble(c)));
                out.add(at + "bigdecimal " + describe(() -> rs.getBigDecimal(c)));
                out.add(at + "boolean " + describe(() -> rs.getBoolean(c)));
                out.add(at + "date " + describe(() -> rs.getDate(c)));
                out.add(at + "time " + describe(() -> rs.getTime(c)));
                out.add(at + "timestamp " + describe(() -> rs.getTimestamp(c)));
                out.add(at + "bytes " + describe(() -> rs.getBytes(c)));
                out.add(at + "array " + describe(() -> rs.getArray(c)));
                out.add(at + "ldt " + describe(() -> rs.getObject(c, java.time.LocalDateTime.class)));
                out.add(at + "odt " + describe(() -> rs.getObject(c, java.time.OffsetDateTime.class)));
            }
        }
        out.add("rows " + row);
        return out;
    }

    interface Get {
        Object get() throws Exception;
    }

    static String describe(Get g) {
        Object v;
        try {
            v = g.get();
        } catch (SQLException e) {
            return "SQLException[" + e.getSQLState() + "] " + identityFree(e.getMessage());
        } catch (Exception e) {
            return e.getClass().getName() + " " + identityFree(e.getMessage());
        }
        try {
            if (v instanceof java.sql.Array) {
                java.sql.Array a = (java.sql.Array) v;
                return "Array<" + a.getBaseTypeName() + "> " + GoldenCases.show(a.getArray());
            }
        } catch (SQLException e) {
            return "Array SQLException[" + e.getSQLState() + "]";
        }
        return GoldenCases.show(v);
    }

    /** A message that prints an object's identity ({@code [B@1b6d3586}) differs per run; drop the hash. */
    static String identityFree(String msg) {
        return msg == null ? null : msg.replaceAll("@[0-9a-f]{4,}", "@<id>");
    }

    private void compare(String sql, int fetchSize, boolean prepared) throws Exception {
        List<String> a = run(arrow, sql, fetchSize, prepared);
        List<String> b = run(builtin, sql, fetchSize, prepared);
        assertEquals(a.size(), b.size(), sql);
        for (int i = 0; i < a.size(); i++) {
            assertEquals(a.get(i), b.get(i), sql + " (fetchSize=" + fetchSize + ", prepared=" + prepared + ")");
        }
    }

    private static List<String> run(Connection c, String sql, int fetchSize, boolean prepared) throws SQLException {
        if (prepared) {
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                if (fetchSize > 0) ps.setFetchSize(fetchSize);
                try (ResultSet rs = ps.executeQuery()) {
                    return snapshot(rs);
                }
            }
        }
        try (Statement s = c.createStatement()) {
            if (fetchSize > 0) s.setFetchSize(fetchSize);
            try (ResultSet rs = s.executeQuery(sql)) {
                return snapshot(rs);
            }
        }
    }

    @Test
    void everyQueryAnswersTheSameOnBothDecoders() throws Exception {
        for (String q : QUERIES) {
            compare(q, 0, false);
            compare(q, 0, true);
            compare(q, 7, false);
        }
    }

    @Test
    void chunkedAndStreamedResultsAnswerTheSame() throws Exception {
        compare(BIG, 0, false);
        compare(BIG, 5000, false);
        compare(BIG, 5000, true);
    }

    @Test
    void parameterizedQueriesAnswerTheSame() throws Exception {
        String sql = "SELECT id, n, n2, dt, ts, tz FROM dec_types WHERE id >= ? ORDER BY id";
        List<List<String>> got = new ArrayList<>();
        for (Connection c : Arrays.asList(arrow, builtin)) {
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setLong(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    got.add(snapshot(rs));
                }
            }
        }
        assertEquals(got.get(0), got.get(1));
    }

    @Test
    void captureLiveGoldens() throws Exception {
        String dir = System.getProperty("pdb.captureGolden");
        assumeTrue(dir != null && !dir.isEmpty(), "set -Dpdb.captureGolden=<dir> to (re)capture");
        Files.createDirectories(Paths.get(dir));
        PdbConnection pc = builtin.unwrap(PdbConnection.class);
        for (String[] live : ArrowIpcReaderGoldenTest.LIVE_QUERIES) {
            org.particledb.wire.StreamState.StreamResult r = pc.executeRaw(live[1]);
            assertEquals(org.particledb.wire.StreamState.StreamResult.Kind.ARROW, r.kind, live[1]);
            Files.write(Paths.get(dir, live[0] + ".arrow"), r.payload);
            ArrowDecoder.Decoded d = ArrowIpcReader.decode(r.payload);
            System.out.println("[capture] " + live[0] + " tokens=" + d.columnTypes + " meta=" + d.columnMetadata);
            for (Object[] row : d.rows) System.out.println("    " + GoldenCases.show(row));
        }
    }
}
