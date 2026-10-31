package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The driver is the REFERENCE implementation of the SDK conformance suite
 * ({@code conformance/SPEC.md}); these are its DEFAULTS, asserted against a
 * live server, from the SERVER's side wherever the server can say.
 *
 * <p>The headline is {@link #aPlainConnectIsMultiplexedV2_asTheServerRecordsIt}:
 * no test used to assert that a default connect is protocol 2. A silent fallback
 * to the unmultiplexed v1 wire "works", which is exactly why it needs a test
 * that fails.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReferenceDefaultsLiveTest {

    private PdbTestServer server;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null, "particledb binary not available");
    }

    @AfterAll
    void tearDown() {
        if (server != null) server.stop();
    }

    /** The documented zero-option URL: nothing but host and port. */
    private Connection plain() throws SQLException {
        return DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + server.port + "/");
    }

    private static String show(Connection c, String p) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SHOW " + p)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private static Set<String> csv(String s) {
        Set<String> out = new TreeSet<>();
        for (String p : s.split(",")) if (!p.isBlank()) out.add(p);
        return out;
    }

    @Test
    void aPlainConnectIsMultiplexedV2_asTheServerRecordsIt() throws Exception {
        try (Connection c = plain()) {
            assertEquals("pdb-native-v2", show(c, "pdb.wire_protocol"), "server's record of the protocol");
            assertEquals("arrow_ipc", show(c, "pdb.wire_result_format"));
            Set<String> caps = csv(show(c, "pdb.wire_capabilities"));
            for (String required : List.of("v2_mux", "prepared", "typed_params", "array_params",
                    "batch_sqlstate", "statement_notice", "numeric_text", "bulk_ingest")) {
                assertTrue(caps.contains(required), "server recorded " + caps + " -- missing " + required);
            }
            // The client's belief must be the server's record, not merely agree in part.
            PdbConnection p = c.unwrap(PdbConnection.class);
            assertEquals(show(c, "pdb.wire_protocol"), p.negotiatedProtocol());
            assertEquals(caps, new TreeSet<>(p.negotiatedCapabilities()));
        }
    }

    @Test
    void theRecordCannotBeForged() throws Exception {
        try (Connection c = plain(); Statement s = c.createStatement()) {
            SQLException e = assertThrows(SQLException.class, () -> s.execute("SET pdb.wire_protocol = 'postgres'"));
            assertEquals("55P02", e.getSQLState(), e.getMessage());
            assertFalse(e.getMessage().contains("55P02:"), "the SQLSTATE prefix is stripped from the message: " + e.getMessage());
            assertEquals("pdb-native-v2", show(c, "pdb.wire_protocol"));
        }
    }

    @Test
    void aPortlessUrlUsesTheDriversDefaultPort() throws Exception {
        assertEquals(7173, Driver.DEFAULT_PORT);
        assertEquals(Driver.DEFAULT_PORT, Pgcompat.parse("jdbc:particledb://somehost/db", new java.util.Properties()).port);
    }

    @Test
    void unconstrainedNumericKeepsEachValuesScale() throws Exception {
        try (Connection c = plain(); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT x FROM (VALUES (0.52::numeric), (1.123456::numeric)) t(x) ORDER BY x")) {
            assertEquals(java.sql.Types.NUMERIC, rs.getMetaData().getColumnType(1));
            assertTrue(rs.next());
            assertEquals(new BigDecimal("0.52"), rs.getBigDecimal(1), "not padded to the batch's largest scale");
            assertEquals(new BigDecimal("0.52"), rs.getObject(1));
            assertTrue(rs.next());
            assertEquals("1.123456", rs.getString(1));
        }
    }

    @Test
    void aFailedBatchEntryCarriesItsSqlState() throws Exception {
        try (Connection c = plain(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS rdl_b");
            s.execute("CREATE TABLE rdl_b (id INT PRIMARY KEY)");
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO rdl_b VALUES (?)")) {
                for (int id : new int[] {1, 1}) { ps.setInt(1, id); ps.addBatch(); }
                BatchUpdateException e = assertThrows(BatchUpdateException.class, ps::executeBatch);
                assertEquals("23505", e.getSQLState(), e.getMessage());
            }
        }
    }

    @Test
    void anEngineAdvisoryArrivesAsAStatementWarning() throws Exception {
        try (Connection c = plain(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS rdl_n");
            s.execute("CREATE TABLE rdl_n (id INT PRIMARY KEY, doc JSONB)");
            // Accepted for compatibility while backing no containment lookup:
            // the server raises an advisory for it (statement_notice).
            s.execute("CREATE INDEX rdl_n_gin ON rdl_n USING gin (doc)");
            assertNotNull(s.getWarnings(), "the server's advisory must reach the application");
            s.execute("SELECT 1");
            assertNull(s.getWarnings(), "a new execution clears the previous warnings");
        }
    }

    @Test
    void vectorAndByteaReadAsValuesNotArrayIdentities() throws Exception {
        try (Connection c = plain(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS rdl_v");
            s.execute("CREATE TABLE rdl_v (id INT PRIMARY KEY, v vector(3), b bytea)");
            s.execute("INSERT INTO rdl_v VALUES (1, '[1,2.5,-3]', '\\x00ff')");
            try (ResultSet rs = s.executeQuery("SELECT v, b FROM rdl_v")) {
                assertTrue(rs.next());
                Object v = rs.getObject(1);
                assertTrue(v instanceof PdbVector, String.valueOf(v));
                assertArrayEquals(new float[] {1f, 2.5f, -3f}, ((PdbVector) v).toArray());
                assertEquals("[1,2.5,-3]", rs.getString(1));
                assertEquals("\\x00ff", rs.getString(2));
                assertArrayEquals(new byte[] {0, (byte) 0xff}, rs.getBytes(2));
            }
        }
    }

    @Test
    void nativeKeyValueFramesRoundTripBinarySafely() throws Exception {
        byte[] all = new byte[256];
        for (int i = 0; i < 256; i++) all[i] = (byte) i;
        byte[] key = "rdl:kv".getBytes(StandardCharsets.UTF_8);
        try (Connection c = plain()) {
            PdbConnection p = c.unwrap(PdbConnection.class);
            p.kvDel(key);
            assertNull(p.kvGet(key));
            p.kvSet(key, all);
            assertArrayEquals(all, p.kvGet(key));
            assertTrue(p.kvDel(key));
            assertFalse(p.kvDel(key));
            byte[] n = "rdl:kvn".getBytes(StandardCharsets.UTF_8);
            p.kvDel(n);
            assertEquals(1, p.kvIncr(n));
            assertEquals(2, p.kvIncr(n));
            // Same keyspace as SQL's KV_GET (and RESP, which the conformance suite checks).
            try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT KV_GET('rdl:kvn')")) {
                assertTrue(rs.next());
                assertEquals("2", rs.getString(1));
            }
        }
    }

    @Test
    void savepointNamesAreQuotedIdentifiers() throws Exception {
        try (Connection c = plain(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            try {
                java.sql.Savepoint sp = c.setSavepoint("odd \"name\"; DROP");
                c.rollback(sp);
                c.releaseSavepoint(sp);
                c.rollback();
            } finally {
                c.setAutoCommit(true);
            }
        }
    }
}
