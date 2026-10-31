package org.particledb;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Native bulk ingest against a live server ({@code PdbTestServer}, {@code PDB_BIN}):
 * {@code PdbConnection.bulkLoad} round-trips 10k typed rows exactly (and a load
 * big enough to need several DATA frames); {@code executeBatch} of a plain
 * INSERT goes through ONE bulk ingest, and a duplicate key anywhere in the
 * batch raises 23505 and loads nothing; a load inside {@code autoCommit=false}
 * is invisible to another connection until commit, and rollback leaves nothing.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@org.junit.jupiter.api.Tag("arrow-java")
public class BulkIngestLiveTest {

    private PdbTestServer server;
    private BufferAllocator alloc;

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null, "particledb binary not available — skipping bulk ingest test");
        alloc = new RootAllocator();
    }

    @AfterAll
    void tearDown() {
        if (alloc != null) alloc.close();
        if (server != null) server.stop();
    }

    private Connection open(String extra) throws SQLException {
        return DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + server.port
                + "/test?preferUnixSocket=false" + extra);
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static long count(Connection c, String table) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT count(*) FROM " + table)) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    private static final long TS0 = 1_700_000_000_000L; // epoch ms

    private static String vOf(int i) {
        return i % 97 == 0 ? null : "row-" + i + (i % 5 == 0 ? " it's \"q\" é" : "");
    }

    private static BigDecimal nOf(int i) {
        return BigDecimal.valueOf((long) i * 37 - 50_000, 2);
    }

    /** id bigint, v text (with NULLs), n as VarChar text, ts typed timestamptz (micros, UTC). */
    private VectorSchemaRoot typedRoot(int from, int rows) {
        BigIntVector id = new BigIntVector("id", alloc);
        VarCharVector v = new VarCharVector("v", alloc);
        VarCharVector n = new VarCharVector("n", alloc);
        TimeStampMicroTZVector ts = new TimeStampMicroTZVector("ts", alloc, "UTC");
        id.allocateNew(rows);
        v.allocateNew(rows);
        n.allocateNew(rows);
        ts.allocateNew(rows);
        for (int k = 0; k < rows; k++) {
            int i = from + k;
            id.set(k, i);
            String s = vOf(i);
            if (s == null) v.setNull(k);
            else v.setSafe(k, s.getBytes(StandardCharsets.UTF_8));
            n.setSafe(k, nOf(i).toPlainString().getBytes(StandardCharsets.UTF_8));
            ts.set(k, (TS0 + i * 1000L) * 1000L);
        }
        VectorSchemaRoot root = new VectorSchemaRoot(Arrays.asList(id, v, n, ts));
        root.setRowCount(rows);
        return root;
    }

    /** Rows loaded from bound JDBC values: ts read back the way it was bound (getTimestamp). */
    private static void assertRows(Connection c, String table, int rows) throws SQLException {
        assertRows(c, table, rows, false);
    }

    /**
     * {@code typedTs}: ts was shipped as a UTC-micros Arrow timestamp, so the
     * stored INSTANT is checked (epoch microseconds, computed by the server),
     * independent of how a JDBC Timestamp maps wall-clock time.
     */
    private static void assertRows(Connection c, String table, int rows, boolean typedTs) throws SQLException {
        String tsExpr = typedTs ? "(extract(epoch from ts) * 1000)::bigint" : "ts";
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT id, v, n, " + tsExpr + " FROM " + table + " ORDER BY id")) {
            for (int i = 0; i < rows; i++) {
                assertTrue(rs.next(), "row " + i);
                assertEquals(i, rs.getLong(1));
                assertEquals(vOf(i), rs.getString(2), "v of " + i);
                assertEquals(0, nOf(i).compareTo(rs.getBigDecimal(3)), "n of " + i);
                long ms = typedTs ? rs.getLong(4) : rs.getTimestamp(4).getTime();
                assertEquals(TS0 + i * 1000L, ms, "ts of " + i);
            }
            assertFalse(rs.next());
        }
    }

    private static final String DDL = " (id bigint primary key, v text, n numeric(10,2), ts timestamptz)";

    @Test
    void bulkLoadRoundTripsTenThousandRowsExactly() throws Exception {
        try (Connection c = open("")) {
            exec(c, "CREATE TABLE bl_rt" + DDL);
            PdbConnection pc = c.unwrap(PdbConnection.class);
            long loaded;
            try (VectorSchemaRoot root = typedRoot(0, 10_000)) {
                loaded = pc.bulkLoad("bl_rt", null, root);
            }
            assertEquals(10_000, loaded);
            assertRows(c, "bl_rt", 10_000, true);
        }
    }

    @Test
    void bulkLoadFromAnArrowReaderAcrossSeveralDataFrames() throws Exception {
        // ~200k rows of ~120-byte strings: over 12 MiB, so several DATA frames.
        int rows = 200_000;
        String pad = String.join("", java.util.Collections.nCopies(110, "x"));
        ByteArrayOutputStream ipc = new ByteArrayOutputStream();
        BigIntVector id = new BigIntVector("id", alloc);
        VarCharVector v = new VarCharVector("v", alloc);
        try (VectorSchemaRoot root = new VectorSchemaRoot(Arrays.asList(id, v))) {
            try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, Channels.newChannel(ipc))) {
                w.start();
                for (int b = 0; b < 2; b++) {
                    id.allocateNew(rows / 2);
                    v.allocateNew(rows / 2);
                    for (int k = 0; k < rows / 2; k++) {
                        int i = b * (rows / 2) + k;
                        id.set(k, i);
                        v.setSafe(k, (i + pad).getBytes(StandardCharsets.UTF_8));
                    }
                    root.setRowCount(rows / 2);
                    w.writeBatch();
                }
                w.end();
            }
        }
        try (Connection c = open("")) {
            exec(c, "CREATE TABLE bl_big (id bigint primary key, v text)");
            long loaded;
            try (ArrowStreamReader r = new ArrowStreamReader(new ByteArrayInputStream(ipc.toByteArray()), alloc)) {
                loaded = c.unwrap(PdbConnection.class).bulkLoad("bl_big", r);
            }
            assertEquals(rows, loaded);
            assertEquals(rows, count(c, "bl_big"));
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT v FROM bl_big WHERE id = 123456")) {
                assertTrue(rs.next());
                assertEquals("123456" + pad, rs.getString(1));
            }
        }
    }

    private static void bindRow(PreparedStatement ps, int i) throws SQLException {
        ps.setLong(1, i);
        ps.setString(2, vOf(i));
        ps.setBigDecimal(3, nOf(i));
        ps.setTimestamp(4, new Timestamp(TS0 + i * 1000L));
    }

    @Test
    void executeBatchOfAPlainInsertIsOneBulkIngest() throws Exception {
        try (Connection c = open("")) {
            exec(c, "CREATE TABLE bl_batch" + DDL);
            PdbConnection pc = c.unwrap(PdbConnection.class);
            long before = pc.bulkBatchesLoaded();
            int rows = 2_000;
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO bl_batch (id, v, n, ts) VALUES (?, ?, ?, ?)")) {
                for (int i = 0; i < rows; i++) {
                    bindRow(ps, i);
                    ps.addBatch();
                }
                int[] counts = ps.executeBatch();
                assertEquals(rows, counts.length);
                for (int k : counts) assertEquals(1, k);
            }
            assertEquals(before + 1, pc.bulkBatchesLoaded(), "the batch went through F_BULK");
            assertRows(c, "bl_batch", rows);

            // No column list: the table's columns, from its metadata.
            exec(c, "CREATE TABLE bl_batch2" + DDL);
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO bl_batch2 VALUES (?, ?, ?, ?)")) {
                for (int i = 0; i < 300; i++) {
                    bindRow(ps, i);
                    ps.addBatch();
                }
                assertEquals(300, ps.executeBatch().length);
            }
            assertEquals(before + 2, pc.bulkBatchesLoaded());
            assertRows(c, "bl_batch2", 300);
        }
    }

    @Test
    void duplicateKeyInABulkBatchRaises23505AndLoadsNothing() throws Exception {
        try (Connection c = open("")) {
            exec(c, "CREATE TABLE bl_dup" + DDL);
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO bl_dup (id, v, n, ts) VALUES (?, ?, ?, ?)")) {
                for (int i = 0; i < 500; i++) {
                    bindRow(ps, i == 400 ? 7 : i);
                    ps.addBatch();
                }
                BatchUpdateException e = assertThrows(BatchUpdateException.class, ps::executeBatch);
                assertEquals("23505", e.getSQLState(), e.getMessage());
                assertEquals(0, e.getUpdateCounts().length);
            }
            assertEquals(0, count(c, "bl_dup"));
            // A key that collides with an existing row: the same.
            exec(c, "INSERT INTO bl_dup (id) VALUES (3)");
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO bl_dup (id, v, n, ts) VALUES (?, ?, ?, ?)")) {
                for (int i = 0; i < 10; i++) {
                    bindRow(ps, i);
                    ps.addBatch();
                }
                BatchUpdateException e = assertThrows(BatchUpdateException.class, ps::executeBatch);
                assertEquals("23505", e.getSQLState(), e.getMessage());
            }
            assertEquals(1, count(c, "bl_dup"));
            // The connection is still usable.
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO bl_dup (id, v, n, ts) VALUES (?, ?, ?, ?)")) {
                bindRow(ps, 100);
                ps.addBatch();
                bindRow(ps, 101);
                ps.addBatch();
                assertEquals(2, ps.executeBatch().length);
            }
            assertEquals(3, count(c, "bl_dup"));
        }
    }

    @Test
    void bulkBatchFalseKeepsTheTextBatch() throws Exception {
        try (Connection c = open("&bulkBatch=false")) {
            exec(c, "CREATE TABLE bl_text" + DDL);
            PdbConnection pc = c.unwrap(PdbConnection.class);
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO bl_text (id, v, n, ts) VALUES (?, ?, ?, ?)")) {
                for (int i = 0; i < 50; i++) {
                    bindRow(ps, i);
                    ps.addBatch();
                }
                assertEquals(50, ps.executeBatch().length);
            }
            assertEquals(0, pc.bulkBatchesLoaded());
            assertRows(c, "bl_text", 50);
        }
    }

    @Test
    void bulkLoadInsideATransactionIsInvisibleUntilCommitAndRollbackLeavesNothing() throws Exception {
        try (Connection c = open(""); Connection other = open("")) {
            exec(c, "CREATE TABLE bl_txn" + DDL);
            PdbConnection pc = c.unwrap(PdbConnection.class);
            c.setAutoCommit(false);
            try (VectorSchemaRoot root = typedRoot(0, 1_000)) {
                assertEquals(1_000, pc.bulkLoad("bl_txn", null, root));
            }
            assertEquals(1_000, count(c, "bl_txn"), "own writes visible inside the transaction");
            assertEquals(0, count(other, "bl_txn"), "invisible to another connection before commit");
            c.commit();
            assertEquals(1_000, count(other, "bl_txn"));

            try (VectorSchemaRoot root = typedRoot(1_000, 500)) {
                assertEquals(500, pc.bulkLoad("bl_txn", null, root));
            }
            assertEquals(1_000, count(other, "bl_txn"));
            c.rollback();
            assertEquals(1_000, count(other, "bl_txn"));
            assertEquals(1_000, count(c, "bl_txn"));

            // executeBatch in a transaction: rolled back with it.
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO bl_txn (id, v, n, ts) VALUES (?, ?, ?, ?)")) {
                for (int i = 5_000; i < 5_010; i++) {
                    bindRow(ps, i);
                    ps.addBatch();
                }
                assertEquals(10, ps.executeBatch().length);
            }
            assertEquals(0, count(other, "bl_txn") - 1_000);
            c.rollback();
            c.setAutoCommit(true);
            assertEquals(1_000, count(c, "bl_txn"));
        }
    }
}
