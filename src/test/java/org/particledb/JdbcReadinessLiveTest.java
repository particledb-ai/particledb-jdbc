package org.particledb;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.particledb.ds.PdbDataSource;

import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class JdbcReadinessLiveTest {
    public static class PlainJvm {
        public static void main(String[] args) throws Exception {
            try (var c = DriverManager.getConnection(args[0]); var s = c.createStatement()) {
                s.execute("CREATE TABLE plain_jvm (id INTEGER PRIMARY KEY, name TEXT)");
                try (var p = c.prepareStatement("INSERT INTO plain_jvm VALUES (?, ?)")) {
                    for (int i : List.of(1, 2)) {
                        p.setInt(1, i);
                        p.setString(2, "name" + i);
                        p.addBatch();
                    }
                    int[] counts = p.executeBatch();
                    if (!java.util.Arrays.equals(new int[]{1, 1}, counts)) throw new AssertionError("batch counts");
                }
                try (var r = s.executeQuery("SELECT count(*) FROM plain_jvm")) {
                    if (!r.next() || r.getInt(1) != 2) throw new AssertionError("batch rows");
                }
            }
            System.out.println("plain-jvm-ok");
        }
    }

    @Test
    void ordinaryJdbcUsageNeedsNoModuleFlags() throws Exception {
        PdbTestServer server = PdbTestServer.tryStart();
        assumeTrue(server != null, "PDB_BIN required");
        java.nio.file.Path log = java.nio.file.Files.createTempFile("pdb-plain-jvm-", ".log");
        Process process = null;
        try {
            process = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-cp",
                    System.getProperty("java.class.path"), PlainJvm.class.getName(),
                    "jdbc:particledb://127.0.0.1:" + server.port + "/test")
                    .redirectErrorStream(true).redirectOutput(log.toFile()).start();
            assertTrue(process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS), "plain JVM timed out");
            String output = java.nio.file.Files.readString(log);
            assertEquals(0, process.exitValue(), output);
            assertTrue(output.contains("plain-jvm-ok"), output);
        } finally {
            if (process != null && process.isAlive()) process.destroyForcibly();
            server.stop();
            java.nio.file.Files.deleteIfExists(log);
        }
    }

    @Test
    void typedSetObjectAndCloseOnCompletionFollowTheJdbcContract() throws Exception {
        PdbTestServer server = PdbTestServer.tryStart();
        assumeTrue(server != null, "PDB_BIN required");
        try (var c = DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + server.port + "/test")) {
            try (var p = c.prepareStatement("SELECT ?")) {
                assertEquals("07001", assertThrows(SQLException.class, p::executeQuery).getSQLState());
                assertEquals("07001", assertThrows(SQLException.class, p::addBatch).getSQLState());
                p.setNull(1, java.sql.Types.INTEGER);
                try (var result = p.executeQuery()) {
                    assertTrue(result.next());
                    assertNull(result.getObject(1));
                }
                p.clearParameters();
                assertEquals("07001", assertThrows(SQLException.class, p::execute).getSQLState());
            }
            try (var p = c.prepareStatement("SELECT 1")) {
                assertEquals("07009", assertThrows(SQLException.class, () -> p.setInt(1, 2)).getSQLState());
            }
            try (var p = c.prepareStatement("SELECT ? AS n, ? AS t, ? AS d, ? AS empty_value")) {
                p.setObject(1, "42", JDBCType.INTEGER);
                p.setObject(2, 42, java.sql.Types.VARCHAR);
                p.setObject(3, "12.345", JDBCType.NUMERIC, 2);
                p.setObject(4, null, JDBCType.DATE);
                try (var r = p.executeQuery()) {
                    assertTrue(r.next());
                    assertEquals(42, r.getInt(1));
                    assertEquals("42", r.getString(2));
                    assertEquals(new java.math.BigDecimal("12.35"), r.getBigDecimal(3));
                    assertNull(r.getObject(4));
                    assertTrue(r.wasNull());
                    assertTrue(r.getObject(1) instanceof Number);
                    assertEquals(java.sql.Types.VARCHAR, r.getMetaData().getColumnType(2));
                }
                assertThrows(SQLFeatureNotSupportedException.class,
                        () -> p.setObject(1, new Object(), java.sql.Types.JAVA_OBJECT));
            }
            try (var s = c.createStatement()) {
                s.execute("CREATE TABLE target_batch (id INTEGER, amount NUMERIC)");
            }
            try (var p = c.prepareStatement("INSERT INTO target_batch VALUES (?, ?)")) {
                for (int i : List.of(1, 2)) {
                    p.setObject(1, Integer.toString(i), JDBCType.INTEGER);
                    p.setObject(2, "12.345", JDBCType.NUMERIC, 2);
                    p.addBatch();
                }
                assertArrayEquals(new int[]{1, 1}, p.executeBatch());
            }
            var s = c.createStatement();
            s.closeOnCompletion();
            assertTrue(s.isCloseOnCompletion());
            var r = s.executeQuery("SELECT sum(amount) FROM target_batch");
            assertTrue(r.next());
            assertEquals(new java.math.BigDecimal("24.70"), r.getBigDecimal(1));
            assertFalse(s.isClosed());
            r.close();
            assertTrue(s.isClosed());
            r.close();
            assertThrows(SQLException.class, () -> s.executeQuery("SELECT 1"));
            try (var p = c.prepareStatement("SELECT g FROM generate_series(1, 10) AS g ORDER BY g "
                    + "OFFSET ? ROWS FETCH FIRST ? ROWS ONLY")) {
                p.setInt(1, 2);
                p.setInt(2, 3);
                try (var result = p.executeQuery()) {
                    for (int expected : List.of(3, 4, 5)) {
                        assertTrue(result.next());
                        assertEquals(expected, result.getInt(1));
                    }
                    assertFalse(result.next());
                }
            }
        } finally {
            server.stop();
        }
    }

    @Test
    void realPoolRollsBackAndResetsConnectionsBetweenBorrowers() throws Exception {
        PdbTestServer server = PdbTestServer.tryStart();
        assumeTrue(server != null, "PDB_BIN required");
        try {
            PdbDataSource source = new PdbDataSource();
            source.setHost("127.0.0.1");
            source.setPort(server.port);
            source.setPreferUnixSocket(false);
            source.setLoginTimeout(5);
            HikariConfig config = new HikariConfig();
            config.setDataSource(source);
            config.setMaximumPoolSize(1);
            config.setMinimumIdle(0);
            config.setConnectionTimeout(5000);
            config.setValidationTimeout(2000);
            try (HikariDataSource pool = new HikariDataSource(config)) {
                try (var c = pool.getConnection(); var s = c.createStatement()) {
                    s.execute("CREATE TABLE pooled_work (id INTEGER PRIMARY KEY)");
                    c.setAutoCommit(false);
                    s.executeUpdate("INSERT INTO pooled_work VALUES (1)");
                }
                try (var c = pool.getConnection(); var s = c.createStatement()) {
                    assertTrue(c.getAutoCommit());
                    assertEquals(0, c.getNetworkTimeout());
                    try (var r = s.executeQuery("SELECT count(*) FROM pooled_work")) {
                        assertTrue(r.next());
                        assertEquals(0, r.getInt(1));
                    }
                    s.executeUpdate("INSERT INTO pooled_work VALUES (2)");
                }
                try (var c = pool.getConnection(); var s = c.createStatement();
                     var r = s.executeQuery("SELECT id FROM pooled_work")) {
                    assertTrue(r.next());
                    assertEquals(2, r.getInt(1));
                    assertFalse(r.next());
                }
            }
        } finally {
            server.stop();
        }
    }
}
