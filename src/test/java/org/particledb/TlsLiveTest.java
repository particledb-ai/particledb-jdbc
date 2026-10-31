package org.particledb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TlsLiveTest {
    private PdbTestServer server;
    private Path directory;
    private Path certificate;

    @BeforeAll
    void start() throws Exception {
        assumeTrue(System.getenv("PDB_BIN") != null
                || Boolean.getBoolean("pdb.requireServer"), "PDB_BIN required for live TLS checks");
        directory = Files.createTempDirectory("pdb-jdbc-tls-");
        certificate = directory.resolve("server.pem");
        Path key = directory.resolve("server.key");
        Process openssl = new ProcessBuilder("openssl", "req", "-x509", "-newkey", "rsa:2048",
                "-nodes", "-keyout", key.toString(), "-out", certificate.toString(), "-days", "2",
                "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost")
                .redirectErrorStream(true).redirectOutput(directory.resolve("openssl.log").toFile()).start();
        assertTrue(openssl.waitFor(20, TimeUnit.SECONDS), "certificate generation timed out");
        assertEquals(0, openssl.exitValue(), "certificate generation failed");
        server = PdbTestServer.tryStart("--tls-cert", certificate.toString(), "--tls-key", key.toString());
        assertNotNull(server, "live TLS checks require an executable server");
    }

    private String url(String host, String mode) {
        return "jdbc:particledb://" + host + ":" + server.port
                + "/test?sslMode=" + mode + "&preferUnixSocket=false";
    }

    private Properties roots() {
        Properties properties = new Properties();
        properties.setProperty("sslRootCert", certificate.toString());
        return properties;
    }

    @Test
    void fullVerificationRunsQueriesThroughTls() throws Exception {
        try (var connection = DriverManager.getConnection(url("localhost", "verify-full"), roots());
             var statement = connection.createStatement(); var result = statement.executeQuery("SELECT 42")) {
            assertTrue(result.next());
            assertEquals(42, result.getInt(1));
        }
    }

    @Test
    void networkTimeoutClosesTlsWithoutWaitingForTheQueryOrShutdown() throws Exception {
        try (var connection = DriverManager.getConnection(url("localhost", "verify-full"), roots());
             var statement = connection.createStatement()) {
            connection.setNetworkTimeout(Runnable::run, 100);
            long started = System.nanoTime();
            SQLException failure = assertThrows(SQLException.class,
                    () -> statement.executeQuery("SELECT pg_sleep(5)"));
            assertEquals("08006", failure.getSQLState());
            assertTrue(connection.isClosed());
            assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 3000);
        }
    }

    @Test
    void chainVerificationAcceptsADifferentHostWhenExplicitlyRequested() throws Exception {
        try (var connection = DriverManager.getConnection(url("127.0.0.1", "verify-ca"), roots());
             var statement = connection.createStatement(); var result = statement.executeQuery("SELECT 7")) {
            assertTrue(result.next());
            assertEquals(7, result.getInt(1));
        }
    }

    @Test
    void fullVerificationRefusesTheWrongHost() {
        assertThrows(SQLException.class,
                () -> DriverManager.getConnection(url("127.0.0.1", "verify-full"), roots()));
    }

    @Test
    void verificationRefusesAnUntrustedCertificate() {
        assertThrows(SQLException.class,
                () -> DriverManager.getConnection(url("localhost", "verify-full")));
    }

    @Test
    void requireEncryptsWithoutCertificateVerification() throws Exception {
        try (var connection = DriverManager.getConnection(url("127.0.0.1", "require"));
             var statement = connection.createStatement(); var result = statement.executeQuery("SELECT 1")) {
            assertTrue(result.next());
            assertEquals(1, result.getInt(1));
        }
    }

    @Test
    void tlsNeverFallsBackToAPlaintextServer() throws Exception {
        try (var plaintext = new AmbiguousFailureTest.FakeServer()) {
            assertThrows(SQLException.class, () -> DriverManager.getConnection(
                    "jdbc:particledb://127.0.0.1:" + plaintext.port()
                            + "/test?sslMode=require&preferUnixSocket=false"));
            assertEquals(0, plaintext.requests.get());
        }
    }

    @AfterAll
    void stop() throws Exception {
        if (server != null) server.stop();
        if (directory != null) {
            try (var paths = Files.walk(directory)) {
                for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toArray(Path[]::new)) {
                    Files.deleteIfExists(path);
                }
            }
        }
    }
}
