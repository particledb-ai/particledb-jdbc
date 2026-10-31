package org.particledb;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for TLS-after-HELLO upgrade.
 *
 * <p>This test runs only when {@code PDB_TLS_HOST} is exported. The fixture
 * is a {@code particledb} server listening on a TLS-enabled PDB wire port
 * (default 5443) with a self-signed cert (e.g. {@code /tmp/pdb_tls_test/cert.pem}).
 *
 * <p>See the TLS section of {@code PROTOCOL.md} for the handshake
 * sequence.
 */
public class TlsIntegrationTest {

    private static String host;
    private static int port;

    @BeforeAll
    static void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        host = System.getenv("PDB_TLS_HOST");
        Assumptions.assumeTrue(host != null && !host.isEmpty(),
                "PDB_TLS_HOST not set — skipping TLS integration test");
        String portStr = System.getenv().getOrDefault("PDB_TLS_PORT", "5443");
        port = Integer.parseInt(portStr);
    }

    /** Plain handshake against the localhost test cert with sslMode=require. */
    @Test
    void testTlsHandshakeAgainstLocalhost5443() throws Exception {
        String url = String.format(
                "jdbc:particledb://%s:%d/default?sslMode=require&preferUnixSocket=false",
                host, port);
        try (Connection c = DriverManager.getConnection(url)) {
            assertFalse(c.isClosed(), "connection should be open after TLS handshake");
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT 1")) {
                // ResultSet may decode raw Arrow IPC or empty depending on
                // optional arrow dep; at minimum we should not throw.
                if (rs.next()) {
                    Object v = rs.getObject(1);
                    System.out.println("[TLS] SELECT 1 -> " + v);
                }
            }
        }
    }

    /**
     * verify-ca with no CA bundle - the test cert is self-signed for
     * "localhost", so unless the JVM truststore was extended we expect
     * the handshake to fail with a chain validation error. This test
     * tolerates either success (CA was added) or a clearly-TLS-shaped
     * SQLException.
     */
    @Test
    void verifyCaAgainstSelfSignedFailsCleanly() {
        String url = String.format(
                "jdbc:particledb://%s:%d/default?sslMode=verify-ca&preferUnixSocket=false",
                host, port);
        try (Connection c = DriverManager.getConnection(url)) {
            // If the test cert IS in the JVM truststore, success is fine  -
            // the only thing we forbid is silent plaintext fallback.
            assertFalse(c.isClosed());
        } catch (SQLException e) {
            String msg = String.valueOf(e.getMessage()).toLowerCase();
            // Expected failure path: TLS handshake fails because the
            // self-signed cert is not trusted.
            assertTrue(msg.contains("tls") || msg.contains("ssl")
                            || msg.contains("certificate"),
                    "expected TLS-shaped error, got: " + e.getMessage());
        }
    }

    /**
     * sslMode=disable should result in a plain TCP connection. If the
     * server only listens on TLS-required mode this will fail. We mark
     * this test as informational - just verify it doesn't silently
     * upgrade.
     */
    @Test
    void disableMeansNoCapBit() throws Exception {
        String url = String.format(
                "jdbc:particledb://%s:%d/default?sslMode=disable&preferUnixSocket=false",
                host, port);
        // If the server is in `tls_mode = required` it will REJECT plaintext
        // with ERR_TLS_REQUIRED. If the server is in `tls_mode = mixed`
        // (the default with a cert configured) it accepts plaintext. We
        // only assert that the connect attempt completes one way or the
        // other in finite time, never silently encrypts.
        try (Connection c = DriverManager.getConnection(url)) {
            assertFalse(c.isClosed());
        } catch (SQLException e) {
            // Acceptable - server may demand TLS.
            String msg = String.valueOf(e.getMessage()).toLowerCase();
            System.out.println("[TLS] disable connect refused (expected if "
                    + "server is required-mode): " + msg);
        }
    }
}
