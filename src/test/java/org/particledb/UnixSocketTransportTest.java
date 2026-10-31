package org.particledb;

import org.particledb.wire.StreamMux;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.ProtocolFamily;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@code preferUnixSocket} actually connects over the server's Unix socket
 * {@code <unixSocketDir>/.s.PDB.<port>}, falls back to TCP only when no server
 * is behind that path, and never retries on TCP after a failure that is not
 * "nobody there".
 *
 * <p>Through 1.0.8 the option was stored by {@code StreamMux} and never read:
 * every connection used TCP. The PROOF a connection used the Unix socket does
 * not rest on the driver's own report ({@link StreamMux#isUnixSocket()}): the
 * strongest tests point the URL at a TCP port NOTHING listens on, so a
 * connection that works there cannot have used TCP -- and a positive control
 * shows the same URL with {@code preferUnixSocket=false} fails.
 */
@DisabledOnOs(value = OS.WINDOWS, disabledReason = "Server Unix sockets require a Unix host")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class UnixSocketTransportTest {

    private static final boolean JAVA16 = Runtime.version().feature() >= 16;

    private PdbTestServer server;
    /** The live server's socket: {@code /tmp/.s.PDB.<port>} (server default dir). */
    private Path serverSocket;
    private final List<Path> cleanup = new ArrayList<>();

    @BeforeAll
    void setUp() throws Exception {
        Class.forName("org.particledb.Driver");
        server = PdbTestServer.tryStart();
        Assumptions.assumeTrue(server != null, "particledb binary not available");
        serverSocket = Paths.get("/tmp", ".s.PDB." + server.port);
        // The server binds its Unix socket on a thread after the TCP port.
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (!Files.exists(serverSocket) && System.nanoTime() < deadline) Thread.sleep(50);
        assertTrue(Files.exists(serverSocket), "server never created " + serverSocket);
    }

    @AfterAll
    void tearDown() throws IOException {
        if (server != null) server.stop();
        for (int i = cleanup.size() - 1; i >= 0; i--) Files.deleteIfExists(cleanup.get(i));
    }

    private static StreamMux mux(Connection c) {
        return ((PdbConnection) c).mux();
    }

    private static int selectOne(Connection c) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    /** A TCP port nothing listens on (closed right after the OS picked it). */
    private static int unusedPort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private Path tempDir() throws IOException {
        // Short: macOS caps sun_path at 104 bytes and java.io.tmpdir is long there.
        Path d = Files.createTempDirectory(Paths.get("/tmp"), "pdbus");
        cleanup.add(d);
        return d;
    }

    // ── It uses the socket ────────────────────────────────────────────────

    @Test
    void defaultLoopbackConnectionUsesTheServersUnixSocket() throws Exception {
        Assumptions.assumeTrue(JAVA16, "Unix-domain channels need Java 16+");
        for (String host : new String[] { "localhost", "127.0.0.1", "[::1]" }) {
            try (Connection c = DriverManager.getConnection(
                    "jdbc:particledb://" + host + ":" + server.port + "/default")) {
                assertTrue(mux(c).isUnixSocket(), host + ": expected the Unix socket, got "
                        + mux(c).transportDescription());
                assertEquals("unix:" + serverSocket, mux(c).transportDescription());
                assertEquals(1, selectOne(c));
            }
        }
    }

    @Test
    void preferUnixSocketFalseUsesTcp() throws Exception {
        try (Connection c = DriverManager.getConnection(
                "jdbc:particledb://localhost:" + server.port + "/default?preferUnixSocket=false")) {
            assertFalse(mux(c).isUnixSocket());
            assertEquals("tcp:localhost:" + server.port, mux(c).transportDescription());
            assertEquals(1, selectOne(c));
        }
    }

    /**
     * Server-independent proof: the URL names a TCP port nothing listens on.
     * The connection works through {@code unixSocketPath}; the identical URL
     * over TCP is refused (positive control: the port really is dead).
     */
    @Test
    void explicitSocketPathWorksWithTheTcpPortDead() throws Exception {
        Assumptions.assumeTrue(JAVA16, "Unix-domain channels need Java 16+");
        int dead = unusedPort();
        String base = "jdbc:particledb://localhost:" + dead + "/default?connectTimeout=2000";
        try (Connection c = DriverManager.getConnection(base + "&unixSocketPath=" + serverSocket)) {
            assertTrue(mux(c).isUnixSocket());
            assertEquals(1, selectOne(c));
            // A real round trip that writes: the session is fully functional.
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE IF NOT EXISTS us_probe (id INT PRIMARY KEY, v INT)");
                st.execute("INSERT INTO us_probe VALUES (1, 10)");
                assertEquals(1, st.executeUpdate("UPDATE us_probe SET v = v + 1 WHERE id = 1"));
                try (ResultSet rs = st.executeQuery("SELECT v FROM us_probe WHERE id = 1")) {
                    assertTrue(rs.next());
                    assertEquals(11, rs.getInt(1));
                }
                st.execute("DROP TABLE us_probe");
            }
        }
        SQLException tcp = assertThrows(SQLException.class,
                () -> DriverManager.getConnection(base + "&preferUnixSocket=false").close());
        assertEquals("08001", tcp.getSQLState());
    }

    /**
     * The DERIVED path: {@code <unixSocketDir>/.s.PDB.<port>} for a loopback
     * host. A symlink named for a dead TCP port leads to the live server, so
     * only the derived Unix path can have carried this connection.
     */
    @Test
    void derivedPathInUnixSocketDirIsUsedWithTheTcpPortDead() throws Exception {
        Assumptions.assumeTrue(JAVA16, "Unix-domain channels need Java 16+");
        int dead = unusedPort();
        Path dir = tempDir();
        Path link = Files.createSymbolicLink(dir.resolve(".s.PDB." + dead), serverSocket);
        cleanup.add(link);
        try (Connection c = DriverManager.getConnection("jdbc:particledb://127.0.0.1:" + dead
                + "/default?connectTimeout=2000&unixSocketDir=" + dir)) {
            assertTrue(mux(c).isUnixSocket());
            assertEquals("unix:" + link, mux(c).transportDescription());
            assertEquals(1, selectOne(c));
        }
    }

    /** A host that is not a loopback name/literal never tries the socket. */
    @Test
    void loopbackDetectionIsLiteral() {
        assertTrue(StreamMux.isLoopbackHost("localhost"));
        assertTrue(StreamMux.isLoopbackHost("LOCALHOST"));
        assertTrue(StreamMux.isLoopbackHost("127.0.0.1"));
        assertTrue(StreamMux.isLoopbackHost("::1"));
        assertTrue(StreamMux.isLoopbackHost("[::1]"));
        assertFalse(StreamMux.isLoopbackHost("127.0.0.2"));
        assertFalse(StreamMux.isLoopbackHost("db.example.com"));
        assertFalse(StreamMux.isLoopbackHost("10.0.0.1"));
        assertFalse(StreamMux.isLoopbackHost(null));
    }

    // ── Fallback: only when nobody is there ───────────────────────────────

    @Test
    void absentSocketFallsBackToTcp() throws Exception {
        Path empty = tempDir();
        try (Connection c = DriverManager.getConnection("jdbc:particledb://localhost:"
                + server.port + "/default?unixSocketDir=" + empty)) {
            assertFalse(mux(c).isUnixSocket());
            assertEquals(1, selectOne(c));
        }
    }

    /** A socket file nobody accepts on (a crashed server's leftover) is "nobody there". */
    @Test
    void staleSocketFileFallsBackToTcp() throws Exception {
        Assumptions.assumeTrue(JAVA16, "Unix-domain channels need Java 16+");
        Path dir = tempDir();
        Path stale = dir.resolve(".s.PDB." + server.port);
        bindUnix(stale).close(); // closing does not unlink: the file stays, refusing
        cleanup.add(stale);
        assertTrue(Files.exists(stale));
        try (Connection c = DriverManager.getConnection("jdbc:particledb://localhost:"
                + server.port + "/default?unixSocketDir=" + dir)) {
            assertFalse(mux(c).isUnixSocket());
            assertEquals(1, selectOne(c));
        }
    }

    @Test
    void explicitSocketPathNeverFallsBack() {
        Path missing = Paths.get("/tmp", "pdb-no-such-socket-" + System.nanoTime());
        SQLException e = assertThrows(SQLException.class, () -> DriverManager.getConnection(
                "jdbc:particledb://localhost:" + server.port + "/default?unixSocketPath=" + missing)
                .close());
        assertEquals("08001", e.getSQLState());
    }

    /** Permission denied is not "nobody there": reported, TCP not tried. */
    @Test
    void permissionDeniedIsReportedNotRetriedOnTcp() throws Exception {
        Assumptions.assumeTrue(JAVA16, "Unix-domain channels need Java 16+");
        Assumptions.assumeFalse("root".equals(System.getProperty("user.name")), "root bypasses modes");
        Path dir = tempDir();
        Path sock = dir.resolve(".s.PDB." + server.port);
        try (ServerSocketChannel listener = bindUnix(sock)) {
            cleanup.add(sock);
            Files.setPosixFilePermissions(sock, PosixFilePermissions.fromString("---------"));
            SQLException e = assertThrows(SQLException.class, () -> DriverManager.getConnection(
                    "jdbc:particledb://localhost:" + server.port + "/default?unixSocketDir=" + dir)
                    .close());
            assertEquals("08001", e.getSQLState());
            assertTrue(e.getMessage().contains("Not falling back"), e.getMessage());
        }
    }

    /**
     * A failure AFTER the socket connected (here: the peer hangs up before
     * HELLO_OK -- what the server's same-UID gate does) fails the connect.
     * TCP on the same port would have worked; it must not be tried.
     */
    @Test
    void handshakeFailureOverTheSocketIsNotRetriedOnTcp() throws Exception {
        Assumptions.assumeTrue(JAVA16, "Unix-domain channels need Java 16+");
        Path dir = tempDir();
        Path sock = dir.resolve(".s.PDB." + server.port);
        try (ServerSocketChannel listener = bindUnix(sock)) {
            cleanup.add(sock);
            Thread hangUp = new Thread(() -> {
                try (SocketChannel peer = listener.accept()) {
                    // accept and close: no HELLO_OK
                } catch (IOException ignored) {
                }
            });
            hangUp.setDaemon(true);
            hangUp.start();
            SQLException e = assertThrows(SQLException.class, () -> DriverManager.getConnection(
                    "jdbc:particledb://localhost:" + server.port
                    + "/default?connectTimeout=3000&unixSocketDir=" + dir).close());
            assertEquals("08001", e.getSQLState());
            hangUp.join(5000);
        }
    }

    // ── Behaviour on the socket ───────────────────────────────────────────

    /** Timed reads (statement timeout) work without SO_TIMEOUT. */
    @Test
    void queryTimeoutFiresOverTheSocket() throws Exception {
        Assumptions.assumeTrue(JAVA16, "Unix-domain channels need Java 16+");
        try (Connection c = DriverManager.getConnection(
                "jdbc:particledb://localhost:" + server.port + "/default")) {
            assertTrue(mux(c).isUnixSocket());
            long t0 = System.nanoTime();
            try (Statement st = c.createStatement()) {
                st.setQueryTimeout(1);
                assertThrows(SQLTimeoutException.class, () -> st.executeQuery("SELECT pg_sleep(10)"));
            }
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertTrue(ms >= 900 && ms < 5000, "timeout took " + ms + " ms");
        }
    }

    /**
     * The driver's contract (the read loop in {@code StreamMux.drive}) is to
     * abandon a statement whose caller is interrupted -- on both transports --
     * and keep the connection: the next statement works. Over the socket the
     * request is WRITTEN by the interrupted thread to an
     * {@code InterruptibleChannel}; this pins that the write neither closes
     * the channel nor fails (it is non-blocking, and interrupt-closing applies
     * to blocking I/O only). Same assertions on TCP, as the reference.
     */
    @Test
    void interruptedCallerDoesNotKillTheConnection() throws Exception {
        Assumptions.assumeTrue(JAVA16, "Unix-domain channels need Java 16+");
        for (boolean unix : new boolean[] { true, false }) {
            try (Connection c = DriverManager.getConnection("jdbc:particledb://localhost:"
                    + server.port + "/default?preferUnixSocket=" + unix)) {
                assertEquals(unix, mux(c).isUnixSocket());
                Thread.currentThread().interrupt();
                try {
                    assertThrows(SQLException.class, () -> selectOne(c),
                            "an interrupted caller's statement is abandoned");
                } finally {
                    Thread.interrupted();
                }
                for (int i = 0; i < 3; i++) assertEquals(1, selectOne(c), "unix=" + unix);
                assertFalse(c.isClosed());
                assertFalse(mux(c).isClosed(), "unix=" + unix + ": the interrupt closed the transport");
            }
        }
    }

    @Test
    void concurrentConnectionsOverTheSocket() throws Exception {
        Assumptions.assumeTrue(JAVA16, "Unix-domain channels need Java 16+");
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<Integer>> fs = new ArrayList<>();
            for (int t = 0; t < 8; t++) {
                fs.add(pool.submit(() -> {
                    int sum = 0;
                    try (Connection c = DriverManager.getConnection(
                            "jdbc:particledb://localhost:" + server.port + "/default")) {
                        assertTrue(mux(c).isUnixSocket());
                        for (int i = 0; i < 500; i++) sum += selectOne(c);
                    }
                    return sum;
                }));
            }
            for (Future<Integer> f : fs) assertEquals(500, f.get().intValue());
        } finally {
            pool.shutdownNow();
        }
    }

    /** Below Java 16 the default quietly uses TCP; an explicit path is refused. */
    @Test
    void preJava16UsesTcpAndRefusesAnExplicitPath() throws Exception {
        Assumptions.assumeFalse(JAVA16, "only meaningful on Java < 16");
        try (Connection c = DriverManager.getConnection(
                "jdbc:particledb://localhost:" + server.port + "/default")) {
            assertFalse(mux(c).isUnixSocket());
            assertEquals(1, selectOne(c));
        }
        SQLException e = assertThrows(SQLException.class, () -> DriverManager.getConnection(
                "jdbc:particledb://localhost:" + server.port + "/default?unixSocketPath="
                + serverSocket).close());
        assertTrue(e.getMessage().contains("Java 16"), e.getMessage());
    }

    // ── helpers ───────────────────────────────────────────────────────────

    /** Bind (and listen on) a Unix socket at {@code path}; Java 16 API by reflection (release 11). */
    private static ServerSocketChannel bindUnix(Path path) throws Exception {
        Method of = Class.forName("java.net.UnixDomainSocketAddress").getMethod("of", Path.class);
        Method open = ServerSocketChannel.class.getMethod("open", ProtocolFamily.class);
        ServerSocketChannel ch = (ServerSocketChannel) open.invoke(null,
                StandardProtocolFamily.valueOf("UNIX"));
        ch.bind((SocketAddress) of.invoke(null, path));
        return ch;
    }
}
