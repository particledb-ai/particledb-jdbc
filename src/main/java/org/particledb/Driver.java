package org.particledb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * ParticleDB JDBC Driver - speaks PDB-wire v2 (multiplexed) with v1
 * fall-back. Registers itself with {@link DriverManager} on classload.
 *
 * <h2>Connection URL</h2>
 * <pre>
 *   jdbc:particledb://host:port/database?prepareThreshold=5&maxStreamsPerConn=64
 * </pre>
 *
 * <h2>Minimal usage</h2>
 * <pre>{@code
 *   try (Connection c = DriverManager.getConnection(
 *           "jdbc:particledb://localhost:7173/default")) {
 *       try (PreparedStatement ps =
 *               c.prepareStatement("INSERT INTO t VALUES (?,?)")) {
 *           ps.setInt(1, 1);
 *           ps.setString(2, "alice");
 *           ps.executeUpdate();
 *       }
 *   }
 * }</pre>
 */
public final class Driver implements java.sql.Driver {

    /**
     * The native wire's default port: what a URL that names no port connects
     * to. The ONE place the driver spells it (the server's own default is
     * {@code particledb start --pdb-port}; the conformance suite's DEF-005 asserts
     * the two agree).
     */
    public static final int DEFAULT_PORT = 7173;


    public static final int MAJOR_VERSION = 1;
    public static final int MINOR_VERSION = 0;

    private static final Driver REGISTERED;
    private static final Logger LOGGER = Logger.getLogger("org.particledb.jdbc");

    static {
        Driver d = null;
        try {
            d = new Driver();
            DriverManager.registerDriver(d);
        } catch (SQLException e) {
            LOGGER.severe("Failed to register ParticleDB JDBC driver: " + e.getMessage());
        }
        REGISTERED = d;
    }

    /**
     * Programmatic registration (callers using {@code Class.forName}
     * implicitly trigger this static initializer).
     */
    public static synchronized void register() throws SQLException {
        if (REGISTERED == null) {
            DriverManager.registerDriver(new Driver());
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null; // contract: return null if not ours
        Pgcompat.ParsedUrl parsed = Pgcompat.parse(url, info);
        return new PdbConnection(parsed);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.toLowerCase().startsWith(Pgcompat.URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        DriverPropertyInfo[] props = new DriverPropertyInfo[18];
        props[0] = mk("user", "",
                "Login role, sent as the wire credential frame at connect time");
        props[1] = mk("password", "",
                "Password for \"user\"; optional for a password-less role");
        props[2] = mk("prepareThreshold", "5",
                "Executions before promotion to a server-side prepared statement");
        props[3] = mk("preparedStatementCacheQueries", "256",
                "Client-side prepared statement cache size (entries)");
        props[4] = mk("preparedStatementCacheSizeMiB", "5",
                "Client-side prepared statement cache cap (MiB)");
        props[5] = mk("maxStreamsPerConn", "64",
                "PDB-wire v2 multiplex stream cap per TCP connection");
        props[6] = mk("sslMode", "disable",
                "TLS mode: disable | require | verify-ca | verify-full");
        props[7] = mk("sslRootCert", "",
                "Path to PEM CA bundle for verify-ca / verify-full");
        props[8] = mk("sslCert", "",
                "Path to PEM client certificate for mTLS");
        props[9] = mk("sslKey", "",
                "Path to PEM client private key for mTLS");
        props[10] = mk("preferUnixSocket", "true",
                "Connect over the server's Unix socket <unixSocketDir>/.s.PDB.<port> when the "
                + "host is localhost / 127.0.0.1 / ::1; TCP only when no server is behind that "
                + "path (Java 16+)");
        // Advertised so the opt-out is DISCOVERABLE. An undocumented escape
        // hatch from a fail-closed default is how a silent downgrade comes
        // back: someone hits the error, greps the driver, and finds a flag
        // nobody wrote down.
        props[12] = mk("requireMux", "true",
                "Require the v2 multiplexed native protocol; false accepts an "
                + "UNMULTIPLEXED v1 connection to an older server");
        props[11] = mk("connectTimeout", "30000",
                "TCP + handshake timeout, milliseconds");
        props[13] = mk("raiseExceptionOnSilentRollback", "false",
                "commit() of a transaction the server rolled back (a statement in it "
                + "failed) raises SQLSTATE 25P02 instead of returning normally");
        props[14] = mk("allowDdlOutsideTransaction", "false",
                "With autoCommit=false, a DDL issued while no transaction is open runs on "
                + "its own, OUTSIDE any transaction, with a SQLWarning (a later rollback() "
                + "cannot undo it) instead of being refused 25001. For migration tools that "
                + "run DDL-only migrations in a transaction (Flyway). A DDL after a DML in "
                + "the same transaction, a batch, and a multi-statement string are still "
                + "refused 25001. The warning is on Connection.getWarnings()");
        props[15] = mk("unixSocketDir", "/tmp",
                "Directory holding the server's .s.PDB.<port> (the server's --unix-socket-dir)");
        props[16] = mk("unixSocketPath", "",
                "Explicit Unix socket path; used whatever the host, and never falls back to TCP");
        props[17] = mk(ArrowDecoder.ResultDecoder.PROPERTY, "auto",
                "Result decoding: auto (default) or builtin (the driver's own reader, every column "
                + "type, no JVM option), or arrow (Apache Arrow Java, which the application adds; "
                + "needs " + ArrowDecoder.ADD_OPENS_FLAG + " on JDK 16+)");
        props[17].choices = new String[] {"auto", "arrow", "builtin"};
        return props;
    }

    private static DriverPropertyInfo mk(String name, String def, String desc) {
        DriverPropertyInfo p = new DriverPropertyInfo(name, def);
        p.description = desc;
        p.required = false;
        return p;
    }

    @Override
    public int getMajorVersion() {
        return MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        // We are not JDBC-compliant in the strict sense (no full SQL92
        // catalog/schema support); pgjdbc returns false here too.
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return LOGGER;
    }
}
