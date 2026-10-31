package org.particledb.ds;

import org.particledb.Driver;
import org.particledb.PdbConnection;
import org.particledb.Pgcompat;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * {@link javax.sql.DataSource} implementation. Compatible with HikariCP /
 * Tomcat-jdbc / DBCP2 configuration patterns.
 *
 * <p>Setter pattern matches pgjdbc - frameworks that introspect bean
 * properties (Spring, HikariCP) can configure via reflection.
 */
public final class PdbDataSource implements DataSource {

    private String host = "localhost";
    private int port = org.particledb.Driver.DEFAULT_PORT;
    private String database = "default";
    private String user;
    private String password;
    private int prepareThreshold = 5;
    private int preparedStatementCacheQueries = 256;
    private int preparedStatementCacheSizeMiB = 5;
    private int maxStreamsPerConn = 64;
    private String sslMode = "disable";
    private String sslRootCert; // PEM CA bundle path for verify-ca / verify-full
    private String sslCert;     // PEM client cert path (mTLS)
    private String sslKey;      // PEM client private key path (mTLS)
    private boolean preferUnixSocket = true;
    private String unixSocketDir;  // null = the server default, /tmp
    private String unixSocketPath; // explicit socket; never falls back to TCP
    // Canonical default per the "URL options" table in README.md
    private int connectTimeout = 30_000;
    private boolean raiseExceptionOnSilentRollback = false;
    private boolean allowDdlOutsideTransaction = false;
    private String resultDecoder = "auto"; // auto | arrow | builtin, validated at connect

    private PrintWriter logWriter;
    private int loginTimeout = 0;

    public void setHost(String host) { this.host = host; }
    public String getHost() { return host; }
    public void setPort(int port) { this.port = port; }
    public int getPort() { return port; }
    public void setDatabase(String d) { this.database = d; }
    public String getDatabase() { return database; }
    public void setUser(String u) { this.user = u; }
    public String getUser() { return user; }
    public void setPassword(String p) { this.password = p; }
    public String getPassword() { return password; }
    public void setPrepareThreshold(int n) { this.prepareThreshold = n; }
    public int getPrepareThreshold() { return prepareThreshold; }
    public void setPreparedStatementCacheQueries(int n) { this.preparedStatementCacheQueries = n; }
    public int getPreparedStatementCacheQueries() { return preparedStatementCacheQueries; }
    public void setPreparedStatementCacheSizeMiB(int n) { this.preparedStatementCacheSizeMiB = n; }
    public int getPreparedStatementCacheSizeMiB() { return preparedStatementCacheSizeMiB; }
    public void setMaxStreamsPerConn(int n) { this.maxStreamsPerConn = n; }
    public int getMaxStreamsPerConn() { return maxStreamsPerConn; }
    public void setSslMode(String s) { this.sslMode = s; }
    public String getSslMode() { return sslMode; }
    public void setSslRootCert(String path) { this.sslRootCert = path; }
    public String getSslRootCert() { return sslRootCert; }
    public void setSslCert(String path) { this.sslCert = path; }
    public String getSslCert() { return sslCert; }
    public void setSslKey(String path) { this.sslKey = path; }
    public String getSslKey() { return sslKey; }
    public void setPreferUnixSocket(boolean b) { this.preferUnixSocket = b; }
    public boolean isPreferUnixSocket() { return preferUnixSocket; }
    public void setUnixSocketDir(String dir) { this.unixSocketDir = dir; }
    /** {@code auto} (default), {@code arrow} or {@code builtin}; see the driver README. */
    public void setResultDecoder(String resultDecoder) { this.resultDecoder = resultDecoder; }
    public String getResultDecoder() { return resultDecoder; }
    public String getUnixSocketDir() { return unixSocketDir; }
    public void setUnixSocketPath(String path) { this.unixSocketPath = path; }
    public String getUnixSocketPath() { return unixSocketPath; }
    public void setRaiseExceptionOnSilentRollback(boolean b) { this.raiseExceptionOnSilentRollback = b; }
    public boolean isRaiseExceptionOnSilentRollback() { return raiseExceptionOnSilentRollback; }
    public void setAllowDdlOutsideTransaction(boolean b) { this.allowDdlOutsideTransaction = b; }
    public boolean isAllowDdlOutsideTransaction() { return allowDdlOutsideTransaction; }
    public void setConnectTimeout(int ms) { this.connectTimeout = ms; }
    public int getConnectTimeout() { return connectTimeout; }

    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(user, password);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        Driver.register();
        Map<String, String> p = new LinkedHashMap<>();
        if (username != null) p.put("user", username);
        if (password != null) p.put("password", password);
        p.put("prepareThreshold", String.valueOf(prepareThreshold));
        p.put("preparedStatementCacheQueries", String.valueOf(preparedStatementCacheQueries));
        p.put("preparedStatementCacheSizeMiB", String.valueOf(preparedStatementCacheSizeMiB));
        p.put("maxStreamsPerConn", String.valueOf(maxStreamsPerConn));
        p.put("sslMode", sslMode);
        if (sslRootCert != null) p.put("sslRootCert", sslRootCert);
        if (sslCert != null)     p.put("sslCert", sslCert);
        if (sslKey != null)      p.put("sslKey", sslKey);
        p.put("preferUnixSocket", String.valueOf(preferUnixSocket));
        if (unixSocketDir != null)  p.put("unixSocketDir", unixSocketDir);
        if (unixSocketPath != null) p.put("unixSocketPath", unixSocketPath);
        p.put("connectTimeout", String.valueOf(connectTimeout));
        p.put("loginTimeout", String.valueOf(loginTimeout));
        p.put("raiseExceptionOnSilentRollback", String.valueOf(raiseExceptionOnSilentRollback));
        p.put("allowDdlOutsideTransaction", String.valueOf(allowDdlOutsideTransaction));
        if (resultDecoder != null) p.put("resultDecoder", resultDecoder);
        Pgcompat.ParsedUrl parsed = new Pgcompat.ParsedUrl(host, port, database, p);
        return new PdbConnection(parsed);
    }

    @Override public PrintWriter getLogWriter() { return logWriter; }
    @Override public void setLogWriter(PrintWriter out) { this.logWriter = out; }
    @Override public void setLoginTimeout(int seconds) throws SQLException {
        if (seconds < 0) throw new SQLException("loginTimeout must be >= 0", "22023");
        this.loginTimeout = seconds;
    }
    @Override public int getLoginTimeout() { return loginTimeout; }
    @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return Logger.getLogger("org.particledb");
    }

    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("not a wrapper for " + iface);
    }
    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }
}
