package org.particledb;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * pgjdbc-compatibility helpers.
 *
 * <p>Most JVM apps reach for pgjdbc-style URLs. We intentionally accept the
 * {@code jdbc:particledb://host:port/db?k=v&k2=v2} form, but with a few
 * pgjdbc-equivalent option names so that drop-in replacement is possible.
 *
 * <p>Supported URL options (with pgjdbc-compatible names where applicable):
 * <ul>
 *     <li>{@code prepareThreshold} (int) - number of executions before
 *         server-side prepare. Default 5 (matches pgjdbc).</li>
 *     <li>{@code preparedStatementCacheQueries} (int) - entries in
 *         client-side prepared cache. Default 256 (matches pgjdbc).</li>
 *     <li>{@code preparedStatementCacheSizeMiB} (int) - bytes cap for
 *         client-side prepared cache. Default 5 MiB (matches pgjdbc).</li>
 *     <li>{@code maxStreamsPerConn} (int) - wire-v2 multiplex stream cap
 *         per TCP. Default 64.</li>
 *     <li>{@code sslMode} (string) - {@code disable|require|verify-ca|verify-full}.
 *         Default {@code disable}. Modes match pgjdbc semantics:
 *         <ul>
 *             <li>{@code disable} - plaintext.</li>
 *             <li>{@code require} - encrypt only, no CA / hostname checks.</li>
 *             <li>{@code verify-ca} - encrypt + verify CA chain.</li>
 *             <li>{@code verify-full} - encrypt + verify CA + verify hostname.</li>
 *         </ul></li>
 *     <li>{@code sslRootCert} (path) - PEM CA bundle for
 *         {@code verify-ca}/{@code verify-full}.</li>
 *     <li>{@code sslCert} (path) - PEM client cert for mTLS.</li>
 *     <li>{@code sslKey} (path) - PEM client private key for mTLS.</li>
 *     <li>{@code user} / {@code password} - auth credentials, sent as the
 *         native wire's credential frame at connect time when the server's
 *         HELLO_OK advertises {@code CAP_AUTH_REQUIRED}. A {@code --no-auth}
 *         server never asks, so nothing is sent. Query-string values are taken
 *         VERBATIM (no percent-decoding), so a password containing
 *         {@code &}, {@code =} or {@code ?} must be passed via
 *         {@link Properties} instead of the URL. See
 *         {@link org.particledb.AuthHandshake}.</li>
 *     <li>{@code applicationName} - string passed in HELLO for
 *         server-side observability.</li>
 *     <li>{@code connectTimeout} (millis) - TCP + handshake timeout.
 *         Default 30000 (canonical default per
 *         the "URL options" table in README.md).</li>
 *     <li>{@code socketTimeout} (millis) - per-call read timeout.
 *         Default 30000.</li>
 *     <li>{@code tcpNoDelay} (boolean) - Default true.</li>
 *     <li>{@code preferUnixSocket} (boolean) - connect over the server's
 *         Unix socket {@code <unixSocketDir>/.s.PDB.<port>} when the host is
 *         {@code localhost} / {@code 127.0.0.1} / {@code ::1} (Java 16+);
 *         TCP only when no server is behind that path. Default true.</li>
 *     <li>{@code unixSocketDir} - the server's {@code --unix-socket-dir}.
 *         Default {@code /tmp}.</li>
 *     <li>{@code unixSocketPath} - explicit socket path; used whatever the
 *         host, never falls back to TCP.</li>
 * </ul>
 */
public final class Pgcompat {

    public static final String URL_PREFIX = "jdbc:particledb:";

    private Pgcompat() {}

    /**
     * Parsed JDBC URL.
     */
    public static final class ParsedUrl {
        public final String host;
        public final int port;
        public final String database;
        public final Map<String, String> params;

        public ParsedUrl(String host, int port, String database, Map<String, String> params) {
            this.host = host;
            this.port = port;
            this.database = database;
            this.params = params;
        }
    }

    /**
     * Parse a {@code jdbc:particledb://host:port/db?k=v} URL.
     *
     * <p>Accepts {@code jdbc:particledb:host:port/db}, {@code jdbc:particledb://host:port/db},
     * and the {@code //host:port/db?k=v} variants.
     *
     * @throws SQLException if the URL is malformed
     */
    public static ParsedUrl parse(String url, Properties extra) throws SQLException {
        if (url == null || !url.toLowerCase().startsWith(URL_PREFIX)) {
            throw new SQLException("Not a ParticleDB JDBC URL");
        }
        String rest = url.substring(URL_PREFIX.length());
        if (rest.startsWith("//")) {
            rest = rest.substring(2);
        }
        // host:port/db?k=v&k2=v2
        String authority;
        String dbAndQuery;
        int slash = rest.indexOf('/');
        if (slash < 0) {
            authority = rest;
            dbAndQuery = "";
        } else {
            authority = rest.substring(0, slash);
            dbAndQuery = rest.substring(slash + 1);
        }

        String host;
        int port;
        int colon = authority.lastIndexOf(':');
        if (colon < 0) {
            host = authority.isEmpty() ? "localhost" : authority;
            port = Driver.DEFAULT_PORT;
        } else {
            host = authority.substring(0, colon);
            try {
                port = Integer.parseInt(authority.substring(colon + 1));
            } catch (NumberFormatException nfe) {
                throw new SQLException("Invalid port in ParticleDB JDBC URL");
            }
        }
        if (host.isEmpty()) host = "localhost";

        String database;
        Map<String, String> params = new LinkedHashMap<>();
        int q = dbAndQuery.indexOf('?');
        if (q < 0) {
            database = dbAndQuery;
        } else {
            database = dbAndQuery.substring(0, q);
            String query = dbAndQuery.substring(q + 1);
            for (String kv : query.split("&")) {
                if (kv.isEmpty()) continue;
                int eq = kv.indexOf('=');
                String k = eq < 0 ? kv : kv.substring(0, eq);
                String v = eq < 0 ? "" : kv.substring(eq + 1);
                params.put(k, v);
            }
        }
        if (database.isEmpty()) database = "default";

        if (extra != null) {
            for (String name : extra.stringPropertyNames()) {
                params.putIfAbsent(name, extra.getProperty(name));
            }
        }
        return new ParsedUrl(host, port, database, params);
    }

    /**
     * Strip a SQL escape sequence to driver-native form. Currently a no-op  -
     * provided so that pgjdbc users with {@code escapeSyntaxCallMode=call}
     * style URLs do not break our parser.
     */
    public static String unescape(String sql) {
        return sql;
    }

    public static int intParam(Map<String, String> params, String key, int def) {
        String v = params.get(key);
        if (v == null || v.isEmpty()) return def;
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            return def;
        }
    }

    public static boolean boolParam(Map<String, String> params, String key, boolean def) {
        String v = params.get(key);
        if (v == null || v.isEmpty()) return def;
        return v.equalsIgnoreCase("true") || v.equals("1") || v.equalsIgnoreCase("yes");
    }

    public static String strParam(Map<String, String> params, String key, String def) {
        String v = params.get(key);
        return v == null ? def : v;
    }
}
