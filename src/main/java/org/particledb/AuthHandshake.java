package org.particledb;

import org.particledb.wire.WireV2Codec;

import java.sql.SQLException;
import java.util.Map;

/**
 * Connect-time credentials and TLS options for one connection.
 *
 * <p>Holds the {@code user} / {@code password} read from the JDBC URL and
 * {@link java.util.Properties}, encodes the native wire's credential frame,
 * and decides - from the server's {@code HELLO_OK} capability byte - whether
 * that frame has to be sent.
 *
 * <h2>Why the capability bit drives the decision</h2>
 *
 * The native wire is fail-closed: a server started WITHOUT {@code --no-auth}
 * refuses every frame but the credential frame itself, and advertises that in
 * {@code HELLO_OK} via {@link WireV2Codec#CAP_AUTH_REQUIRED}. The server
 * computes that bit from the same predicate that arms the refusal gate, so it
 * is an exact oracle: bit set ⟺ credentials required.
 *
 * <p>The driver therefore sends the credential frame when - and only when  -
 * the server asked for it. A {@code --no-auth} server never sets the bit and
 * never receives a credential frame, so its handshake is byte-identical to a
 * driver build with no authentication support at all. That matters beyond
 * saving a round trip: a credential frame accepted by a {@code --no-auth}
 * server installs a session user server-side, which would silently change
 * {@code current_user} for every deployment that runs open and passes a
 * username it never expected to be honoured.
 *
 * <h2>Passing credentials</h2>
 *
 * <pre>{@code
 *   jdbc:particledb://host:7173/db?user=alice&password=s3cret
 * }</pre>
 *
 * or, which is what a password containing {@code &}, {@code =} or {@code ?}
 * needs since URL query values are taken verbatim:
 *
 * <pre>{@code
 *   Properties p = new Properties();
 *   p.setProperty("user", "alice");
 *   p.setProperty("password", "p@ss&w=rd?");
 *   DriverManager.getConnection("jdbc:particledb://host:7173/db", p);
 * }</pre>
 */
public final class AuthHandshake {

    public enum SslMode { DISABLE, REQUIRE, VERIFY_CA, VERIFY_FULL }

    /** SQLSTATE 28000 - invalid_authorization_specification. */
    static final String SQLSTATE_INVALID_AUTHORIZATION = "28000";
    /** SQLSTATE 28P01 - invalid_password (PostgreSQL's code for a bad login). */
    static final String SQLSTATE_INVALID_PASSWORD = "28P01";

    public final SslMode sslMode;
    public final String user;
    public final String password;
    public final String applicationName;

    private AuthHandshake(SslMode m, String u, String p, String app) {
        this.sslMode = m; this.user = u; this.password = p; this.applicationName = app;
    }

    public static AuthHandshake fromParams(Map<String, String> params) throws SQLException {
        String mode = Pgcompat.strParam(params, "sslMode", "disable").toLowerCase();
        SslMode m;
        switch (mode) {
            case "disable":     m = SslMode.DISABLE; break;
            case "require":     m = SslMode.REQUIRE; break;
            case "verify-ca":   m = SslMode.VERIFY_CA; break;
            case "verify-full": m = SslMode.VERIFY_FULL; break;
            default:
                throw new SQLException("unknown sslMode: " + mode);
        }
        String user = Pgcompat.strParam(params, "user", null);
        String password = Pgcompat.strParam(params, "password", null);
        String app = Pgcompat.strParam(params, "applicationName", "particledb-jdbc");
        return new AuthHandshake(m, user, password, app);
    }

    /**
     * True when a username was supplied. The server's decoder rejects an empty
     * username outright, so {@code ?user=} (present but blank) counts as no
     * credentials - otherwise the driver would send a frame guaranteed to be
     * refused as malformed and report that as a password failure.
     *
     * <p>A missing or empty PASSWORD is fine: it frames as a zero-length field
     * and a password-less role authenticates with it.
     */
    public boolean hasCredentials() {
        return user != null && !user.isEmpty();
    }

    /**
     * Does this server demand a credential frame? Reads
     * {@link WireV2Codec#CAP_AUTH_REQUIRED} out of the {@code chosen_caps}
     * byte of HELLO_OK with a bitwise AND, so unrelated capability bits
     * (v2 mux, numeric-text, TLS ALPN) never affect the answer.
     */
    public static boolean serverRequiresAuth(byte chosenCaps) {
        return (chosenCaps & WireV2Codec.CAP_AUTH_REQUIRED) != 0;
    }

    /**
     * Decide what to do at connect time, given the server's capability byte.
     *
     * @return true if the caller must send the credential frame now, false if
     *         the server asks for no credentials (the {@code --no-auth} path,
     *         which must stay byte-identical).
     * @throws SQLException if the server requires credentials and none were
     *         supplied - reported HERE, at connect, rather than letting the
     *         connection open and die at the first query.
     */
    public boolean requireCredentials(byte chosenCaps) throws SQLException {
        if (!serverRequiresAuth(chosenCaps)) {
            return false;
        }
        if (!hasCredentials()) {
            throw new SQLException(
                    "server requires authentication but no credentials were supplied: "
                    + "set the \"user\" (and \"password\") JDBC property, or add "
                    + "?user=…&password=… to the connection URL",
                    SQLSTATE_INVALID_AUTHORIZATION);
        }
        return true;
    }

    /**
     * The credential-frame payload:
     * {@code [u32_le userlen][user utf8][u32_le passlen][pass utf8]}.
     *
     * @throws IllegalArgumentException if the username is empty or a field
     *         exceeds the server's per-field byte cap
     */
    public byte[] credentialPayload() {
        return WireV2Codec.encodeAuthPayload(user, password);
    }
}
