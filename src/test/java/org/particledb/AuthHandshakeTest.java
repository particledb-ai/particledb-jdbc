package org.particledb;

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;
import org.particledb.wire.WireV2Codec;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Credential-frame encoding, credential extraction and the capability-bit
 * decision - all verified against the SERVER's byte layout, without a server.
 *
 * <p>The oracle is the Rust decoder, not the Java encoder. {@link #parseAuthPayload}
 * below is a line-by-line port of
 * the server's native-wire codec ({@code parse_auth_payload}) - including its
 * rejections (short payload, over-long field, empty username) - so a layout
 * drift in the encoder fails here rather than at a live login.
 */
public class AuthHandshakeTest {

    // ── Port of the server's native-wire codec (parse_auth_payload) ────────────
    //
    //   if payload.len() < 4 { return None }
    //   ulen = u32_le(payload[0..4])
    //   if ulen > MAX_AUTH_FIELD_LEN || payload.len() < 4 + ulen + 4 { return None }
    //   plen = u32_le(payload[4+ulen .. 8+ulen])
    //   if plen > MAX_AUTH_FIELD_LEN || payload.len() < 8 + ulen + plen { return None }
    //   user = utf8(payload[4 .. 4+ulen]); pass = utf8(payload[8+ulen .. 8+ulen+plen])
    //   if user.is_empty() { return None }
    //
    /** @return {user, password}, or null where the server would return None. */
    private static String[] parseAuthPayload(byte[] payload) {
        if (payload.length < 4) return null;
        long ulen = readU32LE(payload, 0);
        if (ulen > WireV2Codec.MAX_AUTH_FIELD_LEN || payload.length < 4 + ulen + 4) return null;
        int plenOff = (int) (4 + ulen);
        long plen = readU32LE(payload, plenOff);
        if (plen > WireV2Codec.MAX_AUTH_FIELD_LEN || payload.length < 8 + ulen + plen) return null;
        String user = new String(payload, 4, (int) ulen, StandardCharsets.UTF_8);
        String pass = new String(payload, plenOff + 4, (int) plen, StandardCharsets.UTF_8);
        if (user.isEmpty()) return null;
        return new String[]{user, pass};
    }

    private static long readU32LE(byte[] b, int off) {
        return (b[off] & 0xFFL)
             | ((b[off + 1] & 0xFFL) << 8)
             | ((b[off + 2] & 0xFFL) << 16)
             | ((b[off + 3] & 0xFFL) << 24);
    }

    private static AuthHandshake fromUrl(String url) throws SQLException {
        return AuthHandshake.fromParams(Pgcompat.parse(url, null).params);
    }

    private static AuthHandshake fromUrl(String url, Properties p) throws SQLException {
        return AuthHandshake.fromParams(Pgcompat.parse(url, p).params);
    }

    // ── Credential-frame byte layout ──────────────────────────────────────

    @Test
    void credentialPayload_matchesRustByteLayout() {
        byte[] got = WireV2Codec.encodeAuthPayload("alice", "s3cret");

        // Hand-built to the spec: [u32_le userlen][user][u32_le passlen][pass]
        byte[] want = new byte[]{
                5, 0, 0, 0, 'a', 'l', 'i', 'c', 'e',
                6, 0, 0, 0, 's', '3', 'c', 'r', 'e', 't',
        };
        assertArrayEquals(want, got, "credential payload must be the exact server layout");

        String[] rt = parseAuthPayload(got);
        assertNotNull(rt, "server decoder must accept the frame the driver builds");
        assertEquals("alice", rt[0]);
        assertEquals("s3cret", rt[1]);
    }

    @Test
    void credentialPayload_lengthsAreUtf8ByteCountsNotCharCounts() {
        // 'é' is 2 UTF-8 bytes, '→' is 3, '𝕡' is 4 (surrogate pair in Java).
        String user = "josé";      // 4 chars, 5 bytes
        String pass = "pa→ss𝕡";    // 6 chars (7 with the surrogate), 11 bytes
        byte[] payload = WireV2Codec.encodeAuthPayload(user, pass);

        assertEquals(user.getBytes(StandardCharsets.UTF_8).length, readU32LE(payload, 0));
        assertEquals(pass.getBytes(StandardCharsets.UTF_8).length,
                readU32LE(payload, 4 + user.getBytes(StandardCharsets.UTF_8).length));

        String[] rt = parseAuthPayload(payload);
        assertNotNull(rt, "UTF-8 credentials must survive the server decoder");
        assertEquals(user, rt[0]);
        assertEquals(pass, rt[1]);
    }

    @Test
    void credentialPayload_specialCharactersAreNotEscapedOrMangled() {
        String pass = "p@ss&w=rd?#/\\\"'%20 \t";
        byte[] payload = WireV2Codec.encodeAuthPayload("op", pass);
        String[] rt = parseAuthPayload(payload);
        assertNotNull(rt);
        assertEquals(pass, rt[1], "the credential frame is binary — no escaping of any kind");
    }

    @Test
    void credentialPayload_emptyPasswordIsAZeroLengthField() {
        byte[] payload = WireV2Codec.encodeAuthPayload("alice", "");
        assertEquals(4 + 5 + 4, payload.length);
        assertEquals(0, readU32LE(payload, 9));

        String[] rt = parseAuthPayload(payload);
        assertNotNull(rt, "the server rejects an empty USERNAME, never an empty password");
        assertEquals("alice", rt[0]);
        assertEquals("", rt[1]);
    }

    @Test
    void credentialPayload_nullPasswordFramesAsEmpty() {
        assertArrayEquals(WireV2Codec.encodeAuthPayload("alice", ""),
                WireV2Codec.encodeAuthPayload("alice", null),
                "a missing password must frame exactly like an empty one, not NPE");
    }

    @Test
    void credentialPayload_rejectsEmptyUsernameClientSide() {
        // The server's decoder returns None for an empty username and reports
        // it as a generic malformed-payload auth failure. Reject it here so the
        // caller gets the real reason.
        assertThrows(IllegalArgumentException.class,
                () -> WireV2Codec.encodeAuthPayload("", "pw"));
        assertThrows(IllegalArgumentException.class,
                () -> WireV2Codec.encodeAuthPayload(null, "pw"));
    }

    @Test
    void credentialPayload_enforcesTheServersPerFieldByteCap() {
        String maxUser = repeat('u', WireV2Codec.MAX_AUTH_FIELD_LEN);
        String overUser = repeat('u', WireV2Codec.MAX_AUTH_FIELD_LEN + 1);

        // Exactly at the cap is accepted by the server (`ulen > MAX` is the test).
        assertNotNull(parseAuthPayload(WireV2Codec.encodeAuthPayload(maxUser, "p")));

        assertThrows(IllegalArgumentException.class,
                () -> WireV2Codec.encodeAuthPayload(overUser, "p"));
        assertThrows(IllegalArgumentException.class,
                () -> WireV2Codec.encodeAuthPayload("u", overUser));
    }

    private static String repeat(char c, int n) {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) sb.append(c);
        return sb.toString();
    }

    // ── Frame headers ─────────────────────────────────────────────────────

    @Test
    void authFrame_v2HeaderLayout() {
        ByteBuf buf = WireV2Codec.encodeAuthV2(7L, "alice", "s3cret");
        // [u8 version=2][u8 type][u8 flags][u32_le stream_id][u32_le len]
        assertEquals(WireV2Codec.PROTOCOL_V2, buf.readByte());
        assertEquals(WireV2Codec.MSG_AUTH, buf.readByte());
        assertEquals(WireV2Codec.FLAG_END_STREAM, buf.readByte());
        assertEquals(7L, buf.readUnsignedIntLE());
        int len = buf.readIntLE();
        assertEquals(19, len);
        byte[] payload = new byte[len];
        buf.readBytes(payload);
        assertEquals(0, buf.readableBytes(), "no trailing bytes");
        assertArrayEquals(WireV2Codec.encodeAuthPayload("alice", "s3cret"), payload);
        assertEquals(0x09, WireV2Codec.MSG_AUTH, "F_AUTH opcode");
    }

    @Test
    void authFrame_v1HeaderLayout() {
        ByteBuf buf = WireV2Codec.encodeAuthV1(3, "alice", "s3cret");
        // [u32_le len][u16_le stream_id][u8 type][u8 flags]
        int len = buf.readIntLE();
        assertEquals(19, len);
        assertEquals(3, buf.readUnsignedShortLE());
        assertEquals(WireV2Codec.MSG_AUTH, buf.readByte());
        assertEquals(0, buf.readByte());
        byte[] payload = new byte[len];
        buf.readBytes(payload);
        assertEquals(0, buf.readableBytes(), "no trailing bytes");
        assertArrayEquals(WireV2Codec.encodeAuthPayload("alice", "s3cret"), payload);
    }

    // ── Credential extraction: URL and Properties ─────────────────────────

    @Test
    void credentials_readFromUrlQueryString() throws Exception {
        AuthHandshake a = fromUrl("jdbc:particledb://h:7173/db?user=alice&password=s3cret");
        assertTrue(a.hasCredentials());
        assertEquals("alice", a.user);
        assertEquals("s3cret", a.password);
    }

    @Test
    void credentials_readFromProperties() throws Exception {
        Properties p = new Properties();
        p.setProperty("user", "bob");
        p.setProperty("password", "hunter2");
        AuthHandshake a = fromUrl("jdbc:particledb://h:7173/db", p);
        assertTrue(a.hasCredentials());
        assertEquals("bob", a.user);
        assertEquals("hunter2", a.password);
    }

    @Test
    void credentials_urlWinsOverProperties() throws Exception {
        Properties p = new Properties();
        p.setProperty("user", "from-props");
        p.setProperty("password", "from-props-pw");
        AuthHandshake a = fromUrl(
                "jdbc:particledb://h:7173/db?user=from-url&password=from-url-pw", p);
        assertEquals("from-url", a.user);
        assertEquals("from-url-pw", a.password);
    }

    @Test
    void credentials_propertiesFillGapsLeftByTheUrl() throws Exception {
        Properties p = new Properties();
        p.setProperty("password", "from-props-pw");
        AuthHandshake a = fromUrl("jdbc:particledb://h:7173/db?user=from-url", p);
        assertEquals("from-url", a.user);
        assertEquals("from-props-pw", a.password);
    }

    @Test
    void credentials_missingPasswordIsStillCredentials() throws Exception {
        AuthHandshake a = fromUrl("jdbc:particledb://h:7173/db?user=alice");
        assertTrue(a.hasCredentials(),
                "a password-less role must still send a credential frame");
        assertEquals(null, a.password);
        // …and it frames as a zero-length password field.
        assertNotNull(parseAuthPayload(a.credentialPayload()));
        assertEquals("", parseAuthPayload(a.credentialPayload())[1]);
    }

    @Test
    void credentials_emptyPasswordIsStillCredentials() throws Exception {
        AuthHandshake a = fromUrl("jdbc:particledb://h:7173/db?user=alice&password=");
        assertTrue(a.hasCredentials());
        assertEquals("", a.password);
        assertEquals("", parseAuthPayload(a.credentialPayload())[1]);
    }

    @Test
    void credentials_absentUserIsNoCredentials() throws Exception {
        assertFalse(fromUrl("jdbc:particledb://h:7173/db").hasCredentials());
        assertFalse(fromUrl("jdbc:particledb://h:7173/db?password=lonely").hasCredentials());
    }

    @Test
    void credentials_blankUserIsNoCredentials() throws Exception {
        // `?user=` parses to "" - the server rejects an empty username, so the
        // driver must treat it as "no credentials" rather than send a frame it
        // knows will be refused as malformed.
        AuthHandshake a = fromUrl("jdbc:particledb://h:7173/db?user=&password=pw");
        assertEquals("", a.user);
        assertFalse(a.hasCredentials());
    }

    @Test
    void credentials_urlValuesAreVerbatim() throws Exception {
        // Query values are NOT percent-decoded, so what is in the URL is what
        // goes on the wire. Anything containing & or = needs Properties.
        AuthHandshake a = fromUrl("jdbc:particledb://h:7173/db?user=al%69ce&password=p%40ss");
        assertEquals("al%69ce", a.user);
        assertEquals("p%40ss", a.password);
        assertEquals("p%40ss", parseAuthPayload(a.credentialPayload())[1]);
    }

    @Test
    void credentials_propertiesCarryUrlHostileCharacters() throws Exception {
        Properties p = new Properties();
        p.setProperty("user", "svc-acct");
        p.setProperty("password", "p@ss&w=rd?/ ünïcode");
        AuthHandshake a = fromUrl("jdbc:particledb://h:7173/db", p);
        String[] rt = parseAuthPayload(a.credentialPayload());
        assertNotNull(rt);
        assertEquals("svc-acct", rt[0]);
        assertEquals("p@ss&w=rd?/ ünïcode", rt[1]);
    }

    // ── Capability-bit decision ───────────────────────────────────────────

    @Test
    void capabilityBit_isolatedFromEveryOtherCap() {
        assertEquals(0x20, WireV2Codec.CAP_AUTH_REQUIRED);

        assertFalse(AuthHandshake.serverRequiresAuth((byte) 0x00));
        assertFalse(AuthHandshake.serverRequiresAuth(WireV2Codec.CAP_V2_MUX));
        assertFalse(AuthHandshake.serverRequiresAuth(
                (byte) (WireV2Codec.CAP_V1_LEGACY | WireV2Codec.CAP_V2_MUX
                        | WireV2Codec.CAP_COMPRESSION | WireV2Codec.CAP_TLS_NEXT_PROTO
                        | WireV2Codec.CAP_NUMERIC_TEXT)));

        assertTrue(AuthHandshake.serverRequiresAuth(WireV2Codec.CAP_AUTH_REQUIRED));
        assertTrue(AuthHandshake.serverRequiresAuth(
                (byte) (WireV2Codec.CAP_V2_MUX | WireV2Codec.CAP_AUTH_REQUIRED)));
        assertTrue(AuthHandshake.serverRequiresAuth((byte) 0xFF));
    }

    @Test
    void capabilityBit_readFromAParsedHelloOk() {
        // [ver][caps][fmt][u32 max_streams][u32 init_window] - the 11-byte
        // HELLO_OK an auth-required v2 server sends.
        byte[] helloOk = new byte[]{
                2, (byte) (WireV2Codec.CAP_V2_MUX | WireV2Codec.CAP_AUTH_REQUIRED), 1,
                100, 0, 0, 0,
                0, 0, 1, 0,
        };
        WireV2Codec.HelloOk h = WireV2Codec.parseHelloOk(helloOk);
        assertTrue(h.isV2());
        assertTrue(h.isAuthRequired());
        assertEquals(100, h.maxConcurrentStreams);

        byte[] noAuth = helloOk.clone();
        noAuth[1] = WireV2Codec.CAP_V2_MUX;
        assertFalse(WireV2Codec.parseHelloOk(noAuth).isAuthRequired());
    }

    @Test
    void capabilityBit_legacyTwoByteHelloOkCarriesNoChallenge() {
        // [u8 version][u8 chosen_format] - no caps byte at all.
        WireV2Codec.HelloOk h = WireV2Codec.parseHelloOk(new byte[]{1, WireV2Codec.FMT_ARROW_IPC});
        assertEquals(0, h.chosenCaps);
        assertFalse(h.isAuthRequired(),
                "a legacy HELLO_OK cannot carry the challenge — refusal comes frame by frame");
    }

    // ── The connect-time decision ─────────────────────────────────────────

    @Test
    void decision_serverRequiresAuthAndNoCredentials_failsAtConnect() throws Exception {
        AuthHandshake a = fromUrl("jdbc:particledb://h:7173/db");
        SQLException e = assertThrows(SQLException.class,
                () -> a.requireCredentials(WireV2Codec.CAP_AUTH_REQUIRED));
        assertEquals("28000", e.getSQLState());
        assertTrue(e.getMessage().contains("server requires authentication"), e.getMessage());
        assertTrue(e.getMessage().contains("user"), e.getMessage());
    }

    @Test
    void decision_serverRequiresAuthAndCredentialsPresent_sends() throws Exception {
        AuthHandshake a = fromUrl("jdbc:particledb://h:7173/db?user=alice&password=s3cret");
        assertTrue(a.requireCredentials(
                (byte) (WireV2Codec.CAP_V2_MUX | WireV2Codec.CAP_AUTH_REQUIRED)));
    }

    @Test
    void decision_noAuthServer_sendsNothingEvenWhenCredentialsAreConfigured()
            throws Exception {
        // The --no-auth guarantee: the handshake is byte-identical whether or
        // not the caller configured credentials.
        AuthHandshake withCreds =
                fromUrl("jdbc:particledb://h:7173/db?user=alice&password=s3cret");
        AuthHandshake without = fromUrl("jdbc:particledb://h:7173/db");

        assertFalse(withCreds.requireCredentials((byte) 0x00));
        assertFalse(without.requireCredentials((byte) 0x00));
        assertFalse(withCreds.requireCredentials(WireV2Codec.CAP_V2_MUX));
        assertFalse(without.requireCredentials(WireV2Codec.CAP_V2_MUX));
    }

    @Test
    void sslModesAreParsedAndUnknownModesAreRefused() throws Exception {
        assertEquals(AuthHandshake.SslMode.DISABLE,
                fromUrl("jdbc:particledb://h:7173/db").sslMode);
        assertEquals(AuthHandshake.SslMode.REQUIRE,
                fromUrl("jdbc:particledb://h:7173/db?sslMode=require").sslMode);
        assertThrows(SQLException.class, () -> fromUrl("jdbc:particledb://h:7173/db?sslMode=bogus"));
    }
}
