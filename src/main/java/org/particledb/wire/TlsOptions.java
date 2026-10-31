package org.particledb.wire;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

/**
 * Parsed TLS configuration for the PDB-wire JDBC driver.
 *
 * <p>The driver maps the JDBC URL {@code sslMode} param onto one of four
 * behaviours, modelled on pgjdbc conventions:
 *
 * <table>
 *   <caption>sslMode values</caption>
 *   <tr><th>{@code sslMode}</th><th>cap bit</th><th>verification</th></tr>
 *   <tr><td>{@code disable}</td><td>NOT set</td><td>plaintext, no TLS at all</td></tr>
 *   <tr><td>{@code require}</td><td>set</td><td>accept any cert (trust-all)</td></tr>
 *   <tr><td>{@code verify-ca}</td><td>set</td><td>chain check only, no hostname</td></tr>
 *   <tr><td>{@code verify-full}</td><td>set</td><td>chain + hostname (default JSSE)</td></tr>
 * </table>
 *
 * <p>If the user requested any non-{@code disable} mode but the server does
 * not echo {@code CAP_TLS_NEXT_PROTOCOL} in HELLO_OK, the connect attempt
 * fails with a clear error - there is no silent plaintext fallback.
 *
 * <p>See the TLS section of {@code PROTOCOL.md} and
 * the "URL options" table in {@code README.md}.
 */
public final class TlsOptions {

    public enum Mode {
        /** No TLS - do not set the cap bit, plaintext only. */
        DISABLE,
        /** TLS, no verification. Trust-all + skip hostname. */
        REQUIRE,
        /** TLS, verify CA chain only (no hostname check). */
        VERIFY_CA,
        /** TLS, full verification (chain + hostname). */
        VERIFY_FULL,
    }

    /** Negotiated mode. */
    public final Mode mode;

    /** Path to client cert PEM (for mTLS), or {@code null}. */
    public final String clientCertPath;

    /** Path to client key PEM (for mTLS), or {@code null}. */
    public final String clientKeyPath;

    /** Path to additional CA bundle PEM, or {@code null}. */
    public final String caBundlePath;

    private TlsOptions(Mode mode, String clientCertPath, String clientKeyPath,
                       String caBundlePath) {
        this.mode = mode;
        this.clientCertPath = clientCertPath;
        this.clientKeyPath = clientKeyPath;
        this.caBundlePath = caBundlePath;
    }

    /** TLS-disabled singleton. */
    public static final TlsOptions DISABLED = new TlsOptions(
            Mode.DISABLE, null, null, null);

    public boolean isEnabled() {
        return mode != Mode.DISABLE;
    }

    /** Parse from URL params. */
    public static TlsOptions fromParams(java.util.Map<String, String> params) {
        String raw = params.get("sslMode");
        Mode m = parseMode(raw);
        String cert = emptyToNull(params.get("sslCert"));
        String key  = emptyToNull(params.get("sslKey"));
        String ca   = emptyToNull(params.get("sslRootCert"));
        return new TlsOptions(m, cert, key, ca);
    }

    private static Mode parseMode(String raw) {
        if (raw == null || raw.isEmpty()) return Mode.DISABLE;
        String s = raw.toLowerCase();
        switch (s) {
            case "disable":
            case "disabled":
            case "off":
            case "false":
                return Mode.DISABLE;
            case "require":
            case "required":
            case "on":
            case "true":
                return Mode.REQUIRE;
            case "verify-ca":
            case "verify_ca":
            case "verifyca":
                return Mode.VERIFY_CA;
            case "verify-full":
            case "verify_full":
            case "verifyfull":
                return Mode.VERIFY_FULL;
            default:
                // Unknown → treat as disable so we never silently encrypt without
                // the user asking. (Matches pgjdbc unknown-mode fail-safe.)
                throw new IllegalArgumentException(
                        "unknown sslMode '" + raw + "' "
                        + "(expected: disable | require | verify-ca | verify-full)");
        }
    }

    private static String emptyToNull(String s) {
        return (s == null || s.isEmpty()) ? null : s;
    }

    /**
     * Build a {@link javax.net.ssl.SSLContext}-shape pair of
     * {@link KeyManager}s and {@link TrustManager}s for this config.
     */
    public KeyManager[] buildKeyManagers() throws IOException {
        if (clientCertPath == null && clientKeyPath == null) return null;
        if (clientCertPath == null || clientKeyPath == null) {
            throw new IOException(
                    "sslCert and sslKey must be specified together for mTLS");
        }
        try {
            List<X509Certificate> chain = loadPemCerts(clientCertPath);
            java.security.PrivateKey pk = loadPemPrivateKey(clientKeyPath);

            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null, null);
            ks.setKeyEntry("client", pk, new char[0],
                    chain.toArray(new X509Certificate[0]));
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(
                    KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, new char[0]);
            return kmf.getKeyManagers();
        } catch (Exception e) {
            throw new IOException("failed to load client cert/key: " + e.getMessage(), e);
        }
    }

    /**
     * Build {@link TrustManager}s appropriate for {@link #mode}.
     *
     * <p>For {@link Mode#REQUIRE} we install a trust-all manager.
     * For {@link Mode#VERIFY_CA} we install the platform default chain
     * (optionally extended with the sslRootCert bundle) but skip hostname
     * checks - those are governed at the SSLParameters level.
     * For {@link Mode#VERIFY_FULL} the platform default trust manager
     * is used and the SSLParameters get HTTPS endpoint identification.
     */
    public TrustManager[] buildTrustManagers() throws IOException {
        if (mode == Mode.REQUIRE) {
            return new TrustManager[] { TRUST_ALL };
        }
        try {
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null, null);

            // Seed with the default cacerts so users don't have to re-supply
            // OS trust roots when adding sslRootCert.
            seedWithDefaultTrustStore(ks);

            if (caBundlePath != null) {
                int idx = 0;
                for (X509Certificate c : loadPemCerts(caBundlePath)) {
                    ks.setCertificateEntry("pdb-root-" + (idx++), c);
                }
            }

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
            return tmf.getTrustManagers();
        } catch (Exception e) {
            throw new IOException("failed to build trust managers: " + e.getMessage(), e);
        }
    }

    private static void seedWithDefaultTrustStore(KeyStore ks) {
        try {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
            tmf.init((KeyStore) null);
            int idx = 0;
            for (TrustManager tm : tmf.getTrustManagers()) {
                if (tm instanceof X509TrustManager) {
                    for (X509Certificate c : ((X509TrustManager) tm).getAcceptedIssuers()) {
                        ks.setCertificateEntry("os-root-" + (idx++), c);
                    }
                }
            }
        } catch (Exception ignored) {
            // Best effort - verify-ca with sslRootCert and no OS roots still works.
        }
    }

    // ── PEM parsing helpers ───────────────────────────────────────────────

    static List<X509Certificate> loadPemCerts(String path) throws IOException {
        try (InputStream in = new FileInputStream(path)) {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            Collection<?> certs = cf.generateCertificates(in);
            List<X509Certificate> out = new ArrayList<>();
            for (Object o : certs) {
                if (o instanceof X509Certificate) out.add((X509Certificate) o);
            }
            if (out.isEmpty()) {
                throw new IOException("no certificates found in " + path);
            }
            return out;
        } catch (Exception e) {
            throw new IOException("failed to parse PEM from " + path + ": "
                    + e.getMessage(), e);
        }
    }

    static java.security.PrivateKey loadPemPrivateKey(String path) throws IOException {
        try {
            File f = new File(path);
            byte[] raw = java.nio.file.Files.readAllBytes(f.toPath());
            String pem = new String(raw, java.nio.charset.StandardCharsets.UTF_8);
            String base64 = stripPemMarkers(pem);
            byte[] der = Base64.getMimeDecoder().decode(base64);

            // Try PKCS#8 first, then PKCS#1 RSA fallback.
            try {
                return java.security.KeyFactory.getInstance("RSA").generatePrivate(
                        new java.security.spec.PKCS8EncodedKeySpec(der));
            } catch (Exception e1) {
                try {
                    return java.security.KeyFactory.getInstance("EC").generatePrivate(
                            new java.security.spec.PKCS8EncodedKeySpec(der));
                } catch (Exception e2) {
                    throw new IOException("unsupported private key format in " + path);
                }
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("failed to parse key from " + path + ": "
                    + e.getMessage(), e);
        }
    }

    private static String stripPemMarkers(String pem) {
        return pem
                .replaceAll("-----BEGIN [^-]+-----", "")
                .replaceAll("-----END [^-]+-----", "")
                .replaceAll("\\s+", "");
    }

    // ── trust-all manager for sslMode=require ─────────────────────────────

    private static final X509TrustManager TRUST_ALL = new X509TrustManager() {
        @Override public void checkClientTrusted(X509Certificate[] chain, String authType) {}
        @Override public void checkServerTrusted(X509Certificate[] chain, String authType) {}
        @Override public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
    };
}
