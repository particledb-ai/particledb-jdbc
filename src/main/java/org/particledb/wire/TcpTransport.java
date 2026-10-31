package org.particledb.wire;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

/** {@link WireTransport} over a blocking TCP {@link Socket} (TCP_NODELAY, keep-alive). */
final class TcpTransport implements WireTransport {
    private final Socket socket;
    private final Socket rawSocket;
    private final InputStream in;
    private final OutputStream out;
    private final String description;

    private TcpTransport(Socket socket, String description) throws IOException {
        this(socket, socket, description);
    }

    private TcpTransport(Socket socket, Socket rawSocket, String description) throws IOException {
        this.socket = socket;
        this.rawSocket = rawSocket;
        this.in = socket.getInputStream();
        this.out = socket.getOutputStream();
        this.description = description;
    }

    static TcpTransport connect(String host, int port, int connectTimeoutMillis) throws IOException {
        Socket s = new Socket();
        try {
            s.setTcpNoDelay(true);
            s.setKeepAlive(true);
            s.connect(new InetSocketAddress(host, port), connectTimeoutMillis);
            return new TcpTransport(s, "tcp:" + host + ":" + port);
        } catch (IOException | RuntimeException e) {
            try { s.close(); } catch (IOException ignored) { /* best-effort */ }
            throw e;
        }
    }

    @Override public InputStream in() { return in; }
    TcpTransport secure(String host, int port, int timeoutMillis, TlsOptions options) throws IOException {
        try {
            javax.net.ssl.SSLContext context = javax.net.ssl.SSLContext.getInstance("TLS");
            context.init(options.buildKeyManagers(), options.buildTrustManagers(), null);
            javax.net.ssl.SSLSocket tls = (javax.net.ssl.SSLSocket) context.getSocketFactory()
                    .createSocket(socket, host, port, true);
            javax.net.ssl.SSLParameters parameters = tls.getSSLParameters();
            parameters.setProtocols(new String[]{"TLSv1.3", "TLSv1.2"});
            if (options.mode == TlsOptions.Mode.VERIFY_FULL) {
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
            }
            tls.setSSLParameters(parameters);
            tls.setSoTimeout(timeoutMillis);
            tls.startHandshake();
            tls.setSoTimeout(0);
            return new TcpTransport(tls, rawSocket, "tls:" + host + ":" + port);
        } catch (java.security.GeneralSecurityException e) {
            throw new javax.net.ssl.SSLException("cannot initialize TLS", e);
        }
    }

    @Override public OutputStream out() { return out; }
    @Override public void setReadTimeout(int millis) throws IOException { socket.setSoTimeout(millis); }
    @Override public boolean isClosed() { return rawSocket.isClosed() || socket.isClosed(); }
    @Override public boolean isUnixSocket() { return false; }
    @Override public String describe() { return description; }
    @Override public void abort() throws IOException { rawSocket.close(); }
    @Override public void close() throws IOException { socket.close(); }
}
