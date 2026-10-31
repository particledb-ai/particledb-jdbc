package org.particledb.conformance;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A minimal RESP2 client: the conformance suite's independent reader/writer of
 * the key-value store, so a KV case compares what the SDK did against what a
 * Redis client sees on the same server. Replies: byte[] (bulk / simple string),
 * Long (integer), null (nil), List (array); an error reply throws.
 */
public final class Resp implements AutoCloseable {
    private final Socket sock;
    private final InputStream in;
    private final OutputStream out;

    public Resp(String host, int port) throws IOException {
        sock = new Socket(host, port);
        sock.setSoTimeout(15_000);
        in = new BufferedInputStream(sock.getInputStream());
        out = sock.getOutputStream();
    }

    public Object cmd(Object... args) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.write(('*' + Integer.toString(args.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
        for (Object a : args) {
            byte[] v = a instanceof byte[] ? (byte[]) a : String.valueOf(a).getBytes(StandardCharsets.UTF_8);
            b.write(('$' + Integer.toString(v.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
            b.write(v);
            b.write("\r\n".getBytes(StandardCharsets.US_ASCII));
        }
        out.write(b.toByteArray());
        out.flush();
        return read();
    }

    /** {@link #cmd} with a bulk reply decoded as UTF-8 (null stays null). */
    public String str(Object... args) throws IOException {
        Object r = cmd(args);
        if (r == null) return null;
        if (r instanceof byte[]) return new String((byte[]) r, StandardCharsets.UTF_8);
        return String.valueOf(r);
    }

    public long num(Object... args) throws IOException {
        Object r = cmd(args);
        if (r instanceof Long) return (Long) r;
        throw new IOException("RESP: expected integer reply, got " + describe(r));
    }

    static String describe(Object r) {
        if (r instanceof byte[]) return "bulk '" + new String((byte[]) r, StandardCharsets.UTF_8) + "'";
        return String.valueOf(r);
    }

    private String line() throws IOException {
        StringBuilder s = new StringBuilder();
        int c;
        while ((c = in.read()) != -1) {
            if (c == '\r') {
                if (in.read() != '\n') throw new IOException("RESP: bad line ending");
                return s.toString();
            }
            s.append((char) c);
        }
        throw new IOException("RESP: connection closed");
    }

    private Object read() throws IOException {
        int t = in.read();
        if (t == -1) throw new IOException("RESP: connection closed");
        String l = line();
        switch (t) {
            case '+': return l.getBytes(StandardCharsets.UTF_8);
            case '-': throw new IOException("RESP error: " + l);
            case ':': return Long.parseLong(l);
            case '$': {
                int n = Integer.parseInt(l);
                if (n < 0) return null;
                byte[] v = in.readNBytes(n);
                if (v.length != n || in.read() != '\r' || in.read() != '\n') throw new IOException("RESP: short bulk");
                return v;
            }
            case '*': {
                int n = Integer.parseInt(l);
                if (n < 0) return null;
                List<Object> a = new ArrayList<>(n);
                for (int i = 0; i < n; i++) a.add(read());
                return a;
            }
            default: throw new IOException("RESP: unknown reply type '" + (char) t + "'");
        }
    }

    @Override public void close() throws IOException { sock.close(); }
}
