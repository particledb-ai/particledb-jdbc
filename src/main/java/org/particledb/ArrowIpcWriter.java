package org.particledb;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * The driver's own Arrow IPC stream writer, pure Java on the heap (no
 * allocator, no direct memory, no JVM flag). It writes exactly what the
 * driver sends: one stream of nullable {@code Utf8} columns (schema message,
 * one record batch, end-of-stream), the body of a native bulk-ingest DATA
 * frame ({@link BulkIngest}). The server parses each text value with the
 * target column's input function.
 *
 * <p>Layout follows the Arrow IPC V5 format: every message is the
 * continuation marker, a little-endian metadata length, a flatbuffer
 * {@code Message} padded to 8 bytes, and the body; every body buffer starts at
 * an 8-byte boundary. {@code ArrowIpcWriterTest} reads its output back with
 * this driver's reader and with Apache Arrow Java.
 */
final class ArrowIpcWriter {
    private ArrowIpcWriter() {}

    private static final short METADATA_V5 = 4;
    private static final byte TYPE_UTF8 = 5;

    /**
     * One IPC stream of rows {@code [start, start+count)} of {@code cols}
     * (column-major UTF-8 values, null for SQL NULL), the i-th column named
     * {@code names.get(i)}, written after {@code prefix} (none when null).
     */
    static byte[] utf8Stream(byte[] prefix, List<String> names, byte[][][] cols, int start, int count) {
        int nc = names.size();
        if (cols.length != nc) throw new IllegalArgumentException(nc + " names for " + cols.length + " columns");
        Out out = new Out(1024);
        if (prefix != null) out.bytes(prefix, 0, prefix.length);

        // Schema message.
        List<Table> fields = new ArrayList<>(nc);
        for (String name : names) {
            fields.add(new Table()
                    .ref(0, new Str(name))
                    .u8(1, 1) // nullable
                    .u8(2, TYPE_UTF8)
                    .ref(3, new Table()) // Utf8 has no fields
                    .ref(5, new TableVec(new ArrayList<>())));
        }
        Table schema = new Table().ref(1, new TableVec(fields));
        message(out, new Table().i16(0, METADATA_V5).u8(1, ArrowIpcReader.H_SCHEMA).ref(2, schema).i64(3, 0), null);

        // Record batch: validity, offsets and data per column.
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        byte[] nodes = new byte[16 * nc];
        byte[] buffers = new byte[16 * 3 * nc];
        int bi = 0;
        for (int c = 0; c < nc; c++) {
            byte[][] col = cols[c];
            int nulls = 0;
            byte[] validity = new byte[(count + 7) / 8];
            byte[] offsets = new byte[4 * (count + 1)];
            int dataLen = 0;
            for (int r = 0; r < count; r++) {
                byte[] v = col[start + r];
                if (v == null) {
                    nulls++;
                } else {
                    validity[r >> 3] |= (byte) (1 << (r & 7));
                    dataLen += v.length;
                }
                putI32(offsets, 4 * (r + 1), dataLen);
            }
            byte[] data = new byte[dataLen];
            int at = 0;
            for (int r = 0; r < count; r++) {
                byte[] v = col[start + r];
                if (v != null) {
                    System.arraycopy(v, 0, data, at, v.length);
                    at += v.length;
                }
            }
            putI64(nodes, 16 * c, count);
            putI64(nodes, 16 * c + 8, nulls);
            for (byte[] b : new byte[][] {validity, offsets, data}) {
                putI64(buffers, 16 * bi, body.size());
                putI64(buffers, 16 * bi + 8, b.length);
                bi++;
                body.write(b, 0, b.length);
                for (int pad = pad8(b.length); pad > 0; pad--) body.write(0);
            }
        }
        byte[] bodyBytes = body.toByteArray();
        Table batch = new Table().i64(0, count).ref(1, new StructVec(nodes)).ref(2, new StructVec(buffers));
        message(out, new Table().i16(0, METADATA_V5).u8(1, ArrowIpcReader.H_RECORD_BATCH).ref(2, batch)
                .i64(3, bodyBytes.length), bodyBytes);

        // End of stream: continuation marker, zero length.
        out.i32(-1);
        out.i32(0);
        return out.toByteArray();
    }

    private static void message(Out out, Table msg, byte[] body) {
        byte[] m = encapsulate(msg, body);
        out.bytes(m, 0, m.length);
    }

    /** One encapsulated IPC message: marker, length, the flatbuffer of {@code msg}, then {@code body}. */
    static byte[] encapsulate(Table msg, byte[] body) {
        byte[] fb = new Fbb().finish(msg);
        Out out = new Out(fb.length + 8 + (body == null ? 0 : body.length));
        out.i32(-1);
        out.i32(fb.length);
        out.bytes(fb, 0, fb.length);
        if (body != null) out.bytes(body, 0, body.length);
        return out.toByteArray();
    }

    private static int pad8(int n) {
        return (8 - (n & 7)) & 7;
    }

    static void putI32(byte[] b, int at, int v) {
        b[at] = (byte) v;
        b[at + 1] = (byte) (v >>> 8);
        b[at + 2] = (byte) (v >>> 16);
        b[at + 3] = (byte) (v >>> 24);
    }

    static void putI64(byte[] b, int at, long v) {
        putI32(b, at, (int) v);
        putI32(b, at + 4, (int) (v >>> 32));
    }

    // ── a forward flatbuffer builder for the few shapes above ─────────────

    interface Node {}

    /** A table: scalar fields and references to child objects, by field index. */
    static final class Table implements Node {
        final List<Object[]> fields = new ArrayList<>(); // {index, size, value(Long) | Node}

        Table i32(int idx, int v) { fields.add(new Object[] {idx, 4, (long) v}); return this; }
        Table u8(int idx, int v) { fields.add(new Object[] {idx, 1, (long) v}); return this; }
        Table i16(int idx, int v) { fields.add(new Object[] {idx, 2, (long) v}); return this; }
        Table i64(int idx, long v) { fields.add(new Object[] {idx, 8, v}); return this; }
        Table ref(int idx, Node n) { fields.add(new Object[] {idx, 4, n}); return this; }
    }

    static final class Str implements Node {
        final byte[] utf8;
        Str(String s) { utf8 = s.getBytes(StandardCharsets.UTF_8); }
    }

    static final class TableVec implements Node {
        final List<Table> items;
        TableVec(List<Table> items) { this.items = items; }
    }

    /** A vector of 16-byte structs (FieldNode, Buffer), 8-byte aligned. */
    static final class StructVec implements Node {
        final byte[] raw;
        StructVec(byte[] raw) { this.raw = raw; }
    }

    /**
     * Writes a flatbuffer front to back: an object is written before the
     * objects it references, so every uoffset points forward, as the format
     * requires. Alignment is relative to the buffer start, which the IPC
     * framing places on an 8-byte boundary.
     */
    static final class Fbb {
        final Out o = new Out(256);

        byte[] finish(Table root) {
            o.i32(0); // root uoffset, patched
            int rootPos = write(root);
            o.patchI32(0, rootPos);
            while ((o.size() & 7) != 0) o.u8(0);
            return o.toByteArray();
        }

        private int write(Node n) {
            if (n instanceof Table) return writeTable((Table) n);
            if (n instanceof Str) {
                o.align(4);
                int pos = o.size();
                byte[] b = ((Str) n).utf8;
                o.i32(b.length);
                o.bytes(b, 0, b.length);
                o.u8(0);
                return pos;
            }
            if (n instanceof TableVec) {
                List<Table> items = ((TableVec) n).items;
                o.align(4);
                int pos = o.size();
                o.i32(items.size());
                int slots = o.size();
                for (int i = 0; i < items.size(); i++) o.i32(0);
                for (int i = 0; i < items.size(); i++) {
                    int at = slots + 4 * i;
                    o.patchI32(at, write(items.get(i)) - at);
                }
                return pos;
            }
            StructVec sv = (StructVec) n;
            while (((o.size() + 4) & 7) != 0) o.u8(0);
            int pos = o.size();
            o.i32(sv.raw.length / 16);
            o.bytes(sv.raw, 0, sv.raw.length);
            return pos;
        }

        private int writeTable(Table t) {
            // Inline layout: the soffset, then fields largest first, each
            // aligned to its size (the table itself starts 8-aligned).
            List<Object[]> f = new ArrayList<>(t.fields);
            f.sort((a, b) -> Integer.compare((Integer) b[1], (Integer) a[1]));
            int maxIdx = -1;
            for (Object[] x : f) maxIdx = Math.max(maxIdx, (Integer) x[0]);
            int[] offsetOf = new int[maxIdx + 1];
            int[] inlineAt = new int[f.size()];
            int cursor = 4;
            for (int i = 0; i < f.size(); i++) {
                int size = (Integer) f.get(i)[1];
                cursor = (cursor + size - 1) / size * size;
                inlineAt[i] = cursor;
                offsetOf[(Integer) f.get(i)[0]] = cursor;
                cursor += size;
            }
            int tableSize = cursor;
            // vtable
            o.align(2);
            int vt = o.size();
            o.i16(4 + 2 * (maxIdx + 1));
            o.i16(tableSize);
            for (int i = 0; i <= maxIdx; i++) o.i16(offsetOf[i]);
            o.align(8);
            int tpos = o.size();
            o.i32(tpos - vt); // soffset: vtable = table - soffset
            while (o.size() < tpos + tableSize) o.u8(0);
            for (int i = 0; i < f.size(); i++) {
                Object[] x = f.get(i);
                int at = tpos + inlineAt[i];
                int size = (Integer) x[1];
                if (x[2] instanceof Long) {
                    long v = (Long) x[2];
                    for (int k = 0; k < size; k++) o.patchU8(at + k, (int) (v >>> (8 * k)));
                }
            }
            for (int i = 0; i < f.size(); i++) {
                Object[] x = f.get(i);
                if (x[2] instanceof Node) {
                    int at = tpos + inlineAt[i];
                    o.patchI32(at, write((Node) x[2]) - at);
                }
            }
            return tpos;
        }
    }

    /** A growable little-endian byte buffer. */
    private static final class Out {
        byte[] b;
        int n;

        Out(int cap) { b = new byte[cap]; }

        int size() { return n; }

        private void room(int k) {
            if (n + k > b.length) b = java.util.Arrays.copyOf(b, Math.max(b.length * 2, n + k));
        }

        void u8(int v) { room(1); b[n++] = (byte) v; }
        void i16(int v) { room(2); b[n++] = (byte) v; b[n++] = (byte) (v >>> 8); }
        void i32(int v) { room(4); putI32(b, n, v); n += 4; }
        void bytes(byte[] src, int off, int len) { room(len); System.arraycopy(src, off, b, n, len); n += len; }
        void align(int a) { while (n % a != 0) u8(0); }
        void patchU8(int at, int v) { b[at] = (byte) v; }
        void patchI32(int at, int v) { putI32(b, at, v); }
        byte[] toByteArray() { return java.util.Arrays.copyOf(b, n); }
    }
}
