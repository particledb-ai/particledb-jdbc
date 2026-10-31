package org.particledb;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Hand-built Arrow IPC streams for what no Arrow writer available to the
 * tests produces: a compressed record batch, and schemas of types the reader
 * refuses. Built with the driver's flatbuffer builder ({@link ArrowIpcWriter}).
 */
final class GoldenMutations {
    private GoldenMutations() {}

    private static ArrowIpcWriter.Table message(byte header, ArrowIpcWriter.Table body, long bodyLen) {
        return new ArrowIpcWriter.Table().i16(0, 4).u8(1, header).ref(2, body).i64(3, bodyLen);
    }

    private static byte[] schema(int typeId, ArrowIpcWriter.Table type) {
        return schema(typeId, type, null);
    }

    private static byte[] schema(int typeId, ArrowIpcWriter.Table type, String decl) {
        ArrowIpcWriter.Table field = new ArrowIpcWriter.Table()
                .ref(0, new ArrowIpcWriter.Str("c")).u8(1, 1).u8(2, typeId).ref(3, type)
                .ref(5, new ArrowIpcWriter.TableVec(new ArrayList<>()));
        if (decl != null) {
            ArrowIpcWriter.Table kv = new ArrowIpcWriter.Table()
                    .ref(0, new ArrowIpcWriter.Str("pdb:decl")).ref(1, new ArrowIpcWriter.Str(decl));
            field.ref(6, new ArrowIpcWriter.TableVec(Collections.singletonList(kv)));
        }
        ArrowIpcWriter.Table schema = new ArrowIpcWriter.Table()
                .ref(1, new ArrowIpcWriter.TableVec(Collections.singletonList(field)));
        return ArrowIpcWriter.encapsulate(message(ArrowIpcReader.H_SCHEMA, schema, 0), null);
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        for (byte[] p : parts) o.write(p, 0, p.length);
        o.write(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0, 0, 0, 0}, 0, 8);
        return o.toByteArray();
    }

    /**
     * An Int32 column of one row (value 0); with {@code compressed} its record
     * batch declares LZ4_FRAME body compression.
     */
    static byte[] int32(boolean compressed) {
        return int32(compressed, 0);
    }

    /** {@link #int32(boolean)} whose values buffer claims offset {@code valuesOffset}. */
    static byte[] int32(boolean compressed, long valuesOffset) {
        byte[] s = schema(2, new ArrowIpcWriter.Table().i32(0, 32).u8(1, 1));
        byte[] nodes = new byte[16];
        ArrowIpcWriter.putI64(nodes, 0, 1);
        byte[] buffers = new byte[32];
        ArrowIpcWriter.putI64(buffers, 16, valuesOffset);
        ArrowIpcWriter.putI64(buffers, 24, 8);
        ArrowIpcWriter.Table batch = new ArrowIpcWriter.Table().i64(0, 1)
                .ref(1, new ArrowIpcWriter.StructVec(nodes)).ref(2, new ArrowIpcWriter.StructVec(buffers));
        if (compressed) batch.ref(3, new ArrowIpcWriter.Table().u8(0, 0).u8(1, 0));
        byte[] b = ArrowIpcWriter.encapsulate(message(ArrowIpcReader.H_RECORD_BATCH, batch, 8), new byte[8]);
        return concat(s, b);
    }

    /** A schema-only stream whose one column has flatbuffer type id {@code typeId} (no type fields). */
    static byte[] schemaOfType(int typeId) {
        return concat(schema(typeId, new ArrowIpcWriter.Table()));
    }

    /**
     * A Float16 column (Apache Arrow Java 15 cannot write one): {@code raw}
     * half-float bit patterns, null for NULL, split over two record batches
     * like {@link ArrowGolden}'s streams.
     */
    static byte[] float16(String decl, Object[] raw) {
        byte[] s = schema(3, new ArrowIpcWriter.Table().i16(0, 0), decl);
        int n = raw.length;
        int first = (n + 1) / 2;
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        o.write(s, 0, s.length);
        for (int[] range : new int[][] {{0, first}, {first, n}}) {
            int rows = range[1] - range[0];
            byte[] validity = new byte[8];
            byte[] values = new byte[(2 * rows + 7) / 8 * 8];
            int nulls = 0;
            for (int i = 0; i < rows; i++) {
                Object v = raw[range[0] + i];
                if (v == null) {
                    nulls++;
                    continue;
                }
                validity[i >> 3] |= (byte) (1 << (i & 7));
                long bits = (Long) v;
                values[2 * i] = (byte) bits;
                values[2 * i + 1] = (byte) (bits >>> 8);
            }
            byte[] nodes = new byte[16];
            ArrowIpcWriter.putI64(nodes, 0, rows);
            ArrowIpcWriter.putI64(nodes, 8, nulls);
            byte[] buffers = new byte[32];
            ArrowIpcWriter.putI64(buffers, 0, 0);
            ArrowIpcWriter.putI64(buffers, 8, 8);
            ArrowIpcWriter.putI64(buffers, 16, 8);
            ArrowIpcWriter.putI64(buffers, 24, values.length);
            byte[] body = new byte[8 + values.length];
            System.arraycopy(validity, 0, body, 0, 8);
            System.arraycopy(values, 0, body, 8, values.length);
            ArrowIpcWriter.Table batch = new ArrowIpcWriter.Table().i64(0, rows)
                    .ref(1, new ArrowIpcWriter.StructVec(nodes)).ref(2, new ArrowIpcWriter.StructVec(buffers));
            byte[] b = ArrowIpcWriter.encapsulate(message(ArrowIpcReader.H_RECORD_BATCH, batch, body.length), body);
            o.write(b, 0, b.length);
        }
        return concat(o.toByteArray());
    }

    /**
     * A FixedSizeBinary(0) column whose one record batch claims {@code rows}
     * values (and has no data, as a zero-width type needs none).
     */
    static byte[] zeroWidthBinary(long rows) {
        byte[] s = schema(15, new ArrowIpcWriter.Table().i32(0, 0));
        byte[] nodes = new byte[16];
        ArrowIpcWriter.putI64(nodes, 0, rows);
        byte[] buffers = new byte[32];
        ArrowIpcWriter.Table batch = new ArrowIpcWriter.Table().i64(0, rows)
                .ref(1, new ArrowIpcWriter.StructVec(nodes)).ref(2, new ArrowIpcWriter.StructVec(buffers));
        byte[] b = ArrowIpcWriter.encapsulate(message(ArrowIpcReader.H_RECORD_BATCH, batch, 0), null);
        return concat(s, b);
    }
}
