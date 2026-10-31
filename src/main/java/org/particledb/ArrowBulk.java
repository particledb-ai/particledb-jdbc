package org.particledb;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.particledb.wire.WireV2Codec;

/**
 * Native bulk loads of CALLER-SUPPLIED Apache Arrow Java vectors
 * ({@link PdbConnection#bulkLoad(String, ArrowReader)} and its overloads).
 * The caller built those vectors, so Arrow Java is on its classpath; this is
 * the only driver class besides {@link ArrowJavaDecoder} that links Arrow
 * Java, which is otherwise an optional dependency. Keeping every Arrow
 * reference here (rather than in {@link PdbConnection}, whose public methods
 * only pass the caller's objects through unchanged) means a connection loads
 * and verifies without Arrow Java on the classpath.
 */
final class ArrowBulk {
    private ArrowBulk() {}

    static List<String> fieldNames(VectorSchemaRoot root) {
        List<String> out = new ArrayList<>();
        for (Field f : root.getSchema().getFields()) out.add(f.getName());
        return out;
    }

    static long load(PdbConnection conn, String table, ArrowReader reader, long commitEveryRows)
            throws SQLException {
        final List<String> cols;
        try {
            cols = fieldNames(reader.getVectorSchemaRoot());
        } catch (java.io.IOException e) {
            throw new SQLException("bulk ingest: reading the Arrow schema failed: " + e.getMessage(), "22P04", e);
        }
        return conn.runBulk(table, cols, commitEveryRows, ing -> {
            while (reader.loadNextBatch()) {
                send(ing, reader.getVectorSchemaRoot(), reader, null);
            }
        });
    }

    static long load(PdbConnection conn, String table, List<String> columns, Iterable<VectorSchemaRoot> roots,
                     long commitEveryRows) throws SQLException {
        java.util.Iterator<VectorSchemaRoot> it = roots.iterator();
        if (!it.hasNext()) {
            throw new SQLException("bulk ingest: no data (an empty Iterable of roots)", "22023");
        }
        VectorSchemaRoot first = it.next();
        final List<String> cols = columns != null ? columns : fieldNames(first);
        return conn.runBulk(table, cols, commitEveryRows, ing -> {
            send(ing, first, null, columns);
            while (it.hasNext()) send(ing, it.next(), null, columns);
        });
    }

    /** Send one root's rows, split so every DATA payload stays within {@link BulkIngest#MAX_DATA_BYTES}. */
    static void send(BulkIngest ing, VectorSchemaRoot root, DictionaryProvider provider, List<String> names)
            throws Exception {
        int rows = root.getRowCount();
        if (rows == 0) return;
        long bytes = estimateBytes(root);
        int per = bytes <= BulkIngest.TARGET_DATA_BYTES ? rows
                : (int) Math.max(1L, rows * BulkIngest.TARGET_DATA_BYTES / bytes);
        for (int at = 0; at < rows; at += per) {
            sendRange(ing, root, provider, names, at, Math.min(per, rows - at));
        }
    }

    private static void sendRange(BulkIngest ing, VectorSchemaRoot root, DictionaryProvider provider,
                                  List<String> names, int start, int len) throws Exception {
        byte[] payload = serialize(root, provider, names, start, len);
        if (payload.length > BulkIngest.MAX_DATA_BYTES) {
            if (len > 1) {
                int h = len / 2;
                sendRange(ing, root, provider, names, start, h);
                sendRange(ing, root, provider, names, start + h, len - h);
                return;
            }
            throw new SQLException("bulk ingest: one row encodes to " + payload.length
                    + " bytes, over the " + BulkIngest.MAX_DATA_BYTES + "-byte DATA limit", "54000");
        }
        ing.sendData(payload);
    }

    /** {@code [BULK_DATA]} + one Arrow IPC stream (schema, one batch, EOS) of rows [start, start+len). */
    static byte[] serialize(VectorSchemaRoot root, DictionaryProvider provider, List<String> names,
                            int start, int len) throws Exception {
        VectorSchemaRoot slice = (start == 0 && len == root.getRowCount()) ? root : root.slice(start, len);
        try {
            VectorSchemaRoot out = slice;
            if (names != null) {
                List<Field> src = slice.getSchema().getFields();
                if (names.size() != src.size()) {
                    throw new SQLException("bulk ingest: " + names.size() + " column names for "
                            + src.size() + " vectors", "22023");
                }
                List<Field> renamed = new ArrayList<>(src.size());
                for (int i = 0; i < src.size(); i++) {
                    Field f = src.get(i);
                    renamed.add(new Field(names.get(i), f.getFieldType(), f.getChildren()));
                }
                // Shares the slice's vectors; never closed itself.
                out = new VectorSchemaRoot(new Schema(renamed), slice.getFieldVectors(), slice.getRowCount());
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream(1024 + (int) Math.min(
                    Integer.MAX_VALUE / 2, estimateBytes(out)));
            baos.write(WireV2Codec.BULK_DATA);
            DictionaryProvider p = provider != null ? provider : new DictionaryProvider.MapDictionaryProvider();
            try (ArrowStreamWriter w = new ArrowStreamWriter(out, p, Channels.newChannel(baos))) {
                w.start();
                w.writeBatch();
                w.end();
            }
            return baos.toByteArray();
        } finally {
            if (slice != root) slice.close();
        }
    }

    private static long estimateBytes(VectorSchemaRoot r) {
        long b = 0;
        for (FieldVector v : r.getFieldVectors()) b += v.getBufferSize();
        return b;
    }
}
