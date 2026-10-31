package org.particledb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Result decoding through Apache Arrow Java, used only when the connection
 * asks for it ({@code resultDecoder=arrow}). The default is the driver's own
 * {@link ArrowIpcReader}, which needs neither Arrow Java nor a JVM flag.
 *
 * <p>Arrow Java is an OPTIONAL dependency of this driver: an application that
 * selects this decoder adds {@code org.apache.arrow:arrow-vector} and a memory
 * implementation ({@code arrow-memory-netty} or {@code arrow-memory-unsafe})
 * itself, and on JDK 16 and later starts the JVM with
 * {@link ArrowDecoder#ADD_OPENS_FLAG}. Nothing in this class is linked until
 * {@link ArrowDecoder} has checked that Arrow Java is on the classpath, so a
 * missing Arrow jar produces an actionable message, not a
 * {@code NoClassDefFoundError}.
 */
final class ArrowJavaDecoder {

    /** One allocator for every decode. RootAllocator is thread-safe, and
     *  each reader releases its buffers on close. */
    private static final org.apache.arrow.memory.BufferAllocator ALLOC =
            new org.apache.arrow.memory.RootAllocator(Long.MAX_VALUE);

    private ArrowJavaDecoder() {}

    private static org.apache.arrow.vector.ipc.ArrowStreamReader reader(byte[] payload) {
        return new org.apache.arrow.vector.ipc.ArrowStreamReader(
                new org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel(payload),
                ALLOC);
    }

    static ArrowDecoder.Decoded decode(byte[] payload) throws java.io.IOException {
        try (org.apache.arrow.vector.ipc.ArrowStreamReader r = reader(payload)) {
            org.apache.arrow.vector.VectorSchemaRoot root = r.getVectorSchemaRoot();
            // Column names, type tokens and custom metadata come from the
            // SCHEMA, so they are known for a 0-row result too; projection
            // order and duplicate labels are preserved (positional).
            List<org.apache.arrow.vector.types.pojo.Field> fields = root.getSchema().getFields();
            int ncols = fields.size();
            List<String> columnNames = new ArrayList<>(ncols);
            List<String> columnTypes = new ArrayList<>(ncols);
            List<Map<String, String>> columnMetadata = new ArrayList<>(ncols);
            // A DICTIONARY-encoded field: Arrow Java types the field (and its
            // vector) as the INDEX type; the values live in the reader's
            // dictionary map. Every accessor must see the VALUE.
            Map<Long, org.apache.arrow.vector.dictionary.Dictionary> dicts = r.getDictionaryVectors();
            org.apache.arrow.vector.dictionary.Dictionary[] dictOf =
                    new org.apache.arrow.vector.dictionary.Dictionary[ncols];
            for (int ci = 0; ci < ncols; ci++) {
                org.apache.arrow.vector.types.pojo.Field f = fields.get(ci);
                columnNames.add(f.getName());
                org.apache.arrow.vector.types.pojo.DictionaryEncoding enc = f.getDictionary();
                if (enc != null) {
                    org.apache.arrow.vector.dictionary.Dictionary d = dicts.get(enc.getId());
                    if (d == null) {
                        throw new java.io.IOException("Arrow IPC: column '" + f.getName()
                                + "' is dictionary-encoded (id " + enc.getId()
                                + ") but the stream carries no such dictionary");
                    }
                    dictOf[ci] = d;
                    columnTypes.add(ArrowDecoder.arrowTypeToken(d.getVectorType()));
                } else {
                    columnTypes.add(ArrowDecoder.arrowTypeToken(f.getType()));
                }
                Map<String, String> md = f.getMetadata();
                columnMetadata.add(md == null || md.isEmpty()
                        ? Collections.<String, String>emptyMap() : md);
            }
            List<Object[]> rows = new ArrayList<>();
            while (r.loadNextBatch()) {
                int rowCount = root.getRowCount();
                List<org.apache.arrow.vector.FieldVector> vectors = root.getFieldVectors();
                String[] vecTypes = new String[ncols];
                for (int ci = 0; ci < ncols; ci++) {
                    vecTypes[ci] = dictOf[ci] != null
                            ? dictOf[ci].getVector().getClass().getSimpleName()
                            : vectors.get(ci).getClass().getSimpleName();
                }
                for (int row = 0; row < rowCount; row++) {
                    Object[] out = new Object[ncols];
                    for (int ci = 0; ci < ncols; ci++) {
                        org.apache.arrow.vector.FieldVector v = vectors.get(ci);
                        Object cell = dictOf[ci] != null
                                ? dictionaryValue(v, dictOf[ci], row)
                                : v.getObject(row);
                        out[ci] = normalize(cell, vecTypes[ci]);
                    }
                    rows.add(out);
                }
            }
            return new ArrowDecoder.Decoded(columnNames, rows, columnTypes, columnMetadata);
        }
    }

    /**
     * The VALUE of a dictionary-encoded cell. NULL when the index is null or
     * the dictionary entry is null; an index outside the dictionary fails.
     */
    static Object dictionaryValue(org.apache.arrow.vector.FieldVector indices,
                                  org.apache.arrow.vector.dictionary.Dictionary dict, int row)
            throws java.io.IOException {
        if (indices.isNull(row)) return null;
        if (!(indices instanceof org.apache.arrow.vector.BaseIntVector)) {
            throw new java.io.IOException("Arrow IPC: dictionary index vector is "
                    + indices.getClass().getSimpleName() + ", not an integer vector");
        }
        long idx = ((org.apache.arrow.vector.BaseIntVector) indices).getValueAsLong(row);
        org.apache.arrow.vector.FieldVector values = dict.getVector();
        if (idx < 0 || idx >= values.getValueCount()) {
            throw new java.io.IOException("Arrow IPC: dictionary index " + idx
                    + " outside a dictionary of " + values.getValueCount() + " values");
        }
        return values.getObject((int) idx);
    }

    private static Object normalize(Object v, String vecType) {
        if (v instanceof org.apache.arrow.vector.util.Text) return v.toString();
        if (v instanceof java.util.List) {
            // Arrow List column -> plain Object[] so PdbResultSet.getArray can
            // wrap it; unwrap Text elements.
            java.util.List<?> lst = (java.util.List<?>) v;
            Object[] out = new Object[lst.size()];
            for (int i = 0; i < out.length; i++) {
                Object e = lst.get(i);
                out[i] = e instanceof org.apache.arrow.vector.util.Text ? e.toString() : e;
            }
            return out;
        }
        if (v == null) return null;
        // A DATE column is an Arrow Date32 whose getObject() is a RAW epoch-day
        // Integer; render it as a LocalDate.
        if (v instanceof Integer && "DateDayVector".equals(vecType)) {
            return java.time.LocalDate.ofEpochDay(((Integer) v).longValue());
        }
        return v;
    }
}
