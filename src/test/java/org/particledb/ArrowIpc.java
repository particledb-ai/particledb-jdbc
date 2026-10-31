package org.particledb;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Test-only builder for a REAL Arrow IPC stream in the exact shape the engine
 * ships, so a ResultSet test exercises the true decode path
 * ({@link ArrowDecoder} → {@link PdbResultSet}) instead of a hand-made row list.
 *
 * <p>Deliberately separate from {@code ArrayElementTypeTest}'s array-only
 * helpers: this one builds arbitrary scalar column types (utf8 / int64 / bool /
 * timestamp(µs) / time(µs) / float32 / float64, and numeric as stamped text),
 * which is what the ResultSet getter contract needs.
 */
final class ArrowIpc {

    private final List<Field> fields = new ArrayList<>();
    private final List<Object[]> cols = new ArrayList<>();

    private ArrowIpc() {}

    static ArrowIpc build() { return new ArrowIpc(); }

    ArrowIpc utf8(String name, String... cells) {
        return col(name, new ArrowType.Utf8(), (Object[]) cells);
    }

    ArrowIpc int64(String name, Long... cells) {
        return col(name, new ArrowType.Int(64, true), (Object[]) cells);
    }

    ArrowIpc bool(String name, Boolean... cells) {
        return col(name, ArrowType.Bool.INSTANCE, (Object[]) cells);
    }

    /** TIMESTAMP column, cells are MICROSECONDS since epoch (what PDB ships). */
    ArrowIpc timestampMicros(String name, Long... cells) {
        return col(name, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), (Object[]) cells);
    }

    /**
     * TIMESTAMPTZ column as the engine ships it: the same zone-less Arrow
     * {@code Timestamp(µs)} holding the UTC instant, with the field stamped
     * {@code pdb:decl = timestamptz} and {@code pdb:tz = <session zone>}.
     */
    ArrowIpc timestamptzMicros(String name, String sessionZone, Long... cells) {
        java.util.Map<String, String> md = new java.util.HashMap<>();
        md.put("pdb:decl", "timestamptz");
        md.put("pdb:tz", sessionZone);
        fields.add(new Field(name, new FieldType(true,
                new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), null, md), null));
        cols.add(cells);
        return this;
    }

    /** TIME column, cells are MICROSECONDS of day (what PDB ships). */
    ArrowIpc timeMicros(String name, Long... cells) {
        return col(name, new ArrowType.Time(TimeUnit.MICROSECOND, 64), (Object[]) cells);
    }

    /**
     * Utf8 column stamped {@code pdb:decl = decl}: how the engine ships a
     * {@code numeric} column as text (unconstrained always, {@code numeric(p,s)}
     * when the batch holds a NaN).
     */
    ArrowIpc utf8Decl(String name, String decl, String... cells) {
        java.util.Map<String, String> md = new java.util.HashMap<>();
        md.put("pdb:decl", decl);
        fields.add(new Field(name, new FieldType(true, new ArrowType.Utf8(), null, md), null));
        cols.add(cells);
        return this;
    }

    ArrowIpc float32(String name, Float... cells) {
        return col(name,
                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), (Object[]) cells);
    }

    ArrowIpc float64(String name, Double... cells) {
        return col(name,
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), (Object[]) cells);
    }

    private ArrowIpc col(String name, ArrowType t, Object[] cells) {
        fields.add(new Field(name, new FieldType(true, t, null, null), null));
        cols.add(cells);
        return this;
    }

    byte[] ipc() throws Exception {
        int rows = cols.isEmpty() ? 0 : cols.get(0).length;
        Schema schema = new Schema(Collections.unmodifiableList(fields));
        try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            for (int c = 0; c < cols.size(); c++) {
                FieldVector v = root.getVector(c);
                v.setInitialCapacity(Math.max(rows, 1));
                v.allocateNew();
                Object[] cells = cols.get(c);
                for (int r = 0; r < cells.length; r++) set(v, r, cells[r]);
                v.setValueCount(cells.length);
            }
            root.setRowCount(rows);
            try (ArrowStreamWriter w =
                         new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                w.start();
                w.writeBatch();
                w.end();
            }
            return out.toByteArray();
        }
    }

    /** A live {@link PdbResultSet} over this stream, cursor still before-first. */
    PdbResultSet rs() throws Exception {
        return new PdbResultSet(
                org.particledb.wire.StreamState.StreamResult.arrow(ipc()));
    }

    /** {@link #rs()}, decoded with {@code decoder}. */
    PdbResultSet rs(ArrowDecoder.ResultDecoder decoder) throws Exception {
        return new PdbResultSet(
                org.particledb.wire.StreamState.StreamResult.arrow(ipc()), decoder);
    }

    private static void set(FieldVector v, int r, Object cell) {
        if (cell == null) { v.setNull(r); return; }
        if (v instanceof VarCharVector) {
            ((VarCharVector) v).setSafe(r, ((String) cell).getBytes(StandardCharsets.UTF_8));
        } else if (v instanceof BigIntVector) {
            ((BigIntVector) v).setSafe(r, (Long) cell);
        } else if (v instanceof BitVector) {
            ((BitVector) v).setSafe(r, ((Boolean) cell) ? 1 : 0);
        } else if (v instanceof TimeStampMicroVector) {
            ((TimeStampMicroVector) v).setSafe(r, (Long) cell);
        } else if (v instanceof TimeMicroVector) {
            ((TimeMicroVector) v).setSafe(r, (Long) cell);
        } else if (v instanceof org.apache.arrow.vector.Float4Vector) {
            ((org.apache.arrow.vector.Float4Vector) v).setSafe(r, (Float) cell);
        } else if (v instanceof org.apache.arrow.vector.Float8Vector) {
            ((org.apache.arrow.vector.Float8Vector) v).setSafe(r, (Double) cell);
        } else {
            throw new IllegalStateException("ArrowIpc: unhandled vector " + v.getClass());
        }
    }
}
