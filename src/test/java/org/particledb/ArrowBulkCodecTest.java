package org.particledb;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.particledb.wire.WireV2Codec;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bulk-ingest DATA bodies against Apache Arrow Java: the driver's heap writer
 * ({@link ArrowIpcWriter}, used by {@code executeBatch}) must produce a stream
 * Arrow Java reads back exactly, and {@link ArrowBulk} (the caller's own Arrow
 * vectors) must slice and rename as documented.
 */
@Tag("arrow-java")
public class ArrowBulkCodecTest {

    private static List<Object[]> rows(int n) {
        List<Object[]> rows = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            rows.add(new Object[] {(long) i, i % 4 == 3 ? null : "v" + i + "-é✓", i % 2 == 0});
        }
        return rows;
    }

    @Test
    void heapWriterOutputReadsBackThroughArrowJava() throws Exception {
        List<String> cols = Arrays.asList("id", "v", "flag");
        for (int n : new int[] {0, 1, 7, 8, 9, 1000}) {
            List<Object[]> rows = rows(n);
            byte[][][] text = BulkIngest.rowsToText(cols, rows);
            for (int[] range : new int[][] {{0, n}, {n / 3, n - n / 3}}) {
                byte[] data = BulkIngest.serializeText(cols, text, range[0], range[1]);
                assertEquals(WireV2Codec.BULK_DATA, data[0]);
                try (BufferAllocator a = new RootAllocator();
                     ArrowStreamReader r = new ArrowStreamReader(
                             new ByteArrayInputStream(data, 1, data.length - 1), a)) {
                    VectorSchemaRoot root = r.getVectorSchemaRoot();
                    for (int c = 0; c < cols.size(); c++) {
                        assertEquals(cols.get(c), root.getSchema().getFields().get(c).getName());
                        assertEquals(ArrowType.Utf8.INSTANCE, root.getSchema().getFields().get(c).getType());
                        assertTrue(root.getSchema().getFields().get(c).isNullable());
                    }
                    assertTrue(r.loadNextBatch());
                    assertEquals(range[1], root.getRowCount());
                    for (int i = 0; i < range[1]; i++) {
                        Object[] want = rows.get(range[0] + i);
                        for (int c = 0; c < cols.size(); c++) {
                            VarCharVector v = (VarCharVector) root.getVector(c);
                            Object w = BulkIngest.textOf(want[c]);
                            if (w == null) assertTrue(v.isNull(i), "row " + i + " col " + c);
                            else assertEquals(w, new String(v.get(i), StandardCharsets.UTF_8));
                        }
                    }
                    assertFalse(r.loadNextBatch());
                }
            }
        }
    }

    @Test
    void callerVectorsSliceAndRename() throws Exception {
        try (BufferAllocator a = new RootAllocator();
             VarCharVector id = new VarCharVector("id", a);
             VarCharVector v = new VarCharVector("v", a)) {
            id.allocateNew(3);
            v.allocateNew(3);
            for (int i = 0; i < 3; i++) {
                id.setSafe(i, String.valueOf(i + 1).getBytes(StandardCharsets.UTF_8));
                if (i == 1) v.setNull(i); else v.setSafe(i, ("x" + i).getBytes(StandardCharsets.UTF_8));
            }
            id.setValueCount(3);
            v.setValueCount(3);
            VectorSchemaRoot root = VectorSchemaRoot.of(id, v);
            byte[] data = ArrowBulk.serialize(root, null, null, 1, 2);
            assertEquals(WireV2Codec.BULK_DATA, data[0]);
            ArrowDecoder.Decoded d = ArrowIpcReader.decode(Arrays.copyOfRange(data, 1, data.length));
            assertEquals(Arrays.asList("id", "v"), d.columnNames);
            assertArrayEquals(new Object[] {"2", null}, d.rows.get(0));
            assertArrayEquals(new Object[] {"3", "x2"}, d.rows.get(1));
            byte[] renamed = ArrowBulk.serialize(root, null, Arrays.asList("x", "y"), 0, 3);
            d = ArrowIpcReader.decode(Arrays.copyOfRange(renamed, 1, renamed.length));
            assertEquals(Arrays.asList("x", "y"), d.columnNames);
            assertEquals(3, d.rows.size());
        }
    }
}
