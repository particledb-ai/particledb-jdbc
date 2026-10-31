package org.particledb;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Writes each {@link GoldenCases} case as a real Arrow IPC stream with Apache
 * Arrow Java (test scope only): one column named {@code c}, the rows split over
 * two record batches. {@code ArrowIpcReaderDifferentialTest} decodes these with
 * both decoders; {@code -Dpdb.writeGolden=<dir>} writes them as the golden
 * files the flag-free {@code ArrowIpcReaderGoldenTest} reads.
 */
final class ArrowGolden {
    private ArrowGolden() {}

    static final BufferAllocator ALLOC = new RootAllocator(Long.MAX_VALUE);

    private static final long DICT_ID = 7L;

    static byte[] payload(GoldenCases.Case c) throws Exception {
        String spec = c.type;
        // Arrow Java 15 has no Float16 vector: hand-built (and Arrow Java
        // cannot read it either; see GoldenCases.Case#arrowJavaUnsupported).
        if (spec.equals("float:16")) return GoldenMutations.float16("golden:" + c.name, c.raw);
        Map<String, String> md = Collections.singletonMap("pdb:decl", "golden:" + c.name);
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
        Field field;
        List<Object> dictValues = null;
        if (spec.startsWith("dict:")) {
            String[] p = spec.split(":", 3);
            int indexBits = Integer.parseInt(p[1]);
            String valueSpec = p[2];
            dictValues = new ArrayList<>();
            for (Object v : c.raw) if (v != null && indexOf(dictValues, v) < 0) dictValues.add(v);
            Field valueField = field("dict", valueSpec, null);
            FieldVector dv = valueField.createVector(ALLOC);
            dv.allocateNew();
            for (int i = 0; i < dictValues.size(); i++) set(dv, valueSpec, i, dictValues.get(i));
            dv.setValueCount(dictValues.size());
            DictionaryEncoding enc = new DictionaryEncoding(DICT_ID, false, new ArrowType.Int(indexBits, true));
            provider.put(new Dictionary(dv, enc));
            field = new Field("c", new FieldType(true, new ArrowType.Int(indexBits, true), enc, md), null);
        } else {
            field = field("c", spec, md);
        }
        Schema schema = new Schema(Collections.singletonList(field));
        int n = c.raw.length;
        int first = (n + 1) / 2;
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, ALLOC)) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (ArrowStreamWriter w = new ArrowStreamWriter(root, provider, bos)) {
                w.start();
                for (int[] range : new int[][] {{0, first}, {first, n}}) {
                    root.allocateNew();
                    FieldVector v = root.getVector(0);
                    for (int r = range[0]; r < range[1]; r++) {
                        Object raw = c.raw[r];
                        int at = r - range[0];
                        if (dictValues != null) {
                            if (raw == null) v.setNull(at);
                            else ((BaseIntVector) v).setWithPossibleTruncate(at, indexOf(dictValues, raw));
                        } else {
                            set(v, spec, at, raw);
                        }
                    }
                    root.setRowCount(range[1] - range[0]);
                    w.writeBatch();
                }
                w.end();
            }
            return bos.toByteArray();
        } finally {
            for (long id : provider.getDictionaryIds()) provider.lookup(id).getVector().close();
        }
    }

    /** Position of {@code v} in {@code values} by content (arrays compare deeply). */
    private static int indexOf(List<Object> values, Object v) {
        for (int i = 0; i < values.size(); i++) {
            if (Arrays.deepEquals(new Object[] {values.get(i)}, new Object[] {v})) return i;
        }
        return -1;
    }

    /** Several cases (no dictionaries, equal row counts) side by side, every column named {@code c}. */
    static byte[] wide(List<GoldenCases.Case> cases) throws Exception {
        List<Field> fields = new ArrayList<>();
        for (GoldenCases.Case c : cases) {
            fields.add(field("c", c.type, Collections.singletonMap("pdb:decl", "golden:" + c.name)));
        }
        int n = cases.get(0).raw.length;
        try (VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), ALLOC)) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, bos)) {
                w.start();
                root.allocateNew();
                for (int ci = 0; ci < cases.size(); ci++) {
                    GoldenCases.Case c = cases.get(ci);
                    for (int r = 0; r < n; r++) set(root.getVector(ci), c.type, r, c.raw[r]);
                }
                root.setRowCount(n);
                w.writeBatch();
                w.end();
            }
            return bos.toByteArray();
        }
    }

    private static FieldType ft(ArrowType t, Map<String, String> md) {
        return new FieldType(true, t, null, md);
    }

    /** The Arrow field for a type spec. */
    static Field field(String name, String spec, Map<String, String> md) {
        String[] p = spec.split(":");
        switch (p[0]) {
            case "null": return new Field(name, ft(ArrowType.Null.INSTANCE, md), null);
            case "int": return new Field(name, ft(new ArrowType.Int(Integer.parseInt(p[1]), p[2].equals("s")), md), null);
            case "float": {
                FloatingPointPrecision fp = p[1].equals("16") ? FloatingPointPrecision.HALF
                        : p[1].equals("32") ? FloatingPointPrecision.SINGLE : FloatingPointPrecision.DOUBLE;
                return new Field(name, ft(new ArrowType.FloatingPoint(fp), md), null);
            }
            case "decimal":
                return new Field(name, ft(new ArrowType.Decimal(Integer.parseInt(p[1]), Integer.parseInt(p[2]),
                        Integer.parseInt(p[3])), md), null);
            case "bool": return new Field(name, ft(ArrowType.Bool.INSTANCE, md), null);
            case "utf8": return new Field(name, ft(ArrowType.Utf8.INSTANCE, md), null);
            case "largeutf8": return new Field(name, ft(ArrowType.LargeUtf8.INSTANCE, md), null);
            case "binary": return new Field(name, ft(ArrowType.Binary.INSTANCE, md), null);
            case "largebinary": return new Field(name, ft(ArrowType.LargeBinary.INSTANCE, md), null);
            case "fixedbinary":
                return new Field(name, ft(new ArrowType.FixedSizeBinary(Integer.parseInt(p[1])), md), null);
            case "date":
                return new Field(name, ft(new ArrowType.Date(p[1].equals("day") ? DateUnit.DAY : DateUnit.MILLISECOND), md), null);
            case "time": {
                TimeUnit u = unit(p[1]);
                int bits = (u == TimeUnit.SECOND || u == TimeUnit.MILLISECOND) ? 32 : 64;
                return new Field(name, ft(new ArrowType.Time(u, bits), md), null);
            }
            case "ts": {
                String tz = spec.substring(spec.indexOf(':', 3) + 1);
                return new Field(name, ft(new ArrowType.Timestamp(unit(p[1]), tz.isEmpty() ? null : tz), md), null);
            }
            case "duration": return new Field(name, ft(new ArrowType.Duration(unit(p[1])), md), null);
            case "interval": {
                IntervalUnit u = p[1].equals("ym") ? IntervalUnit.YEAR_MONTH
                        : p[1].equals("dt") ? IntervalUnit.DAY_TIME : IntervalUnit.MONTH_DAY_NANO;
                return new Field(name, ft(new ArrowType.Interval(u), md), null);
            }
            case "list":
                return new Field(name, ft(ArrowType.List.INSTANCE, md),
                        Collections.singletonList(field("item", spec.substring(5), null)));
            case "largelist":
                return new Field(name, ft(ArrowType.LargeList.INSTANCE, md),
                        Collections.singletonList(field("item", spec.substring(10), null)));
            case "fixedlist": {
                int k = Integer.parseInt(p[1]);
                String child = spec.substring(spec.indexOf(':', 10) + 1);
                return new Field(name, ft(new ArrowType.FixedSizeList(k), md),
                        Collections.singletonList(field("item", child, null)));
            }
            case "struct":
                return new Field(name, ft(ArrowType.Struct.INSTANCE, md), Arrays.asList(
                        field("a", "int:32:s", null), field("b", "utf8", null)));
            case "map": {
                Field key = new Field("key", new FieldType(false, ArrowType.Utf8.INSTANCE, null), null);
                Field value = field("value", "int:32:s", null);
                Field entries = new Field("entries", new FieldType(false, ArrowType.Struct.INSTANCE, null),
                        Arrays.asList(key, value));
                return new Field(name, ft(new ArrowType.Map(false), md), Collections.singletonList(entries));
            }
            default: throw new IllegalArgumentException(spec);
        }
    }

    private static TimeUnit unit(String u) {
        switch (u) {
            case "s": return TimeUnit.SECOND;
            case "ms": return TimeUnit.MILLISECOND;
            case "us": return TimeUnit.MICROSECOND;
            default: return TimeUnit.NANOSECOND;
        }
    }

    /** Set cell {@code at} of {@code v} (of type {@code spec}) to {@code raw}; null sets NULL. */
    static void set(ValueVector v, String spec, int at, Object raw) {
        if (raw == null) {
            if (v instanceof NullVector) return;
            ((FieldVector) v).setNull(at);
            return;
        }
        String[] p = spec.split(":");
        switch (p[0]) {
            case "int":
                ((BaseIntVector) v).setWithPossibleTruncate(at, (Long) raw);
                return;
            case "float":
                if (v instanceof Float4Vector) ((Float4Vector) v).setSafe(at, (float) (double) (Double) raw);
                else ((Float8Vector) v).setSafe(at, (Double) raw);
                return;
            case "decimal":
                if (v instanceof DecimalVector) ((DecimalVector) v).setSafe(at, new BigDecimal((String) raw));
                else ((Decimal256Vector) v).setSafe(at, new BigDecimal((String) raw));
                return;
            case "bool": ((BitVector) v).setSafe(at, ((Boolean) raw) ? 1 : 0); return;
            case "utf8": case "largeutf8": {
                byte[] b = ((String) raw).getBytes(StandardCharsets.UTF_8);
                if (v instanceof BaseVariableWidthVector) ((BaseVariableWidthVector) v).setSafe(at, b);
                else ((BaseLargeVariableWidthVector) v).setSafe(at, b);
                return;
            }
            case "binary": case "largebinary":
                if (v instanceof BaseVariableWidthVector) ((BaseVariableWidthVector) v).setSafe(at, (byte[]) raw);
                else ((BaseLargeVariableWidthVector) v).setSafe(at, (byte[]) raw);
                return;
            case "fixedbinary": ((FixedSizeBinaryVector) v).setSafe(at, (byte[]) raw); return;
            case "date":
                if (v instanceof DateDayVector) ((DateDayVector) v).setSafe(at, (int) (long) (Long) raw);
                else ((DateMilliVector) v).setSafe(at, (Long) raw);
                return;
            case "time":
                if (v instanceof TimeSecVector) ((TimeSecVector) v).setSafe(at, (int) (long) (Long) raw);
                else if (v instanceof TimeMilliVector) ((TimeMilliVector) v).setSafe(at, (int) (long) (Long) raw);
                else if (v instanceof TimeMicroVector) ((TimeMicroVector) v).setSafe(at, (Long) raw);
                else ((TimeNanoVector) v).setSafe(at, (Long) raw);
                return;
            case "ts": ((TimeStampVector) v).setSafe(at, (Long) raw); return;
            case "duration": ((DurationVector) v).setSafe(at, (Long) raw); return;
            case "interval":
                if (v instanceof IntervalYearVector) {
                    ((IntervalYearVector) v).setSafe(at, (int) (long) (Long) raw);
                } else if (v instanceof IntervalDayVector) {
                    long[] x = (long[]) raw;
                    ((IntervalDayVector) v).setSafe(at, (int) x[0], (int) x[1]);
                } else {
                    long[] x = (long[]) raw;
                    ((IntervalMonthDayNanoVector) v).setSafe(at, (int) x[0], (int) x[1], x[2]);
                }
                return;
            case "list": {
                ListVector lv = (ListVector) v;
                Object[] items = (Object[]) raw;
                int off = lv.startNewValue(at);
                FieldVector child = lv.getDataVector();
                for (int i = 0; i < items.length; i++) set(child, spec.substring(5), off + i, items[i]);
                lv.endValue(at, items.length);
                return;
            }
            case "largelist": {
                LargeListVector lv = (LargeListVector) v;
                Object[] items = (Object[]) raw;
                long off = lv.startNewValue(at);
                FieldVector child = lv.getDataVector();
                for (int i = 0; i < items.length; i++) set(child, spec.substring(10), (int) off + i, items[i]);
                lv.endValue(at, items.length);
                return;
            }
            case "fixedlist": {
                FixedSizeListVector lv = (FixedSizeListVector) v;
                Object[] items = (Object[]) raw;
                String childSpec = spec.substring(spec.indexOf(':', 10) + 1);
                FieldVector child = lv.getDataVector();
                for (int i = 0; i < items.length; i++) set(child, childSpec, at * items.length + i, items[i]);
                lv.setNotNull(at);
                return;
            }
            case "struct": {
                StructVector sv = (StructVector) v;
                Object[] kids = (Object[]) raw;
                set(sv.getChild("a"), "int:32:s", at, kids[0]);
                set(sv.getChild("b"), "utf8", at, kids[1]);
                sv.setIndexDefined(at);
                return;
            }
            case "map": {
                MapVector mv = (MapVector) v;
                Object[] entries = (Object[]) raw;
                int off = mv.startNewValue(at);
                StructVector sv = (StructVector) mv.getDataVector();
                for (int i = 0; i < entries.length; i++) {
                    Object[] kv = (Object[]) entries[i];
                    set(sv.getChild("key"), "utf8", off + i, kv[0]);
                    set(sv.getChild("value"), "int:32:s", off + i, kv[1]);
                    sv.setIndexDefined(off + i);
                }
                mv.endValue(at, entries.length);
                return;
            }
            default: throw new IllegalArgumentException(spec);
        }
    }
}
