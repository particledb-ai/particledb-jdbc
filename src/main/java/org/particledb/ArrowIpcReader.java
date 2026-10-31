package org.particledb;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The driver's own Arrow IPC stream reader: pure Java over a heap
 * {@code byte[]}, with no allocator, no direct memory and no reflection, so it
 * needs no JVM flag on any JDK.
 *
 * <p>It reads the flatbuffer messages in place (a minimal table reader, see
 * {@link Fb}) and pulls values straight out of the payload. Covered: Null;
 * signed and unsigned Int 8/16/32/64; Float 16/32/64; Decimal 32/64/128/256;
 * Bool; Utf8 / LargeUtf8; Binary / LargeBinary / FixedSizeBinary; Date
 * (day, millisecond); Time 32/64 (every unit); Timestamp with and without a
 * zone (every unit); Duration; Interval (year-month, day-time,
 * month-day-nano); List / LargeList / FixedSizeList; Struct; Map; and
 * dictionary-encoded fields (including delta dictionaries) at any depth.
 * Validity bitmaps and offsets are honoured; a stream it cannot read exactly
 * (an unknown or view type, a compressed body, a big-endian schema, an
 * offset or buffer outside the body) fails with an {@link IpcException}, never
 * a silently wrong value.
 *
 * <p>The Java value of each cell is what Apache Arrow Java's
 * {@code getObject} yields after the driver's normalisation, so every
 * {@link PdbResultSet} getter answers as it did through Arrow Java
 * ({@code ArrowIpcReaderDifferentialTest} pins that). Where Arrow Java's value
 * was wrong it is corrected, and the differential test lists each case:
 * unsigned integers are widened (Arrow Java returned the raw bits as a signed
 * or {@code Character} value); a half float is a {@link Float} (Arrow Java: the
 * raw {@code Short} bits); a millisecond Date is a {@link java.time.LocalDate}
 * (Arrow Java: a {@code LocalDateTime}); every Time unit is a {@link Long} of
 * microseconds of day and every zoned Timestamp a {@link Long} of epoch
 * microseconds (the units {@link PdbResultSet} reads such a number in; Arrow
 * Java returned other units raw); a list element gets the same normalisation
 * as a top-level cell (Arrow Java left a nested date as an epoch-day
 * {@code Integer} and a nested list as a {@code java.util.List}); and a
 * month-day-nano interval is its ISO-8601 text (Arrow Java returned its own
 * {@code PeriodDuration} class).
 *
 * <p>The ParticleDB server sends uncompressed, little-endian, IPC V5 streams
 * with dictionaries already decoded (the server's native-wire codec
 * {@code write_ipc_stream}); dictionary support here is for completeness.
 */
final class ArrowIpcReader {

    private ArrowIpcReader() {}

    /** A stream this reader cannot decode exactly. */
    static final class IpcException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        IpcException(String msg) { super("Arrow IPC: " + msg); }
    }

    // ── flatbuffer enums (format/Message.fbs, Schema.fbs) ─────────────────
    static final byte H_SCHEMA = 1, H_DICTIONARY = 2, H_RECORD_BATCH = 3;

    private static final int T_NULL = 1, T_INT = 2, T_FLOAT = 3, T_BINARY = 4, T_UTF8 = 5, T_BOOL = 6,
            T_DECIMAL = 7, T_DATE = 8, T_TIME = 9, T_TIMESTAMP = 10, T_INTERVAL = 11, T_LIST = 12,
            T_STRUCT = 13, T_UNION = 14, T_FIXED_BINARY = 15, T_FIXED_LIST = 16, T_MAP = 17,
            T_DURATION = 18, T_LARGE_BINARY = 19, T_LARGE_UTF8 = 20, T_LARGE_LIST = 21;

    private static final String[] TYPE_NAMES = {"NONE", "Null", "Int", "FloatingPoint", "Binary", "Utf8",
            "Bool", "Decimal", "Date", "Time", "Timestamp", "Interval", "List", "Struct_", "Union",
            "FixedSizeBinary", "FixedSizeList", "Map", "Duration", "LargeBinary", "LargeUtf8", "LargeList",
            "RunEndEncoded", "BinaryView", "Utf8View", "ListView", "LargeListView"};

    private static final String[] TIME_UNITS = {"SECOND", "MILLISECOND", "MICROSECOND", "NANOSECOND"};

    // ── flatbuffer table reader ───────────────────────────────────────────

    /**
     * A flatbuffer region of the payload, {@code [start, end)}: one IPC
     * message's metadata. Every read is bounds-checked against it.
     */
    static final class Fb {
        final byte[] b;
        final int start;
        final int end;

        Fb(byte[] b, int start, int end) {
            this.b = b;
            this.start = start;
            this.end = end;
        }

        private void check(int p, int n) {
            if (n < 0 || p < start || p > end - n) throw new IpcException("flatbuffer read outside its message");
        }

        int u8(int p) { check(p, 1); return b[p] & 0xFF; }
        int i16(int p) { check(p, 2); return (short) ((b[p] & 0xFF) | (b[p + 1] & 0xFF) << 8); }
        int u16(int p) { return i16(p) & 0xFFFF; }
        int i32(int p) {
            check(p, 4);
            return (b[p] & 0xFF) | (b[p + 1] & 0xFF) << 8 | (b[p + 2] & 0xFF) << 16 | (b[p + 3] & 0xFF) << 24;
        }
        long i64(int p) {
            check(p, 8);
            return (i32(p) & 0xFFFFFFFFL) | ((long) i32(p + 4)) << 32;
        }

        /** The root table. */
        int root() { return ref(start); }

        /** The target of the uoffset stored at {@code p}. */
        int ref(int p) {
            long t = (long) p + (i32(p) & 0xFFFFFFFFL);
            if (t >= end) throw new IpcException("flatbuffer offset outside its message");
            return (int) t;
        }

        /** Position of field {@code idx} of table {@code t}, or -1 when absent. */
        int field(int t, int idx) {
            int vt = t - i32(t);
            int vtSize = u16(vt);
            int slot = 4 + 2 * idx;
            if (slot + 2 > vtSize) return -1;
            int off = u16(vt + slot);
            return off == 0 ? -1 : t + off;
        }

        int fieldI16(int t, int idx, int dflt) { int p = field(t, idx); return p < 0 ? dflt : i16(p); }
        int fieldI32(int t, int idx, int dflt) { int p = field(t, idx); return p < 0 ? dflt : i32(p); }
        long fieldI64(int t, int idx, long dflt) { int p = field(t, idx); return p < 0 ? dflt : i64(p); }
        int fieldU8(int t, int idx, int dflt) { int p = field(t, idx); return p < 0 ? dflt : u8(p); }
        boolean fieldBool(int t, int idx) { int p = field(t, idx); return p >= 0 && u8(p) != 0; }
        /** A referenced table / vector / string, or -1 when absent. */
        int fieldRef(int t, int idx) { int p = field(t, idx); return p < 0 ? -1 : ref(p); }

        int vecLen(int v) {
            int n = i32(v);
            if (n < 0 || n > end - v) throw new IpcException("flatbuffer vector length out of range");
            return n;
        }

        /** Element {@code i} of a vector of tables. */
        int vecTable(int v, int i) { return ref(v + 4 + 4 * i); }

        String str(int s) {
            if (s < 0) return null;
            int n = i32(s);
            check(s + 4, n);
            return new String(b, s + 4, n, StandardCharsets.UTF_8);
        }
    }

    /** What a stream reader needs from one message header. */
    static final class MessageInfo {
        final byte headerType;
        final long bodyLength;
        /** Dictionary id of a DictionaryBatch, else -1. */
        final long dictionaryId;
        final boolean delta;

        MessageInfo(byte headerType, long bodyLength, long dictionaryId, boolean delta) {
            this.headerType = headerType;
            this.bodyLength = bodyLength;
            this.dictionaryId = dictionaryId;
            this.delta = delta;
        }
    }

    /** Parse the header of the message whose flatbuffer is {@code b[start, start+len)}. */
    static MessageInfo messageInfo(byte[] b, int start, int len) {
        Fb fb = new Fb(b, start, start + len);
        int m = fb.root();
        byte ht = (byte) fb.fieldU8(m, 1, 0);
        long body = fb.fieldI64(m, 3, 0);
        long id = -1;
        boolean delta = false;
        if (ht == H_DICTIONARY) {
            int h = fb.fieldRef(m, 2);
            if (h < 0) throw new IpcException("dictionary batch message without a header");
            id = fb.fieldI64(h, 0, 0);
            delta = fb.fieldBool(h, 2);
        }
        return new MessageInfo(ht, body, id, delta);
    }

    // ── schema ────────────────────────────────────────────────────────────

    /** One field of the schema, with everything a batch decode needs. */
    static final class FieldT {
        String name;
        int type;
        int bitWidth;
        boolean signed;
        int precision;
        int scale;
        int unit;
        boolean zoned;
        int byteWidth;
        int listSize;
        boolean keysSorted;
        FieldT[] children = new FieldT[0];
        Map<String, String> metadata = Collections.emptyMap();
        /** Dictionary id, or -1 when the field is not dictionary-encoded. */
        long dictId = -1;
        int indexBitWidth;
        boolean indexSigned;
        String token;
    }

    private static FieldT readField(Fb fb, int f, int depth) {
        if (depth > 64) throw new IpcException("schema nested more than 64 levels");
        FieldT ft = new FieldT();
        ft.name = fb.str(fb.fieldRef(f, 0));
        if (ft.name == null) ft.name = "";
        ft.type = fb.fieldU8(f, 2, 0);
        int t = fb.fieldRef(f, 3);
        switch (ft.type) {
            case T_NULL: case T_BINARY: case T_UTF8: case T_BOOL: case T_STRUCT:
            case T_LARGE_BINARY: case T_LARGE_UTF8: case T_LIST: case T_LARGE_LIST:
                break;
            case T_INT:
                ft.bitWidth = t < 0 ? 0 : fb.fieldI32(t, 0, 0);
                ft.signed = t >= 0 && fb.fieldBool(t, 1);
                if (ft.bitWidth != 8 && ft.bitWidth != 16 && ft.bitWidth != 32 && ft.bitWidth != 64) {
                    throw new IpcException("Int bit width " + ft.bitWidth);
                }
                break;
            case T_FLOAT:
                ft.precision = t < 0 ? 0 : fb.fieldI16(t, 0, 0);
                if (ft.precision < 0 || ft.precision > 2) throw new IpcException("FloatingPoint precision " + ft.precision);
                break;
            case T_DECIMAL:
                ft.precision = t < 0 ? 0 : fb.fieldI32(t, 0, 0);
                ft.scale = t < 0 ? 0 : fb.fieldI32(t, 1, 0);
                ft.bitWidth = t < 0 ? 128 : fb.fieldI32(t, 2, 128);
                if (ft.bitWidth != 32 && ft.bitWidth != 64 && ft.bitWidth != 128 && ft.bitWidth != 256) {
                    throw new IpcException("Decimal bit width " + ft.bitWidth);
                }
                break;
            case T_DATE:
                ft.unit = t < 0 ? 1 : fb.fieldI16(t, 0, 1); // DateUnit default MILLISECOND
                if (ft.unit != 0 && ft.unit != 1) throw new IpcException("Date unit " + ft.unit);
                break;
            case T_TIME:
                ft.unit = t < 0 ? 1 : fb.fieldI16(t, 0, 1); // TimeUnit default MILLISECOND
                ft.bitWidth = t < 0 ? 32 : fb.fieldI32(t, 1, 32);
                checkUnit(ft.unit, "Time");
                if (ft.bitWidth != (ft.unit <= 1 ? 32 : 64)) {
                    throw new IpcException("Time(" + TIME_UNITS[ft.unit] + ") with bit width " + ft.bitWidth);
                }
                break;
            case T_TIMESTAMP:
                ft.unit = t < 0 ? 0 : fb.fieldI16(t, 0, 0);
                ft.zoned = t >= 0 && fb.fieldRef(t, 1) >= 0;
                checkUnit(ft.unit, "Timestamp");
                break;
            case T_DURATION:
                ft.unit = t < 0 ? 1 : fb.fieldI16(t, 0, 1);
                checkUnit(ft.unit, "Duration");
                break;
            case T_INTERVAL:
                ft.unit = t < 0 ? 0 : fb.fieldI16(t, 0, 0);
                if (ft.unit < 0 || ft.unit > 2) throw new IpcException("Interval unit " + ft.unit);
                break;
            case T_FIXED_BINARY:
                ft.byteWidth = t < 0 ? 0 : fb.fieldI32(t, 0, 0);
                if (ft.byteWidth < 0) throw new IpcException("FixedSizeBinary width " + ft.byteWidth);
                break;
            case T_FIXED_LIST:
                ft.listSize = t < 0 ? 0 : fb.fieldI32(t, 0, 0);
                if (ft.listSize < 0) throw new IpcException("FixedSizeList size " + ft.listSize);
                break;
            case T_MAP:
                ft.keysSorted = t >= 0 && fb.fieldBool(t, 0);
                break;
            default:
                throw new IpcException("unsupported Arrow type "
                        + (ft.type < TYPE_NAMES.length ? TYPE_NAMES[ft.type] : "#" + ft.type)
                        + " in column '" + ft.name + "'");
        }
        int ch = fb.fieldRef(f, 5);
        if (ch >= 0) {
            int n = fb.vecLen(ch);
            ft.children = new FieldT[n];
            for (int i = 0; i < n; i++) ft.children[i] = readField(fb, fb.vecTable(ch, i), depth + 1);
        }
        int need = (ft.type == T_LIST || ft.type == T_LARGE_LIST || ft.type == T_FIXED_LIST || ft.type == T_MAP)
                ? 1 : -1;
        if (need == 1 && ft.children.length != 1) {
            throw new IpcException("column '" + ft.name + "' has " + ft.children.length + " children, needs 1");
        }
        if (ft.type == T_MAP && (ft.children[0].type != T_STRUCT || ft.children[0].children.length != 2)) {
            throw new IpcException("Map column '" + ft.name + "' does not hold a key/value struct");
        }
        int md = fb.fieldRef(f, 6);
        if (md >= 0) {
            int n = fb.vecLen(md);
            if (n > 0) {
                Map<String, String> m = new LinkedHashMap<>();
                for (int i = 0; i < n; i++) {
                    int kv = fb.vecTable(md, i);
                    String k = fb.str(fb.fieldRef(kv, 0));
                    String v = fb.str(fb.fieldRef(kv, 1));
                    if (k != null && v != null) m.put(k, v);
                }
                ft.metadata = Collections.unmodifiableMap(m);
            }
        }
        int dict = fb.fieldRef(f, 4);
        if (dict >= 0) {
            ft.dictId = fb.fieldI64(dict, 0, 0);
            int it = fb.fieldRef(dict, 1);
            ft.indexBitWidth = it < 0 ? 32 : fb.fieldI32(it, 0, 0);
            ft.indexSigned = it < 0 || fb.fieldBool(it, 1);
            if (ft.indexBitWidth != 8 && ft.indexBitWidth != 16 && ft.indexBitWidth != 32 && ft.indexBitWidth != 64) {
                throw new IpcException("dictionary index bit width " + ft.indexBitWidth);
            }
        }
        ft.token = token(ft);
        return ft;
    }

    private static void checkUnit(int unit, String type) {
        if (unit < 0 || unit > 3) throw new IpcException(type + " unit " + unit);
    }

    /**
     * The column's type token for {@link PdbResultSet.SimpleMeta}: the token
     * {@link ArrowDecoder#arrowTypeToken} derives from Arrow Java's type
     * rendering, except for unsigned integers, which are tokened by the
     * widened type their values are returned as.
     */
    private static String token(FieldT f) {
        switch (f.type) {
            case T_NULL: return "null";
            case T_INT:
                if (f.signed) return "int" + f.bitWidth;
                switch (f.bitWidth) {
                    case 8: return "int16";
                    case 16: return "int32";
                    case 32: return "int64";
                    default: return "decimal(20,0)";
                }
            case T_FLOAT: return f.precision == 2 ? "float64" : "float32";
            case T_DECIMAL: return "decimal(" + f.precision + "," + f.scale + ")";
            case T_UTF8: return "utf8";
            case T_LARGE_UTF8: return "largeutf8";
            case T_BINARY: case T_LARGE_BINARY: return "binary";
            case T_FIXED_BINARY: return "fixedsizebinary(" + f.byteWidth + ")";
            case T_BOOL: return "bool";
            case T_DATE: return f.unit == 0 ? "date32" : "date64";
            case T_TIME: return "time";
            case T_TIMESTAMP: return "timestamp";
            case T_DURATION: return "duration(" + TIME_UNITS[f.unit].toLowerCase(java.util.Locale.ROOT) + ")";
            case T_INTERVAL:
                return f.unit == 0 ? "interval(year_month)" : f.unit == 1 ? "interval(day_time)"
                        : "interval(month_day_nano)";
            case T_LIST: case T_LARGE_LIST: case T_FIXED_LIST: return "list";
            case T_STRUCT: return "struct";
            case T_MAP: return "map(" + f.keysSorted + ")";
            default: return "null";
        }
    }

    /** A parsed schema, cached by its exact message bytes (every result of a statement repeats it). */
    static final class Schema {
        final byte[] bytes;
        final FieldT[] fields;
        final List<String> names;
        final List<String> tokens;
        final List<Map<String, String>> metas;

        Schema(byte[] bytes, FieldT[] fields) {
            this.bytes = bytes;
            this.fields = fields;
            List<String> n = new ArrayList<>(fields.length);
            List<String> t = new ArrayList<>(fields.length);
            List<Map<String, String>> m = new ArrayList<>(fields.length);
            for (FieldT f : fields) {
                n.add(f.name);
                t.add(f.token);
                m.add(f.metadata);
            }
            this.names = Collections.unmodifiableList(n);
            this.tokens = Collections.unmodifiableList(t);
            this.metas = Collections.unmodifiableList(m);
        }
    }

    private static final int SCHEMA_CACHE_MAX = 1024;
    private static final java.util.concurrent.ConcurrentHashMap<Integer, Schema> SCHEMAS =
            new java.util.concurrent.ConcurrentHashMap<>();

    private static int hashRange(ByteBuffer bb, byte[] p, int from, int to) {
        int h = to - from;
        int i = from;
        for (; i + 8 <= to; i += 8) {
            long l = bb.getLong(i);
            h = 31 * h + (int) (l ^ (l >>> 32));
        }
        for (; i < to; i++) h = 31 * h + p[i];
        return h;
    }

    private static Schema schema(ByteBuffer bb, byte[] p, int from, int to) {
        int h = hashRange(bb, p, from, to);
        Schema s = SCHEMAS.get(h);
        if (s != null && Arrays.equals(s.bytes, 0, s.bytes.length, p, from, to)) return s;
        Fb fb = new Fb(p, from, to);
        int m = fb.root();
        int sch = fb.fieldRef(m, 2);
        if (sch < 0) throw new IpcException("schema message without a schema");
        if (fb.fieldI16(sch, 0, 0) != 0) throw new IpcException("big-endian streams are not supported");
        int fv = fb.fieldRef(sch, 1);
        int n = fv < 0 ? 0 : fb.vecLen(fv);
        FieldT[] fields = new FieldT[n];
        for (int i = 0; i < n; i++) fields[i] = readField(fb, fb.vecTable(fv, i), 0);
        s = new Schema(Arrays.copyOfRange(p, from, to), fields);
        if (SCHEMAS.size() >= SCHEMA_CACHE_MAX) SCHEMAS.clear();
        SCHEMAS.put(h, s);
        return s;
    }

    // ── stream ────────────────────────────────────────────────────────────

    /** Only the projected column names (works for a 0-batch stream). */
    static List<String> columnNames(byte[] payload) {
        ArrowDecoder.Decoded d = decode(payload, true);
        return d.columnNames;
    }

    /** Decode a whole IPC stream positionally. */
    static ArrowDecoder.Decoded decode(byte[] payload) {
        return decode(payload, false);
    }

    private static ArrowDecoder.Decoded decode(byte[] payload, boolean schemaOnly) {
        if (payload == null || payload.length == 0) return ArrowDecoder.Decoded.empty();
        ByteBuffer bb = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        int pos = 0;
        Schema schema = null;
        List<Object[]> rows = new ArrayList<>();
        Map<Long, Object[]> dicts = null;
        boolean eos = false;
        while (pos + 4 <= payload.length) {
            int len = bb.getInt(pos);
            pos += 4;
            if (len == -1) {
                if (pos + 4 > payload.length) throw new IpcException("truncated message length");
                len = bb.getInt(pos);
                pos += 4;
            }
            if (len == 0) {
                eos = true;
                break;
            }
            if (len < 0 || len > payload.length - pos) throw new IpcException("message length " + len + " out of range");
            int metaStart = pos;
            pos += len;
            MessageInfo mi = messageInfo(payload, metaStart, len);
            if (mi.bodyLength < 0 || mi.bodyLength > payload.length - pos) {
                throw new IpcException("body length " + mi.bodyLength + " out of range");
            }
            int bodyStart = pos;
            pos += (int) mi.bodyLength;
            if (mi.headerType == H_SCHEMA) {
                if (schema != null) throw new IpcException("a second schema message");
                schema = schema(bb, payload, metaStart, metaStart + len);
                if (schemaOnly) {
                    eos = true;
                    break;
                }
            } else if (mi.headerType == H_DICTIONARY) {
                if (schema == null) throw new IpcException("dictionary batch before the schema");
                FieldT valueField = dictionaryField(schema.fields, mi.dictionaryId);
                if (valueField == null) {
                    throw new IpcException("dictionary batch for id " + mi.dictionaryId + ", which no column uses");
                }
                Fb fb = new Fb(payload, metaStart, metaStart + len);
                int dictHeader = fb.fieldRef(fb.root(), 2);
                int rb = fb.fieldRef(dictHeader, 1);
                if (rb < 0) throw new IpcException("dictionary batch without data");
                if (dicts == null) dicts = new HashMap<>();
                Batch bt = new Batch(fb, rb, bb, payload, bodyStart, (int) mi.bodyLength, dicts);
                Object[] values = bt.column(valueField, true);
                // One column, whose field nodes (a nested value type has
                // several) must be exactly the batch's.
                if (bt.nodeAt != bt.nodes) {
                    throw new IpcException("dictionary batch has " + bt.nodes + " field nodes, its value type "
                            + bt.nodeAt);
                }
                Object[] old = dicts.get(mi.dictionaryId);
                if (mi.delta && old != null) {
                    Object[] joined = Arrays.copyOf(old, old.length + values.length);
                    System.arraycopy(values, 0, joined, old.length, values.length);
                    values = joined;
                }
                dicts.put(mi.dictionaryId, values);
            } else if (mi.headerType == H_RECORD_BATCH) {
                if (schema == null) throw new IpcException("record batch before the schema");
                Fb fb = new Fb(payload, metaStart, metaStart + len);
                int rb = fb.fieldRef(fb.root(), 2);
                if (rb < 0) throw new IpcException("record batch message without a header");
                Batch bt = new Batch(fb, rb, bb, payload, bodyStart, (int) mi.bodyLength, dicts);
                int ncols = schema.fields.length;
                long nrowsL = bt.length;
                if (nrowsL < 0 || nrowsL > Integer.MAX_VALUE - 8) throw new IpcException("batch length " + nrowsL);
                int nrows = (int) nrowsL;
                Object[][] cols = new Object[ncols][];
                for (int c = 0; c < ncols; c++) {
                    cols[c] = bt.column(schema.fields[c], false);
                    if (cols[c].length != nrows) {
                        throw new IpcException("column '" + schema.fields[c].name + "' has " + cols[c].length
                                + " values in a batch of " + nrows + " rows");
                    }
                }
                if (bt.nodeAt != bt.nodes) throw new IpcException("record batch has unread field nodes");
                for (int r = 0; r < nrows; r++) {
                    Object[] row = new Object[ncols];
                    for (int c = 0; c < ncols; c++) row[c] = cols[c][r];
                    rows.add(row);
                }
            } else {
                throw new IpcException("unsupported message type " + mi.headerType);
            }
        }
        // A stream may end after a whole message without the end-of-stream
        // marker (as Arrow Java's reader also allows); a partial message may not.
        if (!eos && pos != payload.length) throw new IpcException("truncated stream");
        if (schema == null) throw new IpcException("stream without a schema");
        return new ArrowDecoder.Decoded(schema.names, rows, schema.tokens, schema.metas);
    }

    private static FieldT dictionaryField(FieldT[] fields, long id) {
        for (FieldT f : fields) {
            if (f.dictId == id) return f;
            FieldT c = dictionaryField(f.children, id);
            if (c != null) return c;
        }
        return null;
    }

    // ── record batch ──────────────────────────────────────────────────────

    /** Cursor over one RecordBatch's field nodes and buffers. */
    private static final class Batch {
        final Fb fb;
        final ByteBuffer bb;
        final byte[] p;
        final int body;
        final int bodyLen;
        final long length;
        final int nodesVec;
        final int nodes;
        final int buffersVec;
        final int buffers;
        final Map<Long, Object[]> dicts;
        int nodeAt;
        int bufferAt;

        Batch(Fb fb, int rb, ByteBuffer bb, byte[] p, int body, int bodyLen, Map<Long, Object[]> dicts) {
            this.fb = fb;
            this.bb = bb;
            this.p = p;
            this.body = body;
            this.bodyLen = bodyLen;
            this.dicts = dicts;
            if (fb.field(rb, 3) >= 0) {
                throw new IpcException("compressed record batch bodies (LZ4 / ZSTD) are not supported; "
                        + "the ParticleDB server sends them uncompressed");
            }
            int vbc = fb.fieldRef(rb, 4);
            if (vbc >= 0 && fb.vecLen(vbc) > 0) throw new IpcException("variadic (view) buffers are not supported");
            this.length = fb.fieldI64(rb, 0, 0);
            this.nodesVec = fb.fieldRef(rb, 1);
            this.nodes = nodesVec < 0 ? 0 : fb.vecLen(nodesVec);
            this.buffersVec = fb.fieldRef(rb, 2);
            this.buffers = buffersVec < 0 ? 0 : fb.vecLen(buffersVec);
            if (nodesVec >= 0) fb.check(nodesVec + 4, 16 * nodes);
            if (buffersVec >= 0) fb.check(buffersVec + 4, 16 * buffers);
        }

        /** Next field node: {length, nullCount}. */
        long[] node() {
            if (nodeAt >= nodes) throw new IpcException("record batch has too few field nodes");
            int at = nodesVec + 4 + 16 * nodeAt++;
            long len = fb.i64(at);
            long nulls = fb.i64(at + 8);
            if (len < 0 || len > Integer.MAX_VALUE - 8 || nulls < 0 || nulls > len) {
                throw new IpcException("field node length " + len + " / null count " + nulls);
            }
            return new long[] {len, nulls};
        }

        /** Next buffer as {absolute offset, length}. */
        int[] buffer() {
            if (bufferAt >= buffers) throw new IpcException("record batch has too few buffers");
            int at = buffersVec + 4 + 16 * bufferAt++;
            long off = fb.i64(at);
            long len = fb.i64(at + 8);
            // No off + len: it can overflow past a bound check.
            if (off < 0 || len < 0 || off > bodyLen || len > bodyLen - off) {
                throw new IpcException("buffer outside the message body");
            }
            return new int[] {body + (int) off, (int) len};
        }

        /** Validity of {@code n} rows, or null when all are valid. */
        boolean[] validity(int[] buf, int n, long nulls) {
            if (nulls == 0) return null;
            if (buf[1] * 8L < n) throw new IpcException("validity bitmap shorter than its column");
            boolean[] v = new boolean[n];
            for (int i = 0; i < n; i++) v[i] = ((p[buf[0] + (i >> 3)] >> (i & 7)) & 1) != 0;
            return v;
        }

        /**
         * A column with no data to bound its length (Null, a childless Struct,
         * FixedSizeBinary(0), FixedSizeList(0)) has nothing to
         * check its length against; cap it so a corrupt length cannot
         * allocate gigabytes (16M rows, or more with a larger body).
         */
        void plausible(FieldT f, int n) {
            if (n > Math.max(1 << 24, 16L * bodyLen)) {
                throw new IpcException("column '" + f.name + "' claims " + n + " values");
            }
        }

        void need(int[] buf, long bytes) {
            if (buf[1] < bytes) throw new IpcException("buffer of " + buf[1] + " bytes, needs " + bytes);
        }

        /**
         * Decode the next array of field {@code f}. {@code asValue}: read the
         * field as its value type even when it is dictionary-encoded (the
         * dictionary batch itself).
         */
        Object[] column(FieldT f, boolean asValue) {
            long[] node = node();
            int n = (int) node[0];
            long nulls = node[1];
            if (f.dictId >= 0 && !asValue) {
                boolean[] valid = validity(buffer(), n, nulls);
                int[] vb = buffer();
                int w = f.indexBitWidth / 8;
                need(vb, (long) n * w);
                Object[] dict = dicts == null ? null : dicts.get(f.dictId);
                if (dict == null) {
                    throw new IpcException("column '" + f.name + "' is dictionary-encoded (id " + f.dictId
                            + ") but the stream carries no such dictionary");
                }
                Object[] out = new Object[n];
                for (int i = 0; i < n; i++) {
                    if (valid != null && !valid[i]) continue;
                    int at = vb[0] + i * w;
                    long idx;
                    switch (w) {
                        case 1: idx = f.indexSigned ? p[at] : p[at] & 0xFF; break;
                        case 2: idx = f.indexSigned ? bb.getShort(at) : bb.getShort(at) & 0xFFFF; break;
                        case 4: idx = f.indexSigned ? bb.getInt(at) : bb.getInt(at) & 0xFFFFFFFFL; break;
                        default: idx = bb.getLong(at); break;
                    }
                    if (idx < 0 || idx >= dict.length) {
                        throw new IpcException("dictionary index " + idx + " outside a dictionary of "
                                + dict.length + " values");
                    }
                    // A dictionary value is shared by every row that indexes
                    // it; give each row its own copy of a mutable one.
                    out[i] = copyMutable(dict[(int) idx]);
                }
                return out;
            }
            switch (f.type) {
                case T_NULL:
                    plausible(f, n);
                    return new Object[n];
                case T_STRUCT: {
                    if (f.children.length == 0) plausible(f, n);
                    boolean[] valid = validity(buffer(), n, nulls);
                    Object[][] kids = new Object[f.children.length][];
                    for (int c = 0; c < kids.length; c++) {
                        kids[c] = column(f.children[c], false);
                        if (kids[c].length < n) throw new IpcException("struct child shorter than its struct");
                    }
                    Object[] out = new Object[n];
                    for (int i = 0; i < n; i++) {
                        if (valid != null && !valid[i]) continue;
                        // Arrow Java's StructVector.getObject: a LinkedHashMap
                        // of the non-null children.
                        Map<String, Object> m = new LinkedHashMap<>();
                        for (int c = 0; c < kids.length; c++) {
                            if (kids[c][i] != null) m.put(f.children[c].name, kids[c][i]);
                        }
                        out[i] = m;
                    }
                    return out;
                }
                case T_LIST: case T_LARGE_LIST: case T_MAP: {
                    boolean[] valid = validity(buffer(), n, nulls);
                    int[] ob = buffer();
                    boolean large = f.type == T_LARGE_LIST;
                    int ow = large ? 8 : 4;
                    if (n > 0) need(ob, (long) (n + 1) * ow);
                    Object[] child = column(f.children[0], false);
                    Object[] out = new Object[n];
                    for (int i = 0; i < n; i++) {
                        if (valid != null && !valid[i]) continue;
                        long s = large ? bb.getLong(ob[0] + 8 * i) : bb.getInt(ob[0] + 4 * i);
                        long e = large ? bb.getLong(ob[0] + 8 * (i + 1)) : bb.getInt(ob[0] + 4 * (i + 1));
                        if (s < 0 || e < s || e > child.length) throw new IpcException("list offsets out of range");
                        out[i] = Arrays.copyOfRange(child, (int) s, (int) e);
                    }
                    return out;
                }
                case T_FIXED_LIST: {
                    if (f.listSize == 0) plausible(f, n);
                    boolean[] valid = validity(buffer(), n, nulls);
                    Object[] child = column(f.children[0], false);
                    int k = f.listSize;
                    if ((long) n * k > child.length) throw new IpcException("fixed-size list child too short");
                    Object[] out = new Object[n];
                    for (int i = 0; i < n; i++) {
                        if (valid != null && !valid[i]) continue;
                        out[i] = Arrays.copyOfRange(child, i * k, i * k + k);
                    }
                    return out;
                }
                case T_UTF8: case T_BINARY: case T_LARGE_UTF8: case T_LARGE_BINARY: {
                    boolean[] valid = validity(buffer(), n, nulls);
                    int[] ob = buffer();
                    int[] db = buffer();
                    boolean large = f.type == T_LARGE_UTF8 || f.type == T_LARGE_BINARY;
                    boolean text = f.type == T_UTF8 || f.type == T_LARGE_UTF8;
                    int ow = large ? 8 : 4;
                    if (n > 0) need(ob, (long) (n + 1) * ow);
                    Object[] out = new Object[n];
                    for (int i = 0; i < n; i++) {
                        if (valid != null && !valid[i]) continue;
                        long s = large ? bb.getLong(ob[0] + 8 * i) : bb.getInt(ob[0] + 4 * i);
                        long e = large ? bb.getLong(ob[0] + 8 * (i + 1)) : bb.getInt(ob[0] + 4 * (i + 1));
                        if (s < 0 || e < s || e > db[1]) throw new IpcException("value offsets out of range");
                        int from = db[0] + (int) s;
                        int len = (int) (e - s);
                        out[i] = text ? new String(p, from, len, StandardCharsets.UTF_8)
                                : Arrays.copyOfRange(p, from, from + len);
                    }
                    return out;
                }
                case T_BOOL: {
                    boolean[] valid = validity(buffer(), n, nulls);
                    int[] vb = buffer();
                    need(vb, (n + 7L) / 8);
                    Object[] out = new Object[n];
                    for (int i = 0; i < n; i++) {
                        if (valid != null && !valid[i]) continue;
                        out[i] = ((p[vb[0] + (i >> 3)] >> (i & 7)) & 1) != 0;
                    }
                    return out;
                }
                default: {
                    boolean[] valid = validity(buffer(), n, nulls);
                    int[] vb = buffer();
                    int w = width(f);
                    if (w == 0) plausible(f, n); // FixedSizeBinary(0): nothing to bound n by
                    need(vb, (long) n * w);
                    Object[] out = new Object[n];
                    for (int i = 0; i < n; i++) {
                        if (valid != null && !valid[i]) continue;
                        out[i] = fixed(f, vb[0] + i * w);
                    }
                    return out;
                }
            }
        }

        /** A deep copy of a decoded value's mutable parts (arrays, maps); immutable values as they are. */
        private Object copyMutable(Object v) {
            if (v instanceof byte[]) return ((byte[]) v).clone();
            if (v instanceof Object[]) {
                Object[] a = ((Object[]) v).clone();
                for (int k = 0; k < a.length; k++) a[k] = copyMutable(a[k]);
                return a;
            }
            if (v instanceof Map) {
                Map<String, Object> m = new LinkedHashMap<>();
                for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) m.put((String) e.getKey(), copyMutable(e.getValue()));
                return m;
            }
            return v;
        }

        private Object fixed(FieldT f, int at) {
            switch (f.type) {
                case T_INT:
                    switch (f.bitWidth) {
                        case 8: return f.signed ? (Object) p[at] : (Object) (short) (p[at] & 0xFF);
                        case 16: return f.signed ? (Object) bb.getShort(at) : (Object) (bb.getShort(at) & 0xFFFF);
                        case 32: return f.signed ? (Object) bb.getInt(at) : (Object) (bb.getInt(at) & 0xFFFFFFFFL);
                        default: {
                            long v = bb.getLong(at);
                            if (f.signed) return v;
                            return v >= 0 ? BigDecimal.valueOf(v)
                                    : new BigDecimal(new BigInteger(Long.toUnsignedString(v)));
                        }
                    }
                case T_FLOAT:
                    if (f.precision == 2) return bb.getDouble(at);
                    if (f.precision == 1) return bb.getFloat(at);
                    return halfToFloat(bb.getShort(at));
                case T_DECIMAL:
                    return decimal(at, f.bitWidth / 8, f.scale);
                case T_DATE:
                    if (f.unit == 0) return java.time.LocalDate.ofEpochDay(bb.getInt(at));
                    return java.time.LocalDate.ofEpochDay(Math.floorDiv(bb.getLong(at), 86_400_000L));
                case T_TIME: {
                    long v = f.bitWidth == 32 ? bb.getInt(at) : bb.getLong(at);
                    return toMicros(v, f.unit);
                }
                case T_TIMESTAMP: {
                    long v = bb.getLong(at);
                    if (f.zoned) return toMicros(v, f.unit);
                    return localDateTime(v, f.unit);
                }
                case T_DURATION: {
                    long v = bb.getLong(at);
                    switch (f.unit) {
                        case 0: return java.time.Duration.ofSeconds(v);
                        case 1: return java.time.Duration.ofMillis(v);
                        case 2: return java.time.Duration.ofNanos(Math.multiplyExact(v, 1000L));
                        default: return java.time.Duration.ofNanos(v);
                    }
                }
                case T_INTERVAL:
                    if (f.unit == 0) return java.time.Period.ofMonths(bb.getInt(at));
                    if (f.unit == 1) {
                        return java.time.Duration.ofDays(bb.getInt(at)).plusMillis(bb.getInt(at + 4));
                    }
                    return monthDayNano(bb.getInt(at), bb.getInt(at + 4), bb.getLong(at + 8));
                case T_FIXED_BINARY:
                    return Arrays.copyOfRange(p, at, at + f.byteWidth);
                default:
                    throw new IpcException("type " + TYPE_NAMES[f.type] + " is not fixed-width");
            }
        }

        private Object decimal(int at, int w, int scale) {
            if (w == 4) return BigDecimal.valueOf(bb.getInt(at), scale);
            if (w == 8) return BigDecimal.valueOf(bb.getLong(at), scale);
            if (w == 16) {
                long lo = bb.getLong(at);
                long hi = bb.getLong(at + 8);
                if (hi == (lo >> 63)) return BigDecimal.valueOf(lo, scale);
            }
            byte[] be = new byte[w];
            for (int i = 0; i < w; i++) be[i] = p[at + w - 1 - i];
            return new BigDecimal(new BigInteger(be), scale);
        }
    }

    private static int width(FieldT f) {
        switch (f.type) {
            case T_INT: return f.bitWidth / 8;
            case T_FLOAT: return f.precision == 0 ? 2 : f.precision == 1 ? 4 : 8;
            case T_DECIMAL: return f.bitWidth / 8;
            case T_DATE: return f.unit == 0 ? 4 : 8;
            case T_TIME: return f.bitWidth / 8;
            case T_TIMESTAMP: case T_DURATION: return 8;
            case T_INTERVAL: return f.unit == 0 ? 4 : f.unit == 1 ? 8 : 16;
            case T_FIXED_BINARY: return f.byteWidth;
            default: throw new IpcException("type " + TYPE_NAMES[f.type] + " is not fixed-width");
        }
    }

    private static long toMicros(long v, int unit) {
        switch (unit) {
            case 0: return Math.multiplyExact(v, 1_000_000L);
            case 1: return Math.multiplyExact(v, 1000L);
            case 2: return v;
            default: return Math.floorDiv(v, 1000L);
        }
    }

    /** Arrow Java's {@code DateUtility.getLocalDateTimeFromEpoch*}: the UTC wall clock. */
    private static java.time.LocalDateTime localDateTime(long v, int unit) {
        long sec;
        long nanos;
        switch (unit) {
            case 0: sec = v; nanos = 0; break;
            case 1: sec = Math.floorDiv(v, 1000L); nanos = Math.floorMod(v, 1000L) * 1_000_000L; break;
            case 2: sec = Math.floorDiv(v, 1_000_000L); nanos = Math.floorMod(v, 1_000_000L) * 1000L; break;
            default: sec = Math.floorDiv(v, 1_000_000_000L); nanos = Math.floorMod(v, 1_000_000_000L); break;
        }
        return java.time.LocalDateTime.ofEpochSecond(sec, (int) nanos, java.time.ZoneOffset.UTC);
    }

    /** ISO-8601 text of a month-day-nano interval: the period, then the time part. */
    static String monthDayNano(int months, int days, long nanos) {
        String period = java.time.Period.of(0, months, days).toString();
        java.time.Duration d = java.time.Duration.ofNanos(nanos);
        return d.isZero() ? period : period + d.toString().substring(1);
    }

    static float halfToFloat(short h) {
        int bits = h & 0xFFFF;
        int sign = (bits >>> 15) << 31;
        int exp = (bits >>> 10) & 0x1F;
        int mant = bits & 0x3FF;
        if (exp == 0x1F) return Float.intBitsToFloat(sign | 0x7F800000 | (mant << 13));
        if (exp == 0) {
            if (mant == 0) return Float.intBitsToFloat(sign);
            float v = mant / 1024f * (float) Math.pow(2, -14);
            return sign != 0 ? -v : v;
        }
        return Float.intBitsToFloat(sign | ((exp - 15 + 127) << 23) | (mant << 13));
    }
}
