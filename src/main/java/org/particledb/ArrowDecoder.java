package org.particledb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Arrow IPC stream decoder: the entry point every result goes through.
 *
 * <p>By default ({@code resultDecoder=auto} or {@code builtin}) results are
 * read by the driver's own pure-Java reader, {@link ArrowIpcReader}, which
 * covers every column type the server sends and needs no JVM flag and no
 * Apache Arrow Java on any JDK 11 or later.
 *
 * <p>{@code resultDecoder=arrow} decodes with Apache Arrow Java instead
 * ({@link ArrowJavaDecoder}). Arrow Java is an OPTIONAL dependency: the
 * application adds it, and on JDK 16 and later passes {@link #ADD_OPENS_FLAG}.
 * If it is missing, or the decode fails, the result raises a
 * {@link RuntimeException} with an actionable message; nothing ever falls
 * back to a raw-bytes row.
 */
public final class ArrowDecoder {

    private static final boolean ARROW_PRESENT;
    static {
        boolean present = false;
        try {
            Class.forName("org.apache.arrow.vector.ipc.ArrowStreamReader");
            present = true;
        } catch (ClassNotFoundException e) {
            // optional dep absent - decode() will throw with an actionable msg
        }
        ARROW_PRESENT = present;
    }

    private ArrowDecoder() {}

    /**
     * Positional decode result: the projected column names (in projection
     * order, DUPLICATES PRESERVED) plus each row as an {@code Object[]} indexed
     * by column position. This is the authoritative shape - a name-keyed map
     * would collapse duplicate output labels (e.g. {@code SELECT 1 AS x, 2 AS x}
     * or a self-join), losing the second column (finding F18).
     */
    public static final class Decoded {
        public final List<String> columnNames;
        public final List<Object[]> rows;
        /**
         * Per-column Arrow logical type token (projection order, duplicates
         * preserved). Derived from the Arrow schema's {@code Field.getType()} so
         * it is known even for a 0-row result. Consumed by
         * {@link PdbResultSet.SimpleMeta} to report the JDBC
         * {@code getColumnType}/{@code getColumnTypeName}/{@code getPrecision}/
         * {@code getScale}/{@code getColumnClassName} contract. Tokens:
         * {@code int8,int16,int32,int64,float32,float64,decimal(p,s),utf8,
         * largeutf8,binary,bool,date32,date64,timestamp,time,list,null}.
         */
        public final List<String> columnTypes;
        /**
         * Per-column Arrow field CUSTOM METADATA (projection order, duplicates
         * preserved); empty map when a field carries none.
         *
         * <p>The engine stamps the column's DECLARED SQL type here under
         * {@code pdb:decl} (see the server's scan operator
         * {@code field_with_decl}), and Arrow IPC carries per-field
         * {@code custom_metadata} through to the client. This is the ONLY
         * channel that survives for a type the engine materializes as Arrow
         * {@code Utf8} - notably a SQL ARRAY, whose element type is otherwise
         * unrecoverable because the cell is just {@code ARRAY[…]} text
         * (physically identical to a plain text column). {@link PdbResultSet}
         * consumes it so {@code getArray()} can return a correctly typed Java
         * array instead of guessing from the values.
         */
        public final List<Map<String, String>> columnMetadata;

        public Decoded(List<String> columnNames, List<Object[]> rows) {
            this(columnNames, rows, Collections.emptyList());
        }

        public Decoded(List<String> columnNames, List<Object[]> rows, List<String> columnTypes) {
            this(columnNames, rows, columnTypes, Collections.emptyList());
        }

        public Decoded(List<String> columnNames, List<Object[]> rows, List<String> columnTypes,
                       List<Map<String, String>> columnMetadata) {
            this.columnNames = columnNames;
            this.rows = rows;
            this.columnMetadata = columnMetadata == null
                    ? Collections.<Map<String, String>>emptyList() : columnMetadata;
            this.columnTypes = numericTextColumns(columnTypes, this.columnMetadata, rows);
        }

        /**
         * Under {@code CAP_NUMERIC_TEXT} (negotiated by default) the server
         * sends an UNCONSTRAINED {@code numeric} column as per-value-scale text
         * (Arrow {@code Utf8}, its {@code pdb:decl} stamp kept) instead of a
         * Decimal array padded to the batch's largest scale -- {@code 0.52}
         * stays {@code 0.52}, as PostgreSQL renders it, instead of
         * {@code 0.520000}. Turn such a column back into what it is: its
         * values become {@link java.math.BigDecimal} (scale as sent) and its
         * type token {@code decimal}, so getObject / getColumnType report
         * NUMERIC exactly as for a Decimal column. Rewrites in place; the
         * token list is copied only when a column changes.
         *
         * <p>A constrained {@code numeric(p,s)} column also arrives as text
         * when a batch holds a value no Decimal array can carry ({@code NaN}),
         * so any numeric declaration is recognised here; its token becomes
         * {@code decimal(p,s)}, as for the Decimal array of a batch without
         * one. {@code NaN}, {@code Infinity} and {@code -Infinity} have no
         * BigDecimal: each becomes the {@link Double} of the same value, which
         * is what pgjdbc's {@code getObject} returns for them.
         */
        private static List<String> numericTextColumns(List<String> types, List<Map<String, String>> metas,
                                                       List<Object[]> rows) {
            if (types == null || types.isEmpty() || metas.isEmpty()) return types;
            List<String> out = null;
            for (int c = 0; c < types.size() && c < metas.size(); c++) {
                String t = types.get(c);
                Map<String, String> m = metas.get(c);
                String decl = m == null ? null : m.get("pdb:decl");
                if ("binary".equals(t) && decl != null && isVectorDecl(decl)) {
                    // A vector column: little-endian float32 components.
                    for (Object[] r : rows) {
                        if (c < r.length && r[c] instanceof byte[] && ((byte[]) r[c]).length % 4 == 0) {
                            r[c] = PdbVector.fromWire((byte[]) r[c]);
                        }
                    }
                    continue;
                }
                if (!"utf8".equals(t) && !"largeutf8".equals(t)) continue;
                String token = decl == null ? null : numericDeclToken(decl);
                if (token == null) continue;
                for (Object[] r : rows) {
                    if (c < r.length && r[c] instanceof String) {
                        String text = ((String) r[c]).trim();
                        Double special = numericSpecial(text);
                        if (special != null) {
                            r[c] = special;
                            continue;
                        }
                        try {
                            r[c] = new java.math.BigDecimal(text);
                        } catch (NumberFormatException e) {
                            // Not a numeric text at all: keep it as sent.
                        }
                    }
                }
                if (out == null) out = new java.util.ArrayList<>(types);
                out.set(c, token);
            }
            return out == null ? types : Collections.unmodifiableList(out);
        }

        /** The server's {@code field_is_vector_decl}: {@code vector}, {@code vector(n)}, {@code vector n}. */
        static boolean isVectorDecl(String decl) {
            String d = decl.trim().toLowerCase(java.util.Locale.ROOT);
            return d.equals("vector") || d.startsWith("vector(") || d.startsWith("vector ");
        }

        private static final java.util.regex.Pattern CONSTRAINED_NUMERIC = java.util.regex.Pattern.compile(
                "(?i)\\s*(?:numeric|decimal|dec)\\s*\\(\\s*(\\d+)\\s*(?:,\\s*(-?\\d+)\\s*)?\\)\\s*");

        /**
         * The type token a numeric declaration reads as: {@code decimal} for an
         * unconstrained one, {@code decimal(p,s)} for {@code numeric(p,s)} /
         * {@code numeric(p)}, null for any other declaration.
         */
        static String numericDeclToken(String decl) {
            if (isUnconstrainedNumericDecl(decl)) return "decimal";
            java.util.regex.Matcher m = CONSTRAINED_NUMERIC.matcher(decl);
            if (!m.matches()) return null;
            return "decimal(" + m.group(1) + "," + (m.group(2) == null ? "0" : m.group(2)) + ")";
        }

        /**
         * The {@link Double} a PostgreSQL numeric special's text stands for
         * ({@code NaN}, {@code Infinity}, {@code -Infinity}, as PostgreSQL
         * renders them), or null for any other text.
         */
        static Double numericSpecial(String text) {
            switch (text) {
                case "NaN": return Double.NaN;
                case "Infinity": return Double.POSITIVE_INFINITY;
                case "-Infinity": return Double.NEGATIVE_INFINITY;
                default: return null;
            }
        }

        /** The server's {@code field_is_unconstrained_numeric} declared-type names. */
        static boolean isUnconstrainedNumericDecl(String decl) {
            switch (decl.trim().toLowerCase(java.util.Locale.ROOT)) {
                case "numeric": case "decimal": case "dec": case "bignumeric": case "bigdecimal": return true;
                default: return false;
            }
        }

        /**
         * Custom metadata for a 1-based column, never null. Returns an empty map
         * for an out-of-range column or a field with no metadata.
         */
        public Map<String, String> metadataFor(int oneBasedColumn) {
            int i = oneBasedColumn - 1;
            if (i < 0 || i >= columnMetadata.size()) return Collections.emptyMap();
            Map<String, String> m = columnMetadata.get(i);
            return m == null ? Collections.<String, String>emptyMap() : m;
        }

        static Decoded empty() {
            return new Decoded(Collections.emptyList(), Collections.emptyList(),
                    Collections.emptyList(), Collections.emptyList());
        }
    }

    /**
     * Decode an Arrow IPC payload POSITIONALLY. Column names come from the Arrow
     * schema (so they are known even for a 0-row result - finding F7), and each
     * row is an {@code Object[]} so duplicate output labels stay individually
     * addressable by index (finding F18).
     */
    public static Decoded decodePositional(byte[] payload) {
        return decodePositional(payload, ResultDecoder.AUTO);
    }

    /**
     * The {@code resultDecoder} connection property. Parsed once, at connect
     * time, so a bad value fails before any socket is opened.
     */
    public enum ResultDecoder {
        /** The default: the driver's built-in reader ({@link ArrowIpcReader}) for every column type. */
        AUTO,
        /**
         * Every result through Apache Arrow Java, which the application adds to
         * its classpath and which needs {@link #ADD_OPENS_FLAG} on JDK 16+.
         */
        ARROW,
        /** The built-in reader, named explicitly (the same decoder as {@link #AUTO}). */
        BUILTIN;

        public static final String PROPERTY = "resultDecoder";
        public static final String VALID_VALUES = "auto, arrow, builtin";

        /** {@code null} or empty means {@link #AUTO}; matching ignores case. */
        public static ResultDecoder parse(String value) throws java.sql.SQLException {
            if (value == null || value.isEmpty()) return AUTO;
            switch (value.trim().toLowerCase(java.util.Locale.ROOT)) {
                case "auto": return AUTO;
                case "arrow": return ARROW;
                case "builtin": return BUILTIN;
                default:
                    throw new java.sql.SQLException("ParticleDB JDBC: invalid " + PROPERTY + " '" + value
                            + "'; valid values are " + VALID_VALUES + " (default auto)", "22023");
            }
        }
    }

    /** {@link #decodePositional(byte[])} with the decoder the connection selected. */
    public static Decoded decodePositional(byte[] payload, ResultDecoder mode) {
        if (payload == null || payload.length == 0) return Decoded.empty();
        if (mode == ResultDecoder.ARROW) return decodeWithArrowJava(payload);
        try {
            return ArrowIpcReader.decode(payload);
        } catch (RuntimeException e) {
            // Never a raw-bytes row: the application would see byte[] where it
            // asked for an int.
            throw new RuntimeException("ParticleDB JDBC: Arrow IPC decode failed ("
                    + payload.length + " bytes): " + e.getMessage(), e);
        }
    }

    private static Decoded decodeWithArrowJava(byte[] payload) {
        if (!ARROW_PRESENT) {
            throw new RuntimeException("ParticleDB JDBC: " + ResultDecoder.PROPERTY + "=arrow, but Apache "
                    + "Arrow Java is not on the classpath. Add org.apache.arrow:arrow-vector and "
                    + "org.apache.arrow:arrow-memory-netty (and on JDK 16 and later the JVM option "
                    + ADD_OPENS_FLAG + "), or use " + ResultDecoder.PROPERTY + "=auto, which needs neither.");
        }
        try {
            return ArrowJavaDecoder.decode(payload);
        } catch (Throwable t) {
            if (isLikelyReflectionAccessFailure(t)
                    || (Runtime.version().feature() >= 16 && !javaNioOpenToArrow())) {
                throw new RuntimeException(moduleFlagMessage(payload.length), t);
            }
            throw new RuntimeException(
                    "ParticleDB JDBC: Arrow IPC decode with Apache Arrow Java failed ("
                    + payload.length + " bytes)", t);
        }
    }

    /**
     * Name-keyed decode, retained for backward compatibility. Prefer
     * {@link #decodePositional(byte[])} - a {@code Map} collapses duplicate
     * output column labels (F18). Built by delegating to the positional decode.
     */
    public static List<Map<String, Object>> decode(byte[] payload) {
        Decoded d = decodePositional(payload);
        if (d.rows.isEmpty()) return Collections.emptyList();
        List<Map<String, Object>> out = new ArrayList<>(d.rows.size());
        for (Object[] row : d.rows) {
            Map<String, Object> m = new LinkedHashMap<>();
            for (int i = 0; i < d.columnNames.size(); i++) {
                m.put(d.columnNames.get(i), i < row.length ? row[i] : null);
            }
            out.add(m);
        }
        return out;
    }

    /**
     * Read ONLY the projected column names from an Arrow IPC payload. Works even
     * when the payload carries zero record batches (the Arrow stream still
     * carries the schema), so a 0-row SELECT can still report its real projected
     * columns (finding F7). Returns an empty list on any error / when Arrow Java
     * is absent - the caller degrades to an unnamed/empty column list rather
     * than failing.
     */
    public static List<String> decodeColumnNames(byte[] payload) {
        if (payload == null || payload.length == 0) return Collections.emptyList();
        try {
            return ArrowIpcReader.columnNames(payload);
        } catch (RuntimeException e) {
            return Collections.emptyList();
        }
    }

    /**
     * The JVM option Apache Arrow Java needs on JDK 16 and later. Only
     * {@code resultDecoder=arrow} uses Arrow Java; the default decoder never
     * needs this option, and passing it anyway is harmless.
     */
    public static final String ADD_OPENS_FLAG = "--add-opens=java.base/java.nio=ALL-UNNAMED";

    static String moduleFlagMessage(int payloadBytes) {
        return "ParticleDB JDBC: " + ResultDecoder.PROPERTY + "=arrow decodes results with Apache "
                + "Arrow Java, which on JDK 16 and later requires the JVM option " + ADD_OPENS_FLAG
                + " . Add it to the java command line (or JDK_JAVA_OPTIONS), or use "
                + ResultDecoder.PROPERTY + "=auto, which needs no JVM option. (Arrow IPC decode failed, "
                + payloadBytes + " bytes)";
    }

    /**
     * Whether java.base opens java.nio to the module Arrow runs in (the unnamed
     * module on a classpath). JDK 11-15 permit the access anyway, with a
     * warning, so a closed answer is only used once a decode has already failed.
     */
    static boolean javaNioOpenToArrow() {
        try {
            Module arrow = Class.forName("org.apache.arrow.memory.util.MemoryUtil", false,
                    ArrowDecoder.class.getClassLoader()).getModule();
            return java.nio.Buffer.class.getModule().isOpen("java.nio", arrow);
        } catch (Throwable t) {
            return true; // cannot tell: do not claim a missing flag
        }
    }

    private static boolean isLikelyReflectionAccessFailure(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            String msg = c.getMessage();
            if (msg == null) continue;
            if (msg.contains("module java.base does not")
                    || msg.contains("cannot access class")
                    || msg.contains("InaccessibleObjectException")
                    || msg.contains("DirectByteBuffer")
                    || msg.contains("org.apache.arrow.memory.util.MemoryUtil")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Map an Arrow {@code ArrowType} (obtained via {@code Field.getType()}) to a
     * compact lowercase token consumed by {@link PdbResultSet.SimpleMeta}. Uses
     * the type's {@code toString()} rendering (stable across Arrow 15.x) so the
     * driver needs no compile-time Arrow dependency. Decimal precision/scale are
     * embedded as {@code decimal(p,s)}.
     */
    /** Tokens by ArrowType (value-equal); a result's columns repeat the same few types. */
    private static final java.util.concurrent.ConcurrentHashMap<Object, String> TYPE_TOKENS =
            new java.util.concurrent.ConcurrentHashMap<>();

    static String arrowTypeToken(Object arrowType) {
        if (arrowType == null) return "null";
        String cached = TYPE_TOKENS.get(arrowType);
        if (cached != null) return cached;
        String t = computeArrowTypeToken(arrowType);
        if (TYPE_TOKENS.size() < 4096) TYPE_TOKENS.put(arrowType, t);
        return t;
    }

    private static String computeArrowTypeToken(Object arrowType) {
        String s = arrowType.toString().toLowerCase();
        // Arrow's ArrowType.toString() renders e.g.:
        //   Int(32, true) / Int(64, true) / FloatingPoint(SINGLE|DOUBLE) /
        //   Utf8 / LargeUtf8 / Binary / Bool / Date(DAY|MILLISECOND) /
        //   Timestamp(MICROSECOND, null) / Time(...) / Decimal(38, 9, 128) /
        //   List / Null
        if (s.startsWith("int(")) {
            if (s.contains("8,")) return "int8";
            if (s.contains("16,")) return "int16";
            if (s.contains("32,")) return "int32";
            if (s.contains("64,")) return "int64";
            return "int64";
        }
        if (s.startsWith("floatingpoint")) {
            return s.contains("single") ? "float32" : "float64";
        }
        if (s.startsWith("decimal")) {
            // Decimal(precision, scale[, bitWidth]) → decimal(p,s)
            int lp = s.indexOf('('), rp = s.indexOf(')');
            if (lp >= 0 && rp > lp) {
                String[] parts = s.substring(lp + 1, rp).split(",");
                if (parts.length >= 2) {
                    return "decimal(" + parts[0].trim() + "," + parts[1].trim() + ")";
                }
            }
            return "decimal(38,9)";
        }
        if (s.startsWith("largeutf8")) return "largeutf8";
        if (s.startsWith("utf8")) return "utf8";
        if (s.startsWith("largebinary")) return "binary";
        if (s.startsWith("binary")) return "binary";
        if (s.startsWith("bool")) return "bool";
        if (s.startsWith("date")) return s.contains("milli") ? "date64" : "date32";
        if (s.startsWith("timestamp")) return "timestamp";
        if (s.startsWith("time")) return "time";
        if (s.startsWith("list") || s.startsWith("largelist") || s.startsWith("fixedsizelist")) {
            return "list";
        }
        if (s.startsWith("null")) return "null";
        return s;
    }

    /**
     * For tests / debugging: detect Arrow IPC magic and report payload size.
     */
    public static String describePayload(byte[] payload) {
        if (payload == null || payload.length == 0) return "<empty>";
        ByteBuffer bb = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        int first = bb.getInt(0);
        return String.format(
                "ArrowIPC payload: %d bytes, first u32=0x%08x (continuation=0x%s)",
                payload.length, first,
                first == -1 ? "yes" : "no");
    }

    static {
        // suppress "unused" warning for utility constants
        @SuppressWarnings("unused")
        String _placeholder = StandardCharsets.UTF_8.name();
    }
}
