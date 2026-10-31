package org.particledb;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The golden Arrow IPC cases, one single-column stream per Arrow type, with
 * the raw cell values ({@link ArrowGolden} writes them with Apache Arrow Java
 * into {@code src/test/resources/arrow-golden/<name>.arrow}) and the exact Java
 * value {@link ArrowIpcReader} must return for each cell. Arrow-free, so the
 * golden test runs on a plain JVM.
 *
 * <p>{@code divergent} marks the cases where the built-in reader deliberately
 * answers differently from Arrow Java's {@code getObject} (see
 * {@link ArrowIpcReader}'s class comment); {@code ArrowIpcReaderDifferentialTest}
 * requires every other case to match Arrow Java exactly, and every divergent
 * case to really differ. Float16 is a third kind: Arrow Java 15 has no
 * vector for it and cannot read it at all, so 1.0.9 failed on such a column.
 */
final class GoldenCases {
    private GoldenCases() {}

    static final class Case {
        final String name;
        /** Arrow type spec, read by {@link ArrowGolden}. */
        final String type;
        final String token;
        final Object[] raw;
        final Object[] expected;
        final boolean divergent;
        /** Apache Arrow Java 15 cannot read this type at all (the stream is hand-built). */
        final boolean arrowJavaUnsupported;

        Case(String name, String type, String token, Object[] raw, Object[] expected, boolean divergent) {
            this.arrowJavaUnsupported = type.equals("float:16");
            this.name = name;
            this.type = type;
            this.token = token;
            this.raw = raw;
            this.expected = expected;
            this.divergent = divergent;
            if (raw.length != expected.length) throw new IllegalArgumentException(name);
        }
    }

    private static Object[] a(Object... v) { return v; }

    private static Map<String, Object> map(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    private static BigDecimal bd(String s) { return new BigDecimal(s); }

    private static LocalDateTime ldt(String s) { return LocalDateTime.parse(s); }

    static final List<Case> ALL = new ArrayList<>();

    private static void add(String name, String type, String token, Object[] raw, Object[] expected) {
        ALL.add(new Case(name, type, token, raw, expected, false));
    }

    private static void diverge(String name, String type, String token, Object[] raw, Object[] expected) {
        ALL.add(new Case(name, type, token, raw, expected, true));
    }

    static Case byName(String name) {
        for (Case c : ALL) if (c.name.equals(name)) return c;
        throw new IllegalArgumentException(name);
    }

    static {
        add("null", "null", "null", a(null, null, null), a(null, null, null));

        add("int8", "int:8:s", "int8", a(-128L, 0L, null, 127L), a((byte) -128, (byte) 0, null, (byte) 127));
        add("int16", "int:16:s", "int16", a(-32768L, null, 32767L), a((short) -32768, null, (short) 32767));
        add("int32", "int:32:s", "int32", a((long) Integer.MIN_VALUE, null, (long) Integer.MAX_VALUE),
                a(Integer.MIN_VALUE, null, Integer.MAX_VALUE));
        add("int64", "int:64:s", "int64", a(Long.MIN_VALUE, null, Long.MAX_VALUE),
                a(Long.MIN_VALUE, null, Long.MAX_VALUE));
        diverge("uint8", "int:8:u", "int16", a(0L, 255L, null), a((short) 0, (short) 255, null));
        diverge("uint16", "int:16:u", "int32", a(0L, 65535L, null), a(0, 65535, null));
        diverge("uint32", "int:32:u", "int64", a(0L, 4294967295L, null), a(0L, 4294967295L, null));
        diverge("uint64", "int:64:u", "decimal(20,0)", a(0L, -1L, null),
                a(bd("0"), bd("18446744073709551615"), null));

        diverge("float16", "float:16", "float32", a(0x3C00L, 0xC000L, null, 0x7C00L, 0x0001L, 0x7BFFL),
                a(1.0f, -2.0f, null, Float.POSITIVE_INFINITY, 5.9604645E-8f, 65504.0f));
        add("float32", "float:32", "float32", a(1.5d, null, -0.0d, Double.NaN), a(1.5f, null, -0.0f, Float.NaN));
        add("float64", "float:64", "float64", a(1e300, null, -2.5), a(1e300, null, -2.5));

        add("decimal128", "decimal:38:9:128", "decimal(38,9)",
                a("123.456000000", "-0.000000001", null, "99999999999999999999999999999.999999999"),
                a(bd("123.456000000"), bd("-0.000000001"), null, bd("99999999999999999999999999999.999999999")));
        add("decimal128_scale0", "decimal:10:0:128", "decimal(10,0)", a("0", "-42", null), a(bd("0"), bd("-42"), null));
        add("decimal256", "decimal:76:20:256", "decimal(76,20)",
                a("100000000000000000000000000000000000000000000000000.00000000000000000001", null,
                        "-12345678901234567890123456789012345678901234567890.12345678901234567890"),
                a(bd("100000000000000000000000000000000000000000000000000.00000000000000000001"), null,
                        bd("-12345678901234567890123456789012345678901234567890.12345678901234567890")));

        add("bool", "bool", "bool", a(true, false, null), a(true, false, null));
        add("utf8", "utf8", "utf8", a("", "héllo✓", null), a("", "héllo✓", null));
        add("largeutf8", "largeutf8", "largeutf8", a("a", null, "bc"), a("a", null, "bc"));
        add("binary", "binary", "binary", a(new byte[0], new byte[] {0, 1, (byte) 255}, null),
                a(new byte[0], new byte[] {0, 1, (byte) 255}, null));
        add("largebinary", "largebinary", "binary", a(new byte[] {9}, null), a(new byte[] {9}, null));
        add("fixedsizebinary", "fixedbinary:3", "fixedsizebinary(3)", a(new byte[] {1, 2, 3}, null, new byte[] {4, 5, 6}),
                a(new byte[] {1, 2, 3}, null, new byte[] {4, 5, 6}));

        add("date32", "date:day", "date32", a(0L, 19000L, -1L, null),
                a(LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(19000), LocalDate.ofEpochDay(-1), null));
        diverge("date64", "date:ms", "date64", a(0L, 86_400_000L * 19000, null, -1L),
                a(LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(19000), null, LocalDate.ofEpochDay(-1)));

        diverge("time32_s", "time:s", "time", a(0L, 43200L, null), a(0L, 43_200_000_000L, null));
        diverge("time32_ms", "time:ms", "time", a(1L, 43_200_123L, null), a(1000L, 43_200_123_000L, null));
        add("time64_us", "time:us", "time", a(0L, 43_200_123_456L, null), a(0L, 43_200_123_456L, null));
        diverge("time64_ns", "time:ns", "time", a(1500L, 43_200_123_456_789L, null), a(1L, 43_200_123_456L, null));

        add("timestamp_s", "ts:s:", "timestamp", a(0L, 1_700_000_000L, null, -1L),
                a(ldt("1970-01-01T00:00"), ldt("2023-11-14T22:13:20"), null, ldt("1969-12-31T23:59:59")));
        add("timestamp_ms", "ts:ms:", "timestamp", a(1_700_000_000_123L, null, -1L),
                a(ldt("2023-11-14T22:13:20.123"), null, ldt("1969-12-31T23:59:59.999")));
        add("timestamp_us", "ts:us:", "timestamp", a(1_700_000_000_123_456L, null, -1L),
                a(ldt("2023-11-14T22:13:20.123456"), null, ldt("1969-12-31T23:59:59.999999")));
        add("timestamp_ns", "ts:ns:", "timestamp", a(1_700_000_000_123_456_789L, null, -1L),
                a(ldt("2023-11-14T22:13:20.123456789"), null, ldt("1969-12-31T23:59:59.999999999")));
        add("timestamptz_us", "ts:us:UTC", "timestamp", a(1_700_000_000_123_456L, null, -1L),
                a(1_700_000_000_123_456L, null, -1L));
        diverge("timestamptz_s", "ts:s:UTC", "timestamp", a(1_700_000_000L, null), a(1_700_000_000_000_000L, null));
        diverge("timestamptz_ms", "ts:ms:+05:30", "timestamp", a(1_700_000_000_123L, null, -1L),
                a(1_700_000_000_123_000L, null, -1000L));
        diverge("timestamptz_ns", "ts:ns:America/New_York", "timestamp", a(1_700_000_000_123_456_789L, null, -1L),
                a(1_700_000_000_123_456L, null, -1L));

        add("duration_s", "duration:s", "duration(second)", a(5L, null, -1L),
                a(Duration.ofSeconds(5), null, Duration.ofSeconds(-1)));
        add("duration_ms", "duration:ms", "duration(millisecond)", a(1500L, null), a(Duration.ofMillis(1500), null));
        add("duration_us", "duration:us", "duration(microsecond)", a(1_500_000L, null),
                a(Duration.ofMillis(1500), null));
        add("duration_ns", "duration:ns", "duration(nanosecond)", a(7L, null), a(Duration.ofNanos(7), null));

        add("interval_ym", "interval:ym", "interval(year_month)", a(14L, -3L, null),
                a(Period.ofMonths(14), Period.ofMonths(-3), null));
        add("interval_dt", "interval:dt", "interval(day_time)", a(new long[] {1, 500}, new long[] {-2, 0}, null),
                a(Duration.ofDays(1).plusMillis(500), Duration.ofDays(-2), null));
        diverge("interval_mdn", "interval:mdn", "interval(month_day_nano)",
                a(new long[] {1, 2, 3_000_000_000L}, new long[] {0, 0, 0}, null, new long[] {-1, 0, 1_500_000L}),
                a("P1M2DT3S", "P0D", null, "P-1MT0.0015S"));

        add("list_int32", "list:int:32:s", "list", a(a(1L, 2L, null), a(), null, a(7L)),
                a(a(1, 2, null), a(), null, a(7)));
        add("list_utf8", "list:utf8", "list", a(a("a", null, "é"), null, a("")),
                a(a("a", null, "é"), null, a("")));
        diverge("list_date32", "list:date:day", "list", a(a(0L, 19000L), null),
                a(a(LocalDate.ofEpochDay(0), LocalDate.ofEpochDay(19000)), null));
        add("largelist_int64", "largelist:int:64:s", "list", a(a(1L, null), null, a()),
                a(a(1L, null), null, a()));
        add("fixedlist_int32", "fixedlist:2:int:32:s", "list", a(a(1L, 2L), null, a(3L, null)),
                a(a(1, 2), null, a(3, null)));
        diverge("list_list_int32", "list:list:int:32:s", "list", a(a(a(1L, 2L), a(), null), null),
                a(a(a(1, 2), a(), null), null));
        diverge("struct", "struct", "struct", a(a(1L, "x"), null, a(null, "y")),
                a(map("a", 1, "b", "x"), null, map("b", "y")));
        diverge("map", "map", "map(false)", a(a(a("k1", 1L), a("k2", null)), null, a()),
                a(a(map("key", "k1", "value", 1), map("key", "k2")), null, a()));

        add("dict_utf8_int8", "dict:8:utf8", "utf8", a("red", null, "blue", "red"), a("red", null, "blue", "red"));
        add("dict_int64_int32", "dict:32:int:64:s", "int64", a(10L, 20L, null, 10L), a(10L, 20L, null, 10L));
        // A dictionary of a nested type: its dictionary batch has two field nodes.
        add("dict_list_int32_int8", "dict:8:list:int:32:s", "list", a(a(1L, 2L), null, a(1L, 2L), a()),
                a(a(1, 2), null, a(1, 2), a()));
    }

    /** Value equality for decoded cells: arrays (including nested Object[]) by content. */
    static boolean cellEquals(Object want, Object got) {
        if (want == null || got == null) return want == got;
        if (want instanceof byte[] && got instanceof byte[]) return Arrays.equals((byte[]) want, (byte[]) got);
        if (want instanceof Object[] && got instanceof Object[]) {
            Object[] w = (Object[]) want, g = (Object[]) got;
            if (w.length != g.length) return false;
            for (int i = 0; i < w.length; i++) if (!cellEquals(w[i], g[i])) return false;
            return true;
        }
        if (want instanceof Map && got instanceof Map) {
            Map<?, ?> w = (Map<?, ?>) want, g = (Map<?, ?>) got;
            if (!new ArrayList<>(w.keySet()).equals(new ArrayList<>(g.keySet()))) return false;
            for (Object k : w.keySet()) if (!cellEquals(w.get(k), g.get(k))) return false;
            return true;
        }
        if (want instanceof Float && got instanceof Float) {
            return Float.floatToIntBits((Float) want) == Float.floatToIntBits((Float) got);
        }
        if (want instanceof Double && got instanceof Double) {
            return Double.doubleToLongBits((Double) want) == Double.doubleToLongBits((Double) got);
        }
        return want.getClass() == got.getClass() && want.equals(got);
    }

    static String show(Object v) {
        if (v == null) return "null";
        if (v instanceof byte[]) return "byte" + Arrays.toString((byte[]) v);
        if (v instanceof Object[]) {
            StringBuilder sb = new StringBuilder("[");
            for (Object e : (Object[]) v) sb.append(sb.length() > 1 ? ", " : "").append(show(e));
            return sb.append(']').toString();
        }
        return v.getClass().getSimpleName() + ":" + v;
    }
}
