package org.particledb;

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;
import org.particledb.wire.TypedParams;
import org.particledb.wire.WireV2Codec;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.SplittableRandom;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Typed EXECUTE parameters, no server: the canonical float spelling (which the
 * server's {@code native_literal::java_double_text} must reproduce byte for
 * byte -- the goldens here are the same as that module's), which values may
 * travel typed, the byte encodings, and the HELLO / HELLO_OK extension block
 * that negotiates them. The over-the-wire identity is
 * {@link TypedParamsDifferentialTest}.
 */
public class TypedParamsTest {

    @Test
    void canonicalSpellingMatchesTheServerGoldens() {
        Object[][] doubles = {
                {1.5, "1.5"}, {1.0, "1.0"}, {100.0, "100.0"}, {1e7, "1.0E7"}, {9_999_999.0, "9999999.0"},
                {0.001, "0.001"}, {1e-4, "1.0E-4"}, {1e10, "1.0E10"}, {0.1, "0.1"},
                {1.0 / 3.0, "0.3333333333333333"}, {9.223372036854776e18, "9.223372036854776E18"},
                {Double.MIN_NORMAL, "2.2250738585072014E-308"}, {Double.MIN_VALUE, "4.9E-324"},
                {Double.MAX_VALUE, "1.7976931348623157E308"}, {123456.789, "123456.789"}, {-2.5, "-2.5"},
                {0.0, "0.0"}, {-0.0, "-0.0"},
        };
        for (Object[] c : doubles) {
            double d = (Double) c[0];
            assertEquals(c[1], TypedParams.canonicalDouble(d), "canonical " + d);
            assertEquals(c[1], Double.toString(d), "this JDK prints the golden for " + d);
        }
        Object[][] floats = {{1.1f, "1.1"}, {Float.MAX_VALUE, "3.4028235E38"}, {1e-5f, "1.0E-5"}, {0.25f, "0.25"}};
        for (Object[] c : floats) {
            assertEquals(c[1], TypedParams.canonicalFloat((Float) c[0]));
        }
    }

    /**
     * Random doubles: a value travels typed ONLY when this JDK's own
     * {@code Double.toString} is the canonical spelling (so the server's
     * rendering is byte-identical to the inline text); the canonical spelling
     * always reads back as the value; and the rate a JDK 17 falls back is
     * small (measured, printed).
     */
    @Test
    void aDoubleIsTypedOnlyWhenThisJdkPrintsTheCanonicalSpelling() {
        SplittableRandom r = new SplittableRandom(42);
        int typed = 0, n = 200_000;
        for (int i = 0; i < n; i++) {
            double d = (i % 2 == 0) ? Double.longBitsToDouble(r.nextLong())
                    : Math.round(r.nextDouble() * 1e6) / Math.pow(10, r.nextInt(8));
            if (Double.isNaN(d) || Double.isInfinite(d)) continue;
            String canon = TypedParams.canonicalDouble(d);
            assertEquals(d, Double.parseDouble(canon), "canonical must read back: " + canon);
            boolean b = WireV2Codec.bindable(new Object[] {d}, 1, true);
            assertEquals(Double.toString(d).equals(canon), b, "bindable iff identical: " + d);
            if (b) typed++;
        }
        System.err.printf("[TypedParamsTest] %s: %d of %d random doubles typed (%.3f%% fall back)%n",
                System.getProperty("java.version"), typed, n, 100.0 * (n - typed) / n);
        assertTrue(typed > n * 0.95, "a JDK 17 prints the canonical spelling for nearly every value");
        for (int i = 0; i < 100_000; i++) {
            float f = Float.intBitsToFloat(r.nextInt());
            if (Float.isNaN(f) || Float.isInfinite(f)) continue;
            String canon = TypedParams.canonicalFloat(f);
            assertEquals(f, Float.parseFloat(canon), canon);
            assertEquals(Float.toString(f).equals(canon), WireV2Codec.bindable(new Object[] {f}, 1, true));
        }
    }

    @Test
    void whichValuesTravelTyped() {
        Object[] yes = {
                Double.NaN, Double.NEGATIVE_INFINITY, 1.5, 2.5f, new BigDecimal("123.4500"), new BigDecimal("0.000001"),
                new BigDecimal("-5"), java.sql.Timestamp.valueOf("2024-01-01 12:34:56.789"),
                java.sql.Date.valueOf("2024-01-01"), java.sql.Time.valueOf("12:34:56"),
                OffsetDateTime.of(2024, 3, 10, 2, 30, 0, 0, ZoneOffset.ofHours(2)), new byte[] {1, 2},
                UUID.randomUUID(), null, "s", 1L, 1, (short) 1, (byte) 1, true,
        };
        for (Object v : yes) {
            assertTrue(WireV2Codec.bindable(new Object[] {v}, 1, true), "typed: " + v);
        }
        java.sql.Timestamp nanos = java.sql.Timestamp.valueOf("2024-01-01 12:34:56.123456789");
        Object[] no = {
                new BigDecimal("1E+3"), new BigDecimal("1.23E-7"), nanos,
                java.sql.Date.valueOf(java.time.LocalDate.of(10000, 1, 1)),
                java.time.LocalDateTime.of(2024, 1, 1, 0, 0), java.time.LocalDate.of(2024, 1, 1),
                OffsetDateTime.of(2024, 1, 1, 0, 0, 0, 1, ZoneOffset.UTC), new Object(),
        };
        for (Object v : no) {
            assertFalse(WireV2Codec.bindable(new Object[] {v}, 1, true), "text path: " + v);
        }
        // Without the capability only tags 0..3 are bound -- exactly the old rule.
        assertFalse(WireV2Codec.bindable(new Object[] {1.5}, 1, false));
        assertFalse(WireV2Codec.bindable(new Object[] {1.5}, 1));
        assertTrue(WireV2Codec.bindable(new Object[] {1L, "x", null, true}, 4, false));
    }

    @Test
    void encodingsAreTheDocumentedBytes() {
        byte[] name = {'s'};
        byte[] p = WireV2Codec.encodeExecutePayload(name, new Object[] {
                1.5, 1.5f, new BigDecimal("-1.05"), java.sql.Date.valueOf("1970-01-02"),
                java.sql.Timestamp.valueOf("1970-01-01 00:00:01.000002"), java.sql.Time.valueOf("00:00:03"),
                OffsetDateTime.of(1970, 1, 1, 2, 0, 0, 0, ZoneOffset.ofHours(2)),
                new UUID(0x0102030405060708L, 0x090a0b0c0d0e0f10L), new byte[] {(byte) 0xab},
        }, 9);
        int o = 4 + 1;
        assertEquals(9, p[o] & 0xFF);
        o += 2;
        assertEquals(TypedParams.PARAM_FLOAT8, p[o]);
        assertEquals(Double.doubleToLongBits(1.5), le(p, o + 1, 8));
        o += 9;
        assertEquals(TypedParams.PARAM_FLOAT4, p[o]);
        assertEquals(Float.floatToIntBits(1.5f), (int) le(p, o + 1, 4));
        o += 5;
        assertEquals(TypedParams.PARAM_NUMERIC, p[o]);
        assertEquals(2, (int) le(p, o + 1, 4), "scale");
        assertEquals(1, (int) le(p, o + 5, 4), "unscaled length");
        assertEquals((byte) -105, p[o + 9], "two's-complement big-endian");
        o += 10;
        assertEquals(TypedParams.PARAM_DATE, p[o]);
        assertEquals(1, (int) le(p, o + 1, 4));
        o += 5;
        assertEquals(TypedParams.PARAM_TIMESTAMP, p[o]);
        assertEquals(1_000_002L, le(p, o + 1, 8), "wall-clock micros, whatever the JVM zone");
        o += 9;
        assertEquals(TypedParams.PARAM_TIME, p[o]);
        assertEquals(3_000_000L, le(p, o + 1, 8));
        o += 9;
        assertEquals(TypedParams.PARAM_TIMESTAMPTZ, p[o]);
        assertEquals(0L, le(p, o + 1, 8), "the UTC instant");
        assertEquals(7200, (int) le(p, o + 9, 4), "the offset");
        o += 13;
        assertEquals(TypedParams.PARAM_UUID, p[o]);
        for (int i = 0; i < 16; i++) assertEquals(i + 1, p[o + 1 + i]);
        o += 17;
        assertEquals(TypedParams.PARAM_BYTEA, p[o]);
        assertEquals(1, (int) le(p, o + 1, 4));
        assertEquals((byte) 0xab, p[o + 5]);
        assertEquals(o + 6, p.length);
    }

    /**
     * The HELLO's extension block is a pure suffix (an older server reads the
     * same caps and formats), and an older server's 11-byte HELLO_OK reads as
     * "no typed parameters".
     */
    @Test
    void helloExtensionBlockIsAdditive() {
        byte[] caps = {(byte) (WireV2Codec.CAP_V2_MUX | WireV2Codec.CAP_PREPARED)};
        byte[] fmts = {WireV2Codec.FMT_ARROW_IPC};
        ByteBuf old = WireV2Codec.encodeHelloV1(WireV2Codec.PROTOCOL_V2, caps, fmts);
        ByteBuf ext = WireV2Codec.encodeHelloV1(WireV2Codec.PROTOCOL_V2, caps, fmts,
                new byte[] {WireV2Codec.CAP_EXT_TYPED_PARAMS});
        byte[] a = new byte[old.readableBytes()];
        old.readBytes(a);
        byte[] b = new byte[ext.readableBytes()];
        ext.readBytes(b);
        assertEquals(a.length + 2, b.length);
        for (int i = 8; i < a.length; i++) assertEquals(a[i], b[i], "payload prefix byte " + i);
        assertEquals(1, b[a.length]);
        assertEquals(WireV2Codec.CAP_EXT_TYPED_PARAMS, b[a.length + 1]);

        byte[] ok11 = {2, (byte) 0x82, 1, 8, 0, 0, 0, 0, 0, 1, 0};
        WireV2Codec.HelloOk older = WireV2Codec.parseHelloOk(ok11);
        assertTrue(older.supportsPrepared());
        assertFalse(older.supportsTypedParams(), "an older server never echoes the block");
        byte[] ok13 = java.util.Arrays.copyOf(ok11, 13);
        ok13[11] = 1;
        ok13[12] = WireV2Codec.CAP_EXT_TYPED_PARAMS;
        assertTrue(WireV2Codec.parseHelloOk(ok13).supportsTypedParams());
        assertEquals(8, WireV2Codec.parseHelloOk(ok13).maxConcurrentStreams, "body unchanged");
    }

    private static long le(byte[] b, int off, int n) {
        long v = 0;
        for (int i = 0; i < n; i++) v |= ((long) (b[off + i] & 0xFF)) << (8 * i);
        return v;
    }
}
