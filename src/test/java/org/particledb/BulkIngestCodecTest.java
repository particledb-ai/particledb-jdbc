package org.particledb;

import org.junit.jupiter.api.Test;
import org.particledb.wire.WireV2Codec;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Native bulk ingest, no server: the BEGIN / END bytes (mirroring
 * {@code BulkBegin::encode} in the server's native-wire v2 codec), the
 * plain-INSERT shape detector behind {@code executeBatch}'s bulk path, the
 * bound-value text forms, and the DATA serialization (one complete IPC stream).
 */
public class BulkIngestCodecTest {

    @Test
    void beginEncodingIsByteExact() {
        byte[] got = WireV2Codec.encodeBulkBegin("s.t", Arrays.asList("id", "vé"), 0x0102030405060708L);
        byte[] want = {
                0x01,                         // BULK_BEGIN
                3, 0, 's', '.', 't',          // relation
                2, 0,                         // ncols
                2, 0, 'i', 'd',
                3, 0, 'v', (byte) 0xC3, (byte) 0xA9,
                0,                            // flags
                8, 7, 6, 5, 4, 3, 2, 1,       // commit_every_rows LE
        };
        assertArrayEquals(want, got);
    }

    @Test
    void beginWithNoColumnsAndEndEncoding() {
        assertArrayEquals(new byte[] {1, 1, 0, 't', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                WireV2Codec.encodeBulkBegin("t", Collections.emptyList(), 0));
        assertArrayEquals(new byte[] {3, 5, 0, 0, 0, 0, 0, 0, 0}, WireV2Codec.encodeBulkEnd(5));
        assertEquals(0x40, WireV2Codec.MSG_BULK);
        assertEquals(0x02, WireV2Codec.CAP_EXT_BULK_INGEST);
    }

    @Test
    void helloOkBulkBitIsReadFromTheExtensionByte() {
        WireV2Codec.HelloOk with = new WireV2Codec.HelloOk((byte) 2, WireV2Codec.CAP_V2_MUX, (byte) 1, 64, 0,
                WireV2Codec.CAP_EXT_BULK_INGEST);
        WireV2Codec.HelloOk without = new WireV2Codec.HelloOk((byte) 2, WireV2Codec.CAP_V2_MUX, (byte) 1, 64, 0,
                WireV2Codec.CAP_EXT_TYPED_PARAMS);
        assertTrue(with.supportsBulkIngest());
        assertFalse(without.supportsBulkIngest());
    }

    private static BulkIngest.Insert parse(String sql) {
        return BulkIngest.Insert.parse(sql);
    }

    @Test
    void detectsPlainInserts() {
        BulkIngest.Insert a = parse("INSERT INTO t (id, V, \"Mixed\") VALUES (?, ?, ?)");
        assertNotNull(a);
        assertEquals("t", a.relation);
        assertEquals(Arrays.asList("id", "v", "Mixed"), a.columns);
        assertEquals(3, a.values);

        BulkIngest.Insert b = parse("  insert into public.\"T x\" values(?,?) ; ");
        assertNotNull(b);
        assertEquals("public.\"T x\"", b.relation);
        assertNull(b.columns);
        assertEquals(2, b.values);

        BulkIngest.Insert c = parse("insert\ninto t(a)\nvalues\n(?)");
        assertNotNull(c);
        assertEquals(Collections.singletonList("a"), c.columns);
    }

    @Test
    void rejectsEveryOtherShape() {
        String[] no = {
                "INSERT INTO t VALUES (?, ?) ON CONFLICT DO NOTHING",
                "INSERT INTO t VALUES (?) RETURNING id",
                "INSERT INTO t SELECT ?",
                "INSERT INTO t VALUES (?, 1)",
                "INSERT INTO t VALUES (?, DEFAULT)",
                "INSERT INTO t VALUES (?), (?)",
                "INSERT INTO t VALUES (? + 1)",
                "INSERT INTO t VALUES (?::int)",
                "INSERT INTO t (a, b) VALUES (?)",
                "INSERT INTO t (a, a) VALUES (?, ?)",
                "INSERT INTO t DEFAULT VALUES",
                "INSERT INTO t AS x VALUES (?)",
                "WITH x AS (SELECT 1) INSERT INTO t VALUES (?)",
                "INSERT INTO t VALUES (?) -- c",
                "/* c */ INSERT INTO t VALUES (?)",
                "INSERT INTO t VALUES (?); INSERT INTO t VALUES (?)",
                "UPDATE t SET a = ?",
                "INSERT INTO t OVERRIDING SYSTEM VALUE VALUES (?)",
                "INSERT INTO t VALUES ()",
                "INSERT INTO a.b.c.d VALUES (?)",
                "INSERTINTO t VALUES (?)",
                "INSERT INTO t VALUES (??)",
        };
        for (String sql : no) assertNull(parse(sql), sql);
    }

    @Test
    void templateCarriesTheShapeOnlyWhenPlaceholdersAgree() {
        assertNotNull(new PdbPreparedStatement.Template("INSERT INTO t (a, b) VALUES (?, ?)").bulkInsert);
        assertNull(new PdbPreparedStatement.Template("INSERT INTO t (a, b) VALUES ('?', ?)").bulkInsert);
        // The pg_catalog adapter may rewrite this one: never bulk.
        assertNull(new PdbPreparedStatement.Template("INSERT INTO pg_class VALUES (?)").bulkInsert);
    }

    @Test
    void textForms() {
        assertNull(BulkIngest.textOf(null));
        assertEquals("abc", BulkIngest.textOf("abc"));
        assertEquals("42", BulkIngest.textOf(42L));
        assertEquals("1000", BulkIngest.textOf(new BigDecimal("1E+3")));
        assertEquals("NaN", BulkIngest.textOf(Double.NaN));
        assertEquals("true", BulkIngest.textOf(Boolean.TRUE));
        assertEquals("\\x00ff10", BulkIngest.textOf(new byte[] {0, (byte) 0xFF, 0x10}));
        assertSame(BulkIngest.UNSUPPORTED, BulkIngest.textOf(new PdbArray("int4", new Object[] {1})));
        assertSame(BulkIngest.UNSUPPORTED, BulkIngest.textOf(new Object()));
    }

    @Test
    void rowsBecomeOneIpcStreamPerDataFrame() {
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] {1L, "a'b"});
        rows.add(new Object[] {2L, null});
        rows.add(new Object[] {3L, "z\u00e9\u2713"});
        List<String> cols = Arrays.asList("id", "v");
        byte[][][] text = BulkIngest.rowsToText(cols, rows);
        assertNotNull(text);
        byte[] data = BulkIngest.serializeText(cols, text, 1, 2);
        assertEquals(WireV2Codec.BULK_DATA, data[0]);
        // Read back with the driver's reader (ArrowBulkCodecTest also reads
        // it with Apache Arrow Java).
        ArrowDecoder.Decoded d = readBack(data);
        assertEquals(cols, d.columnNames);
        assertEquals(Arrays.asList("utf8", "utf8"), d.columnTypes);
        assertEquals(2, d.rows.size());
        assertArrayEquals(new Object[] {"2", null}, d.rows.get(0));
        assertArrayEquals(new Object[] {"3", "z\u00e9\u2713"}, d.rows.get(1));
        // Every row, and an empty range.
        assertEquals(3, readBack(BulkIngest.serializeText(cols, text, 0, 3)).rows.size());
        assertEquals(0, readBack(BulkIngest.serializeText(cols, text, 0, 0)).rows.size());
        // An unsupported value anywhere: no text (the batch takes the text path).
        rows.add(new Object[] {4L, new Object()});
        assertNull(BulkIngest.rowsToText(cols, rows));
    }

    private static ArrowDecoder.Decoded readBack(byte[] data) {
        return ArrowIpcReader.decode(Arrays.copyOfRange(data, 1, data.length));
    }
}
