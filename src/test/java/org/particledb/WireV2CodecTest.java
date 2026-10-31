package org.particledb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.particledb.wire.WireV2Codec;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/** Pure unit tests for the wire codec - no server required. */
public class WireV2CodecTest {

    @Test
    void encodes_v1_hello_with_v2_capability_announcement() {
        byte[] caps = { (byte) (WireV2Codec.CAP_V1_LEGACY | WireV2Codec.CAP_V2_MUX) };
        byte[] formats = { WireV2Codec.FMT_ARROW_IPC };
        ByteBuf buf = WireV2Codec.encodeHelloV1(WireV2Codec.PROTOCOL_V2, caps, formats);

        // 8-byte v1 header + payload
        assertEquals(8 + 1 + 1 + 1 + 1 + 1, buf.readableBytes());

        // header: [u32 len][u16 stream=0][u8 type=0x00 (HELLO)][u8 flags=0]
        int len = buf.readIntLE();
        assertEquals(5, len);
        assertEquals(0, buf.readUnsignedShortLE());
        assertEquals(WireV2Codec.MSG_HELLO, buf.readByte());
        assertEquals(0, buf.readByte());

        // payload: [version=2][num_caps=1][caps=0x03][num_formats=1][fmt=0x01]
        assertEquals(WireV2Codec.PROTOCOL_V2, buf.readByte());
        assertEquals(1, buf.readByte());
        assertEquals(0x03, buf.readByte());
        assertEquals(1, buf.readByte());
        assertEquals(WireV2Codec.FMT_ARROW_IPC, buf.readByte());
    }

    @Test
    void encodes_v2_query_with_correct_header_layout() {
        ByteBuf buf = WireV2Codec.encodeQueryV2(7L, "SELECT 1");
        // v2 header = 11 bytes, then payload
        assertEquals(11 + "SELECT 1".length(), buf.readableBytes());
        assertEquals(WireV2Codec.PROTOCOL_V2, buf.readByte());
        assertEquals(WireV2Codec.MSG_QUERY, buf.readByte());
        assertEquals(WireV2Codec.FLAG_END_STREAM, buf.readByte());
        assertEquals(7, buf.readIntLE());
        assertEquals("SELECT 1".length(), buf.readIntLE());
        byte[] payload = new byte[8];
        buf.readBytes(payload);
        assertEquals("SELECT 1", new String(payload, StandardCharsets.UTF_8));
    }

    @Test
    void encodes_v2_batch_with_count_then_length_prefixed_sql() {
        ByteBuf buf = WireV2Codec.encodeBatchV2(9L,
                java.util.Arrays.asList("INSERT INTO t VALUES (1)", "COMMIT"));
        // skip header
        buf.skipBytes(11);
        assertEquals(2, buf.readIntLE());
        int firstLen = buf.readIntLE();
        byte[] first = new byte[firstLen];
        buf.readBytes(first);
        assertEquals("INSERT INTO t VALUES (1)", new String(first, StandardCharsets.UTF_8));
        int secondLen = buf.readIntLE();
        byte[] second = new byte[secondLen];
        buf.readBytes(second);
        assertEquals("COMMIT", new String(second, StandardCharsets.UTF_8));
    }

    @Test
    void parses_helloOk_v1_and_v2() {
        // v1 short payload (just version + format)
        byte[] v1 = { 0x01, WireV2Codec.FMT_ARROW_IPC };
        WireV2Codec.HelloOk h1 = WireV2Codec.parseHelloOk(v1);
        assertEquals(0x01, h1.serverVersion);
        assertFalse(h1.isV2());

        // v2 full payload
        byte[] v2 = new byte[11];
        v2[0] = 0x02;
        v2[1] = WireV2Codec.CAP_V2_MUX;
        v2[2] = WireV2Codec.FMT_ARROW_IPC;
        v2[3] = 100;          // max_streams = 100 (LE)
        v2[4] = 0;
        v2[5] = 0;
        v2[6] = 0;
        v2[7] = 0;            // initial_window = 65536 (LE)
        v2[8] = 0;
        v2[9] = 1;
        v2[10] = 0;
        WireV2Codec.HelloOk h2 = WireV2Codec.parseHelloOk(v2);
        assertEquals(0x02, h2.serverVersion);
        assertTrue(h2.isV2());
        assertEquals(100, h2.maxConcurrentStreams);
        assertEquals(65536, h2.initialWindowSize);
    }

    @Test
    void decodes_ok_affected_rows() {
        byte[] payload = new byte[8];
        // 1234 in LE u64
        long v = 1234L;
        for (int i = 0; i < 8; i++) payload[i] = (byte) ((v >> (i * 8)) & 0xFF);
        assertEquals(1234L, WireV2Codec.decodeOkAffected(payload));
    }
}
