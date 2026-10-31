package org.particledb.wire;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Wire frame codec for both v1 and v2.
 *
 * <p>v1 header (8 bytes):
 * <pre>
 *   [u32_le len] [u16_le stream_id] [u8 type] [u8 flags] [payload...]
 * </pre>
 *
 * <p>v2 header (11 bytes), as defined by
 * {@code PROTOCOL.md} in this repository:
 * <pre>
 *   [u8 version=2] [u8 type] [u8 flags] [u32_le stream_id] [u32_le len] [payload...]
 * </pre>
 *
 * <p>Per spec, HELLO is always sent in v1 framing. After HELLO_OK negotiates
 * v2, subsequent frames use the 11-byte v2 layout.
 *
 * <p>Distinguishing v1 vs v2 on the wire is done via mode (set by the codec
 * after HELLO_OK), not by frame heuristics. This decoder takes a {@code mode}
 * field; callers flip it from V1 to V2 once the handshake completes.
 */
public final class WireV2Codec extends ByteToMessageDecoder {

    public enum Mode { V1, V2 }

    public static final int V1_HEADER_SIZE = 8;
    public static final int V2_HEADER_SIZE = 11;
    public static final int MAX_PAYLOAD = 16 * 1024 * 1024;

    // ── Frame types - Client → Server ─────────────────────────────────────
    public static final byte MSG_HELLO         = 0x00;
    public static final byte MSG_QUERY         = 0x01;
    public static final byte MSG_BATCH         = 0x02;
    public static final byte MSG_PING          = 0x03;
    public static final byte MSG_CLOSE         = 0x04;
    public static final byte MSG_SETTINGS      = 0x05;
    public static final byte MSG_RST_STREAM_C  = 0x06;
    public static final byte MSG_WINDOW_UPDATE = 0x07;
    /**
     * Connection-level credential frame. v1 calls it {@code MSG_AUTH}, v2
     * calls it {@code F_AUTH}; both are opcode {@code 0x09} and carry the
     * identical payload, so one constant covers both framings.
     *
     * <p>Server decoder: the server's native-wire codec ({@code parse_auth_payload})
     * (v1 dispatch on {@code MSG_AUTH}; v2 dispatch at
     * the server's native-wire multiplexer {@code F_AUTH}).
     */
    public static final byte MSG_AUTH          = 0x09;
    public static final byte MSG_KV_SET        = 0x10;
    public static final byte MSG_KV_GET        = 0x11;
    public static final byte MSG_KV_DEL        = 0x12;
    public static final byte MSG_KV_INCR       = 0x13;
    /** Prepare a bound-parameter statement: {@code [u32 name_len][name][u32 sql_len][sql]}. */
    public static final byte MSG_PREPARE       = 0x20;
    /** Execute a prepared statement: {@code [u32 name_len][name][u16 n]} then n tagged values. */
    public static final byte MSG_EXECUTE       = 0x21;
    /** Deallocate a prepared statement: {@code [u32 name_len][name]}; answered OK. */
    public static final byte MSG_CLOSE_STMT    = 0x22;
    /** Native queue op ({@link org.particledb.PdbQueue}); payload layout in that class. */
    public static final byte MSG_QUEUE         = 0x30;
    /**
     * Native bulk ingest (see {@code PROTOCOL.md}). Payload
     * {@code [u8 subop][...]}, one request per frame, answered OK
     * ({@code u64} row count) or ERROR. Sent only when the server advertised
     * {@link #CAP_EXT_BULK_INGEST}.
     */
    public static final byte MSG_BULK          = 0x40;
    /** BULK subop: {@link #encodeBulkBegin}; answered OK(0). */
    public static final byte BULK_BEGIN = 0x01;
    /** BULK subop: followed by ONE complete Arrow IPC stream; answered OK(rows accepted so far). */
    public static final byte BULK_DATA  = 0x02;
    /** BULK subop: followed by {@code [u64 LE DATA frames sent]}; answered OK(rows loaded). */
    public static final byte BULK_END   = 0x03;
    /** BULK subop: drop the open load; answered OK. */
    public static final byte BULK_ABORT = 0x04;

    // ── Frame types - Server → Client ─────────────────────────────────────
    public static final byte MSG_HELLO_OK     = (byte) 0x80;
    public static final byte MSG_ARROW_RESULT = (byte) 0x81;
    public static final byte MSG_OK           = (byte) 0x82;
    public static final byte MSG_ERROR        = (byte) 0x83;
    public static final byte MSG_PONG         = (byte) 0x84;
    public static final byte MSG_BATCH_RESULT = (byte) 0x85;
    public static final byte MSG_ARROW_STREAM = (byte) 0x86;
    public static final byte MSG_KV_BULK      = (byte) 0x87;
    public static final byte MSG_KV_NIL       = (byte) 0x88;
    public static final byte MSG_RST_STREAM_S = (byte) 0x89;
    public static final byte MSG_GOAWAY       = (byte) 0x8a;
    public static final byte MSG_SETTINGS_ACK = (byte) 0x8b;
    /** Answer to {@link #MSG_PREPARE}: {@code [u16 parameter_count]}. */
    public static final byte MSG_PREPARE_OK   = (byte) 0x8d;
    /** Answer to {@link #MSG_QUEUE}. */
    public static final byte MSG_QUEUE_RESULT = (byte) 0x8e;
    /**
     * A statement's advisories, sent on its stream BEFORE its terminal frame
     * (only when {@link #CAP_STATEMENT_NOTICE} was negotiated). Payload
     * {@code [u16 count] count x ([u8 len][severity][u8 len][sqlstate][u32 len][message])}.
     */
    public static final byte MSG_NOTICE       = (byte) 0x8c;

    // ── Capability flags ──────────────────────────────────────────────────
    public static final byte CAP_V1_LEGACY        = 0x01;
    public static final byte CAP_V2_MUX           = 0x02;
    public static final byte CAP_COMPRESSION      = 0x04;
    public static final byte CAP_TLS_NEXT_PROTO   = 0x08;
    public static final byte CAP_NUMERIC_TEXT     = 0x10;
    /**
     * The client understands {@link #MSG_NOTICE} frames: the server delivers a
     * statement's non-fatal advisories (PostgreSQL's NoticeResponse) on its
     * stream before the terminal frame instead of only logging them.
     */
    public static final byte CAP_STATEMENT_NOTICE = 0x40;
    /**
     * SERVER → CLIENT ONLY. Set in the {@code chosen_caps} byte of HELLO_OK
     * when the listener demands credentials (i.e. it was NOT started with
     * {@code --no-auth} / {@code auth_method = trust}).
     *
     * <p>The native wire is fail-closed: on such a server every frame other
     * than the credential frame itself - QUERY, BATCH, KV_*, PING, SETTINGS,
     * an inline HELLO, unknown types alike - is refused with
     * {@link #ERR_AUTH_REQUIRED} until an AUTH succeeds. HELLO_OK is the only
     * frame an unauthenticated peer is ever served, which is why the challenge
     * lives there. A client never sends this bit.
     */
    public static final byte CAP_AUTH_REQUIRED    = 0x20;

    /**
     * Bound-parameter statements ({@link #MSG_PREPARE} / {@link #MSG_EXECUTE}).
     * Requested in HELLO; the server echoes it when it serves them (v2 only).
     */
    public static final byte CAP_PREPARED         = (byte) 0x80;

    /**
     * Extension capability block, ext byte 0: typed {@link #MSG_EXECUTE}
     * parameters (tags 4..16, {@link TypedParams}). The caps byte above is
     * full, so new capabilities ride in a block appended after the HELLO's
     * formats ({@code [u8 num_ext][ext...]}) and after HELLO_OK's 11-byte body.
     * An older server ignores the block and sends none back, so
     * {@link HelloOk#supportsTypedParams()} reads false and the driver keeps to
     * tags 0..3.
     */
    public static final byte CAP_EXT_TYPED_PARAMS = 0x01;

    /**
     * Extension capability block, ext byte 0: the server serves
     * {@link #MSG_BULK}. Requested in the HELLO's block and echoed by a server
     * that has it; an older server never echoes it, and the driver then never
     * sends {@link #MSG_BULK} (it would answer ERROR code 2).
     */
    public static final byte CAP_EXT_BULK_INGEST = 0x02;

    /**
     * Extension capability block, ext byte 0: the HELLO carries a session
     * parameter block after the extension block - the native wire's
     * counterpart of the PostgreSQL startup packet's parameters, where pgjdbc
     * sends the JVM's {@code TimeZone}:
     * {@code [u16 LE count] count x ([u16 LE klen][key][u16 LE vlen][value])}.
     * A server that applied them as the session's startup values echoes the
     * bit; an older server ignores the block and does not, and the driver then
     * {@code SET}s the zone after connect instead. 0x08: 0x04 is reserved
     * (batch SQLSTATE entries on the rc.12 line).
     */
    public static final byte CAP_EXT_SESSION_PARAMS = 0x08;

    /**
     * Ext byte 0: a failed entry of a BATCH_RESULT is sent as
     * {@code [u8 0x90][u16 code][5 x u8 SQLSTATE][u32 len][message]} instead of
     * {@code [u8 0x83][u32 len][message]}, so a batch failure carries its class
     * (23505, 40001, ...). Echoed by the server on v1 and v2.
     */
    public static final byte CAP_EXT_BATCH_SQLSTATE = 0x04;

    /** Batch-result entry tag of a failed entry carrying its SQLSTATE ({@link #CAP_EXT_BATCH_SQLSTATE}). */
    public static final byte BATCH_ENTRY_ERROR_SQLSTATE = (byte) 0x90;

    /**
     * Extension capability block, ext byte 0: the server accepts the
     * {@link #MSG_EXECUTE} ARRAY parameter (tag 17, {@link ArrayParams}): an
     * element type, dimensions and typed elements. Requested in the HELLO's
     * block and echoed by a server that has it; without the echo the driver
     * never sends tag 17 (an older server answers "unknown parameter tag").
     */
    public static final byte CAP_EXT_ARRAY_PARAMS = 0x10;

    // ── MSG_EXECUTE parameter tags ────────────────────────────────────────
    public static final byte PARAM_NULL = 0;
    public static final byte PARAM_I64  = 1;
    public static final byte PARAM_TEXT = 2;
    public static final byte PARAM_BOOL = 3;
    // Tags 4..16 (float8, float4, numeric, date, timestamp, timestamptz, time,
    // uuid, bytea, ...) are in {@link TypedParams}, offered under
    // {@link #CAP_EXT_TYPED_PARAMS}.

    // ── Authentication error codes (v1 and v2 use the same numbers) ───────
    /** Any frame received before a successful AUTH on an auth-required server. */
    public static final int ERR_AUTH_REQUIRED = 0x13;
    /** AUTH credentials failed verification; the server closes the connection. */
    public static final int ERR_AUTH_FAILED   = 0x14;
    /**
     * The server refused a HELLO session parameter (PostgreSQL's FATAL for a bad
     * startup parameter); sent in place of HELLO_OK. The text starts with the
     * SQLSTATE: {@code 22023: FATAL: invalid value for parameter "TimeZone": "…"}.
     */
    public static final int ERR_STARTUP_PARAMETER = 0x15;

    /**
     * Server-side hard cap on each credential field, in BYTES
     * ({@code MAX_AUTH_FIELD_LEN} in the server's native-wire codec).
     * A longer field makes the server's decoder reject the payload outright,
     * so reject it here with a clear message instead.
     */
    public static final int MAX_AUTH_FIELD_LEN = 4096;

    // ── Format flags ──────────────────────────────────────────────────────
    public static final byte FMT_ARROW_IPC = 0x01;
    public static final byte FMT_JSON      = 0x02;

    // ── Frame flags ───────────────────────────────────────────────────────
    public static final byte FLAG_END_STREAM  = 0x01;
    public static final byte FLAG_MORE_CHUNKS = 0x02;
    public static final byte FLAG_COMPRESSED  = 0x04;
    /**
     * Request flag (QUERY / EXECUTE): the server runs {@code BEGIN} immediately
     * before the statement, in the same request. Offered with
     * {@link #CAP_PREPARED}.
     */
    public static final byte FLAG_TXN_BEGIN   = 0x08;
    /** Response flag: the connection is inside a transaction block after this request. */
    public static final byte FLAG_TXN_OPEN    = 0x10;
    /** Response flag: the connection's transaction block has failed (only ROLLBACK is accepted). */
    public static final byte FLAG_TXN_FAILED  = 0x20;
    /**
     * Response flag on a request's terminal OK / BATCH_RESULT frame: a COMMIT
     * in this request ended a FAILED transaction by rolling it back -- the
     * native wire's stand-in for PostgreSQL's {@code ROLLBACK} command tag.
     * Sent by every server regardless of capabilities. The driver acts on it
     * only under {@code raiseExceptionOnSilentRollback=true}.
     */
    public static final byte FLAG_TXN_ROLLED_BACK = 0x40;

    // ── Protocol versions ─────────────────────────────────────────────────
    public static final byte PROTOCOL_V1 = 0x01;
    public static final byte PROTOCOL_V2 = 0x02;

    // ── GOAWAY error codes ────────────────────────────────────────────────
    public static final int ERR_NO_ERROR        = 0x0000;
    public static final int ERR_PROTOCOL_ERROR  = 0x0001;
    public static final int ERR_INTERNAL_ERROR  = 0x0002;
    public static final int ERR_FLOW_CONTROL    = 0x0003;
    public static final int ERR_STREAM_LIMIT    = 0x0004;
    public static final int ERR_FRAME_SIZE      = 0x0005;
    public static final int ERR_REFUSED_STREAM  = 0x0006;
    public static final int ERR_CANCEL          = 0x0007;
    public static final int ERR_IDLE_TIMEOUT    = 0x0008;

    /** Decoded wire frame. */
    public static final class WireFrame {
        public final long streamId;        // u32 in v2, u16 in v1 — widened
        public final byte msgType;
        public final byte flags;
        public final byte[] payload;

        public WireFrame(long streamId, byte msgType, byte flags, byte[] payload) {
            this.streamId = streamId;
            this.msgType = msgType;
            this.flags = flags;
            this.payload = payload;
        }

        public boolean hasMoreChunks() { return (flags & FLAG_MORE_CHUNKS) != 0; }
        public boolean isEndStream()   { return (flags & FLAG_END_STREAM)  != 0; }
        public boolean isError()       { return msgType == MSG_ERROR; }

        public WireError parseError() {
            if (payload.length < 2) return new WireError(0, "");
            int code = ((payload[0] & 0xFF)) | ((payload[1] & 0xFF) << 8);
            String msg = payload.length > 2
                    ? new String(payload, 2, payload.length - 2, StandardCharsets.UTF_8)
                    : "";
            return new WireError(code, msg);
        }

        @Override
        public String toString() {
            return "WireFrame{stream=" + streamId
                    + ", type=0x" + Integer.toHexString(msgType & 0xFF)
                    + ", flags=0x" + Integer.toHexString(flags & 0xFF)
                    + ", payload=" + payload.length + "B}";
        }
    }

    public static final class WireError {
        public final int code;
        public final String message;
        public WireError(int code, String message) { this.code = code; this.message = message; }
    }

    // ── Decoder state ─────────────────────────────────────────────────────
    private volatile Mode mode = Mode.V1; // v1 until HELLO_OK upgrades us

    /** Switch to v2 framing. Called after HELLO_OK with chosen_caps&0x02 set. */
    public void upgradeToV2() { this.mode = Mode.V2; }
    public Mode mode() { return mode; }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        while (true) {
            Mode m = this.mode;
            int header = (m == Mode.V1) ? V1_HEADER_SIZE : V2_HEADER_SIZE;
            if (in.readableBytes() < header) return;

            in.markReaderIndex();

            long streamId;
            byte msgType;
            byte flags;
            int payloadLen;

            if (m == Mode.V1) {
                payloadLen = in.readIntLE();
                streamId   = in.readUnsignedShortLE();
                msgType    = in.readByte();
                flags      = in.readByte();
            } else {
                byte version = in.readByte();
                if (version != PROTOCOL_V2) {
                    throw new IllegalStateException("v2 frame with version=" + version);
                }
                msgType    = in.readByte();
                flags      = in.readByte();
                streamId   = in.readUnsignedIntLE();
                payloadLen = in.readIntLE();
            }

            if (payloadLen < 0 || payloadLen > MAX_PAYLOAD) {
                throw new IllegalStateException("invalid payload length: " + payloadLen);
            }
            if (in.readableBytes() < payloadLen) {
                in.resetReaderIndex();
                return;
            }

            byte[] payload = new byte[payloadLen];
            if (payloadLen > 0) in.readBytes(payload);
            out.add(new WireFrame(streamId, msgType, flags, payload));
        }
    }

    // ── Encoders ──────────────────────────────────────────────────────────

    /**
     * Build a v1-framed HELLO. The HELLO is always sent in v1 framing per
     * the spec; this is what bootstraps v2 negotiation.
     *
     * <pre>
     *  payload = [u8 max_proto_version=2]
     *            [u8 num_caps]
     *            [cap bytes...]
     *            [u8 num_formats]
     *            [fmt bytes...]
     * </pre>
     */
    public static ByteBuf encodeHelloV1(byte maxProtoVer, byte[] caps, byte[] formats) {
        return encodeHelloV1(maxProtoVer, caps, formats, new byte[0]);
    }

    /**
     * {@link #encodeHelloV1(byte, byte[], byte[])} plus the extension
     * capability block {@code [u8 num_ext][ext...]} after the formats (omitted
     * when {@code ext} is empty: the bytes are then exactly the old HELLO).
     */
    public static ByteBuf encodeHelloV1(byte maxProtoVer, byte[] caps, byte[] formats, byte[] ext) {
        return encodeHelloV1(maxProtoVer, caps, formats, ext, null);
    }

    /**
     * {@link #encodeHelloV1(byte, byte[], byte[], byte[])} plus the
     * {@link #CAP_EXT_SESSION_PARAMS} block when {@code sessionParams} is non-empty
     * ({@code ext[0]} must then carry the bit): {@code [u16 LE count]} then each
     * {@code {name, value}} as {@code [u16 LE len][utf8]} twice.
     */
    public static ByteBuf encodeHelloV1(byte maxProtoVer, byte[] caps, byte[] formats, byte[] ext,
                                        String[][] sessionParams) {
        byte[] block = new byte[0];
        if (sessionParams != null && sessionParams.length > 0) {
            java.io.ByteArrayOutputStream b = new java.io.ByteArrayOutputStream();
            b.write(sessionParams.length & 0xFF);
            b.write((sessionParams.length >> 8) & 0xFF);
            for (String[] kv : sessionParams) {
                for (String part : kv) {
                    byte[] u = part.getBytes(StandardCharsets.UTF_8);
                    b.write(u.length & 0xFF);
                    b.write((u.length >> 8) & 0xFF);
                    b.write(u, 0, u.length);
                }
            }
            block = b.toByteArray();
        }
        int payloadLen = 1 + 1 + caps.length + 1 + formats.length + (ext.length > 0 ? 1 + ext.length : 0)
                + block.length;
        ByteBuf buf = Unpooled.buffer(V1_HEADER_SIZE + payloadLen);
        // v1 header: [u32 len][u16 stream=0][u8 type][u8 flags]
        buf.writeIntLE(payloadLen);
        buf.writeShortLE(0);
        buf.writeByte(MSG_HELLO);
        buf.writeByte(0);
        // payload
        buf.writeByte(maxProtoVer);
        buf.writeByte(caps.length);
        buf.writeBytes(caps);
        buf.writeByte(formats.length);
        buf.writeBytes(formats);
        if (ext.length > 0) {
            buf.writeByte(ext.length);
            buf.writeBytes(ext);
        }
        buf.writeBytes(block);
        return buf;
    }

    /** Encode a v2 frame. */
    public static ByteBuf encodeV2(int msgType, byte flags, long streamId, byte[] payload) {
        // Built in a plain array and wrapped: `Unpooled.buffer` goes through
        // Netty's instrumented heap allocator, whose per-allocation LongAdder
        // accounting showed up in the per-statement profile (JFR, 32 threads).
        byte[] out = new byte[V2_HEADER_SIZE + payload.length];
        out[0] = PROTOCOL_V2;
        out[1] = (byte) msgType;
        out[2] = flags;
        writeIntLE(out, 3, (int) (streamId & 0xFFFF_FFFFL));
        writeIntLE(out, 7, payload.length);
        System.arraycopy(payload, 0, out, V2_HEADER_SIZE, payload.length);
        return Unpooled.wrappedBuffer(out);
    }

    /** Encode a v1 frame (used only during handshake / when downgraded). */
    public static ByteBuf encodeV1(int msgType, byte flags, int streamId, byte[] payload) {
        ByteBuf buf = Unpooled.buffer(V1_HEADER_SIZE + payload.length);
        buf.writeIntLE(payload.length);
        buf.writeShortLE(streamId & 0xFFFF);
        buf.writeByte(msgType);
        buf.writeByte(flags);
        if (payload.length > 0) buf.writeBytes(payload);
        return buf;
    }

    public static ByteBuf encodeQueryV2(long streamId, String sql) {
        return encodeV2(MSG_QUERY, FLAG_END_STREAM, streamId,
                sql.getBytes(StandardCharsets.UTF_8));
    }

    /** {@link #MSG_PREPARE} payload. */
    public static byte[] encodePreparePayload(String name, String sql) {
        byte[] n = name.getBytes(StandardCharsets.UTF_8);
        byte[] q = sql.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[8 + n.length + q.length];
        writeIntLE(out, 0, n.length);
        System.arraycopy(n, 0, out, 4, n.length);
        writeIntLE(out, 4 + n.length, q.length);
        System.arraycopy(q, 0, out, 8 + n.length, q.length);
        return out;
    }

    /** {@link #MSG_CLOSE_STMT} payload. */
    public static byte[] encodeNamePayload(String name) {
        byte[] n = name.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[4 + n.length];
        writeIntLE(out, 0, n.length);
        System.arraycopy(n, 0, out, 4, n.length);
        return out;
    }

    /**
     * True when every value can travel as a typed {@link #MSG_EXECUTE}
     * parameter: null, Boolean, Byte/Short/Integer/Long, String; and, when the
     * server echoed {@link #CAP_EXT_TYPED_PARAMS} ({@code typed}), the values
     * {@link TypedParams#typedBindable} accepts (Double, Float, BigDecimal,
     * Timestamp, Date, Time, OffsetDateTime, byte[], UUID). Anything else
     * (arrays, java.time local types, a value whose inline spelling the server
     * would not reproduce byte for byte) keeps the text path, where the driver
     * renders it exactly as it always has.
     */
    public static boolean bindable(Object[] params, int n, boolean typed) {
        return bindable(params, n, typed, false);
    }

    /**
     * {@link #bindable(Object[], int, boolean)}, and with {@code arrays} (the
     * server echoed {@link #CAP_EXT_ARRAY_PARAMS}) an {@link ArrayParams.Encoded}
     * array travels as tag 17. The caller converts each {@code java.sql.Array}
     * to its {@link ArrayParams.Encoded} form first.
     */
    public static boolean bindable(Object[] params, int n, boolean typed, boolean arrays) {
        for (int i = 0; i < n; i++) {
            Object v = params[i];
            if (v == null || v instanceof String || v instanceof Long || v instanceof Integer
                    || v instanceof Short || v instanceof Byte || v instanceof Boolean) continue;
            if (typed && TypedParams.typedBindable(v)) continue;
            if (arrays && v instanceof ArrayParams.Encoded) continue;
            return false;
        }
        return true;
    }

    /** {@link #bindable(Object[], int, boolean)} without typed parameters (tags 0..3 only). */
    public static boolean bindable(Object[] params, int n) {
        return bindable(params, n, false);
    }

    /** {@link #MSG_EXECUTE} payload; the values must satisfy {@link #bindable}. */
    public static byte[] encodeExecutePayload(byte[] name, Object[] params, int n) {
        int size = 4 + name.length + 2;
        byte[][] text = null;
        for (int i = 0; i < n; i++) {
            Object v = params[i];
            if (v == null) size += 1;
            else if (v instanceof String) {
                if (text == null) text = new byte[n][];
                text[i] = ((String) v).getBytes(StandardCharsets.UTF_8);
                size += 5 + text[i].length;
            } else if (v instanceof Boolean) size += 2;
            else if (isInteger(v)) size += 9;
            else if (v instanceof ArrayParams.Encoded) size += ((ArrayParams.Encoded) v).bytes.length;
            else size += TypedParams.size(v);
        }
        byte[] out = new byte[size];
        writeIntLE(out, 0, name.length);
        System.arraycopy(name, 0, out, 4, name.length);
        int p = 4 + name.length;
        out[p++] = (byte) n;
        out[p++] = (byte) (n >>> 8);
        for (int i = 0; i < n; i++) {
            Object v = params[i];
            if (v == null) {
                out[p++] = PARAM_NULL;
            } else if (v instanceof String) {
                out[p++] = PARAM_TEXT;
                writeIntLE(out, p, text[i].length);
                System.arraycopy(text[i], 0, out, p + 4, text[i].length);
                p += 4 + text[i].length;
            } else if (v instanceof Boolean) {
                out[p++] = PARAM_BOOL;
                out[p++] = (byte) (((Boolean) v) ? 1 : 0);
            } else if (isInteger(v)) {
                out[p++] = PARAM_I64;
                long l = ((Number) v).longValue();
                for (int b = 0; b < 8; b++) out[p++] = (byte) (l >>> (8 * b));
            } else if (v instanceof ArrayParams.Encoded) {
                byte[] a = ((ArrayParams.Encoded) v).bytes;
                System.arraycopy(a, 0, out, p, a.length);
                p += a.length;
            } else {
                p = TypedParams.write(out, p, v);
            }
        }
        return out;
    }

    private static boolean isInteger(Object v) {
        return v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte;
    }

    public static ByteBuf encodeQueryV1(int streamId, String sql) {
        return encodeV1(MSG_QUERY, (byte) 0, streamId,
                sql.getBytes(StandardCharsets.UTF_8));
    }

    public static ByteBuf encodeBatchV2(long streamId, List<String> sqls) {
        byte[] payload = encodeBatchPayload(sqls);
        return encodeV2(MSG_BATCH, FLAG_END_STREAM, streamId, payload);
    }

    public static ByteBuf encodeBatchV1(int streamId, List<String> sqls) {
        byte[] payload = encodeBatchPayload(sqls);
        return encodeV1(MSG_BATCH, (byte) 0, streamId, payload);
    }

    private static byte[] encodeBatchPayload(List<String> sqls) {
        int total = 4;
        byte[][] enc = new byte[sqls.size()][];
        for (int i = 0; i < sqls.size(); i++) {
            enc[i] = sqls.get(i).getBytes(StandardCharsets.UTF_8);
            total += 4 + enc[i].length;
        }
        byte[] out = new byte[total];
        // [u32 count][u32 len, bytes, ...]
        writeIntLE(out, 0, sqls.size());
        int off = 4;
        for (byte[] b : enc) {
            writeIntLE(out, off, b.length);
            off += 4;
            System.arraycopy(b, 0, out, off, b.length);
            off += b.length;
        }
        return out;
    }

    /**
     * Encode the AUTH payload shared by v1 {@code MSG_AUTH} and v2
     * {@code F_AUTH}:
     *
     * <pre>
     *   [u32_le userlen][user utf8][u32_le passlen][pass utf8]
     * </pre>
     *
     * <p>Byte-for-byte the layout the server decodes in
     * the server's native-wire codec ({@code parse_auth_payload}). Both lengths are
     * BYTE counts of the UTF-8 encoding, not character counts, so non-ASCII
     * credentials frame correctly.
     *
     * <p>A {@code null} password is encoded as a zero-length field - the
     * server accepts an empty password (it only rejects an empty
     * <em>username</em>), so a password-less role still authenticates.
     *
     * @throws IllegalArgumentException if the username is null/empty (the
     *         server's decoder rejects it, which would surface as an opaque
     *         "malformed payload" auth failure) or either field exceeds
     *         {@link #MAX_AUTH_FIELD_LEN} bytes.
     */
    public static byte[] encodeAuthPayload(String user, String password) {
        if (user == null || user.isEmpty()) {
            throw new IllegalArgumentException(
                    "PDB wire AUTH: username must not be empty");
        }
        byte[] u = user.getBytes(StandardCharsets.UTF_8);
        byte[] p = (password == null ? "" : password).getBytes(StandardCharsets.UTF_8);
        if (u.length > MAX_AUTH_FIELD_LEN) {
            throw new IllegalArgumentException(
                    "PDB wire AUTH: username is " + u.length
                    + " bytes, exceeds the server limit of " + MAX_AUTH_FIELD_LEN);
        }
        if (p.length > MAX_AUTH_FIELD_LEN) {
            throw new IllegalArgumentException(
                    "PDB wire AUTH: password is " + p.length
                    + " bytes, exceeds the server limit of " + MAX_AUTH_FIELD_LEN);
        }
        byte[] out = new byte[4 + u.length + 4 + p.length];
        writeIntLE(out, 0, u.length);
        System.arraycopy(u, 0, out, 4, u.length);
        writeIntLE(out, 4 + u.length, p.length);
        System.arraycopy(p, 0, out, 8 + u.length, p.length);
        return out;
    }

    /** v2-framed credential frame ({@code F_AUTH}). */
    public static ByteBuf encodeAuthV2(long streamId, String user, String password) {
        return encodeV2(MSG_AUTH, FLAG_END_STREAM, streamId,
                encodeAuthPayload(user, password));
    }

    /** v1-framed credential frame ({@code MSG_AUTH}). */
    public static ByteBuf encodeAuthV1(int streamId, String user, String password) {
        return encodeV1(MSG_AUTH, (byte) 0, streamId,
                encodeAuthPayload(user, password));
    }

    public static ByteBuf encodePingV2() {
        return encodeV2(MSG_PING, FLAG_END_STREAM, 0L, new byte[0]);
    }

    public static ByteBuf encodePingV1() {
        return encodeV1(MSG_PING, (byte) 0, 0, new byte[0]);
    }

    public static ByteBuf encodeRstStream(long streamId, int errorCode) {
        byte[] p = new byte[2];
        p[0] = (byte) (errorCode & 0xFF);
        p[1] = (byte) ((errorCode >> 8) & 0xFF);
        return encodeV2(MSG_RST_STREAM_C, (byte) 0, streamId, p);
    }

    public static ByteBuf encodeGoaway(long lastStreamId, int errorCode) {
        byte[] p = new byte[6];
        writeIntLE(p, 0, (int) (lastStreamId & 0xFFFFFFFFL));
        p[4] = (byte) (errorCode & 0xFF);
        p[5] = (byte) ((errorCode >> 8) & 0xFF);
        return encodeV2(MSG_GOAWAY, (byte) 0, 0L, p);
    }

    public static ByteBuf encodeClose() {
        return encodeV2(MSG_CLOSE, FLAG_END_STREAM, 0L, new byte[0]);
    }

    /** Decode an OK frame's affected-rows count (LE u64). */
    /**
     * The whole payload of a BULK BEGIN request:
     * {@code [BULK_BEGIN][u16 LE len][relation utf8][u16 LE ncols]{[u16 LE len][col utf8]}*[u8 flags][u64 LE commit_every_rows]}.
     * Mirrors {@code BulkBegin::encode} in the server's native-wire v2 codec.
     */
    public static byte[] encodeBulkBegin(String relation, java.util.List<String> columns, long commitEveryRows) {
        java.io.ByteArrayOutputStream b = new java.io.ByteArrayOutputStream(32 + relation.length());
        b.write(BULK_BEGIN);
        writeU16Str(b, relation);
        int n = columns == null ? 0 : columns.size();
        if (n > 0xFFFF) throw new IllegalArgumentException("bulk ingest: too many columns (" + n + ")");
        b.write(n & 0xFF);
        b.write((n >>> 8) & 0xFF);
        for (int i = 0; i < n; i++) writeU16Str(b, columns.get(i));
        b.write(0); // flags
        for (int i = 0; i < 8; i++) b.write((int) (commitEveryRows >>> (8 * i)) & 0xFF);
        return b.toByteArray();
    }

    /** The whole payload of a BULK END request: {@code [BULK_END][u64 LE dataFrames]}. */
    public static byte[] encodeBulkEnd(long dataFrames) {
        byte[] out = new byte[9];
        out[0] = BULK_END;
        for (int i = 0; i < 8; i++) out[1 + i] = (byte) (dataFrames >>> (8 * i));
        return out;
    }

    private static void writeU16Str(java.io.ByteArrayOutputStream b, String s) {
        byte[] u = s.getBytes(StandardCharsets.UTF_8);
        if (u.length > 0xFFFF) throw new IllegalArgumentException("bulk ingest: name longer than 65535 bytes");
        b.write(u.length & 0xFF);
        b.write((u.length >>> 8) & 0xFF);
        b.write(u, 0, u.length);
    }

    public static long decodeOkAffected(byte[] payload) {
        if (payload.length < 8) return 0L;
        long v = 0;
        for (int i = 0; i < 8; i++) v |= ((long) (payload[i] & 0xFF)) << (8 * i);
        return v;
    }

    /**
     * Decode a MSG_BATCH_RESULT payload's leading statement count.
     *
     * <p>The batch-result payload is {@code [u32_le count][per-stmt entries...]}
     * - a LEADING little-endian u32, NOT the u64 that {@link #decodeOkAffected}
     * reads. Decoding the first 8 bytes as a u64 yields
     * {@code count | (firstEntry << 32)} - garbage (finding F6). This reads only
     * the 4-byte count.
     */
    public static long decodeBatchCount(byte[] payload) {
        if (payload == null || payload.length < 4) return 0L;
        return readIntLE(payload, 0) & 0xFFFF_FFFFL;
    }

    /**
     * A fully decoded MSG_BATCH_RESULT payload.
     *
     * <p>Layout: {@code [u32_le count]} then {@code count} entries, each
     * {@code [u8 type][body]}: {@link #MSG_OK} → {@code u64_le affected},
     * {@link #MSG_ARROW_RESULT} → {@code u32_le len + Arrow IPC bytes},
     * {@link #MSG_ERROR} → {@code u32_le len + UTF-8 message} (no code). The
     * server stops at the first failing statement, so an ERROR entry is the
     * last one present and {@code count} may be below the statements sent.
     */
    public static final class BatchResult {
        /**
         * Per-entry results of the statements that SUCCEEDED, in order:
         * affected rows for OK, {@code -1} for a row-returning statement.
         */
        public final long[] counts;
        /** The ERROR entry's server message, or {@code null} if none. */
        public final String errorMessage;
        /** Number of entries in the payload (successes plus any ERROR). */
        public final int entries;

        /**
         * The failed entry's frame code and SQLSTATE when the server sent the
         * {@link #BATCH_ENTRY_ERROR_SQLSTATE} form (negotiated with
         * {@link #CAP_EXT_BATCH_SQLSTATE}); {@code -1} / {@code null} for the
         * legacy message-only entry or no failure.
         */
        public final int errorCode;
        public final String errorSqlState;

        BatchResult(long[] counts, String errorMessage, int entries) {
            this(counts, errorMessage, entries, -1, null);
        }

        BatchResult(long[] counts, String errorMessage, int entries, int errorCode, String errorSqlState) {
            this.counts = counts;
            this.errorMessage = errorMessage;
            this.entries = entries;
            this.errorCode = errorCode;
            this.errorSqlState = errorSqlState;
        }
    }

    /**
     * Decode every entry of a MSG_BATCH_RESULT payload.
     *
     * @throws IllegalArgumentException if the payload is truncated, has an
     *         unknown entry type, or carries entries after an ERROR entry
     */
    public static BatchResult decodeBatchResult(byte[] payload) {
        if (payload == null || payload.length < 4) {
            throw new IllegalArgumentException("batch result shorter than its 4-byte count");
        }
        long count = readIntLE(payload, 0) & 0xFFFF_FFFFL;
        // Each entry is at least 1 type byte + 4 or 8 body bytes.
        if (count > (payload.length - 4) / 5L) {
            throw new IllegalArgumentException("batch result claims " + count
                    + " entries in " + payload.length + " bytes");
        }
        long[] counts = new long[(int) count];
        int ok = 0;
        int off = 4;
        String error = null;
        int errCode = -1;
        String errState = null;
        for (int i = 0; i < count; i++) {
            if (error != null) {
                throw new IllegalArgumentException("batch result has entries after an ERROR entry");
            }
            if (off >= payload.length) throw new IllegalArgumentException("batch result truncated");
            byte t = payload[off++];
            if (t == MSG_OK) {
                if (off + 8 > payload.length) throw new IllegalArgumentException("batch result truncated");
                long v = 0;
                for (int b = 0; b < 8; b++) v |= ((long) (payload[off + b] & 0xFF)) << (8 * b);
                off += 8;
                counts[ok++] = v;
            } else if (t == BATCH_ENTRY_ERROR_SQLSTATE) {
                // [u16 code][5 x u8 SQLSTATE][u32 len][message]
                if (off + 11 > payload.length) throw new IllegalArgumentException("batch result truncated");
                errCode = (payload[off] & 0xFF) | ((payload[off + 1] & 0xFF) << 8);
                errState = new String(payload, off + 2, 5, java.nio.charset.StandardCharsets.US_ASCII);
                off += 7;
                long len = readIntLE(payload, off) & 0xFFFF_FFFFL;
                off += 4;
                if (off + len > payload.length) throw new IllegalArgumentException("batch result truncated");
                error = new String(payload, off, (int) len, java.nio.charset.StandardCharsets.UTF_8);
                off += (int) len;
            } else if (t == MSG_ARROW_RESULT || t == MSG_ERROR) {
                if (off + 4 > payload.length) throw new IllegalArgumentException("batch result truncated");
                long len = readIntLE(payload, off) & 0xFFFF_FFFFL;
                off += 4;
                if (off + len > payload.length) throw new IllegalArgumentException("batch result truncated");
                if (t == MSG_ARROW_RESULT) {
                    counts[ok++] = -1L;
                } else {
                    error = new String(payload, off, (int) len, java.nio.charset.StandardCharsets.UTF_8);
                }
                off += (int) len;
            } else {
                throw new IllegalArgumentException("unexpected batch entry type 0x"
                        + Integer.toHexString(t & 0xFF));
            }
        }
        return new BatchResult(java.util.Arrays.copyOf(counts, ok), error, (int) count, errCode, errState);
    }

    /**
     * Parse a HELLO_OK payload (sent in v1 framing).
     * <pre>
     *   [u8 server_proto_version]
     *   [u8 chosen_caps]
     *   [u8 chosen_format]
     *   [u32_le max_concurrent_streams]
     *   [u32_le initial_window_size]
     * </pre>
     */
    public static HelloOk parseHelloOk(byte[] payload) {
        if (payload.length < 3) {
            // legacy v1 servers may send just [u8 version][u8 chosen_format]
            byte ver = payload.length > 0 ? payload[0] : 1;
            byte fmt = payload.length > 1 ? payload[1] : FMT_ARROW_IPC;
            return new HelloOk(ver, (byte) 0, fmt, 1, 0, (byte) 0);
        }
        byte ver  = payload[0];
        byte caps = payload[1];
        byte fmt  = payload[2];
        long maxStreams = 1, initWin = 0;
        if (payload.length >= 11) {
            maxStreams = readIntLE(payload, 3) & 0xFFFF_FFFFL;
            initWin    = readIntLE(payload, 7) & 0xFFFF_FFFFL;
        }
        // Extension capability block after the 11-byte body: [u8 num_ext][ext...].
        byte ext = (payload.length >= 13 && (payload[11] & 0xFF) >= 1) ? payload[12] : 0;
        return new HelloOk(ver, caps, fmt, maxStreams, initWin, ext);
    }

    public static final class HelloOk {
        public final byte serverVersion;
        public final byte chosenCaps;
        public final byte chosenFormat;
        public final long maxConcurrentStreams;
        public final long initialWindowSize;
        /** Ext byte 0 the server echoed (0 when HELLO_OK carries no extension block). */
        public final byte extCaps;

        public HelloOk(byte serverVersion, byte chosenCaps, byte chosenFormat,
                       long maxConcurrentStreams, long initialWindowSize) {
            this(serverVersion, chosenCaps, chosenFormat, maxConcurrentStreams, initialWindowSize, (byte) 0);
        }

        public HelloOk(byte serverVersion, byte chosenCaps, byte chosenFormat,
                       long maxConcurrentStreams, long initialWindowSize, byte extCaps) {
            this.serverVersion = serverVersion;
            this.chosenCaps = chosenCaps;
            this.chosenFormat = chosenFormat;
            this.maxConcurrentStreams = maxConcurrentStreams;
            this.initialWindowSize = initialWindowSize;
            this.extCaps = extCaps;
        }

        public boolean isV2()           { return (chosenCaps & CAP_V2_MUX) != 0; }
        public boolean isCompressed()   { return (chosenCaps & CAP_COMPRESSION) != 0; }
        public boolean isTlsAlpn()      { return (chosenCaps & CAP_TLS_NEXT_PROTO) != 0; }
        /**
         * True when the server demands a credential frame before it will serve
         * anything else. A legacy two-byte HELLO_OK carries no caps byte, so
         * {@code chosenCaps} is 0 there and this reads false - such a server
         * still refuses frame by frame, the client just learns one round trip
         * later.
         */
        public boolean isAuthRequired() { return (chosenCaps & CAP_AUTH_REQUIRED) != 0; }
        /** The server serves {@link #MSG_PREPARE} / {@link #MSG_EXECUTE}. */
        public boolean supportsPrepared() { return isV2() && (chosenCaps & CAP_PREPARED) != 0; }
        /** The server accepts the typed {@link #MSG_EXECUTE} tags 4..16 ({@link TypedParams}). */
        public boolean supportsTypedParams() {
            return supportsPrepared() && (extCaps & CAP_EXT_TYPED_PARAMS) != 0;
        }
        /** The server applied the HELLO's session parameters ({@link #CAP_EXT_SESSION_PARAMS}). */
        public boolean appliedSessionParams() {
            return (extCaps & CAP_EXT_SESSION_PARAMS) != 0;
        }
        /** The server accepts the {@link #MSG_EXECUTE} array tag 17 ({@link #CAP_EXT_ARRAY_PARAMS}). */
        public boolean supportsArrayParams() {
            return supportsPrepared() && (extCaps & CAP_EXT_ARRAY_PARAMS) != 0;
        }
        /** The server serves {@link #MSG_BULK} (it echoed {@link #CAP_EXT_BULK_INGEST}). */
        public boolean supportsBulkIngest() {
            return isV2() && (extCaps & CAP_EXT_BULK_INGEST) != 0;
        }
        /** Failed batch entries carry their SQLSTATE ({@link #CAP_EXT_BATCH_SQLSTATE}). */
        public boolean supportsBatchSqlState() {
            return (extCaps & CAP_EXT_BATCH_SQLSTATE) != 0;
        }
        /** The server delivers {@link #MSG_NOTICE} frames ({@link #CAP_STATEMENT_NOTICE}). */
        public boolean supportsStatementNotice() {
            return (chosenCaps & CAP_STATEMENT_NOTICE) != 0;
        }
        /** Unconstrained numerics arrive as per-value-scale text ({@link #CAP_NUMERIC_TEXT}). */
        public boolean numericText() {
            return (chosenCaps & CAP_NUMERIC_TEXT) != 0;
        }

        /**
         * The protocol this handshake selected, named as the server's
         * {@code SHOW pdb.wire_protocol} names it.
         */
        public String protocolName() {
            return isV2() ? "pdb-native-v2" : "pdb-native-v1";
        }

        /**
         * The negotiated capabilities, named as the server's
         * {@code SHOW pdb.wire_capabilities} names them (the server's
         * the server's capability-name table): every echoed caps-byte bit
         * except {@link #CAP_AUTH_REQUIRED} (server posture, not a capability)
         * and every echoed extension bit. Sorted.
         */
        public java.util.SortedSet<String> capabilityNames() {
            java.util.SortedSet<String> n = new java.util.TreeSet<>();
            if ((chosenCaps & CAP_V2_MUX) != 0) n.add("v2_mux");
            if ((chosenCaps & CAP_COMPRESSION) != 0) n.add("compression");
            if ((chosenCaps & CAP_TLS_NEXT_PROTO) != 0) n.add("tls");
            if ((chosenCaps & CAP_NUMERIC_TEXT) != 0) n.add("numeric_text");
            if ((chosenCaps & CAP_STATEMENT_NOTICE) != 0) n.add("statement_notice");
            if ((chosenCaps & CAP_PREPARED) != 0) n.add("prepared");
            if ((extCaps & CAP_EXT_TYPED_PARAMS) != 0) n.add("typed_params");
            if ((extCaps & CAP_EXT_BULK_INGEST) != 0) n.add("bulk_ingest");
            if ((extCaps & CAP_EXT_BATCH_SQLSTATE) != 0) n.add("batch_sqlstate");
            if ((extCaps & CAP_EXT_SESSION_PARAMS) != 0) n.add("session_params");
            if ((extCaps & CAP_EXT_ARRAY_PARAMS) != 0) n.add("array_params");
            return n;
        }
    }

    // ── tiny helpers ─────────────────────────────────────────────────────
    private static void writeIntLE(byte[] buf, int off, int v) {
        buf[off]     = (byte) (v & 0xFF);
        buf[off + 1] = (byte) ((v >> 8) & 0xFF);
        buf[off + 2] = (byte) ((v >> 16) & 0xFF);
        buf[off + 3] = (byte) ((v >> 24) & 0xFF);
    }
    private static int readIntLE(byte[] buf, int off) {
        return (buf[off] & 0xFF)
             | ((buf[off + 1] & 0xFF) << 8)
             | ((buf[off + 2] & 0xFF) << 16)
             | ((buf[off + 3] & 0xFF) << 24);
    }
}
