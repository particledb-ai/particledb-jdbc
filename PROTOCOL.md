# ParticleDB native wire protocol (summary)

This driver speaks ParticleDB's native wire protocol on the server's native
port (7173 by default). This page summarises the framing as implemented in
`src/main/java/org/particledb/wire/WireV2Codec.java`, which is the reference
client. User documentation for the server lives at <https://particledb.ai/docs/>.

All integers are little-endian.

## Handshake

The client opens a TCP connection and sends `HELLO` in **protocol 1 framing**
(8-byte header):

```
[u32 payload_len][u16 stream_id = 0][u8 type][u8 flags][payload ...]
```

`HELLO` payload:

```
[u8 max_protocol_version]
[u8 n_caps][caps ...]            capability bytes (see below)
[u8 n_formats][formats ...]      0x01 = Arrow IPC, 0x02 = JSON
[u8 n_ext][ext ...]              optional extension capability bytes
[session parameter block]        optional, when CAP_EXT_SESSION_PARAMS is set:
                                 [u16 count] then {name, value} each as [u16 len][utf8]
```

The server answers `HELLO_OK` (also protocol 1 framing) echoing the
capabilities and protocol version it accepts. When both sides agree on
protocol 2, every later frame uses the **protocol 2 header** (11 bytes):

```
[u8 version = 2][u8 type][u8 flags][u32 stream_id][u32 payload_len][payload ...]
```

The maximum payload is 16 MiB per frame; larger results are split into
chunks (`FLAG_MORE_CHUNKS`).

If the server requires authentication, the client sends `AUTH` (0x09) after
`HELLO_OK` and before any other frame; frames sent before a successful `AUTH`
fail with error `0x13` (auth required), bad credentials with `0x14`.

## TLS

The client requests TLS by setting `CAP_TLS_NEXT_PROTO` (0x08) in `HELLO`. If
the server echoes the bit in `HELLO_OK`, both sides run a TLS handshake on the
same socket before any further frame. If the client asked for TLS and the
server does not echo the bit, the driver fails the connection; it never falls
back to plaintext silently. See `sslMode` in [README.md](README.md).

## Multiplexing

Protocol 2 carries many concurrent requests over one connection. Each request
uses its own `stream_id`; a response frame carries the stream id of the
request it answers, and `FLAG_END_STREAM` marks a stream's last frame. Either
side may cancel a stream (`RST_STREAM`), and the server may send `GOAWAY`
before closing the connection.

## Frame types

| Client to server | Code | Server to client | Code |
|---|---|---|---|
| `HELLO` | 0x00 | `HELLO_OK` | 0x80 |
| `QUERY` | 0x01 | `ARROW_RESULT` | 0x81 |
| `BATCH` | 0x02 | `OK` | 0x82 |
| `PING` | 0x03 | `ERROR` | 0x83 |
| `CLOSE` | 0x04 | `PONG` | 0x84 |
| `SETTINGS` | 0x05 | `BATCH_RESULT` | 0x85 |
| `RST_STREAM` | 0x06 | `ARROW_STREAM` | 0x86 |
| `WINDOW_UPDATE` | 0x07 | `KV_BULK` | 0x87 |
| `AUTH` | 0x09 | `KV_NIL` | 0x88 |
| `KV_SET` / `KV_GET` / `KV_DEL` / `KV_INCR` | 0x10-0x13 | `RST_STREAM` | 0x89 |
| `PREPARE` | 0x20 | `GOAWAY` | 0x8a |
| `EXECUTE` | 0x21 | `SETTINGS_ACK` | 0x8b |
| `CLOSE_STMT` | 0x22 | `NOTICE` | 0x8c |
| `QUEUE` | 0x30 | `PREPARE_OK` | 0x8d |
| `BULK` (begin / data / end / abort) | 0x40 | `QUEUE_RESULT` | 0x8e |

Query results arrive as Apache Arrow IPC streams.

## Frame flags

| Flag | Bit | Meaning |
|---|---|---|
| `END_STREAM` | 0x01 | Last frame of this stream |
| `MORE_CHUNKS` | 0x02 | Payload continues in the next frame |
| `COMPRESSED` | 0x04 | Payload is compressed (negotiated) |
| `TXN_BEGIN` / `TXN_OPEN` / `TXN_FAILED` / `TXN_ROLLED_BACK` | 0x08 / 0x10 / 0x20 / 0x40 | Transaction state reported with a response |

## Prepared statements and typed parameters

`PREPARE` returns a statement handle in `PREPARE_OK`; `EXECUTE` binds
parameters, each prefixed by a one-byte type tag:

0 NULL, 1 int8, 2 text, 3 bool, 4 float8, 5 float4, 6 numeric, 7 date,
8 timestamp, 9 timestamptz, 10 time, 11 uuid, 12 bytea, 13 int2, 14 int4,
15 json, 16 jsonb, 17 array.

Tags above 3 are used only when the server advertises the matching extension
capability in `HELLO_OK`.

## Stream error codes

`0x00` no error, `0x01` protocol error, `0x02` internal error, `0x03` flow
control, `0x04` stream limit, `0x05` frame size, `0x06` refused stream,
`0x07` cancel, `0x08` idle timeout. SQL errors carry a SQLSTATE in the `ERROR`
payload.
