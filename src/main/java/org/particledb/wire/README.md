# src/main/java/org/particledb/wire

The native wire's client codec and transport, below the JDBC layer.

| file | what it is for |
|---|---|
| `WireV2Codec.java` | Frame encode/decode for v1 and v2 framing; capability bits (`CAP_*`, ext byte `CAP_EXT_*`); HELLO / HELLO_OK (`HelloOk.capabilityNames()` names the negotiated bits exactly as the server's `SHOW pdb.wire_capabilities` does); batch results incl. the SQLSTATE-bearing failed entry (`0x90`) |
| `StreamMux.java` | One socket, N concurrent streams: the HELLO (what a default connect asks for), the caller-driven read role (`drive`, `driveUntil`), the stream cap, `cancel` (RST_STREAM(CANCEL) on a request's stream), KV frames (`kv`) |
| `StreamState.java` | Per-stream state: result accumulation, notices (`MSG_NOTICE`), and the streamed-result hand-over (`attachStreaming`, `pollChunk`) |
| `TypedParams.java` / `ArrayParams.java` | Typed and array `EXECUTE` parameter encodings |
| `WireTransport.java` / `TcpTransport.java` / `UnixSocketTransport.java` | The byte pipe: TCP, or the server's Unix socket (preferred on loopback) |
| `TlsOptions.java` | `sslMode` parsing |
| `SharedEventLoop.java` | The process-wide Netty group |

Non-obvious constraints:

- A capability bit is a promise in both directions: ask for one only when the
  driver handles what the server then sends (`CAP_STATEMENT_NOTICE` means
  `MSG_NOTICE` frames arrive; `CAP_NUMERIC_TEXT` means unconstrained numerics
  arrive as text).
- A streamed result's chunks are queued for its consumer, who drives the
  socket for the next one; other streams keep being served meanwhile.
