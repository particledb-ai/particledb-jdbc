# src/main/java/org/particledb

The ParticleDB JDBC driver (`org.particledb.Driver`, Maven `ai.particledb:particledb-jdbc`):
the native-wire client every other SDK is measured against (the SDK conformance
suite, `conformance/SPEC.md`, runs it as the reference).

| file | what it is for |
|---|---|
| `Driver.java` | SPI entry point; `DEFAULT_PORT`, the one place the driver spells the native wire's default port |
| `package-info.java` | Package Javadoc: Java 11 floor, no JVM flags, where Apache Arrow Java is (optionally) used |
| `Pgcompat.java` | URL / `Properties` parsing (`jdbc:particledb://host[:port]/db?k=v`) and pgjdbc-compatibility switches. Parse errors omit the original URL. |
| `PdbConnection.java` | `java.sql.Connection` over one multiplexed `StreamMux`: lazy transactions, isolation / read-only / schema, savepoints (SQL SAVEPOINT / ROLLBACK TO / RELEASE), server-side prepared statements, the request wait with server-side cancel (`await`, `cancelRequest`), the negotiated-protocol accessors (`negotiatedProtocol`, `negotiatedCapabilities`), the native KV frames (`kvSet/kvGet/kvDel/kvIncr`), queue and bulk-ingest entry points |
| `PdbStatement.java` | Plain-text `Statement`: execution, warnings from the server's notices (`runNoting`), `cancel()` (resets the statement's stream), `setFetchSize` (streamed results) |
| `PdbPreparedStatement.java` | `PreparedStatement`: by server-side handle with typed / array values when possible (`isServerPrepared`), rendered text otherwise; batches (bulk ingest for plain INSERTs) |
| `PdbResultSet.java` | `ResultSet` over a decoded Arrow result, or over `StreamedRows` (forward-only) |
| `StreamedRows.java` | Memory-bounded decode of a chunked result, one record batch at a time (`setFetchSize > 0`) |
| `ArrowDecoder.java` | The one decode entry point (`decodePositional(payload, ResultDecoder)`), the `resultDecoder` property (`ArrowDecoder.ResultDecoder`: `auto` default and `builtin` = `ArrowIpcReader`, `arrow` = `ArrowJavaDecoder`; parsed in the `PdbConnection` constructor before any socket opens, passed to every `PdbResultSet` and `StreamedRows` decode; `ResultDecoderTest`) and `Decoded`, which also maps the server's `numeric_text` columns (any `numeric` / `numeric(p,s)` declaration on a Utf8 carrier) back to `BigDecimal`, their `NaN` / `Infinity` / `-Infinity` to `Double` as pgjdbc does (`numericSpecial`), and `vector` columns to `PdbVector`. `PdbResultSet` refuses those specials (and float ones) in `getBigDecimal` and the boolean getter with pgjdbc's SQLSTATE and message, and the integer getters through `PgNumeric`; `NumericSpecialValuesTest`, `NumericSpecialPg17DifferentialTest` |
| `PgNumeric.java` | pgjdbc's integer conversion of a numeric / float cell (`toIntegral`: truncate toward zero, 22003 `Bad value for type int : <text>` out of range) and PostgreSQL's shortest float text it is computed from (`pgFloatText`). Public only so the Java SDK's `Row` converts identically; `PgNumericTest`, `NumericIntegerPg17DifferentialTest` |
| `ArrowIpcReader.java` | The default decoder: pure-Java Arrow IPC stream reader over a heap `byte[]` (own flatbuffer table reader, every Arrow type the server can send, dictionaries, validity, offsets; no allocator, no JVM flag). Each value is what Arrow Java's `getObject` gave after the driver's normalisation, except the corrections listed in its class comment; `ArrowIpcReaderDifferentialTest` pins both against Arrow Java and `ArrowIpcReaderGoldenTest` against checked-in golden streams |
| `ArrowIpcWriter.java` | Heap-only Arrow IPC writer for the Utf8 streams of automatic bulk inserts (`BulkIngest.sendText`), with the small forward flatbuffer builder it uses |
| `ArrowJavaDecoder.java` | `resultDecoder=arrow`: decode with Apache Arrow Java (optional dependency; JDK 16+ needs `ADD_OPENS_FLAG`). Linked only after `ArrowDecoder` checked Arrow Java is present. `ArrowModuleFlagsTest` pins flag-free operation and this mode's errors in child JVMs |
| `ArrowBulk.java` | `PdbConnection.bulkLoad` of the caller's own Arrow Java vectors. Every Arrow reference outside `ArrowJavaDecoder` lives here, so `PdbConnection` loads and verifies without Arrow Java on the classpath |
| `PdbVector.java` | A `vector` value (little-endian float32 on the wire; pgvector text from `toString`) |
| `PdbArray.java` | `java.sql.Array` for single-dimension arrays |
| `Timestamptz.java` | The driver's one rule for `timestamptz` in both directions |
| `PdbSqlState.java` | Native error code -> SQLSTATE and the pgjdbc-shaped `SQLException` |
| `PdbDatabaseMetaData.java` / `MetaResultSet.java` / `PgTypeMap.java` | `DatabaseMetaData` |
| `PgCatalogAdapter.java` | pg_catalog introspection rewrites for PG-dialect tools |
| `AuthHandshake.java` | Connect-time credentials and TLS options |
| `BatchExecutor.java` / `BulkIngest.java` | Multi-statement batches; native bulk ingest (BEGIN / DATA / END), text rows encoded with `ArrowIpcWriter` |
| `ClientPreparedCache.java` | Per-connection prepared-statement execution counts |
| `PdbQueue.java` | The native queue (`F_QUEUE`) |
| `wire/` | The native-wire codec and transport (`wire/README.md`) |
| `ds/` | `PdbDataSource` |
| `examples/` | Runnable samples (`SecureConnect`, `TxnInvariantSmoke`) |

Non-obvious constraints:

- **Defaults are the product.** A plain connect asks for EVERY capability the
  driver implements (`StreamMux.connect`); a new server capability the driver
  implements must be added there, and the conformance suite's DEF-002 holds the
  server's record of the negotiation to the list.
- **A timed-out request is reset, not abandoned**: `await` sends
  `RST_STREAM(CANCEL)` and waits `CANCEL_GRACE_SECONDS` for the server's answer;
  only a request that still has not answered closes the connection (its
  outcome is unknown).
- **One session per socket.** The server keeps one session (transaction,
  settings) per connection, so two `Connection`s must never share a socket;
  concurrency inside one `Connection` is what the multiplexing is for.
