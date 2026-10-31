# Changelog - ai.particledb:particledb-jdbc

User-facing changes in each version published to Maven Central. The format is
based on [Keep a Changelog](https://keepachangelog.com/), and this project
adheres to [Semantic Versioning](https://semver.org/). Dates are the Maven
Central publication dates.

## [1.0.11] - 2026-10-27

Requires ParticleDB server 1.1.19-rc.15 or later for the full feature set.

### Fixed
- `NaN`, `Infinity` and `-Infinity` read as pgjdbc reads them from
  PostgreSQL. `getObject` on a `numeric` special returns a `Double` (it
  returned the `String` `"NaN"`). `getBigDecimal` and the integer getters
  raise SQLSTATE `22003`, and `getBoolean` raises `42846`, where a special
  used to read as 0, `Long.MAX_VALUE` or `true`.
- Integer getters no longer wrap silently. `getLong`, `getInt`, `getShort`,
  `getByte` and `getObject(i, Long.class)` (and the other integer classes)
  raise SQLSTATE `22003` for a value outside the target type, as pgjdbc
  does. A fraction still truncates toward zero.
- A `numeric(p,s)` column whose batch contains a `NaN` reads as
  `numeric(p,s)` with `BigDecimal` values; it used to read the whole batch as
  text.

### Known issues
- A numeric special typed only by a literal (`SELECT 'NaN'::numeric`) still
  reads as a `String`: the server sends it as untyped text. Specials from
  table columns and from expressions over them are covered.

## [1.0.10] - 2026-10-26

Version 1.0.9 was not published; its changes are included here.

### Changed
- **Default port is 7173**, the server's default native port. A URL with no
  port (`jdbc:particledb://host/db`) and a `PdbDataSource` with no port
  connect to 7173 (earlier versions defaulted to 5440).
- **No JVM flags on any JDK.** Results are decoded by the driver's own
  pure-Java Arrow IPC reader for every type the server sends;
  `--add-opens=java.base/java.nio=ALL-UNNAMED` is no longer needed and is
  harmless if kept.
- **Apache Arrow Java is optional** and no longer a transitive dependency.
  Runtime dependencies are Netty only. Arrow Java is needed only for the
  `resultDecoder=arrow` option and the `PdbConnection.bulkLoad` overloads
  that take Arrow vectors. Batched plain `INSERT ... VALUES (?, ...)` uses
  native bulk ingest without it.
- New connection property `resultDecoder` = `auto` (default) | `arrow` |
  `builtin`.
- `setObject` of a Java array or `Collection` binds an SQL array (it used to
  inline the value's `toString()`).
- The license is MIT.

### Added
- **Array parameters are bound over the native wire**, never inlined into
  SQL text, when the server supports typed array parameters. Element types:
  text / varchar, int2, int4, int8, float4, float8, bool, numeric, uuid,
  timestamp, timestamptz, date, bytea, json, jsonb; rectangular arrays up to
  6 dimensions. Sources: `createArrayOf` + `setArray`, a foreign
  `java.sql.Array`, `setObject` of a Java array or a `Collection`. Older
  servers get a typed SQL literal instead. An array that cannot be bound
  raises `SQLFeatureNotSupportedException` and nothing is sent.
- `getArray` / `getObject` read uuid[], date[], timestamp[], timestamptz[],
  bytea[] and json[] elements typed, and multidimensional arrays as nested
  typed arrays, as pgjdbc does.
- **Unix-domain socket transport.** `preferUnixSocket=true` (the default)
  connects to `<unixSocketDir>/.s.PDB.<port>` for `localhost`, `127.0.0.1`
  or `::1` on Java 16+, falling back to TCP only when no server is behind
  the path. New options `unixSocketDir` (default `/tmp`) and
  `unixSocketPath`.

### Fixed
- `ResultSetMetaData.getTableName` returns the column's source table (it
  always returned `""`), as pgjdbc does; `""` for computed columns.
- `setObject(i, new int[]{1,2,3})` and the other primitive / boxed array
  shapes bind as SQL arrays, matching pgjdbc against PostgreSQL. An
  `Object[]` mixing SQL types raises `07006`.
- 2-D text arrays round-trip as 2-D arrays.

## [1.0.8] - 2026-10-16

Also covers 1.0.6 (2026-10-05) and 1.0.7 (2026-10-13).

### Added
- **Server-side prepared statements on the native wire.** A
  `PreparedStatement` is prepared once per connection and executed by handle
  with typed parameters. `serverPrepare=false` turns it off; servers without
  the capability are served as before.
- `allowDdlOutsideTransaction` option (default `false`): with
  `autoCommit=false`, a DDL issued while no transaction is open runs on its
  own and adds a `SQLWarning`. Needed by migration tools such as Flyway that
  run every migration inside a transaction.
- `raiseExceptionOnSilentRollback` option (default `false`): a COMMIT that
  the server answered by rolling back a failed transaction raises `25P02`
  instead of returning normally.

### Changed
- **Connections run at READ COMMITTED by default**, the server default, as
  in PostgreSQL. Call `setTransactionIsolation` or start the server with
  `--default-isolation` for a stronger level.
- Timestamps, doubles, decimals, dates, bytes and UUIDs stay on the
  server-prepared path with typed parameters.
- With `autoCommit=false`, a DDL is never run outside the transaction; the
  server refuses a DDL inside a transaction with `25001`, as over the
  PostgreSQL wire.
- Transactions start lazily: the first statement carries the BEGIN, so an
  autocommit-off cycle takes fewer round trips.
- The JVM time zone is sent in the handshake as the session's startup zone,
  so `RESET TimeZone` returns to it, as with pgjdbc.

### Fixed
- `timestamptz` results are read as the instant they are; `getObject`
  returns `java.sql.Timestamp`, and `getObject(i, OffsetDateTime.class)` /
  `Instant.class` work. `getString` on a `timestamp` returns PostgreSQL's
  text form.
- A low-cardinality text column could read as `java.lang.Byte` (the
  dictionary index) against servers up to 1.1.19-rc.7. The driver now
  resolves dictionary-encoded columns for every accessor.
- A negative number bound next to a minus sign (`10-?` bound to -2.5) no
  longer turns into a SQL comment.
- More server errors carry their real SQLSTATE (`22012`, `23505`, `42601`
  and 26 more letter-bearing codes).
- `commit()` of a failed transaction returns normally (the transaction is
  rolled back), as pgjdbc does.
- A query timeout closes the connection, so a statement still running on the
  server can no longer be followed by the next statement on the same socket.
- A failed statement in a batch raises `BatchUpdateException` with the
  update counts of the statements that ran; successful batches return real
  per-statement counts.
- Server errors read like pgjdbc's (`ERROR: <message>`) with the exact
  SQLSTATE.
- `setTransactionIsolation(READ_COMMITTED)` on a fresh connection reaches the
  server; `getTransactionIsolation()` reports the server's level.
- DDL with `autoCommit=false` no longer commits the open transaction.
- `setReadOnly(true)` is enforced (`25006` on writes).
- `getWarnings()` returns the connection's warning chain.
- A request waiting for a stream slot no longer stalls behind buffered
  responses.

## [1.0.5] - 2026-09-17

### Added
- [`CONFORMANCE.md`](CONFORMANCE.md): a JDBC conformance matrix listing what
  an application can rely on and the test that proves it.
- Differential tests against PostgreSQL (test scope only).

### Fixed
- `set_config(name, NULL, true)` resets the value for the transaction only,
  not the session.
- `set_config('search_path', ...)` is applied and returns the value in
  effect, so tools such as Flyway create objects in the intended schema.
- `setTransactionIsolation`, `setReadOnly` and `setSchema` reach the server.
- `commit()` / `rollback()` in auto-commit mode raise an error, as JDBC
  requires.
- A failing DDL inside a transaction no longer leaves the connection without
  a transaction.
- `PreparedStatement.addBatch(String)` and the other SQL-taking forms throw,
  as JDBC requires (they used to drop the statements silently).
- `getResultSet()`, `getUpdateCount()` and `ResultSet.getStatement()` after
  `PreparedStatement.executeQuery()` return the right values.
- `setQueryTimeout` is honoured (`57014` on expiry); `setMaxRows` is
  enforced.
- `Statement.cancel()` refuses instead of doing nothing.
- Closed `Statement` and `ResultSet` objects refuse further use.
- `executeUpdate` on a row-returning statement throws.
- `getBoolean` reads `t` / `f`; `getInt` / `getShort` / `getByte` range-check;
  non-numeric cells raise `SQLException` instead of
  `NumberFormatException`.
- `TIME` columns read in the right zone; `getDate` / `getTimestamp` across
  `DATE` / `TIMESTAMP` columns work; the `Calendar` overloads honour the
  calendar.
- Scroll methods return values that describe the cursor.
- `Float` / `Double` `NaN` and infinities are rendered as valid SQL.
- `DatabaseMetaData.getCrossReference` filters both sides; fourteen
  `DatabaseMetaData` methods return the documented columns.

## [1.0.4] - 2026-08-16

Also covers 1.0.3 (2026-08-13).

### Added
- **Authentication over the native wire.** `user` / `password` from the URL
  or `Properties` are sent during `getConnection()`. A missing credential
  fails with `28000`, a rejected login with `28P01`.

### Fixed
- `??` is the JDBC escape for a literal `?` operator (jsonb `?`, `?|`, `?&`)
  and no longer shifts later parameters.
- Every native-wire error carries its SQLSTATE (it was `null`), thrown as
  the matching JDBC 4 subclass; connection failures are `08001` / `08003`.
- Array columns report `Types.ARRAY`, PostgreSQL's array type names and
  `java.sql.Array` from `getObject`.

## [1.0.2] - 2026-07-26

Also covers 1.0.1 (2026-06-12).

### Changed
- Apache Arrow became a regular dependency so `SELECT` works out of the box
  (made optional again in 1.0.10).
- Compiled with full debug info for readable stack traces.

### Fixed
- By-name getters are case-insensitive.
- `Statement.execute()` returns a `ResultSet` for `WITH`, `VALUES`, `SHOW`,
  `EXPLAIN` and `RETURNING` statements.
- `sslMode` values other than `disable` fail loudly instead of connecting in
  plaintext (TLS support was added later).
- `?` counting ignores literals, quoted identifiers, dollar quotes and
  comments.
- `getObject(i, Class)` converts with range checks; temporal getters handle
  all date / time carriers.
- Duplicate column labels stay addressable; an empty result reports its
  columns.
- Batch update counts are decoded correctly; the connection pool no longer
  leaks downgraded connections.

## [1.0.0] - 2026-05-19

First release on Maven Central as `ai.particledb:particledb-jdbc` (Java
packages are `org.particledb.*`).

### Added
- JDBC driver over the ParticleDB native wire protocol (multiplexed, Arrow
  IPC results).
- Runnable samples in [`samples/`](samples/).

### Changed
- Netty modules are pinned to one version to avoid a mixed-version
  classpath hang during the handshake.
