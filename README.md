# ParticleDB JDBC driver

JDBC access to ParticleDB for Java applications and connection pools. Requires Java 11 or later. No JVM flags are needed on any JDK; see [No JVM flags](#no-jvm-flags).

This checkout contains the `1.0.11` release. Earlier published artifacts do not contain all changes in this checkout.

## Install from this checkout

```sh
mvn -DskipTests install
```

```xml
<dependency>
  <groupId>ai.particledb</groupId>
  <artifactId>particledb-jdbc</artifactId>
  <version>1.0.11</version>
</dependency>
```

The driver registers automatically. No `Class.forName` call is needed.

## Start a local server

A server started without options requires authentication but has no password
set, so start it in one of these two ways. For local development on a trusted
machine, turn authentication off and keep every listener on loopback:

```sh
particledb start --no-auth --data-dir ./pdb-dev \
  --http-addr 127.0.0.1:7173 --pg-addr 127.0.0.1:5432
```

To require a password, create the login role from the command line. The
password file holds only the password; keep it readable by you alone.
SCRAM authenticates without TLS; use TLS for remote access.

```sh
particledb start --data-dir ./pdb-dev \
  --http-addr 127.0.0.1:7173 --pg-addr 127.0.0.1:5432 \
  --auth-method scram-sha-256 --pg-username app --pg-password-file ./pdb-password
```

The SDK connects to `localhost:7173` by default. With authentication on,
pass the same user and password:

```java
var properties = new java.util.Properties();
properties.setProperty("user", "app");
properties.setProperty("password", System.getenv("PARTICLEDB_PASSWORD"));
try (var connection = DriverManager.getConnection(
        "jdbc:particledb://localhost:7173/default", properties)) {
    // Use the connection here.
}
```

## Connect and query

Start a local server first (1.1.19-rc.15 or later, port 7173).

```java
import java.sql.DriverManager;

try (var connection = DriverManager.getConnection("jdbc:particledb://localhost:7173/default");
     var query = connection.prepareStatement("SELECT ? AS greeting")) {
    query.setString(1, "hello");
    try (var rows = query.executeQuery()) {
        while (rows.next()) System.out.println(rows.getString("greeting"));
    }
}
```

The driver's URL parser omits connection strings from its error messages.
Pass credentials through `Properties`, keeping passwords out of URLs and logs:

```java
var properties = new java.util.Properties();
properties.setProperty("user", "app");
properties.setProperty("password", System.getenv("PDB_PASSWORD"));
properties.setProperty("sslMode", "verify-full");
properties.setProperty("sslRootCert", "/path/to/ca.pem");
try (var connection = DriverManager.getConnection(
        "jdbc:particledb://db.example.com:7173/default", properties)) {
    // Use the connection here.
}
```

TLS modes are `disable`, `require` (encryption), `verify-ca` (certificate chain) and `verify-full` (chain and hostname). Use `verify-full` for remote connections. `sslCert` and `sslKey` accept a PEM client certificate and PKCS#8 private key. A TLS request never falls back to plaintext or a Unix socket.

## No JVM flags

The driver needs no JVM flags on any JDK from 11 on. It reads every result
type the server sends with its own pure-Java Arrow IPC reader: integers,
floating point, `numeric` (so `SUM` and `AVG` too), text, `bytea`, `boolean`,
`date`, `time`, `timestamp`, `timestamptz`, `interval`, arrays and NULLs.
Apache Arrow Java is not a dependency.

Releases up to JDBC 1.0.9 needed
`--add-opens=java.base/java.nio=ALL-UNNAMED` on JDK 16 and later. You can
remove it; leaving it in place does no harm.

The `resultDecoder` URL parameter or connection property chooses the result decoder:

* `auto` (default) and `builtin`: the driver's own reader.
* `arrow`: Apache Arrow Java. Add `org.apache.arrow:arrow-vector` and
  `org.apache.arrow:arrow-memory-netty` to your application, and on JDK 16 and
  later start the JVM with `--add-opens=java.base/java.nio=ALL-UNNAMED`.
  Without Arrow Java on the classpath, a query that returns rows throws a
  `RuntimeException` (not an `SQLException`) that says so.

Values are case-insensitive; any other value fails when connecting, with a
message listing the valid ones. Set it as a URL parameter,
`jdbc:particledb://localhost:7173/default?resultDecoder=arrow`, as a
`Properties` entry, or with `PdbDataSource.setResultDecoder("arrow")`.

## Parameters and batches

Use `?` placeholders and JDBC setters. Every placeholder must be assigned; use `setNull` for an SQL NULL. Unassigned parameters fail with `07001`, and invalid indexes fail with `07009` before execution. Values stay assigned until changed or cleared.

```java
try (var insert = connection.prepareStatement("INSERT INTO items (id, name) VALUES (?, ?)")) {
    insert.setLong(1, 1);
    insert.setString(2, "first");
    insert.addBatch();
    insert.setLong(1, 2);
    insert.setString(2, "second");
    insert.addBatch();
    int[] counts = insert.executeBatch();
}
```

`real`, `double precision` and `numeric` values `NaN`, `Infinity` and `-Infinity` read as pgjdbc reads them: `getObject` returns a `Float` for `real` and a `Double` for the other two types, `getDouble`, `getFloat` and `getString` return the value, and `getBigDecimal` and the integer getters raise SQLSTATE `22003`. Releases up to 1.0.10 returned a `numeric` special as a `String`.

The integer getters (`getLong`, `getInt`, `getShort`, `getByte`) truncate a fraction toward zero and raise SQLSTATE `22003` for a value outside the target type, as pgjdbc does. Releases up to 1.0.10 wrapped such a value silently (`getLong` on 18446744073709551616 returned 0).

`setObject` supports Java values and explicit JDBC target types. Numeric scale arguments are honored. Calendar overloads use the supplied timezone without changing the Calendar. Use `INSERT ... RETURNING` to read generated values; `getGeneratedKeys()` is not supported.

Batches of parameterized inserts need no JVM flags: an automatic bulk insert is encoded by the driver itself. The explicit `PdbConnection.bulkLoad` overloads take your own Apache Arrow Java vectors, so they need Arrow Java on the classpath and its JVM setup (see [No JVM flags](#no-jvm-flags)).

A batch can apply earlier statements before reporting a later failure. Inspect `BatchUpdateException.getUpdateCounts()`. Use a transaction when all statements must commit together. An automatic bulk insert is atomic.

## Transactions

```java
try (var connection = DriverManager.getConnection("jdbc:particledb://localhost:7173/default")) {
    connection.setAutoCommit(false);
    try {
        // Execute statements on this connection.
        connection.commit();
    } catch (java.sql.SQLException failure) {
        try { connection.rollback(); }
        catch (java.sql.SQLException rollbackFailure) { failure.addSuppressed(rollbackFailure); }
        throw failure;
    }
}
```

Savepoints, isolation and read-only settings are supported. DDL support inside a transaction depends on the server's documented transaction mode. A failed transaction must be rolled back before reuse. Native key-value operations have their own durability rules and are not rolled back with SQL transactions.

## Streaming and timeouts

Set `Statement.setFetchSize` to a positive number to decode results a batch at a time. Close the result and statement when done. A streamed result is forward-only; `setMaxRows` bounds visible rows, not server work or network traffic.

`setQueryTimeout(seconds)` limits a statement, including batches. Its JDBC default is 0, meaning no statement limit. Cancellation requests stop the statement when the server supports them. The connection can be reused after the server confirms the outcome. If no answer arrives within the cancellation grace period, the connection closes because the outcome is unknown. Statements are never automatically replayed.

`setNetworkTimeout(executor, milliseconds)` bounds response waits and closes the connection on expiry. `connectTimeout` is in milliseconds (default 30000). `DataSource.setLoginTimeout` is in seconds and bounds login, including authentication.

Errors preserve SQLSTATE: `23505` for a unique violation, `40001` for a serialization failure, `57014` for cancellation and class `08` for connection failures. Retry a complete transaction only when the application can safely repeat it.

## Connection pools

```java
var source = new org.particledb.ds.PdbDataSource();
source.setHost("localhost");
source.setPort(7173);
source.setDatabase("default");
source.setLoginTimeout(5);

var config = new com.zaxxer.hikari.HikariConfig();
config.setDataSource(source);
config.setMaximumPoolSize(10);
try (var pool = new com.zaxxer.hikari.HikariDataSource(config);
     var connection = pool.getConnection()) {
    // Closing the borrowed connection returns it to the pool.
}
```

HikariCP is an application dependency. The test suite verifies rollback and state reset between borrowers. A JDBC connection represents one database session; concurrent work on it shares session settings and its transaction.

## Scope and validation

Results are read-only and forward-only. Callable statements, LOB factories, generated-key retrieval, field truncation and catalog switching are unsupported. Metadata query failures propagate as SQL exceptions. Consult [CONFORMANCE.md](CONFORMANCE.md) for individual contracts and limits.

```sh
PDB_BIN=$(command -v particledb) mvn -Dpdb.requireServer=true test
PDB_BIN=$(command -v particledb) ./conformance/run_conformance.sh --mode default
PDB_BIN=$(command -v particledb) ./conformance/run_conformance.sh --mode strict
```

Set `PG_ORACLE_URL` to an isolated PostgreSQL 17 database to include differential checks. The tests create and drop their own objects; do not point them at an application database. See [Java and JDBC documentation](https://particledb.ai/sdks/java/) for tested versions. The native wire protocol is summarised in [PROTOCOL.md](PROTOCOL.md).
