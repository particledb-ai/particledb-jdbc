# ParticleDB JDBC Samples

Runnable end-to-end examples for `ai.particledb:particledb-jdbc:1.0.11`.

Each main class is self-contained - boot a ParticleDB engine on its
default ports, then run any sample with `mvn exec:exec`.

## Quick start

```bash
# 1. Boot a ParticleDB engine (in another terminal)
particledb start --no-auth --concurrency-control optimistic \
  --pg-addr 127.0.0.1:5432 --pdb-port 7173

# 2. From this directory
cd samples

# 3. Run any sample
mvn exec:exec -Dexec.mainClass=ai.particledb.jdbc.samples.HelloWorld
mvn exec:exec -Dexec.mainClass=ai.particledb.jdbc.samples.CrudRoundtrip
mvn exec:exec -Dexec.mainClass=ai.particledb.jdbc.samples.TransactionDemo
mvn exec:exec -Dexec.mainClass=ai.particledb.jdbc.samples.HikariCpDemo
mvn exec:exec -Dexec.mainClass=ai.particledb.jdbc.samples.PgJdbcCompat
```

`pom.xml` already passes the JVM flags Arrow Java needs
(`--add-opens=java.base/java.nio=ALL-UNNAMED`,
`--add-opens=java.base/sun.nio.ch=ALL-UNNAMED`) so no extra setup
beyond Java 17+ and Maven 3.9+.

## What each sample shows

| Sample | Transport | Demonstrates |
|---|---|---|
| `HelloWorld` | PDB native wire | Smallest possible round-trip: `SELECT 1`. |
| `CrudRoundtrip` | PDB native wire | CREATE TABLE, parameterised INSERT/UPDATE/DELETE via `PreparedStatement`. |
| `TransactionDemo` | PDB native wire | `setAutoCommit(false)` + BEGIN/COMMIT, RYOW. **Note:** PDB native wire treats BEGIN/COMMIT/ROLLBACK as batch markers, not full ACID brackets - use the PG-wire path (`PgJdbcCompat`) if you need rollback-after-write. |
| `HikariCpDemo` | PDB native wire | Connection-pool integration with HikariCP. |
| `PgJdbcCompat` | PG wire via pgjdbc | Drop-in test that PDB's PG wire (port 5432) is wire-compatible with stock pgjdbc. Also serves as the regression test for the 5-SELECT prepared-statement promotion bug fixed in engine commit on 2026-05-18. |

## Overriding the URL

All PDB-native-wire samples honour `PDB_URL`:

```bash
PDB_URL="jdbc:particledb://prod-host:5443/myapp?sslMode=require&sslRootCert=/etc/ssl/ca.crt" \
  mvn exec:exec -Dexec.mainClass=ai.particledb.jdbc.samples.HelloWorld
```

The PG-wire sample honours `PG_URL`:

```bash
PG_URL="jdbc:postgresql://prod-host:5432/myapp" \
  mvn exec:exec -Dexec.mainClass=ai.particledb.jdbc.samples.PgJdbcCompat
```

## Not a Maven Central artifact

This module is intentionally NOT published to Maven Central. It's a
working scaffold for your own apps to copy. The reference driver is
`ai.particledb:particledb-jdbc:1.0.11` - the only thing that needs to
be on your classpath.
