# particledb-jdbc benchmarks

Standalone `main` programs, not part of the Maven build or the published jar.
Run them with the driver's classes and dependencies on the classpath (Java's
single-file launcher works: `java -cp <driver classpath> <File>.java ARGS`).

- `src/main/java/org/particledb/bench/HammerdbCompatBench.java` - a minimal
  TPC-C NEWORD-shaped insert driver (TPS, one connection per thread), the
  baseline for "JVM apps on the native wire".
- `src/main/java/org/particledb/bench/ArrayUpsertLatencyBench.java` - per-statement
  latency of a Conductor-style task upsert with `index_data` bound by
  `setArray`, `serverPrepare=false` (everything inlined as SQL text, what the
  driver did for any statement with an array through 1.0.8) against the default
  (array bound as a native-wire EXECUTE value). Alternating rounds on one
  server; `SHAPE=plain` drops the `?::jsonb` casts and the `DO UPDATE ... WHERE`
  (a shape the server's bound plans cover) to price binding once the plan is
  served without a parse; `PDB_PID` adds server CPU per statement.
- `src/main/java/org/particledb/bench/SocketSharingBench.java` - T threads of
  prepared point SELECTs over ONE multiplexed Connection versus one Connection
  per thread, alternating rounds: the evidence behind the driver's "one socket
  per Connection, multiplex inside it" decision.
