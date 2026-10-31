# conformance/src/org/particledb/conformance

Java conformance adapters. The harness half is shared by every Java adapter
(the Java SDK's bundle compiles these files too).

| file | what it is for |
|---|---|
| `Harness.java` | The runner contract (env in, results file out), the case loader, the `script`-case interpreter, the canonical value comparison (`conformance/SPEC.md` §4), per-case timeout |
| `Json.java` | Minimal JSON reader/writer (no dependencies beyond the SDK under test) |
| `Resp.java` | Minimal RESP2 client: the independent reader/writer for the KV parity cases |
| `JdbcAdapter.java` | The JDBC driver's adapter: `Driver`/`Session` over `java.sql`, typed binding and typed getters per canonical type, and every `custom` case |
