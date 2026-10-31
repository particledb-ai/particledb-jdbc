# conformance

The JDBC driver's SDK-conformance bundle: the REFERENCE adapter. One command
runs the whole suite against this driver built from source:

```
./run_conformance.sh [--mode default|strict] [--results FILE]
```

`PDB_BIN` defaults to `$PARTICLEDB_BIN`, else `particledb` on `PATH` (install
it with `curl -fsSL https://get.particledb.ai | bash`).

| entry | what it is for |
|---|---|
| `run_conformance.sh` | Starts a fresh server (unless `PDB_CONF_PORT` + `PDB_CONF_CONTROL` name one), builds the driver, compiles the adapter against it, runs the mode's cases in a 256 MiB heap, validates the results file |
| `src/org/particledb/conformance/` | The adapter (`README.md` there) |
| `SPEC.md` | The suite specification: groups, canonical value forms, results contract |
| `cases/`, `lib/` | The shared case files and runner library, identical in every ParticleDB SDK repository -- change every copy together, never one alone |

Constraints: the adapter connects with the documented zero-option URL
(`jdbc:particledb://HOST:PORT/`) because the defaults are what is under test;
`PDB_CONF_MVN_OFFLINE=1` makes the Maven steps offline. The adapter JVM runs
as a consumer would: no JVM module flags and no Apache Arrow Java on the
classpath (an optional dependency, excluded when the classpath is resolved).
