# Contributing

Thank you for helping improve the ParticleDB JDBC driver.

## Build

You need Java 11 or later and Maven 3.6 or later.

```
mvn verify
```

## Test

Most tests need a live server. Install it with
`curl -fsSL https://get.particledb.ai | bash` and point `PDB_BIN` at the
`particledb` binary; each test class starts its own server on free ports:

```
PDB_BIN=$(command -v particledb) mvn verify
```

Run a single class with `-Dtest=ClassName`. Before you open a pull request,
also run the conformance suite against a server on `localhost:7173`:

```
./conformance/run_conformance.sh
```

## Code style

- Follow the style of the surrounding code: four-space indents, no tabs.
- Keep the driver free of new runtime dependencies unless there is a strong
  reason.
- Public API needs complete Javadoc.
- Comments explain why the code does something, in plain sentences.
- Every behaviour change comes with a test that fails without it.

## Reporting issues

Open an issue at https://github.com/particledb-ai/particledb-jdbc/issues with
the driver version, the server version, the Java version, a minimal
reproduction and the full stack trace. Report security problems privately as
described in [SECURITY.md](SECURITY.md).
