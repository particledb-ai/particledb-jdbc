# src/test/resources/arrow-golden

Golden Arrow IPC streams for the driver's built-in reader (`ArrowIpcReader`).
`ArrowIpcReaderGoldenTest` decodes them on a plain JVM (no Apache Arrow Java,
no JVM flag) and checks every cell.

| file | what it is for |
|---|---|
| `<case>.arrow` (one per `GoldenCases` case) | One column `c` of one Arrow type, two record batches, written by Apache Arrow Java 15 (`ArrowGolden`); `float16.arrow` is hand-built (`GoldenMutations`), since Arrow Java 15 has no Float16 vector |
| `live-*.arrow` | Result streams captured from a server built from the release commit, for the queries in `ArrowIpcReaderGoldenTest.LIVE_QUERIES` (what the engine really sends) |

Regenerate, do not hand-edit:

- type goldens: `mvn test -Dtest=ArrowIpcReaderDifferentialTest -Dsurefire.failIfNoSpecifiedTests=false -Dpdb.writeGolden=$PWD/src/test/resources/arrow-golden`
- live goldens: the same with `-Dtest=ResultDecoderLiveDifferentialTest -Dpdb.captureGolden=...` and `PDB_BIN` set; then update `ArrowIpcReaderGoldenTest.LIVE_ROWS` from the printed rows.

The live streams carry the capturing machine's session zone in the
`timestamptz` field's `pdb:tz` metadata; the test checks decoded values only.
