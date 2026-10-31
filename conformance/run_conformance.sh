#!/usr/bin/env bash
# run_conformance.sh -- run the ParticleDB SDK conformance suite against THIS
# driver (this repository), the reference implementation.
#
#   [PDB_BIN=/path/to/particledb] ./run_conformance.sh [--mode default|strict] [--results FILE]
#
# One command, self-contained: cases/ and lib/ are this bundle's copies of the
# shared conformance cases and runner library (identical in every SDK repository).
# It starts a fresh server from PDB_BIN (default: $PARTICLEDB_BIN, else
# `particledb` on PATH) (unless PDB_CONF_PORT names a running one,
# in which case PDB_CONF_CONTROL must be set too), builds the driver from
# source, compiles the adapter against it, runs every case for the mode, and
# validates the results file (every case reported; every skip justified).
# Exit 0 only if no case failed and the results file is valid.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE="$(cd "$HERE/.." && pwd)"
MODE=default
RESULTS=""
while [ $# -gt 0 ]; do
  case "$1" in
    --mode) MODE=$2; shift 2 ;;
    --results) RESULTS=$2; shift 2 ;;
    -h|--help) sed -n '2,14p' "$0"; exit 0 ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done

WORK=$(mktemp -d "${TMPDIR:-/tmp}/pdb-conf-jdbc.XXXXXX")
[ -n "$RESULTS" ] || RESULTS="$WORK/results.json"
STARTED=0
cleanup() {
  if [ $STARTED -eq 1 ]; then "$HERE/lib/server.sh" stop "$WORK/server" || true; fi
}
trap cleanup EXIT INT TERM

if [ -z "${PDB_CONF_PORT:-}" ]; then
  PDB_BIN=${PDB_BIN:-${PARTICLEDB_BIN:-$(command -v particledb || true)}}
  : "${PDB_BIN:?set PDB_BIN to the particledb server binary (or PDB_CONF_PORT + PDB_CONF_CONTROL for a running one)}"
  "$HERE/lib/server.sh" start "$WORK/server" "$PDB_BIN" "$MODE" || exit 1
  STARTED=1
  eval "$("$HERE/lib/server.sh" env "$WORK/server")"
  printf '#!/bin/sh\nexec "%s" "$1" "%s"\n' "$HERE/lib/server.sh" "$WORK/server" > "$WORK/control"
  chmod +x "$WORK/control"
  export PDB_CONF_CONTROL="$WORK/control"
  echo "server: $PDB_BIN on native port $PDB_CONF_PORT (default port $PDB_CONF_DEFAULT_PORT), mode $MODE, log $WORK/server/server.log"
fi
: "${PDB_CONF_CONTROL:?PDB_CONF_CONTROL must be set when PDB_CONF_PORT names a running server}"
export PDB_CONF_MODE=$MODE PDB_CONF_CASES_DIR="$HERE/cases" PDB_CONF_RESULTS="$RESULTS" PDB_CONF_IDS="$WORK/ids"

python3 "$HERE/lib/conformance.py" ids "$HERE/cases" "$MODE" > "$WORK/ids" || exit 2

# Build the driver under test from source and resolve its runtime classpath.
( cd "$MODULE" && mvn -q -B ${PDB_CONF_MVN_OFFLINE:+-o} -DskipTests compile dependency:build-classpath \
    -Dmdep.outputFile="$WORK/cp.txt" -Dmdep.includeScope=runtime -Dmdep.excludeGroupIds=org.apache.arrow ) || { echo "driver build failed" >&2; exit 2; }
CP="$MODULE/target/classes:$(cat "$WORK/cp.txt")"
mkdir -p "$WORK/adapter"
javac -d "$WORK/adapter" -cp "$CP" $(find "$HERE/src" -name '*.java') || { echo "adapter compile failed" >&2; exit 2; }

# SQL-STREAM-001 asserts memory-bounded streaming: the whole adapter runs in a
# 256 MiB heap. It runs as a consumer would: no JVM module flags, and without
# Apache Arrow Java (an optional dependency, excluded from the classpath above).
java -Xmx256m \
  -cp "$WORK/adapter:$CP" org.particledb.conformance.JdbcAdapter
rc=$?
python3 "$HERE/lib/conformance.py" validate "$HERE/cases" "$MODE" "$RESULTS" || rc=1
python3 "$HERE/lib/conformance.py" table "$HERE/cases" "$RESULTS" > /dev/null || true
echo "results: $RESULTS"
exit $rc
