#!/usr/bin/env bash
# Starts a worker, e.g.: ./scripts/worker.sh --id worker-1
set -euo pipefail
cd "$(dirname "$0")/.."
JAR=$(ls worker/target/dtp-worker-*.jar 2>/dev/null | head -1) \
  || { echo "Jar not found. Run ./scripts/build.sh first." >&2; exit 1; }
exec java -jar "$JAR" "$@"
