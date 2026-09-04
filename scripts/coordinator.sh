#!/usr/bin/env bash
# Starts the coordinator. All arguments are passed through, e.g.:
#   ./scripts/coordinator.sh --heartbeat-timeout-millis 6000
set -euo pipefail
cd "$(dirname "$0")/.."
JAR=$(ls coordinator/target/dtp-coordinator-*.jar 2>/dev/null | head -1) \
  || { echo "Jar not found. Run ./scripts/build.sh first." >&2; exit 1; }
exec java -jar "$JAR" "$@"
