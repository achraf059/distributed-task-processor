#!/usr/bin/env bash
# Runs the client CLI, e.g.: ./scripts/client.sh submit sleep --milliseconds 30000
set -euo pipefail
cd "$(dirname "$0")/.."
JAR=$(ls client/target/dtp-client-*.jar 2>/dev/null | head -1) \
  || { echo "Jar not found. Run ./scripts/build.sh first." >&2; exit 1; }
exec java -jar "$JAR" "$@"
