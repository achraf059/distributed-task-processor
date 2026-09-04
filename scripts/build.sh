#!/usr/bin/env bash
# Builds the runnable jars (skipping tests; use ./mvnw clean verify for the full suite).
set -euo pipefail
cd "$(dirname "$0")/.."
./mvnw -q -B package -DskipTests
echo "Built:"
ls coordinator/target/dtp-coordinator-*.jar worker/target/dtp-worker-*.jar client/target/dtp-client-*.jar
