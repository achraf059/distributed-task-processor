#!/usr/bin/env bash
# End-to-end failure-recovery demonstration against the real system:
#
#   1. start the coordinator and three workers
#   2. submit a 20-second SLEEP job
#   3. kill -9 the worker the job was assigned to
#   4. watch the coordinator detect the failure and reassign the job
#   5. print the final state (COMPLETED, attempt 2, on a different worker)
#
# Nothing is faked: the script only drives the CLI and inspects its output.
# Logs are written to ./demo-logs/.
set -euo pipefail
cd "$(dirname "$0")/.."

WORKER_PORT=7070
CLIENT_PORT=7071
LOG_DIR=demo-logs
DB=$LOG_DIR/demo-coordinator.db
CLIENT=(java -jar client/target/dtp-client-*.jar)
PIDS=()

cleanup() {
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

[ -f coordinator/target/dtp-coordinator-1.0.0-SNAPSHOT.jar ] || ./scripts/build.sh
rm -rf "$LOG_DIR" && mkdir -p "$LOG_DIR"

echo "==> Starting coordinator (worker port $WORKER_PORT, client port $CLIENT_PORT)"
java -jar coordinator/target/dtp-coordinator-*.jar \
  --worker-port $WORKER_PORT --client-port $CLIENT_PORT --database "$DB" \
  > "$LOG_DIR/coordinator.log" 2>&1 &
PIDS+=($!)
sleep 2

# Plain variables keep this compatible with the bash 3.2 that ships on macOS.
for i in 1 2 3; do
  echo "==> Starting worker-$i"
  java -jar worker/target/dtp-worker-*.jar \
    --coordinator-port $WORKER_PORT --id "worker-$i" \
    > "$LOG_DIR/worker-$i.log" 2>&1 &
  eval "WORKER_PID_$i=$!"
  PIDS+=($!)
  disown $! 2>/dev/null || true   # keep bash from reporting the later kill -9
done
sleep 2

echo "==> Registered workers:"
"${CLIENT[@]}" workers --port $CLIENT_PORT

echo "==> Submitting a 20-second SLEEP job"
JOB_ID=$("${CLIENT[@]}" submit sleep --milliseconds 20000 --port $CLIENT_PORT | awk '/ID:/{print $2}')
echo "    job id: $JOB_ID"
sleep 2

ASSIGNED=$("${CLIENT[@]}" status "$JOB_ID" --port $CLIENT_PORT | awk '/Worker:/{print $2}')
echo "==> Job is RUNNING on: $ASSIGNED"
"${CLIENT[@]}" status "$JOB_ID" --port $CLIENT_PORT

ASSIGNED_INDEX=${ASSIGNED#worker-}
ASSIGNED_PID=$(eval "echo \$WORKER_PID_$ASSIGNED_INDEX")

echo
echo "==> Killing $ASSIGNED with SIGKILL (pid $ASSIGNED_PID)"
kill -9 "$ASSIGNED_PID"

echo "==> Waiting for the coordinator to detect the failure and reassign..."
sleep 4
"${CLIENT[@]}" status "$JOB_ID" --port $CLIENT_PORT

echo
echo "==> Waiting for the job to complete on the replacement worker..."
"${CLIENT[@]}" wait "$JOB_ID" --port $CLIENT_PORT

echo
echo "==> Coordinator log lines telling the story:"
grep -E "assigned|suspected dead|lost|Retry|requeued|completed" "$LOG_DIR/coordinator.log" \
  | grep -i "$JOB_ID\|worker" | tail -12

echo
echo "Demo complete. Full logs are in $LOG_DIR/."
