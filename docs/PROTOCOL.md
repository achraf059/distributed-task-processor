# Wire Protocol

Both coordinator ports speak the same framing; they differ only in which
messages are legal.

## Framing

```
[4-byte big-endian payload length][UTF-8 JSON payload]
```

- Maximum frame: **1 MiB**. A declared length ≤ 0 or above the maximum is a
  protocol violation.
- TCP delivers a byte stream, not messages: the reader loops
  (`DataInputStream.readFully`) until a full frame is assembled, so partial
  reads and multiple frames per `read()` are handled correctly.
- Clean EOF **between** frames = peer closed normally. EOF **inside** a frame
  = broken connection.

## Messages

Every payload is a JSON object with a `type` discriminator:

```json
{"type":"HEARTBEAT","workerId":"worker-1"}
```

Unknown `type` values, non-JSON payloads, or wrong field types raise a
protocol error that closes *that connection only*.

### Worker port (default 7070)

| Type | Direction | Fields | Notes |
|---|---|---|---|
| `WORKER_REGISTER` | W→C | `workerId`, `capacity` | must be the first message; capacity 1–1024 |
| `WORKER_REGISTERED` | C→W | `workerId` | acknowledgement |
| `HEARTBEAT` | W→C | `workerId` | every 2 s by default |
| `TASK_ASSIGN` | C→W | `jobId`, `attemptId`, `attemptNumber`, `taskType`, `payload` | `attemptId` is the lease |
| `TASK_CANCEL` | C→W | `jobId`, `attemptId` | cancel that specific attempt; ignored unless both ids match the running one |
| `TASK_RESULT` | W→C | `workerId`, `jobId`, `attemptId`, `success`, `result`, `error` | must echo the lease |
| `ERROR` | C→W | `message` | e.g. registering wrong; connection then closes |

There is no explicit `TASK_ACCEPTED`: assignment rides a healthy TCP
connection, the coordinator tracks capacity itself, and a send failure or
connection loss invalidates the attempt anyway (see DESIGN_DECISIONS).

`TASK_CANCEL` is sent when an attempt's lease is revoked — either its execution
deadline expired or a client cancelled the job. It is best-effort and advisory:
the coordinator has already revoked the lease, so whether or not the worker
acts on it, any result the attempt later produces is stale-rejected. The worker
cancels only if **both** `jobId` and `attemptId` match its currently executing
assignment, so a cancel for a superseded attempt can never interrupt a newer one.

### Client port (default 7071)

Strict request/response: one reply frame per request frame, many requests per
connection.

| Request | Reply | Notes |
|---|---|---|
| `SUBMIT_JOB` (`taskType`, `payload`, `maxAttempts`) | `JOB_SUBMITTED` (`jobId`) | `maxAttempts ≤ 0` → server default; payload validated before the job exists |
| `GET_JOB_STATUS` (`jobId`) | `JOB_STATUS` (job snapshot) | unknown id → `ERROR` |
| `LIST_JOBS` | `JOB_LIST` (snapshots, newest first) | |
| `LIST_WORKERS` | `WORKER_LIST` (live workers) | |
| `CANCEL_JOB` (`jobId`) | `JOB_CANCEL` (`cancelled`, job snapshot) | `cancelled=true` if this call moved it to CANCELLED; `false` if already terminal (snapshot shows real state); unknown id → `ERROR` |
| any invalid | `ERROR` (`message`) | malformed frames get a best-effort `ERROR`, then close |

A job snapshot contains: `jobId`, `taskType`, `state` (one of QUEUED, RUNNING,
RETRY_WAIT, COMPLETED, FAILED, CANCELLED), `attempts`, `maxAttempts`,
`workerId` (only while RUNNING), `result`, `error` (last attempt's error, kept
for history even after later success), `createdAtMillis`, `updatedAtMillis`.

## Task payloads

| Task | Payload | Bounds |
|---|---|---|
| `SLEEP` | `{"durationMillis": n}` | 0 – 600 000 |
| `WORD_COUNT` | `{"text": "..."}` | ≤ 200 000 chars |
| `SHA256` | `{"text": "..."}` | ≤ 200 000 chars |
| `PRIME_COUNT` | `{"limit": n}` | 2 – 50 000 000 |
| `FAIL` | `{"failUntilAttempt": n}` | 0 – 1000; fails while attempt < n |

Bounds exist so no job can monopolize a worker; there is deliberately no task
that executes arbitrary commands or touches the filesystem.

## Error handling and versioning

- A misbehaving peer costs only its own connection; the acceptor and all
  other sessions keep running (covered by `MalformedProtocolIT`).
- Unknown *fields* in known messages are ignored (forward compatibility);
  unknown *types* are rejected. There is no version field yet — both sides
  are built from the same tree. Adding `"v":1` to the envelope is the
  documented upgrade path if the protocol ever needs to evolve independently.
- Transport is plaintext TCP; TLS and worker authentication are explicitly
  out of scope for this project and would be required for untrusted networks.
