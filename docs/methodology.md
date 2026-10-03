# Methodology

How a run is shaped, what each number means, and what keeps the comparison fair.

## One pair, one connector at a time

A run compares one backend's direct and Dapr connectors and never more than two connectors
(`CONNECTORS`, enforced by the app). Within a run the two are measured **one at a time**: the
backend and the sidecar only ever serve the connector being measured, and a cooldown separates
steps so one step's tail never bleeds into the next. Only that backend's containers are started
and only its Dapr component is loaded. The only difference within a pair is the sidecar hop.

## Operations: heavy and light

Every connector exposes heavy operations that carry the payload (`write` and `read`, or
`publish`) and one light operation, `stat`, the lightest call its API offers that touches a key
without moving data. The direct side uses the native metadata call: Redis `EXISTS`, Postgres
`SELECT 1 ... WHERE key`, filesystem `stat`. The Dapr state and binding APIs have no exists
call, so the Dapr side fetches a one-byte marker record that the seed step writes next to every
key; NATS has no per-key metadata on either side, so both publish one byte. The two classes
separate per-call overhead, which dominates `stat`, from the cost of moving bytes, which
dominates the heavy ops as payloads grow.

## A step: tasks, agents, payload

A step is a fixed amount of work: `TASKS` operations, shared by `AGENTS` concurrent callers,
each moving `PAYLOAD_BYTES`. Task *k* runs operation *k mod ops* (`write`, `read`, `stat`,
`write`, ...), so every op gets an equal share and each is timed on its own. Every agent is
closed loop: it takes the next task the moment its previous call returns. Nothing paces the
load; the step measures how fast the path can serve that many concurrent callers, and the
**makespan**, first call issued to last call returned, is how long that group of agents waits
for all its work to be done.

Per op the step reports completed operations, errors (timeouts counted separately, with the
first error message), throughput as successful operations per second over the makespan, and
latency percentiles. Each op has a deadline (`OP_TIMEOUT`); a call that hits it is an error
whose latency is the deadline. The whole step has a deadline too (`STEP_TIMEOUT`); a step cut
short by it is recorded with what it completed and `truncated: true`.

## The sweep

Load has two dimensions, each driven under its own profile rather than every combination:

| Dimension       | Default levels                                               | Driven as                               |
| --------------- | ------------------------------------------------------------ | --------------------------------------- |
| `AGENTS`        | `1, 8, 32, 128, 512, 2048, 8192, 32768, 131072, 1000000`     | baseline payload (256 B)                |
| `PAYLOAD_BYTES` | `256, 1024, 4096, 16384, 1 MiB, 16 MiB, 128 MiB, 1 GiB`      | `PAYLOAD_SWEEP_AGENTS` agents (1)       |

Large payloads are moved by a single agent and many agents move a small payload. A gigabyte
value under a thousand agents, or a million agents each holding a megabyte, would only measure
the machine running out of memory; the profiles find where storage falls behind on each axis
without those combinations. `SWEEP_MODE=grid` measures every combination of the two lists
instead, for when that is wanted. A load that belongs to both curves (one agent at 256 B) is
measured once and tagged with both dimensions.

Two quantities shrink as payloads grow so a step stays bounded:

- **Tasks.** `TASKS` operations at small payloads, but never more than `MAX_TASK_BYTES` worth
  of payload per step (8 GiB by default: 100,000 tasks up to 64 KiB, 8192 at 1 MiB, 8 at
  1 GiB), and never fewer tasks than agents, so every agent gets at least one.
- **Keyspace.** `KEYSPACE` distinct keys at small payloads, but never more than
  `MAX_DATASET_BYTES` of seeded data (256 MiB by default: 1000 keys up to 16 KiB, one key at
  1 GiB).

Each trace record carries the tasks and keyspace it was measured with.

For every point the sweep sets the payload size and keyspace, then for each connector of the
pair:

1. **seeds** every key (payload) and every marker (one byte) through the connector, so a
   `read` or `stat` never misses;
2. runs **warmup tasks** (`WARMUP_TASKS`, capped at a tenth of the step, discarded);
3. **runs the tasks** and records the step;
4. rests for `STEP_COOLDOWN` before the next connector starts.

## Percentiles

Every operation of a step is observed into a log-linear histogram with 1% resolution
(`internal/stats`). Agents share up to 64 histogram shards, each under its own lock, so a
million agents cost a million goroutines but not a million histograms; the shards are merged
when the step ends. The reported p50, p95 and p99 are therefore computed from every call of the
step in the app itself, not sampled from a metrics scrape.

## Fairness notes

Benchmarks like this are easy to rig by accident. Each of these is enforced by the code:

- **One connector at a time**, with a cooldown between them.
- **Identical work.** Both connectors of a pair get the same tasks, agents, payload and
  keyspace at every point, in the same key order.
- **The keyspace is seeded before every step**, through the connector under test, so reads
  never miss and the first calls are not a cold-start artefact; warmup tasks are discarded.
- **NATS publishes wait for the JetStream ack on both sides.** Dapr's component calls the
  synchronous `jsc.Publish`, and so does the direct connector. Comparing against a
  fire-and-forget core NATS publish would measure buffering, not overhead.
- **Postgres statement shapes match**: upsert on write, point select on read. Dapr uses its
  own table, so neither path pollutes the other.
- **The filesystem pair writes to the same volume.** The sidecar's localstorage binding and
  the direct connector use sibling directories on one named volume, so both hit the same disk.
- **Sidecar tracing is disabled**; it logs `TraceIDRatioBased{0}` at startup. A trace exporter
  would add latency to the thing being measured.
- **App and sidecar share a network namespace**, exactly like a Kubernetes pod, so the gRPC
  hop is loopback rather than a Docker bridge.
- **Message size limits are raised on both sides** of the Dapr path (the Go SDK's gRPC client
  and the sidecar), so a large payload fails only where the backend itself refuses it, and
  that refusal is recorded as an error with its message.
- **The payload is not valid base64.** Dapr's localstorage binding decodes any data that
  happens to decode as base64, which would silently shrink an all-letters payload by a
  quarter; the payload includes characters outside the base64 alphabet so it is stored as is.

## What the sidecar hop does *not* explain

Dapr's components do not store data the way a direct client would, and none of it is
switchable. It is a real cost of adopting Dapr, so it stays in the measurement, but it means
the overhead is not purely "one gRPC hop":

|              | Direct                  | Through Dapr                                                                                         |
| ------------ | ----------------------- | ---------------------------------------------------------------------------------------------------- |
| **Redis**    | `SET` of a plain string | Lua `EVAL` → `HSET` of `{data, version}`, key prefixed `bench\|\|`                                   |
| **Postgres** | `bytea`                 | base64 inside `jsonb` (~1.4× the bytes) plus `isbinary` / `insertdate` / `updatedate` / `expiredate` |
| **NATS**     | raw payload             | CloudEvent envelope plus `Nats-MsgId` dedup                                                          |
| **Filesystem** | `write(2)` of the bytes | the same, after a base64 probe of the data                                                        |

So Redis in particular is Dapr doing strictly *more work*, a scripted read-modify-write of a
hash, than the plain `SET` it is compared against.

Two client-side limits also shape the agents sweep: the direct Redis and Postgres clients hold
32 connections, so thousands of agents queue on the pool; and plain file calls on the direct
filesystem path cannot be cancelled, so they ignore `OP_TIMEOUT` and report long latencies
where the other paths report timeouts.

**Not measured at all:** resilience, retries, mTLS, observability, portability. The sidecar hop
buys those things; this project only prices it.
