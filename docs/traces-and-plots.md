# Traces and plots

## Trace format

Every step is one JSON line in `traces/<timestamp>-<backend>.jsonl`, after a header line
describing the plan. A record carries the point, the connector, and every measurement:

```json
{"type":"step","sweep":"20261003T173442Z-redis","captured_at":"2026-10-03T17:36:02Z",
 "dimension":"agents","agents":512,"payload_bytes":256,"tasks":100000,"keyspace":1000,
 "connector":"redis-dapr","backend":"redis","mode":"dapr","op":"write",
 "elapsed_seconds":31.4,"completed":100000,"truncated":false,
 "ops":33334,"errors":12,"timeouts":12,"first_error":"error saving state: rpc error: code = DeadlineExceeded desc = context deadline exceeded",
 "throughput":1061.2,
 "latency":{"p50":0.0049,"p95":0.0081,"p99":0.0102,"mean":0.0051,"max":5.0012}}
```

| Field                        | Meaning                                                                                        |
| ---------------------------- | ---------------------------------------------------------------------------------------------- |
| `dimension`                  | which curve the point belongs to: `agents`, `payload_bytes`, `baseline`, or both comma-joined |
| `agents`, `payload_bytes`    | the point                                                                                      |
| `tasks`                      | operations in the step across all ops; shrinks with large payloads                             |
| `keyspace`                   | distinct keys at this point; shrinks with large payloads to bound the dataset                  |
| `elapsed_seconds`            | makespan of the whole step: first call issued to last call returned                            |
| `completed`, `truncated`     | tasks finished across all ops, and whether `STEP_TIMEOUT` cut the step short                   |
| `ops`, `errors`, `timeouts`  | this op's operations, of which failed, of which hit `OP_TIMEOUT`                               |
| `first_error`                | present when `errors` is non-zero: the message of the first failure                            |
| `throughput`                 | this op's successful operations per second over the makespan                                   |
| `latency`                    | seconds; p50/p95/p99/mean/max from a 1%-resolution histogram of every call of this op          |

## Plotting

```sh
make plots                              # every trace
make plots FILES="traces/2026*-redis.jsonl"
```

For each backend, operation and dimension this writes one SVG into `plots/<backend>/`
(`write_vs_agents.svg`, `read_vs_payload.svg`, `stat_vs_agents.svg`, ...) with four panels:
p50 latency, p99 latency, throughput, error rate. Direct is always a solid blue line, Dapr always
a dashed orange one, so the two stay tellable where they overlap. The x axis is logarithmic with
compact labels (`1e3`, `5e4`, `1.31e5`), and latency switches to seconds when a saturated path
pushes it past five digits of milliseconds. The header says what moves along x, what is held
fixed, and how many tasks each step ran.

Next to the charts, `plots/<backend>/summary.md` tabulates every point with the Dapr-vs-direct
ratio for p50, p99, throughput, error rate and makespan, and the same table is printed to stdout.
The p95 and mean are in the trace records for anyone who wants to chart them.

The plotter is **standard-library Python only**: no matplotlib, no virtualenv.

Runs are additive: the plotter merges every trace it is given, and where two traces measured
the same point the later one wins. A run that adds levels to one dimension, for example
`SWEEP_AGENTS=4096,16384 SWEEP_PAYLOAD_BYTES=`, extends earlier traces rather than replacing
them. Points of one dimension measured under different profiles (payloads at one agent in one
run and at eight in another) are charted separately, with the profile in the file name:
`write_vs_payload_at_agents8.svg`.

## Configuration

All knobs are environment variables on the `bench` service in
[docker-compose.yml](../deploy/docker-compose.yml); each can be set on the `make` line.

| Variable                 | Default                                                     | Meaning                                                                      |
| ------------------------ | ----------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `CONNECTORS`             | `redis-direct,redis-dapr`                                   | the pair to compare, at most two; `make sweep BACKEND=x` sets it and starts only that backend |
| `AGENTS`                 | `8`                                                         | baseline agents; the baseline point is `AGENTS` × `PAYLOAD_BYTES`            |
| `PAYLOAD_BYTES`          | `256`                                                       | baseline payload, which the agents sweep is driven at                        |
| `SWEEP_AGENTS`           | `1,8,32,128,512,2048,8192,32768,131072,1000000`             | agent levels; empty skips the dimension                                      |
| `SWEEP_PAYLOAD_BYTES`    | `256,1024,4096,16384,1048576,16777216,134217728,1073741824` | payload levels; empty skips the dimension                                    |
| `PAYLOAD_SWEEP_AGENTS`   | `1`                                                         | agents driving the payload sweep                                             |
| `SWEEP_MODE`             | `oat`                                                       | `oat` (one dimension at a time) or `grid` (every combination)                |
| `TASKS`                  | `100000`                                                    | operations per step at small payloads                                        |
| `MAX_TASK_BYTES`         | `8589934592`                                                | tasks shrink so a step never moves more payload than this (8 GiB)            |
| `WARMUP_TASKS`           | `1000`                                                      | tasks run first and discarded, capped at a tenth of the step                 |
| `KEYSPACE`               | `1000`                                                      | distinct keys at small payloads                                              |
| `MAX_DATASET_BYTES`      | `268435456`                                                 | the keyspace shrinks so seeding never writes more than this (256 MiB)        |
| `STEP_COOLDOWN`          | `5s`                                                        | idle gap before the next connector starts                                    |
| `OP_TIMEOUT`             | `5s`                                                        | per-operation deadline; a hit counts as an error and a timeout               |
| `STEP_TIMEOUT`           | `30m`                                                       | per-step deadline; a step that hits it is recorded as truncated              |
| `FS_DIR`                 | `/data/direct`                                              | where the direct filesystem connector writes, on the volume shared with the sidecar |

```sh
# a million tasks over 64 agents at 4 KiB, nothing else
TASKS=1000000 AGENTS=64 PAYLOAD_BYTES=4096 SWEEP_AGENTS= SWEEP_PAYLOAD_BYTES= make sweep BACKEND=redis
```

Step time is work divided by speed: 100,000 tasks take a few seconds on a fast path and a few
minutes on a slow one, and a step whose agents all time out costs at least `OP_TIMEOUT` per
task per agent. `STEP_TIMEOUT` bounds the worst case.

`make sweep-all` runs the four backends back to back; each run starts only its own backend
(compose profiles) and the sidecar loads only that backend's Dapr component
(`deploy/dapr/components/<backend>/`), so at any moment the machine hosts the pair, the
sidecar, Prometheus and one backend.

`make up` makes `traces/` world-writable, because the bench writes as uid 10001 into that bind
mount and on a Linux host the directory is owned by whoever cloned the repo.

## Sizing the machine

The top of each dimension is meant to break something, and what breaks first on a small
machine is memory rather than storage:

- **1 GiB payloads.** The app holds the value plus a gRPC marshalling copy; the sidecar holds
  its own copy plus whatever the component builds from it (base64 in JSON for Postgres, a Lua
  script argument for Redis, a CloudEvent for NATS). Budget 5 to 6 GB for the pair at that
  level. Each backend also has a hard ceiling that the trace records as an error: Redis values
  stop at 512 MB, Postgres `jsonb` near 255 MB (so Dapr's Postgres store fails before the
  direct `bytea` does), NATS at the 64 MB `max_payload` (roughly 48 MB through Dapr's
  CloudEvent base64), the filesystem has none, and gRPC caps everything through Dapr at 2 GiB.
- **1,000,000 agents.** A million goroutines need 2 to 4 GB of stacks, each with a timer for
  its per-operation deadline. The direct Redis and Postgres clients hold 32 connections, so a
  million agents queue on the pool and the queue shows up as latency and then as 5-second
  timeouts, which is the breakpoint being looked for. The container's open-file limit is
  raised to the kernel default of 1,048,576; a million agents on the filesystem pair sit right
  at it.

On an 8 GB Docker Desktop the defaults are too much for the top levels; trim with
`SWEEP_PAYLOAD_BYTES=...,134217728` and `SWEEP_AGENTS=...,131072`, or run on a machine with
16 GB or more. The sidecar restarts automatically if a step takes it down, so a later point
still has a sidecar to measure; the step that killed it is recorded with its errors.

## Live view

The app also exports Prometheus metrics for watching a step as it happens: bench metrics on
<http://localhost:9100/metrics>, the sidecar's own on <http://localhost:9090/metrics>, and
Prometheus on <http://localhost:9091>.

| Metric                      | Meaning                                               |
| --------------------------- | ----------------------------------------------------- |
| `bench_op_duration_seconds` | latency histogram, labelled `backend` / `mode` / `op` |
| `bench_ops_total`           | attempts, also labelled `status`                      |
| `bench_step_info`           | the point being measured right now, as labels         |
| `bench_connector_up`        | `1` per connector that connected                      |

[rules.yml](../deploy/prometheus/rules.yml) records `bench:latency_p50/p95/p99` and
`bench:throughput`. These are a window into the run, not its record; the trace file is the
record.
