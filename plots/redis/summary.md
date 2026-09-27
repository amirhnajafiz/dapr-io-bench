# redis: direct vs Dapr

Latency in ms, throughput in successful ops/s, errors in % of operations. The ratio is Dapr/direct for latency and errors and direct/Dapr for throughput, so above 1.0 always means Dapr is worse.

## read vs RPS (payload_bytes=256, concurrency=8)

| RPS | p50 lat direct | p50 lat dapr | ratio | p99 lat direct | p99 lat dapr | ratio | thr direct | thr dapr | ratio | err direct | err dapr | ratio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 50 | 0.22 | 0.75 | 3.33x | 1.16 | 1.88 | 1.63x | 25 | 25 | 1.00x | 0 | 0 | - |
| 200 | 0.2 | 0.69 | 3.47x | 1.07 | 1.69 | 1.58x | 100 | 100 | 1.00x | 0 | 0 | - |
| 1k | 0.17 | 0.5 | 2.84x | 0.94 | 1.56 | 1.66x | 500 | 500 | 1.00x | 0 | 0 | - |
| 5k | 0.14 | 0.22 | 1.58x | 0.69 | 0.76 | 1.10x | 2,500 | 2,500 | 1.00x | 0 | 0.0013 | - |
| 20k | 0.073 | 0.21 | 2.87x | 0.25 | 1.14 | 4.63x | 10,000 | 10,000 | 1.00x | 0 | 0.00033 | - |
| 50k | 0.065 | 0.29 | 4.40x | 0.18 | 1.44 | 8.08x | 25,000 | 13,001 | 1.92x | 0 | 0.047 | - |
| 100k | 0.085 | 0.29 | 3.37x | 0.22 | 1.42 | 6.49x | 44,134 | 12,989 | 3.40x | 0 | 0.039 | - |
| closed loop | 0.085 | 0.29 | 3.37x | 0.22 | 1.48 | 6.62x | 43,892 | 12,884 | 3.41x | 0 | 0.00026 | - |

## write vs RPS (payload_bytes=256, concurrency=8)

| RPS | p50 lat direct | p50 lat dapr | ratio | p99 lat direct | p99 lat dapr | ratio | thr direct | thr dapr | ratio | err direct | err dapr | ratio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 50 | 0.23 | 0.76 | 3.33x | 1.06 | 1.79 | 1.69x | 25 | 25 | 1.00x | 0 | 0 | - |
| 200 | 0.21 | 0.68 | 3.30x | 1.07 | 1.69 | 1.58x | 100 | 100 | 1.00x | 0 | 0.033 | - |
| 1k | 0.18 | 0.5 | 2.79x | 0.95 | 1.54 | 1.63x | 500 | 500 | 1.00x | 0 | 0.0067 | - |
| 5k | 0.14 | 0.22 | 1.58x | 0.7 | 0.77 | 1.10x | 2,500 | 2,500 | 1.00x | 0 | 0 | - |
| 20k | 0.074 | 0.21 | 2.87x | 0.25 | 1.14 | 4.63x | 10,000 | 10,000 | 1.00x | 0 | 0.00033 | - |
| 50k | 0.065 | 0.28 | 4.36x | 0.18 | 1.42 | 8.00x | 25,000 | 13,001 | 1.92x | 0 | 0.046 | - |
| 100k | 0.085 | 0.29 | 3.37x | 0.22 | 1.42 | 6.49x | 44,134 | 12,989 | 3.40x | 0 | 0.039 | - |
| closed loop | 0.085 | 0.28 | 3.33x | 0.22 | 1.5 | 6.69x | 43,892 | 12,884 | 3.41x | 0 | 0 | - |

## read vs payload bytes (rate=200, concurrency=8)

| payload bytes | p50 lat direct | p50 lat dapr | ratio | p99 lat direct | p99 lat dapr | ratio | thr direct | thr dapr | ratio | err direct | err dapr | ratio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 0.2 | 0.69 | 3.47x | 1.07 | 1.69 | 1.58x | 100 | 100 | 1.00x | 0 | 0 | - |
| 1,024 | 0.21 | 0.7 | 3.40x | 1.04 | 1.67 | 1.61x | 100 | 100 | 1.00x | 0 | 0 | - |
| 4,096 | 0.21 | 0.78 | 3.68x | 1.18 | 1.9 | 1.61x | 100 | 100 | 1.00x | 0 | 0 | - |
| 16,384 | 0.23 | 1.02 | 4.40x | 1.06 | 2.12 | 2.01x | 100 | 100 | 1.00x | 0 | 0 | - |

## write vs payload bytes (rate=200, concurrency=8)

| payload bytes | p50 lat direct | p50 lat dapr | ratio | p99 lat direct | p99 lat dapr | ratio | thr direct | thr dapr | ratio | err direct | err dapr | ratio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 0.21 | 0.68 | 3.30x | 1.07 | 1.69 | 1.58x | 100 | 100 | 1.00x | 0 | 0.033 | - |
| 1,024 | 0.21 | 0.69 | 3.33x | 1 | 1.69 | 1.69x | 100 | 100 | 1.00x | 0 | 0 | - |
| 4,096 | 0.22 | 0.74 | 3.37x | 1.06 | 1.72 | 1.63x | 100 | 100 | 1.00x | 0 | 0.033 | - |
| 16,384 | 0.23 | 0.87 | 3.79x | 1.05 | 2.1 | 2.01x | 100 | 100 | 1.00x | 0 | 0.033 | - |

## read vs concurrent users (rate=200, payload_bytes=256)

| concurrent users | p50 lat direct | p50 lat dapr | ratio | p99 lat direct | p99 lat dapr | ratio | thr direct | thr dapr | ratio | err direct | err dapr | ratio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.19 | 0.66 | 3.43x | 1 | 1.57 | 1.58x | 100 | 100 | 1.00x | 0 | 0 | - |
| 8 | 0.2 | 0.69 | 3.47x | 1.07 | 1.69 | 1.58x | 100 | 100 | 1.00x | 0 | 0 | - |
| 32 | 0.2 | 0.67 | 3.37x | 1.01 | 1.67 | 1.66x | 100 | 100 | 1.00x | 0 | 0 | - |
| 128 | 0.2 | 0.67 | 3.40x | 1.04 | 1.61 | 1.55x | 100 | 100 | 1.00x | 0 | 0 | - |

## write vs concurrent users (rate=200, payload_bytes=256)

| concurrent users | p50 lat direct | p50 lat dapr | ratio | p99 lat direct | p99 lat dapr | ratio | thr direct | thr dapr | ratio | err direct | err dapr | ratio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.2 | 0.66 | 3.27x | 1.03 | 1.61 | 1.56x | 100 | 100 | 1.00x | 0 | 0.033 | - |
| 8 | 0.21 | 0.68 | 3.30x | 1.07 | 1.69 | 1.58x | 100 | 100 | 1.00x | 0 | 0.033 | - |
| 32 | 0.2 | 0.66 | 3.33x | 1.03 | 1.61 | 1.56x | 100 | 100 | 1.00x | 0 | 0 | - |
| 128 | 0.2 | 0.67 | 3.37x | 1.04 | 1.61 | 1.55x | 100 | 100 | 1.00x | 0 | 0 | - |
