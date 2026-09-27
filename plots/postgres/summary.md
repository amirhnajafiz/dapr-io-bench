# postgres: direct vs Dapr

Latency in ms, throughput in successful ops/s, errors in % of operations. The ratio is Dapr/direct for latency and errors and direct/Dapr for throughput, so above 1.0 always means Dapr is worse.

## read vs RPS (payload_bytes=256, concurrency=8)

| RPS | p50 lat direct | p50 lat dapr | ratio | p99 lat direct | p99 lat dapr | ratio | thr direct | thr dapr | ratio | err direct | err dapr | ratio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 50 | 0.35 | 0.97 | 2.76x | 0.76 | 1.94 | 2.55x | 25 | 25 | 1.00x | 0 | 0 | - |
| 200 | 0.29 | 0.76 | 2.63x | 0.62 | 1.48 | 2.40x | 100 | 100 | 1.00x | 0 | 0 | - |
| 1k | 0.2 | 0.44 | 2.17x | 0.52 | 1.07 | 2.07x | 500 | 500 | 1.00x | 0 | 0 | - |
| 5k | 0.13 | 0.16 | 1.22x | 0.43 | 0.54 | 1.26x | 2,500 | 2,500 | 1.00x | 0 | 0 | - |
| 20k | 0.056 | 0.23 | 4.15x | 0.16 | 1.19 | 7.32x | 10,000 | 10,000 | 1.00x | 0 | 0.001 | - |
| 50k | 0.084 | 0.25 | 3.02x | 0.17 | 1.37 | 7.92x | 18,937 | 10,191 | 1.86x | 0 | 0 | - |
| 100k | 0.083 | 0.25 | 3.05x | 0.17 | 1.41 | 8.24x | 19,255 | 10,177 | 1.89x | 0 | 0 | - |
| closed loop | 0.081 | 0.25 | 3.08x | 0.17 | 1.38 | 8.08x | 19,460 | 10,280 | 1.89x | 0 | 0.00065 | - |

## write vs RPS (payload_bytes=256, concurrency=8)

| RPS | p50 lat direct | p50 lat dapr | ratio | p99 lat direct | p99 lat dapr | ratio | thr direct | thr dapr | ratio | err direct | err dapr | ratio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 50 | 1.18 | 1.92 | 1.63x | 2.23 | 2.92 | 1.31x | 25 | 25 | 1.00x | 0 | 0.13 | - |
| 200 | 1.02 | 1.61 | 1.58x | 1.83 | 2.64 | 1.45x | 100 | 100 | 1.00x | 0 | 0 | - |
| 1k | 0.78 | 0.86 | 1.10x | 1.83 | 1.96 | 1.07x | 500 | 500 | 1.00x | 0 | 0 | - |
| 5k | 0.56 | 0.27 | 0.48x | 1.05 | 0.95 | 0.91x | 2,500 | 2,500 | 1.00x | 0 | 0.0013 | - |
| 20k | 0.21 | 0.44 | 2.11x | 0.44 | 1.57 | 3.57x | 10,000 | 10,000 | 1.00x | 0 | 0.00067 | - |
| 50k | 0.32 | 0.47 | 1.47x | 0.49 | 1.77 | 3.61x | 18,937 | 10,190 | 1.86x | 0 | 0 | - |
| 100k | 0.32 | 0.47 | 1.49x | 0.48 | 1.76 | 3.65x | 19,255 | 10,177 | 1.89x | 0 | 0 | - |
| closed loop | 0.31 | 0.47 | 1.49x | 0.48 | 1.76 | 3.65x | 19,460 | 10,280 | 1.89x | 0 | 0.00065 | - |

## read vs payload bytes (rate=200, concurrency=8)

| payload bytes | p50 lat direct | p50 lat dapr | ratio | p99 lat direct | p99 lat dapr | ratio | thr direct | thr dapr | ratio | err direct | err dapr | ratio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 0.29 | 0.76 | 2.63x | 0.62 | 1.48 | 2.40x | 100 | 100 | 1.00x | 0 | 0 | - |
| 1,024 | 0.29 | 0.79 | 2.70x | 0.85 | 1.86 | 2.19x | 100 | 100 | 1.00x | 0 | 0 | - |
| 4,096 | 0.27 | 0.76 | 2.81x | 0.53 | 1.72 | 3.27x | 100 | 100 | 1.00x | 0 | 0 | - |
| 16,384 | 0.31 | 1.02 | 3.30x | 0.72 | 2.27 | 3.17x | 100 | 100 | 1.00x | 0 | 0 | - |

## write vs payload bytes (rate=200, concurrency=8)

| payload bytes | p50 lat direct | p50 lat dapr | ratio | p99 lat direct | p99 lat dapr | ratio | thr direct | thr dapr | ratio | err direct | err dapr | ratio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 1.02 | 1.61 | 1.58x | 1.83 | 2.64 | 1.45x | 100 | 100 | 1.00x | 0 | 0 | - |
| 1,024 | 1.03 | 1.7 | 1.66x | 2.25 | 3.1 | 1.37x | 100 | 100 | 1.00x | 0 | 0.033 | - |
| 4,096 | 0.99 | 1.72 | 1.75x | 1.64 | 2.61 | 1.60x | 100 | 100 | 1.00x | 0 | 0 | - |
| 16,384 | 1.23 | 1.77 | 1.45x | 2.3 | 4.47 | 1.95x | 100 | 100 | 1.00x | 0 | 0.033 | - |

## read vs concurrent users (rate=200, payload_bytes=256)

| concurrent users | p50 lat direct | p50 lat dapr | ratio | p99 lat direct | p99 lat dapr | ratio | thr direct | thr dapr | ratio | err direct | err dapr | ratio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.3 | 0.75 | 2.52x | 0.6 | 1.62 | 2.70x | 100 | 100 | 1.00x | 0 | 0 | - |
| 8 | 0.29 | 0.76 | 2.63x | 0.62 | 1.48 | 2.40x | 100 | 100 | 1.00x | 0 | 0 | - |
| 32 | 0.29 | 0.73 | 2.52x | 0.72 | 1.56 | 2.15x | 100 | 100 | 1.00x | 0 | 0 | - |
| 128 | 0.29 | 0.77 | 2.65x | 0.61 | 1.64 | 2.68x | 100 | 100 | 1.00x | 0 | 0 | - |

## write vs concurrent users (rate=200, payload_bytes=256)

| concurrent users | p50 lat direct | p50 lat dapr | ratio | p99 lat direct | p99 lat dapr | ratio | thr direct | thr dapr | ratio | err direct | err dapr | ratio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1.08 | 1.72 | 1.60x | 1.92 | 2.89 | 1.50x | 100 | 100 | 1.00x | 0 | 0.033 | - |
| 8 | 1.02 | 1.61 | 1.58x | 1.83 | 2.64 | 1.45x | 100 | 100 | 1.00x | 0 | 0 | - |
| 32 | 1.08 | 1.74 | 1.61x | 1.7 | 2.8 | 1.64x | 100 | 100 | 1.00x | 0 | 0 | - |
| 128 | 1.08 | 1.76 | 1.63x | 1.69 | 2.75 | 1.63x | 100 | 100 | 1.00x | 0 | 0 | - |
