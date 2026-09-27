# nats: direct vs Dapr

Latency in ms, throughput in successful ops/s, errors in % of operations. The ratio is Dapr/direct for latency and errors and direct/Dapr for throughput, so above 1.0 always means Dapr is worse.

## publish vs RPS (payload_bytes=256, concurrency=8)

| RPS | p50 lat direct | p50 lat dapr | ratio | p99 lat direct | p99 lat dapr | ratio | thr direct | thr dapr | ratio | err direct | err dapr | ratio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 50 | 0.37 | 1.3 | 3.54x | 1.3 | 2.89 | 2.22x | 50 | 50 | 1.00x | 0 | 0 | - |
| 200 | 0.31 | 0.94 | 2.99x | 1.04 | 2.49 | 2.40x | 200 | 200 | 1.00x | 0 | 0.017 | - |
| 1k | 0.29 | 0.61 | 2.09x | 0.93 | 1.72 | 1.85x | 1,000 | 1,000 | 1.00x | 0 | 0.0067 | - |
| 5k | 0.22 | 0.19 | 0.88x | 0.58 | 0.88 | 1.53x | 5,000 | 5,000 | 1.00x | 0 | 0.0073 | - |
| 20k | 0.072 | 0.31 | 4.27x | 0.22 | 1.5 | 6.96x | 19,999 | 19,998 | 1.00x | 0 | 0 | - |
| 50k | 0.073 | 0.35 | 4.72x | 0.22 | 1.53 | 6.82x | 49,998 | 21,461 | 2.33x | 0 | 0 | - |
| 100k | 0.072 | 0.35 | 4.82x | 0.25 | 1.53 | 6.12x | 99,999 | 21,509 | 4.65x | 0 | 0 | - |
| closed loop | 0.049 | 0.35 | 7.10x | 0.24 | 1.51 | 6.18x | 120,142 | 21,460 | 5.60x | 0 | 0.018 | - |

## publish vs payload bytes (rate=200, concurrency=8)

| payload bytes | p50 lat direct | p50 lat dapr | ratio | p99 lat direct | p99 lat dapr | ratio | thr direct | thr dapr | ratio | err direct | err dapr | ratio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 0.31 | 0.94 | 2.99x | 1.04 | 2.49 | 2.40x | 200 | 200 | 1.00x | 0 | 0.017 | - |
| 1,024 | 0.3 | 0.91 | 3.05x | 0.63 | 1.74 | 2.76x | 200 | 200 | 1.00x | 0 | 0 | - |
| 4,096 | 0.32 | 0.99 | 3.05x | 1.59 | 2.04 | 1.28x | 200 | 200 | 1.00x | 0 | 0 | - |
| 16,384 | 0.38 | 1.3 | 3.47x | 0.66 | 4.7 | 7.17x | 200 | 200 | 1.00x | 0 | 0.017 | - |

## publish vs concurrent users (rate=200, payload_bytes=256)

| concurrent users | p50 lat direct | p50 lat dapr | ratio | p99 lat direct | p99 lat dapr | ratio | thr direct | thr dapr | ratio | err direct | err dapr | ratio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.29 | 0.87 | 3.02x | 0.55 | 1.62 | 2.96x | 200 | 200 | 1.00x | 0 | 0 | - |
| 8 | 0.31 | 0.94 | 2.99x | 1.04 | 2.49 | 2.40x | 200 | 200 | 1.00x | 0 | 0.017 | - |
| 32 | 0.32 | 0.97 | 3.02x | 0.8 | 1.98 | 2.47x | 200 | 200 | 1.00x | 0 | 0 | - |
| 128 | 0.33 | 0.88 | 2.68x | 0.68 | 1.77 | 2.60x | 200 | 200 | 1.00x | 0 | 0 | - |
