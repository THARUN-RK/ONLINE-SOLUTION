# Performance Evaluation Record

## Scope and reproducibility

This is the actual result of five consecutive runs of the included benchmark on 2026-11-10. Each run used `python -m scripts.benchmark --count 250 --seed 42` (the benchmark's default player count is 250). The run evaluated the Python preprocessing and relational-write path against a fresh in-memory SQLite database. It did **not** run Kafka, PostgreSQL, the API server, the dashboard, or network traffic.

Environment: Python 3.13.12; Windows 11, build 26300; in-memory SQLite; 250 base events per trial; 252 raw records after inserting two known duplicates; three injected invalid score records; missing region/game fields in valid records for imputation checks. A single measured trial takes approximately a few seconds. No warm-up trial was excluded, so results are exploratory and host-state-sensitive.

## Measured results

Values below are the median and range across the five observed trials. They are measurements from this environment, not predicted Kafka cluster performance.

| Metric | Measured result |
|---|---:|
| Unique valid records stored per run | 247 of 250 base events |
| Injected invalid records identified | 3 of 3 |
| Injected duplicate records identified | 2 of 2 |
| Injected missing categorical fields imputed | 33 of 33 |
| Classification accuracy on the known synthetic labels | 100% in all five runs |
| Stored-events throughput | 200.58 events/s median; 108.44–267.45 events/s |
| Per-event database processing latency, median | 3.488 ms median of trial medians; 2.510–6.897 ms |
| Per-event database processing latency, p95 | 6.888 ms median of trial p95 values; 4.612–10.370 ms |
| End-to-end local preprocessing/write wall time | 1.231 s median; 0.924–2.278 s |
| Process CPU time / wall time | 93.40% of one core median; 92.03–94.75% |
| Peak traced Python memory | 678,044 bytes median; 673,066–1,088,335 bytes |

`tracemalloc` does not include native NumPy, pandas, or database-driver allocations. The CPU percentage is normalized to one core, and may vary with scheduling. Synthetic classification accuracy measures only injected examples in this known test set; it is not an estimate of accuracy on real gaming telemetry. The small test is not a scaling benchmark.

## Not measured

Kafka producer-to-consumer latency and throughput, PostgreSQL performance, dashboard response time, consumer lag under load, multi-consumer scaling, broker failure recovery, and full-host CPU/RSS remain **not measured**. Docker was unavailable in the authoring environment. Do not copy the local SQLite measurements into a claim about a Kafka/PostgreSQL deployment.

## Recommended distributed experiment

1. Start the Docker Compose deployment on a host with Docker Compose v2.
2. Record CPU, memory, OS, Python, Kafka, PostgreSQL, partition count, and consumer count.
3. Stop the continuous Compose producer and run the bounded simulator at a documented target rate and event count.
4. Allow a warm-up interval, then repeat at least five measured runs for each configuration.
5. Record observed producer heartbeat rate, consumer throughput, p50/p95 processing latency, event age, Kafka committed-offset lag, validation outcomes, dashboard HTTP response time, process CPU, and resident memory.
6. Publish raw run records and aggregate medians/ranges. Do not equate producer target rate with achieved throughput.

For API response-time checks, use `python -m scripts.benchmark --count 1000 --seed 42 --api-url http://localhost:8000`; this adds HTTP request measurements against the running API but still does not make the local SQLite portion an end-to-end Kafka benchmark.
