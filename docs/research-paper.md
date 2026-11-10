# Large-Scale Online Gaming Data Processing and Data Visualization Dashboard Using Apache Kafka

**Tharun R K, Teju K G, Tharun K, and Tejas S**

Department of Computer Science and Engineering

Sapthagiri NPS University, Bengaluru, India

## Abstract

Online games produce continuous streams of player activity, score, session, achievement, and transaction events. Batch-oriented processing can delay operational visibility, while independently developed producers and dashboards can leave duplicates, malformed records, and data-quality failures undetected. This paper presents a reproducible prototype for streaming gaming-event processing using Apache Kafka, Python, PostgreSQL, FastAPI, and Streamlit. The event simulator publishes keyed records to domain-specific Kafka topics. A consumer applies schema validation and normalization, derives stable identifiers when event IDs are absent, detects duplicates, routes rejected records to a dead-letter topic, and persists valid events and related facts transactionally. A FastAPI service exposes analytics and health endpoints to an automatically refreshing, dark-themed dashboard. The implementation uses manual Kafka commits after database work and a unique event key to make redelivery safe for stored effects; it does not claim end-to-end exactly-once semantics. A local KRaft-based Docker Compose topology is specified for reproducible experiments. The current development environment did not include Docker, so Kafka/PostgreSQL end-to-end throughput, latency, resource utilization, and dashboard response time have not been measured. The paper therefore defines the evaluation protocol and separates verified software tests from performance results that require a running deployment.

**Keywords—** Apache Kafka, stream processing, gaming analytics, data validation, PostgreSQL, real-time dashboard, event-driven architecture.

## I. Introduction

Interactive online games generate high-velocity operational telemetry: player lifecycle changes, match outcomes, scores, sessions, purchases, achievements, and errors. Operators need recent data to observe participation, engagement, game performance, and transaction activity. A dashboard backed only by periodic files or batch exports can become stale, while unvalidated event streams risk corrupting downstream analytics.

This project studies a practical ingestion-to-visualization path built from open-source components. Kafka decouples event generation from processing and retains partitioned logs for consumer groups [1], [4]. Python provides the simulator, validation, consumer, and analytics. PostgreSQL stores deduplicated facts and relational entities. FastAPI provides an HTTP boundary between storage and visualization, and Streamlit provides an interactive dashboard. The goal is a reproducible undergraduate research prototype and an evaluation methodology—not a claim that one local broker constitutes a production-scale gaming platform.

## II. Problem Statement

The system must receive continuous events from simulated game services and expose useful analytics while addressing duplicate delivery, missing or malformed fields, inconsistent identifiers, processing delay, and limited visibility into the processing pipeline. A useful design must distinguish event time from processing time, preserve sufficient source information for audit, avoid applying duplicate economic or score effects, and surface rejected records rather than silently discarding them.

The system is required to provide near-real-time monitoring for player and game activity, record transaction and session facts, report data-quality counts, and support controlled performance experiments. Scalability claims must be supported by measured workloads and declared infrastructure rather than inferred from the use of Kafka alone.

## III. Literature Review

Kreps, Narkhede, and Rao describe Kafka as a distributed messaging system for log processing, emphasizing a partitioned commit-log abstraction that supports decoupled producers and consumers [1]. This architecture motivates separating the simulator, stream processor, and dashboard-facing data service. Kafka's partitioning also clarifies an important constraint: ordering is scoped to a partition, not the entire cluster [4].

The Dataflow model analyzes trade-offs among correctness, latency, and cost for unbounded, potentially out-of-order data [2]. Its treatment reinforces the need to identify event-time and processing-time semantics and to report latency/accuracy trade-offs. The present prototype uses event timestamps for domain facts and processing timestamps for throughput and latency, but it does not implement event-time windows, watermarks, or late-data retractions.

Dean and Ghemawat's MapReduce work established a programming model for large-scale batch processing [3]. Such a model is appropriate for bounded datasets and offline aggregation, but it does not by itself provide the low-latency event-ingestion loop required by this project. The proposed architecture combines Kafka's continuous log with a stateful consumer and a queryable relational store. The prototype is not a replacement for mature distributed stream processors; complex windowing, checkpointed distributed state, and event-time corrections remain future work.

## IV. Existing System

A basic gaming analytics implementation may write events to local files, periodically aggregate them, and render static or manually refreshed charts. Such an approach is simple to demonstrate but offers limited decoupling, replay, independent consumer scaling, and operational insight. A single JSON snapshot also does not provide normalized historical facts, durable deduplication constraints, or a structured rejection path.

## V. Limitations of Existing System

The limitations addressed are: (1) file-oriented ingestion can couple producers and consumers; (2) delayed batches can hide short-lived traffic changes; (3) absent identifiers make exact duplicate handling difficult; (4) missing mandatory values can contaminate KPI calculations; (5) storing only snapshots limits drill-down and historical analysis; (6) dashboards without pipeline monitoring cannot distinguish low activity from a failed consumer; and (7) configured throughput is often mistakenly reported as measured performance. These are design risks rather than measured failure rates of a particular deployed game.

## VI. Proposed System

The proposed prototype routes player events, scores, transactions, sessions, and achievements to separate Kafka topics. The producer uses the player identifier as the Kafka key, acknowledges broker writes, and limits in-flight requests so retries do not reorder sends. The consumer validates event contracts, normalizes categorical values, deduplicates by event ID, and stores the accepted event plus any related relational fact. Invalid records are published to `gaming-events.dlq` with the original payload and validation reason. A unique database key makes reprocessing safe for persisted event effects after a failure between database commit and Kafka offset commit.

The API derives operational KPIs from persisted records. The dashboard reads only the API and refreshes periodically. Producer heartbeats and consumer batch measurements are stored as pipeline metrics; Kafka group offsets are queried for consumer lag. A compact system and entity design is provided in `architecture.md`.

## VII. System Architecture

The pipeline comprises seven logical stages: synthetic data generation; keyed Kafka production; domain topic buffering; consumer polling and preprocessing; transactional PostgreSQL storage; FastAPI analytics; and the Streamlit dashboard. An additional DLQ path records invalid data. The local deployment runs Kafka in KRaft mode with a single broker, PostgreSQL, the API, consumer, producer, and dashboard in Docker Compose.

Topics are `player-events`, `game-scores`, `transactions`, `game-sessions`, `achievements`, and `pipeline-metrics`; invalid records are sent to `gaming-events.dlq`. The Kafka key preserves per-player ordering within each topic. Cross-topic order is not guaranteed. The consumer follows at-least-once processing with idempotent persistence, rather than asserting distributed exactly-once processing.

## VIII. Research Methodology

### A. Data collection and generation

The simulator creates timestamped events for player registration/login/logout, session start/end, scores, purchases, achievements, matchmaking, and errors. A registration event is emitted before other events for a simulated player, allowing the registered-player KPI to be distinguished from merely observed player IDs. A seed can make event-type selection reproducible. Event IDs are UUIDs at generation time. The configured event rate is a target that must be compared with observed producer and consumer rates; it is not a result.

### B. Streaming architecture

The producer serializes JSON records and sends them to domain topics, keyed by player ID. The consumer group polls those topics and uses manual commits. The system records producer heartbeats and consumer batch summaries. Topic partition count, broker count, and consumer count must be recorded for each performance trial.

### C. Data preprocessing

Pydantic validates required identifiers, timestamp offsets, supported event types, and event-specific fields such as score, purchase amount, and achievement name. Identifiers and categorical dimensions are trimmed and lowercased. Missing region or game identifiers are mapped to `unknown`; missing mandatory values are rejected. A stable SHA-256 identifier derived from canonical JSON is assigned only when an event ID is absent. Duplicate IDs within a batch or database are not re-applied. Invalid JSON, invalid schemas, and event/topic routing mismatches are sent to the DLQ with a reason.

### D. Data storage

PostgreSQL holds a canonical event fact with original JSON, event and processing timestamps, topic, player, game, region, and event-specific metrics. Related tables store players, game sessions, transactions, achievements, and pipeline metrics. Writes for an accepted game event and its materialized fact occur in one database transaction. A unique event ID makes retries idempotent. The schema is created by SQLAlchemy for this prototype; a versioned Alembic migration workflow is recommended before production use.

### E. Analytics and visualization

FastAPI exposes summary KPIs, activity, leaderboard, game comparison, engagement, daily/weekly trends, transactions, sessions, processing performance, health, and Kafka monitoring endpoints. Pandas performs bounded time-series grouping, while NumPy calculates latency summaries. Streamlit periodically calls the API and uses Plotly charts. API and Kafka failures are surfaced on the dashboard rather than converted to fabricated healthy values.

## IX. Implementation

The implementation is organized into `src/producer.py`, `src/preprocessing.py`, `src/consumer.py`, `src/storage.py`, `src/api.py`, and `src/dashboard.py`. `docker-compose.yml` defines a local KRaft broker, PostgreSQL, a one-shot schema initializer, and the application services. `tests/` includes deterministic preprocessing, database, and API regression tests. `.env.example` describes local configuration. The dashboard is not a static mock: its metrics are queried from the persisted event pipeline.

## X. Experimental Setup

The reproducible deployment command is `docker compose up --build`; the dashboard is served on port 8501 and the API on port 8000. Producer target rate, simulated player count, refresh period, Kafka partition count, and broker/consumer counts should be varied across trials. Each experiment should include a warm-up period followed by repeated measured runs. A report should specify CPU, memory, operating system, Python, Kafka and PostgreSQL versions, input count, event-size distribution, trial duration, and all rate controls.

Source verification was performed with Python 3.13.12 and automated tests using SQLite for isolated persistence tests. Docker is not installed in the current authoring environment. Consequently, the Kafka/PostgreSQL Compose deployment could not be started here, and no distributed-pipeline or dashboard network-response measurements are claimed.

## XI. Results and Discussion

The automated checks exercise event normalization, duplicate detection, invalid-record rejection, persistence idempotency, session completion, and analytics API responses against a test database. Five seeded local benchmark trials also measured preprocessing and relational writes using in-memory SQLite. These results are separate from the Kafka/PostgreSQL deployment and do not measure its throughput, consumer lag under load, or dashboard response time.

| Measure | Current evidence | Interpretation |
|---|---|---|
| Preprocessing classification on injected labels | 100% in five local benchmark trials | 3/3 injected invalids, 2/2 duplicates, and 33/33 missing categorical fields handled per run |
| Local preprocessing and SQLite-write throughput | 200.58 stored events/s median; 108.44–267.45 range | Five exploratory trials; excludes Kafka, network, and dashboard |
| Local event-write latency | 3.488 ms median of trial medians; 6.888 ms median of trial p95 values | Same five local trials; not Kafka end-to-end latency |
| Kafka processing latency | Not measured | Requires a running broker and consumer |
| Kafka throughput | Not measured | Producer rate setting is not an observed throughput result |
| Dashboard response time | Not measured | Requires the API and dashboard running with a workload |
| Horizontal scalability | Not measured | Local Compose uses a single broker |
| CPU and memory utilization | Not measured | Requires target-host instrumentation |

A local preprocessing/SQLite benchmark measured a five-trial median of 200.58 stored events/s in this environment; per-trial throughput ranged from 108.44 to 267.45 events/s. The benchmark excludes Kafka, network, PostgreSQL, and dashboard rendering. It is an exploratory local microbenchmark, not a result for the distributed architecture. Full run details and limitations are in `performance-evaluation.md`. A future distributed experiment should report measured values with trial variance and clearly label synthetic workloads. The one-node demonstration cannot validate broker failover or multi-broker replication.

## XII. Performance Analysis

For a run containing \(N\) unique valid events over wall-clock interval \(T\), observed throughput is \(N/T\) events per second. Per-event consumer processing latency should be measured from consumer processing start through database write completion; event age at consumption should be reported separately because it includes producer and queue delay. Report median and p95 latency, not only a mean. Dashboard response time should be measured at the HTTP API and, separately, through a browser render if end-user latency is being claimed.

Preprocessing accuracy should compare accepted/rejected outcomes against known injected labels. Duplicate detection should report true duplicates detected and unique records incorrectly rejected. Missing-value handling should report each imputed field and each rejected mandatory field. Consumer lag is computed from topic end offsets minus committed group offsets. CPU and resident memory must be observed on the process host. The only measured result currently available is the explicitly scoped local microbenchmark; the distributed experiment remains unmeasured.

## XIII. Advantages

The design separates producers, stream processing, storage, and visualization; retains raw event payloads for audit; uses domain topics and per-player partition keys; makes event redelivery safe through an event-ID constraint; surfaces invalid input in a DLQ; exposes data and pipeline metrics through an API; and supports repeatable synthetic input. Its local deployment is accessible for undergraduate study.

## XIV. Limitations

The provided Compose broker and database are single instances and are not highly available. The workload is synthetic and cannot represent real player behavior or production traffic without calibration. SQLAlchemy schema creation is not a substitute for versioned migrations. The prototype uses polling and a relational read path; it does not implement a distributed stream-processing engine, event-time windows, watermarks, privacy controls, authentication, rate limiting, schema registry, or production secrets management. Per-topic ordering does not imply a global event order. Distributed performance results are environment-dependent and remain unmeasured.

## XV. Future Enhancements

Future work includes multi-broker Kafka with replication, a schema registry and compatibility policy, versioned Alembic migrations, secure authentication and TLS, batch database writes, partition-aware consumer scaling, event-time windowing and late-data policies, retention and archival, monitoring with Prometheus/Grafana, privacy-preserving identifiers, and tests using generated workloads based on a documented real-world distribution. Controlled scale-out experiments should vary partitions, consumers, payload size, and event rate.

## XVI. Conclusion

This work specifies and implements an extensible path from synthetic gaming events through Kafka preprocessing and relational persistence to API-backed real-time visualization. It addresses duplicates, missing values, invalid records, and monitoring as first-class system concerns. The deliverable is a reproducible research prototype, not evidence of production capacity. The current verified test scope is documented separately from distributed performance, which must be measured after deploying the Compose stack on a declared host.

## References

[1] J. Kreps, N. Narkhede, and J. Rao, “Kafka: A Distributed Messaging System for Log Processing,” in *Proceedings of the NetDB Workshop*, Athens, Greece, 2011. [Online]. Available: https://notes.stephenholiday.com/Kafka.pdf

[2] T. Akidau *et al*., “The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing,” *Proceedings of the VLDB Endowment*, vol. 8, no. 12, pp. 1792–1803, 2015, doi: [10.14778/2824032.2824076](https://doi.org/10.14778/2824032.2824076).

[3] J. Dean and S. Ghemawat, “MapReduce: Simplified Data Processing on Large Clusters,” in *Proceedings of the 6th USENIX Symposium on Operating Systems Design and Implementation (OSDI ’04)*, 2004, pp. 137–150. [Online]. Available: https://www.usenix.org/legacy/event/osdi04/tech/dean.html

[4] Apache Software Foundation, “Apache Kafka Documentation.” [Online]. Available: https://kafka.apache.org/documentation/

[5] PostgreSQL Global Development Group, “PostgreSQL 16 Documentation.” [Online]. Available: https://www.postgresql.org/docs/16/

[6] FastAPI, “FastAPI Documentation.” [Online]. Available: https://fastapi.tiangolo.com/
