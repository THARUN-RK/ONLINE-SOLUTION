# Research Concept and System Architecture

## Research concept

The project is a reproducible streaming-data prototype for online-game operations. A synthetic game telemetry source continuously emits player, score, purchase, session, and achievement events. Kafka buffers those event streams by domain. A consumer validates and normalizes records, identifies duplicates, routes malformed records to a dead-letter topic, and commits Kafka offsets only after database work succeeds. PostgreSQL stores the event fact and related player, session, transaction, achievement, and pipeline-monitoring records. FastAPI serves database-backed analytics, while Streamlit polls those endpoints and renders a dark operational dashboard.

The experiment is designed to evaluate the behavior of this pipeline under controlled input rates. A single-node local deployment demonstrates integration; it is not evidence of production-scale throughput or broker fault tolerance.

## Architecture diagram

```mermaid
flowchart LR
    SIM["Gaming event simulator<br/>seedable player / game events"]
    PRODUCER["Kafka producer<br/>player key · acks=all · gzip · ordered retries"]
    subgraph KAFKA["Apache Kafka · KRaft"]
        P["player-events"]
        S["game-scores"]
        T["transactions"]
        G["game-sessions"]
        A["achievements"]
        M["pipeline-metrics"]
        D["gaming-events.dlq"]
    end
    CONSUMER["Consumer group<br/>poll · validate · deduplicate"]
    PRE["Preprocessing<br/>Pydantic + pandas"]
    DB[("PostgreSQL<br/>events · players · sessions<br/>transactions · achievements · metrics")]
    API["FastAPI<br/>analytics · health · Kafka lag"]
    DASH["Streamlit dashboard<br/>auto-refresh · Plotly"]
    SIM --> PRODUCER
    PRODUCER --> P
    PRODUCER --> S
    PRODUCER --> T
    PRODUCER --> G
    PRODUCER --> A
    PRODUCER --> M
    P --> CONSUMER
    S --> CONSUMER
    T --> CONSUMER
    G --> CONSUMER
    A --> CONSUMER
    M --> CONSUMER
    CONSUMER --> PRE
    PRE -->|valid, unique events| DB
    PRE -->|invalid payloads| D
    CONSUMER -->|commit after persistence| KAFKA
    DB --> API
    API --> DASH
```

## Data-flow diagram

```mermaid
flowchart TD
    A["Generate event<br/>unique event_id + UTC timestamp"] --> B["Select domain topic<br/>key = player_id"]
    B --> C["Kafka partition log"]
    C --> D["Consumer poll batch"]
    D --> E{"JSON payload valid?"}
    E -->|No| X["Publish rejection + reason<br/>to gaming-events.dlq"]
    E -->|Yes| F["Normalize fields<br/>lowercase identifiers; missing region/game → unknown"]
    F --> G["Validate event schema<br/>required fields and event-specific values"]
    G -->|Invalid| X
    G -->|Valid| H{"event_id already stored<br/>or repeated in batch?"}
    H -->|Yes| I["Count duplicate; do not reapply effects"]
    H -->|No| J["Write event and related facts<br/>in one PostgreSQL transaction"]
    J --> K["Update player / session / transaction / achievement facts"]
    X --> L["Flush DLQ producer"]
    I --> M["Commit Kafka offsets after all writes"]
    K --> M
    L --> M
    M --> N["FastAPI queries analytics and health"]
    N --> O["Streamlit refreshes charts and KPIs"]
```

## Use-case diagram

```mermaid
flowchart LR
    Operator["Game operations analyst"]
    Researcher["Researcher / student"]
    Producer["Event simulator"]
    Consumer["Kafka consumer"]
    Dashboard["Gaming analytics dashboard"]
    API["Analytics API"]
    DB["PostgreSQL"]
    Kafka["Kafka cluster"]
    Operator --> U1(["Monitor active players and game KPIs"])
    Operator --> U2(["Inspect transactions and sessions"])
    Operator --> U3(["Review consumer lag and pipeline health"])
    Researcher --> U4(["Configure event rate and random seed"])
    Researcher --> U5(["Run data-quality and performance evaluation"])
    Producer --> U6(["Publish domain events and producer heartbeats"])
    Consumer --> U7(["Validate, deduplicate, persist, and dead-letter"])
    U6 --> Kafka
    Kafka --> U7
    U7 --> DB
    U1 --> Dashboard
    U2 --> Dashboard
    U3 --> Dashboard
    Dashboard --> API
    API --> DB
    U5 --> DB
```

## Relational entity-relationship diagram

```mermaid
erDiagram
    PLAYERS ||--o{ GAMING_EVENTS : generates
    PLAYERS ||--o{ GAME_SESSIONS : plays
    PLAYERS ||--o{ TRANSACTIONS : makes
    PLAYERS ||--o{ PLAYER_ACHIEVEMENTS : earns
    GAMING_EVENTS ||--o| TRANSACTIONS : materializes
    GAMING_EVENTS ||--o| PLAYER_ACHIEVEMENTS : materializes

    PLAYERS {
        varchar player_id PK
        timestamptz first_seen_at
        timestamptz last_seen_at
        timestamptz last_active_at
        timestamptz registered_at
        boolean is_online
        integer events_count
        float total_score
    }
    GAMING_EVENTS {
        varchar event_id PK
        varchar event_type
        varchar topic
        timestamptz event_timestamp
        timestamptz processed_at
        varchar player_id FK
        varchar region
        varchar game_id
        float score
        numeric amount
        integer duration_seconds
        float processing_latency_ms
        json payload
    }
    GAME_SESSIONS {
        varchar session_id PK
        varchar player_id FK
        varchar game_id
        timestamptz started_at
        timestamptz ended_at
        integer duration_seconds
    }
    TRANSACTIONS {
        varchar event_id PK, FK
        varchar player_id FK
        varchar game_id
        numeric amount
        timestamptz occurred_at
    }
    PLAYER_ACHIEVEMENTS {
        varchar event_id PK, FK
        varchar player_id FK
        varchar game_id
        varchar achievement_name
        timestamptz achieved_at
    }
    PIPELINE_METRICS {
        integer id PK
        varchar component
        timestamptz recorded_at
        float events_per_second
        integer total_events
        integer invalid_events
        integer duplicate_events
        float batch_latency_ms
    }
```

## Kafka topic and event contract

All game events contain a unique `event_id`, a timezone-aware ISO-8601 `timestamp`, a supported `type`, and a non-empty `user_id`. `region` and `game_id` are normalized to lowercase and use `unknown` when absent. If an event arrives without an ID, the preprocessor derives a stable SHA-256 ID from its canonical JSON representation; byte-for-byte equivalent payloads therefore deduplicate. A missing player ID, timestamp, required score, purchase amount, or achievement name is rejected rather than silently fabricated.

| Topic | Event types | Key | Purpose |
|---|---|---|---|
| `player-events` | registration, login, logout, error, matchmaking request/result | `user_id` | Player lifecycle and operational events |
| `game-scores` | score | `user_id` | Score facts and leaderboard updates |
| `transactions` | purchase | `user_id` | Economy records |
| `game-sessions` | session start/end | `user_id` | Session facts and duration |
| `achievements` | achievement | `user_id` | Achievement facts |
| `pipeline-metrics` | producer heartbeat | component name | Producer throughput monitoring |
| `gaming-events.dlq` | rejected message envelope | fixed rejection key | Invalid JSON, schema, or routing records |

Kafka ordering is per partition, not global. Using a player key preserves ordering for one player within a topic. Events spanning different topics do not have a total order. The consumer uses manual offset commits: domain records are committed after PostgreSQL writes and dead-letter publishing complete. The event ID primary key makes redelivery idempotent and provides at-least-once delivery with deduplicated database effects; this is not a claim of end-to-end exactly-once processing.

The producer uses `acks=all`, gzip compression, and one in-flight request per broker connection so retries do not reorder its sends. Producer heartbeat totals count broker-acknowledged game events rather than queued sends.

## Methodology and evaluation protocol

1. **Generate:** emit seeded, timestamped player lifecycle, score, purchase, achievement, matchmaking, and session events at a configured rate.
2. **Collect:** route events to domain topics and partition by player ID. The local Compose broker has one node and three default partitions per topic.
3. **Preprocess:** decode JSON, canonicalize identifiers, impute only documented categorical fields, validate required and type-specific values, and remove duplicate IDs.
4. **Reject:** publish malformed or schema-invalid messages to the DLQ with the original payload and rejection reason. No invalid event is represented as a successful game fact.
5. **Persist:** write an event and its normalized side-table facts in one PostgreSQL transaction. Commit Kafka offsets only after processing has completed.
6. **Analyze:** compute KPIs and trends from stored facts using SQL, pandas, and NumPy.
7. **Visualize:** poll the HTTP API from Streamlit at a configurable refresh interval; the dashboard never consumes Kafka directly.
8. **Evaluate:** run unit and integration tests; for a distributed experiment, record machine/VM details, event count, producer target rate, partition count, consumer count, warm-up, repeated trial count, and measured latency/throughput. Report run-level median and p95 rather than treating a configured target as measured throughput.

The Compose setup is a teaching deployment: one Kafka broker and one PostgreSQL instance have no high availability. To study scale, increase Kafka brokers and partitions, run multiple consumer replicas in the same group, and record partition assignment, consumer lag, CPU, memory, and database saturation. A local single-broker result must not be extrapolated to a production cluster.
