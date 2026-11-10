# Large-Scale Online Gaming Data Processing and Visualization

**Research prototype:** a working event-stream pipeline using Apache Kafka, Python, PostgreSQL, FastAPI, and Streamlit.

**Project site:** <https://tharun-rk.github.io/large-scale-online-gaming/>

**Authors:** Tharun R K, Teju K G, Tharun K, and Tejas S<br>
**Department:** Computer Science and Engineering, Sapthagiri NPS University, Bengaluru, India

## Research concept

Game telemetry is generated continuously and arrives faster than a periodic file-based dashboard can reliably represent. This project simulates game-service events, publishes them to domain-specific Kafka topics, validates and deduplicates them in a consumer, persists accepted records and related facts in PostgreSQL, and serves live analytics through FastAPI to an auto-refreshing Streamlit dashboard.

The local deployment demonstrates integration and data-quality handling. It is not a multi-broker production cluster, and no production-scale throughput or fault-tolerance claim is made. See the [complete IEEE-style research paper](docs/research-paper.md) and [architecture, data-flow, use-case, and ER diagrams](docs/architecture.md).

## Architecture

```text
Gaming event simulator
        │
        ▼
Kafka producer ──► Kafka KRaft domain topics
                         │
                         ▼
             Consumer group + preprocessing ──► gaming-events.dlq
                         │
                         ▼
     PostgreSQL: events, players, sessions, transactions,
          achievements, producer/consumer metrics
                         │
                         ▼
               FastAPI analytics API
                         │
                         ▼
        Streamlit dark dashboard (auto-refresh)
```

### Kafka topics

| Topic | Event types |
|---|---|
| `player-events` | registration, login/logout, errors, matchmaking |
| `game-scores` | score |
| `transactions` | purchase |
| `game-sessions` | session start/end |
| `achievements` | achievement |
| `pipeline-metrics` | producer heartbeat |
| `gaming-events.dlq` | invalid JSON, schema, or routing records |

Game events are keyed by `user_id`, preserving a player's order within an individual topic partition. The producer uses one in-flight request per broker connection so retries do not reorder sends. Kafka does not provide a total order across topics.

### Validation and delivery behavior

- Every accepted event requires a supported type, non-empty player ID, and timezone-aware timestamp. The simulator emits a registration event the first time each simulated player appears.
- Scores, purchase amounts, achievements, and matchmaking results have type-specific checks.
- Region and game identifiers are trimmed/lowercased; missing values become `unknown`.
- If `event_id` is absent, a SHA-256 ID is generated from canonical JSON. Repeated IDs are skipped.
- Invalid records are sent to the DLQ with the source payload and rejection reason.
- The consumer commits offsets after persistence and DLQ publishing. PostgreSQL's event-ID primary key makes redelivery idempotent. This is at-least-once processing with deduplicated database effects, not end-to-end exactly-once processing.

## Dashboard

The dark dashboard refreshes by calling FastAPI at a configurable interval. It includes:

1. Registered and active players, event and session counts, mean score, transaction count, observed throughput, and processing latency.
2. Recent player activity and events per second.
3. Top-10 leaderboard and game comparison.
4. Engagement, daily and weekly trends, transaction trends, and session-duration distribution.
5. Consumer batch performance, producer and consumer heartbeats, and Kafka consumer-group lag.

The dashboard reports unavailable API/broker errors explicitly rather than displaying a healthy-looking fallback.

## Database design

SQLAlchemy creates the following PostgreSQL tables at service startup:

- `players`: first/last seen, registration time, online status, event count, cumulative score.
- `gaming_events`: unique event ID, event/processing time, topic, player/game/region, payload, and processing latency.
- `game_sessions`: start/end and duration.
- `transactions`: amount and occurrence time, linked to the source event.
- `player_achievements`: achievement facts, linked to the source event.
- `pipeline_metrics`: producer/consumer throughput, invalid/duplicate counts, and batch latency.

The project uses `create_all` for the teaching prototype. Use versioned migrations (for example Alembic) before evolving a production schema.

## Run locally with Docker Compose

Docker Desktop (with Compose v2) is required.

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
docker compose up --build
```

Compose starts a one-broker Kafka KRaft cluster, PostgreSQL, a one-shot database initializer, the consumer, continuous simulator (20 events/s target), API, and dashboard. Once services are healthy:

- Dashboard: <http://localhost:8501>
- API health: <http://localhost:8000/health>
- Interactive API docs: <http://localhost:8000/docs>

Stop the stack with `Ctrl+C` or `docker compose down`. `docker compose down -v` also removes the local Kafka/PostgreSQL data volumes.

### Run services separately

For development, start Kafka and PostgreSQL first, then activate the virtual environment. Defaults connect to local ports; set `$env:DATABASE_URL` and `$env:KAFKA_BOOTSTRAP_SERVERS` in PowerShell if your local ports or credentials differ. Run each process in a separate terminal:

```powershell
python -m src.consumer
python -m src.producer --rate 20
uvicorn src.api:app --reload
streamlit run src/dashboard.py
```

Generate a bounded, seedable batch instead of continuous traffic:

```powershell
python -m src.producer --count 20000 --rate 500 --seed 42
```

When using the Compose broker from the host, use `localhost:9092`; application containers use `kafka:29092`.

## Configuration

Settings are environment variables; defaults are documented in [.env.example](.env.example).

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Comma-separated broker addresses |
| `DATABASE_URL` | local PostgreSQL URL | SQLAlchemy database connection |
| `CONSUMER_GROUP_ID` | `gaming-events-processor` | Consumer group name |
| `ACTIVE_WINDOW_MINUTES` | `5` | Window for counting recently active, online players |
| `API_BASE_URL` | `http://localhost:8000` | Dashboard's API endpoint |
| `EVENT_RATE` | `20` | Producer target events per second |
| `SIMULATED_PLAYERS` | `250` | Simulated player population |

The Compose credentials and plaintext listeners are for a local development demonstration only. Do not expose them to the Internet or reuse them in production.

## Testing and performance evaluation

Run automated tests:

```powershell
python -m pytest -q
```

Run a repeatable local preprocessing/database microbenchmark:

```powershell
python -m scripts.benchmark --count 1000 --seed 42 --output results/local-benchmark.json
```

This benchmark uses in-memory SQLite by default and measures the local Python preprocessing and relational-write path. It deliberately does **not** measure Kafka, network delay, the Compose PostgreSQL service, or dashboard rendering. It injects known malformed records, duplicate IDs, and missing categorical fields, and reports those counts alongside throughput, latency, CPU time, and traced Python memory. A measured result is meaningful only with its machine and database details.

For API response-time checks, run the stack and pass `--api-url http://localhost:8000`. For a streaming experiment, stop Compose's continuous producer, run the bounded producer at a declared rate, and record the API's observed processing throughput, latency, Kafka lag, CPU, and memory over repeated trials. Do not report the configured producer rate as measured throughput.

**Current validation limitation:** Docker is unavailable in the authoring environment, so the Kafka/PostgreSQL Compose deployment and distributed performance experiment have not been run here. A five-trial local SQLite microbenchmark was measured and is documented in [docs/performance-evaluation.md](docs/performance-evaluation.md); it is not an end-to-end Kafka result.

## Project structure

```text
.
├── .streamlit/
│   └── config.toml
├── .env.example
├── Dockerfile
├── docker-compose.yml
├── docs/
│   ├── architecture.md
│   ├── performance-evaluation.md
│   └── research-paper.md
├── scripts/
│   └── benchmark.py
├── src/
│   ├── __init__.py
│   ├── api.py
│   ├── config.py
│   ├── consumer.py
│   ├── dashboard.py
│   ├── metrics.py
│   ├── preprocessing.py
│   ├── producer.py
│   └── storage.py
└── tests/
    ├── test_metrics.py
    └── test_pipeline.py
```

`src/metrics.py` and `tests/test_metrics.py` preserve the original snapshot-aggregator prototype; the live Compose pipeline and dashboard use PostgreSQL-backed analytics in `src/storage.py` and `src/api.py`.
