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
Copy-Item .env.example .env
```

Edit `.env` and set `DASHBOARD_USERNAME` and `DASHBOARD_PASSWORD` before starting the stack. For example, generate a URL-safe random password with `python -c "import secrets; print(secrets.token_hex(24))"`.

```powershell
docker compose up --build
```

Compose starts a one-broker Kafka KRaft cluster, PostgreSQL, a one-shot database initializer, the consumer, continuous simulator (20 events/s target), API, and dashboard. Once services are healthy:

- Dashboard: <http://localhost:8501>
- API health: <http://localhost:8000/health>
- Interactive API docs: <http://localhost:8000/docs>

The dashboard prompts for the configured account. Analytics and monitoring API routes require HTTP Basic authentication; `/health` is public. This is a local research setup: Basic authentication uses plaintext HTTP, so do not expose the services publicly without TLS and production-grade identity management.

Stop the stack with `Ctrl+C` or `docker compose down`. `docker compose down -v` also removes the local Kafka/PostgreSQL data volumes.

### Run services separately

For development, start Kafka and PostgreSQL first, then activate the virtual environment. Defaults connect to local ports; set `$env:DATABASE_URL`, `$env:KAFKA_BOOTSTRAP_SERVERS`, `$env:DASHBOARD_USERNAME`, and `$env:DASHBOARD_PASSWORD` in PowerShell before starting the API and dashboard. Run each process in a separate terminal:

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

Settings are environment variables; defaults are documented in [.env.example](.env.example). Set `KAFKA_PARTITIONS` to configure the broker's default partition count for newly created topics.

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Comma-separated broker addresses |
| `DATABASE_URL` | local PostgreSQL URL | SQLAlchemy database connection |
| `CONSUMER_GROUP_ID` | `gaming-events-processor` | Consumer group name |
| `ACTIVE_WINDOW_MINUTES` | `5` | Window for counting recently active, online players |
| `API_BASE_URL` | `http://localhost:8000` | Dashboard's API endpoint |
| `EVENT_RATE` | `20` | Producer target events per second |
| `SIMULATED_PLAYERS` | `250` | Simulated player population |
| `KAFKA_PARTITIONS` | `3` | Default partitions for newly created Kafka topics |
| `DASHBOARD_USERNAME` | required | Single configured dashboard/API account |
| `DASHBOARD_PASSWORD` | required | Password for the configured dashboard/API account |

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

Run the requested local scale comparison (1,000 through 500,000 base events; five trials by default) and generate a JSON report, CSV comparison table, and self-contained Plotly graph:

```powershell
python -m scripts.evaluate --repeats 5
```

The largest trial can take substantial time and memory because it measures the local preprocessing plus SQLite write path. Start with `python -m scripts.evaluate --sizes 1000,10000 --repeats 1` to validate the workflow. Outputs contain only completed measurements; the runner never fills missing results with estimates. These local runs do not measure Kafka/PostgreSQL end-to-end performance.

Replay the bundled sample JSONL data to Kafka:

```powershell
python -m scripts.replay_sample
```

Run an authenticated Locust API load test after starting the stack. Use the same account configured in `.env`:

```powershell
$env:DASHBOARD_USERNAME = "your-configured-user"
$env:DASHBOARD_PASSWORD = "your-configured-password"
locust -f scripts/locustfile.py --host http://localhost:8000
```

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
│   ├── benchmark.py
│   ├── evaluate.py
│   ├── locustfile.py
│   └── replay_sample.py
├── data/
│   └── sample_events.jsonl
├── results/
│   ├── local-scale-smoke.csv
│   ├── local-scale-smoke.html
│   └── local-scale-smoke.json
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
