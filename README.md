# Large-Scale Online Gaming Data Processing and Visualization Dashboard

This project demonstrates a real-time gaming analytics architecture using Apache Kafka, stream aggregation, and a live dashboard. It is designed around the scenario described in the abstract: online game events are ingested as streams, aggregated into operational KPIs, and displayed in a real-time operator dashboard.

## Features

- Simulated gaming event producer using Kafka
- Real-time event consumption and KPI aggregation
- Metrics for:
  - concurrent users
  - matchmaking wait time
  - economy volume
  - error rate
  - regional usage
- Streamlit dashboard for live monitoring
- Docker Compose setup for Kafka and Zookeeper

## Architecture

- Event producers: game servers, matchmaking services, economy services
- Kafka topic: `gaming-events`
- Consumer/aggregator: calculates key metrics and writes them to JSON
- Dashboard: Streamlit app reads the aggregated output and visualizes trends

## Quick Start

### 1) Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2) Start Kafka

```bash
docker compose up -d
```

### 3) Run the producer

```bash
python src/producer.py --count 200 --sleep 0.2
```

### 4) Run the consumer

```bash
python src/consumer.py
```

### 5) Launch the dashboard

```bash
streamlit run src/dashboard.py
```

## Offline Demo

If Kafka is not available, you can still generate a sample snapshot locally:

```bash
python src/consumer.py --demo
```

Then open the dashboard with:

```bash
streamlit run src/dashboard.py
```

## Files

- `src/producer.py` – emits gaming events to Kafka
- `src/consumer.py` – consumes events and stores aggregated metrics
- `src/metrics.py` – KPI aggregation logic
- `src/dashboard.py` – Streamlit visualization layer
- `docker-compose.yml` – Kafka/Zookeeper setup
- `tests/test_metrics.py` – unit tests for metric calculations

## Environment Notes

This project is configured for Python 3.13. The dashboard intentionally avoids a pandas dependency to keep the environment portable and installation simple on Windows.

## Example KPIs

- Concurrent users
- Matchmaking latency
- Regional load
- Error rate percentage
- Virtual economy value processed

## Notes

This project is intentionally lightweight and meant as a prototype/reference implementation for academic or learning purposes.
