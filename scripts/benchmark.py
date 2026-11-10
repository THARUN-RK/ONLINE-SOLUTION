from __future__ import annotations

import argparse
import json
import platform
import sys
import time
import tracemalloc
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

import numpy as np
from sqlalchemy import create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from src.preprocessing import preprocess_batch, topic_for_event
from src.producer import GamingEventGenerator
from src.storage import GamingEventRecord, create_schema, store_event


def _engine(database_url: str) -> Engine:
    if database_url == "sqlite+pysqlite:///:memory:":
        return create_engine(
            database_url,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
    return create_engine(database_url, pool_pre_ping=True)


def run_benchmark(
    *,
    count: int,
    seed: int,
    player_count: int,
    database_url: str = "sqlite+pysqlite:///:memory:",
    api_url: str | None = None,
    api_checks: int = 10,
) -> dict[str, Any]:
    if count < 1:
        raise ValueError("count must be at least one")
    if player_count < 1:
        raise ValueError("player_count must be at least one")

    generator = GamingEventGenerator(player_count=player_count, seed=seed)
    raw_events: list[dict[str, Any]] = []
    expected_invalid = 0
    expected_duplicates = 0
    expected_imputed_fields = 0
    latest_valid_event: dict[str, Any] | None = None

    for index in range(count):
        event = generator.build_event()
        is_invalid = index % 97 == 0
        if is_invalid:
            event["type"] = "score"
            event.pop("score", None)
            expected_invalid += 1
        else:
            latest_valid_event = event

        if index % 13 == 0:
            event.pop("region", None)
            expected_imputed_fields += not is_invalid
        if index % 17 == 0:
            event.pop("game_id", None)
            expected_imputed_fields += not is_invalid
        raw_events.append(event)

        if index > 0 and index % 89 == 0 and latest_valid_event is not None:
            raw_events.append(dict(latest_valid_event))
            expected_duplicates += 1

    engine = _engine(database_url)
    create_schema(engine)
    process_cpu_start = time.process_time()
    wall_start = time.perf_counter()
    tracemalloc.start()
    preprocess_start = time.perf_counter()
    batch = preprocess_batch(raw_events)
    preprocessing_ms = (time.perf_counter() - preprocess_start) * 1000

    stored = 0
    for event in batch.events:
        if store_event(engine, event, topic_for_event(event)):
            stored += 1

    wall_seconds = time.perf_counter() - wall_start
    process_cpu_seconds = time.process_time() - process_cpu_start
    _, peak_python_memory_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    latencies: list[float] = []
    event_ids = [event.event_id for event in batch.events]
    with Session(engine) as session:
        for offset in range(0, len(event_ids), 500):
            latencies.extend(
                session.scalars(
                    select(GamingEventRecord.processing_latency_ms).where(
                        GamingEventRecord.event_id.in_(event_ids[offset : offset + 500])
                    )
                ).all()
            )
    engine.dispose()

    detected_invalid = len(batch.invalid)
    observed_imputed_fields = sum(
        event.region == "unknown" for event in batch.events
    ) + sum(event.game_id == "unknown" for event in batch.events)
    classification_total = len(raw_events)
    correctly_classified = stored + detected_invalid + batch.duplicates

    result: dict[str, Any] = {
        "scope": "local Python preprocessing and relational storage; excludes Kafka, network, and dashboard rendering",
        "measured_at_utc": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "database_dialect": database_url.split(":", 1)[0],
            "seed": seed,
            "configured_base_events": count,
            "raw_records_including_injected_duplicates": len(raw_events),
            "simulated_players": player_count,
        },
        "data_quality": {
            "expected_valid_unique": count - expected_invalid,
            "observed_valid_unique": len(batch.events),
            "expected_invalid": expected_invalid,
            "observed_invalid": detected_invalid,
            "expected_duplicates": expected_duplicates,
            "observed_duplicates": batch.duplicates,
            "classification_accuracy_percent": round(
                100 * correctly_classified / max(classification_total, 1), 2
            ),
            "duplicate_detection_rate_percent": (
                round(100 * batch.duplicates / expected_duplicates, 2)
                if expected_duplicates
                else None
            ),
            "missing_categorical_fields_injected": expected_imputed_fields,
            "missing_categorical_fields_imputed": observed_imputed_fields,
        },
        "performance": {
            "wall_clock_seconds": round(wall_seconds, 6),
            "preprocessing_ms": round(preprocessing_ms, 3),
            "stored_events": stored,
            "stored_events_per_second": round(stored / max(wall_seconds, 1e-9), 2),
            "database_processing_latency_median_ms": round(median(latencies), 3) if latencies else 0.0,
            "database_processing_latency_p95_ms": round(float(np.percentile(latencies, 95)), 3)
            if latencies
            else 0.0,
            "process_cpu_seconds": round(process_cpu_seconds, 6),
            "process_cpu_percent_of_one_core": round(100 * process_cpu_seconds / max(wall_seconds, 1e-9), 2),
            "peak_traced_python_memory_bytes": peak_python_memory_bytes,
            "memory_note": "tracemalloc excludes native NumPy, pandas, and database-driver allocations",
        },
    }

    if api_url:
        import requests

        response_times_ms: list[float] = []
        api_error: str | None = None
        for _ in range(max(1, api_checks)):
            request_start = time.perf_counter()
            try:
                response = requests.get(
                    f"{api_url.rstrip('/')}/api/v1/analytics/summary",
                    timeout=5,
                )
                response.raise_for_status()
            except requests.RequestException as error:
                api_error = str(error)
                break
            response_times_ms.append((time.perf_counter() - request_start) * 1000)
        result["api_response"] = (
            {
                "status": "measured",
                "successful_requests": len(response_times_ms),
                "median_ms": round(median(response_times_ms), 3),
                "p95_ms": round(float(np.percentile(response_times_ms, 95)), 3),
            }
            if response_times_ms and api_error is None
            else {"status": "unavailable", "error": api_error}
        )

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Measure local preprocessing and database performance using seeded synthetic events"
    )
    parser.add_argument("--count", type=int, default=1000, help="Base events to generate")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--players", type=int, default=250)
    parser.add_argument("--database-url", default="sqlite+pysqlite:///:memory:")
    parser.add_argument("--api-url", default=None, help="Optional running API URL for response-time checks")
    parser.add_argument("--api-checks", type=int, default=10)
    parser.add_argument("--output", type=Path, default=None, help="Optional JSON report path")
    args = parser.parse_args()

    report = run_benchmark(
        count=args.count,
        seed=args.seed,
        player_count=args.players,
        database_url=args.database_url,
        api_url=args.api_url,
        api_checks=args.api_checks,
    )
    rendered = json.dumps(report, indent=2)
    print(rendered)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
