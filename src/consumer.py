from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List

from kafka import KafkaConsumer

from config import BOOTSTRAP_SERVERS, METRICS_FILE, TOPIC
from metrics import GameMetricsAggregator


def read_history() -> List[Dict[str, Any]]:
    if not METRICS_FILE.exists():
        return []
    try:
        payload = json.loads(METRICS_FILE.read_text(encoding="utf-8"))
        return payload.get("history", [])
    except json.JSONDecodeError:
        return []


def write_metrics(aggregator: GameMetricsAggregator) -> None:
    snapshot = aggregator.snapshot()
    history = read_history()
    history.append(snapshot)
    if len(history) > 40:
        history = history[-40:]

    payload = {"latest": snapshot, "history": history}
    METRICS_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def consume_events() -> None:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="gaming-dashboard-group",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    aggregator = GameMetricsAggregator()

    for message in consumer:
        event = message.value
        aggregator.handle_event(event)
        write_metrics(aggregator)
        print(f"Processed {event.get('type')} at {event.get('timestamp')}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consume gaming events from Kafka and aggregate dashboard metrics")
    parser.add_argument("--demo", action="store_true", help="Consume a small synthetic batch for offline demo mode")
    args = parser.parse_args()

    if args.demo:
        demo_aggregator = GameMetricsAggregator()
        demo_events = [
            {"type": "session_start", "user_id": "u1", "region": "us-east"},
            {"type": "session_start", "user_id": "u2", "region": "us-east"},
            {"type": "session_start", "user_id": "u3", "region": "europe-west"},
            {"type": "matchmaking_request", "user_id": "u1", "region": "us-east"},
            {"type": "match_found", "user_id": "u1", "region": "us-east", "requested_at": "2024-01-01T00:00:00+00:00", "matched_at": "2024-01-01T00:00:04+00:00"},
            {"type": "purchase", "user_id": "u2", "region": "us-east", "amount": 120.5},
            {"type": "error", "user_id": "u3", "region": "europe-west", "error_code": "E104"},
            {"type": "session_end", "user_id": "u2", "region": "us-east"},
        ]
        for event in demo_events:
            demo_aggregator.handle_event(event)
        write_metrics(demo_aggregator)
        print(METRICS_FILE.read_text(encoding="utf-8"))
    else:
        consume_events()
