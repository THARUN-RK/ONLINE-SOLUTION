from __future__ import annotations

import argparse
import json
import random
import time
from datetime import datetime, timezone
from typing import Dict, Any

from kafka import KafkaProducer

from config import BOOTSTRAP_SERVERS, TOPIC

REGIONS = ["us-east", "europe-west", "asia-south", "sa-east"]
EVENT_TYPES = [
    "session_start",
    "session_end",
    "matchmaking_request",
    "match_found",
    "purchase",
    "error",
]


def build_event(user_id: str, event_type: str) -> Dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    region = random.choice(REGIONS)
    payload: Dict[str, Any] = {
        "timestamp": now,
        "type": event_type,
        "region": region,
        "user_id": user_id,
    }

    if event_type == "session_start":
        payload["server"] = f"{region}-server-{random.randint(1, 6)}"
    elif event_type == "matchmaking_request":
        payload["requested_at"] = now
    elif event_type == "match_found":
        payload["requested_at"] = now
        requested_at = datetime.now(timezone.utc)
        matched_at = requested_at.timestamp() + random.uniform(1.5, 6.5)
        payload["matched_at"] = datetime.fromtimestamp(matched_at, tz=timezone.utc).isoformat()
    elif event_type == "purchase":
        payload["amount"] = round(random.uniform(15.0, 200.0), 2)
    elif event_type == "error":
        payload["error_code"] = random.choice(["E104", "E221", "E305", "E410"])

    return payload


def produce_events(count: int = 200, sleep_seconds: float = 0.2) -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    for index in range(count):
        event_type = random.choice(EVENT_TYPES)
        user_id = f"player-{random.randint(1000, 9999)}"
        event = build_event(user_id, event_type)
        producer.send(TOPIC, value=event)
        print(f"Produced {index + 1}/{count}: {event['type']} | {event.get('region')}")
        time.sleep(sleep_seconds)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate sample gaming events into Kafka")
    parser.add_argument("--count", type=int, default=200, help="Number of events to emit")
    parser.add_argument("--sleep", type=float, default=0.2, help="Seconds between emitted events")
    args = parser.parse_args()
    produce_events(count=args.count, sleep_seconds=args.sleep)
