from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from kafka import KafkaProducer

from src.config import BOOTSTRAP_SERVERS
from src.preprocessing import GamingEvent, topic_for_event


def publish_sample(path: Path, bootstrap_servers: list[str] | None = None) -> int:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers or BOOTSTRAP_SERVERS,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
    )
    published = 0
    try:
        with path.open(encoding="utf-8") as sample_file:
            for line_number, line in enumerate(sample_file, start=1):
                if not line.strip():
                    continue
                try:
                    payload: Any = json.loads(line)
                    event = GamingEvent.model_validate(payload)
                except (json.JSONDecodeError, TypeError, ValueError) as error:
                    raise ValueError(f"invalid sample event on line {line_number}: {error}") from error
                producer.send(
                    topic_for_event(event),
                    key=event.user_id,
                    value=event.model_dump(mode="json"),
                ).get(timeout=30)
                published += 1
        producer.flush(timeout=30)
    finally:
        producer.close(timeout=30)
    return published


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publish the bundled sample gaming events to Kafka")
    parser.add_argument("--path", type=Path, default=Path("data/sample_events.jsonl"))
    args = parser.parse_args()
    print(f"Published {publish_sample(args.path)} sample events.")
