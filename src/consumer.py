from __future__ import annotations

import argparse
import json
import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Sequence

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from sqlalchemy.engine import Engine

from .config import (
    BOOTSTRAP_SERVERS,
    CONSUMER_GROUP_ID,
    DATABASE_URL,
    TOPIC_ACHIEVEMENTS,
    TOPIC_DEAD_LETTER,
    TOPIC_GAME_SCORES,
    TOPIC_GAME_SESSIONS,
    TOPIC_PIPELINE_METRICS,
    TOPIC_PLAYER_EVENTS,
    TOPIC_TRANSACTIONS,
)
from .preprocessing import (
    BatchResult,
    InvalidEvent,
    GamingEvent,
    preprocess_batch,
    stable_event_id,
    topic_for_event,
)
from .storage import create_schema, make_engine, store_event, store_pipeline_metric

logger = logging.getLogger(__name__)
EVENT_TOPICS = (
    TOPIC_PLAYER_EVENTS,
    TOPIC_GAME_SCORES,
    TOPIC_TRANSACTIONS,
    TOPIC_GAME_SESSIONS,
    TOPIC_ACHIEVEMENTS,
    TOPIC_PIPELINE_METRICS,
)


@dataclass(frozen=True)
class ProcessBatchStats:
    stored: int = 0
    duplicates: int = 0
    invalid: int = 0


def _send_dead_letter(producer: KafkaProducer | None, invalid: InvalidEvent) -> None:
    if producer is None:
        return
    raw = invalid.raw
    producer.send(
        TOPIC_DEAD_LETTER,
        key=b"invalid-event",
        value={
            "rejected_at": datetime.now(timezone.utc).isoformat(),
            "reason": invalid.reason,
            "event": raw,
        },
    ).get(timeout=10)


def process_batch(
    engine: Engine,
    records: Sequence[Any],
    dead_letter_producer: KafkaProducer | None = None,
) -> ProcessBatchStats:
    game_event_payloads: list[dict[str, Any]] = []
    payload_topics: dict[str, str] = {}
    invalid_events: list[InvalidEvent] = []
    producer_heartbeats: list[dict[str, Any]] = []

    for record in records:
        if record.value is None:
            invalid_events.append(
                InvalidEvent(raw=None, reason="Kafka message value must not be null")
            )
            continue
        try:
            raw = json.loads(record.value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            invalid_events.append(
                InvalidEvent(
                    raw={"raw_value": record.value.decode("utf-8", errors="replace")},
                    reason=f"invalid JSON payload: {error}",
                )
            )
            continue

        if record.topic == TOPIC_PIPELINE_METRICS:
            if isinstance(raw, dict) and raw.get("component") == "producer":
                try:
                    heartbeat = {
                        "timestamp": datetime.fromisoformat(raw["timestamp"]),
                        "events_per_second": float(raw["events_per_second"]),
                        "total_events": int(raw["total_events"]),
                    }
                    if heartbeat["timestamp"].utcoffset() is None:
                        raise ValueError("heartbeat timestamp must include a timezone offset")
                    if not math.isfinite(heartbeat["events_per_second"]) or heartbeat["events_per_second"] < 0:
                        raise ValueError("heartbeat rate must be a finite non-negative number")
                    if heartbeat["total_events"] < 0:
                        raise ValueError("heartbeat total_events must be non-negative")
                    producer_heartbeats.append(heartbeat)
                except (KeyError, TypeError, ValueError) as error:
                    invalid_events.append(
                        InvalidEvent(raw=raw, reason=f"invalid producer heartbeat: {error}")
                    )
            else:
                invalid_events.append(
                    InvalidEvent(raw=raw, reason="unsupported pipeline metric message")
                )
            continue

        if not isinstance(raw, dict):
            game_event_payloads.append(raw)
            continue
        raw_event_id = str(raw.get("event_id") or "").strip()
        if raw_event_id:
            payload_topics[raw_event_id.lower()] = record.topic
        else:
            payload_topics[stable_event_id(raw)] = record.topic
        game_event_payloads.append(raw)

    batch: BatchResult = preprocess_batch(game_event_payloads)
    invalid_events.extend(batch.invalid)

    stored = 0
    persisted_duplicates = 0
    for event in batch.events:
        actual_topic = payload_topics.get(event.event_id.lower())
        expected_topic = topic_for_event(event)
        if actual_topic is not None and actual_topic != expected_topic:
            invalid_events.append(
                InvalidEvent(
                    raw=event.model_dump(mode="json"),
                    reason=f"event type {event.type!r} belongs on topic {expected_topic!r}, not {actual_topic!r}",
                )
            )
            continue
        if store_event(engine, event, expected_topic):
            stored += 1
        else:
            persisted_duplicates += 1

    for heartbeat in producer_heartbeats:
        store_pipeline_metric(
            engine,
            component="producer",
            events_per_second=heartbeat["events_per_second"],
            total_events=heartbeat["total_events"],
            recorded_at=heartbeat["timestamp"],
        )

    for invalid in invalid_events:
        _send_dead_letter(dead_letter_producer, invalid)
    if invalid_events and dead_letter_producer is not None:
        dead_letter_producer.flush(timeout=10)

    return ProcessBatchStats(
        stored=stored,
        duplicates=batch.duplicates + persisted_duplicates,
        invalid=len(invalid_events),
    )


def consume_events(
    *,
    bootstrap_servers: list[str] | None = None,
    database_url: str = DATABASE_URL,
    stop_after_messages: int | None = None,
) -> None:
    engine = make_engine(database_url)
    create_schema(engine)
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers or BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
        max_poll_records=500,
    )
    consumer.subscribe(EVENT_TOPICS)
    dead_letter_producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers or BOOTSTRAP_SERVERS,
        acks="all",
        retries=5,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    processed_messages = 0
    try:
        while stop_after_messages is None or processed_messages < stop_after_messages:
            polled = consumer.poll(timeout_ms=1000, max_records=500)
            records = [record for partition_records in polled.values() for record in partition_records]
            if not records:
                continue

            started = time.perf_counter()
            stats = process_batch(engine, records, dead_letter_producer)
            consumer.commit()
            elapsed_ms = (time.perf_counter() - started) * 1000
            processed_messages += len(records)

            store_pipeline_metric(
                engine,
                component="consumer",
                events_per_second=stats.stored / max(elapsed_ms / 1000, 0.001),
                total_events=stats.stored,
                invalid_events=stats.invalid,
                duplicate_events=stats.duplicates,
                batch_latency_ms=elapsed_ms,
            )
            logger.info(
                "Committed batch messages=%d stored=%d duplicates=%d invalid=%d latency_ms=%.2f",
                len(records),
                stats.stored,
                stats.duplicates,
                stats.invalid,
                elapsed_ms,
            )
    except KafkaError:
        logger.exception("Kafka consumer failed")
        raise
    finally:
        dead_letter_producer.close()
        consumer.close()
        engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate, deduplicate, and store Kafka gaming events")
    parser.add_argument("--max-messages", type=int, default=None, help="Stop after consuming this many Kafka records")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(message)s")
    consume_events(stop_after_messages=args.max_messages)
