from __future__ import annotations

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any

from kafka import KafkaProducer

from .config import BOOTSTRAP_SERVERS, EVENT_RATE, SIMULATED_PLAYERS, TOPIC_PIPELINE_METRICS
from .preprocessing import EventType, TOPIC_BY_EVENT

logger = logging.getLogger(__name__)
EVENT_TYPES: tuple[EventType, ...] = (
    "player_register",
    "player_login",
    "player_logout",
    "session_start",
    "session_end",
    "score",
    "purchase",
    "achievement",
    "matchmaking_request",
    "match_found",
    "error",
)
GAMES = ("arena-legends", "sky-racers", "dungeon-realms", "pixel-football")
REGIONS = ("us-east", "europe-west", "asia-south", "sa-east")


class GamingEventGenerator:
    def __init__(self, *, player_count: int = SIMULATED_PLAYERS, seed: int | None = None) -> None:
        if player_count < 1:
            raise ValueError("player_count must be at least one")
        self.random = random.Random(seed)
        self.players = [f"player-{index:05d}" for index in range(1, player_count + 1)]
        self.registered_players: set[str] = set()
        self.online_players: set[str] = set()
        self.active_sessions: dict[str, str] = {}

    def build_event(self) -> dict[str, Any]:
        event_type: EventType = self.random.choice(EVENT_TYPES)
        user_id = self.random.choice(self.players)
        session_id: str | None = None

        if user_id not in self.registered_players:
            event_type = "player_register"
            self.registered_players.add(user_id)
        elif event_type == "player_login":
            self.online_players.add(user_id)
        elif event_type == "player_logout":
            if user_id not in self.online_players:
                event_type = "player_login"
                self.online_players.add(user_id)
            else:
                self.online_players.remove(user_id)
        elif event_type == "session_start":
            if user_id in self.active_sessions:
                event_type = "session_end"
                session_id = self.active_sessions.pop(user_id)
            else:
                session_id = str(uuid.uuid4())
                self.active_sessions[user_id] = session_id
        elif event_type == "session_end":
            if user_id not in self.active_sessions:
                event_type = "session_start"
                session_id = str(uuid.uuid4())
                self.active_sessions[user_id] = session_id
            else:
                session_id = self.active_sessions.pop(user_id)

        now = datetime.now(timezone.utc)
        event: dict[str, Any] = {
            "event_id": str(uuid.uuid4()),
            "schema_version": 1,
            "timestamp": now.isoformat(),
            "type": event_type,
            "user_id": user_id,
            "region": self.random.choice(REGIONS),
            "game_id": self.random.choice(GAMES),
        }

        if event_type == "score":
            event["score"] = self.random.randint(0, 1000)
        elif event_type == "purchase":
            event["amount"] = round(self.random.uniform(0.99, 99.99), 2)
        elif event_type == "achievement":
            event["achievement_name"] = self.random.choice(
                ("first-win", "streak-5", "explorer", "team-player")
            )
        elif event_type == "session_start":
            event["session_id"] = session_id
        elif event_type == "session_end":
            event["session_id"] = session_id
            event["duration_seconds"] = self.random.randint(60, 7200)
        elif event_type == "match_found":
            event["requested_at"] = (now - timedelta(seconds=self.random.uniform(0.1, 8))).isoformat()
            event["matched_at"] = now.isoformat()
        elif event_type == "error":
            event["error_code"] = self.random.choice(("E104", "E221", "E305", "E410"))

        return event


def produce_events(
    *,
    count: int | None = None,
    events_per_second: float = EVENT_RATE,
    sleep_seconds: float | None = None,
    player_count: int = SIMULATED_PLAYERS,
    seed: int | None = None,
    bootstrap_servers: list[str] | None = None,
) -> None:
    if count is not None and count < 0:
        raise ValueError("count must be non-negative")
    if events_per_second < 0 or (sleep_seconds is not None and sleep_seconds < 0):
        raise ValueError("event rate and sleep interval must be non-negative")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers or BOOTSTRAP_SERVERS,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=5,
        batch_size=32768,
        compression_type="gzip",
        max_in_flight_requests_per_connection=1,
    )
    generator = GamingEventGenerator(player_count=player_count, seed=seed)
    interval = sleep_seconds if sleep_seconds is not None else (1 / events_per_second if events_per_second else 0)
    queued_total = 0
    acknowledged_total = 0
    failed_total = 0
    acknowledgement_lock = Lock()

    def on_acknowledged(_: Any) -> None:
        nonlocal acknowledged_total
        with acknowledgement_lock:
            acknowledged_total += 1

    def on_failed(error: BaseException) -> None:
        nonlocal failed_total
        with acknowledgement_lock:
            failed_total += 1
        logger.error("Kafka producer record failed: %s", error)

    def emit_heartbeat(total: int, rate: float) -> None:
        producer.send(
            TOPIC_PIPELINE_METRICS,
            key="producer",
            value={
                "component": "producer",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "events_per_second": rate,
                "total_events": total,
            },
        ).get(timeout=10)

    heartbeat_total = 0
    heartbeat_started = time.monotonic()
    logger.info("Starting event producer rate=%s events/s players=%d", events_per_second, player_count)

    try:
        while count is None or queued_total < count:
            event = generator.build_event()
            topic = TOPIC_BY_EVENT[event["type"]]
            future = producer.send(topic, key=event["user_id"], value=event)
            future.add_callback(on_acknowledged)
            future.add_errback(on_failed)
            queued_total += 1

            elapsed = time.monotonic() - heartbeat_started
            if elapsed >= 5:
                producer.flush(timeout=10)
                with acknowledgement_lock:
                    acknowledged = acknowledged_total
                    failures = failed_total
                interval_count = acknowledged - heartbeat_total
                emit_heartbeat(acknowledged, interval_count / elapsed)
                heartbeat_total = acknowledged
                heartbeat_started = time.monotonic()
                logger.info("Acknowledged events=%d rate=%.2f events/s", acknowledged, interval_count / elapsed)
                if failures:
                    raise RuntimeError(f"{failures} Kafka event(s) failed to publish")

            if interval:
                time.sleep(interval)

        producer.flush(timeout=30)
        with acknowledgement_lock:
            acknowledged = acknowledged_total
            failures = failed_total
        elapsed = time.monotonic() - heartbeat_started
        if acknowledged > heartbeat_total:
            emit_heartbeat(acknowledged, (acknowledged - heartbeat_total) / max(elapsed, 1e-9))
        if failures:
            raise RuntimeError(f"{failures} Kafka event(s) failed to publish")
    finally:
        producer.close(timeout=30)

    logger.info("Producer completed successfully: %d event(s)", acknowledged_total)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate continuous, realistic gaming events for Kafka")
    parser.add_argument("--count", type=int, default=None, help="Event count; omit for continuous generation")
    parser.add_argument("--rate", type=float, default=EVENT_RATE, help="Target events per second (zero means unthrottled)")
    parser.add_argument("--sleep", type=float, default=None, help="Optional fixed delay between events")
    parser.add_argument("--players", type=int, default=SIMULATED_PLAYERS)
    parser.add_argument("--seed", type=int, default=None, help="Seed event selection for reproducibility")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(message)s")
    produce_events(
        count=args.count,
        events_per_second=args.rate,
        sleep_seconds=args.sleep,
        player_count=args.players,
        seed=args.seed,
    )
