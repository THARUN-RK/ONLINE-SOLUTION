import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from src.api import create_app
from src.config import (
    TOPIC_GAME_SESSIONS,
    TOPIC_GAME_SCORES,
    TOPIC_TRANSACTIONS,
)
from src.consumer import process_batch
from src.producer import GamingEventGenerator, produce_events
from src.preprocessing import preprocess_batch
from src.storage import (
    GameSession,
    GamingEventRecord,
    Player,
    TransactionRecord,
    create_schema,
)


def _event(event_type: str, **fields: object) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    event: dict[str, object] = {
        "event_id": f"event-{event_type}-{now.timestamp()}",
        "type": event_type,
        "timestamp": now.isoformat(),
        "user_id": "Player-1",
        "game_id": "Arena-Legends",
        "region": "US-EAST",
    }
    event.update(fields)
    return event


def _database():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    create_schema(engine)
    return engine


def test_preprocessing_normalizes_and_deduplicates_events():
    raw_event = _event("score", event_id="same-id", score=12)
    result = preprocess_batch([raw_event, dict(raw_event)])

    assert len(result.events) == 1
    assert result.events[0].user_id == "player-1"
    assert result.events[0].game_id == "arena-legends"
    assert result.events[0].region == "us-east"
    assert result.duplicates == 1
    assert result.invalid == []


def test_preprocessing_derives_stable_ids_and_rejects_bad_records():
    missing_id = _event("purchase", amount=10.25)
    missing_id.pop("event_id")
    invalid_score = _event("score")
    result = preprocess_batch([missing_id, dict(missing_id), invalid_score, {"type": "score"}])

    assert len(result.events) == 1
    assert len(result.events[0].event_id) == 64
    assert result.duplicates == 1
    assert len(result.invalid) == 2


def test_preprocessing_rejects_timestamps_without_timezone_offsets():
    event = _event("score", score=5, timestamp="2026-01-01T00:00:00")
    result = preprocess_batch([event])

    assert result.events == []
    assert len(result.invalid) == 1


def test_invalid_duplicate_does_not_suppress_a_valid_event_with_the_same_id():
    invalid = _event("score", event_id="shared-id")
    valid = _event("score", event_id="shared-id", score=99)

    result = preprocess_batch([invalid, valid])

    assert len(result.events) == 1
    assert result.events[0].score == 99
    assert len(result.invalid) == 1
    assert result.duplicates == 0


def test_consumer_persists_events_deduplicates_and_updates_relational_tables():
    engine = _database()
    start = _event("session_start", event_id="session-start", session_id="session-1")
    score = _event("score", event_id="score-1", score=42)
    purchase = _event("purchase", event_id="purchase-1", amount="12.50")
    end = _event(
        "session_end",
        event_id="session-end",
        session_id="session-1",
        timestamp=(datetime.now(timezone.utc) + timedelta(seconds=3)).isoformat(),
        duration_seconds=3,
    )
    records = [
        SimpleNamespace(topic=TOPIC_GAME_SESSIONS, value=json.dumps(start).encode()),
        SimpleNamespace(topic=TOPIC_GAME_SCORES, value=json.dumps(score).encode()),
        SimpleNamespace(topic=TOPIC_TRANSACTIONS, value=json.dumps(purchase).encode()),
        SimpleNamespace(topic=TOPIC_GAME_SESSIONS, value=json.dumps(end).encode()),
        SimpleNamespace(topic=TOPIC_GAME_SCORES, value=json.dumps(score).encode()),
        SimpleNamespace(topic=TOPIC_GAME_SCORES, value=json.dumps(_event("score")).encode()),
    ]

    result = process_batch(engine, records)

    assert result.stored == 4
    assert result.duplicates == 1
    assert result.invalid == 1

    redelivery = process_batch(engine, records[1:2])
    assert redelivery.stored == 0
    assert redelivery.duplicates == 1

    from sqlalchemy.orm import Session

    with Session(engine) as session:
        assert session.query(GamingEventRecord).count() == 4
        assert session.query(Player).one().total_score == 42
        assert session.query(TransactionRecord).one().amount == 12.50
        stored_session = session.query(GameSession).one()
        assert stored_session.ended_at is not None
        assert stored_session.duration_seconds == 3


def test_consumer_routes_bad_json_and_topic_mismatches_to_dead_letter():
    class CapturingProducer:
        def __init__(self):
            self.messages = []
            self.acknowledged = 0
            self.flushed = False

        def send(self, topic, *, key, value):
            self.messages.append((topic, key, value))
            return SimpleNamespace(get=lambda timeout: self._acknowledge())

        def _acknowledge(self):
            self.acknowledged += 1

        def flush(self, timeout):
            assert self.acknowledged == len(self.messages)
            self.flushed = True

    engine = _database()
    producer = CapturingProducer()
    misplaced_score = _event("score", event_id="misplaced-score", score=12)
    records = [
        SimpleNamespace(topic=TOPIC_TRANSACTIONS, value=json.dumps(misplaced_score).encode()),
        SimpleNamespace(topic=TOPIC_GAME_SCORES, value=b"{not-json"),
        SimpleNamespace(topic=TOPIC_GAME_SCORES, value=None),
    ]

    result = process_batch(engine, records, producer)

    assert result.stored == 0
    assert result.invalid == 3
    assert len(producer.messages) == 3
    assert all(topic == "gaming-events.dlq" for topic, _, _ in producer.messages)
    assert producer.flushed


def test_simulator_registers_a_player_before_emitting_gameplay():
    event = GamingEventGenerator(player_count=1, seed=7).build_event()

    assert event["type"] == "player_register"


def test_producer_reports_broker_acknowledged_counts(monkeypatch):
    class FakeFuture:
        def add_callback(self, callback):
            callback(None)
            return self

        def add_errback(self, callback):
            return self

        def get(self, timeout):
            return None

    class FakeProducer:
        def __init__(self, **kwargs):
            self.config = kwargs
            self.messages = []
            self.closed = False

        def send(self, topic, *, key, value):
            self.messages.append((topic, key, value))
            return FakeFuture()

        def flush(self, timeout):
            return None

        def close(self, timeout):
            self.closed = True

    fake_producer = FakeProducer()

    def create_fake_producer(**kwargs):
        fake_producer.config = kwargs
        return fake_producer

    monkeypatch.setattr("src.producer.KafkaProducer", create_fake_producer)

    produce_events(count=4, events_per_second=0, player_count=1, seed=7)

    game_events = [message for message in fake_producer.messages if message[0] != "pipeline-metrics"]
    heartbeats = [message for message in fake_producer.messages if message[0] == "pipeline-metrics"]
    assert len(game_events) == 4
    assert heartbeats[0][2]["total_events"] == 4
    assert fake_producer.config["acks"] == "all"
    assert fake_producer.config["max_in_flight_requests_per_connection"] == 1
    assert fake_producer.closed


def test_api_serves_required_analytics_from_processed_events(monkeypatch):
    engine = _database()
    start_time = datetime.now(timezone.utc) - timedelta(seconds=120)
    payloads = [
        (_event("player_register", event_id="register-1"), "player-events"),
        (_event("player_login", event_id="login-1"), "player-events"),
        (_event("score", event_id="score-1", score=25), TOPIC_GAME_SCORES),
        (_event("purchase", event_id="purchase-1", amount="5.00"), TOPIC_TRANSACTIONS),
        (
            _event("session_start", event_id="session-start", session_id="session-1"),
            TOPIC_GAME_SESSIONS,
        ),
        (
            _event(
                "session_end",
                event_id="session-end",
                session_id="session-1",
                timestamp=(start_time + timedelta(seconds=120)).isoformat(),
                duration_seconds=120,
            ),
            TOPIC_GAME_SESSIONS,
        ),
    ]
    records = [
        SimpleNamespace(topic=topic, value=json.dumps(payload).encode())
        for payload, topic in payloads
    ]
    process_batch(engine, records)
    app = create_app(engine)

    with TestClient(app) as client:
        summary = client.get("/api/v1/analytics/summary")
        leaderboard = client.get("/api/v1/analytics/leaderboard")
        games = client.get("/api/v1/analytics/games")
        transactions = client.get("/api/v1/analytics/transactions")
        engagement = client.get("/api/v1/analytics/engagement")
        achievements = client.get("/api/v1/analytics/achievements")
        sessions = client.get("/api/v1/analytics/sessions")
        activity = client.get("/api/v1/analytics/activity")
        trends = client.get("/api/v1/analytics/trends")
        performance = client.get("/api/v1/analytics/performance")
        monkeypatch.setattr("src.api._kafka_group_lag", lambda: {"status": "connected"})
        monitoring = client.get("/api/v1/monitoring")
        health = client.get("/health")

    assert summary.status_code == 200
    assert summary.json()["registered_players"] == 1
    assert summary.json()["total_gaming_events"] == 6
    assert summary.json()["active_players"] == 1
    assert summary.json()["total_transactions"] == 1
    assert leaderboard.json()[0]["total_score"] == 25
    assert games.json()[0]["game_id"] == "arena-legends"
    assert transactions.status_code == 200
    assert engagement.status_code == 200
    assert achievements.status_code == 200
    assert sessions.json()["total_sessions"] == 1
    assert activity.status_code == 200
    assert trends.status_code == 200
    assert performance.json()["measured_events"] == 6
    assert monitoring.json()["kafka"]["status"] == "connected"
    assert health.json()["database"] == "connected"
