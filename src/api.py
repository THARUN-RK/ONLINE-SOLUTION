from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from sqlalchemy import func, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from .config import (
    ACTIVE_WINDOW_MINUTES,
    BOOTSTRAP_SERVERS,
    CONSUMER_GROUP_ID,
    DATABASE_URL,
    TOPIC_ACHIEVEMENTS,
    TOPIC_GAME_SCORES,
    TOPIC_GAME_SESSIONS,
    TOPIC_PLAYER_EVENTS,
    TOPIC_TRANSACTIONS,
)
from .storage import (
    AchievementRecord,
    GameSession,
    GamingEventRecord,
    PipelineMetric,
    Player,
    TransactionRecord,
    create_schema,
    make_engine,
)

EVENT_TOPICS = (
    TOPIC_PLAYER_EVENTS,
    TOPIC_GAME_SCORES,
    TOPIC_TRANSACTIONS,
    TOPIC_GAME_SESSIONS,
    TOPIC_ACHIEVEMENTS,
)


def _event_rows(engine: Engine, since: datetime, limit: int = 20000) -> list[dict[str, Any]]:
    with Session(engine) as session:
        rows = session.execute(
            select(
                GamingEventRecord.event_id,
                GamingEventRecord.event_type,
                GamingEventRecord.event_timestamp,
                GamingEventRecord.processed_at,
                GamingEventRecord.player_id,
                GamingEventRecord.game_id,
                GamingEventRecord.score,
                GamingEventRecord.amount,
                GamingEventRecord.processing_latency_ms,
            )
            .where(GamingEventRecord.event_timestamp >= since)
            .order_by(GamingEventRecord.event_timestamp.desc())
            .limit(limit)
        ).all()
    return [dict(row._mapping) for row in rows]


def _to_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame["event_timestamp"] = pd.to_datetime(frame["event_timestamp"], utc=True)
    return frame


def _processing_latencies(engine: Engine, since: datetime, limit: int = 20000) -> list[float]:
    with Session(engine) as session:
        rows = session.scalars(
            select(GamingEventRecord.processing_latency_ms)
            .where(GamingEventRecord.processed_at >= since)
            .order_by(GamingEventRecord.processed_at.desc())
            .limit(limit)
        ).all()
    return [float(value) for value in rows]


def _kafka_group_lag() -> dict[str, Any]:
    consumer: KafkaConsumer | None = None
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP_ID,
            enable_auto_commit=False,
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
        )
        partitions: list[TopicPartition] = []
        for topic in EVENT_TOPICS:
            topic_partitions = consumer.partitions_for_topic(topic)
            if topic_partitions:
                partitions.extend(TopicPartition(topic, partition) for partition in topic_partitions)
        if not partitions:
            return {"status": "connected", "consumer_group_lag": 0, "partitions": 0}

        consumer.assign(partitions)
        end_offsets = consumer.end_offsets(partitions, timeout_ms=5000)
        total_lag = 0
        for partition, end_offset in end_offsets.items():
            committed_offset = consumer.committed(partition)
            total_lag += max(0, end_offset - (committed_offset or 0))
        return {
            "status": "connected",
            "consumer_group_lag": total_lag,
            "partitions": len(partitions),
        }
    except KafkaError as error:
        return {
            "status": "unavailable",
            "consumer_group_lag": None,
            "partitions": None,
            "error": str(error),
        }
    finally:
        if consumer is not None:
            consumer.close()


def create_app(engine: Engine | None = None) -> FastAPI:
    database_engine = engine or make_engine(DATABASE_URL)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        create_schema(database_engine)
        yield
        if engine is None:
            database_engine.dispose()

    app = FastAPI(
        title="Gaming Stream Analytics API",
        version="1.0.0",
        description="Live analytics and pipeline health for the Kafka gaming-event platform.",
        lifespan=lifespan,
    )
    app.state.database_engine = database_engine

    @app.get("/health")
    def health() -> dict[str, str]:
        try:
            with database_engine.connect() as connection:
                connection.execute(text("SELECT 1"))
        except SQLAlchemyError as error:
            raise HTTPException(status_code=503, detail=f"database unavailable: {error}") from error
        return {"status": "healthy", "database": "connected"}

    @app.get("/api/v1/analytics/summary")
    def summary() -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        active_since = now - timedelta(minutes=ACTIVE_WINDOW_MINUTES)
        event_since = now - timedelta(seconds=60)
        with Session(database_engine) as session:
            return {
                "registered_players": session.scalar(
                    select(func.count()).select_from(Player).where(Player.registered_at.is_not(None))
                )
                or 0,
                "active_players": session.scalar(
                    select(func.count())
                    .select_from(Player)
                    .where(Player.is_online.is_(True), Player.last_active_at >= active_since)
                )
                or 0,
                "total_gaming_events": session.scalar(
                    select(func.count()).select_from(GamingEventRecord)
                )
                or 0,
                "total_game_sessions": session.scalar(select(func.count()).select_from(GameSession)) or 0,
                "average_player_score": round(
                    float(
                        session.scalar(select(func.avg(GamingEventRecord.score))) or 0.0
                    ),
                    2,
                ),
                "total_transactions": session.scalar(
                    select(func.count()).select_from(TransactionRecord)
                )
                or 0,
                "transaction_value": round(
                    float(session.scalar(select(func.coalesce(func.sum(TransactionRecord.amount), 0))) or 0),
                    2,
                ),
                "events_per_second": round(
                    (session.scalar(
                        select(func.count())
                        .select_from(GamingEventRecord)
                        .where(GamingEventRecord.processed_at >= event_since)
                    )
                    or 0)
                    / 60,
                    2,
                ),
                "average_processing_latency_ms": round(
                    float(session.scalar(select(func.avg(GamingEventRecord.processing_latency_ms))) or 0.0),
                    3,
                ),
                "active_window_minutes": ACTIVE_WINDOW_MINUTES,
            }

    @app.get("/api/v1/analytics/activity")
    def activity() -> dict[str, list[dict[str, Any]]]:
        rows = _event_rows(database_engine, datetime.now(timezone.utc) - timedelta(hours=24))
        if not rows:
            return {"player_activity": [], "events_per_second": []}
        frame = _to_frame(rows).set_index("event_timestamp").sort_index()
        player_activity = (
            frame["player_id"]
            .resample("1min")
            .nunique()
            .tail(60)
            .rename("active_players")
            .reset_index()
        )
        events_per_second = (
            frame["event_id"]
            .resample("1s")
            .count()
            .tail(60)
            .rename("events")
            .reset_index()
        )
        return {
            "player_activity": [
                {"timestamp": row.event_timestamp.isoformat(), "active_players": int(row.active_players)}
                for row in player_activity.itertuples(index=False)
            ],
            "events_per_second": [
                {"timestamp": row.event_timestamp.isoformat(), "events": int(row.events)}
                for row in events_per_second.itertuples(index=False)
            ],
        }

    @app.get("/api/v1/analytics/leaderboard")
    def leaderboard() -> list[dict[str, Any]]:
        with Session(database_engine) as session:
            players = session.scalars(
                select(Player).order_by(Player.total_score.desc(), Player.player_id).limit(10)
            ).all()
        return [
            {"player_id": player.player_id, "total_score": round(player.total_score, 2)}
            for player in players
        ]

    @app.get("/api/v1/analytics/games")
    def games() -> list[dict[str, Any]]:
        with Session(database_engine) as session:
            rows = session.execute(
                select(
                    GamingEventRecord.game_id,
                    func.count(GamingEventRecord.event_id).label("events"),
                    func.avg(GamingEventRecord.score).label("average_score"),
                )
                .group_by(GamingEventRecord.game_id)
                .order_by(func.count(GamingEventRecord.event_id).desc())
            ).all()
        return [
            {
                "game_id": row.game_id,
                "events": row.events,
                "average_score": round(float(row.average_score or 0.0), 2),
            }
            for row in rows
        ]

    @app.get("/api/v1/analytics/engagement")
    def engagement() -> list[dict[str, Any]]:
        rows = _event_rows(database_engine, datetime.now(timezone.utc) - timedelta(days=7))
        if not rows:
            return []
        frame = _to_frame(rows)
        frame["day"] = frame["event_timestamp"].dt.floor("D")
        daily = frame.groupby("day").agg(events=("event_id", "count"), players=("player_id", "nunique"))
        daily["events_per_player"] = daily["events"] / daily["players"].clip(lower=1)
        return [
            {
                "date": timestamp.isoformat(),
                "events": int(row.events),
                "active_players": int(row.players),
                "events_per_player": round(float(row.events_per_player), 2),
            }
            for timestamp, row in daily.iterrows()
        ]

    @app.get("/api/v1/analytics/trends")
    def trends(days: int = Query(default=30, ge=1, le=30)) -> dict[str, list[dict[str, Any]]]:
        rows = _event_rows(database_engine, datetime.now(timezone.utc) - timedelta(days=days))
        if not rows:
            return {"daily": [], "weekly": []}
        frame = _to_frame(rows)
        frame["day"] = frame["event_timestamp"].dt.floor("D")
        daily = frame.groupby("day").agg(
            events=("event_id", "count"),
            active_players=("player_id", "nunique"),
        )
        frame["week"] = (
            frame["event_timestamp"]
            .dt.tz_localize(None)
            .dt.to_period("W")
            .dt.start_time
            .dt.tz_localize("UTC")
        )
        weekly = frame.groupby("week").agg(events=("event_id", "count"))
        return {
            "daily": [
                {
                    "date": timestamp.isoformat(),
                    "events": int(row.events),
                    "active_players": int(row.active_players),
                }
                for timestamp, row in daily.iterrows()
            ],
            "weekly": [
                {"week": timestamp.isoformat(), "events": int(row.events)}
                for timestamp, row in weekly.iterrows()
            ],
        }

    @app.get("/api/v1/analytics/transactions")
    def transactions() -> list[dict[str, Any]]:
        since = datetime.now(timezone.utc) - timedelta(days=30)
        with Session(database_engine) as session:
            rows = session.execute(
                select(TransactionRecord.occurred_at, TransactionRecord.amount)
                .where(TransactionRecord.occurred_at >= since)
                .order_by(TransactionRecord.occurred_at.desc())
                .limit(20000)
            ).all()
        if not rows:
            return []
        frame = pd.DataFrame(
            [{"occurred_at": row.occurred_at, "amount": float(row.amount)} for row in rows]
        )
        frame["day"] = pd.to_datetime(frame["occurred_at"], utc=True).dt.floor("D")
        daily = frame.groupby("day").agg(transactions=("amount", "count"), amount=("amount", "sum"))
        return [
            {
                "date": timestamp.isoformat(),
                "transactions": int(row.transactions),
                "amount": round(float(row.amount), 2),
            }
            for timestamp, row in daily.iterrows()
        ]

    @app.get("/api/v1/analytics/sessions")
    def sessions() -> dict[str, Any]:
        with Session(database_engine) as session:
            records = session.execute(
                select(GameSession.started_at, GameSession.duration_seconds)
                .order_by(GameSession.started_at.desc())
                .limit(5000)
            ).all()
        durations = [int(row.duration_seconds) for row in records if row.duration_seconds is not None]
        if not durations:
            return {"total_sessions": len(records), "average_duration_seconds": 0.0, "duration_distribution": []}
        distribution = pd.Series(durations, name="duration_seconds").value_counts(bins=8, sort=False)
        return {
            "total_sessions": len(records),
            "average_duration_seconds": round(float(np.mean(durations)), 2),
            "duration_distribution": [
                {"duration_range": str(interval), "sessions": int(count)}
                for interval, count in distribution.items()
            ],
        }

    @app.get("/api/v1/analytics/performance")
    def performance() -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        latencies = _processing_latencies(database_engine, now - timedelta(hours=1))
        with Session(database_engine) as session:
            recent_count = session.scalar(
                select(func.count())
                .select_from(GamingEventRecord)
                .where(GamingEventRecord.processed_at >= now - timedelta(seconds=60))
            ) or 0
        history = _latest_metrics(database_engine, "consumer", 60)
        return {
            "events_per_second": round(recent_count / 60, 2),
            "average_processing_latency_ms": round(float(np.mean(latencies)), 3) if latencies else 0.0,
            "p95_processing_latency_ms": round(float(np.percentile(latencies, 95)), 3) if latencies else 0.0,
            "measured_events": len(latencies),
            "invalid_events_last_100_batches": sum(
                metric.invalid_events for metric in _latest_metrics(database_engine, "consumer", 100)
            ),
            "duplicates_last_100_batches": sum(
                metric.duplicate_events for metric in _latest_metrics(database_engine, "consumer", 100)
            ),
            "quality_metrics_batch_window": 100,
            "history": [
                {
                    "timestamp": metric.recorded_at.isoformat(),
                    "events_per_second": metric.events_per_second,
                    "batch_latency_ms": metric.batch_latency_ms,
                }
                for metric in reversed(history)
            ],
        }

    @app.get("/api/v1/monitoring")
    def monitoring() -> dict[str, Any]:
        return {
            "producer": _latest_metric(database_engine, "producer"),
            "consumer": _latest_metric(database_engine, "consumer"),
            "kafka": _kafka_group_lag(),
            "rate_note": "Producer rate is reported by producer heartbeats; consumer rate is the latest committed batch.",
        }

    @app.get("/api/v1/analytics/achievements")
    def achievements() -> list[dict[str, Any]]:
        with Session(database_engine) as session:
            rows = session.execute(
                select(
                    AchievementRecord.achievement_name,
                    func.count(AchievementRecord.event_id).label("awards"),
                )
                .group_by(AchievementRecord.achievement_name)
                .order_by(func.count(AchievementRecord.event_id).desc())
            ).all()
        return [{"achievement": row.achievement_name, "awards": row.awards} for row in rows]

    return app


def _latest_metrics(engine: Engine, component: str, limit: int) -> list[PipelineMetric]:
    with Session(engine) as session:
        return list(
            session.scalars(
                select(PipelineMetric)
                .where(PipelineMetric.component == component)
                .order_by(PipelineMetric.recorded_at.desc())
                .limit(limit)
            ).all()
        )


def _latest_metric(engine: Engine, component: str) -> dict[str, Any] | None:
    metrics = _latest_metrics(engine, component, 1)
    if not metrics:
        return None
    metric = metrics[0]
    return {
        "recorded_at": metric.recorded_at.isoformat(),
        "events_per_second": round(metric.events_per_second, 2),
        "total_events": metric.total_events,
        "invalid_events": metric.invalid_events,
        "duplicate_events": metric.duplicate_events,
        "batch_latency_ms": round(metric.batch_latency_ms, 3),
    }


app = create_app()
