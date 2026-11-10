from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from time import perf_counter
from typing import Any

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    JSON,
    Numeric,
    String,
    create_engine,
    select,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from sqlalchemy.exc import IntegrityError

from .config import DATABASE_URL
from .preprocessing import GamingEvent


class Base(DeclarativeBase):
    pass


class Player(Base):
    __tablename__ = "players"

    player_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_active_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    registered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    is_online: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    events_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)


class GamingEventRecord(Base):
    __tablename__ = "gaming_events"

    event_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(40), nullable=False)
    topic: Mapped[str] = mapped_column(String(128), nullable=False)
    event_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    player_id: Mapped[str] = mapped_column(ForeignKey("players.player_id"), nullable=False)
    region: Mapped[str] = mapped_column(String(64), nullable=False)
    game_id: Mapped[str] = mapped_column(String(128), nullable=False)
    score: Mapped[float | None] = mapped_column(Float)
    amount: Mapped[Decimal | None] = mapped_column(Numeric(14, 2))
    duration_seconds: Mapped[int | None] = mapped_column(Integer)
    session_id: Mapped[str | None] = mapped_column(String(128))
    processing_latency_ms: Mapped[float] = mapped_column(Float, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


Index("ix_gaming_events_processed_at", GamingEventRecord.processed_at)
Index("ix_gaming_events_event_type", GamingEventRecord.event_type)
Index("ix_gaming_events_game_id", GamingEventRecord.game_id)


class GameSession(Base):
    __tablename__ = "game_sessions"

    session_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    player_id: Mapped[str] = mapped_column(ForeignKey("players.player_id"), nullable=False)
    game_id: Mapped[str] = mapped_column(String(128), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    duration_seconds: Mapped[int | None] = mapped_column(Integer)


class TransactionRecord(Base):
    __tablename__ = "transactions"

    event_id: Mapped[str] = mapped_column(ForeignKey("gaming_events.event_id"), primary_key=True)
    player_id: Mapped[str] = mapped_column(ForeignKey("players.player_id"), nullable=False)
    game_id: Mapped[str] = mapped_column(String(128), nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AchievementRecord(Base):
    __tablename__ = "player_achievements"

    event_id: Mapped[str] = mapped_column(ForeignKey("gaming_events.event_id"), primary_key=True)
    player_id: Mapped[str] = mapped_column(ForeignKey("players.player_id"), nullable=False)
    game_id: Mapped[str] = mapped_column(String(128), nullable=False)
    achievement_name: Mapped[str] = mapped_column(String(128), nullable=False)
    achieved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class PipelineMetric(Base):
    __tablename__ = "pipeline_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    component: Mapped[str] = mapped_column(String(32), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    events_per_second: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    total_events: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    invalid_events: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    duplicate_events: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    batch_latency_ms: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)


Index("ix_pipeline_metrics_component_time", PipelineMetric.component, PipelineMetric.recorded_at)


def make_engine(database_url: str = DATABASE_URL) -> Engine:
    return create_engine(database_url, pool_pre_ping=True, future=True)


def create_schema(engine: Engine) -> None:
    Base.metadata.create_all(engine)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def store_event(engine: Engine, event: GamingEvent, topic: str) -> bool:
    started = perf_counter()
    now = datetime.now(timezone.utc)
    event_time = _as_utc(event.timestamp)
    payload = event.model_dump(mode="json")

    try:
        with Session(engine) as session:
            with session.begin():
                if session.get(GamingEventRecord, event.event_id) is not None:
                    return False

                player = session.get(Player, event.user_id)
                if player is None:
                    player = Player(
                        player_id=event.user_id,
                        first_seen_at=event_time,
                        last_seen_at=event_time,
                        last_active_at=event_time,
                        registered_at=event_time if event.type == "player_register" else None,
                        is_online=event.type not in ("player_register", "player_logout"),
                        events_count=0,
                        total_score=0.0,
                    )
                    session.add(player)
                else:
                    first_seen = _as_utc(player.first_seen_at)
                    last_seen = _as_utc(player.last_seen_at)
                    player.first_seen_at = min(first_seen, event_time)
                    player.last_seen_at = max(last_seen, event_time)
                    if event.type != "player_logout":
                        player.last_active_at = max(_as_utc(player.last_active_at), event_time)
                    if event.type == "player_register" and player.registered_at is None:
                        player.registered_at = event_time
                    if event.type == "player_login":
                        player.is_online = True
                    elif event.type in ("player_register", "player_logout"):
                        player.is_online = False

                player.events_count += 1
                if event.score is not None:
                    player.total_score += event.score

                effective_session_id = event.session_id
                if event.type == "session_start" and not effective_session_id:
                    effective_session_id = event.event_id

                record = GamingEventRecord(
                    event_id=event.event_id,
                    event_type=event.type,
                    topic=topic,
                    event_timestamp=event_time,
                    processed_at=now,
                    player_id=event.user_id,
                    region=event.region,
                    game_id=event.game_id,
                    score=event.score,
                    amount=event.amount,
                    duration_seconds=event.duration_seconds,
                    session_id=effective_session_id,
                    processing_latency_ms=0.0,
                    payload=payload,
                )
                session.add(record)
                session.flush()

                if event.type == "purchase" and event.amount is not None:
                    session.add(
                        TransactionRecord(
                            event_id=event.event_id,
                            player_id=event.user_id,
                            game_id=event.game_id,
                            amount=event.amount,
                            occurred_at=event_time,
                        )
                    )
                elif event.type == "achievement" and event.achievement_name:
                    session.add(
                        AchievementRecord(
                            event_id=event.event_id,
                            player_id=event.user_id,
                            game_id=event.game_id,
                            achievement_name=event.achievement_name,
                            achieved_at=event_time,
                        )
                    )
                elif event.type == "session_start":
                    session.add(
                        GameSession(
                            session_id=effective_session_id or event.event_id,
                            player_id=event.user_id,
                            game_id=event.game_id,
                            started_at=event_time,
                        )
                    )
                elif event.type == "session_end":
                    active_session = None
                    if event.session_id:
                        active_session = session.get(GameSession, event.session_id)
                        if active_session is not None and active_session.player_id != event.user_id:
                            active_session = None
                    else:
                        active_session = session.scalar(
                            select(GameSession)
                            .where(
                                GameSession.player_id == event.user_id,
                                GameSession.ended_at.is_(None),
                            )
                            .order_by(GameSession.started_at.desc())
                            .limit(1)
                        )
                    if active_session is not None and active_session.ended_at is None:
                        active_session.ended_at = event_time
                        active_session.duration_seconds = (
                            event.duration_seconds
                            if event.duration_seconds is not None
                            else max(0, int((event_time - _as_utc(active_session.started_at)).total_seconds()))
                        )

                record.processing_latency_ms = (perf_counter() - started) * 1000
    except IntegrityError:
        with Session(engine) as session:
            if session.get(GamingEventRecord, event.event_id) is not None:
                return False
        raise

    return True


def store_pipeline_metric(
    engine: Engine,
    *,
    component: str,
    events_per_second: float = 0.0,
    total_events: int = 0,
    invalid_events: int = 0,
    duplicate_events: int = 0,
    batch_latency_ms: float = 0.0,
    recorded_at: datetime | None = None,
) -> None:
    with Session(engine) as session:
        with session.begin():
            session.add(
                PipelineMetric(
                    component=component,
                    recorded_at=recorded_at or datetime.now(timezone.utc),
                    events_per_second=max(0.0, events_per_second),
                    total_events=max(0, total_events),
                    invalid_events=max(0, invalid_events),
                    duplicate_events=max(0, duplicate_events),
                    batch_latency_ms=max(0.0, batch_latency_ms),
                )
            )
