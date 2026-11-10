from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from .config import (
    TOPIC_ACHIEVEMENTS,
    TOPIC_GAME_SCORES,
    TOPIC_GAME_SESSIONS,
    TOPIC_PIPELINE_METRICS,
    TOPIC_PLAYER_EVENTS,
    TOPIC_TRANSACTIONS,
)

EventType = Literal[
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
]

TOPIC_BY_EVENT: dict[str, str] = {
    "player_register": TOPIC_PLAYER_EVENTS,
    "player_login": TOPIC_PLAYER_EVENTS,
    "player_logout": TOPIC_PLAYER_EVENTS,
    "error": TOPIC_PLAYER_EVENTS,
    "matchmaking_request": TOPIC_PLAYER_EVENTS,
    "match_found": TOPIC_PLAYER_EVENTS,
    "score": TOPIC_GAME_SCORES,
    "purchase": TOPIC_TRANSACTIONS,
    "session_start": TOPIC_GAME_SESSIONS,
    "session_end": TOPIC_GAME_SESSIONS,
    "achievement": TOPIC_ACHIEVEMENTS,
    "producer_heartbeat": TOPIC_PIPELINE_METRICS,
}


class GamingEvent(BaseModel):
    model_config = ConfigDict(extra="allow", allow_inf_nan=False)

    event_id: str = Field(min_length=1, max_length=128)
    type: EventType
    timestamp: datetime
    user_id: str = Field(min_length=1, max_length=128)
    region: str = Field(default="unknown", min_length=1, max_length=64)
    game_id: str = Field(default="unknown", min_length=1, max_length=128)
    score: float | None = Field(default=None, ge=0)
    amount: Decimal | None = Field(default=None, gt=0, max_digits=14, decimal_places=2)
    duration_seconds: int | None = Field(default=None, ge=0)
    session_id: str | None = Field(default=None, max_length=128)
    achievement_name: str | None = Field(default=None, max_length=128)
    requested_at: datetime | None = None
    matched_at: datetime | None = None

    @field_validator("event_id", "user_id", "region", "game_id", mode="before")
    @classmethod
    def normalize_required_strings(cls, value: Any) -> Any:
        if value is None:
            return value
        return str(value).strip().lower()

    @field_validator("session_id", "achievement_name", mode="before")
    @classmethod
    def normalize_optional_strings(cls, value: Any) -> Any:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @field_validator("timestamp", "requested_at", "matched_at")
    @classmethod
    def require_timezone_aware_timestamps(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.utcoffset() is None:
            raise ValueError("timestamps must include a timezone offset")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def validate_type_payload(self) -> GamingEvent:
        if self.type == "score" and self.score is None:
            raise ValueError("score events require a non-negative score")
        if self.type == "purchase" and self.amount is None:
            raise ValueError("purchase events require a positive amount")
        if self.type == "achievement" and not self.achievement_name:
            raise ValueError("achievement events require achievement_name")
        if self.type == "match_found":
            if self.requested_at is None or self.matched_at is None:
                raise ValueError("match_found events require requested_at and matched_at")
            if self.matched_at < self.requested_at:
                raise ValueError("matched_at must not precede requested_at")
        return self


@dataclass(frozen=True)
class InvalidEvent:
    raw: Any
    reason: str


@dataclass(frozen=True)
class BatchResult:
    events: list[GamingEvent]
    invalid: list[InvalidEvent]
    duplicates: int


def stable_event_id(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def preprocess_batch(raw_events: list[Any]) -> BatchResult:
    prepared: list[dict[str, Any]] = []
    invalid: list[InvalidEvent] = []

    for raw in raw_events:
        if not isinstance(raw, dict):
            invalid.append(InvalidEvent(raw=raw, reason="event must be a JSON object"))
            continue

        payload = dict(raw)
        event_id = str(payload.get("event_id") or "").strip()
        if not event_id:
            payload["event_id"] = stable_event_id(payload)
        else:
            payload["event_id"] = event_id

        for field in ("region", "game_id"):
            value = payload.get(field)
            payload[field] = str(value).strip().lower() if value is not None and str(value).strip() else "unknown"
        prepared.append(payload)

    if not prepared:
        return BatchResult(events=[], invalid=invalid, duplicates=0)

    valid_events: list[GamingEvent] = []
    for payload in prepared:
        try:
            valid_events.append(GamingEvent.model_validate(payload))
        except ValidationError as error:
            invalid.append(InvalidEvent(raw=payload, reason=str(error)))

    if not valid_events:
        return BatchResult(events=[], invalid=invalid, duplicates=0)

    frame = pd.DataFrame(
        [
            {"event_id": event.event_id, "region": event.region, "game_id": event.game_id}
            for event in valid_events
        ]
    )
    duplicate_mask = frame.duplicated(subset=["event_id"], keep="first")
    unique_events = [
        event
        for index, event in enumerate(valid_events)
        if not bool(duplicate_mask.iloc[index])
    ]
    return BatchResult(
        events=unique_events,
        invalid=invalid,
        duplicates=int(duplicate_mask.sum()),
    )


def topic_for_event(event: GamingEvent) -> str:
    return TOPIC_BY_EVENT[event.type]
