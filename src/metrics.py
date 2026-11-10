from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List


class GameMetricsAggregator:
    def __init__(self) -> None:
        self.players_online = 0
        self.online_users: set[str] = set()
        self.user_regions: Dict[str, str] = {}
        self.region_users: Dict[str, int] = defaultdict(int)
        self.region_queue_ms: Dict[str, List[float]] = defaultdict(list)
        self.economy_volume = 0.0
        self.total_errors = 0
        self.total_events = 0
        self.matchmaking_waits: List[float] = []
        self.last_seen: str = datetime.now(timezone.utc).isoformat()

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _average_wait_ms(self) -> float:
        if not self.matchmaking_waits:
            return 0.0
        return sum(self.matchmaking_waits) / len(self.matchmaking_waits)

    def _error_rate(self) -> float:
        if self.total_events == 0:
            return 0.0
        return (self.total_errors / self.total_events) * 100

    def _alerts(self) -> List[str]:
        alerts: List[str] = []
        if self._average_wait_ms() > 3500:
            alerts.append("Queue latency above healthy threshold")
        if self._error_rate() > 5:
            alerts.append("Error rate above target limit")
        if self.economy_volume > 25000:
            alerts.append("Economy volume spike detected")
        if not alerts:
            alerts.append("System healthy")
        return alerts

    def handle_event(self, event: Dict[str, Any]) -> None:
        self.total_events += 1
        event_timestamp = event.get("timestamp")
        if isinstance(event_timestamp, str) and event_timestamp:
            self.last_seen = event_timestamp
        else:
            self.last_seen = self._now()

        event_type = event.get("type")
        region = str(event.get("region", "unknown"))

        if event_type == "session_start":
            user_id = str(event.get("user_id", "")).strip()
            if not user_id:
                return
            if user_id not in self.online_users:
                self.online_users.add(user_id)
                self.user_regions[user_id] = region
                self.players_online = len(self.online_users)
                self.region_users[region] += 1
        elif event_type == "session_end":
            user_id = str(event.get("user_id", "")).strip()
            if not user_id:
                return
            if user_id in self.online_users:
                self.online_users.remove(user_id)
                self.players_online = len(self.online_users)
                user_region = self.user_regions.pop(user_id, region)
                self.region_users[user_region] = max(self.region_users.get(user_region, 0) - 1, 0)
            elif user_id in self.user_regions:
                user_region = self.user_regions.pop(user_id)
                self.region_users[user_region] = max(self.region_users.get(user_region, 0) - 1, 0)
        elif event_type == "matchmaking_request":
            pass
        elif event_type == "match_found":
            requested_at = event.get("requested_at")
            matched_at = event.get("matched_at")
            if requested_at and matched_at:
                try:
                    requested_ts = datetime.fromisoformat(requested_at)
                    matched_ts = datetime.fromisoformat(matched_at)
                    wait_ms = (matched_ts - requested_ts).total_seconds() * 1000
                    if wait_ms >= 0:
                        self.matchmaking_waits.append(float(wait_ms))
                        self.region_queue_ms[region].append(float(wait_ms))
                except ValueError:
                    pass
        elif event_type == "purchase":
            try:
                self.economy_volume += float(event.get("amount", 0))
            except (TypeError, ValueError):
                pass
        elif event_type == "error":
            self.total_errors += 1

    def snapshot(self) -> Dict[str, Any]:
        return {
            "timestamp": self.last_seen,
            "concurrent_users": self.players_online,
            "matchmaking_wait_ms": round(self._average_wait_ms(), 2),
            "economy_volume": round(self.economy_volume, 2),
            "error_rate_percent": round(self._error_rate(), 2),
            "region_usage": dict(sorted(self.region_users.items())),
            "region_queue_ms": {
                name: round(sum(values) / len(values), 2) if values else 0.0
                for name, values in sorted(self.region_queue_ms.items())
            },
            "total_events": self.total_events,
            "alerts": self._alerts(),
        }
