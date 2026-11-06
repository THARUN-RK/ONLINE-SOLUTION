from datetime import datetime, timedelta, timezone

from src.metrics import GameMetricsAggregator


def test_aggregator_tracks_players_and_economy():
    aggregator = GameMetricsAggregator()

    now = datetime.now(timezone.utc)
    start_1 = now.isoformat()
    start_2 = (now + timedelta(seconds=5)).isoformat()
    request = now.isoformat()
    matched = (now + timedelta(seconds=3)).isoformat()

    aggregator.handle_event({"type": "session_start", "user_id": "u1", "region": "us-east", "timestamp": start_1})
    aggregator.handle_event({"type": "session_start", "user_id": "u2", "region": "us-east", "timestamp": start_2})
    aggregator.handle_event({"type": "matchmaking_request", "user_id": "u1", "region": "us-east", "timestamp": request})
    aggregator.handle_event({"type": "match_found", "user_id": "u1", "region": "us-east", "requested_at": request, "matched_at": matched})
    aggregator.handle_event({"type": "purchase", "user_id": "u2", "region": "us-east", "amount": 75.50})
    aggregator.handle_event({"type": "error", "user_id": "u1", "region": "us-east", "error_code": "E104"})
    aggregator.handle_event({"type": "session_end", "user_id": "u2", "region": "us-east", "timestamp": now.isoformat()})

    snapshot = aggregator.snapshot()

    assert snapshot["concurrent_users"] == 1
    assert snapshot["economy_volume"] == 75.5
    assert snapshot["error_rate_percent"] > 0
    assert snapshot["matchmaking_wait_ms"] > 0
    assert snapshot["region_usage"]["us-east"] == 1
