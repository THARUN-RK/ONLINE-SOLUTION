from __future__ import annotations

import os

from locust import HttpUser, between, task


class GamingAnalyticsUser(HttpUser):
    wait_time = between(0.5, 2.0)

    def on_start(self) -> None:
        username = os.getenv("DASHBOARD_USERNAME")
        password = os.getenv("DASHBOARD_PASSWORD")
        if not username or not password:
            raise RuntimeError("Set DASHBOARD_USERNAME and DASHBOARD_PASSWORD before starting Locust.")
        self.auth = (username, password)

    @task(5)
    def summary(self) -> None:
        self._get("/api/v1/analytics/summary")

    @task(3)
    def activity(self) -> None:
        self._get("/api/v1/analytics/activity")

    @task(2)
    def leaderboard(self) -> None:
        self._get("/api/v1/analytics/leaderboard")

    def _get(self, path: str) -> None:
        with self.client.get(path, auth=self.auth, catch_response=True) as response:
            if response.status_code != 200:
                response.failure(f"HTTP {response.status_code}: {response.text[:200]}")
