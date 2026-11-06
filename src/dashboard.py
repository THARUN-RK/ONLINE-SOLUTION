from __future__ import annotations

import json

import streamlit as st

from config import METRICS_FILE

st.set_page_config(page_title="Gaming Kafka Dashboard", page_icon="🎮", layout="wide")
st.title("Real-Time Gaming Operations Dashboard")

if not METRICS_FILE.exists():
    st.info("No dashboard data yet. Start the Kafka producer and consumer to populate metrics.")
    st.stop()

payload = json.loads(METRICS_FILE.read_text(encoding="utf-8"))
latest = payload.get("latest", {})
history = payload.get("history", [])

col1, col2, col3, col4 = st.columns(4)
col1.metric("Concurrent Users", latest.get("concurrent_users", 0))
col2.metric("Avg Matchmaking Wait", f"{latest.get('matchmaking_wait_ms', 0):.0f} ms")
col3.metric("Economy Volume", f"${latest.get('economy_volume', 0):,.2f}")
col4.metric("Error Rate", f"{latest.get('error_rate_percent', 0):.2f}%")

if history:
    region_usage = latest.get("region_usage", {})
    if region_usage:
        st.subheader("Regional Usage")
        region_data = {region: users for region, users in region_usage.items()}
        st.bar_chart(region_data)

    st.subheader("Time Series Trends")
    trend_data = {
        "Concurrent Users": [entry.get("concurrent_users", 0) for entry in history],
        "Matchmaking Wait (ms)": [entry.get("matchmaking_wait_ms", 0) for entry in history],
        "Error Rate (%)": [entry.get("error_rate_percent", 0) for entry in history],
    }
    st.line_chart(trend_data)

st.subheader("Operational Alerts")
for alert in latest.get("alerts", ["System healthy"]):
    st.warning(alert)

if history:
    st.subheader("Recent Snapshot")
    st.json(latest)
