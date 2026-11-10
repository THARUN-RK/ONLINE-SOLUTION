from __future__ import annotations

import os
from typing import Any

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000").rstrip("/")

st.set_page_config(
    page_title="Gaming Stream Analytics",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.markdown(
    """
    <style>
    .stApp { background: #0b1020; color: #e7edf7; }
    [data-testid="stSidebar"] { background: #11182a; }
    [data-testid="stMetric"] {
      background: linear-gradient(145deg, #151e33, #101729);
      border: 1px solid #25324d; padding: 14px; border-radius: 12px;
    }
    [data-testid="stMetricLabel"] { color: #9eb0ca; }
    h1, h2, h3 { color: #edf4ff; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("🎮 Gaming Stream Analytics")
st.caption("Real-time operational view • Kafka → validated events → PostgreSQL → FastAPI")
with st.sidebar:
    st.header("Dashboard controls")
    refresh_seconds = st.slider("Refresh interval (seconds)", min_value=2, max_value=30, value=5)
    st.caption(f"Analytics API: `{API_BASE_URL}`")


def fetch(path: str) -> Any | None:
    try:
        response = requests.get(f"{API_BASE_URL}{path}", timeout=5)
        response.raise_for_status()
    except requests.RequestException as error:
        st.error(f"Could not load `{path}`: {error}")
        return None
    try:
        return response.json()
    except ValueError as error:
        st.error(f"The analytics API returned invalid JSON for `{path}`: {error}")
        return None


def show_dataframe_chart(
    data: list[dict[str, Any]],
    *,
    x: str,
    y: str | list[str],
    title: str,
    chart_type: str = "line",
) -> None:
    st.subheader(title)
    if not data:
        st.info("Waiting for matching events.")
        return
    frame = pd.DataFrame(data)
    if chart_type == "bar":
        figure = px.bar(frame, x=x, y=y, template="plotly_dark")
    else:
        figure = px.line(frame, x=x, y=y, markers=True, template="plotly_dark")
    figure.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=12, r=12, t=24, b=12),
        legend_title_text="",
    )
    st.plotly_chart(figure, use_container_width=True)


@st.fragment(run_every=refresh_seconds)
def render_dashboard() -> None:
    summary = fetch("/api/v1/analytics/summary")
    if summary is None:
        st.warning("The dashboard will retry on the next refresh.")
        return

    metrics = [
        ("Total Registered Players", f"{summary['registered_players']:,}"),
        ("Active Players", f"{summary['active_players']:,}"),
        ("Total Gaming Events", f"{summary['total_gaming_events']:,}"),
        ("Total Game Sessions", f"{summary['total_game_sessions']:,}"),
        ("Average Player Score", f"{summary['average_player_score']:,.2f}"),
        ("Total Transactions", f"{summary['total_transactions']:,}"),
        ("Processing Throughput", f"{summary['events_per_second']:.2f} events/s"),
        ("Average Processing Latency", f"{summary['average_processing_latency_ms']:.2f} ms"),
    ]
    for row_start in (0, 4):
        columns = st.columns(4)
        for column, (label, value) in zip(columns, metrics[row_start : row_start + 4]):
            column.metric(label, value)
    st.caption(f"Recorded transaction value: ${summary['transaction_value']:,.2f}")

    activity = fetch("/api/v1/analytics/activity")
    leaderboard = fetch("/api/v1/analytics/leaderboard")
    games = fetch("/api/v1/analytics/games")
    engagement = fetch("/api/v1/analytics/engagement")
    trends = fetch("/api/v1/analytics/trends?days=30")
    transactions = fetch("/api/v1/analytics/transactions")
    sessions = fetch("/api/v1/analytics/sessions")
    performance = fetch("/api/v1/analytics/performance")
    monitoring = fetch("/api/v1/monitoring")

    left, right = st.columns(2)
    with left:
        show_dataframe_chart(
            (activity or {}).get("player_activity", []),
            x="timestamp",
            y="active_players",
            title="Real-Time Player Activity",
        )
    with right:
        show_dataframe_chart(
            (activity or {}).get("events_per_second", []),
            x="timestamp",
            y="events",
            title="Gaming Events per Second",
        )

    left, right = st.columns(2)
    with left:
        show_dataframe_chart(
            leaderboard or [],
            x="player_id",
            y="total_score",
            title="Top 10 Players Leaderboard",
            chart_type="bar",
        )
    with right:
        st.subheader("Game-Wise Performance")
        if games:
            frame = pd.DataFrame(games).melt(
                id_vars="game_id",
                value_vars=["events", "average_score"],
                var_name="measure",
                value_name="value",
            )
            st.plotly_chart(
                px.bar(
                    frame,
                    x="game_id",
                    y="value",
                    color="measure",
                    barmode="group",
                    template="plotly_dark",
                ).update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)"),
                use_container_width=True,
            )
        else:
            st.info("Waiting for game events.")

    left, right = st.columns(2)
    with left:
        show_dataframe_chart(
            engagement or [],
            x="date",
            y=["active_players", "events_per_player"],
            title="Player Engagement (7-Day)",
        )
    with right:
        st.subheader("Daily and Weekly Gaming Trends")
        trend_tab_daily, trend_tab_weekly = st.tabs(["Daily", "Weekly"])
        with trend_tab_daily:
            show_dataframe_chart(
                (trends or {}).get("daily", []),
                x="date",
                y=["events", "active_players"],
                title="Daily Activity",
            )
        with trend_tab_weekly:
            show_dataframe_chart(
                (trends or {}).get("weekly", []),
                x="week",
                y="events",
                title="Weekly Event Volume",
            )

    left, right = st.columns(2)
    with left:
        show_dataframe_chart(
            transactions or [],
            x="date",
            y="amount",
            title="Daily Transaction Analytics",
        )
    with right:
        st.subheader("Game Session Duration")
        distribution = (sessions or {}).get("duration_distribution", [])
        if distribution:
            st.plotly_chart(
                px.bar(
                    pd.DataFrame(distribution),
                    x="duration_range",
                    y="sessions",
                    template="plotly_dark",
                ).update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)"),
                use_container_width=True,
            )
        else:
            st.info("No completed game sessions yet.")
        st.metric(
            "Mean completed session duration",
            f"{(sessions or {}).get('average_duration_seconds', 0):,.0f} sec",
        )

    st.subheader("Data Processing Performance")
    performance_columns = st.columns(4)
    for column, (label, key, suffix) in zip(
        performance_columns,
        (
            ("Observed Throughput", "events_per_second", " events/s"),
            ("Mean Processing Latency", "average_processing_latency_ms", " ms"),
            ("P95 Processing Latency", "p95_processing_latency_ms", " ms"),
            ("Events Measured (last hour)", "measured_events", ""),
        ),
    ):
        value = (performance or {}).get(key, 0)
        column.metric(label, f"{value:,.2f}{suffix}" if isinstance(value, float) else f"{value:,}{suffix}")
    show_dataframe_chart(
        (performance or {}).get("history", []),
        x="timestamp",
        y=["events_per_second", "batch_latency_ms"],
        title="Consumer Batch Performance",
    )

    st.subheader("Kafka Producer and Consumer Monitoring")
    if monitoring is None:
        st.error("Kafka monitoring is unavailable; see API errors above.")
    else:
        producer, consumer = monitoring.get("producer"), monitoring.get("consumer")
        kafka = monitoring.get("kafka", {})
        producer_col, consumer_col, lag_col = st.columns(3)
        if producer:
            producer_col.metric("Producer Rate", f"{producer['events_per_second']:.2f} events/s")
            producer_col.caption(f"Published total: {producer['total_events']:,}")
        else:
            producer_col.info("Waiting for producer heartbeat.")
        if consumer:
            consumer_col.metric("Consumer Rate", f"{consumer['events_per_second']:.2f} events/s")
            consumer_col.caption(
                f"Invalid: {consumer['invalid_events']:,} · Duplicates: {consumer['duplicate_events']:,}"
            )
        else:
            consumer_col.info("Waiting for consumer metrics.")
        if kafka.get("status") == "connected":
            lag_col.metric("Kafka Consumer Group Lag", f"{kafka['consumer_group_lag']:,} records")
            lag_col.caption(f"Monitored partitions: {kafka['partitions']}")
        else:
            lag_col.error(f"Kafka unavailable: {kafka.get('error', 'broker status unknown')}")

    st.caption(monitoring.get("rate_note", "") if monitoring else "")


render_dashboard()
