from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from scripts.benchmark import run_benchmark

DEFAULT_SIZES = (1_000, 10_000, 50_000, 100_000, 500_000)


def summarize(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[int, list[dict[str, Any]]] = {}
    for run in runs:
        grouped.setdefault(run["events"], []).append(run["report"])

    summary: list[dict[str, Any]] = []
    for size, trials in sorted(grouped.items()):
        throughputs = [trial["performance"]["stored_events_per_second"] for trial in trials]
        latencies = [trial["performance"]["database_processing_latency_p95_ms"] for trial in trials]
        summary.append(
            {
                "events": size,
                "trials": len(trials),
                "median_throughput_events_per_second": round(median(throughputs), 2),
                "min_throughput_events_per_second": round(min(throughputs), 2),
                "max_throughput_events_per_second": round(max(throughputs), 2),
                "median_database_p95_latency_ms": round(median(latencies), 3),
                "median_peak_traced_python_memory_bytes": int(
                    median(
                        trial["performance"]["peak_traced_python_memory_bytes"]
                        for trial in trials
                    )
                ),
                "all_trials_duplicate_detection_rate_percent": [
                    trial["data_quality"]["duplicate_detection_rate_percent"]
                    for trial in trials
                ],
                "all_trials_invalid_detection": [
                    trial["data_quality"]["observed_invalid"]
                    == trial["data_quality"]["expected_invalid"]
                    for trial in trials
                ],
            }
        )
    return summary


def write_outputs(output: Path, runs: list[dict[str, Any]]) -> None:
    summary = summarize(runs)
    payload = {
        "scope": "Measured local preprocessing and database microbenchmark; excludes Kafka, network, and dashboard rendering.",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "runs": runs,
        "summary": summary,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    csv_path = output.with_suffix(".csv")
    if summary:
        with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=summary[0].keys())
            writer.writeheader()
            writer.writerows(summary)

        figure = make_subplots(specs=[[{"secondary_y": True}]])
        sizes = [row["events"] for row in summary]
        figure.add_trace(
            go.Scatter(
                x=sizes,
                y=[row["median_throughput_events_per_second"] for row in summary],
                mode="lines+markers",
                name="Median throughput (events/s)",
            ),
            secondary_y=False,
        )
        figure.add_trace(
            go.Scatter(
                x=sizes,
                y=[row["median_database_p95_latency_ms"] for row in summary],
                mode="lines+markers",
                name="Median database p95 latency (ms)",
            ),
            secondary_y=True,
        )
        figure.update_xaxes(title_text="Configured base events", type="log")
        figure.update_yaxes(title_text="Throughput (events/s)", secondary_y=False)
        figure.update_yaxes(title_text="Database p95 latency (ms)", secondary_y=True)
        figure.update_layout(
            title="Measured Local Gaming Event Benchmark",
            template="plotly_dark",
        )
        figure.write_html(output.with_suffix(".html"), include_plotlyjs=True)


def parse_sizes(value: str) -> tuple[int, ...]:
    try:
        sizes = tuple(int(item.strip()) for item in value.split(","))
    except ValueError as error:
        raise argparse.ArgumentTypeError("sizes must be comma-separated positive integers") from error
    if not sizes or any(size < 1 for size in sizes):
        raise argparse.ArgumentTypeError("sizes must be comma-separated positive integers")
    return sizes


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run real local trials and export a comparison table and Plotly graph."
    )
    parser.add_argument("--sizes", type=parse_sizes, default=DEFAULT_SIZES)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--players", type=int, default=250)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--database-url", default="sqlite+pysqlite:///:memory:")
    parser.add_argument("--output", type=Path, default=Path("results/performance-evaluation.json"))
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("--repeats must be at least one")

    completed_runs: list[dict[str, Any]] = []
    for size in args.sizes:
        for trial in range(1, args.repeats + 1):
            report = run_benchmark(
                count=size,
                seed=args.seed,
                player_count=args.players,
                database_url=args.database_url,
            )
            completed_runs.append({"events": size, "trial": trial, "report": report})
            write_outputs(args.output, completed_runs)
            print(
                f"Completed events={size} trial={trial}/{args.repeats} "
                f"throughput={report['performance']['stored_events_per_second']:.2f} events/s"
            )
