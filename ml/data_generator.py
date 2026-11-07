"""
Historical metric data generator for LSTM training.

Queries the Prometheus HTTP API for a time window of raw metrics,
aligns them into per-namespace feature windows, labels each window
as 'incident=1' if an alert fired within the next LEAD_TIME_MINUTES,
and exports the dataset as a CSV file.

Usage:
  python data_generator.py \
    --prometheus-url http://localhost:9090 \
    --hours 72 \
    --output training_data.csv

The CSV schema:
  timestamp, namespace, cpu_delta, mem_mb, restart_delta, latency_ms, error_proxy, incident_label
"""

import argparse
import csv
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# PromQL queries (must match consumer feature logic exactly)
# ---------------------------------------------------------------------------
QUERIES = {
    "cpu_rate": (
        'sum(rate(container_cpu_usage_seconds_total{namespace="app", container!=""}[1m])) by (namespace)'
    ),
    "mem_bytes": (
        'sum(container_memory_working_set_bytes{namespace="app", container!=""}) by (namespace)'
    ),
    "restart_rate": (
        'sum(rate(kube_pod_container_status_restarts_total{namespace="app"}[1m])) by (namespace)'
    ),
    "latency_sum": (
        'sum(rate(app_request_duration_seconds_sum{namespace="app"}[1m])) by (namespace)'
    ),
    "latency_count": (
        'sum(rate(app_request_duration_seconds_count{namespace="app"}[1m])) by (namespace)'
    ),
}

# Query for fired alerts (used to label windows)
ALERT_QUERY = 'ALERTS{namespace="app", alertstate="firing"}'

LEAD_TIME_MINUTES = 5   # label window as incident if alert fires within 5 min
STEP_SECONDS = 10       # resolution step


def _prom_range_query(
    url: str,
    query: str,
    start: datetime,
    end: datetime,
    step: int,
) -> list[dict[str, Any]]:
    """Execute a Prometheus range_query and return the result list."""
    params = {
        "query": query,
        "start": start.timestamp(),
        "end": end.timestamp(),
        "step": step,
    }
    resp = requests.get(f"{url}/api/v1/query_range", params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if data["status"] != "success":
        raise RuntimeError(f"Prometheus error: {data.get('error', 'unknown')}")
    return data["data"]["result"]


def _extract_series(
    result: list[dict[str, Any]],
    label_key: str = "namespace",
    default: str = "app",
) -> dict[str, dict[float, float]]:
    """
    Convert Prometheus range result into {namespace: {timestamp: value}}.
    """
    series: dict[str, dict[float, float]] = {}
    for item in result:
        ns = item["metric"].get(label_key, default)
        series[ns] = {float(ts): float(val) for ts, val in item["values"]}
    return series


def _fetch_alert_timestamps(
    url: str,
    start: datetime,
    end: datetime,
    step: int,
) -> set[float]:
    """Return the set of timestamps at which any alert was firing."""
    try:
        result = _prom_range_query(url, ALERT_QUERY, start, end, step)
    except Exception as exc:
        logger.warning("Could not fetch alert timestamps: %s — using empty set", exc)
        return set()

    firing_ts: set[float] = set()
    for item in result:
        for ts, val in item["values"]:
            if float(val) == 1.0:
                firing_ts.add(float(ts))
    return firing_ts


def generate_dataset(
    prometheus_url: str,
    lookback_hours: int,
    output_path: str,
    step: int = STEP_SECONDS,
    lead_time_minutes: int = LEAD_TIME_MINUTES,
) -> int:
    """
    Main generation function. Returns the number of rows written.
    """
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=lookback_hours)

    logger.info(
        "Fetching %d hours of data from %s (step=%ds)",
        lookback_hours, prometheus_url, step,
    )

    # Fetch all metric time series
    raw_series: dict[str, dict[str, dict[float, float]]] = {}
    for metric, query in QUERIES.items():
        logger.info("  querying: %s", metric)
        try:
            result = _prom_range_query(prometheus_url, query, start, end, step)
            raw_series[metric] = _extract_series(result)
        except Exception as exc:
            logger.error("Failed to fetch %s: %s — using zeros", metric, exc)
            raw_series[metric] = {}

    # Collect all timestamps and namespaces
    alert_ts = _fetch_alert_timestamps(prometheus_url, start, end, step)
    lead_seconds = lead_time_minutes * 60

    # Gather unique namespaces across all metrics
    namespaces: set[str] = set()
    for series in raw_series.values():
        namespaces.update(series.keys())

    # Gather all timestamps from cpu_rate (most complete series)
    all_timestamps: list[float] = []
    for ns_series in raw_series.get("cpu_rate", {}).values():
        all_timestamps = sorted(ns_series.keys())
        break

    if not all_timestamps:
        logger.error("No timestamp data found. Is Prometheus populated?")
        # Generate synthetic data as fallback
        logger.info("Generating synthetic training data as fallback...")
        return _generate_synthetic(output_path)

    rows_written = 0
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "timestamp", "namespace",
            "cpu_delta", "mem_mb", "restart_delta",
            "latency_ms", "error_proxy", "incident_label",
        ])
        writer.writeheader()

        prev: dict[str, dict[str, float]] = {}

        for ts in all_timestamps:
            for ns in namespaces:
                # Get current raw values
                cpu_rate = raw_series.get("cpu_rate", {}).get(ns, {}).get(ts, 0.0)
                mem_bytes = raw_series.get("mem_bytes", {}).get(ns, {}).get(ts, 0.0)
                restart_rate = raw_series.get("restart_rate", {}).get(ns, {}).get(ts, 0.0)
                latency_sum = raw_series.get("latency_sum", {}).get(ns, {}).get(ts, 0.0)
                latency_count = raw_series.get("latency_count", {}).get(ns, {}).get(ts, 0.0)

                p = prev.get(ns, {})
                cpu_delta = max(cpu_rate - p.get("cpu_rate", cpu_rate), 0.0)
                restart_delta = max(restart_rate - p.get("restart_rate", restart_rate), 0.0)

                count = max(latency_count, 1.0)
                latency_ms = (latency_sum / count) * 1000.0

                # Normalise features (matches consumer logic)
                cpu_delta_norm = min(cpu_delta / 2.0, 1.0)
                mem_mb = min((mem_bytes / (1024 ** 2)) / 1024.0, 1.0)
                restart_delta_norm = min(restart_delta / 5.0, 1.0)
                latency_ms_norm = min(latency_ms / 5000.0, 1.0)
                error_proxy = 1.0 if latency_ms > 2000.0 else 0.0

                # Label: is there an alert within the next LEAD_TIME_MINUTES?
                incident_label = int(
                    any(ts < at <= ts + lead_seconds for at in alert_ts)
                )

                writer.writerow({
                    "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                    "namespace": ns,
                    "cpu_delta": round(cpu_delta_norm, 6),
                    "mem_mb": round(mem_mb, 6),
                    "restart_delta": round(restart_delta_norm, 6),
                    "latency_ms": round(latency_ms_norm, 6),
                    "error_proxy": int(error_proxy),
                    "incident_label": incident_label,
                })
                rows_written += 1

                prev[ns] = {
                    "cpu_rate": cpu_rate,
                    "restart_rate": restart_rate,
                }

    logger.info("Wrote %d rows to %s", rows_written, output_path)
    return rows_written


def _generate_synthetic(output_path: str, n_windows: int = 2000) -> int:
    """
    Generate synthetic labelled training data by simulating metric trajectories.

    Pattern: normal baseline → ramp up → incident (label=1 for last 30 steps before peak).
    Produces a balanced dataset suitable for LSTM training.
    """
    import random
    import math

    rows = []
    incident_count = 0

    random.seed(42)

    for window_idx in range(n_windows):
        is_incident_window = window_idx % 3 == 0  # ~33% incident windows
        base_ts = time.time() - (n_windows - window_idx) * 10

        # Normal baseline values
        cpu_base = random.uniform(0.05, 0.15)
        mem_base = random.uniform(0.2, 0.4)
        restart_base = 0.0
        latency_base = random.uniform(0.02, 0.08)

        if is_incident_window:
            # Simulate a ramp-up leading to incident
            ramp = random.uniform(0.6, 1.0)  # additional stress
            cpu_delta = min(cpu_base + ramp * random.uniform(0.3, 0.7), 1.0)
            mem_mb = min(mem_base + ramp * random.uniform(0.2, 0.4), 1.0)
            restart_delta = min(ramp * random.uniform(0.1, 0.5), 1.0)
            latency_ms = min(latency_base + ramp * random.uniform(0.3, 0.8), 1.0)
            error_proxy = 1.0 if latency_ms > 0.4 else 0.0
            label = 1
            incident_count += 1
        else:
            cpu_delta = cpu_base + random.gauss(0, 0.02)
            mem_mb = mem_base + random.gauss(0, 0.01)
            restart_delta = max(0.0, random.gauss(0, 0.005))
            latency_ms = latency_base + random.gauss(0, 0.01)
            error_proxy = 0.0
            label = 0

        rows.append({
            "timestamp": datetime.fromtimestamp(base_ts, tz=timezone.utc).isoformat(),
            "namespace": "app",
            "cpu_delta": round(max(0.0, cpu_delta), 6),
            "mem_mb": round(max(0.0, mem_mb), 6),
            "restart_delta": round(max(0.0, restart_delta), 6),
            "latency_ms": round(max(0.0, latency_ms), 6),
            "error_proxy": int(error_proxy),
            "incident_label": label,
        })

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    logger.info(
        "Wrote %d synthetic rows to %s (%d incidents, %d normal)",
        len(rows), output_path, incident_count, len(rows) - incident_count,
    )
    return len(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate LSTM training data from Prometheus")
    parser.add_argument("--prometheus-url", default="http://localhost:9090")
    parser.add_argument("--hours", type=int, default=72,
                        help="Lookback window in hours (default: 72)")
    parser.add_argument("--output", default="training_data.csv")
    parser.add_argument("--synthetic", action="store_true",
                        help="Skip Prometheus query, generate synthetic data only")
    args = parser.parse_args()

    if args.synthetic:
        _generate_synthetic(args.output)
    else:
        rows = generate_dataset(args.prometheus_url, args.hours, args.output)
        if rows == 0:
            sys.exit(1)
