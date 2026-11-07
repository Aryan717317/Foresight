"""
Prometheus remote_write receiver → Kafka producer.

Prometheus is configured with a remoteWrite target pointing here.
Each write request contains snappy-compressed protobuf TimeSeries data.
We decode, filter to relevant metrics, and publish JSON events to the
Kafka topic 'raw-metrics'.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import snappy
from confluent_kafka import Producer
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import PlainTextResponse
from prometheus_client import (
    Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
)

# Prometheus remote_write protobuf — generated from prometheus/prometheus proto
# We use the pre-built `prometheus-remote-storage-adapter` proto definitions
# bundled in the `prometheus_client` ecosystem. For simplicity we use the
# `snappy` + manual protobuf decode via `betterproto` generated classes.
try:
    from prompb import types_pb2 as prompb
    USE_PROTOBUF = True
except ImportError:
    USE_PROTOBUF = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-metrics")
KAFKA_FLUSH_TIMEOUT = float(os.getenv("KAFKA_FLUSH_TIMEOUT", "5.0"))

# Metrics we care about — everything else is dropped to keep Kafka lean
ALLOWED_METRICS = {
    "container_cpu_usage_seconds_total",
    "container_memory_working_set_bytes",
    "kube_pod_container_status_restarts_total",
    "app_request_duration_seconds_sum",
    "app_request_duration_seconds_count",
    "app_requests_total",
    "node_cpu_seconds_total",
    "node_memory_MemAvailable_bytes",
    "node_memory_MemTotal_bytes",
}

# ---------------------------------------------------------------------------
# Internal Prometheus metrics (for this producer)
# ---------------------------------------------------------------------------
MSGS_PUBLISHED = Counter(
    "producer_messages_published_total",
    "Total messages published to Kafka",
    ["metric_name"],
)
MSGS_DROPPED = Counter(
    "producer_messages_dropped_total",
    "Metric events dropped (not in allow-list)",
)
DECODE_ERRORS = Counter(
    "producer_decode_errors_total",
    "Failed remote_write payload decodes",
)
WRITE_LATENCY = Histogram(
    "producer_write_duration_seconds",
    "End-to-end remote_write handler latency",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
)

# ---------------------------------------------------------------------------
# Kafka producer setup
# ---------------------------------------------------------------------------
_kafka_producer: Producer | None = None


def _get_producer() -> Producer:
    global _kafka_producer
    if _kafka_producer is None:
        conf = {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "client.id": "prometheus-remote-write-producer",
            "acks": "1",
            "linger.ms": 20,
            "batch.num.messages": 500,
            "compression.type": "lz4",
        }
        _kafka_producer = Producer(conf)
        logger.info("Kafka producer created → %s topic=%s", KAFKA_BOOTSTRAP, KAFKA_TOPIC)
    return _kafka_producer


def _delivery_report(err, msg):
    if err:
        logger.error("Kafka delivery failed: %s", err)


def _publish(event: dict) -> None:
    """Serialize and publish a metric event to Kafka."""
    producer = _get_producer()
    key = event.get("metric_name", "unknown").encode()
    value = json.dumps(event, separators=(",", ":")).encode()
    producer.produce(
        KAFKA_TOPIC,
        key=key,
        value=value,
        callback=_delivery_report,
    )
    # Non-blocking — we poll periodically
    producer.poll(0)


# ---------------------------------------------------------------------------
# Payload parsing helpers
# ---------------------------------------------------------------------------

def _parse_labels(label_pairs) -> dict:
    """Convert a repeated LabelPair sequence to a plain dict."""
    return {lp.name: lp.value for lp in label_pairs}


def _decode_protobuf(compressed: bytes) -> list[dict]:
    """
    Decode a Prometheus remote_write protobuf payload.

    Returns a list of metric event dicts:
      {metric_name, labels, value, timestamp_ms, ts_iso}
    """
    raw = snappy.uncompress(compressed)

    if not USE_PROTOBUF:
        # Fallback: we can't decode without the generated proto; log and skip
        logger.warning("prompb not available — cannot decode protobuf payload")
        return []

    write_request = prompb.WriteRequest()
    write_request.ParseFromString(raw)

    events = []
    for ts in write_request.timeseries:
        labels = _parse_labels(ts.labels)
        metric_name = labels.get("__name__", "")

        if metric_name not in ALLOWED_METRICS:
            MSGS_DROPPED.inc()
            continue

        for sample in ts.samples:
            event = {
                "metric_name": metric_name,
                "labels": labels,
                "value": sample.value,
                "timestamp_ms": sample.timestamp,
                "ts_iso": datetime.fromtimestamp(
                    sample.timestamp / 1000, tz=timezone.utc
                ).isoformat(),
            }
            events.append(event)

    return events


def _decode_text_fallback(body: bytes) -> list[dict]:
    """
    Minimal fallback: parse the Prometheus text exposition format.
    Used when remote_write sends text/plain (e.g., during testing).
    """
    events = []
    ts_ms = int(time.time() * 1000)
    ts_iso = datetime.now(timezone.utc).isoformat()
    for line in body.decode(errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        metric_raw = parts[0]
        try:
            value = float(parts[1])
        except ValueError:
            continue

        # Extract name and labels from metric{label=value,...}
        if "{" in metric_raw:
            name, rest = metric_raw.split("{", 1)
            label_str = rest.rstrip("}")
            labels: dict = {"__name__": name}
            for pair in label_str.split(","):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    labels[k.strip()] = v.strip().strip('"')
        else:
            name = metric_raw
            labels = {"__name__": name}

        if name not in ALLOWED_METRICS:
            MSGS_DROPPED.inc()
            continue

        events.append({
            "metric_name": name,
            "labels": labels,
            "value": value,
            "timestamp_ms": ts_ms,
            "ts_iso": ts_iso,
        })
    return events


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="prometheus-remote-write-producer", version="1.0.0")


@app.get("/health")
async def health():
    return {"status": "ok", "ts": datetime.now(timezone.utc).isoformat()}


@app.get("/metrics", include_in_schema=False)
async def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/receive")
async def receive_remote_write(request: Request):
    """
    Prometheus remote_write endpoint.
    Content-Type: application/x-protobuf (snappy compressed)
    Header X-Prometheus-Remote-Write-Version: 0.1.0
    """
    t0 = time.perf_counter()
    content_type = request.headers.get("content-type", "")
    body = await request.body()

    if not body:
        raise HTTPException(status_code=400, detail="Empty body")

    try:
        if "protobuf" in content_type or request.headers.get(
            "X-Prometheus-Remote-Write-Version"
        ):
            events = _decode_protobuf(body)
        else:
            # Text fallback (useful for curl-based testing)
            events = _decode_text_fallback(body)
    except Exception as exc:
        DECODE_ERRORS.inc()
        logger.error("Decode error: %s", exc)
        raise HTTPException(status_code=400, detail=f"Decode error: {exc}") from exc

    published = 0
    for event in events:
        _publish(event)
        MSGS_PUBLISHED.labels(event["metric_name"]).inc()
        published += 1

    # Periodic flush (non-blocking in hot path)
    _get_producer().poll(0)

    elapsed = time.perf_counter() - t0
    WRITE_LATENCY.observe(elapsed)

    return Response(status_code=204)


@app.on_event("shutdown")
async def shutdown():
    if _kafka_producer:
        logger.info("Flushing Kafka producer on shutdown…")
        _kafka_producer.flush(timeout=KAFKA_FLUSH_TIMEOUT)


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
