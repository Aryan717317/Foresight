"""
Kafka consumer → feature pipeline → LSTM inference → predicted-incident events.

This service:
1. Consumes raw metric events from Kafka topic 'raw-metrics'
2. Maintains per-entity rolling windows (30 samples × 10s = 5 min lookback)
3. Computes 5 engineered features per window step
4. Loads the 'Production' LSTM model from MLflow Model Registry (refresh every 5 min)
5. Runs inference; if confidence ≥ threshold, POSTs a predicted-incident event
   to the remediation bot's /predict-webhook endpoint
6. Exposes a /metrics endpoint so Prometheus can track predictor performance
"""

import json
import logging
import os
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import requests
import torch
import torch.nn as nn
import mlflow
import mlflow.pytorch
from confluent_kafka import Consumer, KafkaError, KafkaException
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from prometheus_client import (
    Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
)

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
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "predictor-consumer")

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = os.getenv("MODEL_NAME", "incident-predictor")
MODEL_REFRESH_INTERVAL = int(os.getenv("MODEL_REFRESH_INTERVAL_S", "300"))  # 5 min

REMEDIATION_BOT_URL = os.getenv(
    "REMEDIATION_BOT_URL", "http://remediation-bot-service.monitoring.svc.cluster.local:9000"
)
CONFIDENCE_THRESHOLD = float(os.getenv("CONFIDENCE_THRESHOLD", "0.75"))
ACT_ON_PREDICTIONS = os.getenv("PREDICTOR_ACT_ON_PREDICTIONS", "false").lower() == "true"

WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", "30"))       # steps
WINDOW_STEP_S = int(os.getenv("WINDOW_STEP_S", "10"))   # seconds per step

# ---------------------------------------------------------------------------
# Prometheus metrics (this service's own telemetry)
# ---------------------------------------------------------------------------
PREDICTIONS_TOTAL = Counter(
    "predictor_incident_predictions_total",
    "Total predicted-incident events emitted",
    ["namespace", "acted"],
)
PREDICTIONS_CONFIDENCE = Histogram(
    "predictor_confidence_score",
    "Confidence score distribution of all predictions",
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 0.95, 1.0],
)
WINDOW_FILLS = Counter(
    "predictor_window_fills_total",
    "Number of completed windows processed",
    ["namespace"],
)
MODEL_VERSION = Gauge(
    "predictor_model_version",
    "Currently loaded MLflow model version",
)
INFERENCE_LATENCY = Histogram(
    "predictor_inference_duration_seconds",
    "LSTM inference latency per window",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5],
)
KAFKA_LAG = Gauge("predictor_kafka_consumer_lag", "Estimated Kafka consumer lag")

# ---------------------------------------------------------------------------
# LSTM Model Definition (must match train.py architecture)
# ---------------------------------------------------------------------------
class IncidentLSTM(nn.Module):
    """
    2-layer LSTM → Linear(1) → Sigmoid.
    Input:  (batch, seq_len=30, features=5)
    Output: (batch, 1) — incident probability
    """
    def __init__(self, input_size: int = 5, hidden_size: int = 64, num_layers: int = 2, dropout: float = 0.2):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.head = nn.Sequential(
            nn.Dropout(dropout),
            nn.Linear(hidden_size, 1),
            nn.Sigmoid(),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        _, (h_n, _) = self.lstm(x)
        return self.head(h_n[-1])  # last layer's hidden state


# ---------------------------------------------------------------------------
# Model loader (thread-safe, periodic refresh)
# ---------------------------------------------------------------------------
class ModelLoader:
    def __init__(self):
        self._model: Optional[IncidentLSTM] = None
        self._version: int = -1
        self._lock = threading.Lock()
        self._last_loaded: float = 0.0
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    def get_model(self) -> Optional[IncidentLSTM]:
        now = time.monotonic()
        if now - self._last_loaded > MODEL_REFRESH_INTERVAL:
            self._refresh()
        with self._lock:
            return self._model

    def _refresh(self):
        try:
            client = mlflow.tracking.MlflowClient()
            versions = client.get_latest_versions(MODEL_NAME, stages=["Production"])
            if not versions:
                logger.warning("No Production model found in MLflow registry for '%s'", MODEL_NAME)
                return
            mv = versions[0]
            new_version = int(mv.version)
            if new_version == self._version:
                self._last_loaded = time.monotonic()
                return  # already current

            model_uri = f"models:/{MODEL_NAME}/Production"
            model = mlflow.pytorch.load_model(model_uri, map_location="cpu")
            model.eval()

            with self._lock:
                self._model = model
                self._version = new_version

            MODEL_VERSION.set(new_version)
            self._last_loaded = time.monotonic()
            logger.info("Loaded model %s v%d from MLflow", MODEL_NAME, new_version)

        except Exception as exc:
            logger.error("Model refresh failed: %s", exc)


_model_loader = ModelLoader()

# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------
# Per-namespace rolling windows: each slot holds the latest feature vector
# windows[namespace] = deque(maxlen=WINDOW_SIZE) of np.ndarray(5,)
_windows: dict[str, deque] = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))

# Latest raw metric values per namespace (updated on every event)
_raw: dict[str, dict] = defaultdict(lambda: {
    "cpu_rate": 0.0,
    "mem_bytes": 0.0,
    "restart_rate": 0.0,
    "latency_sum": 0.0,
    "latency_count": 0.0,
    "prev_cpu_rate": 0.0,
    "prev_mem_bytes": 0.0,
    "prev_restart_rate": 0.0,
})

_METRIC_TO_RAW_KEY = {
    "container_cpu_usage_seconds_total": "cpu_rate",
    "container_memory_working_set_bytes": "mem_bytes",
    "kube_pod_container_status_restarts_total": "restart_rate",
    "app_request_duration_seconds_sum": "latency_sum",
    "app_request_duration_seconds_count": "latency_count",
}

_windows_lock = threading.Lock()
_WINDOW_TICKER_INTERVAL = WINDOW_STEP_S  # seconds


def _ingest_event(event: dict) -> None:
    """Update raw metric accumulators from an incoming Kafka event."""
    metric_name = event.get("metric_name", "")
    labels = event.get("labels", {})
    value = float(event.get("value", 0.0))
    namespace = labels.get("namespace", "unknown")

    key = _METRIC_TO_RAW_KEY.get(metric_name)
    if key:
        with _windows_lock:
            _raw[namespace][key] = value


def _compute_feature_vector(ns: str) -> np.ndarray:
    """
    Compute a 5-dimensional feature vector for namespace ns.

    Features:
      0: cpu_delta       — change in CPU rate vs. previous step (normalised)
      1: mem_mb          — memory in MiB (normalised to 1024 MiB max)
      2: restart_delta   — change in restart rate vs. previous step
      3: latency_ms      — avg request latency in ms (capped at 5000ms)
      4: error_proxy     — 0/1 flag: latency spike > 2s (simple proxy for error rate)
    """
    r = _raw[ns]
    cpu_delta = max(r["cpu_rate"] - r["prev_cpu_rate"], 0.0)
    mem_mb = r["mem_bytes"] / (1024 ** 2)
    restart_delta = max(r["restart_rate"] - r["prev_restart_rate"], 0.0)
    count = max(r["latency_count"], 1.0)
    latency_ms = (r["latency_sum"] / count) * 1000.0

    # Normalise to roughly [0, 1]
    feat = np.array([
        min(cpu_delta / 2.0, 1.0),              # cpu_delta (cap at 2 cores/s)
        min(mem_mb / 1024.0, 1.0),              # mem_mb (cap at 1 GiB)
        min(restart_delta / 5.0, 1.0),          # restart_delta (cap at 5/s)
        min(latency_ms / 5000.0, 1.0),          # latency_ms
        1.0 if latency_ms > 2000.0 else 0.0,   # error_proxy
    ], dtype=np.float32)

    # Update previous values for next step
    r["prev_cpu_rate"] = r["cpu_rate"]
    r["prev_mem_bytes"] = r["mem_bytes"]
    r["prev_restart_rate"] = r["restart_rate"]

    return feat


def _tick_windows() -> None:
    """
    Called every WINDOW_STEP_S seconds.
    Appends the current feature vector to each known namespace's window,
    then runs inference if the window is full.
    """
    while True:
        time.sleep(_WINDOW_TICKER_INTERVAL)
        with _windows_lock:
            namespaces = list(_raw.keys())

        for ns in namespaces:
            with _windows_lock:
                feat = _compute_feature_vector(ns)
                _windows[ns].append(feat)
                window_len = len(_windows[ns])

            if window_len >= WINDOW_SIZE:
                WINDOW_FILLS.labels(namespace=ns).inc()
                _run_inference(ns)


def _run_inference(ns: str) -> None:
    """Run LSTM inference on the current window for namespace ns."""
    model = _model_loader.get_model()
    if model is None:
        return

    with _windows_lock:
        window = np.array(list(_windows[ns]), dtype=np.float32)  # (30, 5)

    x = torch.from_numpy(window).unsqueeze(0)  # (1, 30, 5)

    t0 = time.perf_counter()
    with torch.no_grad():
        prob = model(x).item()
    INFERENCE_LATENCY.observe(time.perf_counter() - t0)

    PREDICTIONS_CONFIDENCE.observe(prob)
    logger.debug("Inference ns=%s confidence=%.4f", ns, prob)

    if prob >= CONFIDENCE_THRESHOLD:
        _emit_predicted_incident(ns, prob)


def _emit_predicted_incident(ns: str, confidence: float) -> None:
    """POST a predicted-incident event to the remediation bot."""
    payload = {
        "alerts": [{
            "status": "firing",
            "labels": {
                "alertname": "PredictedIncident",
                "namespace": ns,
                "severity": "warning",
                "source": "lstm-predictor",
                "confidence": f"{confidence:.4f}",
                "model_version": str(int(MODEL_VERSION._value.get())),
            },
            "annotations": {
                "summary": f"LSTM predictor forecasts incident in namespace '{ns}'",
                "description": (
                    f"Confidence: {confidence:.1%}. "
                    f"Predicted 3–5 min before threshold breach. "
                    f"Acting on predictions: {ACT_ON_PREDICTIONS}."
                ),
            },
        }]
    }

    acted = "true" if ACT_ON_PREDICTIONS else "false"
    PREDICTIONS_TOTAL.labels(namespace=ns, acted=acted).inc()

    endpoint = f"{REMEDIATION_BOT_URL}/predict-webhook"
    try:
        resp = requests.post(endpoint, json=payload, timeout=5.0)
        resp.raise_for_status()
        logger.info(
            "Predicted incident emitted ns=%s confidence=%.4f acted=%s status=%d",
            ns, confidence, acted, resp.status_code,
        )
    except Exception as exc:
        logger.error("Failed to emit predicted incident: %s", exc)


# ---------------------------------------------------------------------------
# Kafka consumer loop (runs in background thread)
# ---------------------------------------------------------------------------

def _consume_loop() -> None:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    }
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])
    logger.info("Kafka consumer subscribed to topic=%s group=%s", KAFKA_TOPIC, KAFKA_GROUP_ID)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            try:
                event = json.loads(msg.value().decode())
                _ingest_event(event)
            except Exception as exc:
                logger.warning("Failed to process Kafka message: %s", exc)

    finally:
        consumer.close()


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="predictor-consumer", version="1.0.0")


@app.on_event("startup")
async def startup():
    # Pre-warm model
    _model_loader.get_model()

    # Start window ticker in background
    t = threading.Thread(target=_tick_windows, daemon=True, name="window-ticker")
    t.start()

    # Start Kafka consumer in background
    c = threading.Thread(target=_consume_loop, daemon=True, name="kafka-consumer")
    c.start()

    logger.info("Consumer service started — topic=%s", KAFKA_TOPIC)


@app.get("/health")
async def health():
    model = _model_loader.get_model()
    return {
        "status": "ok",
        "model_loaded": model is not None,
        "model_version": _model_loader._version,
        "ts": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/metrics", include_in_schema=False)
async def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/windows")
async def get_windows():
    """Debug endpoint — shows current window fill level per namespace."""
    with _windows_lock:
        return {ns: len(w) for ns, w in _windows.items()}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8081"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
