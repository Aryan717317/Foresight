import logging
import os
from collections import deque
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict

from flask import Flask, request, jsonify

from remediation_bot import handle_alert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# When True, predicted-incident events trigger actual k8s remediation.
# When False (default / predict-only mode), events are logged only.
_ACT_ON_PREDICTIONS: bool = os.getenv("PREDICTOR_ACT_ON_PREDICTIONS", "false").lower() == "true"

_log_lock = Lock()
_action_log: deque = deque(maxlen=100)
_prediction_log: deque = deque(maxlen=500)


@app.get("/health")
def health():
    return {"status": "ok", "ts": datetime.now(timezone.utc).isoformat()}


@app.post("/webhook")
def webhook():
    """
    Webhook endpoint for receiving alerts from Alertmanager.
    """
    if not request.is_json:
        return jsonify({"error": "expected application/json"}), 415

    payload = request.get_json(force=True)
    alerts  = payload.get("alerts", [])
    results = []

    for raw in alerts:
        alert = {
            "alertname": raw.get("labels", {}).get("alertname", ""),
            "labels":    raw.get("labels", {}),
            "status":    raw.get("status", ""),
        }
        result = handle_alert(alert)
        entry  = {"alert": alert["alertname"], "status": alert["status"], "result": result,
                  "ts": datetime.now(timezone.utc).isoformat()}
        with _log_lock:
            _action_log.append(entry)
        results.append(entry)

    return jsonify({"processed": len(results), "results": results}), 200


@app.post("/trigger")
def trigger():
    """
    Manual remediation trigger for testing.
    """
    body   = request.get_json(force=True)
    result = handle_alert({
        "alertname": body.get("alertname", ""),
        "labels":    body.get("labels", {}),
        "status":    "firing",
    })
    return jsonify(result), 200


@app.get("/log")
def get_log():
    """Returns a log of the last 100 remediation actions."""
    with _log_lock:
        entries = list(_action_log)
    return jsonify({"count": len(entries), "entries": entries}), 200


@app.post("/predict-webhook")
def predict_webhook():
    """
    Receives predicted-incident events from the LSTM consumer service.

    In predict-only mode (PREDICTOR_ACT_ON_PREDICTIONS=false), events are
    logged and counted but no Kubernetes action is taken.  This is the
    default safe mode — flip the env var to enable actual remediation.
    """
    if not request.is_json:
        return jsonify({"error": "expected application/json"}), 415

    payload = request.get_json(force=True)
    alerts: list[Dict[str, Any]] = payload.get("alerts", [])
    results = []

    for raw in alerts:
        labels = raw.get("labels", {})
        alert: Dict[str, Any] = {
            "alertname": labels.get("alertname", "PredictedIncident"),
            "labels": labels,
            "status": raw.get("status", "firing"),
        }

        confidence = labels.get("confidence", "n/a")
        namespace = labels.get("namespace", "unknown")
        model_version = labels.get("model_version", "unknown")

        if _ACT_ON_PREDICTIONS:
            result = handle_alert(alert)
            action_taken = True
        else:
            # Predict-only: log but don't act
            result = {
                "action": "predicted",
                "acted": False,
                "confidence": confidence,
                "namespace": namespace,
                "model_version": model_version,
                "reason": "PREDICTOR_ACT_ON_PREDICTIONS=false",
            }
            action_taken = False

        entry = {
            "alert": alert["alertname"],
            "namespace": namespace,
            "confidence": confidence,
            "model_version": model_version,
            "acted": action_taken,
            "result": result,
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        with _log_lock:
            _prediction_log.append(entry)
        results.append(entry)

        logger.info(
            "Predicted incident ns=%s confidence=%s model_v=%s acted=%s",
            namespace, confidence, model_version, action_taken,
        )

    return jsonify({"processed": len(results), "results": results, "acting": _ACT_ON_PREDICTIONS}), 200


@app.get("/predictions")
def get_predictions():
    """Returns a log of the last 500 predicted-incident events."""
    with _log_lock:
        entries = list(_prediction_log)
    return jsonify({
        "count": len(entries),
        "acting_on_predictions": _ACT_ON_PREDICTIONS,
        "entries": entries,
    }), 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", "9000"))
    app.run(host="0.0.0.0", port=port, threaded=True)
