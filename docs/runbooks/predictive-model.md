# Runbook: Predictive Incident Model — Failure & Drift Scenarios

**Alert source**: LSTM predictor consumer, MLflow drift check, GitHub Actions  
**Audience**: Platform SRE  
**Related docs**: [ARCHITECTURE.md](../ARCHITECTURE.md), [SOP.md](../SOP.md)

---

## Table of Contents

1. [Predictor Consumer Down](#1-predictor-consumer-down)
2. [No Model Loaded — MLflow Unreachable](#2-no-model-loaded--mlflow-unreachable)
3. [Feature Drift Detected](#3-feature-drift-detected)
4. [False Positive Storm (Too Many Predictions)](#4-false-positive-storm)
5. [Model Precision Degradation](#5-model-precision-degradation)
6. [Kafka Topic Lag Growing](#6-kafka-topic-lag-growing)
7. [Enable Prediction-Based Remediation](#7-enable-prediction-based-remediation)

---

## 1. Predictor Consumer Down

**Symptom**: `predictor-consumer` pod is CrashLoopBackOff or not running.  
**Impact**: Predictions stop; reactive Alertmanager path continues unaffected.

```bash
# Check pod status
kubectl get pods -n monitoring -l app=predictor-consumer

# View crash logs
kubectl logs -n monitoring deploy/predictor-consumer --previous

# Common causes:
# - Kafka broker not reachable (check KAFKA_BOOTSTRAP_SERVERS env var)
# - MLflow tracking server down (model can't be loaded at startup)
# - OOM (bump memory limit if torch model is too large)

# Check Kafka connectivity from inside cluster
kubectl run kafka-test --rm -it --restart=Never \
  --image=apache/kafka:3.7.0 \
  -- /opt/kafka/bin/kafka-broker-api-versions.sh \
     --bootstrap-server kafka.monitoring.svc.cluster.local:9092

# Restart the consumer
kubectl rollout restart deployment/predictor-consumer -n monitoring
kubectl rollout status deployment/predictor-consumer -n monitoring
```

**Resolution**: Fix the root cause (Kafka/MLflow connectivity), then restart.  
**Note**: The reactive remediation bot is independent — no action needed to maintain SLA.

---

## 2. No Model Loaded — MLflow Unreachable

**Symptom**: `/health` endpoint returns `model_loaded: false`; consumer runs but never predicts.

```bash
# Check health endpoint
kubectl port-forward svc/predictor-consumer-service 8081:8081 -n monitoring
curl http://localhost:8081/health | jq .

# Check MLflow reachability from consumer pod
kubectl exec -n monitoring deploy/predictor-consumer -- \
  wget -qO- http://mlflow.monitoring.svc.cluster.local:5000/health

# Check MLflow pod
kubectl get pods -n monitoring -l app=mlflow
kubectl logs -n monitoring deploy/mlflow

# If MLflow PVC is full (SQLite DB)
kubectl exec -n monitoring deploy/mlflow -- df -h /mlflow

# Restart MLflow
kubectl rollout restart deployment/mlflow -n monitoring
```

**If no Production model exists** (fresh cluster, first deploy):
```bash
# Run training locally against in-cluster MLflow
make port-mlflow   # -> http://localhost:5000

# In a separate terminal:
make train-model-synthetic
# OR with real Prometheus data:
make port-prometheus  # -> http://localhost:9090
cd ml && python data_generator.py \
  --prometheus-url http://localhost:9090 \
  --hours 72 \
  --output training_data.csv
python train.py --data training_data.csv --mlflow-uri http://localhost:5000
```

---

## 3. Feature Drift Detected

**Trigger**: GitHub Actions `retrain.yml` opened an issue labelled `drift-alert`.  
**Symptom**: MLflow model version tagged `drift_detected=True`, `promotion_blocked=true` on Staging versions.

### Investigate

```bash
# Download the drift report from the GitHub Actions artifacts
# (see the issue for the Run ID link)

# Or run drift check locally:
make port-prometheus   # -> http://localhost:9090
make port-mlflow       # -> http://localhost:5000

cd ml
# Get current week's data
python data_generator.py \
  --prometheus-url http://localhost:9090 \
  --hours 168 \
  --output current_data.csv

python drift_check.py \
  --reference training_data.csv \
  --current current_data.csv \
  --mlflow-uri http://localhost:5000 \
  --output drift_report.json

cat drift_report.json | python -m json.tool
```

### Common Causes & Fixes

| Cause | Indicator | Fix |
|---|---|---|
| New workload added to `app` namespace | cpu_delta PSI > 0.20 | Retrain on data including new workload |
| Infrastructure change (node class, limits) | mem_mb distribution shifted | Retrain with post-change data |
| Prometheus scrape config changed | Multiple features drifted | Verify PromQL queries in `data_generator.py` still return data |
| Seasonal traffic pattern | Latency features shifted at specific hours | Collect longer history window; retrain |

### Retrain with fresh data

```bash
cd ml
python data_generator.py \
  --prometheus-url http://localhost:9090 \
  --hours 336 \
  --output fresh_training_data.csv

python train.py \
  --data fresh_training_data.csv \
  --mlflow-uri http://localhost:5000 \
  --epochs 50 \
  --promote-threshold 0.80

# Verify drift clears after retrain
python drift_check.py \
  --reference fresh_training_data.csv \
  --current current_data.csv \
  --mlflow-uri http://localhost:5000 \
  --output drift_report_post_retrain.json
```

---

## 4. False Positive Storm

**Symptom**: `/predictions` endpoint shows many events in a short window; no actual incidents occur.

```bash
# Check prediction log
make port-bot  # -> http://localhost:9000
curl http://localhost:9000/predictions | jq '.entries | length'
curl http://localhost:9000/predictions | jq '[.entries[] | .confidence] | add / length'
```

### Mitigations (in order of severity)

**1. Raise the confidence threshold** (no restart required — set env var):
```bash
# Temporarily raise threshold to 0.90
kubectl set env deployment/predictor-consumer \
  -n monitoring \
  CONFIDENCE_THRESHOLD=0.90

# Monitor prediction rate
kubectl logs -f deploy/predictor-consumer -n monitoring | grep 'confidence'
```

**2. Increase the minimum window fill before inference**:
```bash
# Require 45 steps (7.5 min) instead of 30 (5 min)
kubectl set env deployment/predictor-consumer \
  -n monitoring \
  WINDOW_SIZE=45
```

**3. Switch back to predict-only mode** (if actions were enabled):
```bash
kubectl set env deployment/predictor-consumer \
  -n monitoring \
  PREDICTOR_ACT_ON_PREDICTIONS=false
kubectl set env deployment/remediation-bot \
  -n monitoring \
  PREDICTOR_ACT_ON_PREDICTIONS=false
```

**4. Retrain with adjusted class weights** — if false positives are systematic,
the model's threshold calibration needs adjustment. Re-run `train.py` with
`--promote-threshold 0.85` to enforce tighter precision requirements.

---

## 5. Model Precision Degradation

**Symptom**: Predictions fire but incidents don't follow; precision measured retroactively is low.

### Measure precision manually

```bash
# Compare prediction timestamps with actual alert firings
# 1. Export predictions log
make port-bot
curl http://localhost:9000/predictions | jq '.entries' > predictions.json

# 2. Export alert history from Prometheus
make port-prometheus
curl 'http://localhost:9090/api/v1/query_range?query=ALERTS{alertstate="firing"}&start=<START>&end=<END>&step=60' \
  | jq '.data.result' > alerts.json

# 3. Cross-reference: predictions within 5 min of an alert = true positive
python3 -c "
import json, datetime

with open('predictions.json') as f:
    preds = json.load(f)
with open('alerts.json') as f:
    alerts = json.load(f)

# Build set of alert timestamps
alert_ts = set()
for series in alerts:
    for ts, val in series['values']:
        if val == '1':
            alert_ts.add(float(ts))

tp, fp = 0, 0
for p in preds:
    pts = datetime.datetime.fromisoformat(p['ts']).timestamp()
    if any(pts < at <= pts + 300 for at in alert_ts):
        tp += 1
    else:
        fp += 1

print(f'TP={tp} FP={fp} Precision={tp/(tp+fp) if tp+fp>0 else 0:.3f}')
"
```

### Fix

If precision < 0.70 for two consecutive weeks, force a retrain and adjust threshold:
```bash
# Retrain with stricter promotion gate
cd ml && python train.py \
  --data training_data.csv \
  --promote-threshold 0.85  # stricter than default 0.80
```

---

## 6. Kafka Topic Lag Growing

**Symptom**: Consumer can't keep up with producer; windows are stale; predictions delayed.

```bash
# Check consumer group lag (from outside cluster via port-forward)
# Or exec into Kafka pod:
kubectl exec -n monitoring kafka-0 -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group predictor-consumer \
  --describe

# Expected: LAG should be < 100 for healthy operation
# If LAG > 10,000: consumer is stuck or too slow
```

**Resolutions**:

```bash
# Option 1: Scale consumer replicas (each replica handles different partitions)
kubectl scale deployment/predictor-consumer -n monitoring --replicas=3

# Option 2: Reduce producer throughput (tighten remote_write filter in prometheus-values.yaml)
# Edit helm/prometheus-values.yaml → remoteWrite.queueConfig.maxSamplesPerSend: 200
helm upgrade kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --values helm/prometheus-values.yaml \
  --reuse-values

# Option 3: Reset consumer offset to latest (discard backlog)
kubectl exec -n monitoring kafka-0 -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group predictor-consumer \
  --topic raw-metrics \
  --reset-offsets --to-latest --execute
```

---

## 7. Enable Prediction-Based Remediation

> [!WARNING]
> Only enable this after validating precision ≥ 0.80 on at least 2 weeks of real data.
> Start with a low-stakes namespace/deployment first.

```bash
# Enable on the remediation bot
kubectl set env deployment/remediation-bot \
  -n monitoring \
  PREDICTOR_ACT_ON_PREDICTIONS=true

# Enable on the consumer (controls what it reports in metrics)
kubectl set env deployment/predictor-consumer \
  -n monitoring \
  PREDICTOR_ACT_ON_PREDICTIONS=true

# Monitor the prediction action log
make port-bot
watch -n5 'curl -s http://localhost:9000/predictions | jq "{total: .count, acting: .acting_on_predictions}"'

# Watch for any unexpected k8s actions
kubectl get events -n app --sort-by=.lastTimestamp -w

# To roll back immediately:
kubectl set env deployment/remediation-bot \
  -n monitoring \
  PREDICTOR_ACT_ON_PREDICTIONS=false
kubectl set env deployment/predictor-consumer \
  -n monitoring \
  PREDICTOR_ACT_ON_PREDICTIONS=false
```

---

## Quick Reference

| Situation | Command |
|---|---|
| Check predictor health | `curl http://localhost:8081/health` (after `make port-forward`) |
| View recent predictions | `curl http://localhost:9000/predictions \| jq .entries[-10:]` |
| Tail consumer logs | `make logs-consumer` |
| Retrain (synthetic) | `make train-model-synthetic` |
| Open MLflow UI | `make port-mlflow` |
| Check drift | `make check-drift` |
| Raise confidence threshold | `kubectl set env deploy/predictor-consumer -n monitoring CONFIDENCE_THRESHOLD=0.90` |
| Disable prediction actions | `kubectl set env deploy/remediation-bot -n monitoring PREDICTOR_ACT_ON_PREDICTIONS=false` |
