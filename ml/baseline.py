"""
Isolation Forest baseline anomaly detector.

Provides a simple comparison baseline against the LSTM model.
Outputs precision, recall, F1, and lead-time statistics.

Usage:
  python baseline.py --data training_data.csv --output baseline_metrics.json
"""

import argparse
import json
import logging
import warnings

import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn
from sklearn.ensemble import IsolationForest
from sklearn.metrics import (
    classification_report,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
)
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FEATURE_COLS = ["cpu_delta", "mem_mb", "restart_delta", "latency_ms", "error_proxy"]
LABEL_COL = "incident_label"
WINDOW_SIZE = 30
CONTAMINATION = 0.1  # estimated fraction of anomalies

MLFLOW_TRACKING_URI = "http://localhost:5000"
EXPERIMENT_NAME = "incident-prediction-baseline"


def build_windows(df: pd.DataFrame, window_size: int) -> tuple[np.ndarray, np.ndarray]:
    """
    Build sliding windows from the flat feature DataFrame.
    Each window collapses to its mean feature vector (simple aggregation).

    Returns X: (N, features), y: (N,) labels (1 if any step in window is incident)
    """
    features = df[FEATURE_COLS].values.astype(np.float32)
    labels = df[LABEL_COL].values.astype(int)

    X, y = [], []
    for i in range(len(features) - window_size + 1):
        window = features[i : i + window_size]
        X.append(window.mean(axis=0))
        y.append(int(labels[i : i + window_size].any()))

    return np.array(X), np.array(y)


def train_and_evaluate(data_path: str, output_path: str) -> dict:
    logger.info("Loading data from %s", data_path)
    df = pd.read_csv(data_path)

    if LABEL_COL not in df.columns:
        raise ValueError(f"Missing column '{LABEL_COL}' in dataset")

    # Train/test split (80/20 chronological)
    split = int(len(df) * 0.8)
    train_df, test_df = df.iloc[:split], df.iloc[split:]

    logger.info("Train size: %d rows | Test size: %d rows", len(train_df), len(test_df))

    X_train, y_train = build_windows(train_df, WINDOW_SIZE)
    X_test, y_test = build_windows(test_df, WINDOW_SIZE)

    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s = scaler.transform(X_test)

    logger.info(
        "Training IsolationForest (contamination=%.2f, n_estimators=200)…",
        CONTAMINATION,
    )
    iso = IsolationForest(
        n_estimators=200,
        contamination=CONTAMINATION,
        max_samples="auto",
        random_state=42,
        n_jobs=-1,
    )
    iso.fit(X_train_s)

    # IsolationForest returns -1 (anomaly) or +1 (normal)
    raw_preds = iso.predict(X_test_s)
    y_pred = np.where(raw_preds == -1, 1, 0)
    anomaly_scores = -iso.score_samples(X_test_s)  # higher = more anomalous

    precision = precision_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)
    f1 = f1_score(y_test, y_pred, zero_division=0)
    try:
        auc = roc_auc_score(y_test, anomaly_scores)
    except Exception:
        auc = float("nan")

    report = classification_report(y_test, y_pred, target_names=["normal", "incident"])
    logger.info("\n%s", report)
    logger.info("Precision: %.4f | Recall: %.4f | F1: %.4f | ROC-AUC: %.4f",
                precision, recall, f1, auc)

    metrics = {
        "model": "IsolationForest",
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "roc_auc": round(auc, 4) if not np.isnan(auc) else None,
        "n_train_windows": len(X_train),
        "n_test_windows": len(X_test),
        "contamination": CONTAMINATION,
        "window_size": WINDOW_SIZE,
    }

    # Log to MLflow
    try:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment(EXPERIMENT_NAME)
        with mlflow.start_run(run_name="isolation-forest-baseline"):
            mlflow.log_params({
                "contamination": CONTAMINATION,
                "n_estimators": 200,
                "window_size": WINDOW_SIZE,
            })
            mlflow.log_metrics({
                "precision": precision,
                "recall": recall,
                "f1": f1,
                "roc_auc": auc if not np.isnan(auc) else 0.0,
            })
            mlflow.sklearn.log_model(iso, "isolation-forest")
            logger.info("Logged to MLflow experiment '%s'", EXPERIMENT_NAME)
    except Exception as exc:
        logger.warning("MLflow logging failed (non-fatal): %s", exc)

    with open(output_path, "w") as f:
        json.dump(metrics, f, indent=2)

    logger.info("Metrics written to %s", output_path)
    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train Isolation Forest baseline")
    parser.add_argument("--data", default="training_data.csv")
    parser.add_argument("--output", default="baseline_metrics.json")
    args = parser.parse_args()

    m = train_and_evaluate(args.data, args.output)
    print(json.dumps(m, indent=2))
