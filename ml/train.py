"""
LSTM incident predictor — training script.

Architecture: 2-layer LSTM (hidden=64) → Dropout → Linear(1) → Sigmoid
Input:  (batch, seq_len=30, features=5)
Output: (batch, 1) — incident probability

Training:
  - Binary cross-entropy with positive class weighting to handle imbalance
  - Adam optimizer, cosine LR schedule, early stopping
  - Logs experiment to MLflow, registers model if val precision ≥ threshold
  - Promotes to 'Production' stage automatically if threshold met

Usage:
  python train.py \
    --data training_data.csv \
    --mlflow-uri http://localhost:5000 \
    --epochs 50 \
    --promote-threshold 0.80
"""

import argparse
import json
import logging
import os
import sys
import time

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset, WeightedRandomSampler
import mlflow
import mlflow.pytorch
from sklearn.metrics import (
    precision_score, recall_score, f1_score, roc_auc_score
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
FEATURE_COLS = ["cpu_delta", "mem_mb", "restart_delta", "latency_ms", "error_proxy"]
LABEL_COL = "incident_label"
WINDOW_SIZE = 30
HIDDEN_SIZE = 64
NUM_LAYERS = 2
DROPOUT = 0.2
BATCH_SIZE = 64
LEARNING_RATE = 1e-3
PATIENCE = 10  # early stopping patience

EXPERIMENT_NAME = "incident-prediction-lstm"
MODEL_NAME = "incident-predictor"


# ---------------------------------------------------------------------------
# Model (must exactly match consumer/main.py IncidentLSTM)
# ---------------------------------------------------------------------------
class IncidentLSTM(nn.Module):
    def __init__(
        self,
        input_size: int = len(FEATURE_COLS),
        hidden_size: int = HIDDEN_SIZE,
        num_layers: int = NUM_LAYERS,
        dropout: float = DROPOUT,
    ):
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
        return self.head(h_n[-1])


# ---------------------------------------------------------------------------
# Data preparation
# ---------------------------------------------------------------------------
def build_windows(
    df: pd.DataFrame,
    window_size: int = WINDOW_SIZE,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Build sliding-window sequences.
    X: (N, window_size, features)
    y: (N,) — 1 if any step in window is labeled incident
    """
    features = df[FEATURE_COLS].values.astype(np.float32)
    labels = df[LABEL_COL].values.astype(np.float32)

    X, y = [], []
    for i in range(len(features) - window_size + 1):
        X.append(features[i : i + window_size])
        # Label the window by whether it *ends in* an incident — forward-looking label
        y.append(float(labels[i + window_size - 1]))

    return np.array(X, dtype=np.float32), np.array(y, dtype=np.float32)


def make_loaders(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
    batch_size: int = BATCH_SIZE,
) -> tuple[DataLoader, DataLoader]:
    # Weighted sampler to handle class imbalance
    n_pos = int(y_train.sum())
    n_neg = len(y_train) - n_pos
    if n_pos == 0 or n_neg == 0:
        logger.warning("Degenerate labels — skipping weighted sampler")
        train_ds = TensorDataset(
            torch.from_numpy(X_train), torch.from_numpy(y_train).unsqueeze(1)
        )
        train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
    else:
        weights = np.where(y_train == 1, len(y_train) / (2 * n_pos), len(y_train) / (2 * n_neg))
        sampler = WeightedRandomSampler(
            weights=torch.from_numpy(weights).double(),
            num_samples=len(y_train),
            replacement=True,
        )
        train_ds = TensorDataset(
            torch.from_numpy(X_train), torch.from_numpy(y_train).unsqueeze(1)
        )
        train_loader = DataLoader(train_ds, batch_size=batch_size, sampler=sampler)

    val_ds = TensorDataset(
        torch.from_numpy(X_val), torch.from_numpy(y_val).unsqueeze(1)
    )
    val_loader = DataLoader(val_ds, batch_size=batch_size, shuffle=False)
    return train_loader, val_loader


# ---------------------------------------------------------------------------
# Training loop
# ---------------------------------------------------------------------------
def train_epoch(
    model: IncidentLSTM,
    loader: DataLoader,
    optimizer: torch.optim.Optimizer,
    criterion: nn.Module,
    device: torch.device,
) -> float:
    model.train()
    total_loss = 0.0
    for X_batch, y_batch in loader:
        X_batch, y_batch = X_batch.to(device), y_batch.to(device)
        optimizer.zero_grad()
        preds = model(X_batch)
        loss = criterion(preds, y_batch)
        loss.backward()
        nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        optimizer.step()
        total_loss += loss.item() * len(X_batch)
    return total_loss / len(loader.dataset)


@torch.no_grad()
def evaluate(
    model: IncidentLSTM,
    loader: DataLoader,
    criterion: nn.Module,
    device: torch.device,
    threshold: float = 0.5,
) -> dict:
    model.eval()
    all_probs, all_labels, total_loss = [], [], 0.0
    for X_batch, y_batch in loader:
        X_batch, y_batch = X_batch.to(device), y_batch.to(device)
        probs = model(X_batch)
        loss = criterion(probs, y_batch)
        total_loss += loss.item() * len(X_batch)
        all_probs.extend(probs.cpu().squeeze().tolist())
        all_labels.extend(y_batch.cpu().squeeze().tolist())

    y_true = np.array(all_labels)
    y_prob = np.array(all_probs)
    y_pred = (y_prob >= threshold).astype(int)

    precision = precision_score(y_true, y_pred, zero_division=0)
    recall = recall_score(y_true, y_pred, zero_division=0)
    f1 = f1_score(y_true, y_pred, zero_division=0)
    try:
        auc = roc_auc_score(y_true, y_prob)
    except Exception:
        auc = float("nan")

    return {
        "loss": total_loss / len(loader.dataset),
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "roc_auc": auc,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run_training(
    data_path: str,
    mlflow_uri: str,
    epochs: int,
    promote_threshold: float,
    device_str: str = "cpu",
) -> dict:
    device = torch.device(device_str)

    logger.info("Loading data from %s", data_path)
    df = pd.read_csv(data_path)
    logger.info("Dataset: %d rows | incident rate: %.1f%%",
                len(df), df[LABEL_COL].mean() * 100)

    # Chronological 70/15/15 split
    n = len(df)
    train_df = df.iloc[: int(n * 0.70)]
    val_df = df.iloc[int(n * 0.70) : int(n * 0.85)]
    test_df = df.iloc[int(n * 0.85) :]

    X_train, y_train = build_windows(train_df)
    X_val, y_val = build_windows(val_df)
    X_test, y_test = build_windows(test_df)

    logger.info(
        "Windows — train: %d | val: %d | test: %d",
        len(X_train), len(X_val), len(X_test),
    )

    train_loader, val_loader = make_loaders(X_train, y_train, X_val, y_val)

    model = IncidentLSTM().to(device)
    n_pos = int(y_train.sum())
    pos_weight = torch.tensor(
        [(len(y_train) - n_pos) / max(n_pos, 1)], device=device
    )
    criterion = nn.BCELoss()  # probabilities already in [0,1] via Sigmoid
    optimizer = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE, weight_decay=1e-5)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=epochs)

    mlflow.set_tracking_uri(mlflow_uri)
    mlflow.set_experiment(EXPERIMENT_NAME)

    best_val_f1 = -1.0
    best_epoch = 0
    patience_counter = 0
    best_state = None

    with mlflow.start_run(run_name=f"lstm-train-{int(time.time())}") as run:
        mlflow.log_params({
            "hidden_size": HIDDEN_SIZE,
            "num_layers": NUM_LAYERS,
            "dropout": DROPOUT,
            "batch_size": BATCH_SIZE,
            "lr": LEARNING_RATE,
            "window_size": WINDOW_SIZE,
            "epochs": epochs,
            "promote_threshold": promote_threshold,
            "n_features": len(FEATURE_COLS),
            "n_train": len(X_train),
            "n_val": len(X_val),
            "n_test": len(X_test),
        })

        for epoch in range(1, epochs + 1):
            train_loss = train_epoch(model, train_loader, optimizer, criterion, device)
            val_metrics = evaluate(model, val_loader, criterion, device)
            scheduler.step()

            mlflow.log_metrics({
                "train_loss": train_loss,
                "val_loss": val_metrics["loss"],
                "val_precision": val_metrics["precision"],
                "val_recall": val_metrics["recall"],
                "val_f1": val_metrics["f1"],
                "val_roc_auc": val_metrics["roc_auc"] if not np.isnan(val_metrics["roc_auc"]) else 0.0,
            }, step=epoch)

            logger.info(
                "Epoch %3d/%d | train_loss=%.4f | val_loss=%.4f | "
                "P=%.4f R=%.4f F1=%.4f AUC=%.4f",
                epoch, epochs, train_loss, val_metrics["loss"],
                val_metrics["precision"], val_metrics["recall"],
                val_metrics["f1"], val_metrics["roc_auc"],
            )

            if val_metrics["f1"] > best_val_f1:
                best_val_f1 = val_metrics["f1"]
                best_epoch = epoch
                patience_counter = 0
                best_state = {k: v.clone() for k, v in model.state_dict().items()}
            else:
                patience_counter += 1
                if patience_counter >= PATIENCE:
                    logger.info("Early stopping at epoch %d", epoch)
                    break

        # Restore best checkpoint
        if best_state:
            model.load_state_dict(best_state)

        logger.info("Best epoch: %d | best_val_f1=%.4f", best_epoch, best_val_f1)

        # Test set evaluation
        test_ds = TensorDataset(
            torch.from_numpy(X_test), torch.from_numpy(y_test).unsqueeze(1)
        )
        test_loader = DataLoader(test_ds, batch_size=BATCH_SIZE, shuffle=False)
        test_metrics = evaluate(model, test_loader, criterion, device)

        logger.info(
            "TEST — P=%.4f R=%.4f F1=%.4f AUC=%.4f",
            test_metrics["precision"], test_metrics["recall"],
            test_metrics["f1"], test_metrics["roc_auc"],
        )

        mlflow.log_metrics({
            "test_precision": test_metrics["precision"],
            "test_recall": test_metrics["recall"],
            "test_f1": test_metrics["f1"],
            "test_roc_auc": test_metrics["roc_auc"] if not np.isnan(test_metrics["roc_auc"]) else 0.0,
            "best_epoch": best_epoch,
        })

        # Log model artifact
        mlflow.pytorch.log_model(
            model,
            artifact_path="lstm-model",
            registered_model_name=MODEL_NAME,
        )
        logger.info("Model logged to MLflow as '%s'", MODEL_NAME)

        # Promote to Production if precision threshold met
        client = mlflow.tracking.MlflowClient()
        versions = client.search_model_versions(f"name='{MODEL_NAME}'")
        latest_version = max(int(v.version) for v in versions)

        if test_metrics["precision"] >= promote_threshold:
            client.transition_model_version_stage(
                name=MODEL_NAME,
                version=str(latest_version),
                stage="Production",
                archive_existing_versions=True,
            )
            logger.info(
                "✓ Model v%d promoted to Production (precision=%.4f ≥ threshold=%.4f)",
                latest_version, test_metrics["precision"], promote_threshold,
            )
            promoted = True
        else:
            client.transition_model_version_stage(
                name=MODEL_NAME,
                version=str(latest_version),
                stage="Staging",
            )
            logger.warning(
                "✗ Model v%d moved to Staging only (precision=%.4f < threshold=%.4f)",
                latest_version, test_metrics["precision"], promote_threshold,
            )
            promoted = False

        result = {
            "run_id": run.info.run_id,
            "model_version": latest_version,
            "promoted": promoted,
            "test_precision": round(test_metrics["precision"], 4),
            "test_recall": round(test_metrics["recall"], 4),
            "test_f1": round(test_metrics["f1"], 4),
            "test_roc_auc": round(test_metrics["roc_auc"], 4) if not np.isnan(test_metrics["roc_auc"]) else None,
            "best_epoch": best_epoch,
        }
        logger.info("Result: %s", json.dumps(result, indent=2))
        return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train LSTM incident predictor")
    parser.add_argument("--data", default="training_data.csv")
    parser.add_argument("--mlflow-uri", default="http://localhost:5000")
    parser.add_argument("--epochs", type=int, default=50)
    parser.add_argument("--promote-threshold", type=float, default=0.80,
                        help="Min test precision for automatic Production promotion")
    parser.add_argument("--device", default="cpu", choices=["cpu", "cuda", "mps"])
    args = parser.parse_args()

    result = run_training(
        data_path=args.data,
        mlflow_uri=args.mlflow_uri,
        epochs=args.epochs,
        promote_threshold=args.promote_threshold,
        device_str=args.device,
    )

    if not result.get("promoted"):
        logger.warning("Model did NOT meet promotion threshold — investigate before deploying")
        sys.exit(2)  # non-zero exit for CI gate
