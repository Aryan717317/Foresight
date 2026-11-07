"""
Feature drift detection using statistical tests.

Compares the feature distribution of the current week's inference data
against the training distribution. Uses:
  - Kolmogorov-Smirnov (KS) test per feature
  - Population Stability Index (PSI) as an aggregate drift score

If drift is detected:
  - Tags the latest MLflow model version with drift=True
  - Blocks auto-promotion of any pending Staging models
  - Exits with code 1 so CI can open an alert/issue

Usage:
  python drift_check.py \
    --reference training_data.csv \
    --current current_week_data.csv \
    --mlflow-uri http://localhost:5000 \
    --output drift_report.json
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import scipy.stats as stats
import mlflow

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FEATURE_COLS = ["cpu_delta", "mem_mb", "restart_delta", "latency_ms", "error_proxy"]
MODEL_NAME = "incident-predictor"

# Thresholds
KS_P_VALUE_THRESHOLD = 0.05      # reject null (distributions same) if p < 0.05
PSI_DRIFT_THRESHOLD = 0.20       # PSI > 0.2 = significant drift
PSI_WARNING_THRESHOLD = 0.10     # PSI 0.1–0.2 = moderate drift (warn but don't block)
N_BINS = 10


def compute_psi(reference: np.ndarray, current: np.ndarray, n_bins: int = N_BINS) -> float:
    """
    Population Stability Index.
      PSI < 0.10 : no significant change
      PSI 0.10–0.20 : moderate change
      PSI > 0.20 : significant change
    """
    # Build bins from reference distribution
    bin_edges = np.percentile(reference, np.linspace(0, 100, n_bins + 1))
    bin_edges = np.unique(bin_edges)  # remove duplicates at edge extremes

    if len(bin_edges) < 2:
        return 0.0

    ref_counts, _ = np.histogram(reference, bins=bin_edges)
    cur_counts, _ = np.histogram(current, bins=bin_edges)

    ref_pct = (ref_counts + 1e-8) / len(reference)
    cur_pct = (cur_counts + 1e-8) / len(current)

    psi = np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct))
    return float(psi)


def check_drift(
    reference_df: pd.DataFrame,
    current_df: pd.DataFrame,
    mlflow_uri: str,
    output_path: str,
) -> dict:
    """
    Run KS test and PSI for each feature.
    Returns a report dict and tags MLflow model if drift detected.
    """
    mlflow.set_tracking_uri(mlflow_uri)

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "n_reference": len(reference_df),
        "n_current": len(current_df),
        "features": {},
        "overall_drift": False,
        "overall_psi": 0.0,
        "ks_drift_features": [],
        "psi_drift_features": [],
    }

    all_psi = []

    for feat in FEATURE_COLS:
        if feat not in reference_df.columns or feat not in current_df.columns:
            logger.warning("Feature '%s' missing from data — skipping", feat)
            continue

        ref_vals = reference_df[feat].dropna().values.astype(float)
        cur_vals = current_df[feat].dropna().values.astype(float)

        if len(ref_vals) < 10 or len(cur_vals) < 10:
            logger.warning("Too few samples for feature '%s' — skipping", feat)
            continue

        ks_stat, ks_p = stats.ks_2samp(ref_vals, cur_vals)
        psi = compute_psi(ref_vals, cur_vals)
        ks_drift = ks_p < KS_P_VALUE_THRESHOLD
        psi_drift = psi > PSI_DRIFT_THRESHOLD

        all_psi.append(psi)

        report["features"][feat] = {
            "ks_statistic": round(ks_stat, 6),
            "ks_p_value": round(ks_p, 6),
            "ks_drift_detected": ks_drift,
            "psi": round(psi, 6),
            "psi_drift_detected": psi_drift,
        }

        if ks_drift:
            report["ks_drift_features"].append(feat)
        if psi_drift:
            report["psi_drift_features"].append(feat)

        logger.info(
            "%-20s KS=%.4f (p=%.4f, drift=%s) | PSI=%.4f (drift=%s)",
            feat, ks_stat, ks_p, ks_drift, psi, psi_drift,
        )

    overall_psi = float(np.mean(all_psi)) if all_psi else 0.0
    drift_detected = (
        len(report["psi_drift_features"]) > 0 or
        len(report["ks_drift_features"]) >= len(FEATURE_COLS) // 2
    )

    report["overall_psi"] = round(overall_psi, 6)
    report["overall_drift"] = drift_detected

    # Tag MLflow model version with drift status
    try:
        client = mlflow.tracking.MlflowClient()

        # Tag the Production model with drift results
        prod_versions = client.get_latest_versions(MODEL_NAME, stages=["Production"])
        for mv in prod_versions:
            client.set_model_version_tag(mv.name, mv.version, "drift_detected", str(drift_detected))
            client.set_model_version_tag(mv.name, mv.version, "drift_psi", f"{overall_psi:.4f}")
            client.set_model_version_tag(mv.name, mv.version, "drift_checked_at", report["timestamp"])
            logger.info(
                "Tagged Production model v%s: drift_detected=%s psi=%.4f",
                mv.version, drift_detected, overall_psi,
            )

        # Block Staging → Production promotion if drift detected
        if drift_detected:
            staging_versions = client.get_latest_versions(MODEL_NAME, stages=["Staging"])
            for mv in staging_versions:
                client.set_model_version_tag(mv.name, mv.version, "promotion_blocked", "true")
                client.set_model_version_tag(mv.name, mv.version, "block_reason", "drift_detected")
                logger.warning(
                    "BLOCKED promotion of Staging model v%s due to drift (PSI=%.4f)",
                    mv.version, overall_psi,
                )

        # Log as MLflow run for history
        mlflow.set_experiment("drift-detection")
        with mlflow.start_run(run_name=f"drift-check-{report['timestamp'][:10]}"):
            mlflow.log_metrics({f"psi_{f}": report["features"].get(f, {}).get("psi", 0.0)
                                for f in FEATURE_COLS})
            mlflow.log_metric("overall_psi", overall_psi)
            mlflow.log_param("drift_detected", drift_detected)
            mlflow.log_param("n_reference", len(reference_df))
            mlflow.log_param("n_current", len(current_df))

    except Exception as exc:
        logger.warning("MLflow tagging failed (non-fatal): %s", exc)

    # Write report
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    logger.info(
        "Drift check complete — overall_psi=%.4f drift_detected=%s",
        overall_psi, drift_detected,
    )
    logger.info("Report written to %s", output_path)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Feature drift detection")
    parser.add_argument("--reference", default="training_data.csv",
                        help="Reference dataset (training distribution)")
    parser.add_argument("--current", required=True,
                        help="Current week's feature data CSV")
    parser.add_argument("--mlflow-uri", default="http://localhost:5000")
    parser.add_argument("--output", default="drift_report.json")
    args = parser.parse_args()

    ref_df = pd.read_csv(args.reference)
    cur_df = pd.read_csv(args.current)

    report = check_drift(ref_df, cur_df, args.mlflow_uri, args.output)
    print(json.dumps(report, indent=2))

    if report["overall_drift"]:
        logger.error(
            "DRIFT DETECTED — PSI=%.4f | drifted features: %s",
            report["overall_psi"],
            report["psi_drift_features"] + report["ks_drift_features"],
        )
        sys.exit(1)  # CI gate: fail the step → triggers alert/issue
