"""
Streaming Anomaly Detection Scorer (Gate A7)
=============================================
Simulates streaming anomaly detection scoring and measures detection latency.

Satisfies review gate A7: detection latency measurement with p50/p95
statistics and cold-start behavior analysis.

Processes records in simulated 5-second micro-batches (matching the Spark
Structured Streaming window configuration), scores each record with the
trained Random Forest model and frozen preprocessor, and measures:
  - Detection latency: alert_emit_time - first_attack_event_time (p50/p95)
  - Cold-start behavior: detection accuracy on the first N windows
  - Per-window scoring throughput and detection rate

Usage:
    python src/anomaly/streaming_scorer.py
    python src/anomaly/streaming_scorer.py --limit 100000 --session 1
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import joblib
import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.raw_loader import FEATURE_NAMES
from src.anomaly.session_split import (
    load_raw_dataframe,
    assign_sessions,
    get_feature_matrix,
)
from src.anomaly.preprocessing import FrozenPreprocessor

# Default paths for artifacts
MODELS_DIR = ROOT_DIR / "data" / "processed" / "models"
PREPROC_DIR = ROOT_DIR / "data" / "processed" / "preprocessing_artifacts"
RESULTS_DIR = ROOT_DIR / "reports" / "anomaly_detection"

# Match Spark Structured Streaming's 5-second tumbling windows
WINDOW_SECONDS = 5


def simulate_streaming_detection(
    df: pd.DataFrame,
    rf_model,
    preprocessor: FrozenPreprocessor,
    window_seconds: int = WINDOW_SECONDS,
    threshold: float = 0.5,
    verbose: bool = True,
) -> dict:
    """
    Simulate streaming scoring and measure detection latency.

    Models the real-world streaming flow:
      1. Records arrive continuously and buffer in a tumbling window.
      2. Every `window_seconds`, the window fires and all buffered records
         are preprocessed and scored in a single micro-batch.
      3. If any record scores above `threshold`, an alert is emitted.
      4. Detection latency = (window_end - first_attack_time) + processing_time.

    Args:
        df: DataFrame with all 49 raw columns + session/stime.
        rf_model: Trained sklearn RandomForestClassifier.
        preprocessor: Fitted FrozenPreprocessor.
        window_seconds: Tumbling window size (default 5s, matches Spark config).
        threshold: Classification threshold for alert emission.
        verbose: Print progress updates.

    Returns:
        Dict with latency, cold_start, and throughput statistics.
    """
    # Sort by event time to simulate temporal arrival
    df = df.sort_values("stime").reset_index(drop=True)

    # Assign window IDs based on event time offset from first record
    min_time = int(df["stime"].min())
    df = df.copy()
    df["window_id"] = ((df["stime"] - min_time) // window_seconds).astype(int)

    detection_latencies = []
    window_results = []
    missed_attack_windows = 0
    total_attack_windows = 0
    total_records_scored = 0
    total_scoring_time_ms = 0.0

    window_groups = list(df.groupby("window_id"))
    n_windows = len(window_groups)

    if verbose:
        print(f"  Simulating streaming detection over {n_windows:,} windows...")
        print(f"  Window size: {window_seconds}s | Total records: {len(df):,}")

    for i, (window_id, window_df) in enumerate(window_groups):
        # Extract features (41 behavioral columns) and labels
        X_window, y_window = get_feature_matrix(window_df)

        # ── Simulate micro-batch processing ──
        batch_start = time.perf_counter()

        # Apply frozen preprocessing (identical to training transforms)
        X_processed = preprocessor.transform(X_window)

        # Score with trained Random Forest
        y_pred = rf_model.predict(X_processed)
        y_score = rf_model.predict_proba(X_processed)[:, 1]

        batch_end = time.perf_counter()
        processing_time_ms = (batch_end - batch_start) * 1000

        total_records_scored += len(window_df)
        total_scoring_time_ms += processing_time_ms

        # ── Compute detection latency ──
        window_start_time = min_time + int(window_id) * window_seconds
        window_end_time = window_start_time + window_seconds

        attack_mask = (y_window == 1).values
        n_attacks = int(attack_mask.sum())
        n_alerts = int((y_pred == 1).sum())

        has_attacks = n_attacks > 0
        detected_any = has_attacks and bool((y_pred[attack_mask] == 1).any())

        if has_attacks:
            total_attack_windows += 1

            if detected_any:
                # Latency = time the first attack record waited in the buffer
                #         + time to process the entire batch
                first_attack_time = int(
                    window_df.loc[window_df.index[attack_mask], "stime"].min()
                )
                wait_time = window_end_time - first_attack_time
                detection_latency_s = wait_time + (processing_time_ms / 1000)
                detection_latencies.append(detection_latency_s)
            else:
                missed_attack_windows += 1

        # Track per-window results
        normal_mask = ~attack_mask
        window_results.append({
            "window_id": int(window_id),
            "n_records": len(window_df),
            "n_attacks": n_attacks,
            "n_alerts": n_alerts,
            "true_positives": int((y_pred[attack_mask] == 1).sum()) if has_attacks else 0,
            "false_negatives": int((y_pred[attack_mask] == 0).sum()) if has_attacks else 0,
            "false_positives": int((y_pred[normal_mask] == 1).sum()) if normal_mask.any() else 0,
            "processing_time_ms": round(processing_time_ms, 3),
            "detected_any_attack": detected_any,
        })

        if verbose and (i + 1) % 1000 == 0:
            print(f"    Window {i+1:,}/{n_windows:,} processed...")

    # ── Latency statistics ──
    latency_stats = {}
    if detection_latencies:
        latencies_arr = np.array(detection_latencies)
        latency_stats = {
            "p50_latency_s": round(float(np.percentile(latencies_arr, 50)), 4),
            "p95_latency_s": round(float(np.percentile(latencies_arr, 95)), 4),
            "mean_latency_s": round(float(np.mean(latencies_arr)), 4),
            "min_latency_s": round(float(np.min(latencies_arr)), 4),
            "max_latency_s": round(float(np.max(latencies_arr)), 4),
            "std_latency_s": round(float(np.std(latencies_arr)), 4),
            "n_detected_windows": len(detection_latencies),
            "n_missed_windows": missed_attack_windows,
            "n_total_attack_windows": total_attack_windows,
            "window_detection_rate": round(
                len(detection_latencies) / total_attack_windows, 4
            ) if total_attack_windows > 0 else 0.0,
        }

    # ── Cold-start analysis (first 20 windows) ──
    cold_start_n = min(20, len(window_results))
    cold_start_windows = window_results[:cold_start_n]
    cold_start_attack_windows = [w for w in cold_start_windows if w["n_attacks"] > 0]
    cold_start_detected = sum(
        1 for w in cold_start_attack_windows if w["detected_any_attack"]
    )

    cold_start_stats = {
        "n_cold_start_windows": cold_start_n,
        "n_cold_start_attack_windows": len(cold_start_attack_windows),
        "n_cold_start_detected": cold_start_detected,
        "cold_start_detection_rate": round(
            cold_start_detected / len(cold_start_attack_windows), 4
        ) if cold_start_attack_windows else None,
        "cold_start_avg_processing_ms": round(
            float(np.mean([w["processing_time_ms"] for w in cold_start_windows])), 3
        ) if cold_start_windows else None,
    }

    # ── Throughput statistics ──
    processing_times = [w["processing_time_ms"] for w in window_results]
    throughput_stats = {
        "total_records_scored": total_records_scored,
        "total_scoring_time_ms": round(total_scoring_time_ms, 2),
        "n_windows": n_windows,
        "avg_records_per_window": round(total_records_scored / n_windows, 1)
            if n_windows > 0 else 0,
        "avg_window_processing_ms": round(total_scoring_time_ms / n_windows, 3)
            if n_windows > 0 else 0,
        "p95_window_processing_ms": round(
            float(np.percentile(processing_times, 95)), 3
        ) if processing_times else 0,
        "records_per_second": round(
            total_records_scored / (total_scoring_time_ms / 1000), 1
        ) if total_scoring_time_ms > 0 else 0,
    }

    return {
        "latency": latency_stats,
        "cold_start": cold_start_stats,
        "throughput": throughput_stats,
        "window_seconds": window_seconds,
        "threshold": threshold,
    }


def run_streaming_evaluation(
    limit: Optional[int] = None,
    session: int = 1,
    verbose: bool = True,
) -> dict:
    """
    Run complete streaming evaluation pipeline for Gate A7.

    Args:
        limit: Max total records to load (None for full dataset).
        session: Which session to evaluate on (default 1 = 20.6% attack).
        verbose: Print progress.

    Returns:
        Complete results dict, also saved to JSON.
    """
    print("=" * 70)
    print("STREAMING DETECTION LATENCY EVALUATION (Gate A7)")
    print("=" * 70)

    # ── Load model and preprocessor ──
    print("\nStep 1: Loading trained model and preprocessor...")
    model_path = MODELS_DIR / "rf_detector.joblib"
    if not model_path.exists():
        raise FileNotFoundError(
            f"Trained model not found at {model_path}. "
            "Run train_and_evaluate.py first."
        )
    rf = joblib.load(model_path)
    pp = FrozenPreprocessor.load(PREPROC_DIR)
    print(f"  Model:        {model_path.name}")
    print(f"  Preprocessor: fitted on {pp.provenance.get('training_rows', '?')} rows")

    # ── Load raw data ──
    print(f"\nStep 2: Loading raw data...")
    df = load_raw_dataframe(limit=limit, verbose=verbose)
    df = assign_sessions(df)

    # Filter to evaluation session
    df_eval = df[df["session"] == session].copy().reset_index(drop=True)
    n_attacks = int((df_eval["label"] == 1).sum())
    n_total = len(df_eval)
    duration_s = int(df_eval["stime"].max() - df_eval["stime"].min()) if n_total > 0 else 0
    duration_h = duration_s / 3600

    print(f"  Session {session}: {n_total:,} records, "
          f"{n_attacks:,} attacks ({n_attacks/n_total*100:.1f}%), "
          f"duration {duration_h:.1f}h")

    # ── Run streaming simulation ──
    print(f"\nStep 3: Simulating streaming detection...")
    t0 = time.time()
    results = simulate_streaming_detection(
        df_eval, rf, pp,
        window_seconds=WINDOW_SECONDS,
        verbose=verbose,
    )
    sim_time = time.time() - t0
    results["simulation_wall_time_s"] = round(sim_time, 2)
    results["evaluation_session"] = session
    results["evaluation_records"] = n_total
    results["evaluation_attacks"] = n_attacks
    results["evaluation_duration_h"] = round(duration_h, 2)

    # ── Print results ──
    print(f"\n{'=' * 70}")
    print("STREAMING DETECTION RESULTS (Gate A7)")
    print(f"{'=' * 70}")

    lat = results.get("latency", {})
    if lat:
        print(f"\n  Detection Latency:")
        print(f"    p50 (median):  {lat['p50_latency_s']:.4f} s")
        print(f"    p95:           {lat['p95_latency_s']:.4f} s")
        print(f"    mean:          {lat['mean_latency_s']:.4f} s")
        print(f"    min:           {lat['min_latency_s']:.4f} s")
        print(f"    max:           {lat['max_latency_s']:.4f} s")
        print(f"    Windows with attacks: {lat['n_total_attack_windows']:,}")
        print(f"    Detected:            {lat['n_detected_windows']:,}")
        print(f"    Missed:              {lat['n_missed_windows']:,}")
        print(f"    Window detect rate:  {lat['window_detection_rate']:.4f}")
    else:
        print("\n  No attack windows found in evaluation data.")

    cs = results.get("cold_start", {})
    if cs:
        print(f"\n  Cold-Start Behavior (first {cs['n_cold_start_windows']} windows):")
        if cs.get("cold_start_detection_rate") is not None:
            print(f"    Attack windows in cold-start: {cs['n_cold_start_attack_windows']}")
            print(f"    Detected in cold-start:       {cs['n_cold_start_detected']}")
            print(f"    Cold-start detection rate:     {cs['cold_start_detection_rate']:.4f}")
        else:
            print(f"    No attack windows in cold-start period")
        if cs.get("cold_start_avg_processing_ms") is not None:
            print(f"    Cold-start avg processing:    {cs['cold_start_avg_processing_ms']:.1f} ms")

    tp = results.get("throughput", {})
    if tp:
        print(f"\n  Throughput:")
        print(f"    Total records scored:  {tp['total_records_scored']:,}")
        print(f"    Scoring speed:         {tp['records_per_second']:,.0f} records/s")
        print(f"    Avg window processing: {tp['avg_window_processing_ms']:.1f} ms")
        print(f"    P95 window processing: {tp['p95_window_processing_ms']:.1f} ms")
        print(f"    Avg records/window:    {tp['avg_records_per_window']:.0f}")

    print(f"\n  Simulation wall time: {sim_time:.1f}s")

    # ── Save results ──
    results_path = RESULTS_DIR / "streaming_detection_results.json"
    results_path.parent.mkdir(parents=True, exist_ok=True)
    results_path.write_text(
        json.dumps(results, indent=2, default=str),
        encoding="utf-8",
    )
    print(f"  Results saved to: {results_path}")

    return results


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Evaluate streaming anomaly detection latency (Gate A7)"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Limit total records loaded (None for full dataset).",
    )
    parser.add_argument(
        "--session", type=int, default=1, choices=[0, 1],
        help="Session to evaluate (default: 1, the 20.6%%-attack session).",
    )
    args = parser.parse_args()

    run_streaming_evaluation(limit=args.limit, session=args.session)


if __name__ == "__main__":
    main()
