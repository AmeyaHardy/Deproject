"""
Window-Level Evaluation (Task 5)
================================
Evaluates 5-second tumbling window false alarm rate on normal-only windows.
Compares decision rules: alert if >= k flagged flows (k=1, 3, 5).
Evaluates on:
 - Session 0 (low attack rate: ~2-3%)
 - Session 1 (high attack rate: ~22%)
 - Combined (all non-empty windows)
"""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import json
import joblib
import numpy as np
import pandas as pd
from src.raw_loader import FEATURE_NAMES, clean_attack_cat
from src.anomaly.session_split import assign_sessions, get_feature_matrix, load_raw_dataframe
from src.anomaly.preprocessing import FrozenPreprocessor

SEED = 42
np.random.seed(SEED)
print(f"SEED: {SEED}")

# Load models
MODELS_DIR = ROOT / "data" / "processed" / "models"
PREPROC_DIR = ROOT / "data" / "processed" / "preprocessing_artifacts"

rf_model = joblib.load(MODELS_DIR / "rf_detector.joblib")
preprocessor = FrozenPreprocessor.load(PREPROC_DIR)

print(f"Loaded RF model and FrozenPreprocessor from {MODELS_DIR}")

# Load raw data
print("Loading data (limit=200000)...")
df_raw = load_raw_dataframe(limit=200000, verbose=False)
df_raw = assign_sessions(df_raw)

# Preprocessing & Scoring
X_raw, y_true = get_feature_matrix(df_raw)
X_proc = preprocessor.transform(X_raw)
y_scores = rf_model.predict_proba(X_proc)[:, 1]
y_flagged = (y_scores >= 0.5).astype(int)

df_raw["is_attack"] = y_true.values
df_raw["is_flagged"] = y_flagged

WINDOW_SECONDS = 5

def evaluate_subset(df, subset_name):
    min_time = int(df["stime"].min())
    df = df.copy()
    df["window_id"] = ((df["stime"] - min_time) // WINDOW_SECONDS).astype(int)

    window_stats = df.groupby("window_id").agg(
        n_flows=("label", "count"),
        n_attacks=("is_attack", "sum"),
        n_normal=("is_attack", lambda s: (s == 0).sum()),
        n_flagged=("is_flagged", "sum"),
    ).reset_index()

    n_total = len(window_stats)
    pure_normal = window_stats[window_stats["n_attacks"] == 0]
    pure_attack = window_stats[window_stats["n_normal"] == 0]
    mixed = window_stats[(window_stats["n_attacks"] > 0) & (window_stats["n_normal"] > 0)]
    any_attack = window_stats[window_stats["n_attacks"] > 0]

    pct_mixed = len(mixed) / n_total * 100 if n_total > 0 else 0
    pct_pure_norm = len(pure_normal) / n_total * 100 if n_total > 0 else 0
    pct_pure_atk = len(pure_attack) / n_total * 100 if n_total > 0 else 0

    print(f"\n--- {subset_name} ---")
    print(f"Total non-empty windows: {n_total:,}")
    print(f"  Pure Normal (0 attacks): {len(pure_normal):,} ({pct_pure_norm:.2f}%)")
    print(f"  Mixed (norm + attack):   {len(mixed):,} ({pct_mixed:.2f}%)")
    print(f"  Pure Attack:             {len(pure_attack):,} ({pct_pure_atk:.2f}%)")

    k_results = {}
    n_norm = len(pure_normal)
    n_atk = len(any_attack)

    for k in [1, 3, 5]:
        fa = (pure_normal["n_flagged"] >= k).sum() if n_norm > 0 else 0
        far = fa / n_norm if n_norm > 0 else 0.0
        det = (any_attack["n_flagged"] >= k).sum() if n_atk > 0 else 0
        dr = det / n_atk if n_atk > 0 else 0.0
        print(f"  k = {k:2d}: Normal False Alarms = {fa:4d}/{n_norm:4d} (FAR: {far*100:6.2f}%) | Attack Det = {det:4d}/{n_atk:4d} ({dr*100:6.2f}%)")
        k_results[f"k_{k}"] = {
            "k": k,
            "false_alarms": int(fa),
            "normal_windows": int(n_norm),
            "window_far": float(far),
            "detected_attack_windows": int(det),
            "attack_windows": int(n_atk),
            "detection_rate": float(dr),
        }

    return {
        "total_windows": n_total,
        "pure_normal_windows": len(pure_normal),
        "mixed_windows": len(mixed),
        "pure_attack_windows": len(pure_attack),
        "any_attack_windows": len(any_attack),
        "mixed_window_pct": float(pct_mixed),
        "k_evaluations": k_results,
    }

all_res = {
    "window_seconds": WINDOW_SECONDS,
    "flow_to_window_rule": "window_id = ((stime - min_time) // 5)",
    "window_label_definition": "A window is positive (attack) if sum(label == 1) > 0; a window is pure normal if sum(label == 1) == 0.",
    "subsets": {}
}

s0_df = df_raw[df_raw["session"] == 0].sort_values("stime").reset_index(drop=True)
s1_df = df_raw[df_raw["session"] == 1].sort_values("stime").reset_index(drop=True)

all_res["subsets"]["Session_0"] = evaluate_subset(s0_df, "Session 0 (Jan 22-23)")
all_res["subsets"]["Session_1"] = evaluate_subset(s1_df, "Session 1 (Feb 18)")
all_res["subsets"]["Combined"]  = evaluate_subset(df_raw, "Combined (Session 0 + Session 1)")

out_file = ROOT / "reports" / "anomaly_detection" / "window_eval_results.json"
out_file.write_text(json.dumps(all_res, indent=2), encoding="utf-8")
print(f"\nSaved to: {out_file}")
