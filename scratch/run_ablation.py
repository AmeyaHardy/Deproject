import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import json
import time
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, average_precision_score, confusion_matrix
)
from src.raw_loader import FEATURE_NAMES
from src.anomaly.session_split import load_raw_dataframe, assign_sessions, session_split, get_feature_matrix
from src.anomaly.preprocessing import FrozenPreprocessor
from src.anomaly.train_and_evaluate import (
    srcip_prefix_baseline, train_ttl_trio_baseline, compute_metrics, TTL_TRIO
)

SEED = 42
np.random.seed(SEED)
print(f"SEED: {SEED}")

df = load_raw_dataframe(limit=200000, verbose=False)
df = assign_sessions(df)

# Feature subsets
TTL_TRIO_COLS = ["sttl", "dttl", "ct_state_ttl"]
TCP_RTT_COLS = ["tcprtt", "synack", "ackdat"]
FEATURE_SETS = {
    "all_41": FEATURE_NAMES,
    "no_ttl": [c for c in FEATURE_NAMES if c not in TTL_TRIO_COLS],
    "no_ttl_no_rtt": [c for c in FEATURE_NAMES if c not in TTL_TRIO_COLS and c not in TCP_RTT_COLS],
}

results = {}

for (s_train, s_test) in [(0, 1), (1, 0)]:
    dir_name = f"S{s_train}->S{s_test}"
    print(f"\n=======================================================")
    print(f"Direction: {dir_name}")
    print(f"=======================================================")
    results[dir_name] = {}

    train_df, test_df = session_split(df, train_session=s_train, test_session=s_test, verbose=False)
    duration_hours = (test_df["stime"].max() - test_df["stime"].min()) / 3600.0

    # 1. Shortcut: srcip-prefix rule
    y_pred_srcip = srcip_prefix_baseline(test_df)
    m_srcip = compute_metrics(
        test_df["label"].values, y_pred_srcip,
        attack_cats=test_df["attack_cat"], session_duration_hours=duration_hours,
        model_name="srcip-prefix-rule"
    )
    results[dir_name]["srcip_prefix"] = m_srcip

    # 2. Shortcut: TTL-trio Decision Tree
    X_train_ttl = train_df[TTL_TRIO_COLS].fillna(0)
    X_test_ttl = test_df[TTL_TRIO_COLS].fillna(0)
    tree = DecisionTreeClassifier(max_depth=5, random_state=SEED)
    tree.fit(X_train_ttl, train_df["label"])
    y_pred_tree = tree.predict(X_test_ttl)
    y_score_tree = tree.predict_proba(X_test_ttl)[:, 1]
    m_tree = compute_metrics(
        test_df["label"].values, y_pred_tree, y_score_tree,
        attack_cats=test_df["attack_cat"], session_duration_hours=duration_hours,
        model_name="TTL-trio-tree"
    )
    # Distinct score levels
    distinct_scores = len(np.unique(y_score_tree))
    m_tree["distinct_score_levels"] = distinct_scores
    results[dir_name]["ttl_trio_tree"] = m_tree

    # 3. Random Forest on feature subsets
    for fset_name, fset_cols in FEATURE_SETS.items():
        print(f"  Training RF with '{fset_name}' ({len(fset_cols)} features)...")
        X_train_raw = train_df[fset_cols].copy()
        X_test_raw = test_df[fset_cols].copy()

        pp = FrozenPreprocessor()
        # Adapt preprocessor to subset of features
        X_train_proc = pp.fit_transform(X_train_raw)
        X_test_proc = pp.transform(X_test_raw)

        rf = RandomForestClassifier(
            n_estimators=200, max_depth=20, min_samples_leaf=5,
            class_weight="balanced", random_state=SEED, n_jobs=-1
        )
        t0 = time.time()
        rf.fit(X_train_proc, train_df["label"])
        train_time = time.time() - t0

        y_pred = rf.predict(X_test_proc)
        y_score = rf.predict_proba(X_test_proc)[:, 1]

        m_rf = compute_metrics(
            test_df["label"].values, y_pred, y_score,
            attack_cats=test_df["attack_cat"], session_duration_hours=duration_hours,
            model_name=f"RF-{fset_name}"
        )
        m_rf["train_time_s"] = train_time
        results[dir_name][fset_name] = m_rf

# Print formatted summary table
print("\n" + "=" * 90)
print("TTL ABLATION EXPERIMENT SUMMARY (Gate A4)")
print("=" * 90)
header = f"{'Direction':<10} | {'Model / Feature Set':<22} | {'Precision':<9} | {'Recall':<7} | {'F1':<7} | {'ROC-AUC':<8} | {'PR-AUC':<7} | {'FPR':<7} | {'Alerts/hr':<9}"
print(header)
print("-" * len(header))

for dir_name, dir_res in results.items():
    for m_key, m in dir_res.items():
        name = m.get("model", m_key)
        p = f"{m['precision']:.4f}"
        r = f"{m['recall']:.4f}"
        f1 = f"{m['f1']:.4f}"
        roc = f"{m['roc_auc']:.4f}" if m.get('roc_auc') is not None else "N/A"
        pr = f"{m['pr_auc']:.4f}" if m.get('pr_auc') is not None else "N/A"
        fpr = f"{m['fpr']:.4f}"
        aph = f"{m['alerts_per_hour']:.1f}"
        print(f"{dir_name:<10} | {name:<22} | {p:<9} | {r:<7} | {f1:<7} | {roc:<8} | {pr:<7} | {fpr:<7} | {aph:<9}")

# Save JSON results
out_path = Path("reports/anomaly_detection/ablation_results.json")
out_path.parent.mkdir(parents=True, exist_ok=True)
out_path.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
print(f"\nAblation results saved to: {out_path}")
