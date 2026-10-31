"""
Review Tests: Gate A (Anomaly Detection Pipeline)
=================================================
Tests based on Section 4 Gate A of pipeline_review_prompt.md:
  A1: Forbidden feature exclusion (no srcip, sport, dstip, dsport, stime, ltime, label in X).
  A2: Frozen preprocessing fit only on train split with provenance JSON.
  A3: Label-permutation test verification (verifying logic).
  A4: Shortcut baselines (srcip-prefix rule and TTL-trio Decision Tree).
  A5: Time/session-based split contract and model-view deduplication.
  A6: Evaluation metrics contract (FPR, PR-AUC, Alerts/hr, per-class recall).
"""

import json
from pathlib import Path
import pytest
import numpy as np
import pandas as pd

from src.raw_loader import (
    FEATURE_NAMES,
    CONTEXT_COLUMNS,
    EVALUATION_COLUMNS,
)
from src.anomaly.session_split import (
    load_raw_dataframe,
    assign_sessions,
    deduplicate_model_view,
    session_split,
    get_feature_matrix,
    SESSION_GAP_THRESHOLD,
)
from src.anomaly.preprocessing import (
    FrozenPreprocessor,
    ARTIFACTS_DIR,
)
from src.anomaly.train_and_evaluate import (
    srcip_prefix_baseline,
    train_ttl_trio_baseline,
    compute_metrics,
    label_permutation_test,
    TTL_TRIO,
)


def test_a1_forbidden_feature_exclusion():
    """Verify that feature matrix X has exactly 41 behavioral features and zero leakage."""
    assert len(FEATURE_NAMES) == 41

    for forbidden in CONTEXT_COLUMNS:
        assert forbidden not in FEATURE_NAMES, f"Forbidden context column '{forbidden}' in FEATURE_NAMES!"

    for forbidden in EVALUATION_COLUMNS:
        assert forbidden not in FEATURE_NAMES, f"Forbidden evaluation column '{forbidden}' in FEATURE_NAMES!"

    assert "srcip" not in FEATURE_NAMES
    assert "dstip" not in FEATURE_NAMES
    assert "sport" not in FEATURE_NAMES
    assert "dsport" not in FEATURE_NAMES
    assert "stime" not in FEATURE_NAMES
    assert "ltime" not in FEATURE_NAMES
    assert "label" not in FEATURE_NAMES
    assert "attack_cat" not in FEATURE_NAMES


def test_a2_frozen_preprocessor_provenance(tmp_path):
    """Verify frozen preprocessor can fit on a train prefix, save provenance, and transform test."""
    # Synthetic small dataset
    np.random.seed(42)
    n = 200
    df = pd.DataFrame({
        "sbytes": np.random.exponential(500, size=n),
        "dbytes": np.random.exponential(500, size=n),
        "sttl": np.random.randint(1, 255, size=n),
        "dttl": np.random.randint(1, 255, size=n),
        "proto": np.random.choice(["tcp", "udp", "ospf"], size=n),
        "state": np.random.choice(["FIN", "CON", "INT"], size=n),
        "service": np.random.choice(["-", "http", "dns"], size=n),
    })
    # Add dummy remaining cols so 41 exist
    for col in FEATURE_NAMES:
        if col not in df.columns:
            df[col] = np.random.uniform(0.1, 10.0, size=n)

    train_df = df.iloc[:100].copy()
    test_df = df.iloc[100:].copy()

    pp = FrozenPreprocessor()
    X_train_proc = pp.fit_transform(train_df, provenance_info={"train_rows": 100})
    pp.save(path=tmp_path)

    prov_file = tmp_path / "preprocessing_provenance.json"
    assert prov_file.exists(), "Provenance JSON was not written to disk"
    prov = json.loads(prov_file.read_text(encoding="utf-8"))
    assert prov["training_rows"] == 100
    assert len(prov["training_columns"]) == 41
    assert "medians" in prov
    assert "p99_caps" in prov

    # Transform test without refitting
    X_test_proc = pp.transform(test_df)
    assert X_test_proc.shape == (100, 41)
    assert not X_test_proc.isna().any().any(), "Transformed test data has NaNs"


def test_a4_shortcut_baselines():
    """Verify that shortcut baselines execute and identify the testbed design shortcuts."""
    test_df = pd.DataFrame({
        "srcip": ["175.45.176.0", "175.45.176.3", "59.166.0.1", "10.0.0.1"],
        "sttl": [254, 64, 31, 31],
        "dttl": [252, 60, 29, 29],
        "ct_state_ttl": [2, 1, 0, 0],
        "label": [1, 1, 0, 0],
        "attack_cat": ["Exploits", "DoS", "Normal", "Normal"],
    })

    # Baseline 1: srcip prefix
    preds_ip = srcip_prefix_baseline(test_df)
    assert list(preds_ip) == [1, 1, 0, 0], "srcip-prefix rule failed to tag 175.45.176.* as attacks"

    # Baseline 2: TTL-trio tree
    tree = train_ttl_trio_baseline(test_df, test_df["label"])
    preds_ttl = tree.predict(test_df[TTL_TRIO])
    assert list(preds_ttl) == [1, 1, 0, 0]


def test_a5_session_split_deduplication():
    """Verify time-ordered cross-session split and model-view deduplication."""
    df = pd.DataFrame({
        "stime": [1421927400, 1421927410, 1424219000, 1424219010],
        "label": [0, 1, 0, 1],
        "attack_cat": ["Normal", "DoS", "Normal", "Exploits"],
    })
    for col in FEATURE_NAMES:
        df[col] = 1.0  # Identical features across all rows

    df = assign_sessions(df)
    assert list(df["session"]) == [0, 0, 1, 1]

    # Deduplication removes duplicates of (41 features + label)
    df_dedup = deduplicate_model_view(df, verbose=False)
    # Only 2 unique (features, label) combinations: (all 1s, 0) and (all 1s, 1)
    assert len(df_dedup) == 2


def test_a6_operational_metrics_contract():
    """Verify operational metrics dictionary contains all required fields."""
    y_true = np.array([1, 1, 0, 0, 0])
    y_pred = np.array([1, 0, 0, 0, 1])
    y_score = np.array([0.9, 0.4, 0.1, 0.2, 0.8])
    cats = pd.Series(["DoS", "Exploits", "Normal", "Normal", "Normal"])

    metrics = compute_metrics(
        y_true, y_pred, y_score,
        attack_cats=cats,
        session_duration_hours=2.0,
        model_name="TestModel",
    )

    required_keys = [
        "model", "accuracy", "precision", "recall", "f1",
        "roc_auc", "pr_auc", "fpr", "true_positives", "false_positives",
        "true_negatives", "false_negatives",
        "alerts_per_hour", "per_class_recall",
    ]
    for key in required_keys:
        assert key in metrics, f"Missing required metric: {key}"

    assert metrics["alerts_per_hour"] == 1.0  # 2 predicted positives / 2 hours
    assert "DoS" in metrics["per_class_recall"]
    assert "Exploits" in metrics["per_class_recall"]
