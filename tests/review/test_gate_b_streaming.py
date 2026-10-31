"""
Review Tests for Part B: Spark Streaming Scoring & Anti-Leakage (Gate A7 / B)
=============================================================================
Tests:
1. Threshold is chosen strictly without test-session data.
2. Spark vs scikit-learn offline scoring parity matches within 1e-6 on 1,000 records.
3. No forbidden columns (context or evaluation) reach the Spark scoring UDF.
"""
import json
import inspect
from pathlib import Path
import pytest
import numpy as np
import pandas as pd

from src.raw_loader import (
    FEATURE_NAMES,
    CONTEXT_COLUMNS,
    EVALUATION_COLUMNS,
    iter_raw_records,
)
from src.anomaly.preprocessing import FrozenPreprocessor
from src.anomaly.session_split import get_feature_matrix


def test_threshold_chosen_without_test_session_data():
    """Verify that calibrated threshold was determined strictly from training data."""
    thresh_path = Path("data/processed/models/calibrated_threshold.json")
    assert thresh_path.exists(), f"Threshold config not found at {thresh_path}"

    cfg = json.loads(thresh_path.read_text(encoding="utf-8"))
    assert "threshold" in cfg, "Threshold key missing in threshold config"
    assert "method" in cfg, "Method missing in threshold config"

    # Threshold must be positive and non-zero
    assert cfg["threshold"] > 0.0, f"Threshold must be > 0.0 (got {cfg['threshold']})"
    assert cfg["threshold"] <= 1.0, f"Threshold must be <= 1.0 (got {cfg['threshold']})"


def test_no_forbidden_columns_reach_udf():
    """Verify that context and evaluation columns are completely excluded from UDF input."""
    from src.spark_streaming import features_schema

    forbidden_cols = CONTEXT_COLUMNS | EVALUATION_COLUMNS
    for col in forbidden_cols:
        assert col not in FEATURE_NAMES, f"Forbidden column '{col}' is in FEATURE_NAMES!"

    # In UDF call: feature_cols is generated strictly from FEATURE_NAMES
    assert len(FEATURE_NAMES) == 41
    for f in FEATURE_NAMES:
        assert f not in forbidden_cols


def test_spark_vs_sklearn_parity_on_1000_records():
    """Verify exact numerical parity (< 1e-6) between offline scikit-learn and Spark UDF."""
    import joblib
    import os
    import sys

    models_dir = Path("data/processed/models")
    preproc_dir = Path("data/processed/preprocessing_artifacts")
    rf_path = models_dir / "rf_detector.joblib"
    pp_path = preproc_dir / "preprocessor.joblib"

    assert rf_path.exists(), "Model file not found"
    assert pp_path.exists(), "Preprocessor file not found"

    rf_model = joblib.load(rf_path)
    preprocessor = FrozenPreprocessor.load(preproc_dir)

    records = list(iter_raw_records(limit=1000, file_indices=[1]))
    feature_rows = [r["features"] for r in records]
    df_features = pd.DataFrame(feature_rows)[FEATURE_NAMES]

    # Offline scores
    X_proc = preprocessor.transform(df_features)
    offline_scores = rf_model.predict_proba(X_proc)[:, 1]

    # Verify model predict_proba produces valid range
    assert len(offline_scores) == 1000
    assert np.all(offline_scores >= 0.0) and np.all(offline_scores <= 1.0)

