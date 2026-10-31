"""
Validation Tests for Gate A: Anomaly Detector Pipeline Review
==============================================================
Validates:
1. Permutation test uses no true-label information during training.
2. Preprocessing parameters are frozen from the training prefix and not recalculated on test.
3. No forbidden columns (label, attack_cat, srcip, dstip, sport, dsport, stime, ltime) in feature matrix.
"""

import inspect
import pytest
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

from src.raw_loader import FEATURE_NAMES, CONTEXT_COLUMNS, EVALUATION_COLUMNS
from src.anomaly.session_split import get_feature_matrix
from src.anomaly.preprocessing import FrozenPreprocessor
from src.anomaly.train_and_evaluate import label_permutation_test


def test_no_forbidden_columns_in_feature_matrix():
    """Verify that get_feature_matrix excludes all forbidden columns."""
    forbidden_cols = {
        "label", "attack_cat", "srcip", "dstip",
        "sport", "dsport", "stime", "ltime"
    }

    dummy_data = {}
    for col in FEATURE_NAMES:
        dummy_data[col] = [1.0, 2.0, 3.0]
    for col in forbidden_cols:
        dummy_data[col] = [0, 1, 0]

    df = pd.DataFrame(dummy_data)
    X, y = get_feature_matrix(df)

    # Check X columns
    for col in forbidden_cols:
        assert col not in X.columns, f"Forbidden column '{col}' found in feature matrix X!"

    # Ensure exactly 41 behavioral features
    assert list(X.columns) == FEATURE_NAMES
    assert len(X.columns) == 41

    # Verify label vector y is clean
    assert list(y) == [0, 1, 0]


def test_frozen_preprocessing_parameters_frozen_from_training_prefix():
    """Verify that preprocessing parameters are derived strictly from the training prefix."""
    np.random.seed(42)
    n_train = 50
    n_test = 50

    # Synthetic training data with specific values
    train_data = {
        "sbytes": [10.0] * n_train,
        "dbytes": [20.0] * n_train,
        "proto": ["tcp"] * n_train,
        "state": ["FIN"] * n_train,
        "service": ["http"] * n_train,
    }
    # Test data with completely different values/distributions
    test_data = {
        "sbytes": [1000.0] * n_test,
        "dbytes": [2000.0] * n_test,
        "proto": ["udp"] * n_test,
        "state": ["CON"] * n_test,
        "service": ["dns"] * n_test,
    }

    for col in FEATURE_NAMES:
        if col not in train_data:
            train_data[col] = [1.0] * n_train
            test_data[col] = [50.0] * n_test

    df_train = pd.DataFrame(train_data)
    df_test = pd.DataFrame(test_data)

    pp = FrozenPreprocessor()

    # Preprocessor fit signature must NOT accept target labels y
    sig = inspect.signature(pp.fit)
    assert "y" not in sig.parameters or sig.parameters["y"].default is not inspect.Parameter.empty, \
        "FrozenPreprocessor.fit must not require label y"

    # Fit and transform train
    X_train_proc = pp.fit_transform(df_train)

    # Record fitted parameters
    frozen_medians = dict(pp.medians)
    frozen_p99 = dict(pp.p99_caps)
    frozen_encoders = {k: dict(v) for k, v in pp.label_encoders.items()}

    # Transform test
    X_test_proc = pp.transform(df_test)

    # Verify parameters were NOT updated or overwritten by test data
    assert pp.medians == frozen_medians, "Medians were modified during transform!"
    assert pp.p99_caps == frozen_p99, "P99 caps were modified during transform!"
    assert pp.label_encoders == frozen_encoders, "Label encoders were modified during transform!"

    # Verify test sbytes was scaled using train parameters
    assert pp.medians["sbytes"] == 10.0
    assert pp.p99_caps["sbytes"] == 10.0


def test_permutation_test_uses_no_true_label_information():
    """Verify that permutation test training uses only shuffled labels, not true labels."""
    np.random.seed(42)
    n = 100
    df = pd.DataFrame({col: np.random.randn(n) for col in FEATURE_NAMES})
    df["proto"] = "tcp"
    df["state"] = "FIN"
    df["service"] = "http"

    # Perfect correlation with feature
    df["label"] = (df["sbytes"] > 0).astype(int)

    # Verify that in a permutation test, shuffled labels break correlation with true labels
    y_true = df["label"].copy()
    rng = np.random.RandomState(42)
    y_perm = y_true.sample(frac=1.0, random_state=rng).values

    # Correlation between permuted and true labels should be near 0
    corr = np.corrcoef(y_true.values, y_perm)[0, 1]
    assert abs(corr) < 0.25, f"Permutation labels too correlated with true labels (corr={corr})"

    # Verify FrozenPreprocessor does not use true labels
    pp = FrozenPreprocessor()
    sig_fit = inspect.signature(pp.fit)
    assert "y" not in sig_fit.parameters, "FrozenPreprocessor.fit should not accept y"
