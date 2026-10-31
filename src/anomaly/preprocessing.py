"""
Frozen Preprocessing Pipeline
==============================
Implements the anti-leakage preprocessing required by gate A2:

  1. Compute P99 caps, medians, and z-score parameters on the TRAINING
     time-prefix ONLY.
  2. Serialize (freeze) those parameters to disk with provenance metadata.
  3. Apply them identically to the test split and to the streaming pipeline.

The same transformations from Lab3 EDA are applied:
  - Median imputation (with flag)
  - P99 Winsorization
  - log1p transform
  - Z-score standardization
  - Label encoding for categoricals (proto, service, state)

But critically: all statistics are computed ONLY on the training data.
"""

import json
import sys
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

# Columns that receive the full Winsorize + log1p + z-score treatment
WINSOR_COLS = [
    "dur", "sbytes", "dbytes", "sload", "dload", "sjit", "djit",
    "spkts", "dpkts", "sloss", "dloss",
    "sttl", "dttl",
    "sintpkt", "dintpkt",
    "smeansz", "dmeansz",
    "tcprtt", "synack", "ackdat",
    "swin", "dwin",
    "stcpb", "dtcpb",
    "trans_depth", "res_bdy_len",
    "ct_srv_src", "ct_srv_dst",
    "ct_dst_ltm", "ct_src_ltm",
    "ct_src_dport_ltm", "ct_dst_sport_ltm", "ct_dst_src_ltm",
    "ct_state_ttl", "ct_flw_http_mthd",
    "is_ftp_login", "ct_ftp_cmd",
    "is_sm_ips_ports",
]

# Categorical features that need label encoding
CATEGORICAL_COLS = ["proto", "service", "state"]

# Default output directory for serialized preprocessing artifacts
ARTIFACTS_DIR = ROOT_DIR / "data" / "processed" / "preprocessing_artifacts"


class FrozenPreprocessor:
    """
    A preprocessing pipeline that can be fit on a training set and then
    applied identically (frozen) to any other data.

    Satisfies gate A2: all statistics are computed from the training prefix
    and frozen. Causality is preserved.
    """

    def __init__(self):
        self.is_fitted = False

        # Parameters learned from training data only
        self.medians: dict[str, float] = {}
        self.p99_caps: dict[str, float] = {}
        self.zscore_means: dict[str, float] = {}
        self.zscore_stds: dict[str, float] = {}
        self.label_encoders: dict[str, dict[str, int]] = {}

        # Provenance metadata
        self.provenance: dict = {}

    def fit(
        self,
        X_train: pd.DataFrame,
        provenance_info: Optional[dict] = None,
    ) -> "FrozenPreprocessor":
        """
        Compute all preprocessing parameters from the training data only.

        Args:
            X_train: Training feature matrix (41 behavioral features)
            provenance_info: Optional dict with metadata about the training data
        """
        print("  Fitting preprocessing pipeline on training data...")

        # 1. Medians for imputation
        for col in WINSOR_COLS:
            if col in X_train.columns:
                self.medians[col] = float(X_train[col].median())

        # 2. P99 caps for Winsorization
        for col in WINSOR_COLS:
            if col in X_train.columns:
                self.p99_caps[col] = float(X_train[col].quantile(0.99))

        # 3. Z-score parameters (computed on Winsorized + log1p values)
        for col in WINSOR_COLS:
            if col in X_train.columns:
                winsorized = X_train[col].fillna(self.medians[col]).clip(
                    upper=self.p99_caps[col]
                )
                log_vals = np.log1p(winsorized)
                self.zscore_means[col] = float(log_vals.mean())
                self.zscore_stds[col] = float(log_vals.std())
                if self.zscore_stds[col] == 0:
                    self.zscore_stds[col] = 1.0  # prevent division by zero

        # 4. Label encoders for categoricals
        for col in CATEGORICAL_COLS:
            if col in X_train.columns:
                unique_vals = sorted(X_train[col].dropna().unique())
                self.label_encoders[col] = {
                    val: idx for idx, val in enumerate(unique_vals)
                }

        # Record provenance
        self.provenance = {
            "fit_timestamp": datetime.now(timezone.utc).isoformat(),
            "training_rows": len(X_train),
            "training_columns": list(X_train.columns),
            "winsor_cols": WINSOR_COLS,
            "categorical_cols": CATEGORICAL_COLS,
        }
        if provenance_info:
            self.provenance.update(provenance_info)

        self.is_fitted = True
        print(f"    Learned: {len(self.medians)} medians, "
              f"{len(self.p99_caps)} P99 caps, "
              f"{len(self.zscore_means)} z-score params, "
              f"{len(self.label_encoders)} label encoders")
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Apply the frozen preprocessing to any DataFrame.

        Transforms:
          1. Median imputation for NaN
          2. P99 cap (Winsorize)
          3. log1p transform
          4. Z-score standardization
          5. Label encoding for categoricals

        Returns a new DataFrame with numeric-only columns suitable for
        model training/inference.
        """
        if not self.is_fitted:
            raise RuntimeError("Preprocessor has not been fitted. Call fit() first.")

        result = pd.DataFrame(index=X.index)

        # Process numeric columns
        for col in WINSOR_COLS:
            if col not in X.columns:
                result[col] = 0.0
                continue

            vals = X[col].copy()

            # Step 1: Median imputation
            median = self.medians.get(col, 0.0)
            vals = vals.fillna(median)

            # Step 2: P99 Winsorization
            cap = self.p99_caps.get(col, vals.max())
            vals = vals.clip(upper=cap)

            # Step 3: log1p transform
            vals = np.log1p(vals)

            # Step 4: Z-score standardization
            mu = self.zscore_means.get(col, 0.0)
            sigma = self.zscore_stds.get(col, 1.0)
            vals = (vals - mu) / sigma

            result[col] = vals

        # Process categorical columns
        for col in CATEGORICAL_COLS:
            if col not in X.columns:
                result[f"{col}_enc"] = 0
                continue

            encoder = self.label_encoders.get(col, {})
            # Map known categories; unseen categories get -1
            result[f"{col}_enc"] = X[col].map(
                lambda v, enc=encoder: enc.get(v, -1)
            )

        return result

    def fit_transform(
        self,
        X_train: pd.DataFrame,
        provenance_info: Optional[dict] = None,
    ) -> pd.DataFrame:
        """Fit on training data and transform it in one step."""
        self.fit(X_train, provenance_info)
        return self.transform(X_train)

    def save(self, path: Optional[Path] = None) -> Path:
        """
        Serialize the fitted preprocessor to disk.

        Saves two files:
          - preprocessor.joblib: the full Python object
          - preprocessing_provenance.json: human-readable provenance
        """
        if path is None:
            path = ARTIFACTS_DIR
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)

        # Save the full object
        joblib_path = path / "preprocessor.joblib"
        joblib.dump(self, joblib_path)

        # Save human-readable provenance
        provenance_path = path / "preprocessing_provenance.json"
        provenance = {
            **self.provenance,
            "medians": self.medians,
            "p99_caps": self.p99_caps,
            "zscore_means": self.zscore_means,
            "zscore_stds": self.zscore_stds,
            "label_encoders": self.label_encoders,
        }
        provenance_path.write_text(
            json.dumps(provenance, indent=2, default=str),
            encoding="utf-8",
        )

        print(f"  Saved preprocessor to: {joblib_path}")
        print(f"  Saved provenance to:   {provenance_path}")
        return path

    @classmethod
    def load(cls, path: Optional[Path] = None) -> "FrozenPreprocessor":
        """Load a previously saved preprocessor."""
        if path is None:
            path = ARTIFACTS_DIR
        path = Path(path)
        joblib_path = path / "preprocessor.joblib"
        return joblib.load(joblib_path)
