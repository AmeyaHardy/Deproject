"""
Session Splitter
================
Loads all 2,540,047 raw UNSW-NB15 records into a pandas DataFrame,
identifies the two capture sessions (separated by a 26-day gap),
deduplicates by model-view hash, and produces time-ordered train/test
splits suitable for causal anomaly detection evaluation.

Session 0: Jan 22 2015 11:49 to Jan 23 00:25 UTC  (1.09M flows, ~2% attack)
Session 1: Feb 18 2015 00:23 to Feb 18 12:21 UTC  (1.45M flows, ~20.6% attack)

Satisfies review gate A5: time/session-based split, deduplicated,
cross-session evaluation in both directions.
"""

import csv
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

# Add project root to path so we can import raw_loader
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.raw_loader import (
    RAW_DIR,
    RAW_CSVS,
    COLUMN_NAMES,
    FEATURE_NAMES,
    CONTEXT_COLUMNS,
    EVALUATION_COLUMNS,
    FEATURES_CSV,
    parse_field,
    clean_attack_cat,
)

# The 26-day gap boundary (epoch seconds)
# Session 0 ends around Jan 23 2015 00:25 UTC  -> stime ~ 1421972700
# Session 1 starts around Feb 18 2015 00:23 UTC -> stime ~ 1424218980
SESSION_GAP_THRESHOLD = 1423000000  # Any stime after this is Session 1


def load_raw_dataframe(
    limit: Optional[int] = None,
    file_indices: Optional[list[int]] = None,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Load raw UNSW-NB15 records into a pandas DataFrame.

    Returns a DataFrame with columns:
      - file_idx, row_idx (provenance)
      - All 49 raw columns from COLUMN_NAMES
      - record_id (e.g., "1:0")
    """
    if file_indices is None:
        file_indices = [1, 2, 3, 4]

    all_rows = []
    emitted = 0

    per_file_limit = (limit // len(file_indices)) + 1 if limit else None

    for file_idx in file_indices:
        csv_path = RAW_DIR / f"UNSW-NB15_{file_idx}.csv"
        if not csv_path.exists():
            if verbose:
                print(f"  WARNING: {csv_path} not found, skipping.")
            continue

        if verbose:
            print(f"  Loading file {file_idx}: {csv_path.name} ...", end=" ", flush=True)

        with csv_path.open("r", encoding="utf-8-sig", errors="ignore") as f:
            reader = csv.reader(f)
            file_rows = 0
            for row_idx, row in enumerate(reader):
                if not row or len(row) < 49:
                    continue

                parsed = {}
                for col_name, val in zip(COLUMN_NAMES, row):
                    parsed[col_name] = parse_field(col_name, val)

                parsed["file_idx"] = file_idx
                parsed["row_idx"] = row_idx
                parsed["record_id"] = f"{file_idx}:{row_idx}"

                all_rows.append(parsed)
                file_rows += 1
                emitted += 1

                if per_file_limit and file_rows >= per_file_limit:
                    break
                if limit and emitted >= limit:
                    break

            if verbose:
                print(f"{file_rows:,} rows")

        if limit and emitted >= limit:
            break

    df = pd.DataFrame(all_rows)

    if verbose:
        print(f"\n  Total loaded: {len(df):,} rows, {len(df.columns)} columns")

    return df


def assign_sessions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign session labels based on the 26-day gap in stime.

    Session 0: Jan 22-23, 2015 (stime < SESSION_GAP_THRESHOLD)
    Session 1: Feb 18, 2015     (stime >= SESSION_GAP_THRESHOLD)
    """
    df = df.copy()
    df["session"] = (df["stime"] >= SESSION_GAP_THRESHOLD).astype(int)
    return df


def compute_model_view_hash(row: pd.Series, feature_cols: list[str]) -> str:
    """
    Compute a hash of the model-view features for deduplication.
    Uses only the 41 behavioral features + label (the "model view").
    """
    values = tuple(row[c] for c in feature_cols)
    return hashlib.md5(str(values).encode()).hexdigest()


def deduplicate_model_view(
    df: pd.DataFrame,
    feature_cols: Optional[list[str]] = None,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Remove exact duplicates in the model-view feature space.

    Per review prompt: 18.9% full-record, 20.6% model-view duplicates.
    Deduplication is required before splitting (gate A5).
    """
    if feature_cols is None:
        # Model-view: 41 features + label
        feature_cols = FEATURE_NAMES + ["label"]

    before = len(df)
    df_dedup = df.drop_duplicates(subset=feature_cols, keep="first")
    after = len(df_dedup)

    if verbose:
        removed = before - after
        print(f"  Deduplication: {before:,} -> {after:,} "
              f"(removed {removed:,}, {removed/before*100:.1f}%)")

    return df_dedup.reset_index(drop=True)


def session_split(
    df: pd.DataFrame,
    train_session: int = 0,
    test_session: int = 1,
    deduplicate: bool = True,
    verbose: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split data by capture session for cross-session evaluation.

    Args:
        df: DataFrame with 'session' column
        train_session: Which session to use for training (0 or 1)
        test_session: Which session to use for testing (0 or 1)
        deduplicate: Whether to remove model-view duplicates before splitting
        verbose: Print statistics

    Returns:
        (train_df, test_df) tuple
    """
    if "session" not in df.columns:
        df = assign_sessions(df)

    if deduplicate:
        df = deduplicate_model_view(df, verbose=verbose)

    train_df = df[df["session"] == train_session].copy().reset_index(drop=True)
    test_df = df[df["session"] == test_session].copy().reset_index(drop=True)

    if verbose:
        for name, split_df, session_id in [("Train", train_df, train_session),
                                             ("Test", test_df, test_session)]:
            n_attack = (split_df["label"] == 1).sum()
            attack_pct = n_attack / len(split_df) * 100 if len(split_df) > 0 else 0
            print(f"  {name} (Session {session_id}): {len(split_df):,} rows, "
                  f"{n_attack:,} attacks ({attack_pct:.1f}%)")

            if "attack_cat" in split_df.columns:
                cats = split_df["attack_cat"].value_counts()
                for cat, count in cats.items():
                    print(f"    {cat}: {count:,}")

    return train_df, test_df


def get_feature_matrix(
    df: pd.DataFrame,
    feature_names: Optional[list[str]] = None,
) -> tuple[pd.DataFrame, pd.Series]:
    """
    Extract the feature matrix X and label vector y from a raw DataFrame.

    Returns:
        (X, y) where X contains the 41 behavioral features and
        y is the binary label (0=Normal, 1=Attack).
    """
    if feature_names is None:
        feature_names = FEATURE_NAMES

    X = df[feature_names].copy()
    y = df["label"].copy()

    return X, y


if __name__ == "__main__":
    print("=" * 70)
    print("Session Splitter: Loading UNSW-NB15 Raw Dataset")
    print("=" * 70)

    print("\nStep 1: Loading raw data...")
    df = load_raw_dataframe()

    print("\nStep 2: Assigning sessions...")
    df = assign_sessions(df)

    s0 = df[df["session"] == 0]
    s1 = df[df["session"] == 1]
    print(f"  Session 0: {len(s0):,} rows "
          f"({(s0['label']==1).sum()/(len(s0))*100:.1f}% attack)")
    print(f"  Session 1: {len(s1):,} rows "
          f"({(s1['label']==1).sum()/(len(s1))*100:.1f}% attack)")

    print("\nStep 3: Cross-session split (train=S0, test=S1)...")
    train_df, test_df = session_split(df, train_session=0, test_session=1)

    print("\nStep 4: Reverse split (train=S1, test=S0)...")
    train_df_rev, test_df_rev = session_split(df, train_session=1, test_session=0)

    print("\nDone.")
