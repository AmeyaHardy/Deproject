"""
Helper: Train S1 Model for Verification Pass (C3)
=================================================
Trains RandomForestClassifier on Session 1 (Files 3 and 4) with held-out calibration split.
Saves model and preprocessor to disk so scoring can run in a clean process with minimal memory footprint.
"""
import sys
import gc
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

from src.raw_loader import FEATURE_NAMES, COLUMN_NAMES, RAW_CSVS
from src.anomaly.session_split import assign_sessions, get_feature_matrix
from src.anomaly.preprocessing import FrozenPreprocessor

SEED = 42
print(f"SEED: {SEED}")

raw_csvs = RAW_CSVS
col_names = COLUMN_NAMES
KEEP_COLS = set(FEATURE_NAMES) | {"label", "stime", "ltime"}

print("Loading Session 1 raw data (Files 3 and 4)...")
s1_chunks = []
for fid in [2, 3]:
    p = raw_csvs[fid]
    cdf = pd.read_csv(p, header=None, names=col_names, low_memory=False)
    # Drop unneeded columns immediately
    drop_cols = [c for c in cdf.columns if c not in KEEP_COLS]
    cdf.drop(columns=drop_cols, inplace=True)
    
    # Clean types
    for col in FEATURE_NAMES:
        if col in cdf.columns and col not in ("proto", "service", "state"):
            cdf[col] = pd.to_numeric(cdf[col], errors="coerce").fillna(0).astype("float32")
    if "ct_ftp_cmd" in cdf.columns:
        cdf["ct_ftp_cmd"] = pd.to_numeric(cdf["ct_ftp_cmd"], errors="coerce").fillna(0)
    for col in ["service", "state"]:
        if col in cdf.columns:
            cdf[col] = cdf[col].fillna("none").apply(
                lambda v: "none" if str(v).strip() in ("-", "", " ") else str(v).strip()
            )
    if "label" in cdf.columns:
        cdf["label"] = pd.to_numeric(cdf["label"], errors="coerce").fillna(0).astype("int8")
    if "stime" in cdf.columns:
        cdf["stime"] = pd.to_numeric(cdf["stime"], errors="coerce").fillna(0).astype("int64")
        
    s1_chunks.append(cdf)

s1_df = pd.concat(s1_chunks, ignore_index=True)
del s1_chunks; gc.collect()
s1_df = assign_sessions(s1_df)
s1_df = s1_df[s1_df["session"] == 1].reset_index(drop=True)
print(f"Session 1 size: {len(s1_df):,} rows")

# Fit Preprocessor
pp1 = FrozenPreprocessor()
X1_raw, y1 = get_feature_matrix(s1_df)
X1_proc = pp1.fit_transform(X1_raw, provenance_info={"train_session": 1})
del X1_raw; gc.collect()

# Train split: held-out middle 20%
n1 = len(s1_df)
c_start, c_end = int(0.4 * n1), int(0.6 * n1)
train_idx_1b = list(range(0, c_start)) + list(range(c_end, n1))

print(f"Fitting RandomForest on S1 (training rows: {len(train_idx_1b):,})...")
rf1_b = RandomForestClassifier(
    n_estimators=100,
    max_depth=20,
    min_samples_leaf=5,
    class_weight="balanced",
    random_state=SEED,
    n_jobs=-1
)
rf1_b.fit(X1_proc.iloc[train_idx_1b], y1.iloc[train_idx_1b])

# Save artifacts
out_model = ROOT / "data" / "processed" / "models" / "rf1_b_verify.joblib"
out_pp = ROOT / "data" / "processed" / "models" / "pp1_verify.joblib"
joblib.dump(rf1_b, out_model)
joblib.dump(pp1, out_pp)
print(f"Saved artifacts to {out_model} and {out_pp}")

