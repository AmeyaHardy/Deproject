"""
Inspect temporal attack distribution in Session 0 and Session 1.
"""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd
from src.raw_loader import FEATURE_NAMES, COLUMN_NAMES, RAW_DIR, clean_attack_cat

KEEP_COLS = set(FEATURE_NAMES) | {"label", "attack_cat", "stime", "ltime"}
SESSION_GAP_THRESHOLD = 1_423_000_000

print("Loading raw CSVs...")
dfs = []
for fid in [1, 2, 3, 4]:
    csv_path = RAW_DIR / f"UNSW-NB15_{fid}.csv"
    chunk = pd.read_csv(
        csv_path, header=None, names=COLUMN_NAMES,
        encoding="utf-8-sig", on_bad_lines="skip", low_memory=False,
    )
    chunk.drop(columns=[c for c in chunk.columns if c not in KEEP_COLS], inplace=True)
    dfs.append(chunk)

df = pd.concat(dfs, ignore_index=True)
df["session"] = (df["stime"] >= SESSION_GAP_THRESHOLD).astype("int8")
df["label"] = pd.to_numeric(df["label"], errors="coerce").fillna(0).astype("int8")
df["stime"] = pd.to_numeric(df["stime"], errors="coerce").fillna(0).astype("float64")

# Model-view deduplication
print(f"Total rows before dedup: {len(df):,}")
df.drop_duplicates(subset=FEATURE_NAMES + ["label"], keep="first", inplace=True)
df.reset_index(drop=True, inplace=True)
print(f"Total rows after dedup: {len(df):,}")

for sid in [0, 1]:
    sdf = df[df["session"] == sid].sort_values("stime").reset_index(drop=True)
    n = len(sdf)
    n_atk = (sdf["label"] == 1).sum()
    print(f"\n--- Session {sid} ({n:,} rows, {n_atk:,} attacks = {n_atk/n*100:.2f}%) ---")
    
    # 5 quintiles (20% each)
    for q in range(5):
        q_start = int(q * 0.2 * n)
        q_end = int((q + 1) * 0.2 * n)
        slice_df = sdf.iloc[q_start:q_end]
        s_atk = (slice_df["label"] == 1).sum()
        s_min_t = slice_df["stime"].min()
        s_max_t = slice_df["stime"].max()
        print(f"  Quintile {q+1} (rows {q_start:,}..{q_end:,}): {len(slice_df):,} rows, {s_atk:,} attacks ({s_atk/len(slice_df)*100:.2f}%) | stime: {s_min_t:.0f}..{s_max_t:.0f}")

