"""
Empirical Watermark Derivation: Lateness & Out-of-Order Distribution (Task B4 / Task C1 Corrected)
===================================================================================================
Streams raw UNSW-NB15 files in chunked original arrival order without writing large intermediate files.
Dynamically resolves column indices for 'stime' and 'ltime' from NUSW-NB15_features.csv.
Measures arrival lateness:
  Pairwise inversion: current_timestamp < previous_timestamp
  Running-max lateness: current_timestamp < max_seen_so_far
  Lateness = max_seen_so_far - current_timestamp
Reports out-of-order fraction, lateness percentiles (p50, p90, p99, p99.9, max),
and watermark drop rates (10s, 30s, 60s, 300s) for BOTH stime and ltime.
"""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import time
import numpy as np
import pandas as pd
from src.raw_loader import RAW_DIR, RAW_CSVS, load_feature_metadata

SEED = 42
np.random.seed(SEED)
print(f"SEED: {SEED}")

# Dynamically resolve column indices from features CSV
metadata = load_feature_metadata()
col_names = [col["name"].strip().lower() for col in metadata]

stime_col_idx = col_names.index("stime")
ltime_col_idx = col_names.index("ltime")

print(f"Dynamically resolved column indices from NUSW-NB15_features.csv:")
print(f"  Column {stime_col_idx} -> '{metadata[stime_col_idx]['name']}' (type: {metadata[stime_col_idx]['type']})")
print(f"  Column {ltime_col_idx} -> '{metadata[ltime_col_idx]['name']}' (type: {metadata[ltime_col_idx]['type']})")

WATERMARK_CANDIDATES = [10, 30, 60, 300]

def analyze_stream(carry_over_state=None):
    """
    Streams all 4 CSV files in file sequence and computes global & per-file metrics.
    """
    global_total = 0
    global_stime_pairwise_inv = 0
    global_ltime_pairwise_inv = 0
    global_stime_ooo = 0
    global_ltime_ooo = 0
    
    global_stime_lateness = []
    global_ltime_lateness = []

    global_stime_wm_drops = {wm: 0 for wm in WATERMARK_CANDIDATES}
    global_ltime_wm_drops = {wm: 0 for wm in WATERMARK_CANDIDATES}

    running_max_stime = -float("inf")
    running_max_ltime = -float("inf")
    prev_stime = None
    prev_ltime = None

    per_file_results = {}

    for csv_file in RAW_CSVS:
        fpath = Path(csv_file)
        if not fpath.is_absolute():
            fpath = ROOT / fpath
        if not fpath.exists():
            print(f"Warning: {fpath} not found, skipping.")
            continue

        print(f"\nProcessing {csv_file} in chunks...")
        t0 = time.time()
        file_total = 0
        file_stime_pairwise_inv = 0
        file_ltime_pairwise_inv = 0
        file_stime_ooo = 0
        file_ltime_ooo = 0
        file_stime_lateness = []
        file_ltime_lateness = []
        file_stime_wm_drops = {wm: 0 for wm in WATERMARK_CANDIDATES}
        file_ltime_wm_drops = {wm: 0 for wm in WATERMARK_CANDIDATES}

        chunksize = 100_000
        for chunk in pd.read_csv(
            fpath,
            header=None,
            usecols=[stime_col_idx, ltime_col_idx],
            names=["stime", "ltime"],
            chunksize=chunksize,
            on_bad_lines="skip",
            dtype={"stime": float, "ltime": float}
        ):
            stimes = chunk["stime"].fillna(0).values
            ltimes = chunk["ltime"].fillna(0).values

            for s, l in zip(stimes, ltimes):
                file_total += 1
                global_total += 1

                # stime pairwise
                if prev_stime is not None and s < prev_stime:
                    file_stime_pairwise_inv += 1
                    global_stime_pairwise_inv += 1
                prev_stime = s

                # ltime pairwise
                if prev_ltime is not None and l < prev_ltime:
                    file_ltime_pairwise_inv += 1
                    global_ltime_pairwise_inv += 1
                prev_ltime = l

                # stime running max & lateness
                if s < running_max_stime:
                    file_stime_ooo += 1
                    global_stime_ooo += 1
                    late_s = running_max_stime - s
                    file_stime_lateness.append(late_s)
                    global_stime_lateness.append(late_s)
                    for wm in WATERMARK_CANDIDATES:
                        if s < (running_max_stime - wm):
                            file_stime_wm_drops[wm] += 1
                            global_stime_wm_drops[wm] += 1
                else:
                    running_max_stime = s

                # ltime running max & lateness
                if l < running_max_ltime:
                    file_ltime_ooo += 1
                    global_ltime_ooo += 1
                    late_l = running_max_ltime - l
                    file_ltime_lateness.append(late_l)
                    global_ltime_lateness.append(late_l)
                    for wm in WATERMARK_CANDIDATES:
                        if l < (running_max_ltime - wm):
                            file_ltime_wm_drops[wm] += 1
                            global_ltime_wm_drops[wm] += 1
                else:
                    running_max_ltime = l

        dt = time.time() - t0
        s_arr = np.array(file_stime_lateness) if file_stime_lateness else np.array([0.0])
        l_arr = np.array(file_ltime_lateness) if file_ltime_lateness else np.array([0.0])

        print(f"  Processed {file_total:,} rows in {dt:.1f}s")
        print(f"  stime: pairwise_inv={file_stime_pairwise_inv/file_total*100:.3f}% ({file_stime_pairwise_inv:,}) | running_max_ooo={file_stime_ooo/file_total*100:.3f}% | p50={np.percentile(s_arr, 50):.1f}s | p90={np.percentile(s_arr, 90):.1f}s | p99={np.percentile(s_arr, 99):.1f}s | max={np.max(s_arr):.1f}s")
        print(f"  ltime: pairwise_inv={file_ltime_pairwise_inv/file_total*100:.3f}% ({file_ltime_pairwise_inv:,}) | running_max_ooo={file_ltime_ooo/file_total*100:.3f}% | p50={np.percentile(l_arr, 50):.1f}s | max={np.max(l_arr):.1f}s")
        print(f"  stime drops: 10s={file_stime_wm_drops[10]:,} ({file_stime_wm_drops[10]/file_total*100:.4f}%), 30s={file_stime_wm_drops[30]:,} ({file_stime_wm_drops[30]/file_total*100:.4f}%), 60s={file_stime_wm_drops[60]:,} ({file_stime_wm_drops[60]/file_total*100:.4f}%), 300s={file_stime_wm_drops[300]:,} ({file_stime_wm_drops[300]/file_total*100:.4f}%)")

        per_file_results[csv_file] = {
            "total": file_total,
            "stime_pairwise_pct": file_stime_pairwise_inv / file_total * 100,
            "stime_ooo_pct": file_stime_ooo / file_total * 100,
            "ltime_pairwise_pct": file_ltime_pairwise_inv / file_total * 100,
            "ltime_ooo_pct": file_ltime_ooo / file_total * 100,
        }

    # Global summary
    gs_arr = np.array(global_stime_lateness) if global_stime_lateness else np.array([0.0])
    gl_arr = np.array(global_ltime_lateness) if global_ltime_lateness else np.array([0.0])

    print("\n" + "="*80)
    print("GLOBAL EMPIRICAL SUMMARY ACROSS ALL 4 FILES")
    print("="*80)
    print(f"Total Rows: {global_total:,}")
    print("\n--- stime (Flow Start Time) ---")
    print(f"Pairwise Inversions: {global_stime_pairwise_inv:,} / {global_total:,} ({global_stime_pairwise_inv/global_total*100:.3f}%)")
    print(f"Running-Max Lateness: {global_stime_ooo:,} / {global_total:,} ({global_stime_ooo/global_total*100:.3f}%)")
    print(f"Lateness Percentiles: p50={np.percentile(gs_arr, 50):.1f}s | p90={np.percentile(gs_arr, 90):.1f}s | p99={np.percentile(gs_arr, 99):.1f}s | p99.9={np.percentile(gs_arr, 99.9):.1f}s | max={np.max(gs_arr):.1f}s")
    print(f"Watermark Drop Rates:")
    for wm in WATERMARK_CANDIDATES:
        drops = global_stime_wm_drops[wm]
        print(f"  {wm:3d}s watermark: drops {drops:6,d} flows ({drops/global_total*100:.4f}%)")

    print("\n--- ltime (Flow End Time) ---")
    print(f"Pairwise Inversions: {global_ltime_pairwise_inv:,} / {global_total:,} ({global_ltime_pairwise_inv/global_total*100:.3f}%)")
    print(f"Running-Max Lateness: {global_ltime_ooo:,} / {global_total:,} ({global_ltime_ooo/global_total*100:.3f}%)")
    print(f"Lateness Percentiles: p50={np.percentile(gl_arr, 50):.1f}s | max={np.max(gl_arr):.1f}s")
    for wm in WATERMARK_CANDIDATES:
        drops = global_ltime_wm_drops[wm]
        print(f"  {wm:3d}s watermark: drops {drops:6,d} flows ({drops/global_total*100:.4f}%)")
    print("="*80)

if __name__ == "__main__":
    analyze_stream()
