"""
Empirical Watermark Derivation: Lateness & Out-of-Order Distribution (Task B4)
==============================================================================
Streams raw UNSW-NB15 files in chunked arrival order without writing large intermediate files.
Measures arrival lateness:
  Lateness = max_timestamp_seen_so_far - current_timestamp  (for current_timestamp < max_seen)
Reports out-of-order fraction and lateness percentiles (p50, p90, p99, p99.9, max)
for BOTH stime and ltime, per file and globally.
"""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import time
import numpy as np
import pandas as pd
from src.raw_loader import RAW_DIR

def measure_file_lateness(file_path):
    print(f"\nScanning {file_path.name} in chunks...")
    t0 = time.time()
    
    max_stime = -float("inf")
    max_ltime = -float("inf")

    stime_lateness = []
    ltime_lateness = []

    total_records = 0
    ooo_stime_count = 0
    ooo_ltime_count = 0

    chunksize = 100_000
    for chunk in pd.read_csv(file_path, header=None, usecols=[29, 30], names=["stime", "ltime"], chunksize=chunksize, on_bad_lines="skip"):
        stimes = pd.to_numeric(chunk["stime"], errors="coerce").fillna(0).values
        ltimes = pd.to_numeric(chunk["ltime"], errors="coerce").fillna(0).values

        for s, l in zip(stimes, ltimes):
            total_records += 1

            # stime lateness
            if s < max_stime:
                ooo_stime_count += 1
                late = max_stime - s
                stime_lateness.append(late)
            else:
                max_stime = s

            # ltime lateness
            if l < max_ltime:
                ooo_ltime_count += 1
                late_l = max_ltime - l
                ltime_lateness.append(late_l)
            else:
                max_ltime = l

    stime_arr = np.array(stime_lateness) if stime_lateness else np.array([0.0])
    ltime_arr = np.array(ltime_lateness) if ltime_lateness else np.array([0.0])

    def calc_pcts(arr, total, ooo_cnt):
        return {
            "total_records": total,
            "out_of_order_count": ooo_cnt,
            "out_of_order_pct": (ooo_cnt / total * 100.0) if total > 0 else 0.0,
            "p50_lateness_sec": float(np.percentile(arr, 50)) if len(arr) > 0 else 0.0,
            "p90_lateness_sec": float(np.percentile(arr, 90)) if len(arr) > 0 else 0.0,
            "p99_lateness_sec": float(np.percentile(arr, 99)) if len(arr) > 0 else 0.0,
            "p99_9_lateness_sec": float(np.percentile(arr, 99.9)) if len(arr) > 0 else 0.0,
            "max_lateness_sec": float(np.max(arr)) if len(arr) > 0 else 0.0,
        }

    res_stime = calc_pcts(stime_arr, total_records, ooo_stime_count)
    res_ltime = calc_pcts(ltime_arr, total_records, ooo_ltime_count)

    print(f"  Processed {total_records:,} rows in {time.time()-t0:.1f}s")
    print(f"  stime: Out-of-order={res_stime['out_of_order_pct']:.2f}% | p50={res_stime['p50_lateness_sec']:.1f}s | p99={res_stime['p99_lateness_sec']:.1f}s | p99.9={res_stime['p99_9_lateness_sec']:.1f}s | Max={res_stime['max_lateness_sec']:.1f}s")
    print(f"  ltime: Out-of-order={res_ltime['out_of_order_pct']:.2f}% | p50={res_ltime['p50_lateness_sec']:.1f}s | p99={res_ltime['p99_lateness_sec']:.1f}s | p99.9={res_ltime['p99_9_lateness_sec']:.1f}s | Max={res_ltime['max_lateness_sec']:.1f}s")

    return res_stime, res_ltime

if __name__ == "__main__":
    print("="*70)
    print("Empirical Watermark Lateness Analysis (Task B4)")
    print("="*70)
    
    global_results = {}
    for fid in [1, 2, 3, 4]:
        fpath = RAW_DIR / f"UNSW-NB15_{fid}.csv"
        if fpath.exists():
            s_res, l_res = measure_file_lateness(fpath)
            global_results[fpath.name] = {"stime": s_res, "ltime": l_res}

    print("\nSummary complete.")

