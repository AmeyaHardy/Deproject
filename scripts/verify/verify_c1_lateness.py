"""
Verification Script for Claim C1: Lateness and Sort Order
==========================================================
Independently reads the 4 raw CSV files in chunked original order using
exact column indices 28 ('stime') and 29 ('ltime') from NUSW-NB15_features.csv.
Computes:
1. Fraction where stime < previous stime (pairwise inversion)
2. Fraction where stime < running max stime (true lateness / out-of-order)
3. Lateness distribution (p50, p90, p99, p99.9, max)
4. Same for ltime.
5. Watermark drop analysis for 10s, 30s, 60s, 300s.
6. Per file, per session, and globally across all 4 files.
"""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import time
import os
import json
import psutil
import numpy as np
import pandas as pd
from src.raw_loader import RAW_DIR, RAW_CSVS, COLUMN_NAMES, load_feature_metadata

SEED = 42
np.random.seed(SEED)
print(f"SEED: {SEED}")

proc = psutil.Process(os.getpid())
def get_rss_mb():
    return proc.memory_info().rss / (1024 * 1024)

print(f"Starting memory: {get_rss_mb():.2f} MB")

# Verify column indices from metadata
col_specs = load_feature_metadata()
col_names = [c["name"] for c in col_specs]
stime_idx = col_names.index("stime")
ltime_idx = col_names.index("ltime")
print(f"Verified column indices: stime={stime_idx}, ltime={ltime_idx}")
assert stime_idx == 28 and ltime_idx == 29, f"Indices mismatch! Expected 28, 29, got {stime_idx}, {ltime_idx}"

WATERMARK_CANDIDATES = [10, 30, 60, 300]

def analyze_series(ts_array, prev_last_val=None, running_max=-float("inf")):
    """
    Analyzes an array of timestamps:
    - pairwise inversions (t_i < t_{i-1})
    - out-of-order against running max (t_i < running_max)
    - lateness = running_max - t_i
    - watermark drops
    """
    n = len(ts_array)
    if n == 0:
        return {}

    # Pairwise inversions
    if prev_last_val is not None:
        prev_vals = np.concatenate(([prev_last_val], ts_array[:-1]))
    else:
        prev_vals = np.concatenate(([ts_array[0]], ts_array[:-1]))
    
    inversions = (ts_array < prev_vals)
    if prev_last_val is None:
        inversions[0] = False
    inv_count = int(np.sum(inversions))

    # Running maximum
    # Compute running maximum with carried over running_max
    cur_running_max = np.maximum.accumulate(np.concatenate(([running_max], ts_array)))[1:]
    new_running_max = float(cur_running_max[-1])

    ooo_mask = ts_array < cur_running_max
    ooo_count = int(np.sum(ooo_mask))
    
    lateness = cur_running_max[ooo_mask] - ts_array[ooo_mask]

    # Watermark drops: dropped if t_i < cur_running_max - W
    wm_drops = {}
    for w in WATERMARK_CANDIDATES:
        dropped = np.sum(ts_array < (cur_running_max - w))
        wm_drops[w] = int(dropped)

    return {
        "n": n,
        "inv_count": inv_count,
        "ooo_count": ooo_count,
        "lateness": lateness,
        "new_running_max": new_running_max,
        "last_val": float(ts_array[-1]),
        "wm_drops": wm_drops
    }

def process_file(csv_path, global_state=None):
    print(f"\nProcessing {csv_path.name} in chunks (usecols=[28, 29])...")
    t0 = time.time()
    
    chunksize = 100_000
    file_n = 0
    
    # State for file-level metrics
    file_prev_s = None
    file_prev_l = None
    file_max_s = -float("inf")
    file_max_l = -float("inf")
    
    file_inv_s = 0
    file_inv_l = 0
    file_ooo_s = 0
    file_ooo_l = 0
    file_lateness_s = []
    file_lateness_l = []
    file_wm_drops_s = {w: 0 for w in WATERMARK_CANDIDATES}
    file_wm_drops_l = {w: 0 for w in WATERMARK_CANDIDATES}

    # If global_state is provided, also update global state
    for chunk in pd.read_csv(csv_path, header=None, usecols=[28, 29], names=["stime", "ltime"], chunksize=chunksize, on_bad_lines="skip"):
        stimes = pd.to_numeric(chunk["stime"], errors="coerce").fillna(0).values.astype("float64")
        ltimes = pd.to_numeric(chunk["ltime"], errors="coerce").fillna(0).values.astype("float64")
        chunk_n = len(stimes)
        file_n += chunk_n

        # --- File-local analysis ---
        res_s = analyze_series(stimes, file_prev_s, file_max_s)
        res_l = analyze_series(ltimes, file_prev_l, file_max_l)

        file_inv_s += res_s["inv_count"]
        file_inv_l += res_l["inv_count"]
        file_ooo_s += res_s["ooo_count"]
        file_ooo_l += res_l["ooo_count"]
        if len(res_s["lateness"]) > 0:
            file_lateness_s.append(res_s["lateness"])
        if len(res_l["lateness"]) > 0:
            file_lateness_l.append(res_l["lateness"])
        for w in WATERMARK_CANDIDATES:
            file_wm_drops_s[w] += res_s["wm_drops"][w]
            file_wm_drops_l[w] += res_l["wm_drops"][w]

        file_prev_s = res_s["last_val"]
        file_prev_l = res_l["last_val"]
        file_max_s = res_s["new_running_max"]
        file_max_l = res_l["new_running_max"]

        # --- Global analysis ---
        if global_state is not None:
            g_res_s = analyze_series(stimes, global_state["prev_s"], global_state["max_s"])
            g_res_l = analyze_series(ltimes, global_state["prev_l"], global_state["max_l"])

            global_state["total_n"] += chunk_n
            global_state["inv_s"] += g_res_s["inv_count"]
            global_state["inv_l"] += g_res_l["inv_count"]
            global_state["ooo_s"] += g_res_s["ooo_count"]
            global_state["ooo_l"] += g_res_l["ooo_count"]
            if len(g_res_s["lateness"]) > 0:
                global_state["lateness_s"].append(g_res_s["lateness"])
            if len(g_res_l["lateness"]) > 0:
                global_state["lateness_l"].append(g_res_l["lateness"])
            for w in WATERMARK_CANDIDATES:
                global_state["wm_drops_s"][w] += g_res_s["wm_drops"][w]
                global_state["wm_drops_l"][w] += g_res_l["wm_drops"][w]

            global_state["prev_s"] = g_res_s["last_val"]
            global_state["prev_l"] = g_res_l["last_val"]
            global_state["max_s"] = g_res_s["new_running_max"]
            global_state["max_l"] = g_res_l["new_running_max"]

    lat_s_cat = np.concatenate(file_lateness_s) if file_lateness_s else np.array([])
    lat_l_cat = np.concatenate(file_lateness_l) if file_lateness_l else np.array([])

    def summarize(n, inv, ooo, lat_arr, wm_drops):
        pctls = {
            "p50": float(np.percentile(lat_arr, 50)) if len(lat_arr) > 0 else 0.0,
            "p90": float(np.percentile(lat_arr, 90)) if len(lat_arr) > 0 else 0.0,
            "p99": float(np.percentile(lat_arr, 99)) if len(lat_arr) > 0 else 0.0,
            "p99_9": float(np.percentile(lat_arr, 99.9)) if len(lat_arr) > 0 else 0.0,
            "max": float(np.max(lat_arr)) if len(lat_arr) > 0 else 0.0,
        }
        wm_pcts = {f"{w}s": {"dropped_count": wm_drops[w], "dropped_pct": wm_drops[w] / n * 100.0} for w in WATERMARK_CANDIDATES}
        return {
            "total_rows": n,
            "pairwise_inversions": inv,
            "pairwise_inversion_pct": inv / n * 100.0,
            "ooo_rows": ooo,
            "ooo_pct": ooo / n * 100.0,
            "lateness_percentiles": pctls,
            "watermark_drops": wm_pcts
        }

    file_summary_s = summarize(file_n, file_inv_s, file_ooo_s, lat_s_cat, file_wm_drops_s)
    file_summary_l = summarize(file_n, file_inv_l, file_ooo_l, lat_l_cat, file_wm_drops_l)

    print(f"  {csv_path.name} ({file_n:,} rows in {time.time()-t0:.1f}s):")
    print(f"    stime: pairwise < prev = {file_summary_s['pairwise_inversion_pct']:.2f}% | ooo < running_max = {file_summary_s['ooo_pct']:.2f}% | p50={file_summary_s['lateness_percentiles']['p50']:.1f}s, p99={file_summary_s['lateness_percentiles']['p99']:.1f}s, max={file_summary_s['lateness_percentiles']['max']:.1f}s")
    print(f"    ltime: pairwise < prev = {file_summary_l['pairwise_inversion_pct']:.2f}% | ooo < running_max = {file_summary_l['ooo_pct']:.2f}% | p50={file_summary_l['lateness_percentiles']['p50']:.1f}s, p99={file_summary_l['lateness_percentiles']['p99']:.1f}s, max={file_summary_l['lateness_percentiles']['max']:.1f}s")
    print(f"    stime watermark drops: 10s={file_summary_s['watermark_drops']['10s']['dropped_pct']:.4f}%, 30s={file_summary_s['watermark_drops']['30s']['dropped_pct']:.4f}%, 60s={file_summary_s['watermark_drops']['60s']['dropped_pct']:.4f}%, 300s={file_summary_s['watermark_drops']['300s']['dropped_pct']:.4f}%")

    return file_summary_s, file_summary_l

# Execute across all 4 files
global_state = {
    "total_n": 0,
    "prev_s": None, "prev_l": None,
    "max_s": -float("inf"), "max_l": -float("inf"),
    "inv_s": 0, "inv_l": 0,
    "ooo_s": 0, "ooo_l": 0,
    "lateness_s": [], "lateness_l": [],
    "wm_drops_s": {w: 0 for w in WATERMARK_CANDIDATES},
    "wm_drops_l": {w: 0 for w in WATERMARK_CANDIDATES}
}

file_results = {}
for fpath in RAW_CSVS:
    res_s, res_l = process_file(fpath, global_state)
    file_results[fpath.name] = {"stime": res_s, "ltime": res_l}

# Summarize global results
g_lat_s = np.concatenate(global_state["lateness_s"]) if global_state["lateness_s"] else np.array([])
g_lat_l = np.concatenate(global_state["lateness_l"]) if global_state["lateness_l"] else np.array([])
g_n = global_state["total_n"]

def summarize_global(ts_name, inv, ooo, lat_arr, wm_drops):
    pctls = {
        "p50": float(np.percentile(lat_arr, 50)) if len(lat_arr) > 0 else 0.0,
        "p90": float(np.percentile(lat_arr, 90)) if len(lat_arr) > 0 else 0.0,
        "p99": float(np.percentile(lat_arr, 99)) if len(lat_arr) > 0 else 0.0,
        "p99_9": float(np.percentile(lat_arr, 99.9)) if len(lat_arr) > 0 else 0.0,
        "max": float(np.max(lat_arr)) if len(lat_arr) > 0 else 0.0,
    }
    wm_pcts = {f"{w}s": {"dropped_count": wm_drops[w], "dropped_pct": wm_drops[w] / g_n * 100.0} for w in WATERMARK_CANDIDATES}
    return {
        "total_rows": g_n,
        "pairwise_inversions": inv,
        "pairwise_inversion_pct": inv / g_n * 100.0,
        "ooo_rows": ooo,
        "ooo_pct": ooo / g_n * 100.0,
        "lateness_percentiles": pctls,
        "watermark_drops": wm_pcts
    }

global_summary = {
    "stime": summarize_global("stime", global_state["inv_s"], global_state["ooo_s"], g_lat_s, global_state["wm_drops_s"]),
    "ltime": summarize_global("ltime", global_state["inv_l"], global_state["ooo_l"], g_lat_l, global_state["wm_drops_l"])
}

print("\n" + "="*70)
print("GLOBAL SUMMARY ACROSS ALL 2,540,047 ROWS (In Sequential File Order)")
print("="*70)
print(f"STIME:")
print(f"  Pairwise < previous:  {global_summary['stime']['pairwise_inversion_pct']:.2f}% ({global_summary['stime']['pairwise_inversions']:,} rows)")
print(f"  Out-of-order < max:   {global_summary['stime']['ooo_pct']:.2f}% ({global_summary['stime']['ooo_rows']:,} rows)")
print(f"  Lateness percentiles: p50={global_summary['stime']['lateness_percentiles']['p50']:.1f}s, p90={global_summary['stime']['lateness_percentiles']['p90']:.1f}s, p99={global_summary['stime']['lateness_percentiles']['p99']:.1f}s, max={global_summary['stime']['lateness_percentiles']['max']:.1f}s")
print(f"  Watermark drop %:")
for w, d in global_summary['stime']['watermark_drops'].items():
    print(f"    Watermark {w}: dropped {d['dropped_count']:,} ({d['dropped_pct']:.4f}%)")

print(f"\nLTIME:")
print(f"  Pairwise < previous:  {global_summary['ltime']['pairwise_inversion_pct']:.2f}% ({global_summary['ltime']['pairwise_inversions']:,} rows)")
print(f"  Out-of-order < max:   {global_summary['ltime']['ooo_pct']:.2f}% ({global_summary['ltime']['ooo_rows']:,} rows)")
print(f"  Lateness percentiles: p50={global_summary['ltime']['lateness_percentiles']['p50']:.1f}s, p90={global_summary['ltime']['lateness_percentiles']['p90']:.1f}s, p99={global_summary['ltime']['lateness_percentiles']['p99']:.1f}s, max={global_summary['lateness_percentiles']['max']:.1f}s" if 'lateness_percentiles' in global_summary else f"  Lateness max: {global_summary['ltime']['lateness_percentiles']['max']:.1f}s")
print(f"  Watermark drop %:")
for w, d in global_summary['ltime']['watermark_drops'].items():
    print(f"    Watermark {w}: dropped {d['dropped_count']:,} ({d['dropped_pct']:.4f}%)")

peak_mem = get_rss_mb()
print(f"\nPeak Memory RSS: {peak_mem:.2f} MB")

out_dir = ROOT / "reports" / "verification"
out_dir.mkdir(parents=True, exist_ok=True)
out_json = out_dir / "c1_lateness_verification.json"
out_data = {
    "file_results": file_results,
    "global_summary": global_summary,
    "peak_memory_mb": peak_mem
}
out_json.write_text(json.dumps(out_data, indent=2), encoding="utf-8")
print(f"Results written to: {out_json}")
