"""Full-data evaluation: memory-optimised for 7.8 GB RAM.

Key changes vs previous version:
 - assign session IN PLACE (no df.copy())
 - drop unneeded columns immediately after load
 - downcast float64→float32, int64→int32 where possible
 - run ONE direction at a time, freeing the other session early
 - explicit gc.collect() between stages
"""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import json, time, gc, os
import numpy as np
import pandas as pd
import psutil
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, average_precision_score, confusion_matrix,
    precision_recall_curve,
)
from src.raw_loader import FEATURE_NAMES, COLUMN_NAMES, RAW_DIR, clean_attack_cat
from src.anomaly.preprocessing import FrozenPreprocessor

SESSION_GAP_THRESHOLD = 1_423_000_000
SEED = 42
np.random.seed(SEED)
print(f"SEED: {SEED}")

proc = psutil.Process(os.getpid())
def mem_mb():
    return proc.memory_info().rss / (1024 * 1024)

# Columns we actually need (41 features + metadata)
KEEP_COLS = set(FEATURE_NAMES) | {"label", "attack_cat", "stime", "ltime"}

# ─── Step 1: Load ────────────────────────────────────────────────
print(f"Starting memory: {mem_mb():.1f} MB")
print("\n" + "=" * 70)
print("STEP 1: Loading Full UNSW-NB15 Raw Dataset")
print("=" * 70)
t_start = time.time()

dfs = []
for fid in [1, 2, 3, 4]:
    csv_path = RAW_DIR / f"UNSW-NB15_{fid}.csv"
    if not csv_path.exists():
        print(f"  WARNING: {csv_path} not found"); continue
    print(f"  Loading file {fid}: {csv_path.name} ...", end=" ", flush=True)
    chunk = pd.read_csv(
        csv_path, header=None, names=COLUMN_NAMES,
        encoding="utf-8-sig", on_bad_lines="skip", low_memory=False,
    )
    # Drop columns we don't need to save RAM
    drop = [c for c in chunk.columns if c not in KEEP_COLS]
    chunk.drop(columns=drop, inplace=True)
    print(f"{len(chunk):,} rows")
    dfs.append(chunk)

df = pd.concat(dfs, ignore_index=True)
del dfs; gc.collect()
print(f"Total loaded: {len(df):,} rows, {len(df.columns)} columns | Memory: {mem_mb():.1f} MB")

# ─── Clean fields ────────────────────────────────────────────────
if "ct_ftp_cmd" in df.columns:
    df["ct_ftp_cmd"] = pd.to_numeric(df["ct_ftp_cmd"], errors="coerce").fillna(0)
if "attack_cat" in df.columns:
    df["attack_cat"] = df["attack_cat"].fillna("Normal").apply(clean_attack_cat)
for col in ["service", "state"]:
    if col in df.columns:
        df[col] = df[col].fillna("none").apply(
            lambda v: "none" if str(v).strip() in ("-", "") else str(v).strip()
        )
if "label" in df.columns:
    df["label"] = pd.to_numeric(df["label"], errors="coerce").fillna(0).astype("int8")
for col in FEATURE_NAMES:
    if col in df.columns and col not in ("proto", "service", "state"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("float32")
for col in ["stime", "ltime"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("float64")

gc.collect()
print(f"After cleaning: Memory: {mem_mb():.1f} MB")

# ─── Assign sessions IN PLACE ────────────────────────────────────
df["session"] = (df["stime"] >= SESSION_GAP_THRESHOLD).astype("int8")

# Raw stats (before dedup)
for sid in [0, 1]:
    mask = df["session"] == sid
    n = mask.sum()
    atk = ((df["label"] == 1) & mask).sum()
    print(f"Session {sid} raw: {n:,} rows, {atk:,} attacks ({atk/n*100:.2f}%)")

# ─── Step 2: Deduplication ───────────────────────────────────────
print("\n" + "=" * 70)
print("STEP 2: Model-view deduplication")
print("=" * 70)
t0 = time.time()
before = len(df)
df.drop_duplicates(subset=FEATURE_NAMES + ["label"], keep="first", inplace=True)
df.reset_index(drop=True, inplace=True)
after = len(df)
print(f"  Dedup: {before:,} -> {after:,}  (removed {before-after:,}, {(before-after)/before*100:.1f}%)")
print(f"  Dedup time: {time.time()-t0:.1f}s | Memory: {mem_mb():.1f} MB")

# Stats per session after dedup
stats = {}
for sid in [0, 1]:
    mask = df["session"] == sid
    n = int(mask.sum())
    atk = int(((df["label"] == 1) & mask).sum())
    pct = atk / n * 100 if n > 0 else 0.0
    print(f"Session {sid} dedup: {n:,} rows, {atk:,} attacks ({pct:.2f}%)")
    stats[f"s{sid}_dedup_rows"] = n
    stats[f"s{sid}_dedup_attacks"] = atk
    stats[f"s{sid}_dedup_attack_pct"] = pct

all_results = {"dataset_stats": stats}

# ─── Helper: evaluate one direction ─────────────────────────────
def get_feature_matrix(sub_df):
    X = sub_df[FEATURE_NAMES].copy()
    y = sub_df["label"].copy()
    return X, y

def evaluate_direction(train_sid, test_sid, calib_frac=0.2):
    tag = f"S{train_sid}->S{test_sid}"
    print("\n" + "=" * 70)
    print(f"EVALUATING: {tag}")
    print("=" * 70)

    train_df = df[df["session"] == train_sid].sort_values("stime").reset_index(drop=True)
    test_df  = df[df["session"] == test_sid].sort_values("stime").reset_index(drop=True)

    n = len(train_df)
    split_idx = int(n * (1.0 - calib_frac))
    train_prefix = train_df.iloc[:split_idx]
    calib_split  = train_df.iloc[split_idx:]

    print(f"Train prefix: {len(train_prefix):,} (atk {(train_prefix['label']==1).sum():,}, {(train_prefix['label']==1).mean()*100:.2f}%)")
    print(f"Calibration:  {len(calib_split):,} (atk {(calib_split['label']==1).sum():,}, {(calib_split['label']==1).mean()*100:.2f}%)")
    print(f"Test:         {len(test_df):,} (atk {(test_df['label']==1).sum():,}, {(test_df['label']==1).mean()*100:.2f}%)")

    X_train, y_train = get_feature_matrix(train_prefix)
    X_calib, y_calib = get_feature_matrix(calib_split)
    X_test,  y_test  = get_feature_matrix(test_df)

    pp = FrozenPreprocessor()
    X_train_p = pp.fit_transform(X_train, provenance_info={"train": f"S{train_sid}_prefix_80%"})
    del X_train; gc.collect()
    X_calib_p = pp.transform(X_calib)
    del X_calib; gc.collect()
    X_test_p  = pp.transform(X_test)
    del X_test; gc.collect()

    rf = RandomForestClassifier(
        n_estimators=100, max_depth=20, min_samples_leaf=5,
        class_weight="balanced", random_state=SEED, n_jobs=-1,
    )
    t0 = time.time()
    rf.fit(X_train_p, y_train)
    fit_time = time.time() - t0
    print(f"RF fit: {fit_time:.1f}s | Memory: {mem_mb():.1f} MB")
    del X_train_p; gc.collect()

    # Calibrate threshold on calib split
    y_sc = rf.predict_proba(X_calib_p)[:, 1]
    pr, rc, th = precision_recall_curve(y_calib, y_sc)
    f1s = 2 * (pr * rc) / (pr + rc + 1e-10)
    bi = np.argmax(f1s)
    best_thresh = float(th[min(bi, len(th)-1)]) if len(th) > 0 else 0.5
    print(f"Calibrated threshold: {best_thresh:.4f} (Calib F1: {f1s[bi]:.4f})")
    del X_calib_p, y_sc; gc.collect()

    # Score test
    t0 = time.time()
    y_score = rf.predict_proba(X_test_p)[:, 1]
    score_time = time.time() - t0
    y_pred = (y_score >= best_thresh).astype(int)

    dur_h = (test_df["stime"].max() - test_df["stime"].min()) / 3600.0

    cm = confusion_matrix(y_test, y_pred)
    tn, fp, fn, tp = cm.ravel()
    fpr = float(fp / (fp + tn)) if (fp + tn) > 0 else 0.0
    alerts_hr = float((tp + fp) / dur_h) if dur_h > 0 else 0.0

    acc  = float(accuracy_score(y_test, y_pred))
    prec = float(precision_score(y_test, y_pred, zero_division=0))
    rec  = float(recall_score(y_test, y_pred, zero_division=0))
    f1v  = float(f1_score(y_test, y_pred, zero_division=0))
    roc  = float(roc_auc_score(y_test, y_score))
    prauc = float(average_precision_score(y_test, y_score))

    print(f"\nResults on test S{test_sid} (thresh={best_thresh:.4f}):")
    print(f"  Accuracy:  {acc:.4f}")
    print(f"  Precision: {prec:.4f}")
    print(f"  Recall:    {rec:.4f}")
    print(f"  F1-Score:  {f1v:.4f}")
    print(f"  ROC-AUC:   {roc:.4f}")
    print(f"  PR-AUC:    {prauc:.4f}")
    print(f"  FPR:       {fpr:.6f}")
    print(f"  Alerts/hr: {alerts_hr:.1f}")
    print(f"  TP={tp:,}  FP={fp:,}  TN={tn:,}  FN={fn:,}")

    # Per-class recall
    pcr = {}
    print("\nPer-class Recall:")
    for cat in sorted(test_df["attack_cat"].dropna().unique()):
        mask = test_df["attack_cat"].values == cat
        cnt = int(mask.sum())
        if cnt == 0:
            continue
        r = float((y_pred[mask] == 1).mean())
        flag = " [UNRELIABLE: n<50]" if cnt < 50 else ""
        print(f"  {cat:15s}: {r:.4f} (n={cnt:,}){flag}")
        pcr[cat] = {"count": cnt, "recall": r, "unreliable": cnt < 50}

    # DoS / Analysis / Backdoor confusion
    dab = {}
    print("\nDoS / Analysis / Backdoor confusion:")
    for cat in ["DoS", "Analysis", "Backdoor"]:
        mask = test_df["attack_cat"].values == cat
        cnt = int(mask.sum())
        if cnt > 0:
            flagged = int((y_pred[mask] == 1).sum())
            missed = cnt - flagged
            print(f"  {cat:10s} (n={cnt:,}): Flagged={flagged:,} ({flagged/cnt*100:.1f}%), Missed={missed:,} ({missed/cnt*100:.1f}%)")
            dab[cat] = {"total": cnt, "flagged": flagged, "missed": missed, "recall": flagged/cnt}

    # Feature importances
    fi = sorted(zip(list(X_test_p.columns), rf.feature_importances_), key=lambda x: x[1], reverse=True)
    print("\nTop 10 Feature Importances:")
    for name, imp in fi[:10]:
        print(f"  {name:25s}: {imp:.4f}")

    del X_test_p, rf; gc.collect()

    return {
        "calibrated_threshold": best_thresh,
        "accuracy": acc, "precision": prec, "recall": rec, "f1": f1v,
        "roc_auc": roc, "pr_auc": prauc, "fpr": fpr, "alerts_per_hour": alerts_hr,
        "tp": int(tp), "fp": int(fp), "tn": int(tn), "fn": int(fn),
        "test_duration_hours": dur_h,
        "rf_train_time_s": fit_time, "scoring_time_s": score_time,
        "per_class_recall": pcr,
        "dos_analysis_backdoor_confusion": dab,
        "top_features": {n: float(v) for n, v in fi[:15]},
    }

# ─── Run both directions ─────────────────────────────────────────
all_results["S0_to_S1"] = evaluate_direction(0, 1)
gc.collect()
all_results["S1_to_S0"] = evaluate_direction(1, 0)
gc.collect()

peak = mem_mb()
total_t = time.time() - t_start
all_results["performance"] = {"peak_memory_mb": peak, "total_runtime_s": total_t}

print("\n" + "=" * 70)
print(f"DONE  |  Peak memory: {peak:.0f} MB  |  Total: {total_t:.0f}s ({total_t/60:.1f} min)")
print("=" * 70)

out = ROOT / "reports" / "anomaly_detection" / "anomaly_detection_results_20261031.json"
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(all_results, indent=2, default=str), encoding="utf-8")
print(f"Results saved to: {out}")
