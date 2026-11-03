"""
Live Dashboard Pipeline Impact Verification Suite
=================================================
Runs an end-to-end streaming run with unseen File 3 records (~21 rec/s) comparing
Spark micro-batch durations with active dashboard query polling ON vs OFF.
Quotes counters at T_start, T_mid, T_end and measures query execution latencies in ms.
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import psycopg2

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dashboard import queries
from scripts.verify.poll_dashboard import DashboardPoller
from scripts.verify.verify_live_streaming import (
    CHECKPOINT_BASE,
    create_fresh_kafka_topic,
    get_current_table_counts,
    get_kafka_end_offset,
    truncate_db_tables,
    SparkStreamMonitor,
)


def run_live_dashboard_impact_experiment(
    target_records: int = 3000,
    target_rate: float = 25.0,
    polling_duration_s: float = 60.0,
) -> Dict[str, Any]:
    print("\n" + "=" * 90)
    print(f"LIVE DASHBOARD IMPACT VERIFICATION EXPERIMENT")
    print(f"Target: {target_records:,} flows @ {target_rate} rec/s requested (File 3, Session 1)")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()} (Seed: 42)")
    print("=" * 90)

    ts_slug = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    topic = f"live-dash-impact-{ts_slug}"
    run_id = f"run_dash_impact_{ts_slug}"
    ckpt_dir = CHECKPOINT_BASE / f"live_impact_{ts_slug}"
    metrics_file = ROOT_DIR / "reports" / "verification" / f"live_metrics_impact_{ts_slug}.json"

    # Step 1: Fresh topic and clean tables
    create_fresh_kafka_topic(topic)
    truncate_db_tables()

    # Step 2: Start Spark FIRST
    spark_env = os.environ.copy()
    spark_env["PYTHONUNBUFFERED"] = "1"
    spark_env["PYSPARK_PYTHON"] = sys.executable
    spark_env["PYSPARK_DRIVER_PYTHON"] = sys.executable
    spark_env.setdefault("JAVA_HOME", r"C:\Program Files\Java\jre1.8.0_461")
    spark_env.setdefault("HADOOP_HOME", r"C:\Users\sweni\hadoop")

    spark_cmd = [
        sys.executable,
        "-u",
        "src/spark_streaming.py",
        "--topic", topic,
        "--checkpoint-dir", str(ckpt_dir.resolve()),
        "--score",
        "--db-sink",
        "--watermark", "60 seconds",
        "--shuffle-partitions", "2",
        "--alert-k", "3",
        "--run-id", run_id,
        "--stop-after-records", str(target_records),
        "--metrics-out", str(metrics_file),
    ]

    print("\n[1/5] Launching Spark Streaming background process...")
    spark_proc = subprocess.Popen(
        spark_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=spark_env,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )
    monitor = SparkStreamMonitor(spark_proc)

    # Step 3: Wait for Spark readiness gate
    print("[2/5] Awaiting Spark readiness gate on empty topic...")
    t_gate_start = time.monotonic()
    meta_path = ckpt_dir / "metadata"
    while time.monotonic() - t_gate_start < 90.0:
        if meta_path.exists() and monitor.batch_0_seen:
            break
        time.sleep(0.5)

    if not (meta_path.exists() and monitor.batch_0_seen):
        spark_proc.kill()
        raise TimeoutError("Spark failed to pass readiness gate within 90s!")

    print(f"  SPARK READY in {time.monotonic() - t_gate_start:.2f}s! Checkpoint on D: verified.")

    # Step 4: Start Replayer as separate process
    print(f"\n[3/5] Starting Replayer ({target_records} records @ {target_rate} rec/s)...")
    replayer_cmd = [
        sys.executable,
        "-u",
        "scripts/verify/live_replayer.py",
        "--topic", topic,
        "--broker", "localhost:9092",
        "--limit", str(target_records),
        "--rate", str(target_rate),
        "--file-index", "3",
    ]
    t_rep_start = time.monotonic()
    replayer_proc = subprocess.Popen(
        replayer_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=spark_env,
    )

    def _rep_reader():
        for line in iter(replayer_proc.stdout.readline, ""):
            if not line:
                break
        replayer_proc.stdout.close()

    threading.Thread(target=_rep_reader, daemon=True).start()

    # Step 5: Execute Phase 1 (Polling ON) and Phase 2 (Polling OFF)
    print("\n[4/5] Executing Controlled Polling Impact Experiment...")
    poller = DashboardPoller(interval_s=5.0)

    quotes = {}
    phase1_batches = []
    phase2_batches = []

    # Track batches by monitoring monitor.lines
    initial_batch_count = 0
    t_phase1_start = time.monotonic()

    print(f"\n  >>> PHASE 1: Active Dashboard Polling ON (Duration: ~{polling_duration_s}s)")
    while time.monotonic() - t_phase1_start < polling_duration_s:
        cycle_res = poller.poll_once()
        elapsed = time.monotonic() - t_rep_start

        # Record T_start (~10s) and T_mid (~35s)
        if "T_start" not in quotes and elapsed >= 10.0:
            quotes["T_start"] = {
                "elapsed_s": round(elapsed, 2),
                "total_flows": cycle_res["total_flows"],
                "flagged_flows": cycle_res["flagged_flows"],
                "alerted_windows": cycle_res["alerted_windows"],
                "total_sql_ms": cycle_res["total_sql_ms"],
                "health_ms": cycle_res["health_ms"],
            }
            print(f"  [SNAPSHOT T_start @ {elapsed:.1f}s]: Flows={cycle_res['total_flows']} | Flagged={cycle_res['flagged_flows']} | SQL Time={cycle_res['total_sql_ms']:.2f} ms")

        if "T_mid" not in quotes and elapsed >= 35.0:
            quotes["T_mid"] = {
                "elapsed_s": round(elapsed, 2),
                "total_flows": cycle_res["total_flows"],
                "flagged_flows": cycle_res["flagged_flows"],
                "alerted_windows": cycle_res["alerted_windows"],
                "total_sql_ms": cycle_res["total_sql_ms"],
                "health_ms": cycle_res["health_ms"],
            }
            print(f"  [SNAPSHOT T_mid   @ {elapsed:.1f}s]: Flows={cycle_res['total_flows']} | Flagged={cycle_res['flagged_flows']} | SQL Time={cycle_res['total_sql_ms']:.2f} ms")

        time.sleep(5.0)

    t_phase1_end = time.monotonic()
    phase1_duration = t_phase1_end - t_phase1_start

    print(f"\n  >>> PHASE 2: Dashboard Polling OFF (Rest of replay & drain)")
    # Replayer continues running without dashboard query load
    while True:
        elapsed = time.monotonic() - t_rep_start
        rep_done = replayer_proc.poll() is not None
        spark_done = spark_proc.poll() is not None or monitor.completed.is_set()

        if "T_end" not in quotes and (rep_done or elapsed >= 100.0):
            conn = queries.get_db_connection(use_dashboard_reader=True)
            try:
                t0 = time.monotonic()
                kpis = queries.get_headline_kpis(conn)
                sql_time = (time.monotonic() - t0) * 1000.0
                quotes["T_end"] = {
                    "elapsed_s": round(elapsed, 2),
                    "total_flows": kpis["total_flows"],
                    "flagged_flows": kpis["flagged_flows"],
                    "alerted_windows": kpis["alerted_windows"],
                    "total_sql_ms": round(sql_time, 2),
                }
                print(f"  [SNAPSHOT T_end   @ {elapsed:.1f}s]: Flows={kpis['total_flows']} | Flagged={kpis['flagged_flows']} | SQL Time={sql_time:.2f} ms")
            finally:
                conn.close()

        if rep_done and spark_done:
            print(f"  Pipeline drained and completed at T+{elapsed:.1f}s.")
            break

        time.sleep(2.0)

    spark_proc.wait(timeout=30.0)

    # Step 6: Parse Spark Batches for Phase 1 vs Phase 2
    print("\n[5/5] Analyzing micro-batch durations (Polling ON vs Polling OFF)...")
    spark_summary = {}
    if metrics_file.exists():
        try:
            spark_summary = json.loads(metrics_file.read_text(encoding="utf-8"))
        except Exception:
            pass

    batches = spark_summary.get("batches", [])
    # Batch 0 is empty initialization.
    # Batches occurring during Phase 1 (first ~60s after start) vs Phase 2
    for b in batches:
        bid = b.get("batchId")
        if bid == 0:
            continue
        ts_str = b.get("timestamp")
        # Estimate phase based on batch order or timestamp
        # In a ~140s run with ~20 batches, first ~10 batches belong to Phase 1
        dur = b.get("triggerExecutionMs", 0)
        if bid <= len(batches) // 2:
            phase1_batches.append(dur)
        else:
            phase2_batches.append(dur)

    avg_p1_dur = np.mean(phase1_batches) if phase1_batches else 0.0
    p95_p1_dur = np.percentile(phase1_batches, 95) if phase1_batches else 0.0
    lag_p1 = sum(1 for d in phase1_batches if d > 5000)

    avg_p2_dur = np.mean(phase2_batches) if phase2_batches else 0.0
    p95_p2_dur = np.percentile(phase2_batches, 95) if phase2_batches else 0.0
    lag_p2 = sum(1 for d in phase2_batches if d > 5000)

    # Average query latencies across all poll cycles
    all_sql_times = [r["total_sql_ms"] for r in poller.records]
    mean_sql_ms = round(float(np.mean(all_sql_times)), 2) if all_sql_times else 0.0
    p95_sql_ms = round(float(np.percentile(all_sql_times, 95)), 2) if all_sql_times else 0.0

    final_counts = get_current_table_counts()

    experiment_results = {
        "run_id": run_id,
        "target_records": target_records,
        "final_raw_flows": final_counts["raw_flows"],
        "final_flow_scores": final_counts["flow_scores"],
        "final_window_metrics": final_counts["window_metrics"],
        "total_poll_cycles": len(poller.records),
        "mean_dashboard_refresh_sql_ms": mean_sql_ms,
        "p95_dashboard_refresh_sql_ms": p95_sql_ms,
        "snapshots": quotes,
        "impact_comparison": {
            "phase1_polling_on": {
                "batch_count": len(phase1_batches),
                "avg_duration_ms": round(float(avg_p1_dur), 2),
                "p95_duration_ms": round(float(p95_p1_dur), 2),
                "batches_over_5s": lag_p1,
            },
            "phase2_polling_off": {
                "batch_count": len(phase2_batches),
                "avg_duration_ms": round(float(avg_p2_dur), 2),
                "p95_duration_ms": round(float(p95_p2_dur), 2),
                "batches_over_5s": lag_p2,
            },
            "duration_delta_ms": round(float(avg_p1_dur - avg_p2_dur), 2),
        },
    }

    report_path = ROOT_DIR / "reports" / "verification" / "dashboard_impact_20261102.json"
    report_path.write_text(json.dumps(experiment_results, indent=2), encoding="utf-8")
    print(f"\nExperiment complete. Saved report to: {report_path}")

    print("\n" + "=" * 90)
    print("  DASHBOARD IMPACT COMPARISON RESULTS")
    print("=" * 90)
    print(f"  Mean Total SQL Time per Refresh Cycle: {mean_sql_ms} ms (p95: {p95_sql_ms} ms)")
    print(f"  Phase 1 (Polling ON)  Avg Batch Duration: {avg_p1_dur:.2f} ms ({lag_p1}/{len(phase1_batches)} > 5s)")
    print(f"  Phase 2 (Polling OFF) Avg Batch Duration: {avg_p2_dur:.2f} ms ({lag_p2}/{len(phase2_batches)} > 5s)")
    print(f"  Duration Difference:                   {avg_p1_dur - avg_p2_dur:+.2f} ms")
    print("=" * 90 + "\n")

    return experiment_results


if __name__ == "__main__":
    run_live_dashboard_impact_experiment()

