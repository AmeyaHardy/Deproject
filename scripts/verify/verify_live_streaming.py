"""
Live Streaming Verification Suite: Kafka -> Spark (Scoring) -> PostgreSQL
===========================================================================
Executes live streaming tests where Spark starts FIRST on an empty topic and
processes live micro-batches concurrently while the replayer feeds traffic.

Runs:
  - Run A: 5,000 records at 25 rec/s requested (out-of-sample File 3, Session 1)
  - Run B1: 3,000 records at 50 rec/s requested
  - Run B2: 3,000 records at 100 rec/s requested
  - Run D: 5,000 records idempotent rerun against existing DB (no truncation)
"""

import argparse
import csv
import json
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import psycopg2
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.admin import NewTopic

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.db_sink import get_connection, get_db_config, load_env_file
from src.raw_loader import iter_raw_records

load_env_file()

# Disk directories
CHECKPOINT_BASE = Path("D:/spark-checkpoints")
SPARK_TEMP = Path("D:/spark_temp")
CHECKPOINT_BASE.mkdir(parents=True, exist_ok=True)
SPARK_TEMP.mkdir(parents=True, exist_ok=True)


def check_disk_space(min_c_gb: float = 5.0, min_d_gb: float = 5.0) -> Tuple[float, float]:
    import shutil
    c_free = shutil.disk_usage("C:").free / (1024**3)
    d_free = shutil.disk_usage("D:").free / (1024**3)
    print(f"Disk check: C: {c_free:.2f} GB free | D: {d_free:.2f} GB free")
    if c_free < min_c_gb:
        raise RuntimeError(f"ABORT: C: drive free space ({c_free:.2f} GB) is below {min_c_gb} GB threshold!")
    if d_free < min_d_gb:
        raise RuntimeError(f"ABORT: D: drive free space ({d_free:.2f} GB) is below {min_d_gb} GB threshold!")
    return c_free, d_free


def get_current_table_counts() -> Dict[str, int]:
    conn = get_connection(use_superuser=True)
    counts = {}
    try:
        with conn.cursor() as cur:
            for tbl in ["raw_flows", "flow_scores", "window_metrics", "model_runs"]:
                cur.execute(f"SELECT count(*) FROM {tbl};")
                counts[tbl] = cur.fetchone()[0]
    finally:
        conn.close()
    return counts


def truncate_db_tables() -> Dict[str, int]:
    counts = get_current_table_counts()
    print(f"Table row counts before truncation: {counts}")
    conn = get_connection(use_superuser=True)
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE flow_scores, raw_flows, window_metrics CASCADE;")
        conn.commit()
        print("PostgreSQL tables truncated cleanly: flow_scores, raw_flows, window_metrics.")
    finally:
        conn.close()
    return counts


def create_fresh_kafka_topic(topic_name: str, broker: str = "localhost:9092") -> None:
    admin = KafkaAdminClient(bootstrap_servers=broker)
    try:
        topic = NewTopic(
            name=topic_name,
            num_partitions=1,
            replication_factor=1,
            topic_configs={"retention.ms": "-1", "retention.bytes": "-1"},
        )
        admin.create_topics(new_topics=[topic], validate_only=False)
        print(f"Created fresh Kafka topic '{topic_name}' (retention.ms=-1).")
    finally:
        admin.close()


def get_kafka_end_offset(topic: str, broker: str = "localhost:9092") -> int:
    consumer = KafkaConsumer(
        bootstrap_servers=broker,
        enable_auto_commit=False,
        group_id=f"offset-checker-{int(time.time()*1000)}",
        auto_offset_reset="earliest",
    )
    try:
        tp = TopicPartition(topic, 0)
        consumer.assign([tp])
        consumer.seek_to_end(tp)
        return consumer.position(tp)
    finally:
        consumer.close()


def get_spark_committed_offset(ckpt_dir: Path, topic: str) -> Optional[int]:
    commits_dir = ckpt_dir / "commits"
    offsets_dir = ckpt_dir / "offsets"
    if not commits_dir.exists() or not offsets_dir.exists():
        return None

    commit_nums = []
    for f in commits_dir.iterdir():
        if f.is_file() and not f.name.endswith(".crc") and f.name.isdigit():
            commit_nums.append(int(f.name))

    if not commit_nums:
        return None

    latest_commit = max(commit_nums)
    offset_file = offsets_dir / str(latest_commit)
    if not offset_file.exists():
        return None

    try:
        lines = [l.strip() for l in offset_file.read_text(encoding="utf-8").splitlines() if l.strip()]
        if len(lines) >= 3:
            data = json.loads(lines[2])
            if topic in data:
                return data[topic].get("0")
    except Exception:
        pass
    return None


class SparkStreamMonitor:
    """Monitors Spark process stdout and manages state."""

    def __init__(self, proc: subprocess.Popen):
        self.proc = proc
        self.ready_event = threading.Event()
        self.batch_0_seen = False
        self.ready_time: Optional[float] = None
        self.recorded_batches: List[Dict[str, Any]] = []
        self.latest_batch_duration_ms: float = 0.0
        self.latest_cumulative_rows: int = 0
        self.completed = threading.Event()
        self.lines: List[str] = []
        self._thread = threading.Thread(target=self._reader, daemon=True)
        self._thread.start()

    def _reader(self):
        for line in iter(self.proc.stdout.readline, ""):
            if not line:
                break
            line_str = line.strip()
            self.lines.append(line_str)
            print(f"    [SPARK] {line_str}")

            if "[BENCHMARK Batch 0]" in line_str:
                self.batch_0_seen = True
                self.ready_time = time.monotonic()
                self.ready_event.set()

            if "[BENCHMARK Batch " in line_str and "]" in line_str:
                # e.g.: [BENCHMARK Batch 1] Input: 120 rows | Duration: 2314 ms | Cumulative: 120/5000
                try:
                    parts = line_str.split("|")
                    dur_part = parts[1].strip()
                    dur_ms = float(dur_part.split()[1])
                    cum_part = parts[2].strip()
                    cum_rows = int(cum_part.split()[1].split("/")[0])
                    self.latest_batch_duration_ms = dur_ms
                    self.latest_cumulative_rows = cum_rows
                except Exception:
                    pass

            if "Target of " in line_str and "records reached" in line_str:
                self.completed.set()

            if "Spark session stopped." in line_str:
                self.completed.set()

        self.proc.stdout.close()
        self.completed.set()


def execute_live_run(
    run_tag: str,
    target_records: int,
    target_rate: float,
    truncate_first: bool = True,
    broker: str = "localhost:9092",
    watermark: str = "60 seconds",
    alert_k: int = 3,
) -> Dict[str, Any]:
    print("\n" + "=" * 90)
    print(f"EXECUTING RUN {run_tag}: {target_records:,} records @ {target_rate} rec/s requested")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()} (Seed: 42)")
    print("=" * 90)

    check_disk_space(min_c_gb=5.0, min_d_gb=5.0)

    ts_slug = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    topic = f"live-stream-{run_tag.lower()}-{ts_slug}"
    run_id = f"run_{run_tag.lower()}_{ts_slug}"
    ckpt_dir = CHECKPOINT_BASE / f"live_{run_tag.lower()}_{ts_slug}"
    metrics_file = ROOT_DIR / "reports" / "verification" / f"live_metrics_{run_tag.lower()}_{ts_slug}.json"
    csv_sample_file = ROOT_DIR / "reports" / "verification" / f"live_growth_{run_tag.lower()}_{ts_slug}.csv"

    # Step 1: Pre-run setup
    create_fresh_kafka_topic(topic, broker=broker)

    initial_counts = {}
    if truncate_first:
        initial_counts = truncate_db_tables()
    else:
        initial_counts = get_current_table_counts()
        print(f"Run {run_tag} preserving existing tables: {initial_counts}")

    # Step 2: Start Spark FIRST as a background process
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
        "--watermark", watermark,
        "--shuffle-partitions", "2",
        "--alert-k", str(alert_k),
        "--run-id", run_id,
        "--stop-after-records", str(target_records),
        "--metrics-out", str(metrics_file),
    ]

    print(f"\n[1/6] Launching Spark Streaming background process...")
    print(f"Command: {' '.join(spark_cmd)}")
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

    # Step 3: Readiness Gate
    print(f"\n[2/6] Waiting for Spark readiness gate on empty topic '{topic}'...")
    print(f"  Criteria: Checkpoint metadata exists on D: AND Batch 0 logged by Spark.")
    print(f"  Note: startingOffsets='earliest' configured; no incoming records can be lost.")

    t_gate_start = time.monotonic()
    meta_path = ckpt_dir / "metadata"
    while time.monotonic() - t_gate_start < 90.0:
        if meta_path.exists() and monitor.batch_0_seen:
            break
        time.sleep(0.5)

    if not (meta_path.exists() and monitor.batch_0_seen):
        spark_proc.kill()
        raise TimeoutError("Spark failed to pass readiness gate within 90 seconds!")

    spark_ready_elapsed = time.monotonic() - t_gate_start
    print(f"  SPARK READY in {spark_ready_elapsed:.2f}s! Checkpoint initialized at {ckpt_dir}")

    # Step 4: Start Replayer as a SEPARATE process
    print(f"\n[3/6] Starting Replayer process (File 3, Session 1, limit={target_records:,}, rate={target_rate})...")
    replayer_cmd = [
        sys.executable,
        "-u",
        "scripts/verify/live_replayer.py",
        "--topic", topic,
        "--broker", broker,
        "--limit", str(target_records),
        "--rate", str(target_rate),
        "--file-index", "3",
    ]
    replayer_start_time = time.monotonic()
    replayer_proc = subprocess.Popen(
        replayer_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=spark_env,
    )

    replayer_output_lines = []
    def _replayer_reader():
        for line in iter(replayer_proc.stdout.readline, ""):
            if not line:
                break
            replayer_output_lines.append(line.strip())
            print(f"    {line.strip()}")
        replayer_proc.stdout.close()

    rep_thread = threading.Thread(target=_replayer_reader, daemon=True)
    rep_thread.start()

    # Step 5: Sampling loop (every 2 seconds)
    print(f"\n[4/6] Sampling live progress every 2 seconds...")
    samples = []
    sample_csv_rows = []
    batches_during_replay = 0
    replay_active = True
    replayer_finished_time = None
    last_growth_time = time.monotonic()
    prev_rf_count = 0

    while True:
        now_mono = time.monotonic()
        elapsed_s = now_mono - replayer_start_time
        iso_now = datetime.now(timezone.utc).isoformat()

        # Check if replayer ended
        if replay_active and replayer_proc.poll() is not None:
            replay_active = False
            replayer_finished_time = now_mono
            rep_thread.join(timeout=2.0)
            print(f"\n  --> Replayer process finished at T+{elapsed_s:.1f}s.")

        # Query metrics
        k_end = get_kafka_end_offset(topic, broker=broker)
        tbl_counts = get_current_table_counts()
        rf_c = tbl_counts["raw_flows"]
        fs_c = tbl_counts["flow_scores"]
        wm_c = tbl_counts["window_metrics"]

        # Track progress
        if rf_c > prev_rf_count:
            last_growth_time = now_mono
            prev_rf_count = rf_c

        # Committed lag
        spark_committed = get_spark_committed_offset(ckpt_dir, topic)
        spark_lag = (k_end - spark_committed) if spark_committed is not None else (k_end - rf_c)
        e2e_lag = k_end - rf_c

        sample_entry = {
            "timestamp": iso_now,
            "elapsed_s": round(elapsed_s, 2),
            "replayer_active": replay_active,
            "kafka_end_offset": k_end,
            "raw_flows": rf_c,
            "flow_scores": fs_c,
            "window_metrics": wm_c,
            "latest_batch_duration_ms": monitor.latest_batch_duration_ms,
            "spark_committed_offset": spark_committed,
            "spark_lag": spark_lag,
            "e2e_lag": e2e_lag,
        }
        samples.append(sample_entry)
        sample_csv_rows.append(sample_entry)

        if replay_active:
            batches_during_replay = len([b for b in monitor.lines if "[BENCHMARK Batch " in b])

        # Check if Spark finished
        spark_done = spark_proc.poll() is not None or monitor.completed.is_set()
        if spark_done and not replay_active:
            print(f"\n  --> Spark stream finished and exited at T+{elapsed_s:.1f}s.")
            break

        # Check drain timeout (if replayer finished and row counts stalled for 60s)
        if not replay_active and (now_mono - last_growth_time > 60.0):
            print(f"\n  [WARN] Drain stalled! No row growth for 60 seconds (rf_c={rf_c}/{target_records}).")
            if os.name == "nt":
                spark_proc.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                spark_proc.terminate()
            break

        time.sleep(2.0)

    # Wait for spark process to terminate cleanly
    spark_proc.wait(timeout=30.0)
    spark_exit_code = spark_proc.poll()
    print(f"Spark process exited with code: {spark_exit_code}")

    # Write sampled time series CSV
    with csv_sample_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(sample_csv_rows[0].keys()))
        writer.writeheader()
        writer.writerows(sample_csv_rows)
    print(f"Live growth series written to: {csv_sample_file}")

    # Load Spark benchmark summary if saved
    spark_summary = {}
    if metrics_file.exists():
        try:
            spark_summary = json.loads(metrics_file.read_text(encoding="utf-8"))
        except Exception:
            pass

    # Step 6: Detailed STEP 2 Measurements
    print(f"\n[5/6] Computing comprehensive STEP 2 measurements...")
    final_counts = get_current_table_counts()
    kafka_final = get_kafka_end_offset(topic, broker=broker)

    # 1. Reconciliation
    expected_delta = target_records if truncate_first else 0
    actual_rf_delta = final_counts["raw_flows"] if truncate_first else (final_counts["raw_flows"] - initial_counts.get("raw_flows", 0))
    actual_fs_delta = final_counts["flow_scores"] if truncate_first else (final_counts["flow_scores"] - initial_counts.get("flow_scores", 0))

    reconciled = (
        target_records == kafka_final
        and actual_rf_delta == expected_delta
        and actual_fs_delta == expected_delta
    )

    conn = get_connection(use_superuser=True)
    cursor = conn.cursor()

    # Sum of window flow counts vs flow scores
    cursor.execute("""
        SELECT COALESCE(SUM(flow_count), 0), COUNT(*)
        FROM window_metrics;
    """)
    sum_wm_flows, wm_total_count = cursor.fetchone()

    # End-to-end data latency per flow
    cursor.execute("""
        SELECT
            record_id,
            EXTRACT(EPOCH FROM (loaded_at - ingest_time)) AS latency_s
        FROM raw_flows;
    """)
    flow_latencies = cursor.fetchall()
    lat_values = np.array([float(row[1]) for row in flow_latencies if row[1] is not None], dtype=float)

    lat_p50 = float(np.percentile(lat_values, 50)) if len(lat_values) > 0 else 0.0
    lat_p95 = float(np.percentile(lat_values, 95)) if len(lat_values) > 0 else 0.0
    lat_max = float(np.max(lat_values)) if len(lat_values) > 0 else 0.0

    # Separate warm-up batch from steady-state
    # In live streaming, Batch 1 is the first batch that receives data from replayer
    first_batch_rows = spark_summary.get("batches", [{}])[1].get("numInputRows", 0) if len(spark_summary.get("batches", [])) > 1 else 0
    warmup_lat_p50 = float(np.percentile(lat_values[:first_batch_rows], 50)) if first_batch_rows > 0 and len(lat_values) >= first_batch_rows else lat_p50
    steady_lat_p50 = float(np.percentile(lat_values[first_batch_rows:], 50)) if first_batch_rows > 0 and len(lat_values) > first_batch_rows else lat_p50
    steady_lat_p95 = float(np.percentile(lat_values[first_batch_rows:], 95)) if first_batch_rows > 0 and len(lat_values) > first_batch_rows else lat_p95

    # Window alert latency per attack burst
    raw_slice = list(iter_raw_records(limit=target_records, file_indices=[3]))
    bursts = []
    in_b = False
    b_s = 0
    for i, r in enumerate(raw_slice):
        if r["evaluation"]["label"] == 1:
            if not in_b:
                in_b = True
                b_s = i
        else:
            if in_b:
                in_b = False
                bursts.append((b_s, i - 1))
    if in_b:
        bursts.append((b_s, len(raw_slice) - 1))

    # Fetch all alerted windows
    cursor.execute("""
        SELECT
            window_start,
            window_end,
            flow_count,
            flagged_count,
            window_alert,
            attack_records,
            updated_at
        FROM window_metrics;
    """)
    wm_rows = cursor.fetchall()

    cursor.execute("""
        SELECT
            COUNT(*) FILTER (WHERE window_alert = TRUE AND attack_records = 0) AS pure_normal_alerted,
            COUNT(*) FILTER (WHERE attack_records = 0) AS pure_normal_total
        FROM window_metrics;
    """)
    pn_alerted, pn_total = cursor.fetchone()

    # Match bursts to windows
    # Window start is floor(stime / 5) * 5
    burst_latencies = []
    detected_bursts = 0
    for s_idx, e_idx in bursts:
        b_first_rec = raw_slice[s_idx]
        b_rec_id = b_first_rec["record_id"]
        cursor.execute("SELECT ingest_time, stime FROM raw_flows WHERE record_id = %s;", (b_rec_id,))
        rf_row = cursor.fetchone()
        if not rf_row:
            continue
        b_ingest_time = rf_row[0].astimezone(timezone.utc)
        b_stime = rf_row[1]
        w_start_epoch = (b_stime // 5) * 5

        # Check if window for this burst alerted
        cursor.execute("""
            SELECT updated_at, window_alert
            FROM window_metrics
            WHERE window_start = to_timestamp(%s);
        """, (w_start_epoch,))
        match = cursor.fetchone()
        if match and match[1] is True:  # window_alert is True
            detected_bursts += 1
            w_updated = match[0].astimezone(timezone.utc)
            alert_lat = (w_updated - b_ingest_time).total_seconds()
            burst_latencies.append(max(0.0, alert_lat))

    b_lat_p50 = float(np.percentile(burst_latencies, 50)) if burst_latencies else 0.0
    b_lat_p95 = float(np.percentile(burst_latencies, 95)) if burst_latencies else 0.0

    conn.close()

    # Throughput & micro-batch statistics
    rep_elapsed = (replayer_finished_time - replayer_start_time) if replayer_finished_time else (time.monotonic() - replayer_start_time)
    achieved_producer_rate = target_records / rep_elapsed if rep_elapsed > 0 else 0.0

    spark_elapsed = spark_summary.get("totalElapsedSec", 0.0)
    spark_proc_rate = spark_summary.get("recordsPerSec", 0.0)
    num_batches = spark_summary.get("numBatches", len([b for b in monitor.lines if "[BENCHMARK Batch " in b]))
    avg_batch_dur = spark_summary.get("avgBatchDurationMs", 0.0)
    max_batch_dur = spark_summary.get("maxBatchDurationMs", 0.0)
    batches_over_5s = spark_summary.get("laggingBatchesCount", 0)

    # Liveness snapshots from samples
    n_samp = len(samples)
    snap_start = samples[min(int(n_samp * 0.2), n_samp - 1)] if n_samp > 0 else {}
    snap_mid = samples[min(int(n_samp * 0.5), n_samp - 1)] if n_samp > 0 else {}
    snap_end = samples[-1] if n_samp > 0 else {}

    results = {
        "run_tag": run_tag,
        "run_id": run_id,
        "topic": topic,
        "target_records": target_records,
        "requested_rate_recs": target_rate,
        "achieved_producer_rate_recs": round(achieved_producer_rate, 2),
        "replayer_elapsed_s": round(rep_elapsed, 2),
        "spark_elapsed_s": spark_elapsed,
        "spark_processing_rate_recs": spark_proc_rate,
        "reconciled": reconciled,
        "kafka_final_offset": kafka_final,
        "raw_flows_count": final_counts["raw_flows"],
        "flow_scores_count": final_counts["flow_scores"],
        "window_metrics_count": final_counts["window_metrics"],
        "sum_window_flow_count": sum_wm_flows,
        "num_microbatches": num_batches,
        "microbatches_during_replay": batches_during_replay,
        "avg_batch_duration_ms": avg_batch_dur,
        "max_batch_duration_ms": max_batch_dur,
        "batches_exceeding_5s": batches_over_5s,
        "flow_latency_overall_p50_s": round(lat_p50, 3),
        "flow_latency_overall_p95_s": round(lat_p95, 3),
        "flow_latency_overall_max_s": round(lat_max, 3),
        "warmup_batch_latency_p50_s": round(warmup_lat_p50, 3),
        "steady_state_latency_p50_s": round(steady_lat_p50, 3),
        "steady_state_latency_p95_s": round(steady_lat_p95, 3),
        "total_attack_bursts": len(bursts),
        "detected_attack_bursts": detected_bursts,
        "burst_detection_rate": round((detected_bursts / len(bursts) * 100), 2) if bursts else 0.0,
        "burst_alert_latency_p50_s": round(b_lat_p50, 3),
        "burst_alert_latency_p95_s": round(b_lat_p95, 3),
        "pure_normal_windows_alerted": pn_alerted,
        "pure_normal_windows_total": pn_total,
        "liveness_snapshots": {
            "start_20pct": snap_start,
            "mid_50pct": snap_mid,
            "end_100pct": snap_end,
        },
        "sample_csv_path": str(csv_sample_file),
    }

    print("\n" + "=" * 90)
    print(f"RUN {run_tag} COMPLETED:")
    print(f"  Reconciliation:        {'PASS' if reconciled else 'FAIL'}")
    print(f"  Produced:              {target_records} | In Kafka: {kafka_final} | In Postgres: {final_counts['raw_flows']}")
    print(f"  Achieved Producer Rate:{results['achieved_producer_rate_recs']} rec/s (requested: {target_rate})")
    print(f"  Spark Processing Rate: {results['spark_processing_rate_recs']} rec/s")
    print(f"  Micro-batches:         {num_batches} total ({batches_during_replay} ran during active replay)")
    print(f"  Avg Batch Duration:    {avg_batch_dur} ms (Batches > 5s: {batches_over_5s})")
    print(f"  Flow Latency:          p50 = {lat_p50:.3f} s | p95 = {lat_p95:.3f} s (Steady-state p50: {steady_lat_p50:.3f}s)")
    print(f"  Burst Detection:       {detected_bursts}/{len(bursts)} ({results['burst_detection_rate']}%) | Latency p50: {b_lat_p50:.3f} s")
    print(f"  Pure Normal FAR:       {pn_alerted}/{pn_total} alerted")
    print("=" * 90 + "\n")

    return results


def main():
    parser = argparse.ArgumentParser(description="Live streaming verification suite.")
    parser.add_argument("--runs", nargs="+", default=["A", "B1", "B2", "D"], help="Runs to execute")
    parser.add_argument("--output-json", type=str, default="reports/verification/live_streaming_20261102.json")
    args = parser.parse_args()

    all_results = {}

    for run_name in args.runs:
        run_name = run_name.upper()
        if run_name == "A":
            res = execute_live_run("A", target_records=5000, target_rate=25.0, truncate_first=True)
            all_results["Run_A"] = res
        elif run_name == "B1":
            res = execute_live_run("B1", target_records=3000, target_rate=50.0, truncate_first=True)
            all_results["Run_B1"] = res
        elif run_name == "B2":
            res = execute_live_run("B2", target_records=3000, target_rate=100.0, truncate_first=True)
            all_results["Run_B2"] = res
        elif run_name == "D":
            # Idempotent live rerun: replay same 5000 records live into fresh topic/checkpoint against existing DB
            res = execute_live_run("D", target_records=5000, target_rate=25.0, truncate_first=False)
            all_results["Run_D"] = res
        else:
            print(f"Unknown run name: {run_name}")

    out_path = Path(args.output_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(all_results, indent=2), encoding="utf-8")
    print(f"\nAll verification results saved to: {out_path}")


if __name__ == "__main__":
    main()
