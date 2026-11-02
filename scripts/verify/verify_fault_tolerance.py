"""
Fault Tolerance Verification Suite (Gate F6)
=============================================
Tests:
  - Fault F6-A: Stop/pause PostgreSQL for ~10 s mid-run. Stream retries and recovers with no lost rows.
  - Fault F6-B: Stop PostgreSQL for > 30 s (exceeding retry budget). Query fails cleanly without offset commit;
                restart from same checkpoint recovers and replays batch with zero duplicates.
"""

import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.db_sink import get_connection
from scripts.verify.verify_postgres_pipeline import (
    create_kafka_topic,
    replay_file3_slice,
    truncate_db_tables,
)


def run_fault_f6a():
    print("\n" + "=" * 80)
    print("  TEST FAULT F6-A: BRIEF POSTGRES OUTAGE (~10 SECONDS)")
    print("=" * 80)

    truncate_db_tables()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    topic = f"postgres-f6a-{ts}"
    ckpt = f"D:/spark_checkpoints/{topic}"
    metrics_file = f"reports/verification/f6a_metrics_{ts}.json"
    create_kafka_topic(topic)

    # Produce 1,000 records at 25 rec/s in background thread / upfront
    replay_file3_slice(topic=topic, limit=1000, rate=50.0)

    # Start Spark in subprocess
    cmd = [
        sys.executable,
        "src/spark_streaming.py",
        "--topic", topic,
        "--checkpoint-dir", ckpt,
        "--score",
        "--db-sink",
        "--watermark", "60 seconds",
        "--shuffle-partitions", "2",
        "--alert-k", "3",
        "--stop-after-records", "1000",
        "--metrics-out", metrics_file,
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT_DIR)
    env["PYSPARK_PYTHON"] = sys.executable
    env["PYSPARK_DRIVER_PYTHON"] = sys.executable
    p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Wait for Spark to initialize JVM and begin streaming, then pause postgres for 8s
    print("Waiting 18 seconds for Spark streaming to initialize JVM and begin streaming...")
    time.sleep(18)
    print("Triggering Fault F6-A: Pausing PostgreSQL container for 8 seconds...")
    pause_start = time.monotonic()
    subprocess.run(["docker", "pause", "postgres"], check=True)
    time.sleep(8)
    subprocess.run(["docker", "unpause", "postgres"], check=True)
    pause_duration = time.monotonic() - pause_start
    print(f"PostgreSQL resumed after {pause_duration:.2f}s outage.")

    stdout, stderr = p.communicate(timeout=90)
    print("Spark completed. Exit code:", p.returncode)
    if p.returncode != 0:
        print("STDERR:", stderr)
        raise RuntimeError("Spark process failed during F6-A test")

    conn = get_connection(use_superuser=False)
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM raw_flows;")
        cnt_raw = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM flow_scores;")
        cnt_scores = cur.fetchone()[0]
        cur.execute("SELECT sum(flow_count) FROM window_metrics;")
        cnt_windows = cur.fetchone()[0]
    conn.close()

    print(f"Results F6-A: raw_flows={cnt_raw} | flow_scores={cnt_scores} | window_sum={cnt_windows}")
    assert cnt_raw == 1000, f"Expected 1000 raw_flows, got {cnt_raw}"
    assert cnt_scores == 1000, f"Expected 1000 flow_scores, got {cnt_scores}"
    assert cnt_windows == 1000, f"Expected sum(window_metrics) == 1000, got {cnt_windows}"

    if Path(metrics_file).exists():
        m = json.loads(Path(metrics_file).read_text(encoding="utf-8"))
        durations = [b["triggerExecutionMs"] for b in m.get("batches", [])]
        print(f"Batch durations: {durations} ms. Max batch duration: {max(durations)} ms (shows expected retry spike).")
    print("=> Gate F6-A PASSED: Stream survived ~10s outage with zero data loss or duplicates!")


def run_fault_f6b():
    print("\n" + "=" * 80)
    print("  TEST FAULT F6-B: EXTENDED OUTAGE (> 30S) & CHECKPOINT REPLAY")
    print("=" * 80)

    truncate_db_tables()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    topic = f"postgres-f6b-{ts}"
    ckpt = f"D:/spark_checkpoints/{topic}"
    create_kafka_topic(topic)

    # Produce 500 records
    replay_file3_slice(topic=topic, limit=500, rate=50.0)

    # First run: pause postgres for 35 seconds to trigger retry budget exhaustion
    cmd = [
        sys.executable,
        "src/spark_streaming.py",
        "--topic", topic,
        "--checkpoint-dir", ckpt,
        "--score",
        "--db-sink",
        "--watermark", "60 seconds",
        "--shuffle-partitions", "2",
        "--alert-k", "3",
        "--stop-after-records", "500",
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT_DIR)
    env["PYSPARK_PYTHON"] = sys.executable
    env["PYSPARK_DRIVER_PYTHON"] = sys.executable

    print("Starting Spark streaming process...")
    p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    print("Waiting 18 seconds for Spark streaming to initialize JVM and begin streaming...")
    time.sleep(18)

    print("Pausing PostgreSQL for 35 seconds to exhaust 30s retry budget...")
    pause_start = time.monotonic()
    subprocess.run(["docker", "pause", "postgres"], check=True)
    time.sleep(35)
    subprocess.run(["docker", "unpause", "postgres"], check=True)
    print(f"PostgreSQL unpaused after {time.monotonic() - pause_start:.2f}s outage.")

    stdout, stderr = p.communicate(timeout=60)
    print("First run exit code:", p.returncode)
    # The first run should have failed due to exhausted retry budget
    assert p.returncode != 0, "Expected query to fail when budget is exhausted"
    print("=> First run cleanly aborted on exhausted budget without corrupting state.")

    # Second run: restart Spark from the SAME checkpoint directory
    print("\nRestarting Spark from the SAME checkpoint...")
    p2 = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=90)
    print("Second run exit code:", p2.returncode)

    conn = get_connection(use_superuser=False)
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM raw_flows;")
        cnt_raw = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM flow_scores;")
        cnt_scores = cur.fetchone()[0]
        cur.execute("SELECT sum(flow_count) FROM window_metrics;")
        cnt_windows = cur.fetchone()[0]
    conn.close()

    print(f"Results F6-B after restart: raw_flows={cnt_raw} | flow_scores={cnt_scores} | window_sum={cnt_windows}")
    assert cnt_raw == 500, f"Expected 500 raw_flows, got {cnt_raw}"
    assert cnt_scores == 500, f"Expected 500 flow_scores, got {cnt_scores}"
    assert cnt_windows == 500, f"Expected sum(window_metrics) == 500, got {cnt_windows}"
    print("=> Gate F6-B PASSED: Query aborted safely on long outage and recovered from checkpoint with zero loss/duplication!")


def main():
    run_fault_f6a()
    run_fault_f6b()


if __name__ == "__main__":
    main()
