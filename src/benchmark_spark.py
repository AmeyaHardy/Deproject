"""
Benchmark Runner for Spark Structured Streaming
===============================================
Runs controlled experiments comparing Spark Structured Streaming performance
under different shuffle partition counts (200 vs 2) and workloads (500 vs 5,000).

Experiments:
  A: 200 partitions, 500 records   (small batch, default partitions)
  B:   2 partitions, 500 records   (small batch, tuned partitions)
  C:   2 partitions, 5,000 records (large batch, tuned partitions)
  D: 200 partitions, 5,000 records (large batch, default partitions)

Usage:
  # Run a single experiment:
  python src/benchmark_spark.py --experiment A

  # Run all four experiments sequentially:
  python src/benchmark_spark.py --all
"""

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

# ── Experiment Definitions ───────────────────────────────────────────────────

EXPERIMENTS = {
    "A": {
        "name": "Experiment A (200 partitions, 500 records)",
        "shuffle_partitions": 200,
        "workload_records": 500,
        "topic": "benchmark-a-500",
        "checkpoint_dir": "data/processed/spark-checkpoints/benchmark/A-200-small",
        "metrics_file": "outputs/benchmark/metrics_A.json",
    },
    "B": {
        "name": "Experiment B (2 partitions, 500 records)",
        "shuffle_partitions": 2,
        "workload_records": 500,
        "topic": "benchmark-b-500",
        "checkpoint_dir": "data/processed/spark-checkpoints/benchmark/B-2-small",
        "metrics_file": "outputs/benchmark/metrics_B.json",
    },
    "C": {
        "name": "Experiment C (2 partitions, 5000 records)",
        "shuffle_partitions": 2,
        "workload_records": 5000,
        "topic": "benchmark-c-5000",
        "checkpoint_dir": "data/processed/spark-checkpoints/benchmark/C-2-large",
        "metrics_file": "outputs/benchmark/metrics_C.json",
    },
    "D": {
        "name": "Experiment D (200 partitions, 5000 records)",
        "shuffle_partitions": 200,
        "workload_records": 5000,
        "topic": "benchmark-d-5000",
        "checkpoint_dir": "data/processed/spark-checkpoints/benchmark/D-200-large",
        "metrics_file": "outputs/benchmark/metrics_D.json",
    },
}

RESULTS_CSV = Path("data/processed/benchmark-results.csv")
FIXED_RATE = 100.0  # records per second


def ensure_kafka_topic(topic: str):
    """Ensure dedicated benchmark topic exists with 1 partition."""
    print(f"Creating/verifying isolated Kafka topic '{topic}' ...", end=" ", flush=True)
    cmd = [
        "docker", "exec", "kafka",
        "kafka-topics",
        "--bootstrap-server", "localhost:29092",
        "--create",
        "--if-not-exists",
        "--topic", topic,
        "--partitions", "1",
        "--replication-factor", "1",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(f"FAILED: {res.stderr}")
        sys.exit(1)
    print("ready.")


def count_state_files(checkpoint_dir_str: str) -> tuple[int, int]:
    """Count partition directories and total state files."""
    state_dir = Path(checkpoint_dir_str) / "state" / "0"
    if not state_dir.exists():
        return 0, 0
    partition_dirs = [p for p in state_dir.iterdir() if p.is_dir() and not p.name.startswith(".")]
    total_files = len(list(state_dir.rglob("*.*")))
    return len(partition_dirs), total_files


def run_experiment(exp_key: str) -> dict:
    exp = EXPERIMENTS[exp_key]
    print("\n" + "=" * 80)
    print(f"  STARTING {exp['name'].upper()}")
    print("=" * 80)
    print(f"  Shuffle Partitions: {exp['shuffle_partitions']}")
    print(f"  Workload Records:   {exp['workload_records']}")
    print(f"  Replay Rate:        {FIXED_RATE} rec/s")
    print(f"  Kafka Topic:        {exp['topic']}")
    print(f"  Checkpoint Path:    {exp['checkpoint_dir']}")
    print()

    # 1. Topic setup
    ensure_kafka_topic(exp["topic"])

    # 2. Checkpoint clean slate
    ckpt_path = Path(exp["checkpoint_dir"])
    if ckpt_path.exists():
        shutil.rmtree(ckpt_path, ignore_errors=True)
    ckpt_path.mkdir(parents=True, exist_ok=True)

    # 3. Clean metrics file
    metrics_path = Path(exp["metrics_file"])
    if metrics_path.exists():
        metrics_path.unlink()
    metrics_path.parent.mkdir(parents=True, exist_ok=True)

    # 4. Start Spark Streaming Consumer
    py_exec = sys.executable
    spark_cmd = [
        py_exec, "-u", "src/spark_streaming.py",
        "--topic", exp["topic"],
        "--checkpoint-dir", str(ckpt_path.resolve()),
        "--shuffle-partitions", str(exp["shuffle_partitions"]),
        "--stop-after-records", str(exp["workload_records"]),
        "--metrics-out", str(metrics_path.resolve()),
    ]

    print("Launching Spark streaming consumer...")
    spark_proc = subprocess.Popen(
        spark_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    if spark_proc.stdout is None:
        print("ERROR: Failed to capture Spark process stdout.")
        return None

    # Stream Spark output until query is initialized
    print("Waiting for Spark to initialize and connect to Kafka...")
    initialized = False
    for line in iter(spark_proc.stdout.readline, ""):
        print(f"  [Spark] {line.strip()}")
        if "STREAMING OUTPUT" in line or "Waiting for records" in line:
            initialized = True
            break
        if spark_proc.poll() is not None:
            break

    if spark_proc.poll() is not None and not initialized:
        print(f"ERROR: Spark process exited unexpectedly with code {spark_proc.returncode}")
        return None

    # Brief settle time
    time.sleep(3)

    # 5. Start CSV Replayer (Producer)
    print(f"\nStarting CSV replayer ({exp['workload_records']} records at {FIXED_RATE} rec/s)...")
    producer_cmd = [
        py_exec, "src/csv_replayer.py",
        "--kafka",
        "--topic", exp["topic"],
        "--limit", str(exp["workload_records"]),
        "--rate", str(FIXED_RATE),
    ]

    t0_producer = time.monotonic()
    prod_proc = subprocess.run(producer_cmd, capture_output=True, text=True)
    producer_elapsed = time.monotonic() - t0_producer

    if prod_proc.returncode != 0:
        print(f"ERROR in producer: {prod_proc.stderr}")
        spark_proc.kill()
        return None
    print(f"Producer finished in {producer_elapsed:.2f}s.")

    # 6. Stream remaining Spark output until completion
    print("\nWaiting for Spark to process all records and flush final tumbling window...")
    for line in iter(spark_proc.stdout.readline, ""):
        print(f"  [Spark] {line.strip()}")

    spark_proc.wait()
    print("Spark consumer stopped.")

    # 7. Collect results
    if not metrics_path.exists():
        print("ERROR: Metrics file was not generated.")
        return None

    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    state_partitions, state_files = count_state_files(exp["checkpoint_dir"])

    res = {
        "experiment": exp_key,
        "shuffle_partitions": exp["shuffle_partitions"],
        "target_records": exp["workload_records"],
        "actual_records": metrics.get("actualRecordsProcessed", 0),
        "total_elapsed_sec": metrics.get("totalElapsedSec", 0.0),
        "producer_elapsed_sec": round(producer_elapsed, 2),
        "num_batches": metrics.get("numBatches", 0),
        "avg_batch_duration_ms": metrics.get("avgBatchDurationMs", 0.0),
        "min_batch_duration_ms": metrics.get("minBatchDurationMs", 0),
        "max_batch_duration_ms": metrics.get("maxBatchDurationMs", 0),
        "records_per_sec": metrics.get("recordsPerSec", 0.0),
        "lagging_batches_count": metrics.get("laggingBatchesCount", 0),
        "state_partitions_count": state_partitions,
        "state_files_count": state_files,
    }

    print("\n" + "-" * 80)
    print(f"  {exp_key} RESULT: {res['actual_records']} records in {res['total_elapsed_sec']}s "
          f"({res['records_per_sec']} rec/s) | Avg batch: {res['avg_batch_duration_ms']}ms | "
          f"Lagging batches: {res['lagging_batches_count']} | State files: {res['state_files_count']}")
    print("-" * 80 + "\n")

    return res


def save_results(results: list[dict]):
    RESULTS_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "experiment",
        "shuffle_partitions",
        "target_records",
        "actual_records",
        "total_elapsed_sec",
        "producer_elapsed_sec",
        "num_batches",
        "avg_batch_duration_ms",
        "min_batch_duration_ms",
        "max_batch_duration_ms",
        "records_per_sec",
        "lagging_batches_count",
        "state_partitions_count",
        "state_files_count",
    ]
    existing = {}
    if RESULTS_CSV.exists():
        with RESULTS_CSV.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing[row["experiment"]] = row

    for r in results:
        existing[r["experiment"]] = {k: str(v) for k, v in r.items()}

    all_sorted = [existing[k] for k in ["A", "B", "C", "D"] if k in existing]

    with RESULTS_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in all_sorted:
            writer.writerow(r)
    print(f"Results saved to: {RESULTS_CSV}")
    return all_sorted


def print_comparison_table(results: list[dict]):
    print("\n" + "=" * 110)
    print("  BENCHMARK PERFORMANCE COMPARISON TABLE")
    print("=" * 110)
    header = (
        f"{'Exp':<4} | {'Partitions':<10} | {'Records':<8} | {'Elapsed (s)':<11} | "
        f"{'Batches':<7} | {'Avg Batch (ms)':<14} | {'Max Batch (ms)':<14} | "
        f"{'Rate (rec/s)':<12} | {'Lag Warnings':<12} | {'State Files':<11}"
    )
    print(header)
    print("-" * 110)
    for r in results:
        row = (
            f"{r['experiment']:<4} | {r['shuffle_partitions']:<10} | {r['actual_records']:<8} | "
            f"{r['total_elapsed_sec']:<11} | {r['num_batches']:<7} | {r['avg_batch_duration_ms']:<14} | "
            f"{r['max_batch_duration_ms']:<14} | {r['records_per_sec']:<12} | "
            f"{r['lagging_batches_count']:<12} | {r['state_files_count']:<11}"
        )
        print(row)
    print("=" * 110 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Run Spark Structured Streaming Benchmark")
    parser.add_argument("--experiment", choices=["A", "B", "C", "D"], help="Run single experiment")
    parser.add_argument("--all", action="store_true", help="Run all 4 experiments sequentially")
    args = parser.parse_args()

    if not args.experiment and not args.all:
        parser.print_help()
        sys.exit(1)

    selected = ["A", "B", "C", "D"] if args.all else [args.experiment]
    results = []

    for key in selected:
        res = run_experiment(key)
        if res:
            results.append(res)
        time.sleep(2)  # Cooldown between runs

    if results:
        all_res = save_results(results)
        print_comparison_table(all_res)


if __name__ == "__main__":
    main()
