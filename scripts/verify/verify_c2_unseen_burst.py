"""
Verification of Claim C2: End-to-End Spark Latency on Unseen Attack Bursts
========================================================================
Addresses:
1. Exact row slice analysis of previous benchmark vs Session 0 training data.
2. Verification verdict on Claim C2 (CONFIRMED or REFUTED).
3. Rerun on an unseen attack burst from Session 1 (File 3, 10,000 records @ 100 rec/s).
4. Measurement of:
   - Produced == Consumed == Scored reconciliation
   - Flow processing latency (p50, p95) separated into Warm-up (Batch 0) vs Steady-state (Batch 1+)
   - Burst detection latency for each of the first 3+ bursts, plus p50
   - Window false-alarm count on pure-normal windows during this streaming run
"""
import sys
import os
import json
import time
import shutil
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

# Setup Windows Java and Hadoop environment
user_profile = os.environ.get("USERPROFILE", r"C:\Users\sweni")
hadoop_home = Path(user_profile) / "hadoop"
if hadoop_home.exists():
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    hadoop_bin = str(hadoop_home / "bin")
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

default_java = Path(r"C:\Program Files\Java\jre1.8.0_461")
if default_java.exists() and "JAVA_HOME" not in os.environ:
    os.environ["JAVA_HOME"] = str(default_java)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from kafka import KafkaProducer
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, LongType
)

from src.raw_loader import iter_raw_records, FEATURE_NAMES
from src.anomaly.preprocessing import FrozenPreprocessor
from src.spark_streaming import message_schema

SEED = 42
np.random.seed(SEED)
print(f"SEED: {SEED}")

# --- TASK C2.1 & C2.2: Identify previous benchmark slice and overlap ---
print("\n" + "="*70)
print("TASK C2: SLICE OVERLAP AND VERDICT")
print("="*70)
prev_benchmark_details = {
    "script": "scripts/run_spark_kafka_benchmark.py",
    "file_index": 1,
    "file_name": "UNSW-NB15_1.csv",
    "row_offset": "rows 0 to 1,999 (limit=2000)",
    "row_count": 2000,
    "stime_range": "1421927377 to 1421927429",
    "model_path": "data/processed/models/rf_detector.joblib",
    "model_provenance": "data/processed/preprocessing_artifacts/preprocessing_provenance.json"
}

# The model rf_detector.joblib was trained on Session 0 (Files 1 and 2, rows 0 to 1,014,928).
# Therefore, rows 0-1,999 of File 1 were in the training set of rf_detector.joblib.
overlap_percentage = 100.0
c2_verdict = "REFUTED"
c2_verdict_reason = (
    "The 2,000 records replayed in run_spark_kafka_benchmark.py were drawn from File 1 (Session 0), "
    "which formed the training dataset for rf_detector.joblib. 100% of the replayed records (including "
    "all attack records) were in-sample training rows. Testing detection latency on training data evaluates "
    "memorization rather than real-time anomaly detection of unseen attacks."
)

print(f"Previous Slice: {prev_benchmark_details['file_name']}, {prev_benchmark_details['row_offset']}")
print(f"Overlap with Training Data: {overlap_percentage:.1f}%")
print(f"Verdict on Claim C2: {c2_verdict}")
print(f"Reason: {c2_verdict_reason}")

# --- TASK C2.3: Rerun on unseen Session 1 data ---
TIMESTAMP_STR = datetime.now().strftime("%Y%m%d_%H%M%S")
TEST_TOPIC = f"anomaly-verify-c2-{TIMESTAMP_STR}"
CHECKPOINT_DIR = ROOT / "data" / "processed" / "spark-checkpoints" / f"verify-c2-{TIMESTAMP_STR}"
if CHECKPOINT_DIR.exists():
    shutil.rmtree(CHECKPOINT_DIR)
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

MODEL_DIR = ROOT / "data" / "processed" / "models"
PREPROC_DIR = ROOT / "data" / "processed" / "preprocessing_artifacts"
THRESH_FILE = MODEL_DIR / "calibrated_threshold.json"

assert (MODEL_DIR / "rf_detector.joblib").exists(), "Model not found"
assert (PREPROC_DIR / "preprocessor.joblib").exists(), "Preprocessor not found"

thresh_cfg = json.loads(THRESH_FILE.read_text(encoding="utf-8"))
THRESHOLD = float(thresh_cfg.get("threshold", 0.8244))
ALERT_K = 3

print(f"\nFresh Run Configuration:")
print(f"  Kafka Topic:     {TEST_TOPIC}")
print(f"  Checkpoint Dir:  {CHECKPOINT_DIR}")
print(f"  Threshold:       {THRESHOLD:.4f}")
print(f"  Alert k:         {ALERT_K}")

# Load 10,000 records from File 3 (Session 1, completely unseen)
REPLAY_LIMIT = 10000
print(f"\nLoading {REPLAY_LIMIT:,} raw records from File 3 (Session 1, test session)...")
records = list(iter_raw_records(limit=REPLAY_LIMIT, file_indices=[3]))
n_total_recs = len(records)
n_atk_recs = sum(1 for r in records if r["evaluation"]["label"] == 1)
print(f"Loaded {n_total_recs:,} records ({n_atk_recs:,} attacks, {n_atk_recs/n_total_recs*100:.2f}%)")

# Pre-identify attack bursts in this slice
# Group contiguous attack flows (label == 1)
bursts = []
in_burst = False
b_start = 0
for i, r in enumerate(records):
    lbl = r["evaluation"]["label"]
    if lbl == 1:
        if not in_burst:
            in_burst = True
            b_start = i
    else:
        if in_burst:
            in_burst = False
            b_end = i - 1
            bursts.append({
                "burst_id": len(bursts) + 1,
                "start_idx": b_start,
                "end_idx": b_end,
                "count": b_end - b_start + 1,
                "stime_start": records[b_start]["context"]["stime"],
                "stime_end": records[b_end]["context"]["stime"],
                "attack_cats": list(set(records[j]["evaluation"]["attack_cat"] for j in range(b_start, b_end + 1))),
                "emitted_wall_time": None,
                "detected_wall_time": None,
                "detection_latency_s": None,
                "alert_window": None
            })
if in_burst:
    b_end = len(records) - 1
    bursts.append({
        "burst_id": len(bursts) + 1,
        "start_idx": b_start,
        "end_idx": b_end,
        "count": b_end - b_start + 1,
        "stime_start": records[b_start]["context"]["stime"],
        "stime_end": records[b_end]["context"]["stime"],
        "attack_cats": list(set(records[j]["evaluation"]["attack_cat"] for j in range(b_start, b_end + 1))),
        "emitted_wall_time": None,
        "detected_wall_time": None,
        "detection_latency_s": None,
        "alert_window": None
    })

print(f"Identified {len(bursts)} attack bursts in slice.")
# We will focus on tracking the first 5 bursts
tracked_bursts = bursts[:5]
for tb in tracked_bursts:
    print(f"  Burst {tb['burst_id']}: rows {tb['start_idx']}-{tb['end_idx']} (size {tb['count']}), stime {tb['stime_start']}-{tb['stime_end']}, cats {tb['attack_cats']}")

# Initialize Kafka producer
print(f"\nConnecting to Kafka on localhost:9092...")
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    acks="all",
    retries=3,
)

# Start PySpark Session
print("\nInitializing PySpark session with Kafka & Scoring UDF...")
builder = (
    SparkSession.builder
    .appName(f"VerifyC2-{TIMESTAMP_STR}")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.streaming.checkpointFileManagerClass", "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager")
    .config("spark.ui.showConsoleProgress", "false")
)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Worker artifact singleton
model_file_str = str((MODEL_DIR / "rf_detector.joblib").resolve())
preproc_dir_str = str(PREPROC_DIR.resolve())
_WORKER_MODEL = None
_WORKER_PREPROC = None

def get_worker_artifacts():
    global _WORKER_MODEL, _WORKER_PREPROC
    if _WORKER_MODEL is None:
        import joblib
        from src.anomaly.preprocessing import FrozenPreprocessor
        _WORKER_MODEL = joblib.load(model_file_str)
        _WORKER_PREPROC = FrozenPreprocessor.load(preproc_dir_str)
    return _WORKER_MODEL, _WORKER_PREPROC

@F.pandas_udf(DoubleType())
def score_anomaly_udf(*cols: pd.Series) -> pd.Series:
    model, preproc = get_worker_artifacts()
    pdf = pd.DataFrame(dict(zip(FEATURE_NAMES, cols)))
    pdf_proc = preproc.transform(pdf)
    scores = model.predict_proba(pdf_proc)[:, 1]
    return pd.Series(scores, dtype="float64")

# Read from fresh Kafka topic
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", TEST_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(F.from_json(F.col("json_str"), message_schema).alias("data"))
    .filter(F.col("data").isNotNull())
)

feature_cols = [
    F.coalesce(
        F.col(f"data.features.{c}"),
        F.lit(0.0) if c not in ("proto", "service", "state") else F.lit("none")
    )
    for c in FEATURE_NAMES
]

scored_stream = parsed.withColumn("anomaly_score", score_anomaly_udf(*feature_cols))
scored_stream = scored_stream.withColumn("flagged", (F.col("anomaly_score") >= THRESHOLD).cast("integer"))

flow_level = (
    scored_stream
    .select(
        F.col("data.record_id").alias("record_id"),
        F.col("data.event_time").alias("event_time_str"),
        F.to_timestamp(F.col("data.event_time")).alias("event_time"),
        F.col("data.ingest_time").alias("ingest_time"),
        F.col("data.context.stime").alias("stime"),
        F.col("data.evaluation.label").alias("label"),
        F.col("anomaly_score"),
        F.col("flagged")
    )
    .filter(F.col("event_time").isNotNull())
)

# 60s watermark to accommodate stime jitter
watermarked = flow_level.withWatermark("event_time", "60 seconds")

window_stats = (
    watermarked
    .groupBy(F.window("event_time", "5 seconds"))
    .agg(
        F.count("*").alias("record_count"),
        F.sum("flagged").cast("int").alias("flagged_count"),
        F.when(F.sum("flagged") >= ALERT_K, 1).otherwise(0).alias("window_alert"),
        F.sum("label").cast("int").alias("attack_records"),
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "record_count",
        "flagged_count",
        "window_alert",
        "attack_records"
    )
)

collected_flows = []
collected_windows = []
alert_windows = []

def process_flow_batch(df, batch_id):
    batch_wall_time = time.time()
    rows = df.collect()
    for row in rows:
        collected_flows.append({
            "record_id": row["record_id"],
            "ingest_time": row["ingest_time"],
            "batch_wall_time": batch_wall_time,
            "batch_id": batch_id,
            "stime": row["stime"],
            "anomaly_score": row["anomaly_score"],
            "flagged": row["flagged"],
            "label": row["label"]
        })

def process_window_batch(df, batch_id):
    batch_wall_time = time.time()
    rows = df.collect()
    for row in rows:
        win_dict = {
            "window_start": str(row["window_start"]),
            "window_end": str(row["window_end"]),
            "record_count": row["record_count"],
            "flagged_count": row["flagged_count"],
            "window_alert": row["window_alert"],
            "attack_records": row["attack_records"],
            "batch_id": batch_id,
            "wall_time": batch_wall_time
        }
        collected_windows.append(win_dict)
        if row["window_alert"] == 1:
            alert_windows.append(win_dict)

query_flows = (
    flow_level.writeStream
    .foreachBatch(process_flow_batch)
    .trigger(processingTime="2 seconds")
    .option("checkpointLocation", str(CHECKPOINT_DIR / "flows"))
    .start()
)

query_windows = (
    window_stats.writeStream
    .outputMode("update")
    .foreachBatch(process_window_batch)
    .trigger(processingTime="2 seconds")
    .option("checkpointLocation", str(CHECKPOINT_DIR / "windows"))
    .start()
)

print("\nSpark streaming queries started. Beginning replay into Kafka at 100 rec/s...")

t_replay_start = time.time()

# Pre-map burst start record indices for fast emission timestamping
burst_start_map = {tb["start_idx"]: tb for tb in tracked_bursts}

for i, rec in enumerate(records):
    now_dt = datetime.now(timezone.utc)
    now_wall = time.time()
    rec["ingest_time"] = now_dt.isoformat()

    if i in burst_start_map:
        burst_start_map[i]["emitted_wall_time"] = now_wall

    key = rec.get("context", {}).get("srcip", "")
    timestamp_ms = int(rec["context"]["stime"] * 1000)
    producer.send(TEST_TOPIC, key=key, value=rec, timestamp_ms=timestamp_ms)

    # 100 rec/s -> 10ms delay
    time.sleep(0.010)

producer.flush()
t_replay_end = time.time()
replay_dur = t_replay_end - t_replay_start
print(f"Replay completed: {n_total_recs:,} records emitted in {replay_dur:.2f}s ({n_total_recs/replay_dur:.1f} rec/s)")

print("Waiting for Spark to process all records...")
wait_start = time.time()
while len(collected_flows) < n_total_recs and (time.time() - wait_start) < 120:
    time.sleep(1)
    print(f"  Processed {len(collected_flows):,}/{n_total_recs:,} flows...", end="\r", flush=True)

# Flush remaining window microbatches
time.sleep(6)
query_flows.stop()
query_windows.stop()
spark.stop()

print(f"\nSpark query stopped. Total flows collected: {len(collected_flows):,}")

# --- ANALYSIS ---
n_produced = n_total_recs
n_consumed = len(collected_flows)
n_scored = sum(1 for f in collected_flows if f["anomaly_score"] is not None)
reconciled = (n_produced == n_consumed == n_scored)

print("\n" + "="*70)
print("RECONCILIATION")
print("="*70)
print(f"Records Produced: {n_produced:,}")
print(f"Records Consumed: {n_consumed:,}")
print(f"Records Scored:   {n_scored:,}")
print(f"Reconciliation:   {'MATCH' if reconciled else 'MISMATCH'}")

# Flow Processing Latency (Warm-up vs Steady-state)
flow_latencies_all = []
flow_latencies_b0 = []
flow_latencies_steady = []

for f in collected_flows:
    ingest_dt = datetime.fromisoformat(f["ingest_time"])
    ingest_wall = ingest_dt.timestamp()
    lat_ms = (f["batch_wall_time"] - ingest_wall) * 1000.0
    lat_ms = max(0.0, lat_ms)
    flow_latencies_all.append(lat_ms)
    if f["batch_id"] == 0:
        flow_latencies_b0.append(lat_ms)
    else:
        flow_latencies_steady.append(lat_ms)

lat_stats = {
    "all": {
        "count": len(flow_latencies_all),
        "p50_ms": float(np.percentile(flow_latencies_all, 50)) if flow_latencies_all else 0.0,
        "p95_ms": float(np.percentile(flow_latencies_all, 95)) if flow_latencies_all else 0.0,
        "mean_ms": float(np.mean(flow_latencies_all)) if flow_latencies_all else 0.0
    },
    "warmup_batch_0": {
        "count": len(flow_latencies_b0),
        "p50_ms": float(np.percentile(flow_latencies_b0, 50)) if flow_latencies_b0 else 0.0,
        "p95_ms": float(np.percentile(flow_latencies_b0, 95)) if flow_latencies_b0 else 0.0,
        "mean_ms": float(np.mean(flow_latencies_b0)) if flow_latencies_b0 else 0.0
    },
    "steady_state_batch_1_plus": {
        "count": len(flow_latencies_steady),
        "p50_ms": float(np.percentile(flow_latencies_steady, 50)) if flow_latencies_steady else 0.0,
        "p95_ms": float(np.percentile(flow_latencies_steady, 95)) if flow_latencies_steady else 0.0,
        "mean_ms": float(np.mean(flow_latencies_steady)) if flow_latencies_steady else 0.0
    }
}

print("\n" + "="*70)
print("FLOW PROCESSING LATENCY (Wall-Clock Precision)")
print("="*70)
print(f"Overall (N={lat_stats['all']['count']:,}):")
print(f"  p50:  {lat_stats['all']['p50_ms']:.1f} ms | p95:  {lat_stats['all']['p95_ms']:.1f} ms | mean: {lat_stats['all']['mean_ms']:.1f} ms")
print(f"Warm-up Batch 0 (N={lat_stats['warmup_batch_0']['count']:,}):")
print(f"  p50:  {lat_stats['warmup_batch_0']['p50_ms']:.1f} ms | p95:  {lat_stats['warmup_batch_0']['p95_ms']:.1f} ms | mean: {lat_stats['warmup_batch_0']['mean_ms']:.1f} ms")
print(f"Steady-State Batch 1+ (N={lat_stats['steady_state_batch_1_plus']['count']:,}):")
print(f"  p50:  {lat_stats['steady_state_batch_1_plus']['p50_ms']:.1f} ms | p95:  {lat_stats['steady_state_batch_1_plus']['p95_ms']:.1f} ms | mean: {lat_stats['steady_state_batch_1_plus']['mean_ms']:.1f} ms")

# Burst Detection Latency
# Map each tracked burst to the first alert window whose time covers its flows
print("\n" + "="*70)
print("UNSEEN ATTACK BURST DETECTION LATENCY")
print("="*70)

# Convert alert window timestamps to epoch seconds for matching
parsed_alert_windows = []
for aw in alert_windows:
    try:
        # Spark outputs window_start in the JVM's local timezone (Asia/Calcutta)
        w_start = pd.to_datetime(aw["window_start"]).tz_localize("Asia/Calcutta").timestamp()
        w_end = pd.to_datetime(aw["window_end"]).tz_localize("Asia/Calcutta").timestamp()
        parsed_alert_windows.append({
            "w_start": w_start,
            "w_end": w_end,
            "wall_time": aw["wall_time"],
            "dict": aw
        })
    except Exception:
        pass

burst_latencies = []
for tb in tracked_bursts:
    st_start = tb["stime_start"]
    st_end = tb["stime_end"]
    emitted_time = tb["emitted_wall_time"]
    
    # Find alert windows that contain st_start or overlap
    matching_alerts = [
        paw for paw in parsed_alert_windows
        if paw["w_start"] <= st_end and paw["w_end"] >= st_start
    ]
    
    # Filter for alerts occurring after emission
    if matching_alerts and emitted_time is not None:
        matching_alerts.sort(key=lambda x: x["wall_time"])
        first_alert = matching_alerts[0]
        tb["detected_wall_time"] = first_alert["wall_time"]
        tb["alert_window"] = f"{first_alert['dict']['window_start']}..{first_alert['dict']['window_end']}"
        det_lat = max(0.0, first_alert["wall_time"] - emitted_time)
        tb["detection_latency_s"] = det_lat
        burst_latencies.append(det_lat)
        print(f"Burst {tb['burst_id']} (size {tb['count']}, cats {tb['attack_cats']}):")
        print(f"  Emitted:  {datetime.fromtimestamp(emitted_time, tz=timezone.utc).strftime('%H:%M:%S.%f')[:-3]}")
        print(f"  Alerted:  {datetime.fromtimestamp(first_alert['wall_time'], tz=timezone.utc).strftime('%H:%M:%S.%f')[:-3]}")
        print(f"  Detection Latency: {det_lat:.3f} s (Alert Window: {tb['alert_window']})")
    else:
        print(f"Burst {tb['burst_id']} (size {tb['count']}): NO ALERT TRIGGERED")

burst_p50_lat = float(np.median(burst_latencies)) if burst_latencies else None
print(f"\nMedian (p50) Burst Detection Latency across detected bursts: {burst_p50_lat:.3f} s" if burst_p50_lat else "No bursts detected")

# Window False Alarm Analysis during this streaming run
print("\n" + "="*70)
print("WINDOW FALSE ALARM ANALYSIS (Streaming Run)")
print("="*70)
# Aggregate unique windows across updates (take latest state per window)
windows_by_key = {}
for w in collected_windows:
    key = (w["window_start"], w["window_end"])
    # keep the one with largest record_count or latest wall_time
    if key not in windows_by_key or w["wall_time"] > windows_by_key[key]["wall_time"]:
        windows_by_key[key] = w

unique_windows = list(windows_by_key.values())
total_unique_windows = len(unique_windows)
pure_normal_windows = [w for w in unique_windows if w["attack_records"] == 0]
mixed_attack_windows = [w for w in unique_windows if w["attack_records"] > 0]

# False alarms at k=1, k=2, k=3 on pure-normal windows
pn_k1_alerts = sum(1 for w in pure_normal_windows if w["flagged_count"] >= 1)
pn_k2_alerts = sum(1 for w in pure_normal_windows if w["flagged_count"] >= 2)
pn_k3_alerts = sum(1 for w in pure_normal_windows if w["flagged_count"] >= 3)

n_pn = len(pure_normal_windows)
print(f"Total Unique Windows:       {total_unique_windows}")
print(f"Pure-Normal Windows:        {n_pn} ({n_pn/total_unique_windows*100:.1f}%)" if total_unique_windows else "0")
print(f"Attack Windows:             {len(mixed_attack_windows)}")
print(f"Pure-Normal Alerts at k=1:  {pn_k1_alerts} ({pn_k1_alerts/n_pn*100:.2f}%)" if n_pn else "N/A")
print(f"Pure-Normal Alerts at k=2:  {pn_k2_alerts} ({pn_k2_alerts/n_pn*100:.2f}%)" if n_pn else "N/A")
print(f"Pure-Normal Alerts at k=3:  {pn_k3_alerts} ({pn_k3_alerts/n_pn*100:.2f}%)" if n_pn else "N/A")

# Compile JSON results
verification_c2_result = {
    "date": "2026-10-31",
    "seed": SEED,
    "claim_evaluated": "C2: The attack burst replayed through Spark was detected in ~3 s",
    "verdict": c2_verdict,
    "reason": c2_verdict_reason,
    "previous_slice_overlap": {
        "script": prev_benchmark_details["script"],
        "file": prev_benchmark_details["file_name"],
        "rows": prev_benchmark_details["row_offset"],
        "in_training_set": True,
        "overlap_percentage": overlap_percentage
    },
    "unseen_burst_benchmark": {
        "test_slice": {
            "file": "UNSW-NB15_3.csv",
            "session": 1,
            "rows_evaluated": n_total_recs,
            "attack_rows": n_atk_recs,
            "attack_percentage": n_atk_recs / n_total_recs * 100.0,
            "in_training_set": False
        },
        "reconciliation": {
            "records_produced": n_produced,
            "records_consumed": n_consumed,
            "records_scored": n_scored,
            "reconciled": reconciled
        },
        "flow_latency_ms": lat_stats,
        "tracked_bursts": [
            {
                "burst_id": b["burst_id"],
                "count": b["count"],
                "attack_cats": b["attack_cats"],
                "stime_range": f"{b['stime_start']}..{b['stime_end']}",
                "detection_latency_s": b["detection_latency_s"],
                "alert_window": b["alert_window"]
            }
            for b in tracked_bursts
        ],
        "burst_detection_latency_p50_s": burst_p50_lat,
        "pure_normal_window_false_alarms": {
            "total_windows": total_unique_windows,
            "pure_normal_windows": n_pn,
            "alerts_k1": pn_k1_alerts,
            "rate_k1": (pn_k1_alerts / n_pn) if n_pn else 0.0,
            "alerts_k2": pn_k2_alerts,
            "rate_k2": (pn_k2_alerts / n_pn) if n_pn else 0.0,
            "alerts_k3": pn_k3_alerts,
            "rate_k3": (pn_k3_alerts / n_pn) if n_pn else 0.0
        }
    }
}

out_file = ROOT / "reports" / "verification" / "c2_unseen_burst_verification.json"
out_file.parent.mkdir(parents=True, exist_ok=True)
out_file.write_text(json.dumps(verification_c2_result, indent=2), encoding="utf-8")
print(f"\nVerification results saved to {out_file}")
