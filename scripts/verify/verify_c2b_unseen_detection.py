"""
Benchmark & Verification Pass C2B: End-to-End Spark Anomaly Detection on Unseen Data
====================================================================================
Evaluates PySpark Structured Streaming anomaly detection across three ingestion rates:
25 rec/s, 50 rec/s, and 100 rec/s on >=10,000 unseen flows from File 3 (Session 1).

Measures for each rate:
1. Reconciliation: produced == consumed == scored
2. Flow Recall, Precision, and FPR (threshold calibrated from S0)
3. Burst Detection: total bursts, detected bursts, detection rate, detection latency (p50, p95)
4. Window Evaluation: attack windows alerted/total, pure-normal window FAR (k=1, 2, 3)
5. Processing Latency: warm-up (Batch 0) vs steady-state (Batch 1+) p50, p95
6. Microbatch Performance: batch duration (mean, p50, p95, max), batches > 5s, throughput
7. Watermark Drops: flows excluded by the 60-second watermark
"""
import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)
import os
import json
import time
import shutil
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

# Setup Windows Java, Hadoop, and PySpark paths
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
from kafka.admin import KafkaAdminClient, NewTopic
import numpy as np
import pandas as pd
import psutil
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

# Check disk space on D:
d_usage = psutil.disk_usage("D:\\")
free_d_gb = d_usage.free / (1024**3)
print(f"Free disk space on D:\\: {free_d_gb:.2f} GB")
if free_d_gb < 5.0:
    print(f"ERROR: Free space on D: is under 5 GB ({free_d_gb:.2f} GB). Aborting.")
    sys.exit(1)

SPARK_TEMP = Path("D:/spark_temp")
SPARK_CHECKPOINTS = Path("D:/spark-checkpoints")
SPARK_TEMP.mkdir(parents=True, exist_ok=True)
SPARK_CHECKPOINTS.mkdir(parents=True, exist_ok=True)

MODEL_DIR = ROOT / "data" / "processed" / "models"
PREPROC_DIR = ROOT / "data" / "processed" / "preprocessing_artifacts"
THRESH_FILE = MODEL_DIR / "calibrated_threshold.json"

assert (MODEL_DIR / "rf_detector.joblib").exists(), "Model not found"
assert (PREPROC_DIR / "preprocessor.joblib").exists(), "Preprocessor not found"

thresh_cfg = json.loads(THRESH_FILE.read_text(encoding="utf-8"))
THRESHOLD = float(thresh_cfg.get("threshold", 0.8243745741878048))
ALERT_K = 3
WATERMARK_STR = "60 seconds"
SHUFFLE_PARTITIONS = "2"
LIMIT_RECORDS = 10000

print(f"\nModel Configuration:")
print(f"  Model:           rf_detector.joblib (Trained on Session 0)")
print(f"  Threshold:       {THRESHOLD:.6f}")
print(f"  Alert k:         {ALERT_K}")
print(f"  Watermark:       {WATERMARK_STR}")
print(f"  Shuffle Parts:   {SHUFFLE_PARTITIONS}")

# Verify Session 1 / File 3 disjointness from training set
prov_file = PREPROC_DIR / "preprocessing_provenance.json"
prov_data = json.loads(prov_file.read_text(encoding="utf-8")) if prov_file.exists() else {}
train_files = prov_data.get("train_files", ["UNSW-NB15_1.csv", "UNSW-NB15_2.csv"])
print(f"Training Files: {train_files}")
assert "UNSW-NB15_3.csv" not in train_files, "UNSW-NB15_3.csv was used in training!"
print("Verified: File 3 is 100% unseen out-of-sample test data.")

# Load 10,000 records from File 3
print(f"\nLoading {LIMIT_RECORDS:,} raw records from File 3 (Session 1)...")
raw_records = list(iter_raw_records(limit=LIMIT_RECORDS, file_indices=[3]))
n_total = len(raw_records)
n_atk = sum(1 for r in raw_records if r["evaluation"]["label"] == 1)
print(f"Loaded {n_total:,} records ({n_atk:,} attacks, {n_atk/n_total*100:.2f}%)")

# Extract contiguous attack bursts
bursts = []
in_b = False
b_s = 0
for i, r in enumerate(raw_records):
    lbl = r["evaluation"]["label"]
    if lbl == 1:
        if not in_b:
            in_b = True
            b_s = i
    else:
        if in_b:
            in_b = False
            b_e = i - 1
            bursts.append({
                "burst_id": len(bursts) + 1,
                "start_idx": b_s,
                "end_idx": b_e,
                "count": b_e - b_s + 1,
                "stime_start": raw_records[b_s]["context"]["stime"],
                "stime_end": raw_records[b_e]["context"]["stime"],
                "attack_cats": sorted(list(set(raw_records[j]["evaluation"]["attack_cat"] for j in range(b_s, b_e + 1))))
            })
if in_b:
    b_e = len(raw_records) - 1
    bursts.append({
        "burst_id": len(bursts) + 1,
        "start_idx": b_s,
        "end_idx": b_e,
        "count": b_e - b_s + 1,
        "stime_start": raw_records[b_s]["context"]["stime"],
        "stime_end": raw_records[b_e]["context"]["stime"],
        "attack_cats": sorted(list(set(raw_records[j]["evaluation"]["attack_cat"] for j in range(b_s, b_e + 1))))
    })

print(f"Identified {len(bursts)} total attack bursts in slice.")

# Initialize PySpark
print("\nStarting PySpark session with explicit UTC timezone...")
spark = (
    SparkSession.builder
    .appName("BenchmarkC2B-Rates")
    .master("local[4]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")
    .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.driver.extraJavaOptions", f"-Djava.library.path={hadoop_bin} -Duser.timezone=UTC")
    .config("spark.executor.extraJavaOptions", f"-Djava.library.path={hadoop_bin} -Duser.timezone=UTC")
    .config("spark.local.dir", str(SPARK_TEMP.resolve()))
    .config("spark.sql.streaming.checkpointFileManagerClass", "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

if os.name == "nt" and hadoop_bin:
    hadoop_dll = Path(hadoop_bin) / "hadoop.dll"
    if hadoop_dll.exists():
        try:
            spark.sparkContext._jvm.java.lang.System.load(str(hadoop_dll.resolve()))
        except Exception:
            pass

model_file_str = str((MODEL_DIR / "rf_detector.joblib").resolve())
preproc_dir_str = str(PREPROC_DIR.resolve())
_WORKER_MODEL = None
_WORKER_PREPROC = None

def get_worker_artifacts():
    global _WORKER_MODEL, _WORKER_PREPROC
    if _WORKER_MODEL is None:
        import joblib
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

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    acks="all",
    retries=3,
)

def to_epoch_utc(ts):
    if hasattr(ts, "timestamp"):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc).timestamp()
        return ts.timestamp()
    dt = pd.to_datetime(ts)
    if dt.tz is None:
        dt = dt.tz_localize("UTC")
    return dt.timestamp()

def run_benchmark_for_rate(rate_rec_s: int):
    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    topic_name = f"anomaly-c2b-r{rate_rec_s}-{run_timestamp}"
    ckpt_dir = SPARK_CHECKPOINTS / f"c2b-r{rate_rec_s}-{run_timestamp}"
    if ckpt_dir.exists():
        shutil.rmtree(ckpt_dir)
    ckpt_dir.mkdir(parents=True, exist_ok=True)

    # Ensure topic exists in Kafka before starting Spark stream
    try:
        admin = KafkaAdminClient(bootstrap_servers="localhost:9092")
        admin.create_topics([NewTopic(name=topic_name, num_partitions=1, replication_factor=1)])
        admin.close()
    except Exception as e:
        pass
    time.sleep(1)

    print("\n" + "="*80)
    print(f"STARTING BENCHMARK RUN: {rate_rec_s} rec/s")
    print(f"Topic: {topic_name} | Checkpoint: {ckpt_dir}")
    print("="*80)

    # Reset burst tracking for this run
    current_bursts = [dict(b) for b in bursts]
    burst_start_map = {b["start_idx"]: b for b in current_bursts}
    for b in current_bursts:
        b["emitted_wall_time"] = None
        b["detected_wall_time"] = None
        b["detection_latency_s"] = None
        b["alert_window"] = None

    collected_flows = []
    collected_windows = []
    alert_windows = []
    batch_durations_ms = []

    def process_flow_batch(df, batch_id):
        t_b_start = time.time()
        rows = df.collect()
        t_b_end = time.time()
        batch_dur = (t_b_end - t_b_start) * 1000.0
        batch_durations_ms.append(batch_dur)

        for row in rows:
            collected_flows.append({
                "record_id": row["record_id"],
                "ingest_time": row["ingest_time"],
                "batch_wall_time": t_b_end,
                "batch_id": batch_id,
                "stime": row["stime"],
                "anomaly_score": row["anomaly_score"],
                "flagged": row["flagged"],
                "label": row["label"]
            })

    def process_window_batch(df, batch_id):
        t_b_end = time.time()
        rows = df.collect()
        for row in rows:
            w_start_epoch = int(row["w_start_epoch"])
            w_end_epoch = int(row["w_end_epoch"])
            win_dict = {
                "window_start": str(row["window_start"]),
                "window_end": str(row["window_end"]),
                "w_start_epoch": w_start_epoch,
                "w_end_epoch": w_end_epoch,
                "record_count": row["record_count"],
                "flagged_count": row["flagged_count"],
                "window_alert": row["window_alert"],
                "attack_records": row["attack_records"],
                "batch_id": batch_id,
                "wall_time": t_b_end
            }
            collected_windows.append(win_dict)
            if row["window_alert"] == 1:
                alert_windows.append(win_dict)

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic_name)
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

    scored = parsed.withColumn("anomaly_score", score_anomaly_udf(*feature_cols))
    scored = scored.withColumn("flagged", (F.col("anomaly_score") >= THRESHOLD).cast("integer"))

    flow_stream = (
        scored
        .select(
            F.col("data.record_id").alias("record_id"),
            F.to_timestamp(F.col("data.event_time")).alias("event_time"),
            F.col("data.ingest_time").alias("ingest_time"),
            F.col("data.context.stime").alias("stime"),
            F.col("data.evaluation.label").alias("label"),
            F.col("anomaly_score"),
            F.col("flagged")
        )
        .filter(F.col("event_time").isNotNull())
    )

    watermarked = flow_stream.withWatermark("event_time", WATERMARK_STR)

    window_stream = (
        watermarked
        .groupBy(F.window("event_time", "5 seconds"))
        .agg(
            F.count("*").alias("record_count"),
            F.sum("flagged").cast("int").alias("flagged_count"),
            F.when(F.sum("flagged") >= ALERT_K, 1).otherwise(0).alias("window_alert"),
            F.sum("label").cast("int").alias("attack_records"),
        )
        .select(
            F.col("window.start").cast("long").alias("w_start_epoch"),
            F.col("window.end").cast("long").alias("w_end_epoch"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "record_count",
            "flagged_count",
            "window_alert",
            "attack_records"
        )
    )

    q_flows = (
        flow_stream.writeStream
        .foreachBatch(process_flow_batch)
        .trigger(processingTime="2 seconds")
        .option("checkpointLocation", str(ckpt_dir / "flows"))
        .start()
    )

    q_windows = (
        window_stream.writeStream
        .outputMode("update")
        .foreachBatch(process_window_batch)
        .trigger(processingTime="2 seconds")
        .option("checkpointLocation", str(ckpt_dir / "windows"))
        .start()
    )

    print(f"Queries active. Beginning paced replay at {rate_rec_s} rec/s...")
    t_start = time.time()
    for i, r in enumerate(raw_records):
        target_t = t_start + (i + 1) / float(rate_rec_s)
        now_dt = datetime.now(timezone.utc)
        now_wall = time.time()
        rec_copy = dict(r)
        rec_copy["ingest_time"] = now_dt.isoformat()

        if i in burst_start_map:
            burst_start_map[i]["emitted_wall_time"] = now_wall

        k = rec_copy.get("context", {}).get("srcip", "")
        ts_ms = int(rec_copy["context"]["stime"] * 1000)
        producer.send(topic_name, key=k, value=rec_copy, timestamp_ms=ts_ms)

        sleep_d = target_t - time.time()
        if sleep_d > 0:
            time.sleep(sleep_d)

    producer.flush()
    replay_duration = time.time() - t_start
    print(f"Replay finished: {n_total:,} rows emitted in {replay_duration:.1f}s ({n_total/replay_duration:.1f} rec/s).")

    print("Waiting for Spark stream completion...")
    last_count = len(collected_flows)
    stagnant_seconds = 0
    t_wait_start = time.time()
    max_total_wait = max(120, int(n_total / rate_rec_s) + 180)
    while len(collected_flows) < n_total and stagnant_seconds < 60 and (time.time() - t_wait_start) < max_total_wait:
        time.sleep(1)
        cur_count = len(collected_flows)
        if cur_count == last_count:
            stagnant_seconds += 1
        else:
            stagnant_seconds = 0
            last_count = cur_count
        print(f"  Processed {cur_count:,}/{n_total:,} flows...", end="\r", flush=True)

    print(f"\n  Finalizing microbatches: flushing remaining offsets...")
    try:
        q_flows.processAllAvailable()
        q_windows.processAllAvailable()
    except Exception as e:
        print(f"  Warning on processAllAvailable: {e}")
    time.sleep(4)
    q_flows.stop()
    q_windows.stop()

    print(f"\nProcessing complete for {rate_rec_s} rec/s. Collected {len(collected_flows):,} flows.")

    # 1. Reconciliation
    n_produced = n_total
    n_consumed = len(collected_flows)
    n_scored = sum(1 for f in collected_flows if f["anomaly_score"] is not None)
    reconciled = (n_produced == n_consumed == n_scored)

    # 2. Flow Metrics (Recall, Precision, FPR)
    tp = sum(1 for f in collected_flows if f["flagged"] == 1 and f["label"] == 1)
    fp = sum(1 for f in collected_flows if f["flagged"] == 1 and f["label"] == 0)
    fn = sum(1 for f in collected_flows if f["flagged"] == 0 and f["label"] == 1)
    tn = sum(1 for f in collected_flows if f["flagged"] == 0 and f["label"] == 0)

    flow_recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    flow_precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    flow_fpr = fp / (fp + tn) if (fp + tn) > 0 else 0.0

    # 3. Flow Processing Latency (Warm-up vs Steady-state)
    lat_all = []
    lat_b0 = []
    lat_steady = []
    for f in collected_flows:
        ingest_epoch = datetime.fromisoformat(f["ingest_time"]).timestamp()
        l_ms = max(0.0, (f["batch_wall_time"] - ingest_epoch) * 1000.0)
        lat_all.append(l_ms)
        if f["batch_id"] == 0:
            lat_b0.append(l_ms)
        else:
            lat_steady.append(l_ms)

    # 4. Burst Detection
    detected_burst_count = 0
    burst_latencies = []
    for b in current_bursts:
        st_start = b["stime_start"]
        st_end = b["stime_end"]
        emitted_t = b["emitted_wall_time"]

        # Alert windows covering stime range
        matching_alerts = [
            aw for aw in alert_windows
            if aw["w_start_epoch"] <= st_end and aw["w_end_epoch"] >= st_start
        ]
        if matching_alerts and emitted_t is not None:
            matching_alerts.sort(key=lambda x: x["wall_time"])
            fa = matching_alerts[0]
            b["detected_wall_time"] = fa["wall_time"]
            b["alert_window"] = f"{fa['window_start']}..{fa['window_end']}"
            d_lat = max(0.0, fa["wall_time"] - emitted_t)
            b["detection_latency_s"] = d_lat
            burst_latencies.append(d_lat)
            detected_burst_count += 1

    burst_detection_rate = detected_burst_count / len(current_bursts) if current_bursts else 0.0
    burst_p50_s = float(np.percentile(burst_latencies, 50)) if burst_latencies else None
    burst_p95_s = float(np.percentile(burst_latencies, 95)) if burst_latencies else None

    # 5. Window Evaluation
    unique_windows = {}
    for w in collected_windows:
        k = (w["window_start"], w["window_end"])
        if k not in unique_windows or w["wall_time"] > unique_windows[k]["wall_time"]:
            unique_windows[k] = w
    
    uw_list = list(unique_windows.values())
    tot_uw = len(uw_list)
    atk_windows = [w for w in uw_list if w["attack_records"] > 0]
    pn_windows = [w for w in uw_list if w["attack_records"] == 0]

    tot_atk_w = len(atk_windows)
    atk_w_alerted_k3 = sum(1 for w in atk_windows if w["flagged_count"] >= 3)
    atk_w_alert_rate = atk_w_alerted_k3 / tot_atk_w if tot_atk_w > 0 else 0.0

    tot_pn_w = len(pn_windows)
    pn_k1 = sum(1 for w in pn_windows if w["flagged_count"] >= 1)
    pn_k2 = sum(1 for w in pn_windows if w["flagged_count"] >= 2)
    pn_k3 = sum(1 for w in pn_windows if w["flagged_count"] >= 3)
    pn_far_k3 = pn_k3 / tot_pn_w if tot_pn_w > 0 else 0.0

    # 6. Microbatch Performance & Throughput
    tot_batches = len(batch_durations_ms)
    b_mean = float(np.mean(batch_durations_ms)) if batch_durations_ms else 0.0
    b_p50 = float(np.percentile(batch_durations_ms, 50)) if batch_durations_ms else 0.0
    b_p95 = float(np.percentile(batch_durations_ms, 95)) if batch_durations_ms else 0.0
    b_max = float(np.max(batch_durations_ms)) if batch_durations_ms else 0.0
    b_over_5s = sum(1 for d in batch_durations_ms if d > 5000.0)

    # Total windowed records
    windowed_records = sum(w["record_count"] for w in uw_list)
    wm_drops = max(0, n_consumed - windowed_records)
    wm_drop_pct = (wm_drops / n_consumed * 100.0) if n_consumed > 0 else 0.0

    res = {
        "rate_rec_s": rate_rec_s,
        "topic": topic_name,
        "reconciliation": {
            "produced": n_produced,
            "consumed": n_consumed,
            "scored": n_scored,
            "reconciled": reconciled
        },
        "flow_evaluation": {
            "tp": tp, "fp": fp, "fn": fn, "tn": tn,
            "recall": flow_recall,
            "precision": flow_precision,
            "fpr": flow_fpr
        },
        "flow_processing_latency_ms": {
            "overall": {
                "count": len(lat_all),
                "p50_ms": float(np.percentile(lat_all, 50)) if lat_all else 0.0,
                "p95_ms": float(np.percentile(lat_all, 95)) if lat_all else 0.0,
                "mean_ms": float(np.mean(lat_all)) if lat_all else 0.0
            },
            "warmup_batch_0": {
                "count": len(lat_b0),
                "p50_ms": float(np.percentile(lat_b0, 50)) if lat_b0 else 0.0,
                "p95_ms": float(np.percentile(lat_b0, 95)) if lat_b0 else 0.0,
                "mean_ms": float(np.mean(lat_b0)) if lat_b0 else 0.0
            },
            "steady_state_batch_1_plus": {
                "count": len(lat_steady),
                "p50_ms": float(np.percentile(lat_steady, 50)) if lat_steady else 0.0,
                "p95_ms": float(np.percentile(lat_steady, 95)) if lat_steady else 0.0,
                "mean_ms": float(np.mean(lat_steady)) if lat_steady else 0.0
            }
        },
        "burst_detection": {
            "total_bursts": len(current_bursts),
            "detected_bursts": detected_burst_count,
            "detection_rate": burst_detection_rate,
            "detection_latency_p50_s": burst_p50_s,
            "detection_latency_p95_s": burst_p95_s
        },
        "window_evaluation": {
            "total_windows": tot_uw,
            "attack_windows": tot_atk_w,
            "attack_windows_alerted_k3": atk_w_alerted_k3,
            "attack_window_alert_rate": atk_w_alert_rate,
            "pure_normal_windows": tot_pn_w,
            "pure_normal_alerts_k1": pn_k1,
            "pure_normal_far_k1": (pn_k1 / tot_pn_w) if tot_pn_w > 0 else 0.0,
            "pure_normal_alerts_k2": pn_k2,
            "pure_normal_far_k2": (pn_k2 / tot_pn_w) if tot_pn_w > 0 else 0.0,
            "pure_normal_alerts_k3": pn_k3,
            "pure_normal_far_k3": pn_far_k3
        },
        "microbatch_performance": {
            "total_batches": tot_batches,
            "batch_duration_mean_ms": b_mean,
            "batch_duration_p50_ms": b_p50,
            "batch_duration_p95_ms": b_p95,
            "batch_duration_max_ms": b_max,
            "batches_exceeding_5s": b_over_5s,
            "sustained_rec_per_sec": float(n_consumed / (replay_duration + (tot_batches * 2.0 / tot_batches if tot_batches else 2.0)))
        },
        "watermark_drops": {
            "watermark": WATERMARK_STR,
            "dropped_flows": wm_drops,
            "drop_percentage": wm_drop_pct
        }
    }

    print(f"\n--- Summary for Rate {rate_rec_s} rec/s ---")
    print(f"Reconciliation: Produced={n_produced:,} == Consumed={n_consumed:,} == Scored={n_scored:,} -> {reconciled}")
    print(f"Flow Metrics:   Recall={flow_recall*100:.2f}% | Precision={flow_precision*100:.2f}% | FPR={flow_fpr*100:.2f}%")
    print(f"Latency Steady: p50={res['flow_processing_latency_ms']['steady_state_batch_1_plus']['p50_ms']:.1f}ms | p95={res['flow_processing_latency_ms']['steady_state_batch_1_plus']['p95_ms']:.1f}ms")
    p50_str = f"{burst_p50_s:.2f}s" if burst_p50_s is not None else "N/A"
    p95_str = f"{burst_p95_s:.2f}s" if burst_p95_s is not None else "N/A"
    print(f"Burst Detect:   {detected_burst_count}/{len(current_bursts)} ({burst_detection_rate*100:.1f}%) | Latency p50={p50_str} | p95={p95_str}")
    print(f"Window FAR:     k=1: {pn_k1}/{tot_pn_w} ({pn_k1/tot_pn_w*100:.2f}%) | k=3: {pn_k3}/{tot_pn_w} ({pn_far_k3*100:.2f}%)")
    print(f"Batch Duration: Mean={b_mean:.1f}ms | p95={b_p95:.1f}ms | Batches>5s={b_over_5s}")
    print(f"Watermark Drops: {wm_drops} ({wm_drop_pct:.4f}%)")

    return res

if __name__ == "__main__":
    RATES = [25, 50, 100]
    all_results = {}

    for r in RATES:
        # Check disk space on C: and D: before every rate run
        c_gb = psutil.disk_usage("C:\\").free / (1024**3)
        d_gb = psutil.disk_usage("D:\\").free / (1024**3)
        print(f"\n[DISK CHECK before {r} rec/s]: Free C: = {c_gb:.2f} GB | Free D: = {d_gb:.2f} GB")
        if c_gb < 5.0:
            print(f"ERROR: Free disk space on C: is below 5 GB ({c_gb:.2f} GB). Halting benchmark.")
            spark.stop()
            sys.exit(1)

        all_results[f"rate_{r}"] = run_benchmark_for_rate(r)
        time.sleep(3)

    spark.stop()

    now_utc = datetime.now(timezone.utc)
    final_payload = {
        "timestamp": now_utc.isoformat(),
        "date": now_utc.strftime("%Y-%m-%d"),
        "seed": SEED,
        "slice": {
            "file": "UNSW-NB15_3.csv",
            "session": 1,
            "is_unseen": True,
            "limit": LIMIT_RECORDS,
            "attacks": n_atk,
            "attack_pct": n_atk / n_total * 100.0,
            "total_bursts": len(bursts)
        },
        "configuration": {
            "threshold": THRESHOLD,
            "alert_k": ALERT_K,
            "watermark": WATERMARK_STR,
            "shuffle_partitions": int(SHUFFLE_PARTITIONS),
            "timezone": "UTC"
        },
        "rates": all_results
    }

    # Save to both date-stamped file and 20261031 file
    date_str = now_utc.strftime("%Y%m%d")
    out_file = ROOT / "reports" / "verification" / f"c2b_unseen_detection_{date_str}.json"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(final_payload, indent=2), encoding="utf-8")
    
    out_file_prev = ROOT / "reports" / "verification" / "c2b_unseen_detection_20261031.json"
    out_file_prev.write_text(json.dumps(final_payload, indent=2), encoding="utf-8")
    
    print(f"\nAll benchmark results successfully saved to:")
    print(f"  - {out_file}")
    print(f"  - {out_file_prev}")

