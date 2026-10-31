"""
Spark Structured Streaming Consumer
====================================
Consumes JSON records from the Kafka 'network-traffic' topic and computes
five-second tumbling-window traffic statistics.

Data flow:
  csv_replayer.py  -->  Kafka (network-traffic)  -->  THIS SCRIPT  -->  console

The JSON schema expected from Kafka matches the output of csv_replayer.py:
    {
      "record_id": "1",
      "event_time": "2026-10-13T08:48:46.112544+00:00",
      "features": { "dur": 0.12, "proto": "tcp", "spkts": 6, ... },
      "evaluation": { "label": 0, "attack_cat": "Normal" }
    }

Usage:
    # Set JAVA_HOME first (required for PySpark):
    $env:JAVA_HOME = "C:\\Program Files\\Java\\jre1.8.0_461"

    # Run the streaming consumer:
    python src/spark_streaming.py

    # Then, in another terminal, replay records into Kafka:
    python src/csv_replayer.py --kafka --limit 50 --rate 10
"""

import argparse
import json
import os
import sys
import shutil
import time
from pathlib import Path

# Pre-configure Windows Hadoop and Java environments before JVM initialization
USER_PROFILE = os.environ.get("USERPROFILE", r"C:\Users\sweni")
hadoop_home = Path(USER_PROFILE) / "hadoop"
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

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    LongType,
)
from src.raw_loader import FEATURE_NAMES

# ── Configuration Defaults ───────────────────────────────────────────────────

DEFAULT_KAFKA_BROKER = "localhost:9092"
DEFAULT_KAFKA_TOPIC = "network-traffic"
DEFAULT_CHECKPOINT_DIR = str(
    Path("data/processed/spark-checkpoints/network-traffic-windows").resolve()
)

# ── JSON Schema ──────────────────────────────────────────────────────────────
#
# This schema must match the exact output of csv_replayer.py's build_record().
# The producer converts numeric strings to int/float and keeps the rest as
# strings.  Most feature values are numeric, but proto/service/state are
# strings.  We declare numeric columns as DoubleType so both int and float
# values parse correctly (Spark treats JSON ints as valid doubles).

features_schema = StructType([
    StructField("dur",                DoubleType()),
    StructField("proto",              StringType()),
    StructField("service",            StringType()),
    StructField("state",              StringType()),
    StructField("spkts",              DoubleType()),
    StructField("dpkts",              DoubleType()),
    StructField("sbytes",             DoubleType()),
    StructField("dbytes",             DoubleType()),
    StructField("rate",               DoubleType()),
    StructField("sttl",               DoubleType()),
    StructField("dttl",               DoubleType()),
    StructField("sload",              DoubleType()),
    StructField("dload",              DoubleType()),
    StructField("sloss",              DoubleType()),
    StructField("dloss",              DoubleType()),
    StructField("sinpkt",             DoubleType()),
    StructField("dinpkt",             DoubleType()),
    StructField("sjit",               DoubleType()),
    StructField("djit",               DoubleType()),
    StructField("swin",               DoubleType()),
    StructField("stcpb",              DoubleType()),
    StructField("dtcpb",              DoubleType()),
    StructField("dwin",               DoubleType()),
    StructField("tcprtt",             DoubleType()),
    StructField("synack",             DoubleType()),
    StructField("ackdat",             DoubleType()),
    StructField("smean",              DoubleType()),
    StructField("dmean",              DoubleType()),
    StructField("trans_depth",        DoubleType()),
    StructField("response_body_len",  DoubleType()),
    StructField("ct_srv_src",         DoubleType()),
    StructField("ct_state_ttl",       DoubleType()),
    StructField("ct_dst_ltm",         DoubleType()),
    StructField("ct_src_dport_ltm",   DoubleType()),
    StructField("ct_dst_sport_ltm",   DoubleType()),
    StructField("ct_dst_src_ltm",     DoubleType()),
    StructField("is_ftp_login",       DoubleType()),
    StructField("ct_ftp_cmd",         DoubleType()),
    StructField("ct_flw_http_mthd",   DoubleType()),
    StructField("ct_src_ltm",         DoubleType()),
    StructField("ct_srv_dst",         DoubleType()),
    StructField("is_sm_ips_ports",    DoubleType()),
    # Support alternate naming in raw 49-column files
    StructField("sintpkt",            DoubleType()),
    StructField("dintpkt",            DoubleType()),
    StructField("smeansz",            DoubleType()),
    StructField("dmeansz",            DoubleType()),
    StructField("res_bdy_len",        DoubleType()),
])

context_schema = StructType([
    StructField("srcip",  StringType()),
    StructField("sport",  IntegerType()),
    StructField("dstip",  StringType()),
    StructField("dsport", IntegerType()),
    StructField("stime",  LongType()),
    StructField("ltime",  LongType()),
])

evaluation_schema = StructType([
    StructField("label",      IntegerType()),
    StructField("attack_cat", StringType()),
])

message_schema = StructType([
    StructField("record_id",   StringType()),
    StructField("event_time",  StringType()),
    StructField("ingest_time", StringType()),
    StructField("context",     context_schema),
    StructField("features",    features_schema),
    StructField("evaluation",  evaluation_schema),
])


# ── Main ─────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Spark Structured Streaming Network Monitor & Benchmark"
    )
    parser.add_argument(
        "--broker",
        type=str,
        default=DEFAULT_KAFKA_BROKER,
        help=f"Kafka bootstrap broker (default: {DEFAULT_KAFKA_BROKER}).",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=DEFAULT_KAFKA_TOPIC,
        help=f"Kafka topic to consume from (default: {DEFAULT_KAFKA_TOPIC}).",
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=str,
        default=DEFAULT_CHECKPOINT_DIR,
        help=f"Checkpoint directory (default: {DEFAULT_CHECKPOINT_DIR}).",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=200,
        help="spark.sql.shuffle.partitions (default: 200).",
    )
    parser.add_argument(
        "--trigger-interval",
        type=str,
        default="5 seconds",
        help="Micro-batch processing trigger interval (default: 5 seconds).",
    )
    parser.add_argument(
        "--stop-after-records",
        type=int,
        default=None,
        help="Stop query after ingesting N records (used for controlled benchmarks).",
    )
    parser.add_argument(
        "--metrics-out",
        type=str,
        default=None,
        help="Output path to write benchmark execution metrics JSON.",
    )
    # Anomaly scoring options (Gate A7 / Part B)
    parser.add_argument(
        "--score",
        action="store_true",
        default=False,
        help="Enable real-time anomaly detection scoring inside Spark.",
    )
    parser.add_argument(
        "--model-dir",
        type=str,
        default=str(Path("data/processed/models").resolve()),
        help="Directory containing trained model and preprocessing artifacts.",
    )
    parser.add_argument(
        "--threshold-file",
        type=str,
        default=str(Path("data/processed/models/calibrated_threshold.json").resolve()),
        help="JSON file containing the calibrated anomaly detection threshold.",
    )
    parser.add_argument(
        "--alert-k",
        type=int,
        default=3,
        help="Minimum number of flagged flows in a 5s window to trigger a window_alert (default: 3).",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # ── Guard: JAVA_HOME ────────────────────────────────────────────────
    if not os.environ.get("JAVA_HOME"):
        default_java = Path(r"C:\Program Files\Java\jre1.8.0_461")
        if default_java.exists():
            os.environ["JAVA_HOME"] = str(default_java)
            print(f"INFO: JAVA_HOME auto-set to: {os.environ['JAVA_HOME']}")
        else:
            print(
                "ERROR: JAVA_HOME is not set.\n"
                '       Run:  $env:JAVA_HOME = "C:\\Program Files\\Java\\jre1.8.0_461"\n'
                "       Then re-run this script.",
                file=sys.stderr,
            )
            sys.exit(1)

    # ── Guard: HADOOP_HOME (Required for Spark on Windows) ──────────────
    hadoop_home = os.environ.get("HADOOP_HOME")
    if not hadoop_home:
        default_hadoop = Path(os.environ.get("USERPROFILE", "")) / "hadoop"
        if default_hadoop.exists():
            hadoop_home = str(default_hadoop)
            os.environ["HADOOP_HOME"] = hadoop_home
            print(f"INFO: HADOOP_HOME auto-set to: {hadoop_home}")
    
    hadoop_bin = str(Path(hadoop_home) / "bin") if hadoop_home else ""
    if hadoop_bin and hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = f"{hadoop_bin};{os.environ.get('PATH', '')}"

    # ── Build Spark session ─────────────────────────────────────────────
    # The Kafka connector JAR is not bundled with PySpark.
    # We tell Spark to download it automatically via --packages / config.
    # spark-sql-kafka requires matching Spark + Scala versions:
    #   PySpark 3.5.x  →  Spark 3.5  →  Scala 2.12
    print("Initializing Spark session...")
    print(f"  JAVA_HOME          = {os.environ.get('JAVA_HOME')}")
    print(f"  Kafka broker       = {args.broker}")
    print(f"  Kafka topic        = {args.topic}")
    print(f"  Checkpoint         = {args.checkpoint_dir}")
    print(f"  Shuffle partitions = {args.shuffle_partitions}")
    print(f"  Trigger interval   = {args.trigger_interval}")
    if args.stop_after_records:
        print(f"  Benchmark mode     = Stop after {args.stop_after_records} records")
    print()

    builder = (
        SparkSession.builder
        .appName("NetworkTrafficStreaming")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
        )
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        # Use FileSystemBasedCheckpointFileManager and java.library.path to prevent Windows NativeIO errors
        .config(
            "spark.sql.streaming.checkpointFileManagerClass",
            "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager",
        )
        .config("spark.driver.extraJavaOptions", f"-Djava.library.path={hadoop_bin}")
        .config("spark.executor.extraJavaOptions", f"-Djava.library.path={hadoop_bin}")
        # Reduce noisy log output
        .config("spark.ui.showConsoleProgress", "false")
    )
    if Path("D:/").exists():
        d_temp = Path("D:/spark_temp")
        d_temp.mkdir(parents=True, exist_ok=True)
        builder = builder.config("spark.local.dir", str(d_temp.resolve()))

    spark = builder.getOrCreate()

    # Explicitly load hadoop.dll to avoid Windows NativeIO.access0 UnsatisfiedLinkError
    if os.name == "nt" and hadoop_bin:
        hadoop_dll = Path(hadoop_bin) / "hadoop.dll"
        if hadoop_dll.exists():
            try:
                spark.sparkContext._jvm.java.lang.System.load(str(hadoop_dll.resolve()))
            except Exception:
                pass

    # Lower Spark's own log verbosity (keep WARN and above)
    spark.sparkContext.setLogLevel("WARN")

    print("Spark session created successfully.\n")

    # ── Real-Time Scoring Setup (Gate A7) ────────────────────────────────
    if args.score:
        model_path = Path(args.model_dir) / "rf_detector.joblib"
        if not model_path.exists():
            model_path = Path("data/processed/models/rf_detector.joblib").resolve()
        preproc_path = Path("data/processed/preprocessing_artifacts").resolve()

        thresh_path = Path(args.threshold_file)
        threshold = 0.5
        if thresh_path.exists():
            try:
                t_cfg = json.loads(thresh_path.read_text(encoding="utf-8"))
                threshold = float(t_cfg.get("threshold", 0.5))
            except Exception as e:
                print(f"WARN: Could not load threshold from {thresh_path}: {e}")

        print(f"Real-time scoring enabled:")
        print(f"  Model file       = {model_path}")
        print(f"  Preprocessor dir = {preproc_path}")
        print(f"  Threshold        = {threshold:.4f} (from {thresh_path})")
        print(f"  Alert threshold  = >= {args.alert_k} flagged flows per window\n")

        model_file_str = str(model_path.resolve())
        preproc_dir_str = str(preproc_path.resolve())

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

        import pandas as pd

        @F.pandas_udf(DoubleType())
        def score_anomaly_udf(*cols: pd.Series) -> pd.Series:
            model, preproc = get_worker_artifacts()
            # Feature matrix strictly containing the 41 behavioral features
            pdf = pd.DataFrame(dict(zip(FEATURE_NAMES, cols)))
            pdf_proc = preproc.transform(pdf)
            scores = model.predict_proba(pdf_proc)[:, 1]
            return pd.Series(scores, dtype="float64")

    # ── Read from Kafka ─────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.broker)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON ──────────────────────────────────────────────────────
    parsed_base = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(
            F.from_json(F.col("json_str"), message_schema).alias("data")
        )
        .filter(F.col("data").isNotNull())
    )

    if args.score:
        feature_cols = [
            F.coalesce(
                F.col(f"data.features.{c}"),
                F.lit(0.0) if c not in ("proto", "service", "state") else F.lit("none")
            )
            for c in FEATURE_NAMES
        ]
        parsed_scored = parsed_base.withColumn("anomaly_score", score_anomaly_udf(*feature_cols))
        parsed_scored = parsed_scored.withColumn("flagged", (F.col("anomaly_score") >= threshold).cast("integer"))
    else:
        parsed_scored = parsed_base.withColumn("anomaly_score", F.lit(0.0).cast("double"))
        parsed_scored = parsed_scored.withColumn("flagged", F.lit(0).cast("integer"))

    parsed = (
        parsed_scored
        .select(
            F.col("data.record_id").alias("record_id"),
            F.col("data.context.srcip").alias("srcip"),
            F.to_timestamp(F.col("data.event_time")).alias("event_time"),
            F.col("data.ingest_time").alias("ingest_time"),
            F.col("data.features.proto").alias("proto"),
            F.col("data.features.spkts").alias("spkts"),
            F.col("data.features.dpkts").alias("dpkts"),
            F.col("data.features.sbytes").alias("sbytes"),
            F.col("data.features.dbytes").alias("dbytes"),
            F.col("data.features.dur").alias("dur"),
            F.col("data.features.rate").alias("rate"),
            F.col("data.features.sload").alias("sload"),
            F.col("data.features.dload").alias("dload"),
            F.col("data.evaluation.label").alias("label"),
            F.col("data.evaluation.attack_cat").alias("attack_cat"),
            F.col("anomaly_score"),
            F.col("flagged"),
        )
        .filter(F.col("event_time").isNotNull())
    )

    # ── Watermark ───────────────────────────────────────────────────────
    watermarked = parsed.withWatermark("event_time", "10 seconds")

    # ── 5-Second Tumbling Window Aggregations ───────────────────────────
    agg_exprs = [
        F.count("*").alias("record_count"),
        F.round(F.avg("spkts"), 2).alias("avg_src_pkts"),
        F.round(F.avg("dpkts"), 2).alias("avg_dst_pkts"),
        F.round(F.avg("sbytes"), 2).alias("avg_src_bytes"),
        F.round(F.avg("dbytes"), 2).alias("avg_dst_bytes"),
        F.round(F.sum("sbytes") + F.sum("dbytes"), 2).alias("total_bytes"),
        F.round(F.avg("dur"), 4).alias("avg_duration"),
        F.round(F.avg("rate"), 2).alias("avg_rate"),
        F.round(F.avg("sload"), 2).alias("avg_src_load"),
        F.round(F.avg("dload"), 2).alias("avg_dst_load"),
        F.count(F.when(F.col("proto") == "tcp", True)).alias("tcp_count"),
        F.count(F.when(F.col("proto") == "udp", True)).alias("udp_count"),
        F.count(
            F.when(
                ~F.col("proto").isin("tcp", "udp"), True
            )
        ).alias("other_proto_count"),
        F.sum("flagged").cast("int").alias("flagged_count"),
        F.when(F.sum("flagged") >= args.alert_k, 1).otherwise(0).alias("window_alert"),
        F.sum(F.col("label")).cast("int").alias("attack_records"),
    ]

    window_stats = (
        watermarked
        .groupBy(
            F.window("event_time", "5 seconds"),
        )
        .agg(*agg_exprs)
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "record_count",
            "flagged_count",
            "window_alert",
            "avg_src_pkts",
            "avg_dst_pkts",
            "avg_src_bytes",
            "avg_dst_bytes",
            "total_bytes",
            "avg_duration",
            "avg_rate",
            "avg_src_load",
            "avg_dst_load",
            "tcp_count",
            "udp_count",
            "other_proto_count",
            "attack_records",
        )
    )

    # ── Write to Console ────────────────────────────────────────────────
    print("Starting streaming query...")
    print(f"Waiting for records from Kafka topic '{args.topic}' ...")
    print(f"(In another terminal, run: python src/csv_replayer.py --kafka --topic {args.topic} --limit 50 --rate 10)")
    print()
    print("=" * 80)
    print("  STREAMING OUTPUT — 5-Second Tumbling Window Aggregations")
    print("  Note: 'attack_records' is ground-truth count, NOT detection output.")
    print("=" * 80)
    print()

    query = (
        window_stats.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 50)
        # Process every trigger interval (default 5 seconds)
        .trigger(processingTime=args.trigger_interval)
        .option("checkpointLocation", args.checkpoint_dir)
        .queryName(f"network_traffic_{args.topic.replace('-', '_')}")
        .start()
    )

    if args.stop_after_records is not None:
        print(f"\n[BENCHMARK] Monitoring query until {args.stop_after_records} records are processed...")
        start_time = time.time()
        seen_batches = set()
        recorded_batches = []
        cumulative_rows = 0
        empty_batches_after_target = 0

        try:
            while query.isActive:
                time.sleep(1)
                recent = query.recentProgress
                for p in recent:
                    bid = p.get("batchId")
                    if bid is not None and bid not in seen_batches:
                        seen_batches.add(bid)
                        num_rows = p.get("numInputRows", 0)
                        cumulative_rows += num_rows
                        durations = p.get("durationMs", {})
                        total_batch_ms = durations.get("triggerExecution", 0)
                        input_rate = p.get("inputRowsPerSecond", 0.0)
                        proc_rate = p.get("processedRowsPerSecond", 0.0)

                        recorded_batches.append({
                            "batchId": bid,
                            "numInputRows": num_rows,
                            "cumulativeRows": cumulative_rows,
                            "triggerExecutionMs": total_batch_ms,
                            "inputRowsPerSecond": input_rate,
                            "processedRowsPerSecond": proc_rate,
                            "timestamp": p.get("timestamp"),
                        })

                        print(
                            f"  [BENCHMARK Batch {bid}] Input: {num_rows} rows | "
                            f"Duration: {total_batch_ms} ms | "
                            f"Cumulative: {cumulative_rows}/{args.stop_after_records}"
                        )

                        if cumulative_rows >= args.stop_after_records:
                            empty_batches_after_target += 1
                            if empty_batches_after_target >= 1:
                                print(
                                    f"\n[BENCHMARK] Target of {args.stop_after_records} records reached "
                                    f"and final window flushed. Stopping query..."
                                )
                                query.stop()
                                break
        except KeyboardInterrupt:
            print("\nBenchmark interrupted by user.")
            query.stop()

        elapsed_total = time.time() - start_time

        batch_durations = [b["triggerExecutionMs"] for b in recorded_batches]
        avg_batch_ms = sum(batch_durations) / len(batch_durations) if batch_durations else 0
        max_batch_ms = max(batch_durations) if batch_durations else 0
        min_batch_ms = min(batch_durations) if batch_durations else 0
        records_per_sec = (cumulative_rows / elapsed_total) if elapsed_total > 0 else 0

        trigger_ms = 5000
        try:
            parts = args.trigger_interval.split()
            if len(parts) == 2 and "sec" in parts[1]:
                trigger_ms = int(parts[0]) * 1000
        except Exception:
            pass
        lagging_batches = sum(1 for d in batch_durations if d > trigger_ms)

        summary = {
            "topic": args.topic,
            "shufflePartitions": args.shuffle_partitions,
            "targetRecords": args.stop_after_records,
            "actualRecordsProcessed": cumulative_rows,
            "totalElapsedSec": round(elapsed_total, 2),
            "numBatches": len(recorded_batches),
            "avgBatchDurationMs": round(avg_batch_ms, 2),
            "minBatchDurationMs": min_batch_ms,
            "maxBatchDurationMs": max_batch_ms,
            "recordsPerSec": round(records_per_sec, 2),
            "laggingBatchesCount": lagging_batches,
            "checkpointDir": args.checkpoint_dir,
            "batches": recorded_batches,
        }

        print("\n" + "=" * 80)
        print("  BENCHMARK SUMMARY")
        print("=" * 80)
        print(f"  Topic:                     {summary['topic']}")
        print(f"  Shuffle Partitions:        {summary['shufflePartitions']}")
        print(f"  Records Processed:         {summary['actualRecordsProcessed']} / {summary['targetRecords']}")
        print(f"  Total Elapsed Time:        {summary['totalElapsedSec']} s")
        print(f"  Batches Executed:          {summary['numBatches']}")
        print(f"  Avg Batch Duration:        {summary['avgBatchDurationMs']} ms")
        print(f"  Max Batch Duration:        {summary['maxBatchDurationMs']} ms")
        print(f"  Processing Rate:           {summary['recordsPerSec']} rec/s")
        print(f"  Batches Exceeding Trigger: {summary['laggingBatchesCount']}")
        print("=" * 80 + "\n")

        if args.metrics_out:
            metrics_path = Path(args.metrics_out)
            metrics_path.parent.mkdir(parents=True, exist_ok=True)
            metrics_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
            print(f"Metrics saved to: {metrics_path}")

        spark.stop()
        print("Spark session stopped.")

    else:
        try:
            # Block until the query is stopped (Ctrl+C)
            query.awaitTermination()
        except KeyboardInterrupt:
            print("\n\nStopping streaming query...")
            query.stop()
            spark.stop()
            print("Spark session stopped.")


if __name__ == "__main__":
    main()
