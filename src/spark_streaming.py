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

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

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
])

evaluation_schema = StructType([
    StructField("label",      IntegerType()),
    StructField("attack_cat", StringType()),
])

message_schema = StructType([
    StructField("record_id",  StringType()),
    StructField("event_time", StringType()),
    StructField("features",   features_schema),
    StructField("evaluation", evaluation_schema),
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
    if not os.environ.get("HADOOP_HOME"):
        default_hadoop = Path(os.environ.get("USERPROFILE", "")) / "hadoop"
        if default_hadoop.exists():
            os.environ["HADOOP_HOME"] = str(default_hadoop)
            bin_dir = str(default_hadoop / "bin")
            if bin_dir not in os.environ.get("PATH", ""):
                os.environ["PATH"] = f"{bin_dir};{os.environ.get('PATH', '')}"
            print(f"INFO: HADOOP_HOME auto-set to: {os.environ['HADOOP_HOME']}")

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
        # Reduce noisy log output
        .config("spark.ui.showConsoleProgress", "false")
    )
    if Path("D:/").exists():
        d_temp = Path("D:/spark_temp")
        d_temp.mkdir(parents=True, exist_ok=True)
        builder = builder.config("spark.local.dir", str(d_temp.resolve()))

    spark = builder.getOrCreate()

    # Lower Spark's own log verbosity (keep WARN and above)
    spark.sparkContext.setLogLevel("WARN")

    print("Spark session created successfully.\n")

    # ── Read from Kafka ─────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.broker)
        .option("subscribe", args.topic)
        # Start from the earliest available offset so we don't miss messages
        # that were produced before this consumer started.
        .option("startingOffsets", "earliest")
        # failOnDataLoss=false prevents the stream from crashing if Kafka
        # has deleted old segments (common during development restarts).
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON ──────────────────────────────────────────────────────
    # Kafka gives us: key (binary), value (binary), topic, partition,
    # offset, timestamp, timestampType.
    # We only need the value, which contains our JSON payload.
    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(
            F.from_json(F.col("json_str"), message_schema).alias("data")
        )
        # Filter out rows where JSON parsing failed (data will be null)
        .filter(F.col("data").isNotNull())
        .select(
            F.col("data.record_id").alias("record_id"),
            # Parse the ISO-8601 event_time string into a proper timestamp
            F.to_timestamp(F.col("data.event_time")).alias("event_time"),
            # Traffic metrics we'll aggregate
            F.col("data.features.proto").alias("proto"),
            F.col("data.features.spkts").alias("spkts"),
            F.col("data.features.dpkts").alias("dpkts"),
            F.col("data.features.sbytes").alias("sbytes"),
            F.col("data.features.dbytes").alias("dbytes"),
            F.col("data.features.dur").alias("dur"),
            F.col("data.features.rate").alias("rate"),
            F.col("data.features.sload").alias("sload"),
            F.col("data.features.dload").alias("dload"),
            # Keep ground-truth separate — NOT used for detection
            F.col("data.evaluation.label").alias("label"),
            F.col("data.evaluation.attack_cat").alias("attack_cat"),
        )
        # Drop records where event_time couldn't be parsed
        .filter(F.col("event_time").isNotNull())
    )

    # ── Watermark ───────────────────────────────────────────────────────
    # We set a 10-second watermark to allow for event-time disorder.
    # Because csv_replayer.py generates event_time at emission time,
    # records should be approximately in order.  A 10-second watermark
    # is generous enough to handle minor delays without dropping data.
    watermarked = parsed.withWatermark("event_time", "10 seconds")

    # ── 5-Second Tumbling Window Aggregations ───────────────────────────
    #
    # Why "update" output mode?
    # ─────────────────────────
    # "complete" mode outputs the entire result table on every trigger,
    #   which grows unbounded for streaming data.
    # "append" mode only outputs rows after the watermark passes them,
    #   meaning results appear with a delay equal to the watermark.
    # "update" mode outputs only changed/new rows each trigger, which
    #   gives the fastest feedback during development and keeps output
    #   compact.  It works well with windowed aggregations + watermark.
    #
    # For a later stage (writing to PostgreSQL/Parquet), we may switch
    # to "append" mode for exactly-once guarantees.

    window_stats = (
        watermarked
        .groupBy(
            F.window("event_time", "5 seconds"),
        )
        .agg(
            F.count("*").alias("record_count"),
            # Packet statistics
            F.round(F.avg("spkts"), 2).alias("avg_src_pkts"),
            F.round(F.avg("dpkts"), 2).alias("avg_dst_pkts"),
            # Byte statistics
            F.round(F.avg("sbytes"), 2).alias("avg_src_bytes"),
            F.round(F.avg("dbytes"), 2).alias("avg_dst_bytes"),
            F.round(F.sum("sbytes") + F.sum("dbytes"), 2).alias("total_bytes"),
            # Duration and rate
            F.round(F.avg("dur"), 4).alias("avg_duration"),
            F.round(F.avg("rate"), 2).alias("avg_rate"),
            # Load
            F.round(F.avg("sload"), 2).alias("avg_src_load"),
            F.round(F.avg("dload"), 2).alias("avg_dst_load"),
            # Protocol distribution (top protocols as a summary string)
            F.count(F.when(F.col("proto") == "tcp", True)).alias("tcp_count"),
            F.count(F.when(F.col("proto") == "udp", True)).alias("udp_count"),
            F.count(
                F.when(
                    ~F.col("proto").isin("tcp", "udp"), True
                )
            ).alias("other_proto_count"),
            # Ground-truth stats (for offline evaluation only, NOT detection)
            F.sum(F.col("label")).cast("int").alias("attack_records"),
        )
        # Format the window boundaries for readable output
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "record_count",
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
