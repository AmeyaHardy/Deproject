"""
Review Tests: Gate S (PySpark Structured Streaming & Event-Time Windowing)
========================================================================
Tests based on Section 4 Gate S of pipeline_review_prompt.md:
  S2: Event time extraction from payload event_time (from stime).
      Explicit schema with nullable fields parses without loss.
  S4: Correctness of tumbling 5s event-time window aggregation.
"""

import json
import os
import sys
from pathlib import Path
import pytest

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from src.raw_loader import iter_raw_records
from src.spark_streaming import message_schema


@pytest.fixture(scope="module")
def spark_session():
    """Create a lightweight local Spark session for contract testing."""
    from pyspark.sql import SparkSession
    builder = (
        SparkSession.builder
        .appName("GateS-ReviewTest")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if Path("D:/").exists():
        builder = builder.config("spark.local.dir", "D:/spark_temp")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


def test_s2_spark_schema_parses_canonical_raw_record(spark_session):
    """Verify PySpark from_json correctly unpacks the 6-key message without data loss."""
    from pyspark.sql import functions as F

    raw_records = list(iter_raw_records(limit=25, file_indices=[1]))
    json_lines = [json.dumps(r) for r in raw_records]

    # Cast to string and parse with message_schema
    parsed = (
        spark_session.createDataFrame([(j,) for j in json_lines], ["json_str"])
        .select(F.from_json(F.col("json_str"), message_schema).alias("data"))
        .filter(F.col("data").isNotNull())
        .select(
            F.col("data.record_id").alias("record_id"),
            F.col("data.context.srcip").alias("srcip"),
            F.to_timestamp(F.col("data.event_time")).alias("event_time"),
            F.col("data.features.proto").alias("proto"),
            F.col("data.features.sbytes").alias("sbytes"),
            F.col("data.features.dbytes").alias("dbytes"),
            F.col("data.evaluation.label").alias("label"),
            F.col("data.evaluation.attack_cat").alias("attack_cat"),
        )
    )

    rows = parsed.collect()
    assert len(rows) == 25, f"Expected 25 parsed rows, got {len(rows)}"

    first = rows[0]
    assert first["record_id"] == "1:0"
    assert first["srcip"] == "59.166.0.0"
    assert first["proto"] == "udp"
    assert first["sbytes"] == 132.0
    assert first["dbytes"] == 164.0
    assert first["label"] == 0
    assert first["attack_cat"] == "Normal"
    assert first["event_time"] is not None


def test_s4_spark_tumbling_5s_windows(spark_session):
    """Verify 5-second tumbling window aggregation groups event times properly."""
    from pyspark.sql import functions as F

    raw_records = list(iter_raw_records(limit=100, file_indices=[1]))
    json_lines = [json.dumps(r) for r in raw_records]

    parsed = (
        spark_session.createDataFrame([(j,) for j in json_lines], ["json_str"])
        .select(F.from_json(F.col("json_str"), message_schema).alias("data"))
        .select(
            F.to_timestamp(F.col("data.event_time")).alias("event_time"),
            F.col("data.features.proto").alias("proto"),
            F.col("data.features.sbytes").alias("sbytes"),
            F.col("data.features.dbytes").alias("dbytes"),
        )
    )

    windowed = (
        parsed
        .groupBy(F.window(F.col("event_time"), "5 seconds"))
        .agg(
            F.count("*").alias("record_count"),
            F.sum("sbytes").alias("total_sbytes"),
            F.sum("dbytes").alias("total_dbytes"),
        )
    )

    results = windowed.collect()
    assert len(results) > 0, "No windows generated from 100 raw records"
    for w in results:
        assert w["record_count"] > 0
        assert w["window"] is not None
        print(f"Window: {w['window'].start} to {w['window'].end} | Count: {w['record_count']}")
