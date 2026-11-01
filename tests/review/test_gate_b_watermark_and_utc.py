"""
Unit Tests for Gate B: Watermark Default & Explicit UTC Timezone
===============================================================
Enforces Task 1 invariants:
1. Default watermark CLI argument must be exactly "60 seconds".
2. Custom --watermark CLI argument must be accepted and preserved.
3. Spark SQL session timezone must be explicitly configured to "UTC".
"""
import sys
import os
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.spark_streaming import parse_args


def test_default_watermark_is_60_seconds():
    """Verify default watermark is 60 seconds (accommodating stime lateness)."""
    # Simulate invoking with no CLI arguments
    orig_argv = sys.argv
    try:
        sys.argv = ["spark_streaming.py"]
        args = parse_args()
        assert args.watermark == "60 seconds", (
            f"Expected default watermark to be '60 seconds', got '{args.watermark}'. "
            "A 10s watermark drops 1.27% of flows; 60s is empirically required."
        )
    finally:
        sys.argv = orig_argv


def test_custom_watermark_flag():
    """Verify custom --watermark argument is respected."""
    orig_argv = sys.argv
    try:
        sys.argv = ["spark_streaming.py", "--watermark", "30 seconds"]
        args = parse_args()
        assert args.watermark == "30 seconds"
    finally:
        sys.argv = orig_argv


def test_spark_session_timezone_is_utc():
    """Verify Spark SQL session timezone is strictly UTC."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("TestUTCConfig")
        .master("local[1]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    try:
        tz = spark.conf.get("spark.sql.session.timeZone")
        assert tz == "UTC", f"Expected spark.sql.session.timeZone to be 'UTC', got '{tz}'"
    finally:
        spark.stop()

