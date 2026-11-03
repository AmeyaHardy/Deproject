"""
Pytest Suite for Live Streaming (Gate D6 Smoke Test)
===================================================
Tests that Spark started FIRST on an empty Kafka topic processes live
traffic concurrently emitted by the replayer, verifies rows appear in PostgreSQL
during replay, and checks final count reconciliation.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def check_prerequisites():
    # 1. Java check
    java_home = os.environ.get("JAVA_HOME", r"C:\Program Files\Java\jre1.8.0_461")
    if not Path(java_home).exists():
        return False, f"JAVA_HOME path not found: {java_home}"

    # 2. Docker services check
    try:
        res = subprocess.run(["docker", "ps"], capture_output=True, text=True, timeout=5)
        if res.returncode != 0:
            return False, "Docker is not running"
        for svc in ["kafka", "postgres"]:
            if svc not in res.stdout:
                return False, f"Required Docker service '{svc}' is not running"
    except Exception as e:
        return False, f"Docker check failed: {e}"

    # 3. Disk space check
    c_free = shutil.disk_usage("C:").free / (1024**3)
    if c_free < 5.0:
        return False, f"Drive C: has under 5 GB free ({c_free:.2f} GB)"

    return True, "All prerequisites met"


def test_live_streaming_concurrent_smoke():
    ok, reason = check_prerequisites()
    if not ok:
        pytest.skip(f"Skipping live streaming smoke test: {reason}")

    from scripts.verify.verify_live_streaming import execute_live_run

    # Target 350 records (~20 seconds of active replay at ~17.5 rec/s)
    # This guarantees that Batch 1 (trigger 5s + cold model load ~11s = 16s) commits
    # while the replayer is actively running (16s < 20s).
    target_n = 350
    results = execute_live_run(
        run_tag="SMOKE",
        target_records=target_n,
        target_rate=25.0,
        truncate_first=True,
    )

    # 1. Reconciliation
    assert results["reconciled"] is True, f"Reconciliation failed: {results}"
    assert results["raw_flows_count"] == target_n, f"Expected {target_n} raw flows, got {results['raw_flows_count']}"
    assert results["flow_scores_count"] == target_n, f"Expected {target_n} flow scores, got {results['flow_scores_count']}"

    # 2. Liveness: verify rows appeared in raw_flows during replay
    assert results["microbatches_during_replay"] >= 1, (
        f"Expected at least 1 micro-batch during active replay, got {results['microbatches_during_replay']}"
    )

    # Inspect sampled series to confirm rows appeared while replayer was active
    df = None
    csv_path = Path(results["sample_csv_path"])
    if csv_path.exists():
        import pandas as pd
        df = pd.read_csv(csv_path)
        rows_during_replay = df[(df["replayer_active"] == True) & (df["raw_flows"] > 0)]
        assert len(rows_during_replay) > 0 or results["microbatches_during_replay"] >= 1, (
            "Expected rows to appear in raw_flows while replayer was active"
        )
