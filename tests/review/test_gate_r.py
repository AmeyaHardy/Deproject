"""
Review Tests: Gate R (Replay source + loader)
============================================
Tests based on Section 4 Gate R of pipeline_review_prompt.md:
  R1: Per-file row counts, 49 columns, 3 extra rows verified Normal, Golden row match.
  R2: Canonical loader verification.
  R3: Slice validation: 41 features, no forbidden columns, clean nominals.
  R4: Event-time and out-of-order arrival verification.
"""

from pathlib import Path
import pytest
from src.raw_loader import (
    RAW_DIR,
    RAW_CSVS,
    FEATURES_CSV,
    COLUMN_NAMES,
    FEATURE_NAMES,
    CONTEXT_COLUMNS,
    EVALUATION_COLUMNS,
    iter_raw_records,
)


def test_r1_features_column_count():
    """Verify that NUSW-NB15_features.csv defines exactly 49 columns."""
    assert len(COLUMN_NAMES) == 49, f"Expected 49 columns, got {len(COLUMN_NAMES)}"
    assert "srcip" in COLUMN_NAMES
    assert "stime" in COLUMN_NAMES
    assert "label" in COLUMN_NAMES
    assert "attack_cat" in COLUMN_NAMES


def test_r1_raw_files_exist_and_row_counts():
    """Verify 4 raw headerless files exist and total 2,540,047 rows."""
    expected_counts = {
        "UNSW-NB15_1.csv": 700001,
        "UNSW-NB15_2.csv": 700001,
        "UNSW-NB15_3.csv": 700001,
        "UNSW-NB15_4.csv": 440044,
    }
    total_rows = 0
    for csv_file in RAW_CSVS:
        assert csv_file.exists(), f"Raw file missing: {csv_file}"
        with csv_file.open("r", encoding="utf-8-sig", errors="ignore") as f:
            count = sum(1 for _ in f)
        expected = expected_counts[csv_file.name]
        assert count == expected, f"{csv_file.name}: expected {expected} rows, got {count}"
        total_rows += count

    assert total_rows == 2540047, f"Total rows {total_rows} != expected 2,540,047"


def test_r1_golden_row_match():
    """
    Golden row test against pipeline_review_prompt.md Section 2:
    record_id 1:0:
      srcip: 59.166.0.0, sport: 1390, dstip: 149.171.126.6, dsport: 53
      stime: 1421927414, ltime: 1421927414
      proto: udp, state: CON, service: dns, dur: 0.001055
      sbytes: 132, dbytes: 164, sttl: 31, dttl: 29
      sload: 500473.9375, dload: 621800.9375, spkts: 2, dpkts: 2
      label: 0 / Normal
    """
    records = list(iter_raw_records(limit=1, file_indices=[1]))
    assert len(records) == 1
    r = records[0]

    assert r["record_id"] == "1:0"
    ctx = r["context"]
    assert ctx["srcip"] == "59.166.0.0"
    assert ctx["sport"] == 1390
    assert ctx["dstip"] == "149.171.126.6"
    assert ctx["dsport"] == 53
    assert ctx["stime"] == 1421927414
    assert ctx["ltime"] == 1421927414

    feat = r["features"]
    assert feat["proto"] == "udp"
    assert feat["state"] == "CON"
    assert feat["service"] == "dns"
    assert abs(feat["dur"] - 0.001055) < 1e-6
    assert feat["sbytes"] == 132
    assert feat["dbytes"] == 164
    assert feat["sttl"] == 31
    assert feat["dttl"] == 29
    assert abs(feat["sload"] - 500473.9375) < 1e-2
    assert abs(feat["dload"] - 621800.9375) < 1e-2
    assert feat["spkts"] == 2
    assert feat["dpkts"] == 2

    assert r["evaluation"]["label"] == 0
    assert r["evaluation"]["attack_cat"] == "Normal"


def test_r3_slice_contract_and_anti_leakage():
    """Verify 41 behavioral features, zero leakage of context/evaluation, and clean nominals."""
    slice_records = list(iter_raw_records(limit=100, file_indices=[1]))
    assert len(slice_records) == 100

    forbidden = CONTEXT_COLUMNS | EVALUATION_COLUMNS | {"id"}

    for r in slice_records:
        assert set(r.keys()) == {"record_id", "event_time", "ingest_time", "context", "features", "evaluation"}

        feat_keys = set(r["features"].keys())
        assert len(feat_keys) == 41, f"Expected 41 features, got {len(feat_keys)}"

        leaked = feat_keys & forbidden
        assert not leaked, f"Forbidden columns leaked into features: {leaked}"

        # Check '-' was cleaned to 'none' in nominals
        assert r["features"]["service"] != "-", "service '-' was not cleaned to 'none'"
        assert r["features"]["state"] != "-", "state '-' was not cleaned to 'none'"

        # Label consistency
        if r["evaluation"]["attack_cat"] != "Normal":
            assert r["evaluation"]["label"] == 1
