"""
Canonical Raw UNSW-NB15 Dataset Loader
======================================
Loads the 4 headerless raw CSV files (UNSW-NB15_1..4.csv) using NUSW-NB15_features.csv
schema definitions, producing standardized 6-key records:
  - record_id:  "{file_idx}:{row_idx}"
  - event_time: ISO-8601 UTC timestamp computed from stime
  - ingest_time: ISO-8601 UTC timestamp of current processing
  - context:    srcip, sport, dstip, dsport, stime, ltime (6 network identifiers)
  - features:   41 behavioral flow metrics (49 total - 6 context - 2 evaluation)
  - evaluation: label, attack_cat (clean ground-truth)
"""

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

RAW_DIR = Path("data/raw/OneDrive_1_10-12-2026")
FEATURES_CSV = RAW_DIR / "NUSW-NB15_features.csv"
RAW_CSVS = [
    RAW_DIR / "UNSW-NB15_1.csv",
    RAW_DIR / "UNSW-NB15_2.csv",
    RAW_DIR / "UNSW-NB15_3.csv",
    RAW_DIR / "UNSW-NB15_4.csv",
]

CONTEXT_COLUMNS = {"srcip", "sport", "dstip", "dsport", "stime", "ltime"}
EVALUATION_COLUMNS = {"label", "attack_cat"}
INTEGER_COLUMNS = {
    "sport", "dsport", "sbytes", "dbytes", "sttl", "dttl", "sloss", "dloss",
    "spkts", "dpkts", "swin", "dwin", "stcpb", "dtcpb", "smeansz", "dmeansz",
    "trans_depth", "res_bdy_len", "stime", "ltime", "is_sm_ips_ports",
    "ct_state_ttl", "ct_flw_http_mthd", "is_ftp_login", "ct_ftp_cmd",
    "ct_srv_src", "ct_srv_dst", "ct_dst_ltm", "ct_src_ltm", "ct_src_dport_ltm",
    "ct_dst_sport_ltm", "ct_dst_src_ltm", "label"
}
FLOAT_COLUMNS = {
    "dur", "sload", "dload", "sjit", "djit", "sintpkt", "dintpkt",
    "tcprtt", "synack", "ackdat"
}


def load_feature_metadata() -> list[dict[str, Any]]:
    """Reads NUSW-NB15_features.csv and returns normalized column specs."""
    if not FEATURES_CSV.exists():
        raise FileNotFoundError(f"Features file not found: {FEATURES_CSV}")

    cols = []
    with FEATURES_CSV.open(mode="r", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        _ = next(reader)  # ['No.', 'Name', 'Type ', 'Description']
        for row in reader:
            if not row or len(row) < 3:
                continue
            idx = int(row[0].strip())
            name = row[1].strip().lower()
            if name == "ct_src_ ltm":
                name = "ct_src_ltm"
            col_type = row[2].strip().lower()
            cols.append({"index": idx, "name": name, "type": col_type})

    if len(cols) != 49:
        raise ValueError(f"Expected 49 columns in features definition, got {len(cols)}")
    return cols


COLUMN_SPECS = load_feature_metadata()
COLUMN_NAMES = [c["name"] for c in COLUMN_SPECS]
FEATURE_NAMES = [c for c in COLUMN_NAMES if c not in CONTEXT_COLUMNS and c not in EVALUATION_COLUMNS]


def clean_attack_cat(val: str) -> str:
    """Normalizes attack category variants to canonical categories."""
    if not val:
        return "Normal"
    cleaned = val.strip()
    if not cleaned or cleaned.lower() == "normal":
        return "Normal"
    if cleaned.lower() == "backdoors":
        return "Backdoor"
    return cleaned


def clean_nominal(val: str) -> str:
    """Normalizes nominal service and state dashes to 'none'."""
    val = val.strip()
    return "none" if val == "-" or not val else val


def parse_field(name: str, raw_val: str) -> Any:
    """Parses raw text into appropriate typed Python primitives."""
    raw_val = raw_val.strip()
    if name == "attack_cat":
        return clean_attack_cat(raw_val)
    if name in ("service", "state"):
        return clean_nominal(raw_val)
    if name in INTEGER_COLUMNS:
        if not raw_val:
            return 0
        try:
            return int(raw_val)
        except ValueError:
            try:
                return int(float(raw_val))
            except ValueError:
                return 0
    if name in FLOAT_COLUMNS:
        if not raw_val:
            return 0.0
        try:
            return float(raw_val)
        except ValueError:
            return 0.0
    return raw_val


def parse_raw_row(raw_fields: list[str], file_idx: int, row_idx: int) -> dict[str, Any]:
    """Transforms a raw 49-field CSV row into the 6-key canonical schema."""
    if len(raw_fields) != 49:
        raise ValueError(f"Expected 49 columns, got {len(raw_fields)} at {file_idx}:{row_idx}")

    row_dict = {}
    for col_name, val in zip(COLUMN_NAMES, raw_fields):
        row_dict[col_name] = parse_field(col_name, val)

    stime = row_dict["stime"]
    ltime = row_dict["ltime"]
    event_dt = datetime.fromtimestamp(stime, tz=timezone.utc)
    event_time_iso = event_dt.isoformat()
    ingest_time_iso = datetime.now(timezone.utc).isoformat()

    context = {
        "srcip": row_dict["srcip"],
        "sport": row_dict["sport"],
        "dstip": row_dict["dstip"],
        "dsport": row_dict["dsport"],
        "stime": stime,
        "ltime": ltime,
    }

    features = {name: row_dict[name] for name in FEATURE_NAMES}

    label = row_dict["label"]
    attack_cat = row_dict["attack_cat"]

    if attack_cat != "Normal" and label == 0:
        label = 1

    return {
        "record_id": f"{file_idx}:{row_idx}",
        "event_time": event_time_iso,
        "ingest_time": ingest_time_iso,
        "context": context,
        "features": features,
        "evaluation": {
            "label": label,
            "attack_cat": attack_cat,
        },
    }


def iter_raw_records(limit: int | None = None, file_indices: list[int] | None = None) -> Generator[dict[str, Any], None, None]:
    """Iterates through raw records up to limit across specified files (1-indexed)."""
    if file_indices is None:
        file_indices = [1, 2, 3, 4]

    emitted = 0
    for file_idx in file_indices:
        csv_path = RAW_DIR / f"UNSW-NB15_{file_idx}.csv"
        if not csv_path.exists():
            continue

        with csv_path.open("r", encoding="utf-8-sig", errors="ignore") as f:
            reader = csv.reader(f)
            for row_idx, row in enumerate(reader):
                if not row or len(row) < 49:
                    continue
                record = parse_raw_row(row, file_idx, row_idx)
                yield record
                emitted += 1
                if limit and emitted >= limit:
                    return
