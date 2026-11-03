"""
Pure Query Module for PostgreSQL Serving & Observability Dashboard
===================================================================
Contains standalone database access functions running under the least-privilege
'dashboard_reader' role.

Rules & Security Constraints:
  - NO Streamlit imports here (pure Python so tests and polling scripts can import it).
  - Uses strictly granted columns:
      raw_flows: record_id, event_time, ingest_time, loaded_at, srcip, label, attack_cat
      flow_scores: all columns
      window_metrics: all columns
      model_runs: all columns
  - Does NOT touch raw_flows.features (column-level DENIED).
  - Ground-truth labels (label, attack_cat) appear ONLY in get_ground_truth_evaluation().
  - Gracefully handles empty tables and connection drops.
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.db_sink import load_env_file

load_env_file()


def get_db_connection(use_dashboard_reader: bool = True):
    """Establishes psycopg2 connection as dashboard_reader (or configured role)."""
    load_env_file()
    db_name = os.environ.get("POSTGRES_DB", "network_monitoring")
    db_host = os.environ.get("POSTGRES_HOST", "localhost")
    db_port = int(os.environ.get("POSTGRES_PORT", 5433))

    if use_dashboard_reader:
        user = os.environ.get("DASHBOARD_DB_USER", "dashboard_reader")
        password = os.environ.get("DASHBOARD_DB_PASSWORD", "dashboard_secure_pass")
    else:
        user = os.environ.get("APP_DB_USER", "app_user")
        password = os.environ.get("APP_DB_PASSWORD", "app_secure_pass")

    return psycopg2.connect(
        dbname=db_name,
        user=user,
        password=password,
        host=db_host,
        port=db_port,
        connect_timeout=3,
    )


# -----------------------------------------------------------------------------
# Section A: Headline KPIs & Model Provenance
# -----------------------------------------------------------------------------

def get_headline_kpis(conn) -> Dict[str, Any]:
    """Queries flow counts, anomaly share, alerted windows, and latest update."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Flow scores & flagged count
        cur.execute("""
            SELECT
                COUNT(*)::int AS total_flows,
                COUNT(*) FILTER (WHERE flagged = TRUE)::int AS flagged_flows
            FROM flow_scores;
        """)
        fs_row = cur.fetchone() or {"total_flows": 0, "flagged_flows": 0}

        # Window metrics
        cur.execute("""
            SELECT
                COUNT(*)::int AS total_windows,
                COUNT(*) FILTER (WHERE window_alert = TRUE)::int AS alerted_windows,
                MAX(updated_at) AS last_window_update
            FROM window_metrics;
        """)
        wm_row = cur.fetchone() or {"total_windows": 0, "alerted_windows": 0, "last_window_update": None}

        # Raw flows latest load
        cur.execute("SELECT MAX(loaded_at) AS last_flow_loaded FROM raw_flows;")
        rf_row = cur.fetchone() or {"last_flow_loaded": None}

    total_flows = fs_row["total_flows"] or 0
    flagged_flows = fs_row["flagged_flows"] or 0
    flagged_pct = (flagged_flows * 100.0 / total_flows) if total_flows > 0 else 0.0

    last_ts = wm_row["last_window_update"] or rf_row["last_flow_loaded"]

    return {
        "total_flows": total_flows,
        "flagged_flows": flagged_flows,
        "flagged_pct": round(flagged_pct, 2),
        "total_windows": wm_row["total_windows"] or 0,
        "alerted_windows": wm_row["alerted_windows"] or 0,
        "last_db_update": last_ts,
    }


def get_latest_model_run(conn) -> Dict[str, Any]:
    """Queries latest model provenance and hyperparameters from model_runs."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT
                run_id,
                model_file,
                preprocessor_file,
                threshold,
                alert_k,
                watermark,
                provenance,
                created_at
            FROM model_runs
            ORDER BY created_at DESC
            LIMIT 1;
        """)
        row = cur.fetchone()
        return dict(row) if row else {}


# -----------------------------------------------------------------------------
# Section B: Streaming Pipeline Health
# -----------------------------------------------------------------------------

def get_pipeline_health_metrics(conn, limit: int = 2000) -> Dict[str, Any]:
    """
    Computes end-to-end data latency (loaded_at - ingest_time) over the MOST RECENT
    N flows by loaded_at, along with throughput over time.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Note: ONLY granted columns loaded_at, ingest_time, record_id
        cur.execute("""
            WITH recent_flows AS (
                SELECT
                    record_id,
                    loaded_at,
                    ingest_time,
                    EXTRACT(EPOCH FROM (loaded_at - ingest_time)) AS latency_s
                FROM raw_flows
                ORDER BY loaded_at DESC
                LIMIT %(limit)s
            )
            SELECT
                record_id,
                loaded_at,
                ingest_time,
                latency_s
            FROM recent_flows
            ORDER BY loaded_at ASC;
        """, {"limit": limit})
        rows = cur.fetchall()

    if not rows:
        return {
            "p50_latency_s": 0.0,
            "p95_latency_s": 0.0,
            "max_latency_s": 0.0,
            "sample_count": 0,
            "latency_series": [],
            "throughput_series": [],
        }

    latencies = [float(r["latency_s"]) for r in rows if r["latency_s"] is not None]
    p50_lat = float(np.percentile(latencies, 50)) if latencies else 0.0
    p95_lat = float(np.percentile(latencies, 95)) if latencies else 0.0
    max_lat = float(np.max(latencies)) if latencies else 0.0

    # Build latency time series
    latency_series = [
        {
            "loaded_at": r["loaded_at"],
            "latency_s": float(r["latency_s"]) if r["latency_s"] is not None else 0.0,
        }
        for r in rows
    ]

    # Compute effective storage throughput (flows per 5s bucket of loaded_at)
    df_lat = pd.DataFrame(rows)
    throughput_series = []
    if not df_lat.empty and "loaded_at" in df_lat.columns:
        df_lat["loaded_at"] = pd.to_datetime(df_lat["loaded_at"], utc=True)
        # Resample by 5-second buckets
        grouped = (
            df_lat.set_index("loaded_at")
            .resample("5s")
            .size()
            .reset_index(name="count")
        )
        grouped["rate_rec_s"] = grouped["count"] / 5.0
        throughput_series = grouped.to_dict(orient="records")

    return {
        "p50_latency_s": round(p50_lat, 3),
        "p95_latency_s": round(p95_lat, 3),
        "max_latency_s": round(max_lat, 3),
        "sample_count": len(rows),
        "latency_series": latency_series,
        "throughput_series": throughput_series,
    }


# -----------------------------------------------------------------------------
# Section C: Windowed Traffic & Operational Alerts (No Ground Truth)
# -----------------------------------------------------------------------------

def get_windowed_traffic(conn, window_limit: Optional[int] = 50) -> Dict[str, Any]:
    """
    Retrieves flow and flagged counts per 5-second window from window_metrics,
    along with protocol mix (tcp_count, udp_count, other_count).
    Does NOT touch raw_flows.features.
    """
    limit_clause = f"LIMIT {int(window_limit)}" if window_limit and window_limit > 0 else ""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Query window metrics ordered chronologically
        cur.execute(f"""
            WITH ranked_windows AS (
                SELECT
                    window_start,
                    window_end,
                    flow_count,
                    flagged_count,
                    window_alert,
                    tcp_count,
                    udp_count,
                    other_count,
                    updated_at
                FROM window_metrics
                ORDER BY window_start DESC
                {limit_clause}
            )
            SELECT * FROM ranked_windows
            ORDER BY window_start ASC;
        """)
        rows = cur.fetchall()

    windows = [dict(r) for r in rows]

    # Aggregate protocol totals across selected windows
    tcp_total = sum(w.get("tcp_count", 0) for w in windows)
    udp_total = sum(w.get("udp_count", 0) for w in windows)
    other_total = sum(w.get("other_count", 0) for w in windows)

    return {
        "windows": windows,
        "protocol_mix": {
            "TCP": tcp_total,
            "UDP": udp_total,
            "Other": other_total,
        },
    }


def get_top_source_ips(conn, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Finds top 10 source IPs by flagged anomaly count.
    Uses strictly granted columns r.srcip and s.flagged.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT
                r.srcip,
                COUNT(*)::int AS total_flows,
                COUNT(*) FILTER (WHERE s.flagged = TRUE)::int AS flagged_count,
                ROUND(
                    COUNT(*) FILTER (WHERE s.flagged = TRUE) * 100.0 / NULLIF(COUNT(*), 0),
                    2
                ) AS flagged_pct
            FROM raw_flows r
            JOIN flow_scores s ON r.record_id = s.record_id
            GROUP BY r.srcip
            HAVING COUNT(*) FILTER (WHERE s.flagged = TRUE) > 0
            ORDER BY flagged_count DESC
            LIMIT %(limit)s;
        """, {"limit": limit})
        return [dict(r) for r in cur.fetchall()]


def get_latest_alerted_windows(conn, limit: int = 20) -> List[Dict[str, Any]]:
    """Retrieves the latest alerted windows (window_alert = TRUE)."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT
                window_start,
                window_end,
                flow_count,
                flagged_count,
                tcp_count,
                udp_count,
                other_count,
                updated_at
            FROM window_metrics
            WHERE window_alert = TRUE
            ORDER BY window_start DESC
            LIMIT %(limit)s;
        """, {"limit": limit})
        return [dict(r) for r in cur.fetchall()]


# -----------------------------------------------------------------------------
# Section D: GROUND TRUTH Evaluation (Post-Hoc Verification Only)
# -----------------------------------------------------------------------------

def get_ground_truth_evaluation(conn, window_limit: Optional[int] = 50) -> Dict[str, Any]:
    """
    Computes confusion matrix and operational accuracy metrics using ground-truth labels.
    Explicitly labeled as GROUND TRUTH post-hoc verification.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE s.flagged = TRUE AND r.label > 0)::int AS tp,
                COUNT(*) FILTER (WHERE s.flagged = TRUE AND r.label = 0)::int AS fp,
                COUNT(*) FILTER (WHERE s.flagged = FALSE AND r.label = 0)::int AS tn,
                COUNT(*) FILTER (WHERE s.flagged = FALSE AND r.label > 0)::int AS fn
            FROM raw_flows r
            JOIN flow_scores s ON r.record_id = s.record_id;
        """)
        cm = cur.fetchone() or {"tp": 0, "fp": 0, "tn": 0, "fn": 0}

        limit_clause = f"LIMIT {int(window_limit)}" if window_limit and window_limit > 0 else ""
        cur.execute(f"""
            WITH recent_wm AS (
                SELECT
                    window_start,
                    window_end,
                    flow_count,
                    flagged_count,
                    attack_records,
                    window_alert
                FROM window_metrics
                ORDER BY window_start DESC
                {limit_clause}
            )
            SELECT * FROM recent_wm
            ORDER BY window_start ASC;
        """)
        wm_rows = cur.fetchall()

    tp = cm["tp"] or 0
    fp = cm["fp"] or 0
    tn = cm["tn"] or 0
    fn = cm["fn"] or 0

    precision = (tp / (tp + fp) * 100.0) if (tp + fp) > 0 else 0.0
    recall = (tp / (tp + fn) * 100.0) if (tp + fn) > 0 else 0.0
    fpr = (fp / (fp + tn) * 100.0) if (fp + tn) > 0 else 0.0

    return {
        "confusion_matrix": {
            "tp": tp,
            "fp": fp,
            "tn": tn,
            "fn": fn,
        },
        "precision_pct": round(precision, 2),
        "recall_pct": round(recall, 2),
        "fpr_pct": round(fpr, 4),
        "window_ground_truth": [dict(r) for r in wm_rows],
    }

