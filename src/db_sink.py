"""
PostgreSQL Database Sink for Real-Time Anomaly Detection
=========================================================
Implements Review Gates D1-D5, F6, X1-X4:
  - D1: Raw and processed layers in PostgreSQL.
  - D2: Strict role enforcement (raw_writer INSERT-only, processed_writer).
        Non-superuser application login (app_user).
  - D3: SQL-based 5-second tumbling window recomputation on touched windows.
  - D5: Model run metadata and training provenance tracking.
  - F6: Transactional retry on OperationalError with exponential backoff (~30s budget).
"""

import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json, execute_values

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# Load .env if present
def load_env_file(env_path: Path | None = None) -> None:
    if env_path is None:
        env_path = ROOT_DIR / ".env"
    if env_path.exists():
        with env_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ[k.strip()] = v.strip()

load_env_file()


def get_db_config(use_superuser: bool = False) -> dict[str, Any]:
    """Returns database connection parameters from environment or defaults."""
    load_env_file()
    db_name = os.environ.get("POSTGRES_DB", "network_monitoring")
    db_host = os.environ.get("POSTGRES_HOST", "localhost")
    db_port = int(os.environ.get("POSTGRES_PORT", 5433))

    if use_superuser:
        user = os.environ.get("POSTGRES_USER", "postgres")
        password = os.environ.get("POSTGRES_PASSWORD", "postgres_secure_pass")
    else:
        user = os.environ.get("APP_DB_USER", "app_user")
        password = os.environ.get("APP_DB_PASSWORD", "app_secure_pass")

    return {
        "dbname": db_name,
        "user": user,
        "password": password,
        "host": db_host,
        "port": db_port,
        "connect_timeout": 3,
    }


def get_connection(use_superuser: bool = False):
    """Establishes and returns a psycopg2 database connection."""
    cfg = get_db_config(use_superuser=use_superuser)
    return psycopg2.connect(**cfg)


def verify_non_superuser(conn) -> bool:
    """Verifies that the connected database user is NOT a superuser (Gate D2)."""
    with conn.cursor() as cur:
        cur.execute("SELECT usesuper FROM pg_user WHERE usename = CURRENT_USER;")
        res = cur.fetchone()
        if res is None:
            raise ValueError("Could not determine superuser status for current user.")
        is_super = res[0]
        if is_super:
            raise PermissionError(f"Security violation: Application user '{conn.info.user}' is a superuser.")
        return True


def insert_model_run(
    run_id: str,
    model_file: str,
    preprocessor_file: str,
    threshold: float,
    alert_k: int,
    watermark: str,
    provenance: dict[str, Any] | None = None,
    max_budget_s: float = 30.0,
) -> None:
    """Inserts a model run record into model_runs (Gate D5) with Gate F6 retry."""
    start_time = time.monotonic()
    backoff = 0.5
    while True:
        try:
            cfg = get_db_config(use_superuser=False)
            conn = psycopg2.connect(**cfg)
            try:
                with conn.cursor() as cur:
                    cur.execute("SET LOCAL ROLE processed_writer;")
                    cur.execute(
                        """
                        INSERT INTO model_runs (
                            run_id, model_file, preprocessor_file, threshold, alert_k, watermark, provenance, created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                        ON CONFLICT (run_id) DO UPDATE SET
                            model_file = EXCLUDED.model_file,
                            preprocessor_file = EXCLUDED.preprocessor_file,
                            threshold = EXCLUDED.threshold,
                            alert_k = EXCLUDED.alert_k,
                            watermark = EXCLUDED.watermark,
                            provenance = EXCLUDED.provenance;
                        """,
                        (
                            run_id,
                            model_file,
                            preprocessor_file,
                            threshold,
                            alert_k,
                            watermark,
                            Json(provenance or {}),
                        ),
                    )
                conn.commit()
                return
            finally:
                conn.close()
        except psycopg2.OperationalError as e:
            elapsed = time.monotonic() - start_time
            if elapsed + backoff > max_budget_s:
                raise RuntimeError(
                    f"PostgreSQL insert_model_run retry budget exhausted ({elapsed:.1f}s >= {max_budget_s}s): {e}"
                ) from e
            time.sleep(backoff)
            backoff = min(backoff * 1.5, 4.0)


def write_microbatch_transaction(
    conn,
    raw_tuples: list[tuple],
    score_tuples: list[tuple],
    touched_windows: list[datetime],
    alert_k: int,
) -> None:
    """
    Executes a single-transaction micro-batch write enforcing least privilege (Gate D2):
      1. SET LOCAL ROLE raw_writer -> INSERT INTO raw_flows
      2. SET LOCAL ROLE processed_writer -> INSERT INTO flow_scores
      3. Recompute window_metrics in SQL for touched windows
    """
    with conn.cursor() as cur:
        # Step 1: Raw layer insert under raw_writer (INSERT only)
        cur.execute("SET LOCAL ROLE raw_writer;")
        execute_values(
            cur,
            """
            INSERT INTO raw_flows (
                record_id, event_time, ingest_time, srcip, sport, dstip, dsport,
                stime, ltime, features, label, attack_cat -- GROUND-TRUTH PASSTHROUGH ONLY (NEVER MODEL INPUT - Gate X3)
            )
            VALUES %s
            ON CONFLICT (record_id) DO NOTHING;
            """,
            raw_tuples,
        )

        # Step 2: Processed layer insert under processed_writer
        cur.execute("SET LOCAL ROLE processed_writer;")
        execute_values(
            cur,
            """
            INSERT INTO flow_scores (
                record_id, score, flagged, run_id
            )
            VALUES %s
            ON CONFLICT (record_id) DO UPDATE SET
                score = EXCLUDED.score,
                flagged = EXCLUDED.flagged,
                run_id = EXCLUDED.run_id,
                scored_at = now();
            """,
            score_tuples,
        )

        # Step 3: SQL-based 5-second window recomputation (Gate D3)
        if touched_windows:
            min_w = min(touched_windows)
            max_w = max(touched_windows) + timedelta(seconds=5)
            recompute_query = """
            INSERT INTO window_metrics (
                window_start, window_end, flow_count, flagged_count,
                window_alert, attack_records, tcp_count, udp_count, other_count, updated_at
            )
            SELECT
                to_timestamp(floor(extract(epoch from r.event_time) / 5) * 5) AS window_start,
                to_timestamp(floor(extract(epoch from r.event_time) / 5) * 5) + interval '5 seconds' AS window_end,
                COUNT(*)::int AS flow_count,
                COUNT(*) FILTER (WHERE s.flagged = TRUE)::int AS flagged_count,
                (COUNT(*) FILTER (WHERE s.flagged = TRUE) >= %(alert_k)s) AS window_alert,
                COUNT(*) FILTER (WHERE r.label > 0)::int AS attack_records, -- GROUND-TRUTH PASSTHROUGH ONLY (NEVER MODEL INPUT - Gate X3)
                COUNT(*) FILTER (WHERE r.features->>'proto' = 'tcp')::int AS tcp_count,
                COUNT(*) FILTER (WHERE r.features->>'proto' = 'udp')::int AS udp_count,
                COUNT(*) FILTER (WHERE r.features->>'proto' NOT IN ('tcp', 'udp'))::int AS other_count,
                now() AS updated_at
            FROM raw_flows r
            JOIN flow_scores s ON r.record_id = s.record_id
            WHERE r.event_time >= %(min_w)s AND r.event_time < %(max_w)s
              AND to_timestamp(floor(extract(epoch from r.event_time) / 5) * 5) = ANY(%(touched_windows)s)
            GROUP BY 1
            ON CONFLICT (window_start) DO UPDATE SET
                window_end = EXCLUDED.window_end,
                flow_count = EXCLUDED.flow_count,
                flagged_count = EXCLUDED.flagged_count,
                window_alert = EXCLUDED.window_alert,
                attack_records = EXCLUDED.attack_records,
                tcp_count = EXCLUDED.tcp_count,
                udp_count = EXCLUDED.udp_count,
                other_count = EXCLUDED.other_count,
                updated_at = now();
            """
            cur.execute(
                recompute_query,
                {
                    "alert_k": alert_k,
                    "min_w": min_w,
                    "max_w": max_w,
                    "touched_windows": touched_windows,
                },
            )

    conn.commit()


def write_microbatch_with_retry(
    raw_tuples: list[tuple],
    score_tuples: list[tuple],
    touched_windows: list[datetime],
    alert_k: int,
    max_budget_s: float = 30.0,
) -> None:
    """
    Executes microbatch write with Gate F6 retry policy:
      - On OperationalError (e.g. database down, connection drop), retries the WHOLE
        transaction on a NEW connection with exponential backoff up to max_budget_s.
      - IntegrityError and other data errors fail immediately without retry.
      - If budget is exhausted, raises RuntimeError so Spark query stops and offsets are not committed.
    """
    start_time = time.monotonic()
    backoff = 0.5
    last_error = None

    while True:
        conn = None
        try:
            conn = get_connection(use_superuser=False)
            verify_non_superuser(conn)
            write_microbatch_transaction(
                conn=conn,
                raw_tuples=raw_tuples,
                score_tuples=score_tuples,
                touched_windows=touched_windows,
                alert_k=alert_k,
            )
            return
        except psycopg2.OperationalError as e:
            last_error = e
            elapsed = time.monotonic() - start_time
            if elapsed + backoff > max_budget_s:
                raise RuntimeError(
                    f"PostgreSQL retry budget exhausted ({elapsed:.1f}s >= {max_budget_s}s): {e}"
                ) from e
            time.sleep(backoff)
            backoff = min(backoff * 1.5, 4.0)
        except (psycopg2.IntegrityError, psycopg2.DataError, psycopg2.ProgrammingError):
            # Fatal data or schema errors must fail immediately
            raise
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass


def process_spark_batch(
    batch_df,
    batch_id: int,
    run_id: str,
    alert_k: int,
    feature_names: list[str],
) -> None:
    """
    Spark Structured Streaming foreachBatch callback.
    Extracts rows, builds payloads, and executes idempotent transactional writes.
    """
    rows = batch_df.collect()
    if not rows:
        return

    raw_tuples = []
    score_tuples = []
    touched_windows_set = set()

    for r in rows:
        row_dict = r.asDict(recursive=True)
        rec_id = str(row_dict["record_id"])
        srcip = str(row_dict.get("srcip", ""))
        sport = int(row_dict.get("sport") or 0)
        dstip = str(row_dict.get("dstip", ""))
        dsport = int(row_dict.get("dsport") or 0)
        stime = int(row_dict.get("stime") or 0)
        ltime = int(row_dict.get("ltime") or stime)

        if stime > 0:
            event_dt = datetime.fromtimestamp(stime, tz=timezone.utc)
        else:
            event_time = row_dict.get("event_time")
            if isinstance(event_time, str):
                event_dt = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
            elif isinstance(event_time, datetime):
                event_dt = event_time if event_time.tzinfo else event_time.replace(tzinfo=timezone.utc)
            else:
                event_dt = datetime.now(timezone.utc)
                stime = int(event_dt.timestamp())
                ltime = stime

        ingest_time = row_dict.get("ingest_time")
        if isinstance(ingest_time, str):
            ingest_dt = datetime.fromisoformat(ingest_time.replace("Z", "+00:00"))
        elif isinstance(ingest_time, datetime):
            ingest_dt = ingest_time if ingest_time.tzinfo else ingest_time.replace(tzinfo=timezone.utc)
        else:
            ingest_dt = datetime.now(timezone.utc)

        features = row_dict.get("features", {})
        if not isinstance(features, dict):
            features = {}

        label = int(row_dict.get("label", 0))  # GROUND-TRUTH PASSTHROUGH ONLY (NEVER MODEL INPUT - Gate X3)
        attack_cat = str(row_dict.get("attack_cat", "Normal"))  # GROUND-TRUTH PASSTHROUGH ONLY (NEVER MODEL INPUT - Gate X3)

        score = float(row_dict.get("anomaly_score", 0.0))
        flagged = bool(int(row_dict.get("flagged", 0)))

        raw_tuples.append((
            rec_id,
            event_dt,
            ingest_dt,
            srcip,
            sport,
            dstip,
            dsport,
            stime,
            ltime,
            Json(features),
            label,       # GROUND-TRUTH PASSTHROUGH ONLY (NEVER MODEL INPUT - Gate X3)
            attack_cat,  # GROUND-TRUTH PASSTHROUGH ONLY (NEVER MODEL INPUT - Gate X3)
        ))

        score_tuples.append((
            rec_id,
            score,
            flagged,
            run_id,
        ))

        w_epoch = (int(event_dt.timestamp()) // 5) * 5
        touched_windows_set.add(datetime.fromtimestamp(w_epoch, tz=timezone.utc))

    write_microbatch_with_retry(
        raw_tuples=raw_tuples,
        score_tuples=score_tuples,
        touched_windows=list(touched_windows_set),
        alert_k=alert_k,
    )
