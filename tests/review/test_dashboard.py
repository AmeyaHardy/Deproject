"""
Pytest Suite for Dashboard RBAC & Query Functionality
=====================================================
Tests Review Gates:
  - (a) RBAC: dashboard_reader cannot INSERT/UPDATE/DELETE, and cannot select raw_flows.features.
  - (b) Live DB: every query function runs successfully as dashboard_reader.
  - (c)-(e) Throwaway DB:
      - Query functions return expected columns.
      - Detection & health queries contain zero reference to ground-truth label / attack_cat.
      - Every query function handles empty database cleanly without crashing.
"""

import inspect
import os
import sys
from pathlib import Path

import psycopg2
import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dashboard import queries
from src.db_sink import get_connection, load_env_file

load_env_file()


def test_dashboard_reader_rbac_restrictions_on_live_db():
    """(a) Asserts dashboard_reader cannot write and cannot select raw_flows.features."""
    conn = queries.get_db_connection(use_dashboard_reader=True)
    cur = conn.cursor()

    # 1. Column-level SELECT on features must be denied
    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
        cur.execute("SELECT features FROM raw_flows LIMIT 1;")
    conn.rollback()

    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
        cur.execute("SELECT * FROM raw_flows LIMIT 1;")
    conn.rollback()

    # 2. INSERT must be denied
    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
        cur.execute("""
            INSERT INTO raw_flows (record_id, event_time, ingest_time, srcip, sport, dstip, dsport, stime, ltime, features, label, attack_cat)
            VALUES ('test_rbac', now(), now(), '1.1.1.1', 80, '2.2.2.2', 80, 0, 0, '{}', 0, 'Normal');
        """)
    conn.rollback()

    # 3. UPDATE must be denied
    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
        cur.execute("UPDATE flow_scores SET flagged = TRUE WHERE record_id = 'test_rbac';")
    conn.rollback()

    # 4. DELETE must be denied
    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
        cur.execute("DELETE FROM window_metrics WHERE flow_count = 0;")
    conn.rollback()

    cur.close()
    conn.close()


def test_all_dashboard_queries_succeed_as_dashboard_reader():
    """(b) Asserts every query function in dashboard/queries.py executes under dashboard_reader."""
    conn = queries.get_db_connection(use_dashboard_reader=True)

    # 1. Headline KPIs
    kpis = queries.get_headline_kpis(conn)
    assert isinstance(kpis, dict)
    assert "total_flows" in kpis
    assert "flagged_flows" in kpis
    assert "flagged_pct" in kpis
    assert "alerted_windows" in kpis

    # 2. Model run lineage
    model_info = queries.get_latest_model_run(conn)
    assert isinstance(model_info, dict)

    # 3. Pipeline health metrics
    health = queries.get_pipeline_health_metrics(conn, limit=100)
    assert isinstance(health, dict)
    assert "p50_latency_s" in health
    assert "p95_latency_s" in health
    assert "latency_series" in health

    # 4. Windowed traffic
    win_data = queries.get_windowed_traffic(conn, window_limit=20)
    assert isinstance(win_data, dict)
    assert "windows" in win_data
    assert "protocol_mix" in win_data

    # 5. Top source IPs
    top_ips = queries.get_top_source_ips(conn, limit=5)
    assert isinstance(top_ips, list)

    # 6. Latest alerted windows
    alert_wins = queries.get_latest_alerted_windows(conn, limit=10)
    assert isinstance(alert_wins, list)

    # 7. Ground truth evaluation
    gt = queries.get_ground_truth_evaluation(conn, window_limit=20)
    assert isinstance(gt, dict)
    assert "confusion_matrix" in gt
    assert "precision_pct" in gt
    assert "recall_pct" in gt

    conn.close()


def test_throwaway_database_schema_and_empty_behavior():
    """(c)-(e) Verifies throwaway database creation, schema setup, empty results, and no label leakage."""
    super_conn = get_connection(use_superuser=True)
    super_conn.autocommit = True
    super_cur = super_conn.cursor()

    db_test = "dashboard_test"
    super_cur.execute(f"DROP DATABASE IF EXISTS {db_test};")
    super_cur.execute(f"CREATE DATABASE {db_test};")
    super_conn.close()

    db_host = os.environ.get("POSTGRES_HOST", "localhost")
    db_port = int(os.environ.get("POSTGRES_PORT", 5433))
    super_user = os.environ.get("POSTGRES_USER", "postgres")
    super_pass = os.environ.get("POSTGRES_PASSWORD", "postgres_secure_pass")

    try:
        # Apply schema.sql, roles.sql, dashboard_role.sql
        init_conn = psycopg2.connect(
            dbname=db_test, user=super_user, password=super_pass, host=db_host, port=db_port
        )
        init_conn.autocommit = True
        with init_conn.cursor() as cur:
            for sql_file in ["db/schema.sql", "db/roles.sql", "db/dashboard_role.sql"]:
                sql_content = (ROOT_DIR / sql_file).read_text(encoding="utf-8")
                cur.execute(sql_content)
        init_conn.close()

        # Connect as dashboard_reader to the EMPTY throwaway database
        d_user = os.environ.get("DASHBOARD_DB_USER", "dashboard_reader")
        d_pass = os.environ.get("DASHBOARD_DB_PASSWORD", "dashboard_secure_pass")
        test_conn = psycopg2.connect(
            dbname=db_test, user=d_user, password=d_pass, host=db_host, port=db_port
        )

        # 1. Assert clean empty result handling
        kpis = queries.get_headline_kpis(test_conn)
        assert kpis["total_flows"] == 0
        assert kpis["flagged_flows"] == 0
        assert kpis["flagged_pct"] == 0.0

        model_run = queries.get_latest_model_run(test_conn)
        assert model_run == {}

        health = queries.get_pipeline_health_metrics(test_conn, limit=100)
        assert health["sample_count"] == 0
        assert health["p50_latency_s"] == 0.0
        assert health["latency_series"] == []

        win_data = queries.get_windowed_traffic(test_conn, window_limit=50)
        assert win_data["windows"] == []
        assert win_data["protocol_mix"] == {"TCP": 0, "UDP": 0, "Other": 0}

        top_ips = queries.get_top_source_ips(test_conn, limit=10)
        assert top_ips == []

        alert_wins = queries.get_latest_alerted_windows(test_conn, limit=20)
        assert alert_wins == []

        gt = queries.get_ground_truth_evaluation(test_conn, window_limit=50)
        assert gt["confusion_matrix"] == {"tp": 0, "fp": 0, "tn": 0, "fn": 0}
        assert gt["precision_pct"] == 0.0
        assert gt["recall_pct"] == 0.0

        test_conn.close()

        # 2. Assert zero label/attack_cat reference in detection & health queries
        detection_and_health_funcs = [
            queries.get_headline_kpis,
            queries.get_latest_model_run,
            queries.get_pipeline_health_metrics,
            queries.get_windowed_traffic,
            queries.get_top_source_ips,
            queries.get_latest_alerted_windows,
        ]
        for func in detection_and_health_funcs:
            code_text = inspect.getsource(func)
            assert "r.label" not in code_text, f"{func.__name__} references r.label!"
            assert "raw_flows.label" not in code_text, f"{func.__name__} references label!"
            assert "attack_cat" not in code_text, f"{func.__name__} references attack_cat!"

    finally:
        # Cleanly drop throwaway database
        drop_conn = get_connection(use_superuser=True)
        drop_conn.autocommit = True
        with drop_conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {db_test};")
        drop_conn.close()

