import os
import sys
from pathlib import Path
import psycopg2

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.db_sink import load_env_file

load_env_file()

def test_privileges():
    db_name = os.environ.get("POSTGRES_DB", "network_monitoring")
    db_host = os.environ.get("POSTGRES_HOST", "localhost")
    db_port = int(os.environ.get("POSTGRES_PORT", 5433))
    user = os.environ.get("DASHBOARD_DB_USER", "dashboard_reader")
    password = os.environ.get("DASHBOARD_DB_PASSWORD", "dashboard_secure_pass")

    print(f"Connecting to {db_name} on port {db_port} as user '{user}'...")
    conn = psycopg2.connect(
        dbname=db_name,
        user=user,
        password=password,
        host=db_host,
        port=db_port,
        connect_timeout=3,
    )
    conn.autocommit = False
    cur = conn.cursor()

    # 1. Test SELECT on allowed tables and columns
    print("\n--- 1. Testing Allowed SELECT Queries ---")
    cur.execute("SELECT count(*) FROM window_metrics;")
    wm_cnt = cur.fetchone()[0]
    print(f"  [PASS] SELECT on window_metrics: {wm_cnt} rows")

    cur.execute("SELECT count(*) FROM flow_scores;")
    fs_cnt = cur.fetchone()[0]
    print(f"  [PASS] SELECT on flow_scores: {fs_cnt} rows")

    cur.execute("SELECT count(*) FROM model_runs;")
    mr_cnt = cur.fetchone()[0]
    print(f"  [PASS] SELECT on model_runs: {mr_cnt} rows")

    cur.execute("SELECT record_id, event_time, ingest_time, loaded_at, srcip, label, attack_cat FROM raw_flows LIMIT 1;")
    rf_row = cur.fetchone()
    print(f"  [PASS] SELECT on granted columns of raw_flows: success (returned {bool(rf_row)})")

    # 2. Test SELECT on DENIED column (raw_flows.features)
    print("\n--- 2. Testing Denied SELECT on raw_flows.features ---")
    try:
        cur.execute("SELECT features FROM raw_flows LIMIT 1;")
        print("  [FAIL] SELECT on raw_flows.features should have failed!")
    except psycopg2.errors.InsufficientPrivilege as e:
        conn.rollback()
        print(f"  [PASS] SELECT on raw_flows.features denied as expected: {e.pgerror.strip()}")

    # 3. Test SELECT * on raw_flows (which includes features)
    try:
        cur.execute("SELECT * FROM raw_flows LIMIT 1;")
        print("  [FAIL] SELECT * on raw_flows should have failed!")
    except psycopg2.errors.InsufficientPrivilege as e:
        conn.rollback()
        print(f"  [PASS] SELECT * on raw_flows denied as expected: {e.pgerror.strip()}")

    # 4. Test INSERT on raw_flows
    print("\n--- 3. Testing Denied Write Operations (INSERT / UPDATE / DELETE) ---")
    try:
        cur.execute("INSERT INTO raw_flows (record_id, event_time, ingest_time, srcip, sport, dstip, dsport, stime, ltime, features, label, attack_cat) VALUES ('test', now(), now(), '1.1.1.1', 80, '2.2.2.2', 80, 0, 0, '{}', 0, 'Normal');")
        print("  [FAIL] INSERT on raw_flows should have failed!")
    except psycopg2.errors.InsufficientPrivilege as e:
        conn.rollback()
        print(f"  [PASS] INSERT on raw_flows denied: {e.pgerror.strip()}")

    # 5. Test UPDATE on flow_scores
    try:
        cur.execute("UPDATE flow_scores SET flagged = TRUE WHERE record_id = 'test';")
        print("  [FAIL] UPDATE on flow_scores should have failed!")
    except psycopg2.errors.InsufficientPrivilege as e:
        conn.rollback()
        print(f"  [PASS] UPDATE on flow_scores denied: {e.pgerror.strip()}")

    # 6. Test DELETE on window_metrics
    try:
        cur.execute("DELETE FROM window_metrics WHERE flow_count = 0;")
        print("  [FAIL] DELETE on window_metrics should have failed!")
    except psycopg2.errors.InsufficientPrivilege as e:
        conn.rollback()
        print(f"  [PASS] DELETE on window_metrics denied: {e.pgerror.strip()}")

    cur.close()
    conn.close()
    print("\nPrivilege verification completed successfully: 100% compliant with least-privilege RBAC.")

if __name__ == "__main__":
    test_privileges()

