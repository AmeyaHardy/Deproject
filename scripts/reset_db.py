"""
Interactive Database Reset Script
=================================
Safely truncates raw_flows, flow_scores, and window_metrics tables in PostgreSQL.
Prints current row counts and requires explicit confirmation before truncating.
"""

import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.db_sink import get_connection, load_env_file

load_env_file()


def main():
    conn = get_connection(use_superuser=True)
    counts = {}
    try:
        with conn.cursor() as cur:
            for tbl in ["raw_flows", "flow_scores", "window_metrics"]:
                cur.execute(f"SELECT count(*) FROM {tbl};")
                counts[tbl] = cur.fetchone()[0]
    finally:
        conn.close()

    print("\n" + "=" * 60)
    print("  POSTGRESQL DATABASE RESET CONFIRMATION")
    print("=" * 60)
    print("Current table row counts to be cleared:")
    for tbl, cnt in counts.items():
        print(f"  • {tbl:<16}: {cnt:,} rows")
    print("=" * 60)

    total_rows = sum(counts.values())
    if total_rows == 0:
        print("Tables are already empty. No action taken.\n")
        return

    try:
        ans = input("\nType 'CONFIRM' to truncate these tables (or anything else to cancel): ")
    except (EOFError, KeyboardInterrupt):
        print("\nCancelled.")
        return

    if ans.strip().upper() == "CONFIRM":
        conn = get_connection(use_superuser=True)
        try:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE flow_scores, raw_flows, window_metrics CASCADE;")
            conn.commit()
            print("Successfully truncated flow_scores, raw_flows, and window_metrics.\n")
        finally:
            conn.close()
    else:
        print("Reset cancelled. No tables were modified.\n")


if __name__ == "__main__":
    main()

