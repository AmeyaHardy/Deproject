"""
Dashboard Query Polling Simulator & Pipeline Impact Analyzer
============================================================
Simulates an active browser session by calling all uncached query functions
from dashboard/queries.py every 5 seconds as dashboard_reader.
Measures execution time per query and total SQL refresh latency in ms.
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dashboard import queries


class DashboardPoller:
    """Polls all uncached dashboard queries on a fixed schedule as dashboard_reader."""

    def __init__(self, interval_s: float = 5.0):
        self.interval_s = interval_s
        self.records: List[Dict[str, Any]] = []
        self._running = False

    def poll_once(self) -> Dict[str, Any]:
        conn = queries.get_db_connection(use_dashboard_reader=True)
        t_cycle_start = time.monotonic()
        query_timings_ms = {}

        try:
            # 1. Headline KPIs
            t0 = time.monotonic()
            kpis = queries.get_headline_kpis(conn)
            query_timings_ms["kpis_ms"] = round((time.monotonic() - t0) * 1000.0, 2)

            # 2. Latest model run
            t0 = time.monotonic()
            _ = queries.get_latest_model_run(conn)
            query_timings_ms["model_run_ms"] = round((time.monotonic() - t0) * 1000.0, 2)

            # 3. Pipeline health (recent 2,000 flows)
            t0 = time.monotonic()
            _ = queries.get_pipeline_health_metrics(conn, limit=2000)
            query_timings_ms["health_ms"] = round((time.monotonic() - t0) * 1000.0, 2)

            # 4. Windowed traffic (last 50 windows)
            t0 = time.monotonic()
            _ = queries.get_windowed_traffic(conn, window_limit=50)
            query_timings_ms["window_traffic_ms"] = round((time.monotonic() - t0) * 1000.0, 2)

            # 5. Top 10 source IPs
            t0 = time.monotonic()
            _ = queries.get_top_source_ips(conn, limit=10)
            query_timings_ms["top_ips_ms"] = round((time.monotonic() - t0) * 1000.0, 2)

            # 6. Latest alerted windows
            t0 = time.monotonic()
            _ = queries.get_latest_alerted_windows(conn, limit=20)
            query_timings_ms["alerted_windows_ms"] = round((time.monotonic() - t0) * 1000.0, 2)

            # 7. Ground truth evaluation (last 50 windows)
            t0 = time.monotonic()
            _ = queries.get_ground_truth_evaluation(conn, window_limit=50)
            query_timings_ms["ground_truth_ms"] = round((time.monotonic() - t0) * 1000.0, 2)

        finally:
            conn.close()

        total_sql_ms = round((time.monotonic() - t_cycle_start) * 1000.0, 2)
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_sql_ms": total_sql_ms,
            "total_flows": kpis.get("total_flows", 0),
            "flagged_flows": kpis.get("flagged_flows", 0),
            "alerted_windows": kpis.get("alerted_windows", 0),
            **query_timings_ms,
        }
        self.records.append(record)
        return record


def main():
    parser = argparse.ArgumentParser(description="Simulate active dashboard polling.")
    parser.add_argument("--interval", type=float, default=5.0, help="Poll interval in seconds")
    parser.add_argument("--duration", type=float, default=60.0, help="Total polling duration in seconds")
    parser.add_argument("--output-csv", type=str, default="reports/verification/dashboard_polling.csv")
    args = parser.parse_args()

    poller = DashboardPoller(interval_s=args.interval)
    print(f"Starting dashboard polling simulation (every {args.interval}s for {args.duration}s)...")

    start_t = time.monotonic()
    cycle = 0
    while time.monotonic() - start_t < args.duration:
        cycle += 1
        res = poller.poll_once()
        print(f"  [Cycle {cycle:>2}] Total SQL: {res['total_sql_ms']:>6.2f} ms | Flows: {res['total_flows']:>5} | Flagged: {res['flagged_flows']:>4} | Health: {res['health_ms']:>5.2f} ms")
        time.sleep(args.interval)

    out_path = Path(args.output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if poller.records:
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(poller.records[0].keys()))
            writer.writeheader()
            writer.writerows(poller.records)
        print(f"Polling metrics saved to: {out_path}")


if __name__ == "__main__":
    main()

