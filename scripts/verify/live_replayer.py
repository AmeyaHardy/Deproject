"""
Live Traffic Replayer for Out-of-Sample File 3 (Session 1)
=========================================================
Streams raw UNSW-NB15 records from File 3 into Kafka at a specified rate.
Stamps message ingest_time with current UTC ISO-8601 wall clock timestamp at send.
Preserves exact pacing behavior of src/csv_replayer.py (sleep per record).
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from kafka import KafkaProducer
from src.raw_loader import iter_raw_records


def main():
    parser = argparse.ArgumentParser(description="Live stream records from File 3 to Kafka.")
    parser.add_argument("--topic", type=str, required=True, help="Kafka topic name")
    parser.add_argument("--broker", type=str, default="localhost:9092", help="Kafka broker")
    parser.add_argument("--limit", type=int, default=5000, help="Record count limit")
    parser.add_argument("--rate", type=float, default=25.0, help="Target records per second")
    parser.add_argument("--file-index", type=int, default=3, help="File index (default: 3 for Session 1)")
    parser.add_argument("--print-interval", type=int, default=500, help="Progress log frequency")
    args = parser.parse_args()

    print(f"Replayer starting:")
    print(f"  Target topic:  {args.topic}")
    print(f"  Broker:        {args.broker}")
    print(f"  Record limit:  {args.limit:,}")
    print(f"  Target rate:   {args.rate} rec/s")
    print(f"  File index:    {args.file_index} (UNSW-NB15_{args.file_index}.csv)")

    producer = KafkaProducer(
        bootstrap_servers=args.broker,
        key_serializer=lambda k: k.encode("utf-8") if k else b"",
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        acks="all",
        retries=3,
        max_block_ms=10000,
    )

    delay = 1.0 / args.rate if args.rate > 0 else 0.0
    emitted = 0
    t_start = time.monotonic()

    print(f"Streaming records...")
    for record in iter_raw_records(limit=args.limit, file_indices=[args.file_index]):
        # Set ingest_time at send (UTC wall clock)
        record["ingest_time"] = datetime.now(timezone.utc).isoformat()

        key = record.get("context", {}).get("srcip") or str(record.get("record_id", ""))
        ts_ms = None
        if "context" in record and "stime" in record["context"]:
            ts_ms = int(record["context"]["stime"] * 1000)

        producer.send(args.topic, key=key, value=record, timestamp_ms=ts_ms)
        emitted += 1

        if emitted == 1 or emitted % args.print_interval == 0 or emitted == args.limit:
            elapsed = time.monotonic() - t_start
            cur_rate = emitted / elapsed if elapsed > 0 else 0
            print(f"  [REPLAYER] Emitted {emitted:>6}/{args.limit} | Elapsed: {elapsed:>6.1f}s | Current rate: {cur_rate:>5.1f} rec/s")

        if delay > 0:
            time.sleep(delay)

    producer.flush()
    producer.close()
    elapsed_total = time.monotonic() - t_start
    achieved_rate = emitted / elapsed_total if elapsed_total > 0 else 0
    print(f"Replayer finished: Emitted {emitted} records in {elapsed_total:.2f}s ({achieved_rate:.2f} rec/s).")


if __name__ == "__main__":
    main()

