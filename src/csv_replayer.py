"""
CSV Traffic Replayer
====================
Reads records from the UNSW-NB15 training CSV and emits them as JSONL
at a configurable rate to simulate incoming network traffic.

Supports two output modes:
  - File mode (default): writes JSONL to a local file.
  - Kafka mode (--kafka): produces JSON messages to a Kafka topic.

Usage:
    # File output (same as before)
    python src/csv_replayer.py --limit 20 --rate 5

    # Kafka output
    python src/csv_replayer.py --kafka --limit 100 --rate 10
    python src/csv_replayer.py --kafka --topic my-topic --broker localhost:9092 --rate 50
"""

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ── Constants ────────────────────────────────────────────────────────────────

DEFAULT_INPUT = (
    Path("data/raw/OneDrive_1_10-12-2026")
    / "Training and Testing Sets"
    / "UNSW_NB15_training-set.csv"
)
DEFAULT_OUTPUT = Path("outputs/replay_preview.jsonl")
DEFAULT_KAFKA_TOPIC = "network-traffic"
DEFAULT_KAFKA_BROKER = "localhost:9092"

# Columns that must NOT appear inside the feature payload
EXCLUDE_FROM_FEATURES = {"id", "label", "attack_cat"}


# ── Helpers ──────────────────────────────────────────────────────────────────

def build_record(row: dict, fieldnames: list[str]) -> dict:
    """
    Transform a raw CSV row into the replay record structure.

    Returns a dict with:
      - record_id:  original dataset id
      - event_time: UTC ISO-8601 timestamp of emission
      - features:   traffic feature fields (no ground-truth leakage)
      - evaluation: ground-truth label and attack_cat (kept separate)
    """
    features = {}
    for col in fieldnames:
        if col in EXCLUDE_FROM_FEATURES:
            continue
        value = row[col]
        # Attempt numeric conversion so downstream consumers get proper types
        try:
            value = int(value)
        except (ValueError, TypeError):
            try:
                value = float(value)
            except (ValueError, TypeError):
                pass  # keep as string
        features[col] = value

    # Parse label safely
    raw_label = row.get("label", "")
    try:
        label_value = int(raw_label)
    except (ValueError, TypeError):
        label_value = raw_label

    record = {
        "record_id": row.get("id", ""),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "features": features,
        "evaluation": {
            "label": label_value,
            "attack_cat": row.get("attack_cat", "").strip(),
        },
    }
    return record


# ── Output Sinks ─────────────────────────────────────────────────────────────

class FileSink:
    """Writes JSON records to a local JSONL file."""

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = None

    def open(self):
        self._file = self.path.open("w", encoding="utf-8")
        print(f"Sink   : File -> {self.path}")

    def send(self, record: dict):
        line = json.dumps(record, separators=(",", ":"))
        self._file.write(line + "\n")

    def close(self):
        if self._file:
            self._file.close()
        print(f"Output written to: {self.path}")


class KafkaSink:
    """Produces JSON records to a Kafka topic."""

    def __init__(self, broker: str, topic: str):
        self.broker = broker
        self.topic = topic
        self._producer = None

    def open(self):
        try:
            from kafka import KafkaProducer
        except ImportError:
            print(
                "ERROR: kafka-python-ng is not installed.\n"
                "       Run: pip install kafka-python-ng",
                file=sys.stderr,
            )
            sys.exit(1)

        print(f"Sink   : Kafka -> {self.broker} / topic={self.topic}")
        print("Connecting to Kafka broker...", end=" ", flush=True)

        try:
            self._producer = KafkaProducer(
                bootstrap_servers=self.broker,
                value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                max_block_ms=10000,  # fail fast if broker is unreachable
            )
            print("connected.")
        except Exception as e:
            print(f"\nERROR: Could not connect to Kafka at {self.broker}: {e}", file=sys.stderr)
            sys.exit(1)

    def send(self, record: dict):
        # Use record_id as the Kafka message key for partitioning
        key = str(record.get("record_id", ""))
        self._producer.send(self.topic, key=key, value=record)

    def close(self):
        if self._producer:
            self._producer.flush()
            self._producer.close()
            print(f"Kafka producer flushed and closed. Topic: {self.topic}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Replay UNSW-NB15 CSV records as a simulated traffic stream."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Path to the input CSV file (default: training set).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Path to the output JSONL file (default: outputs/replay_preview.jsonl).",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=5.0,
        help="Records per second to emit (default: 5).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of records to emit (default: all).",
    )

    # ── Kafka options ────────────────────────────────────────────────────
    parser.add_argument(
        "--kafka",
        action="store_true",
        help="Send records to Kafka instead of a file.",
    )
    parser.add_argument(
        "--broker",
        type=str,
        default=DEFAULT_KAFKA_BROKER,
        help=f"Kafka bootstrap server (default: {DEFAULT_KAFKA_BROKER}).",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=DEFAULT_KAFKA_TOPIC,
        help=f"Kafka topic name (default: {DEFAULT_KAFKA_TOPIC}).",
    )

    args = parser.parse_args()

    # ── Validate input ───────────────────────────────────────────────────
    if not args.input.exists():
        print(f"ERROR: Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    if args.rate <= 0:
        print("ERROR: --rate must be positive.", file=sys.stderr)
        sys.exit(1)

    delay = 1.0 / args.rate  # seconds between records

    # ── Choose sink ──────────────────────────────────────────────────────
    if args.kafka:
        sink = KafkaSink(broker=args.broker, topic=args.topic)
    else:
        sink = FileSink(path=args.output)

    # ── Replay loop ──────────────────────────────────────────────────────
    print(f"Input  : {args.input}")
    print(f"Rate   : {args.rate} records/sec (delay {delay:.4f}s)")
    if args.limit is not None:
        print(f"Limit  : {args.limit} records")

    sink.open()
    print()

    emitted = 0
    start_wall = time.monotonic()

    try:
        with args.input.open("r", encoding="utf-8-sig", newline="") as fin:
            reader = csv.DictReader(fin)
            fieldnames = reader.fieldnames

            if fieldnames is None:
                print("ERROR: Could not read CSV header.", file=sys.stderr)
                sys.exit(1)

            # Sanity check: confirm expected columns are present
            missing = EXCLUDE_FROM_FEATURES - set(fieldnames)
            if missing:
                print(
                    f"WARNING: Expected columns missing from CSV: {missing}",
                    file=sys.stderr,
                )

            for row in reader:
                record = build_record(row, fieldnames)
                sink.send(record)

                emitted += 1
                if emitted <= 5 or emitted % 10 == 0:
                    print(
                        f"  [{emitted:>6}] id={record['record_id']:>6}  "
                        f"label={record['evaluation']['label']}  "
                        f"attack_cat={record['evaluation']['attack_cat']}"
                    )

                if args.limit is not None and emitted >= args.limit:
                    break

                time.sleep(delay)

    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")

    finally:
        sink.close()

    elapsed = time.monotonic() - start_wall
    effective_rate = emitted / elapsed if elapsed > 0 else 0

    print()
    print(f"Done. Emitted {emitted} records in {elapsed:.2f}s "
          f"(effective rate: {effective_rate:.1f} rec/s).")


if __name__ == "__main__":
    main()
