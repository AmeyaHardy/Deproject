"""
Kafka Consumer Test
===================
Connects to Kafka and reads messages from the network-traffic topic.
Prints each received message to the console and optionally saves to a file.

Usage:
    python src/kafka_consumer_test.py
    python src/kafka_consumer_test.py --topic network-traffic --limit 10
"""

import argparse
import json
import sys
from datetime import datetime, timezone

DEFAULT_TOPIC = "network-traffic"
DEFAULT_BROKER = "localhost:9092"


def main():
    parser = argparse.ArgumentParser(
        description="Test consumer: read messages from a Kafka topic."
    )
    parser.add_argument(
        "--broker",
        type=str,
        default=DEFAULT_BROKER,
        help=f"Kafka bootstrap server (default: {DEFAULT_BROKER}).",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=DEFAULT_TOPIC,
        help=f"Kafka topic to consume from (default: {DEFAULT_TOPIC}).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after consuming this many messages (default: run until Ctrl+C).",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Read from the beginning of the topic (earliest offset).",
    )
    args = parser.parse_args()

    try:
        from kafka import KafkaConsumer
    except ImportError:
        print(
            "ERROR: kafka-python-ng is not installed.\n"
            "       Run: pip install kafka-python-ng",
            file=sys.stderr,
        )
        sys.exit(1)

    auto_offset = "earliest" if args.from_beginning else "latest"

    print(f"Broker : {args.broker}")
    print(f"Topic  : {args.topic}")
    print(f"Offset : {auto_offset}")
    if args.limit:
        print(f"Limit  : {args.limit} messages")
    print()
    print("Connecting to Kafka...", end=" ", flush=True)

    try:
        consumer = KafkaConsumer(
            args.topic,
            bootstrap_servers=args.broker,
            auto_offset_reset=auto_offset,
            enable_auto_commit=True,
            group_id="test-consumer-group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=30000,  # stop if no messages for 30s
        )
        print("connected.")
        print("Waiting for messages... (Ctrl+C to stop)\n")
    except Exception as e:
        print(f"\nERROR: Could not connect to Kafka at {args.broker}: {e}", file=sys.stderr)
        sys.exit(1)

    consumed = 0
    attack_counts = {"Normal": 0}

    try:
        for message in consumer:
            consumed += 1
            record = message.value
            rec_id = record.get("record_id", "?")
            label = record.get("evaluation", {}).get("label", "?")
            attack = record.get("evaluation", {}).get("attack_cat", "?")

            # Track attack category distribution
            attack_counts[attack] = attack_counts.get(attack, 0) + 1

            if consumed <= 5 or consumed % 10 == 0:
                print(
                    f"  [{consumed:>6}] partition={message.partition} "
                    f"offset={message.offset} "
                    f"id={rec_id} label={label} attack_cat={attack}"
                )

            if args.limit and consumed >= args.limit:
                break

    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")

    finally:
        consumer.close()

    print(f"\nConsumed {consumed} messages.")
    print(f"Attack category distribution: {json.dumps(attack_counts, indent=2)}")


if __name__ == "__main__":
    main()
