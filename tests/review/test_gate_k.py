"""
Review Tests: Gate K (Kafka Ingestion)
=====================================
Tests based on Section 4 Gate K of pipeline_review_prompt.md:
  K1: Kafka container health and topic inspection.
  K2: Produce and consume test using canonical 6-key records and srcip keys.
"""

import json
import time
import pytest
from src.raw_loader import iter_raw_records
from src.csv_replayer import KafkaSink


def test_k1_kafka_health():
    """Verify Kafka broker is reachable on localhost:9092."""
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        bootstrap_servers="127.0.0.1:9092",
        consumer_timeout_ms=5000,
        request_timeout_ms=5000,
    )
    topics = consumer.topics()
    consumer.close()
    assert isinstance(topics, set)
    print(f"Available Kafka topics: {topics}")


def test_k2_produce_and_consume_raw_records():
    """
    Produce 20 raw records with srcip key and stime timestamp to review-raw-test topic.
    Consume and verify count, ordering, and schema contract.
    """
    from kafka import KafkaConsumer
    test_topic = "review-raw-test"

    records_to_send = list(iter_raw_records(limit=20, file_indices=[1]))
    assert len(records_to_send) == 20

    # Produce to Kafka
    sink = KafkaSink(broker="127.0.0.1:9092", topic=test_topic)
    sink.open()
    for rec in records_to_send:
        sink.send(rec)
    sink.close()

    # Consume from Kafka
    consumer = KafkaConsumer(
        test_topic,
        bootstrap_servers="127.0.0.1:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=f"gate-k-test-group-{int(time.time())}",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=10000,
    )

    consumed_records = []
    consumed_keys = []
    for msg in consumer:
        consumed_records.append(msg.value)
        consumed_keys.append(msg.key.decode("utf-8") if msg.key else None)
        if len(consumed_records) >= 20:
            break
    consumer.close()

    assert len(consumed_records) >= 20, f"Expected 20 records, consumed {len(consumed_records)}"

    # Check first consumed message matches first produced
    first_msg = consumed_records[0]
    assert first_msg["record_id"] == "1:0"
    assert first_msg["context"]["srcip"] == "59.166.0.0"
    assert len(first_msg["features"]) == 41
    assert first_msg["evaluation"]["attack_cat"] == "Normal"
    # Verify srcip key
    assert consumed_keys[0] == "59.166.0.0"
