# Real-Time Network Traffic Monitoring and Anomaly Detection

[![Python](https://img.shields.io/badge/Python-3.10-blue.svg)](https://www.python.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.5-orange.svg)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.7.0-black.svg)](https://kafka.apache.org/)
[![Tests](https://img.shields.io/badge/Tests-13%20Passed-brightgreen.svg)]()

An end-to-end, production-grade real-time Data Engineering streaming and anomaly detection pipeline that simulates enterprise network traffic using the **UNSW-NB15** benchmark dataset (2.54M records), streams telemetry through **Apache Kafka**, processes micro-batches with 5-second tumbling event-time windows using **Apache Spark Structured Streaming**, and executes causal, cross-session anomaly detection with frozen preprocessing.

> 📄 **Complete Submission Report**: See the comprehensive, publication-ready report with all empirical benchmarks, EDA alignments, anomaly detection results, and fresh test outputs in [reports/PROJECT_SUBMISSION_REPORT.md](reports/PROJECT_SUBMISSION_REPORT.md).

---

## Architecture Overview

```
┌─────────────────────────────────┐
│     UNSW-NB15 Raw CSVs         │
│  (2,540,047 49-Column Records)  │
│  Session 0: ~1.09M (2% attack)  │
│  Session 1: ~1.45M (20% attack) │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│   Canonical Loader & Replayer   │
│ (src/raw_loader.py & replayer)  │
│  - 6-key segregated contract    │
│  - Zero ground-truth leakage    │
└────────────────┬────────────────┘
                 │ (Kafka JSON Messages with srcip Key)
                 ▼
┌─────────────────────────────────┐
│     Apache Kafka (Docker)       │
│    Topic: 'network-traffic'     │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│    Spark Structured Streaming   │
│     (src/spark_streaming.py)    │
│  - 5s Tumbling Windows          │
│  - 10s Watermark Tolerance      │
│  - Tuned Shuffle Partitions (2) │
└────────────────┬────────────────┘
                 │
        ┌────────┴────────┐
        ▼                 ▼
┌───────────────┐ ┌─────────────────────────────────┐
│  Console Sink │ │   Causal Anomaly Detection      │
│  14 Metrics   │ │  - Frozen Preprocessing (A2)    │
│               │ │  - 41-Feature Random Forest (A1)│
│               │ │  - Cross-Session Evaluation (A5)│
└───────────────┘ └─────────────────────────────────┘
```

---

## Key Achievements & Highlights

1. **Canonical 49-Column Ingestion**: Fully integrated all 2,540,047 flow records across 4 raw files with `NUSW-NB15_features.csv`, transitioning away from truncated 45-column static splits.
2. **Anti-Leakage Data Contract**: Formulated a 6-key JSON contract (`record_id`, `event_time`, `ingest_time`, `context`, `features`, `evaluation`) isolating network identifiers (`srcip`, `dstip`) and ground truth from the 41 behavioral features.
3. **~6x PySpark Performance Optimization**: Diagnosed and eliminated Windows NTFS state-store disk thrashing by tuning `spark.sql.shuffle.partitions` from 200 to 2. Reduced average batch duration from 44.6s to 4.84s (sub-5s trigger latency achieved) and reduced state files by 98.4%.
4. **Causal Anomaly Detection (Gates A1-A6)**: Built a 41-feature Random Forest detector evaluated across temporal sessions with frozen preprocessing provenance, outperforming testbed shortcut baselines by **+15.93% PR-AUC** (0.9327 vs 0.7734) and achieving **0.9527 PR-AUC** with an operational FPR of **0.0050**.
5. **100% Automated Test Pass Rate**: All 13 automated review gates passing in ~26 seconds (`pytest tests/review -v`).

---

## Quick Start & Reproduction

### 1. Start Infrastructure (Docker)
```powershell
docker compose up -d
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

### 2. Configure Environment & Activate Virtual Environment
```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jre1.8.0_461"
$env:HADOOP_HOME = "C:\Users\sweni\hadoop"
.\.venv\Scripts\Activate.ps1
```

### 3. Run Automated Tests
```powershell
python -m pytest tests/review -v -s
```

### 4. Run Streaming Pipeline
```powershell
# Terminal 1: Start Spark Streaming Consumer
python src/spark_streaming.py --shuffle-partitions 2

# Terminal 2: Replay canonical records into Kafka
python src/csv_replayer.py --raw --kafka --limit 50 --rate 10
```

### 5. Run Performance Benchmark Suite
```powershell
python src/benchmark_spark.py --all
```

---

## Detailed Documentation
- [Comprehensive Submission Report](reports/PROJECT_SUBMISSION_REPORT.md)
- [Spark Performance Benchmark & Partition Analysis](docs/spark_performance_benchmark.md)
- [Spark Streaming Setup & Run Guide](docs/spark_streaming_setup.md)
- [Living Project Context Log](context.md)
