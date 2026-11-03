# Live Streaming Architecture & Demonstration Guide

This guide documents the end-to-end live streaming architecture for real-time network anomaly detection on UNSW-NB15, where Apache Spark Structured Streaming runs continuously on incoming Kafka traffic and writes inference results directly to PostgreSQL.

---

## 1. Pipeline Architecture

```
[ UNSW-NB15 File 3 ] (Session 1, 100% Unseen Data)
         │
         ▼  (Paced replay: 25, 50, 100 rec/s; UTC ingest_time stamped at send)
[ Kafka Producer ] 
         │
         ▼  (Topic: network-traffic / live-stream-*, retention.ms=-1)
[ Apache Kafka ] (Port 9092, Confluent Platform)
         │
         ▼  (startingOffsets='earliest', failOnDataLoss='false')
[ PySpark Structured Streaming ] (5s UTC micro-batches, 60s watermark, shuffle partitions: 2)
   ├── 41 Behavioral Features Extraction (Zero label/context leakage)
   ├── Worker-Level Singleton Inference (Random Forest + FrozenPreprocessor)
   └── Least-Privilege Micro-Batch Sink (src/db_sink.py)
         │
         ▼  (Port 5433:5432, Transactional Batched Inserts)
[ PostgreSQL Storage Layer ] (Database: network_monitoring)
   ├── raw_flows       (raw_writer role: INSERT only)
   ├── flow_scores     (processed_writer role: INSERT + ON CONFLICT UPDATE)
   └── window_metrics  (processed_writer role: SQL 5s tumbling window recomputation)
```

---

## 2. Live Streaming Demonstration (Step-by-Step PowerShell)

### Step 2.1: Open Terminal 1 — Start Infrastructure Services

```powershell
# Navigate to repository root
cd C:\Users\sweni\OneDrive\Documents\DE-Network-Monitoring

# Start Docker containers (Zookeeper, Kafka, PostgreSQL)
docker compose up -d

# Verify all services are healthy and running
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

---

### Step 2.2: Open Terminal 2 — Start Apache Spark FIRST

In a true live streaming setup, Spark starts **first** on an empty topic and awaits incoming streams.

```powershell
# Navigate to repository root and activate virtual environment
cd C:\Users\sweni\OneDrive\Documents\DE-Network-Monitoring
.\.venv\Scripts\Activate.ps1

# Set required runtime environment variables
$env:JAVA_HOME = "C:\Program Files\Java\jre1.8.0_461"
$env:HADOOP_HOME = "C:\Users\sweni\hadoop"
$env:PATH = "$env:HADOOP_HOME\bin;" + $env:PATH
$env:PYSPARK_PYTHON = (Get-Command python).Source
$env:PYSPARK_DRIVER_PYTHON = (Get-Command python).Source

# Set unique topic and checkpoint directory
$TOPIC = "live-stream-demo"
$CHECKPOINT = "D:\spark-checkpoints\live-demo"

# Start Spark Structured Streaming with Real-Time Scoring & PostgreSQL Sink
python -u src/spark_streaming.py `
  --topic $TOPIC `
  --checkpoint-dir $CHECKPOINT `
  --score `
  --db-sink `
  --watermark "60 seconds" `
  --shuffle-partitions 2 `
  --alert-k 3 `
  --run-id "run_live_demo"
```
*Wait until Spark outputs:* `[BENCHMARK Batch 0] Input: 0 rows` *(Spark is now actively waiting for messages).*

---

### Step 2.3: Open Terminal 3 — Start the Concurrent Replayer

In a third terminal, start emitting records at the desired rate (e.g., 25 rec/s):

```powershell
# Navigate to repository root and activate virtual environment
cd C:\Users\sweni\OneDrive\Documents\DE-Network-Monitoring
.\.venv\Scripts\Activate.ps1

# Start streaming out-of-sample File 3 records into Kafka
python -u scripts/verify/live_replayer.py `
  --topic "live-stream-demo" `
  --broker "localhost:9092" `
  --limit 5000 `
  --rate 25.0 `
  --file-index 3
```

---

### Step 2.4: Open Terminal 4 — Watch Rows Grow Live in PostgreSQL

Watch rows populate into the database in real time during active replay:

```powershell
# Watch raw flows row count grow
while ($true) {
  docker exec postgres psql -U postgres -d network_monitoring -c "
    SELECT 
      (SELECT count(*) FROM raw_flows) AS raw_flows,
      (SELECT count(*) FROM flow_scores) AS flow_scores,
      (SELECT count(*) FROM window_metrics) AS window_metrics,
      (SELECT count(*) FROM window_metrics WHERE window_alert = TRUE) AS alerted_windows;
  "
  Start-Sleep -Seconds 2
}
```

---

## 3. Automated Live Verification Suite

To run the complete automated benchmark across all rates (Run A @ 25 rec/s, Run D idempotent live rerun, Run B1 @ 50 rec/s, Run B2 @ 100 rec/s) with sub-second sample logging:

```powershell
cd C:\Users\sweni\OneDrive\Documents\DE-Network-Monitoring
.\.venv\Scripts\Activate.ps1

python scripts/verify/verify_live_streaming.py `
  --runs A D B1 B2 `
  --output-json reports/verification/live_streaming_20261102.json
```

---

## 4. Key Performance Observations

1. **Readiness Gate**: Spark is initialized on an empty Kafka topic before traffic ingestion starts. Configured with `startingOffsets='earliest'`, no early messages are dropped even during initial worker warm-up.
2. **Cold-Start Latency**: Batch 0 is empty (0 input rows). Batch 1 experiences cold-start initialization (~7-11 s) as PySpark workers deserialize `rf_detector.joblib` and `preprocessor.joblib`.
3. **Steady-State Stability**: From Batch 2 onwards, worker singleton caching reduces per-batch execution times to ~1.8-2.5 s, comfortably sustaining 25 rec/s within the 5 s trigger interval.
4. **Zero Overlap Guarantee**: All live evaluation traffic is drawn from `UNSW-NB15_3.csv` (File 3, Session 1), ensuring 0.00% overlap with Session 0 training rows.

