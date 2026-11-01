# Spark Structured Streaming Setup

## Purpose

This stage adds **real-time stream processing** to the pipeline. Records that have been published to Kafka by `csv_replayer.py` are consumed by Spark Structured Streaming, grouped into **5-second tumbling windows**, and aggregated into traffic statistics.

This is the first step toward anomaly detection: before we can identify suspicious behaviour, we need a baseline view of traffic patterns over short time windows.

## Data Flow

```
UNSW-NB15 CSV  ──▶  csv_replayer.py  ──▶  Kafka (network-traffic topic)
                                                     │
                                                     ▼
                                           spark_streaming.py
                                           (Structured Streaming)
                                                     │
                                          ┌──────────┴──────────┐
                                          │  5-second tumbling  │
                                          │  window aggregation │
                                          └──────────┬──────────┘
                                                     │
                                                     ▼
                                              Console output
                                          (later: PostgreSQL / Parquet)
```

## Dependencies

| Component | Version | Purpose |
|-----------|---------|---------|
| Python | 3.10.0 | Runtime |
| Java (JRE) | 1.8.0_461 | Required by Spark |
| PySpark | 3.5.5 | Spark engine + Structured Streaming |
| kafka-python-ng | 2.2.3 | Used by csv_replayer.py (producer) |
| Confluent Kafka | 7.7.0 | Docker images for Kafka + Zookeeper |
| spark-sql-kafka | 3.5.5 (auto-downloaded) | Kafka connector for Spark |

### `JAVA_HOME`

PySpark requires `JAVA_HOME` to be set. In each new PowerShell session:

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jre1.8.0_461"
```

To make this persistent across sessions (optional):

```powershell
[System.Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Java\jre1.8.0_461", "User")
```

## How to Start Services

### 1. Start Docker Desktop

Launch Docker Desktop from the Start menu or taskbar. Wait until it says "Docker Desktop is running".

### 2. Start Kafka and Zookeeper

```powershell
docker compose up -d
```

Verify:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Both `kafka` and `zookeeper` should show "Up" and "(healthy)".

### 3. Verify the Kafka Topic

```powershell
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list
```

You should see `network-traffic` in the output.

## How to Run the Spark Streaming Consumer

### 1. Set JAVA_HOME (every new terminal)

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jre1.8.0_461"
```

### 2. Activate the virtual environment

```powershell
.\.venv\Scripts\Activate.ps1
```

### 3. Start the streaming consumer

```powershell
python src/spark_streaming.py
```

The first run will download the Kafka connector JAR (~15 MB) from Maven Central. Subsequent runs use the cached JAR.

The consumer will print "Waiting for records from Kafka topic 'network-traffic' ..." and then block, waiting for data.

### 4. Replay records (in a separate terminal)

Open a second PowerShell terminal:

```powershell
cd C:\Users\sweni\Documents\DE-Network-Monitoring
.\.venv\Scripts\Activate.ps1
python src/csv_replayer.py --kafka --limit 50 --rate 10
```

This sends 50 records to Kafka at 10 records/second.

### 5. Observe output

Switch back to the Spark terminal. Within ~10 seconds, you should see window aggregation tables appearing.

## How the 5-Second Tumbling Windows Work

A **tumbling window** divides the time axis into fixed, non-overlapping intervals:

```
|<--- 5s --->|<--- 5s --->|<--- 5s --->|
   Window 1     Window 2     Window 3
```

Each record is assigned to exactly one window based on its `event_time`. Records with `event_time` between `10:00:00` and `10:00:05` go into one window; records between `10:00:05` and `10:00:10` go into the next.

**Watermark & UTC Alignment**:
- **Watermark (`--watermark`)**: Configured with a default of **`"60 seconds"`**. Empirical lateness analysis across all 2.54M raw flows (Task C1) revealed that flow start times (`stime`) exhibit 13.62% pairwise inversions and 19.20% running-max lateness (p99 lateness = 50.0s, p99.9 = 60.0s). A 10-second watermark dropped **1.27% of traffic (32,172 flows)**; the 60-second watermark reduces data drops to **0.0014% (36 flows)**.
- **Timezone (Explicit UTC)**: SparkSession is configured with `spark.sql.session.timeZone = "UTC"` and driver/executor JVM flags `-Duser.timezone=UTC`. This guarantees that 5-second tumbling window boundaries evaluate strictly as $\lfloor stime / 5 \rfloor \times 5$ in UTC without local JVM timezone offsets (e.g. IST +05:30).

**Output mode**: We use "update" mode, which outputs only new or changed window rows each trigger cycle. This gives fast feedback during development without producing unbounded output.

**Trigger**: Processing runs every 5 seconds, aligning with the window size.

## Output Columns Explained

| Column | Meaning |
|--------|---------|
| `window_start` / `window_end` | The 5-second window boundaries |
| `record_count` | Number of network records in this window |
| `flagged_count` | Number of flows in this window scored as anomalous (when `--score` is enabled) |
| `window_alert` | Binary alert flag (1 if `flagged_count >= alert_k`, else 0) |
| `avg_src_pkts` / `avg_dst_pkts` | Average source/destination packets per record |
| `avg_src_bytes` / `avg_dst_bytes` | Average source/destination bytes per record |
| `total_bytes` | Total bytes (src + dst) transferred in this window |
| `avg_duration` | Average connection duration in seconds |
| `avg_rate` | Average connection rate |
| `avg_src_load` / `avg_dst_load` | Average source/destination load |
| `tcp_count` / `udp_count` / `other_proto_count` | Protocol distribution within the window |
| `attack_records` | Count of records with ground-truth `label=1` |

> **Important**: `attack_records` is the ground-truth label count from the dataset used strictly for evaluation. `flagged_count` and `window_alert` are the actual machine-learning detection outputs from the Random Forest model.

## Real-Time Anomaly Scoring Inside Spark (Gate A7)

To score records in real time with the Random Forest model inside Spark:

### 1. Environment Variables (Required for Every New Terminal)
```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jre1.8.0_461"
$env:HADOOP_HOME = "C:\Users\sweni\hadoop"
cd C:\Users\sweni\Documents\DE-Network-Monitoring
.\.venv\Scripts\Activate.ps1
```

### 2. Start Consumer with ML Scoring Enabled
```powershell
python src/spark_streaming.py `
  --score `
  --watermark "60 seconds" `
  --shuffle-partitions 2 `
  --alert-k 3 `
  --threshold-file data/processed/models/calibrated_threshold.json
```

### 3. Replay Canonical Raw Traffic (In Second Terminal)
```powershell
cd C:\Users\sweni\Documents\DE-Network-Monitoring
.\.venv\Scripts\Activate.ps1
python src/csv_replayer.py --raw --kafka --topic network-traffic --rate 100 --limit 2000
```

## Fresh Test Procedure

### Why this matters

Spark Structured Streaming uses **checkpoints** to track which Kafka offsets have been processed. If you re-run the consumer without clearing checkpoints, it resumes from where it left off — which means previously consumed messages won't be re-processed.

Similarly, Kafka messages are persistent. Old messages from previous test runs are still on the topic.

### To run a completely fresh test:

1. **Delete the checkpoint directory**:
   ```powershell
   Remove-Item -Recurse -Force "data\processed\spark-checkpoints" -ErrorAction SilentlyContinue
   ```

2. **Start the Spark consumer** (it will read from the earliest offset):
   ```powershell
   python src/spark_streaming.py
   ```

3. **Replay new records**:
   ```powershell
   python src/csv_replayer.py --kafka --limit 50 --rate 10
   ```

The consumer's `startingOffsets = "earliest"` setting means that, with a fresh checkpoint, it will read **all** messages on the topic — including ones from previous test runs. If you want only new messages, you can either:
- Recreate the topic: `docker exec kafka kafka-topics --bootstrap-server localhost:29092 --delete --topic network-traffic` then `docker exec kafka kafka-topics --bootstrap-server localhost:29092 --create --topic network-traffic --partitions 1 --replication-factor 1`
- Or simply accept that old records will also be processed (they will appear in the window output).

## Checkpoint Directory

Checkpoints are stored at:

```
data/processed/spark-checkpoints/network-traffic-windows/
```

This directory is already excluded from Git via `data/processed/*` in `.gitignore`.

**Never commit checkpoint files** — they are machine-specific runtime state.

## Known Limitations

1. **First-run JAR download**: The first execution downloads the `spark-sql-kafka` connector and its dependencies from Maven. This requires internet access and may take 30–60 seconds.

2. **`JAVA_HOME` must be set per session**: PowerShell `$env:JAVA_HOME` is session-scoped. To avoid setting it every time, add it to your system environment variables.

3. **Spark logs**: Spark produces verbose log output at startup. The script sets log level to WARN, but some initial INFO messages from the Kafka connector are expected.

4. **Window timing**: Since `csv_replayer.py` generates `event_time` as the current wall-clock time at emission, all records from a single replay run will have closely spaced timestamps. With `--rate 10`, you'll typically see 2–3 windows of ~25 records each for a 50-record batch.

5. **Persistence and Dashboard**: Real-time anomaly detection and window aggregation are implemented via `--score`. Downstream PostgreSQL / Parquet persistence and a live monitoring dashboard are planned for future stages.
