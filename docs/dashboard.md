# Serving & Observability Dashboard Documentation

This document describes the design, architecture, security model, and operational procedures for the Real-Time Serving & Observability Dashboard built on Streamlit, Apache Spark Structured Streaming, and PostgreSQL.

---

## 1. Dashboard Architecture & Security Model

The dashboard operates strictly on PostgreSQL as an analytical serving and observability layer, completely decoupled from Apache Spark executors and the Apache Kafka broker.

```
[ Apache Kafka ] ──► [ Apache Spark ] ──► [ PostgreSQL ] ◄── [ Streamlit Dashboard ]
 (Ingestion)           (In-Stream          (Storage:         (Observability:
                        Inference)          Port 5433)        Port 8501)
                                                 ▲
                                                 │
                                        dashboard_reader
                                   (Least-Privilege RBAC)
```

### Least-Privilege RBAC (`dashboard_reader`)
The dashboard connects exclusively using the `dashboard_reader` database role (`db/dashboard_role.sql`):
- **Full Table SELECT**:
  - `window_metrics`: 5-second tumbling window aggregations and alerts.
  - `flow_scores`: Model inference outputs (`score`, `flagged`).
  - `model_runs`: Model hyperparameters, threshold calibration, and training provenance.
- **Column-Level SELECT on `raw_flows`**:
  - **Granted**: `record_id`, `event_time`, `ingest_time`, `loaded_at`, `srcip`, `label`, `attack_cat`.
  - **DENIED**: `features` (JSONB), `sport`, `dstip`, `dsport`, `stime`, `ltime`.
- **Denied Operations**:
  - `INSERT`, `UPDATE`, and `DELETE` are disallowed across all tables.

### Key Analytical Limitation: Protocol Mix Only
Because `raw_flows.features` is denied, the dashboard cannot parse `r.features->>'proto'` at query time to compute the anomaly share per protocol. Instead, transport protocol distribution is read directly from `window_metrics` (`tcp_count`, `udp_count`, `other_count`), which were pre-aggregated during in-stream micro-batch execution. This intentional constraint enforces data privacy and least-privilege security boundaries.

---

## 2. Dashboard Panels & Sections

### Section 1: Headline KPIs & Model Lineage
- **Metric Cards**: Total Flows Processed, Flagged Flows, Flagged Anomaly Share (%), Alerted Windows ($k \ge 3$), and Last Database Load Timestamp (UTC).
- **Model Lineage Expander (Gate D5)**: Active `run_id`, model artifact path, calibrated decision threshold, alert threshold $k$, and watermark duration.

### Section 2: Streaming Pipeline Health
- **End-to-End Latency Metrics**: Median (p50), 95th percentile (p95), and maximum ingestion latency ($$\text{loaded\_at} - \text{ingest\_time}$$ in seconds) calculated over the **most recent 2,000 flows** by `loaded_at`.
- **Flow Latency Time Series**: Visualizes ingestion latency variation across recent micro-batches.
- **Storage Throughput**: Effective rows/second persisted to PostgreSQL aggregated into 5-second buckets.

### Section 3: Windowed Traffic & Operational Alerts (No Ground Truth)
- **Windowed Traffic Chart**: Overlay bar chart of total flows and flagged flows per 5-second tumbling window (in 2015 simulated event time), highlighting windows where `window_alert = TRUE`.
- **Protocol Distribution**: Donut chart visualizing the balance between TCP, UDP, and Other protocols aggregated from `window_metrics`.
- **Top 10 Flagged Source IPs**: Identifies the primary traffic sources generating alerts using `raw_flows.srcip` joined to `flow_scores.flagged`.
- **Latest Alerted Windows Table**: Real-time tabular log of the 20 most recent alerted windows.

### Section 4: Ground-Truth Operational Evaluation (Post-Hoc Verification Only)
- **Notice Banner**: Explicitly marks ground-truth evaluation as post-hoc analysis. Ground-truth labels are never used as model inputs or operational alert triggers.
- **Confusion Matrix**: True Positives (TP), False Positives (FP), True Negatives (TN), and False Negatives (FN).
- **Operational Precision & Recall**: Real-time accuracy metrics on the live ingested traffic slice.
- **Attack Records vs Flagged Flows**: Dual-line trend chart comparing ground-truth attack volume with model anomaly detections per window.
- **Evaluation Caveats**:
  1. *Window Mixing*: 5-second tumbling windows aggregate multiple concurrent flows, naturally mixing background normal traffic with attack bursts.
  2. *Short-Slice Variance*: Pure-normal window counts in short replay slices are small ($<30$), resulting in high variance for live false-alarm rates.
  3. *Generalization Performance*: Definitive model detection metrics (AUC 0.94+) are established by the deduplicated cross-session test suite, not individual live stream slices.

---

## 3. Live Demonstration Guide (Using Safe Session Initializer)

To run a clean live streaming demonstration without risking topic or checkpoint collisions:

### Automated Setup: `scripts/demo_live.ps1`
Run the demo helper in PowerShell:
```powershell
powershell -ExecutionPolicy Bypass -File scripts/demo_live.ps1
```
This script:
1. Generates a fresh timestamped topic (`live-stream-demo-YYYYMMDD_HHMMSS`) with `retention.ms=-1`.
2. Allocates a clean checkpoint directory on the fast `D:` volume (`D:\spark-checkpoints\...`).
3. Guarantees safety: **does not truncate or modify database tables**.
4. Prints the exact copy-paste commands for Terminals 1, 2, and 3:

```powershell
# Terminal 1: Start Apache Spark FIRST (Waits on empty topic)
.\.venv\Scripts\python.exe -u src/spark_streaming.py `
  --topic "<generated_topic>" `
  --checkpoint-dir "<generated_checkpoint_dir>" `
  --score `
  --db-sink `
  --watermark "60 seconds" `
  --shuffle-partitions 2 `
  --alert-k 3 `
  --run-id "<generated_run_id>"

# Terminal 2: Start Streamlit Dashboard
.\.venv\Scripts\python.exe -m streamlit run dashboard/app.py

# Terminal 3: Start Traffic Replayer (After Spark displays Batch 0)
.\.venv\Scripts\python.exe -u scripts/verify/live_replayer.py `
  --topic "<generated_topic>" `
  --rate 25 `
  --limit 3000 `
  --file-index 3
```

---

## 4. Query Performance & Polling Impact Analysis

An empirical evaluation of dashboard query performance under active 5-second polling was conducted during live streaming (`reports/verification/dashboard_impact_20261102.json`):

### 4.1 Measured Query Overhead
- **Refresh Execution Time**: All 7 uncached analytical queries combined execute in approximately **75 ms** of PostgreSQL read time per 5-second cycle (mean: **75.67 ms**, p95: **186.40 ms**).
- **Driver / Network Overhead**: With 3-second Streamlit `@st.cache_data` caching and connection pooling, the dashboard operates without saturating connection limits or database CPU.
- **Spark Batch Durations**: PySpark micro-batches consistently measure between **6 and 9 seconds** against the 5-second trigger. The batch duration is driven by Python UDF serialization and Random Forest inference overhead, not database locks.
- **Polling ON vs. OFF Evaluation**: The observed 1.4-second delta between Polling ON (mean 8.89 s) and Polling OFF (mean 7.51 s) was driven by the initial PySpark JVM and JIT compilation warm-up batch (Batch 0/1 at 17.1 s) which fell entirely into the ON phase, rather than dashboard read pressure.

### 4.2 Latency Query Execution Plan (`EXPLAIN ANALYZE`)
The pipeline health query fetches recent ingestion latency:
```sql
SELECT loaded_at, ingest_time
FROM raw_flows
WHERE ingest_time IS NOT NULL
ORDER BY loaded_at DESC
LIMIT 2000;
```
- **Execution Plan**: A sequential scan plus sort (`Sort Key: loaded_at DESC`) over `raw_flows`.
- **Operational Assessment**: Measured on a 350-row test table, execution completes in **~0.5–2 ms**. At demonstration scale (several thousand rows), this provides snappy interactive refresh with negligible CPU cost.
- **Production Recommendation**: At full scale (millions of flows), sequential scan and sorting would become a bottleneck; an explicit B-tree index on `loaded_at DESC` (`CREATE INDEX idx_raw_flows_loaded_at ON raw_flows(loaded_at DESC)`) would be required to maintain sub-millisecond index scans.

---

## 5. Capturing Slide Screenshots

To capture clean presentation screenshots:
1. **Screenshot 1: KPIs & Windowed Traffic**:
   - Scroll to Section 1 & 3.
   - Capture headline metric cards, the model lineage expander, and the 5-second window traffic chart showing red-highlighted alerts.
2. **Screenshot 2: Streaming Pipeline Health & Protocol Mix**:
   - Scroll to Section 2.
   - Capture the end-to-end ingestion latency cards (p50, p95), the latency line graph, and the transport protocol donut chart.
3. **Screenshot 3: Ground-Truth Operational Evaluation**:
   - Scroll to Section 4.
   - Capture the confusion matrix cards, operational precision/recall, and the dual-line attack vs. flagged flow comparison.

---

## 6. Safe Database Reset Procedure

To clear existing tables between test runs, use `scripts/reset_db.py`:
```powershell
python scripts/reset_db.py
```
*The script will print the exact row counts in `raw_flows`, `flow_scores`, and `window_metrics`, and require typing `CONFIRM` before executing `TRUNCATE`.*

