# PostgreSQL Persistence Layer Setup & Architecture

## 1. Purpose of the Layer

The PostgreSQL persistence layer provides ACID-compliant, queryable storage for real-time network anomaly detection on the UNSW-NB15 dataset. Records consumed from Kafka and scored inside Apache Spark Structured Streaming are durably persisted to PostgreSQL to support:
1. **Auditable Append-Only Raw Telemetry**: Storing received flow messages post-contract with network context, features, and evaluation labels segregated.
2. **Scored Flow History**: Storing inference outputs (`score`, binary `flagged`, `run_id`, `scored_at`) foreign-keyed to raw records.
3. **Exact Temporal Window Aggregations**: Idempotently maintaining 5-second tumbling window statistics in SQL for active windows, enabling late-flow re-aggregation.
4. **Model Run Lineage (Gate D5)**: Tracking model thresholds, alert configurations, watermark policies, and training dataset provenance.

---

## 2. Schema Architecture & Key Design Decisions

The database contains four core tables designed for least privilege, relational integrity, and analytical efficiency:

### `raw_flows` (Append-Only Raw Layer)
- **Primary Key**: `record_id TEXT` (e.g. `"1:0"`, `"3:2451"`). Enforces uniqueness per raw flow.
- **Columns**: `event_time TIMESTAMPTZ`, `ingest_time TIMESTAMPTZ`, `srcip TEXT`, `sport INT`, `dstip TEXT`, `dsport INT`, `stime BIGINT`, `ltime BIGINT`, `features JSONB`, `label SMALLINT`, `attack_cat TEXT`, `loaded_at TIMESTAMPTZ DEFAULT now()`.
- **Indices**: `idx_raw_flows_event_time` on `event_time` for fast window range scans.
- **Contract Boundary**: Stores the Kafka message as received post-contract. Nominal missing fields (`"-"`) have been canonicalized to `"none"` and NaNs to `0`/`null` by the loader. Ground-truth `label` and `attack_cat` reside in dedicated columns outside `features` to prevent model leakage.

### `flow_scores` (Processed Inference Layer)
- **Primary Key**: `record_id TEXT REFERENCES raw_flows(record_id) ON DELETE CASCADE`.
- **Columns**: `score DOUBLE PRECISION`, `flagged BOOLEAN`, `run_id TEXT REFERENCES model_runs(run_id)`, `scored_at TIMESTAMPTZ DEFAULT now()`.
- **Indices**: `idx_flow_scores_flagged` on `flagged` for fast anomaly filtering.

### `window_metrics` (5-Second Tumbling Window Aggregations)
- **Primary Key**: `window_start TIMESTAMPTZ`.
- **Columns**: `window_end TIMESTAMPTZ`, `flow_count INT`, `flagged_count INT`, `window_alert BOOLEAN`, `attack_records INT`, `tcp_count INT`, `udp_count INT`, `other_count INT`, `updated_at TIMESTAMPTZ DEFAULT now()`.
- **Key Design**: Keyed **strictly by `window_start` only**. Per-source host windows were explicitly rejected because source IP encoding serves as an empirical label shortcut on UNSW-NB15 testbeds.

### `model_runs` (Model Lineage & Provenance - Gate D5)
- **Primary Key**: `run_id TEXT`.
- **Columns**: `model_file TEXT`, `preprocessor_file TEXT`, `threshold DOUBLE PRECISION`, `alert_k INT`, `watermark TEXT`, `provenance JSONB`, `created_at TIMESTAMPTZ DEFAULT now()`.

---

## 3. Technology Trade-Offs: Why PostgreSQL?

We evaluated PostgreSQL against four alternative data architectures:

| Storage Engine | Strengths | Trade-Offs / Disqualifiers for This Stage | Selected? |
| :--- | :--- | :--- | :---: |
| **PostgreSQL (Selected)** | ACID transactions across raw & processed writes; `ON CONFLICT` idempotency; rich JSONB indexing; single-node relational joins for SQL window recomputation; standard role-based access control (Gate D2). | No horizontal write scaling without sharding (Citus); local disk bound. | **YES** |
| **Parquet on Object Store / Disk** | High columnar read throughput; compact compression; native Spark integration. | Lacks transactional point-updates (`UPDATE` / `ON CONFLICT`); re-aggregating late flows requires expensive partition rewrites. | No |
| **MongoDB** | Flexible document schema; fast JSON ingestion. | Multi-document transactions carry substantial latency overhead; less robust relational join support for SQL window recomputation. | No |
| **Apache Cassandra** | Highly scalable horizontal write throughput; masterless clustering. | Lacks ACID transactions across multiple tables; no ad-hoc joins; tumbling window re-aggregation requires complex external coordinator. | No |
| **TimescaleDB** | Automatic time hyper-partitioning; continuous aggregates. | Additional custom extensions required; continuous aggregate materialization introduces lag for real-time alerting; overkill for single-node development footprint. | No |

---

## 4. Least-Privilege Role Model (Gate D2)

Access control enforces least privilege via PostgreSQL role inheritance:

1. **`raw_writer`**:
   - Privileges: `INSERT` ONLY on `raw_flows`.
   - Explicitly revoked: `UPDATE`, `DELETE` are disallowed, guaranteeing append-only immutability.
2. **`processed_writer`**:
   - Privileges: `INSERT`, `UPDATE`, `SELECT` on `flow_scores`, `window_metrics`, `model_runs`.
   - `SELECT`, `REFERENCES` on `raw_flows` for foreign-key validation and window aggregation joins.
3. **`app_user` (Application Login)**:
   - Non-superuser (`NOSUPERUSER NOCREATEDB NOCREATEROLE`).
   - Member of both `raw_writer` and `processed_writer`.
   - In each micro-batch, execution runs in a single transaction that switches roles locally:
     `SET LOCAL ROLE raw_writer;` for raw flow insertion, then `SET LOCAL ROLE processed_writer;` for scores and window metric recomputation.

---

## 5. Startup Commands & Environment Configuration

### 1. Environment Configuration (`.env`)
Create a `.env` file from `.env.example`:
```bash
POSTGRES_DB=network_monitoring
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres_secure_pass
POSTGRES_PORT=5432
POSTGRES_HOST=localhost

APP_DB_USER=app_user
APP_DB_PASSWORD=app_secure_pass
```

### 2. Start PostgreSQL Container
```powershell
docker compose up -d postgres
```

### 3. Apply Schema and Roles
```powershell
cmd /c "docker exec -i postgres psql -U postgres -d network_monitoring < db\schema.sql"
cmd /c "docker exec -i postgres psql -U postgres -d network_monitoring < db\roles.sql"
```

### 4. Run Streaming Consumer with Database Sink
```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jre1.8.0_461"
$env:HADOOP_HOME = "C:\Users\sweni\hadoop"
.\.venv\Scripts\Activate.ps1

python src/spark_streaming.py `
  --score `
  --db-sink `
  --watermark "60 seconds" `
  --shuffle-partitions 2 `
  --alert-k 3
```

---

## 6. Safe Reset Procedure

To safely truncate and reset data without dropping database volumes or altering Kafka:

```powershell
docker exec -i postgres psql -U postgres -d network_monitoring -c "
  TRUNCATE TABLE flow_scores, raw_flows, window_metrics CASCADE;
"
```

---

## 7. Known Architectural Limitations

1. **Post-Contract Raw Representation**: The `raw_flows` table stores payloads as received from Kafka post-contract. Initial parsing by `raw_loader.py` has already mapped dashes (`"-"`) to `"none"`, converted empty strings to numeric defaults (`0`, `0.0`), and parsed timestamps into UTC ISO-8601 strings.
2. **SQL Window Recomputation vs Spark Watermark**: Spark's state store drops records arriving later than the watermark (`60 seconds`). In contrast, the PostgreSQL layer executes an SQL `ON CONFLICT DO UPDATE` recomputation across all touched windows, re-aggregating late flows into their true historical window rather than dropping them.
3. **Throughput Ceiling**: The current single-node pipeline sustains ingestion at ~25 rec/s. Ingestion rates of 50–100 rec/s trigger microbatch execution delays (~6.8–7.1 s) exceeding the 5.0 s trigger, bound primarily by PyArrow UDF scoring.
4. **Fatal Outage Recovery**: Under a database outage, `db_sink.py` retries for up to ~30 s with exponential backoff (Gate F6). If the outage exceeds 30 s, the query terminates cleanly without committing offsets. Upon service restoration, restarting Spark from the existing checkpoint re-processes uncommitted batches idempotently with zero data loss or duplicates.

