-- Schema Definition: Real-Time Network Anomaly Detection (UNSW-NB15)
-- Database: network_monitoring
-- Review Gates: D1, D3, D5

-- 1. Model runs table (Gate D5: parameters, thresholds, and training provenance)
CREATE TABLE IF NOT EXISTS model_runs (
    run_id TEXT PRIMARY KEY,
    model_file TEXT NOT NULL,
    preprocessor_file TEXT NOT NULL,
    threshold DOUBLE PRECISION NOT NULL,
    alert_k INT NOT NULL,
    watermark TEXT NOT NULL,
    provenance JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- 2. Raw flows table (Gate D1: append-only Kafka messages as received)
-- Note: This is post-contract Kafka payload (nominal '-' cleaned to 'none', NaN to null/0 by replayer)
CREATE TABLE IF NOT EXISTS raw_flows (
    record_id TEXT PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    ingest_time TIMESTAMPTZ NOT NULL,
    srcip TEXT NOT NULL,
    sport INT NOT NULL,
    dstip TEXT NOT NULL,
    dsport INT NOT NULL,
    stime BIGINT NOT NULL,
    ltime BIGINT NOT NULL,
    features JSONB NOT NULL,
    label SMALLINT NOT NULL,
    attack_cat TEXT NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT now()
);

-- 3. Processed flow scores table (Gate D1: inference outputs per flow)
CREATE TABLE IF NOT EXISTS flow_scores (
    record_id TEXT PRIMARY KEY REFERENCES raw_flows(record_id) ON DELETE CASCADE,
    score DOUBLE PRECISION NOT NULL,
    flagged BOOLEAN NOT NULL,
    run_id TEXT REFERENCES model_runs(run_id),
    scored_at TIMESTAMPTZ DEFAULT now()
);

-- 4. Window metrics table (Gate D1, D3: 5-second tumbling window aggregations)
-- Keyed strictly by window_start only (no per-source encoding to prevent label shortcuts)
CREATE TABLE IF NOT EXISTS window_metrics (
    window_start TIMESTAMPTZ PRIMARY KEY,
    window_end TIMESTAMPTZ NOT NULL,
    flow_count INT NOT NULL,
    flagged_count INT NOT NULL,
    window_alert BOOLEAN NOT NULL,
    attack_records INT NOT NULL,
    tcp_count INT NOT NULL,
    udp_count INT NOT NULL,
    other_count INT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- 5. Indexes strictly on query keys (event_time, flagged, window_start)
CREATE INDEX IF NOT EXISTS idx_raw_flows_event_time ON raw_flows (event_time);
CREATE INDEX IF NOT EXISTS idx_flow_scores_flagged ON flow_scores (flagged);
-- window_start is already indexed as PRIMARY KEY

