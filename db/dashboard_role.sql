-- Role Definition & Least-Privilege Grants for Serving & Observability Dashboard
-- Database: network_monitoring

-- 1. Create Dashboard Login Role (Non-Superuser, Read-Only)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dashboard_reader') THEN
        CREATE ROLE dashboard_reader WITH LOGIN PASSWORD 'dashboard_secure_pass' NOSUPERUSER NOCREATEDB NOCREATEROLE;
    END IF;
END $$;

-- 2. Schema & Connection privileges
GRANT CONNECT ON DATABASE network_monitoring TO dashboard_reader;
GRANT USAGE ON SCHEMA public TO dashboard_reader;

-- 3. Explicitly revoke all existing privileges to ensure clean state
REVOKE ALL ON TABLE raw_flows, flow_scores, window_metrics, model_runs FROM dashboard_reader;

-- 4. Full Table SELECT on Analytics, Scoring, and Model Provenance Tables
GRANT SELECT ON TABLE window_metrics TO dashboard_reader;
GRANT SELECT ON TABLE flow_scores TO dashboard_reader;
GRANT SELECT ON TABLE model_runs TO dashboard_reader;

-- 5. Strict Column-Level SELECT on raw_flows (Features column DENIED)
-- Granted columns: record_id, event_time, ingest_time, loaded_at, srcip, label, attack_cat
-- Denied columns: features, sport, dstip, dsport, stime, ltime
GRANT SELECT (record_id, event_time, ingest_time, loaded_at, srcip, label, attack_cat) ON TABLE raw_flows TO dashboard_reader;

