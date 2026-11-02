-- Operational & Analytical Monitoring Queries (Review Gate D6)
-- Database: network_monitoring

-- 1. Window Alerts per Minute
SELECT
    date_trunc('minute', window_start) AS alert_minute,
    COUNT(*) AS total_windows,
    COUNT(*) FILTER (WHERE window_alert = TRUE) AS alert_windows,
    SUM(flow_count) AS total_flows,
    SUM(flagged_count) AS total_flagged_flows
FROM window_metrics
GROUP BY date_trunc('minute', window_start)
ORDER BY alert_minute DESC;

-- 2. Flagged Share by Transport Protocol
SELECT
    COALESCE(r.features->>'proto', 'unknown') AS protocol,
    COUNT(*) AS total_flows,
    COUNT(*) FILTER (WHERE s.flagged = TRUE) AS flagged_flows,
    ROUND(
        COUNT(*) FILTER (WHERE s.flagged = TRUE) * 100.0 / NULLIF(COUNT(*), 0),
        2
    ) AS flagged_percentage
FROM raw_flows r
JOIN flow_scores s ON r.record_id = s.record_id
GROUP BY r.features->>'proto'
ORDER BY total_flows DESC;

-- 3. Top 10 Source IPs by Flagged Anomaly Count
SELECT
    r.srcip,
    COUNT(*) AS total_flows_emitted,
    COUNT(*) FILTER (WHERE s.flagged = TRUE) AS flagged_anomaly_count,
    ROUND(
        COUNT(*) FILTER (WHERE s.flagged = TRUE) * 100.0 / NULLIF(COUNT(*), 0),
        2
    ) AS source_flagged_percentage
FROM raw_flows r
JOIN flow_scores s ON r.record_id = s.record_id
GROUP BY r.srcip
HAVING COUNT(*) FILTER (WHERE s.flagged = TRUE) > 0
ORDER BY flagged_anomaly_count DESC
LIMIT 10;

-- 4. Confusion Matrix Counts (EVALUATION ONLY - Gate X3)
-- GROUND-TRUTH PASSTHROUGH ONLY (NEVER MODEL INPUT - Gate X3)
-- True labels are utilized strictly post-hoc for operational verification.
SELECT
    COUNT(*) FILTER (WHERE s.flagged = TRUE AND r.label > 0) AS true_positives,
    COUNT(*) FILTER (WHERE s.flagged = TRUE AND r.label = 0) AS false_positives,
    COUNT(*) FILTER (WHERE s.flagged = FALSE AND r.label = 0) AS true_negatives,
    COUNT(*) FILTER (WHERE s.flagged = FALSE AND r.label > 0) AS false_negatives,
    ROUND(
        COUNT(*) FILTER (WHERE s.flagged = TRUE AND r.label > 0) * 100.0 /
        NULLIF(COUNT(*) FILTER (WHERE r.label > 0), 0),
        2
    ) AS recall_pct,
    ROUND(
        COUNT(*) FILTER (WHERE s.flagged = TRUE AND r.label > 0) * 100.0 /
        NULLIF(COUNT(*) FILTER (WHERE s.flagged = TRUE), 0),
        2
    ) AS precision_pct,
    ROUND(
        COUNT(*) FILTER (WHERE s.flagged = TRUE AND r.label = 0) * 100.0 /
        NULLIF(COUNT(*) FILTER (WHERE r.label = 0), 0),
        4
    ) AS fpr_pct
FROM raw_flows r
JOIN flow_scores s ON r.record_id = s.record_id;

