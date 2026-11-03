# Live Streaming Demonstration Setup Script
# =========================================
# Generates a fresh timestamped Kafka topic and Spark checkpoint folder on D:,
# ensures the topic has infinite retention (retention.ms=-1), and prints the
# exact copy-paste commands for Spark and the live replayer.
#
# SAFETY GUARANTEE: Does NOT truncate or modify any database tables.

$ErrorActionPreference = "Stop"

$TIMESTAMP = Get-Date -Format "yyyyMMdd_HHmmss"
$TOPIC = "live-stream-demo-$TIMESTAMP"
$CHECKPOINT_DIR = "D:\spark-checkpoints\$TOPIC"
$RUN_ID = "run_demo_$TIMESTAMP"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  LIVE DEMO PIPELINE SETUP (Safe Session Initializer)        " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Timestamp         : $TIMESTAMP"
Write-Host "Target Kafka Topic: $TOPIC"
Write-Host "Spark Checkpoint  : $CHECKPOINT_DIR"
Write-Host "Model Run ID      : $RUN_ID"
Write-Host "Storage Safety    : NO DATABASE TABLES ARE TRUNCATED" -ForegroundColor Green
Write-Host "------------------------------------------------------------"

# Ensure checkpoint parent directory exists on D:
if (-not (Test-Path "D:\spark-checkpoints")) {
    New-Item -ItemType Directory -Path "D:\spark-checkpoints" | Out-Null
}

# Create topic with infinite retention in Kafka Docker container
Write-Host "Creating Kafka topic with retention.ms=-1..." -ForegroundColor Yellow
docker exec kafka kafka-topics --create `
  --topic $TOPIC `
  --bootstrap-server localhost:9092 `
  --partitions 1 `
  --replication-factor 1 `
  --config retention.ms=-1

if ($LASTEXITCODE -eq 0) {
    Write-Host "Kafka topic '$TOPIC' created successfully." -ForegroundColor Green
} else {
    Write-Host "Warning: Failed to create Kafka topic. Ensure Docker and Kafka are running." -ForegroundColor Red
    exit 1
}

Write-Host "`n============================================================" -ForegroundColor Cyan
Write-Host "  EXECUTION INSTRUCTIONS (Follow in Order)                  " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

Write-Host "`n[STEP 1] Start Spark FIRST in Terminal 1 (Waits on empty topic):" -ForegroundColor White
Write-Host "cd C:\Users\sweni\OneDrive\Documents\DE-Network-Monitoring" -ForegroundColor Gray
Write-Host ".\.venv\Scripts\python.exe -u src/spark_streaming.py --topic `"$TOPIC`" --checkpoint-dir `"$CHECKPOINT_DIR`" --score --db-sink --watermark `"60 seconds`" --shuffle-partitions 2 --alert-k 3 --run-id `"$RUN_ID`"" -ForegroundColor Yellow

Write-Host "`n[STEP 2] Start Streamlit Dashboard in Terminal 2:" -ForegroundColor White
Write-Host "cd C:\Users\sweni\OneDrive\Documents\DE-Network-Monitoring" -ForegroundColor Gray
Write-Host ".\.venv\Scripts\python.exe -m streamlit run dashboard/app.py" -ForegroundColor Yellow

Write-Host "`n[STEP 3] Start Traffic Replayer in Terminal 3 (After Spark starts Batch 0):" -ForegroundColor White
Write-Host "cd C:\Users\sweni\OneDrive\Documents\DE-Network-Monitoring" -ForegroundColor Gray
Write-Host ".\.venv\Scripts\python.exe -u scripts/verify/live_replayer.py --topic `"$TOPIC`" --rate 25 --limit 3000 --file-index 3" -ForegroundColor Yellow

Write-Host "`n============================================================" -ForegroundColor Cyan

