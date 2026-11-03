# Run Streamlit Serving & Observability Dashboard
# Host: localhost:8501
$ErrorActionPreference = "Stop"

$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$ROOT_DIR = Split-Path -Parent $SCRIPT_DIR
Set-Location $ROOT_DIR

# Activate virtual environment
if (Test-Path ".\.venv\Scripts\Activate.ps1") {
    .\.venv\Scripts\Activate.ps1
}

# Load environment variables from .env
if (Test-Path ".env") {
    Get-Content ".env" | ForEach-Object {
        $line = $_.Trim()
        if ($line -and (-not $line.StartsWith("#")) -and ($line -match "^([^=]+)=(.*)$")) {
            [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), "Process")
        }
    }
}

Write-Host "Starting Serving & Observability Dashboard on http://localhost:8501..." -ForegroundColor Cyan
Write-Host "Role: dashboard_reader (Least-Privilege Read-Only)" -ForegroundColor Gray

streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0

