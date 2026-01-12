# BoltDB to DuckDB ETL Script
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

# Clear-Host

Write-Host "=== BoltDB to DuckDB ETL Tool ===" -ForegroundColor Cyan
Write-Host ""

# Check if source database exists
$sourceDB = "pkg/tests/copy/shared/main_test.db"

if (-not (Test-Path $sourceDB)) {
    Write-Host "ERROR: Source database not found: $sourceDB" -ForegroundColor Red
    Write-Host "Please run the copy test first (run.ps1) to generate main_test.db" -ForegroundColor Yellow
    exit 1
}

Write-Host "Source BoltDB: $sourceDB" -ForegroundColor Yellow
Write-Host ""

# Run the ETL tool
$startTime = Get-Date

Write-Host "Running ETL migration..." -ForegroundColor Cyan
go run pkg/tests/copy/normal/etl/main.go

$exitCode = $LASTEXITCODE
$endTime = Get-Date
$duration = $endTime - $startTime

Write-Host ""
Write-Host "=== ETL Summary ===" -ForegroundColor Cyan
Write-Host "Duration: $($duration.TotalSeconds) seconds"

if ($exitCode -eq 0) {
    Write-Host "Status: SUCCESS" -ForegroundColor Green
    Write-Host ""
    Write-Host "DuckDB file created: pkg/tests/copy/shared/main_test-duck.db" -ForegroundColor Green
    Write-Host "You can now open this file in DBeaver or another DuckDB client." -ForegroundColor Cyan
} else {
    Write-Host "Status: FAILED" -ForegroundColor Red
}

Write-Host ""

exit $exitCode
