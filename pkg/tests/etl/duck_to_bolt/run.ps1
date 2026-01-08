# Sylos ETL Test Runner (Duck to Bolt)
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

# Clear-Host

Write-Host "=== Sylos ETL Test Runner (Duck to Bolt) ===" -ForegroundColor Cyan
Write-Host ""

# Clean up existing BoltDB file
Write-Host "Cleaning up test databases..." -ForegroundColor Yellow

# Remove the BoltDB file if it exists
$boltDBPath = "pkg/tests/etl/duck_to_bolt/main-bolt.db"
if (Test-Path $boltDBPath) {
    Write-Host "Removing $boltDBPath file..." -ForegroundColor Yellow
    Remove-Item -Path $boltDBPath -Force -ErrorAction SilentlyContinue
}

Write-Host "Cleanup complete" -ForegroundColor Green
Write-Host ""

# Check if DuckDB exists (source)
$duckDBPath = "pkg/tests/etl/duck_to_bolt/main-duck.db"
if (-not (Test-Path $duckDBPath)) {
    Write-Host "⚠️  Warning: DuckDB file not found at $duckDBPath" -ForegroundColor Yellow
    Write-Host "   The DuckDB file should exist as the source for this test." -ForegroundColor Yellow
    Write-Host ""
}

# Run the test
$startTime = Get-Date

# Execute ETL test runner
go run pkg/tests/etl/duck_to_bolt/main/main.go

$exitCode = $LASTEXITCODE

$endTime = Get-Date
$duration = $endTime - $startTime

Write-Host ""
Write-Host "=== Test Summary ===" -ForegroundColor Cyan
Write-Host "Duration: $($duration.TotalSeconds) seconds"
Write-Host ""

if ($exitCode -eq 0) {
    Write-Host "Status: " -NoNewline
    Write-Host "PASSED" -ForegroundColor Green -BackgroundColor Black
} else {
    Write-Host "Status: " -NoNewline
    Write-Host "FAILED" -ForegroundColor Red -BackgroundColor Black
}

Write-Host ""

exit $exitCode
