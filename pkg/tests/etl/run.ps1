# Sylos ETL Test Runner
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

# Clear-Host

Write-Host "=== Sylos ETL Test Runner ===" -ForegroundColor Cyan
Write-Host ""

# Clean up existing DuckDB file
Write-Host "Cleaning up test databases..." -ForegroundColor Yellow

# Remove the DuckDB file if it exists
$duckDBPath = "pkg/tests/etl/main-duck.db"
if (Test-Path $duckDBPath) {
    Write-Host "Removing $duckDBPath file..." -ForegroundColor Yellow
    Remove-Item -Path $duckDBPath -Force -ErrorAction SilentlyContinue
}

Write-Host "Cleanup complete" -ForegroundColor Green
Write-Host ""

# Check if BoltDB exists (from traversal test)
$boltDBPath = "pkg/tests/etl/main-bolt.db"
if (-not (Test-Path $boltDBPath)) {
    Write-Host "⚠️  Warning: BoltDB file not found at $boltDBPath" -ForegroundColor Yellow
    Write-Host "   You may need to run a traversal test first to generate the BoltDB." -ForegroundColor Yellow
    Write-Host ""
}

# Run the test
$startTime = Get-Date

# Execute ETL test runner
go run pkg/tests/etl/main/main.go

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
