# Sylos Migration Local Filesystem Test Runner
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

Clear-Host

Write-Host "=== Sylos Migration Local Filesystem Test Runner ===" -ForegroundColor Cyan
Write-Host ""

# Clean up existing test databases
Write-Host "Cleaning up test databases..." -ForegroundColor Yellow

# Remove the BoltDB file if it exists
if (Test-Path "pkg/tests/shared/main_test.db") {
    Write-Host "Removing pkg/tests/shared/main_test.db file..." -ForegroundColor Yellow
    Remove-Item -Path "pkg/tests/shared/main_test.db" -Force -ErrorAction SilentlyContinue
}

# Remove the migration config YAML file if it exists
if (Test-Path "pkg/tests/shared/main_test.yaml") {
    Write-Host "Removing pkg/tests/shared/main_test.yaml file..." -ForegroundColor Yellow
    Remove-Item -Path "pkg/tests/shared/main_test.yaml" -Force -ErrorAction SilentlyContinue
}

Write-Host "Cleanup complete" -ForegroundColor Green
Write-Host ""

# Run the test
$startTime = Get-Date

# Execute local test runner
go run pkg/tests/local/main.go

$exitCode = $LASTEXITCODE

$endTime = Get-Date
$duration = $endTime - $startTime

Write-Host ""
Write-Host "=== Test Summary ===" -ForegroundColor Cyan
Write-Host "Duration: $($duration.TotalSeconds) seconds"

if ($exitCode -eq 0) {
    Write-Host "Status: PASSED" -ForegroundColor Green
} else {
    Write-Host "Status: FAILED" -ForegroundColor Red
}

Write-Host ""

exit $exitCode

