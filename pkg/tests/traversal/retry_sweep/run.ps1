# Retry Sweep Test Runner
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later
# Test DB is DuckDB at pkg/tests/traversal/shared/main_test.db. If missing, it is generated (traversal from Spectra roots).

Clear-Host
Write-Host "=== Retry Sweep Test Runner ===" -ForegroundColor Cyan
Write-Host ""

$destDB = "pkg/tests/traversal/shared/main_test.db"

if (-not (Test-Path $destDB)) {
    Write-Host "Test DB not found. Generating DuckDB (traversal from Spectra roots)..." -ForegroundColor Yellow
    go run ./cmd/gen_traversal_test_db
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    Write-Host ""
}

$startTime = Get-Date
Write-Host "Running retry sweep test..." -ForegroundColor Cyan
go run ./pkg/tests/traversal/retry_sweep/main.go
$exitCode = $LASTEXITCODE
$endTime = Get-Date
$duration = ($endTime - $startTime).TotalSeconds

Write-Host ""
Write-Host "=== Test Summary ===" -ForegroundColor Cyan
Write-Host "Duration: $duration seconds"
if ($exitCode -eq 0) {
    Write-Host "Status: PASSED" -ForegroundColor Green
} else {
    Write-Host "Status: FAILED" -ForegroundColor Red
}
Write-Host ""
exit $exitCode
