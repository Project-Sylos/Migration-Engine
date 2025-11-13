# Sylos Migration Test Runner
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

Write-Host "=== Sylos Migration Test Runner ===" -ForegroundColor Cyan
Write-Host ""

# Clean up existing test databases
Write-Host "Cleaning up test databases..." -ForegroundColor Yellow
Remove-Item -Path "internal\tests\main\migration_test.db" -ErrorAction SilentlyContinue
Remove-Item -Path "internal\tests\main\migration_test.db-wal" -ErrorAction SilentlyContinue
Remove-Item -Path "internal\tests\main\migration_test.db-shm" -ErrorAction SilentlyContinue
Write-Host "Cleanup complete" -ForegroundColor Green
Write-Host ""

# Run the test
$startTime = Get-Date

# Navigate to tests/main directory and run test runner
Push-Location internal\tests\main
go run setup.go test_runner.go verify.go

$exitCode = $LASTEXITCODE
Pop-Location

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

