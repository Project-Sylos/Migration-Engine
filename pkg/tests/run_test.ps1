# Sylos Migration Test Runner
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

Clear-Host

Write-Host "=== Sylos Migration Test Runner ===" -ForegroundColor Cyan
Write-Host ""

# Clean up existing test databases
Write-Host "Cleaning up test databases..." -ForegroundColor Yellow
# Remove the /badger folder if it exists
if (Test-Path "pkg\tests\badger") {
    Write-Host "Removing pkg\tests\badger folder..." -ForegroundColor Yellow
    Remove-Item -Path "pkg\tests\badger" -Recurse -Force -ErrorAction SilentlyContinue
}
# Remove the migration_test.yaml file if it exists
Remove-Item -Path "pkg\tests\migration_test.yaml" -ErrorAction SilentlyContinue
# remove the spectra.db file if it exists
Remove-Item -Path "spectra.db" -ErrorAction SilentlyContinue
Write-Host "Cleanup complete" -ForegroundColor Green
Write-Host ""

# Run the test
$startTime = Get-Date

# Execute normal test runner
go run pkg/tests/normal/main.go

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

