# Sylos Migration Test Runner - Ephemeral Mode
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

Clear-Host

Write-Host "=== Sylos Migration Test Runner (Ephemeral Mode) ===" -ForegroundColor Cyan
Write-Host ""

# Clean up existing test databases
Write-Host "Cleaning up test databases..." -ForegroundColor Yellow

# Remove the BoltDB file if it exists
if (Test-Path "pkg/tests/traversal/shared/main_test.db") {
    Write-Host "Removing pkg/tests/traversal/shared/main_test.db file..." -ForegroundColor Yellow
    Remove-Item -Path "pkg/tests/traversal/shared/main_test.db" -Force -ErrorAction SilentlyContinue
}

# Remove the migration config YAML file if it exists
if (Test-Path "pkg/tests/traversal/shared/main_test.yaml") {
    Write-Host "Removing pkg/tests/traversal/shared/main_test.yaml file..." -ForegroundColor Yellow
    Remove-Item -Path "pkg/tests/traversal/shared/main_test.yaml" -Force -ErrorAction SilentlyContinue
}

# Note: Ephemeral mode doesn't use a Spectra DB, so no cleanup needed for spectra_test.db

Write-Host "Cleanup complete" -ForegroundColor Green
Write-Host ""

# Run the test
$startTime = Get-Date

# Execute ephemeral test runner
go run pkg/tests/traversal/ephemeral/main.go

$exitCode = $LASTEXITCODE

$endTime = Get-Date
$duration = $endTime - $startTime

Write-Host ""
Write-Host "=== Test Summary ===" -ForegroundColor Cyan
Write-Host "Duration: $($duration.TotalSeconds) seconds"

if ($exitCode -eq 0) {
    Write-Host "Status: PASSED" -ForegroundColor Green
    
    <#
      Note: Ephemeral mode doesn't persist a Spectra DB, so we can't do
      cross-validation with Spectra. The test validates only the internal
      Sylos migration database (no pending nodes, no NotOnSrc nodes).
    #>
} else {
    Write-Host "Status: FAILED" -ForegroundColor Red
}

Write-Host ""

exit $exitCode
