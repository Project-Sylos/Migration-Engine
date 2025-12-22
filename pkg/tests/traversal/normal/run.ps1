# Sylos Migration Test Runner
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

Clear-Host

Write-Host "=== Sylos Migration Test Runner ===" -ForegroundColor Cyan
Write-Host ""

# # Clean up existing test databases
Write-Host "Cleaning up test databases..." -ForegroundColor Yellow

# # Remove the BoltDB file if it exists
if (Test-Path "pkg/tests/traversal/shared/main_test.db") {
    Write-Host "Removing pkg/tests/traversal/shared/main_test.db file..." -ForegroundColor Yellow
    Remove-Item -Path "pkg/tests/traversal/shared/main_test.db" -Force -ErrorAction SilentlyContinue
}

# # Remove the migration config YAML file if it exists
if (Test-Path "pkg/tests/traversal/shared/main_test.yaml") {
    Write-Host "Removing pkg/tests/traversal/shared/main_test.yaml file..." -ForegroundColor Yellow
    Remove-Item -Path "pkg/tests/traversal/shared/main_test.yaml" -Force -ErrorAction SilentlyContinue
}

# # Remove the spectra.db file if it exists
if (Test-Path "pkg/tests/traversal/shared/spectra_test.db") {
    Write-Host "Removing pkg/tests/traversal/shared/spectra_test.db file..." -ForegroundColor Yellow
    Remove-Item -Path "pkg/tests/traversal/shared/spectra_test.db" -Force -ErrorAction SilentlyContinue
}

Write-Host "Cleanup complete" -ForegroundColor Green
Write-Host ""

# Run the test
$startTime = Get-Date

# Execute normal test runner
go run pkg/tests/traversal/normal/main.go

$exitCode = $LASTEXITCODE

$endTime = Get-Date
$duration = $endTime - $startTime

Write-Host ""
Write-Host "=== Test Summary ===" -ForegroundColor Cyan
Write-Host "Duration: $($duration.TotalSeconds) seconds"

if ($exitCode -eq 0) {
    Write-Host "Status: PASSED" -ForegroundColor Green
    
    <#
      You can optionally enable the block below
      to verify the test and Spectra DBs with the inspector.
      NOTE: This will scan the entire migration and Spectra databases (O(n) runtime),
      so it could take a while on large datasets!
    #>

    # # Run DB inspector with Spectra comparison if test passed
    # Write-Host ""
    # Write-Host "=== Running DB Inspector with Spectra Comparison ===" -ForegroundColor Cyan
    # # The normal test uses pkg/tests/normal/main_test.db (from shared.SetupTest)
    # if (Test-Path "pkg/tests/traversal/shared/main_test.db") {
    #     go run cmd/inspect_db/main.go 
    #     $inspectExitCode = $LASTEXITCODE
    #     if ($inspectExitCode -ne 0) {
    #         Write-Host "⚠️  DB Inspector reported issues (exit code: $inspectExitCode)" -ForegroundColor Yellow
    #     }
    # } else {
    #     Write-Host "⚠️  Database file not found (pkg/tests/traversal/shared/main_test.db), skipping inspection" -ForegroundColor Yellow
    # }
} else {
    Write-Host "Status: FAILED" -ForegroundColor Red
}

Write-Host ""

exit $exitCode

