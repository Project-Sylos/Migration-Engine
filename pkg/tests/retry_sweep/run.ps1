# Retry Sweep Test Runner
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

Clear-Host

Write-Host "=== Retry Sweep Test Runner ===" -ForegroundColor Cyan
Write-Host ""

# Source files (user-provided pre-configured test data)
$sourceDB = "pkg/tests/shared/main.db"
$sourceYAML = "pkg/tests/shared/main.yaml"
$sourceSpectra = "pkg/tests/shared/spectra.db"

# Destination files (mutable test objects)
$destDB = "pkg/tests/shared/main_test.db"
$destYAML = "pkg/tests/shared/main_test.yaml"
$destSpectra = "pkg/tests/shared/spectra_test.db"

# Check if source files exist
if (-not (Test-Path $sourceDB)) {
    Write-Host "❌ ERROR: Source database not found: $sourceDB" -ForegroundColor Red
    Write-Host "Please provide a pre-configured test database." -ForegroundColor Yellow
    exit 1
}

if (-not (Test-Path $sourceYAML)) {
    Write-Host "⚠️  WARNING: Source YAML not found: $sourceYAML (continuing anyway)" -ForegroundColor Yellow
}

if (-not (Test-Path $sourceSpectra)) {
    Write-Host "⚠️  WARNING: Source Spectra DB not found: $sourceSpectra (continuing anyway)" -ForegroundColor Yellow
}

# Clean up previous test files
Write-Host "Cleaning up previous test files..." -ForegroundColor Yellow
if (Test-Path $destDB) {
    Remove-Item -Path $destDB -Force -ErrorAction SilentlyContinue
}
if (Test-Path $destYAML) {
    Remove-Item -Path $destYAML -Force -ErrorAction SilentlyContinue
}
if (Test-Path $destSpectra) {
    Remove-Item -Path $destSpectra -Force -ErrorAction SilentlyContinue
}

# Copy source files to destination
Write-Host "Copying test files..." -ForegroundColor Yellow
Copy-Item -Path $sourceDB -Destination $destDB -Force
Write-Host "  Copied: $sourceDB -> $destDB" -ForegroundColor Green

if (Test-Path $sourceYAML) {
    Copy-Item -Path $sourceYAML -Destination $destYAML -Force
    Write-Host "  Copied: $sourceYAML -> $destYAML" -ForegroundColor Green
}

if (Test-Path $sourceSpectra) {
    Copy-Item -Path $sourceSpectra -Destination $destSpectra -Force
    Write-Host "  Copied: $sourceSpectra -> $destSpectra" -ForegroundColor Green
}

Write-Host ""

# Run the test
$startTime = Get-Date

Write-Host "Running retry sweep test..." -ForegroundColor Cyan
go run pkg/tests/retry_sweep/main.go

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

