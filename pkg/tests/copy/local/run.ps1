# Local Copy Phase Test Runner
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

Clear-Host

Write-Host "=== Local Copy Phase Test Runner ===" -ForegroundColor Cyan
Write-Host ""

# Get user's Documents folder
$documentsPath = [Environment]::GetFolderPath("MyDocuments")
if (-not $documentsPath) {
    Write-Host "ERROR: Could not determine Documents folder path" -ForegroundColor Red
    exit 1
}

Write-Host "Source (Documents): $documentsPath" -ForegroundColor Yellow
Write-Host ""

# Create temporary destination folder
# Use timestamp to avoid conflicts if script is run multiple times
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$tempDestPath = Join-Path $env:TEMP "SylosCopyTest_$timestamp"

Write-Host "Creating temporary destination folder..." -ForegroundColor Yellow
Write-Host "  Path: $tempDestPath" -ForegroundColor Gray

try {
    # Create destination folder
    New-Item -ItemType Directory -Path $tempDestPath -Force | Out-Null
    if (-not (Test-Path $tempDestPath)) {
        Write-Host "ERROR: Failed to create destination folder" -ForegroundColor Red
        exit 1
    }
    Write-Host "  Created successfully" -ForegroundColor Green
} catch {
    Write-Host "ERROR: Failed to create destination folder: $_" -ForegroundColor Red
    exit 1
}

Write-Host ""

# Clean up existing test databases
Write-Host "Cleaning up test databases..." -ForegroundColor Yellow

# Remove the BoltDB file if it exists
if (Test-Path "pkg/tests/copy/shared/main_test.db") {
    Write-Host "Removing pkg/tests/copy/shared/main_test.db file..." -ForegroundColor Yellow
    Remove-Item -Path "pkg/tests/copy/shared/main_test.db" -Force -ErrorAction SilentlyContinue
}

# Remove the migration config YAML file if it exists
if (Test-Path "pkg/tests/copy/shared/main_test.yaml") {
    Write-Host "Removing pkg/tests/copy/shared/main_test.yaml file..." -ForegroundColor Yellow
    Remove-Item -Path "pkg/tests/copy/shared/main_test.yaml" -Force -ErrorAction SilentlyContinue
}

Write-Host "Cleanup complete" -ForegroundColor Green
Write-Host ""

# Set environment variables for source and destination paths
$env:SYLOS_COPY_TEST_SRC = $documentsPath
$env:SYLOS_COPY_TEST_DST = $tempDestPath

# Run the test
$startTime = Get-Date

Write-Host "Running local copy phase test..." -ForegroundColor Cyan
Write-Host ""

# Execute local test runner
go run pkg/tests/copy/local/main.go

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

# Cleanup: Delete temporary destination folder
Write-Host "Cleaning up temporary destination folder..." -ForegroundColor Yellow
try {
    if (Test-Path $tempDestPath) {
        Remove-Item -Path $tempDestPath -Recurse -Force -ErrorAction SilentlyContinue
        if (Test-Path $tempDestPath) {
            Write-Host "WARNING: Failed to delete destination folder: $tempDestPath" -ForegroundColor Yellow
            Write-Host "  You may need to delete it manually" -ForegroundColor Yellow
        } else {
            Write-Host "  Deleted successfully" -ForegroundColor Green
        }
    }
} catch {
    Write-Host "WARNING: Error during cleanup: $_" -ForegroundColor Yellow
    Write-Host "  Destination folder: $tempDestPath" -ForegroundColor Yellow
    Write-Host "  You may need to delete it manually" -ForegroundColor Yellow
}

Write-Host ""

# Clear environment variables
Remove-Item Env:\SYLOS_COPY_TEST_SRC -ErrorAction SilentlyContinue
Remove-Item Env:\SYLOS_COPY_TEST_DST -ErrorAction SilentlyContinue

exit $exitCode
