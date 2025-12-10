# PowerShell script to run the unexclusion sweep test
# This script prepares the base DB (if needed) and runs the unexclusion sweep test

$ErrorActionPreference = "Stop"

Write-Host "=== Unexclusion Sweep Test Runner ===" -ForegroundColor Cyan
Write-Host ""

# Check if base DB exists, if not, prepare it
# Paths are relative to this script's location
$baseDBPath = "pkg/tests/unexclusion_sweep/unexclusion_sweep_test_base.db"
if (-not (Test-Path $baseDBPath)) {
    Write-Host "Base DB not found. Preparing base DB..." -ForegroundColor Yellow
    Write-Host "This will mark the first root folder as excluded and run an exclusion sweep." -ForegroundColor Yellow
    Write-Host ""
    
    # Run prepare script (located in preparation subfolder)
    go run pkg/tests/unexclusion_sweep/preparation/prepare_unexclusion_test_db.go
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Failed to prepare base DB" -ForegroundColor Red
        exit 1
    }
    
    Write-Host ""
}

# Clean up previous test files
Write-Host "Cleaning up previous test files..." -ForegroundColor Yellow
$testFiles = @(
    "pkg/tests/unexclusion_sweep/unexclusion_sweep_test.db",
    "pkg/tests/unexclusion_sweep/unexclusion_sweep_test.yaml",
    "pkg/tests/unexclusion_sweep/spectra.db"
)

foreach ($file in $testFiles) {
    if (Test-Path $file) {
        Remove-Item $file -Force
        Write-Host "  Removed: $file" -ForegroundColor Gray
    }
}

# Copy test files
Write-Host "Copying test files..." -ForegroundColor Yellow
$baseFiles = @{
    "pkg/tests/unexclusion_sweep/unexclusion_sweep_test_base.db" = "pkg/tests/unexclusion_sweep/unexclusion_sweep_test.db"
    "pkg/tests/unexclusion_sweep/unexclusion_sweep_test_base.yaml" = "pkg/tests/unexclusion_sweep/unexclusion_sweep_test.yaml"
    "pkg/tests/unexclusion_sweep/spectra_unexclusion_sweep_base.db" = "pkg/tests/unexclusion_sweep/spectra.db"
}

foreach ($baseFile in $baseFiles.GetEnumerator()) {
    if (-not (Test-Path $baseFile.Key)) {
        Write-Host "  ERROR: Base file not found: $($baseFile.Key)" -ForegroundColor Red
        Write-Host "  NOTE: Run the prepare script first: go run ../prepare_unexclusion_test_db.go" -ForegroundColor Yellow
        exit 1
    }
    Copy-Item $baseFile.Key $baseFile.Value -Force
    Write-Host "  Copied: $($baseFile.Key) -> $($baseFile.Value)" -ForegroundColor Gray
}

# Run the test
Write-Host ""
Write-Host "Running unexclusion sweep test..." -ForegroundColor Yellow
$startTime = Get-Date

go run pkg/tests/unexclusion_sweep/main.go

$exitCode = $LASTEXITCODE
$endTime = Get-Date
$duration = $endTime - $startTime

Write-Host ""
Write-Host "=== Test Summary ===" -ForegroundColor Cyan
Write-Host "Duration: $($duration.TotalSeconds) seconds" -ForegroundColor $(if ($exitCode -eq 0) { "Green" } else { "Red" })
Write-Host "Status: $(if ($exitCode -eq 0) { "PASSED" } else { "FAILED" })" -ForegroundColor $(if ($exitCode -eq 0) { "Green" } else { "Red" })

exit $exitCode

