# Sylos Migration Resumption Test Runner
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later
#
# This test verifies that migrations can be cleanly shutdown and resumed:
# 1. Starts a migration
# 2. Kills it midway through (force shutdown)
# 3. Resumes the same migration with same config
# 4. Verifies it completes successfully

Write-Host "=== Sylos Migration Resumption Test Runner ===" -ForegroundColor Cyan
Write-Host ""

# Clean up existing test databases ONLY at the start for a fresh test
# The DB will persist between Phase 1 (kill) and Phase 2 (resume)
# Final cleanup happens at the end after verification
Write-Host "Cleaning up test databases for fresh test run..." -ForegroundColor Yellow
Remove-Item -Path "pkg\tests\migration_test.db" -ErrorAction SilentlyContinue
Remove-Item -Path "pkg\tests\migration_test.db-wal" -ErrorAction SilentlyContinue
Remove-Item -Path "pkg\tests\migration_test.db-shm" -ErrorAction SilentlyContinue
Remove-Item -Path "pkg\tests\migration_test.yaml" -ErrorAction SilentlyContinue
Write-Host "Cleanup complete" -ForegroundColor Green
Write-Host ""

# Phase 1: Start migration and kill it midway
Write-Host "=== Phase 1: Starting Migration (will be killed) ===" -ForegroundColor Cyan
Write-Host ""

$startTime = Get-Date

Write-Host "Starting migration..." -ForegroundColor Yellow

# Start the process with a process info object so we can kill it
$processInfo = New-Object System.Diagnostics.ProcessStartInfo
$processInfo.FileName = "go"
$processInfo.Arguments = "run pkg/tests/resumption/main.go"
$processInfo.WorkingDirectory = (Get-Location).Path
$processInfo.UseShellExecute = $false
$processInfo.CreateNoWindow = $false
$processInfo.RedirectStandardOutput = $true
$processInfo.RedirectStandardError = $true

$process = New-Object System.Diagnostics.Process
$process.StartInfo = $processInfo
$process.EnableRaisingEvents = $true

# Capture output
$outputBuilder = New-Object System.Text.StringBuilder
$errorBuilder = New-Object System.Text.StringBuilder
$outputEvent = Register-ObjectEvent -InputObject $process -EventName OutputDataReceived -Action {
    $Event.SourceEventArgs.Data | ForEach-Object {
        [void]$Event.MessageData.AppendLine($_)
        Write-Host $_ -ForegroundColor Gray
    }
} -MessageData $outputBuilder

$errorEvent = Register-ObjectEvent -InputObject $process -EventName ErrorDataReceived -Action {
    $Event.SourceEventArgs.Data | ForEach-Object {
        [void]$Event.MessageData.AppendLine($_)
        Write-Host $_ -ForegroundColor DarkGray
    }
} -MessageData $errorBuilder

$process.Start() | Out-Null
$process.BeginOutputReadLine()
$process.BeginErrorReadLine()

$processId = $process.Id
Write-Host "Migration started (PID: $processId)" -ForegroundColor Yellow
Write-Host "Waiting for migration to progress..." -ForegroundColor Yellow

# Wait for migration to make some progress (let it run for a few seconds)
Start-Sleep -Seconds 10

Write-Host ""
Write-Host "Killing migration mid-execution (sending SIGINT)..." -ForegroundColor Red

# Send SIGINT to the process (Ctrl+C equivalent)
# On Windows, this requires stopping the process group
# We'll try to close the process gracefully first
if ($process -and !$process.HasExited) {
    try {
        if ($PSVersionTable.PSVersion.Major -ge 6) {
            # PowerShell Core - can use Unix signals
            Stop-Process -Id $processId -Force
        } else {
            # Windows PowerShell
            $process.Kill()
        }
    } catch {
        Write-Host "Error killing process: $_" -ForegroundColor Yellow
    }
}

# Wait a moment for cleanup
Start-Sleep -Seconds 2

# Clean up event handlers
Unregister-Event -SourceIdentifier outputEvent.Name -ErrorAction SilentlyContinue
Unregister-Event -SourceIdentifier errorEvent.Name -ErrorAction SilentlyContinue

Write-Host "Migration process terminated. Will resume" -ForegroundColor Yellow

$endTime = Get-Date
$phase1Duration = $endTime - $startTime

Write-Host ""
Write-Host "Phase 1 Duration: $($phase1Duration.TotalSeconds) seconds" -ForegroundColor Cyan
Write-Host ""

# Verify shutdown state was saved
if (Test-Path "pkg\tests\migration_test.yaml") {
    Write-Host "YAML config file exists (suspended state saved)" -ForegroundColor Green
    
    # Read YAML to check status
    $yamlContent = Get-Content "pkg\tests\migration_test.yaml" -Raw
    if ($yamlContent -match 'state:\s*\r?\n\s*status\s*:\s*(suspended|running)') {
        Write-Host "Migration state indicates suspension or ready to resume" -ForegroundColor Green
    } else {
        Write-Host "Warning: YAML status not found or unexpected" -ForegroundColor Yellow
    }
} else {
    Write-Host "YAML config file not found - shutdown may not have saved state!" -ForegroundColor Red
    exit 1
}

if (Test-Path "pkg\tests\migration_test.db") {
    Write-Host "Database file exists (checkpoint saved)" -ForegroundColor Green
} else {
    Write-Host "Database file not found!" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "=== Phase 2: Resuming Migration ===" -ForegroundColor Cyan
Write-Host ""

# Phase 2: Resume the migration
$resumeStartTime = Get-Date

Write-Host "Starting migration with same config (should auto-resume)..." -ForegroundColor Yellow

# Run the resumption test runner (it will resume and complete)
go run pkg/tests/resumption/main.go -resume

$exitCode = $LASTEXITCODE

$resumeEndTime = Get-Date
$phase2Duration = $resumeEndTime - $resumeStartTime
$totalDuration = $resumeEndTime - $startTime

Write-Host ""
Write-Host "=== Test Summary ===" -ForegroundColor Cyan
Write-Host "Phase 1 (Initial + Kill): $([math]::Round($phase1Duration.TotalSeconds, 2)) seconds"
Write-Host "Phase 2 (Resume + Complete): $([math]::Round($phase2Duration.TotalSeconds, 2)) seconds"
Write-Host "Total Duration: $([math]::Round($totalDuration.TotalSeconds, 2)) seconds"
Write-Host ""

if ($exitCode -eq 0) {
    Write-Host "TEST PASSED - Migration successfully resumed and completed!" -ForegroundColor Green
    
    # Verify final state
    if (Test-Path "pkg\tests\migration_test.yaml") {
        $finalYaml = Get-Content "pkg\tests\migration_test.yaml" -Raw
        if ($finalYaml -match 'status:\s*completed') {
            Write-Host "Final YAML status is 'completed'" -ForegroundColor Green
        } else {
            Write-Host "Warning: Final status may not be 'completed'" -ForegroundColor Yellow
        }
    }
    
    # Clean up test databases after successful test completion
    Write-Host ""
    Write-Host "Cleaning up test databases after successful test..." -ForegroundColor Yellow
    Remove-Item -Path "pkg\tests\migration_test.db" -ErrorAction SilentlyContinue
    Remove-Item -Path "pkg\tests\migration_test.db-wal" -ErrorAction SilentlyContinue
    Remove-Item -Path "pkg\tests\migration_test.db-shm" -ErrorAction SilentlyContinue
    Remove-Item -Path "pkg\tests\migration_test.yaml" -ErrorAction SilentlyContinue
    Write-Host "Cleanup complete" -ForegroundColor Green
} else {
    Write-Host "TEST FAILED - Migration resumption did not complete successfully" -ForegroundColor Red
    Write-Host "Test databases preserved for inspection" -ForegroundColor Yellow
}

Write-Host ""

exit $exitCode

