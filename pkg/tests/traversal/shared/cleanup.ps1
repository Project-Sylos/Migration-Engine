# Removes main_test artifacts from this shared folder (main_test.db, main_test.db.wal, spectra_test.db).
# Run from repo root: & ".\pkg\tests\traversal\shared\cleanup.ps1"
# Or from script dir: & "$PSScriptRoot\cleanup.ps1"

$ErrorActionPreference = "SilentlyContinue"
$dir = $PSScriptRoot
if (-not $dir) { $dir = Split-Path -Parent $MyInvocation.MyCommand.Path }

Write-Host "Cleaning up test databases..." -ForegroundColor Yellow
Remove-Item -Path "$dir\main_test.db" -Force -ErrorAction SilentlyContinue
Remove-Item -Path "$dir\main_test.db.wal" -Force -ErrorAction SilentlyContinue
Remove-Item -Path "$dir\spectra_test.db" -Force -ErrorAction SilentlyContinue
Write-Host "Cleanup complete" -ForegroundColor Green
