# Copies main.db to main_test.db in this shared folder (fresh test DB from template).
# Run from repo root: & ".\pkg\tests\copy\shared\setup.ps1"

$dir = $PSScriptRoot
if (-not $dir) { $dir = Split-Path -Parent $MyInvocation.MyCommand.Path }
$src = Join-Path $dir "main.db"
$dest = Join-Path $dir "main_test.db"

if (-not (Test-Path $src)) {
    Write-Host "No main.db found at $src - skipping setup (tests may generate main_test.db instead)." -ForegroundColor Yellow
    exit 0
}
Write-Host "Copying main.db to main_test.db..." -ForegroundColor Cyan
Copy-Item -Path $src -Destination $dest -Force
Write-Host "Setup complete" -ForegroundColor Green
