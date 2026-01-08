# restore.ps1
# Restores permissions to A/items folder

$BaseDir = "$env:USERPROFILE\sylos_retry_test"
$TreeA   = "$BaseDir\A\items"

$Icacls = "$env:SystemRoot\System32\icacls.exe"

if (-not (Test-Path $Icacls)) {
    Write-Host "icacls.exe not found at expected path: $Icacls" -ForegroundColor Red
    exit 1
}

Write-Host "Restoring access to A/items..." -ForegroundColor Cyan

# Restore permissions on A/items if it exists
if (Test-Path $TreeA) {
    & $Icacls $TreeA /remove:d "$($env:USERNAME)" | Out-Null
    & $Icacls $TreeA /grant "$($env:USERNAME):(OI)(CI)F" | Out-Null
    & $Icacls $TreeA /inheritance:e | Out-Null
    
    Write-Host "Permissions restored for A/items." -ForegroundColor Green
} else {
    Write-Host "A/items folder not found at: $TreeA" -ForegroundColor Yellow
    exit 1
}
