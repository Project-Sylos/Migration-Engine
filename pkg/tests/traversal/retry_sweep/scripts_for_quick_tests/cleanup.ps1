# cleanup.ps1
# Restores permissions and removes retry-sweep test folders

$BaseDir = "$env:USERPROFILE\sylos_retry_test"
$TreeA   = "$BaseDir\A\items"

$Icacls = "$env:SystemRoot\System32\icacls.exe"

if (-not (Test-Path $Icacls)) {
    Write-Host "icacls.exe not found at expected path: $Icacls" -ForegroundColor Red
    exit 1
}


Write-Host "Cleaning up retry-sweep test artifacts..." -ForegroundColor Cyan

# Restore permissions on A/items if it exists
if (Test-Path $TreeA) {
    Write-Host "Restoring permissions on A/items..." -ForegroundColor Yellow

    & $Icacls $TreeA /remove:d "$($env:USERNAME)" | Out-Null
    & $Icacls $TreeA /grant "$($env:USERNAME):(OI)(CI)F" | Out-Null
    & $Icacls $TreeA /inheritance:e | Out-Null
}

# Remove everything
Remove-Item $BaseDir -Recurse -Force -ErrorAction SilentlyContinue

Write-Host "Cleanup complete." -ForegroundColor Green
