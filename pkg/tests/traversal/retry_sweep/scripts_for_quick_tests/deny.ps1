# deny.ps1
# Denies permissions to A/items folder

$BaseDir = "$env:USERPROFILE\sylos_retry_test"
$TreeA   = "$BaseDir\A\items"

$Icacls = "$env:SystemRoot\System32\icacls.exe"

if (-not (Test-Path $Icacls)) {
    Write-Host "icacls.exe not found at expected path: $Icacls" -ForegroundColor Red
    exit 1
}

Write-Host "Denying access to A/items..." -ForegroundColor Cyan

# Deny permissions on A/items if it exists
if (Test-Path $TreeA) {
    & $Icacls $TreeA /inheritance:r | Out-Null
    & $Icacls $TreeA /deny "$($env:USERNAME):(OI)(CI)F" | Out-Null
    
    Write-Host "Permissions denied for A/items." -ForegroundColor Green
} else {
    Write-Host "A/items folder not found at: $TreeA" -ForegroundColor Yellow
    exit 1
}
