# setup.ps1
# Creates two identical folder trees:
#   A/items  -> access denied (SRC-like)
#   B/items  -> accessible (DST-like)

$BaseDir = "$env:USERPROFILE\sylos_retry_test"
$TreeA   = "$BaseDir\A\items"
$TreeB   = "$BaseDir\B\items"

$Icacls = "$env:SystemRoot\System32\icacls.exe"

if (-not (Test-Path $Icacls)) {
    Write-Host "icacls.exe not found at expected path: $Icacls" -ForegroundColor Red
    exit 1
}


Write-Host "Setting up retry-sweep dual-tree permission test..." -ForegroundColor Cyan

# Clean slate
Remove-Item $BaseDir -Recurse -Force -ErrorAction SilentlyContinue

# Create folder trees
$trees = @($TreeA, $TreeB)

foreach ($tree in $trees) {
    New-Item -ItemType Directory -Path $tree | Out-Null
    New-Item -ItemType Directory -Path "$tree\subfolder_a" | Out-Null
    New-Item -ItemType Directory -Path "$tree\subfolder_b" | Out-Null

    "test file 1" | Out-File "$tree\file1.txt"
    "test file 2" | Out-File "$tree\subfolder_a\file2.txt"
    "test file 3" | Out-File "$tree\subfolder_b\file3.txt"
}

Write-Host "Created identical folder trees A/items and B/items." -ForegroundColor Green

# Deny access ONLY to A/items
Write-Host "Denying access to A/items (SRC simulation)..." -ForegroundColor Yellow

& $Icacls $TreeA /inheritance:r | Out-Null
& $Icacls $TreeA /deny "$($env:USERNAME):(OI)(CI)F" | Out-Null

Write-Host "Access denied for A/items." -ForegroundColor Green
Write-Host "B/items remains accessible." -ForegroundColor Green

Write-Host ""
Write-Host "Test folder locations:" -ForegroundColor Cyan
Write-Host "  SRC (A): $TreeA" -ForegroundColor White
Write-Host "  DST (B): $TreeB" -ForegroundColor White
Write-Host ""
Write-Host "Use these paths when configuring Sylos:" -ForegroundColor Gray
Write-Host "  - Source root -> $BaseDir\A" -ForegroundColor Gray
Write-Host "  - Destination root -> $BaseDir\B" -ForegroundColor Gray
Write-Host ""

Write-Host "Ready for initial Sylos traversal." -ForegroundColor Cyan