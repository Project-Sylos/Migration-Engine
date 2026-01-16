# setup.ps1
# Creates two asymmetric folder trees:
#   A/items  -> access denied (SRC-like, minimal structure)
#   B/items  -> accessible (DST-like, superset structure with extra files/folders)

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

# Create asymmetric folder trees

# Create A/items (SRC)
New-Item -ItemType Directory -Path $TreeA | Out-Null
New-Item -ItemType Directory -Path "$TreeA\subfolder_a" | Out-Null
"test file 1" | Out-File "$TreeA\file1.txt"
"test file 2" | Out-File "$TreeA\subfolder_a\file2.txt"
# Only A/items has file1.txt and subfolder_a with a file

# Create B/items (DST) with a superset structure
New-Item -ItemType Directory -Path $TreeB | Out-Null
New-Item -ItemType Directory -Path "$TreeB\subfolder_a" | Out-Null
New-Item -ItemType Directory -Path "$TreeB\subfolder_b" | Out-Null
New-Item -ItemType Directory -Path "$TreeB\extra_folder" | Out-Null

"test file 1" | Out-File "$TreeB\file1.txt"
"test file 2" | Out-File "$TreeB\subfolder_a\file2.txt"
"test file 3" | Out-File "$TreeB\subfolder_b\file3.txt"
"dst only file" | Out-File "$TreeB\extra_folder\dst_extra.txt"

Write-Host "Created asymmetric folder trees: A/items (SRC) and B/items (DST)." -ForegroundColor Green

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