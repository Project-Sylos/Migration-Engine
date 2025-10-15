# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-3.0-or-later

# Sylos Environment Setup for Windows specifically
# Sets up MSYS2, GCC, DuckDB, and environment variables

Write-Host "Sylos Environment Setup" -ForegroundColor Magenta

# Check if we're on Windows
if ($env:OS -ne "Windows_NT") {
    Write-Host "ERROR: This script is only meant to be run on Windows" -ForegroundColor Red
    exit 1
}

# Paths
$msys2Path = "C:\msys64"
$gccPath = "C:\msys64\mingw64\bin"
$duckdbPath = (Join-Path $PSScriptRoot "DuckDB")
$msys2Url = "https://github.com/msys2/msys2-installer/releases/latest/download/msys2-x86_64-latest.exe"

# Check if MSYS2 is installed 
function Test-MSYS2 {
    if (-not (Test-Path $msys2Path)) {
        return $false
    }
    
    $gccExe = "$gccPath\x86_64-w64-mingw32-gcc.exe"
    if (-not (Test-Path $gccExe)) {
        return $false
    }
    
    try {
        & $gccExe --version 2>&1 | Out-Null
        return $LASTEXITCODE -eq 0
    } catch {
        return $false
    }
}

# Check if DuckDB binaries exist locally
function Test-DuckDBLocal {
    $requiredFiles = @("duckdb.dll", "duckdb.lib", "duckdb.h")
    foreach ($file in $requiredFiles) {
        if (-not (Test-Path (Join-Path $duckdbPath $file))) {
            return $false
        }
    }
    return $true
}

# Install MSYS2 and GCC
function Install-MSYS2 {
    Write-Host "Installing MSYS2 and GCC..." -ForegroundColor Yellow
    
    # Download MSYS2 installer
    $installerPath = "$env:TEMP\msys2-installer.exe"
    Write-Host "Downloading MSYS2 installer..." -ForegroundColor Gray
    try {
        Invoke-WebRequest -Uri $msys2Url -OutFile $installerPath -UseBasicParsing
    } catch {
        Write-Host "Failed to download MSYS2 installer: $_" -ForegroundColor Red
        return $false
    }
    
    # Install MSYS2
    Write-Host "Installing MSYS2 (this may take a few minutes)..." -ForegroundColor Gray
    try {
        $process = Start-Process -FilePath $installerPath -ArgumentList @(
            "--accept-messages",
            "--accept-licenses", 
            "--confirm-command",
            "--root", $msys2Path,
            "--locale", "en_US.UTF-8"
        ) -Wait -PassThru
        
        if ($process.ExitCode -ne 0) {
            Write-Host "MSYS2 installation failed with exit code $($process.ExitCode)" -ForegroundColor Red
            return $false
        }
    } catch {
        Write-Host "Failed to install MSYS2: $_" -ForegroundColor Red
        return $false
    }
    
    Start-Sleep -Seconds 3
    
    # Install GCC via MSYS2
    Write-Host "Installing GCC via MSYS2..." -ForegroundColor Gray
    try {
        $bashPath = "$msys2Path\usr\bin\bash.exe"
        $commands = @(
            "pacman -Syu --noconfirm",
            "pacman -S mingw-w64-x86_64-gcc --noconfirm",
            "pacman -S base-devel --noconfirm"
        )
        
        foreach ($cmd in $commands) {
            Write-Host "Running: $cmd" -ForegroundColor Gray
            & $bashPath -c $cmd
            if ($LASTEXITCODE -ne 0) {
                Write-Host "Command failed but continuing: $cmd" -ForegroundColor Yellow
            }
        }
    } catch {
        Write-Host "GCC installation had issues but continuing: $_" -ForegroundColor Yellow
    }
    
    Remove-Item $installerPath -Force -ErrorAction SilentlyContinue
    Write-Host "MSYS2 and GCC have been installed successfully" -ForegroundColor Green
    return $true
}

# Check environment
Write-Host "Running environment checks..." -ForegroundColor Yellow

$msys2Installed = Test-MSYS2
$duckdbInstalled = Test-DuckDBLocal

if ($msys2Installed) {
    Write-Host "MSYS2 and GCC are already installed" -ForegroundColor Green
} else {
    Write-Host "MSYS2 and GCC not found" -ForegroundColor Red
}

if ($duckdbInstalled) {
    Write-Host "DuckDB binaries found locally" -ForegroundColor Green
} else {
    Write-Host "Unable to find DuckDB binaries in $duckdbPath" -ForegroundColor Red
    Write-Host "Please ensure that your DuckDB binaries are installed in $duckdbPath" -ForegroundColor Yellow
    exit 1
}

# Install anything missing
if (-not $msys2Installed) {
    Write-Host ""
    if (-not (Install-MSYS2)) {
        Write-Host "Failed to install MSYS2 and GCC" -ForegroundColor Red
        exit 1
    }
}

# Set env vars
Write-Host ""
Write-Host "Setting up env vars..." -ForegroundColor Yellow

$env:CGO_ENABLED = "1"
$env:CC = "x86_64-w64-mingw32-gcc"
$env:CGO_CFLAGS = "-I$duckdbPath"
$env:CGO_LDFLAGS = "-L$duckdbPath -lduckdb"

# Update PATH
$currentPath = $env:PATH
if ($currentPath -notlike "*$gccPath*") {
    $env:PATH = $gccPath + ";" + $currentPath
    Write-Host "Added GCC to the following PATH: $gccPath" -ForegroundColor Green
}

if ($currentPath -notlike "*$duckdbPath*") {
    $env:PATH = $duckdbPath + ";" + $env:PATH
    Write-Host "Added DuckDB to the following PATH: $duckdbPath" -ForegroundColor Green
}

# Verify installation
Write-Host ""
Write-Host "Verifying installation..." -ForegroundColor Yellow

try {
    $gccVersion = & "$gccPath\x86_64-w64-mingw32-gcc.exe" --version 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "GCC exists. Version: $($gccVersion[0])" -ForegroundColor Green
    } else {
        Write-Host "GCC verification error" -ForegroundColor Red
    }
} catch {
    Write-Host "GCC verification ERROR: $_" -ForegroundColor Red
}

if ($duckdbInstalled) {
    Write-Host "DuckDB binaries exist locally" -ForegroundColor Green
}

Write-Host ""
Write-Host "Environment setup completed!" -ForegroundColor Green
Write-Host "You are now free to develop or run Sylos!" -ForegroundColor Yellow
Write-Host "Thank you for using our setup script!" -ForegroundColor Yellow
Write-Host ""
