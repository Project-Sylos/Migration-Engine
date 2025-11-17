# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

# Sylos Environment Setup for Windows specifically
# Sets up MSYS2, GCC, and environment variables

Write-Host "Sylos Environment Setup" -ForegroundColor Magenta

# Check if we're on Windows
if ($env:OS -ne "Windows_NT") {
    Write-Host "ERROR: This script is only meant to be run on Windows" -ForegroundColor Red
    exit 1
}

# Paths
$msys2Path = "C:\msys64"
$gccPath = "C:\msys64\mingw64\bin"
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

# Install GCC via MSYS2 (assumes MSYS2 is already installed)
function Install-GCC {
    Write-Host "Installing GCC via MSYS2..." -ForegroundColor Yellow
    
    # Check for MSYS2 mingw64 bash (this has the proper environment)
    $bashPath = "$msys2Path\mingw64\bin\bash.exe"
    if (-not (Test-Path $bashPath)) {
        # Fallback to usr/bin/bash if mingw64 bash doesn't exist
        $bashPath = "$msys2Path\usr\bin\bash.exe"
        if (-not (Test-Path $bashPath)) {
            Write-Host "MSYS2 bash not found. MSYS2 may not be properly installed." -ForegroundColor Red
            return $false
        }
    }
    
    try {
        # Create a temporary script file with all commands
        $scriptFile = [System.IO.Path]::GetTempFileName() + ".sh"
        $stdoutFile = [System.IO.Path]::GetTempFileName()
        $stderrFile = [System.IO.Path]::GetTempFileName()
        
        # Use full path to pacman to ensure it's found
        $pacmanPath = "$msys2Path\usr\bin\pacman.exe"
        # Remove shebang to avoid BOM issues - bash will execute it directly
        $scriptContent = @"
set -e
export MSYSTEM=MINGW64
export CHERE_INVOKING=1
`"$pacmanPath`" -Syu --noconfirm
`"$pacmanPath`" -S mingw-w64-x86_64-gcc --noconfirm
`"$pacmanPath`" -S base-devel --noconfirm
"@
        # Use UTF8NoBOM to avoid BOM issues that bash can't handle
        $utf8NoBom = New-Object System.Text.UTF8Encoding $false
        [System.IO.File]::WriteAllLines($scriptFile, $scriptContent, $utf8NoBom)
        
        Write-Host "Running pacman commands via MSYS2 (this may take a few minutes)..." -ForegroundColor Gray
        
        # Use bash directly with proper environment - this runs completely headless
        $env:MSYSTEM = "MINGW64"
        $env:CHERE_INVOKING = "1"
        
        # Use bash to execute the script file directly (non-interactive)
        # MSYS2 bash can handle Windows paths, so we can pass it directly
        $process = Start-Process -FilePath $bashPath -ArgumentList @(
            $scriptFile
        ) -Wait -PassThru -NoNewWindow `
            -RedirectStandardOutput $stdoutFile `
            -RedirectStandardError $stderrFile `
            -WorkingDirectory $msys2Path
        
        # Read and display output
        $stdout = Get-Content $stdoutFile -Raw -ErrorAction SilentlyContinue
        $stderr = Get-Content $stderrFile -Raw -ErrorAction SilentlyContinue
        
        if ($stdout) {
            Write-Host $stdout
        }
        if ($stderr) {
            Write-Host $stderr -ForegroundColor Yellow
        }
        
        # Check if GCC was actually installed, even if script had warnings
        $gccInstalled = Test-Path "$gccPath\x86_64-w64-mingw32-gcc.exe"
        
        # Clean up files
        Remove-Item $scriptFile -Force -ErrorAction SilentlyContinue
        Remove-Item $stdoutFile -Force -ErrorAction SilentlyContinue
        Remove-Item $stderrFile -Force -ErrorAction SilentlyContinue
        
        # If GCC is installed, consider it a success even if exit code was non-zero
        # (the BOM/shebang warning is harmless)
        if ($gccInstalled) {
            Write-Host "GCC installation completed" -ForegroundColor Green
            return $true
        }
        
        if ($process.ExitCode -ne 0) {
            Write-Host "GCC installation failed (exit code: $($process.ExitCode))" -ForegroundColor Red
            return $false
        }
        
        Write-Host "GCC installation completed" -ForegroundColor Green
        return $true
    } catch {
        Write-Host "GCC installation failed: $_" -ForegroundColor Red
        if (Test-Path $scriptFile) {
            Remove-Item $scriptFile -Force -ErrorAction SilentlyContinue
        }
        if (Test-Path $stdoutFile) {
            Remove-Item $stdoutFile -Force -ErrorAction SilentlyContinue
        }
        if (Test-Path $stderrFile) {
            Remove-Item $stderrFile -Force -ErrorAction SilentlyContinue
        }
        return $false
    }
}

# Install MSYS2 (only if it doesn't exist)
function Install-MSYS2 {
    Write-Host "Installing MSYS2..." -ForegroundColor Yellow
    
    # Check if MSYS2 directory already exists
    if (Test-Path $msys2Path) {
        Write-Host "MSYS2 directory already exists at $msys2Path" -ForegroundColor Yellow
        Write-Host "Skipping MSYS2 installation. If you need to reinstall, please remove the directory first." -ForegroundColor Yellow
        return $true
    }
    
    # Download MSYS2 installer
    $installerPath = "$env:TEMP\msys2-installer.exe"
    
    # Check if installer is already running/locked
    if (Test-Path $installerPath) {
        Write-Host "Installer file exists. Checking if it's in use..." -ForegroundColor Gray
        try {
            # Try to delete it to see if it's locked
            Remove-Item $installerPath -Force -ErrorAction Stop
            Write-Host "Removed existing installer file" -ForegroundColor Gray
        } catch {
            Write-Host "Installer file is locked by another process. Please close any MSYS2 installer windows and try again." -ForegroundColor Red
            return $false
        }
    }
    
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
            Remove-Item $installerPath -Force -ErrorAction SilentlyContinue
            return $false
        }
    } catch {
        Write-Host "Failed to install MSYS2: $_" -ForegroundColor Red
        Remove-Item $installerPath -Force -ErrorAction SilentlyContinue
        return $false
    }
    
    Start-Sleep -Seconds 3
    Remove-Item $installerPath -Force -ErrorAction SilentlyContinue
    Write-Host "MSYS2 has been installed successfully" -ForegroundColor Green
    return $true
}

# Check environment
Write-Host "Running environment checks..." -ForegroundColor Yellow

$msys2Installed = Test-MSYS2
$msys2DirExists = Test-Path $msys2Path
$gccExists = Test-Path "$gccPath\x86_64-w64-mingw32-gcc.exe"

$setupSuccess = $true

if ($msys2Installed) {
    Write-Host "MSYS2 and GCC are already installed" -ForegroundColor Green
} else {
    if ($msys2DirExists -and -not $gccExists) {
        Write-Host "MSYS2 directory exists but GCC is not installed" -ForegroundColor Yellow
        $setupSuccess = $false
    } elseif (-not $msys2DirExists) {
        Write-Host "MSYS2 not found" -ForegroundColor Red
        $setupSuccess = $false
    } else {
        Write-Host "MSYS2 found but GCC verification failed" -ForegroundColor Yellow
        $setupSuccess = $false
    }
}

# Install anything missing
if (-not $msys2Installed) {
    Write-Host ""
    
    # First, ensure MSYS2 directory exists
    if (-not $msys2DirExists) {
        if (-not (Install-MSYS2)) {
            Write-Host "Failed to install MSYS2" -ForegroundColor Red
            $setupSuccess = $false
        }
    }
    
    # Then, install GCC if it's missing
    if (-not $gccExists) {
        if (-not (Install-GCC)) {
            Write-Host "Failed to install GCC" -ForegroundColor Red
            $setupSuccess = $false
        } else {
            # Re-check if GCC now exists after installation
            Start-Sleep -Seconds 2
            $gccExists = Test-Path "$gccPath\x86_64-w64-mingw32-gcc.exe"
        }
    } else {
        # GCC exists but verification failed - try to fix by updating
        Write-Host "GCC exists but verification failed. Attempting to update..." -ForegroundColor Yellow
        if (-not (Install-GCC)) {
            $setupSuccess = $false
        }
    }
}

# Set env vars
Write-Host ""
Write-Host "Setting up env vars..." -ForegroundColor Yellow

$env:CGO_ENABLED = "1"
$env:CC = "x86_64-w64-mingw32-gcc"

# Unset DuckDB-related variables (no longer needed)
if ($env:CGO_CFLAGS) {
    Remove-Item Env:\CGO_CFLAGS -ErrorAction SilentlyContinue
    Write-Host "Unset CGO_CFLAGS (DuckDB no longer needed)" -ForegroundColor Gray
}
if ($env:CGO_LDFLAGS) {
    Remove-Item Env:\CGO_LDFLAGS -ErrorAction SilentlyContinue
    Write-Host "Unset CGO_LDFLAGS (DuckDB no longer needed)" -ForegroundColor Gray
}

# Update PATH
$currentPath = $env:PATH
if ($currentPath -notlike "*$gccPath*") {
    $env:PATH = $gccPath + ";" + $currentPath
    Write-Host "Added GCC to PATH: $gccPath" -ForegroundColor Green
}

# Verify installation
Write-Host ""
Write-Host "Verifying installation..." -ForegroundColor Yellow

$gccVerified = $false
try {
    if (Test-Path "$gccPath\x86_64-w64-mingw32-gcc.exe") {
        $gccVersion = & "$gccPath\x86_64-w64-mingw32-gcc.exe" --version 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "GCC verified. Version: $($gccVersion[0])" -ForegroundColor Green
            $gccVerified = $true
        } else {
            Write-Host "GCC verification failed (exit code: $LASTEXITCODE)" -ForegroundColor Red
            $setupSuccess = $false
        }
    } else {
        Write-Host "GCC executable not found at: $gccPath\x86_64-w64-mingw32-gcc.exe" -ForegroundColor Red
        $setupSuccess = $false
    }
} catch {
    Write-Host "GCC verification ERROR: $_" -ForegroundColor Red
    $setupSuccess = $false
}

# Only show success if everything worked
Write-Host ""
if ($setupSuccess -and $gccVerified) {
    Write-Host "Environment setup completed!" -ForegroundColor Green
    Write-Host "You are now free to develop or run Sylos!" -ForegroundColor Yellow
    Write-Host "Thank you for using our setup script!" -ForegroundColor Yellow
    Write-Host ""
    exit 0
} else {
    Write-Host "Environment setup failed!" -ForegroundColor Red
    Write-Host "Please check the errors above and try again." -ForegroundColor Yellow
    Write-Host ""
    exit 1
}