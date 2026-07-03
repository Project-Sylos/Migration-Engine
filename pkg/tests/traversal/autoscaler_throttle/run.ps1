# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..\..\..")
go run ./pkg/tests/traversal/autoscaler_throttle/
