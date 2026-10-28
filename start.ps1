$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
if (-not (Test-Path -LiteralPath '.venv/Scripts/python.exe')) { throw '请先执行 scripts/setup.ps1' }
& .venv/Scripts/python.exe scripts/launch.py @args
