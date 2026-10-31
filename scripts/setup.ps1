param([switch]$SkipDsh)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
Set-Location -LiteralPath $projectRoot
$env:UV_CACHE_DIR = Join-Path $projectRoot '.cache/uv'
if (-not (Test-Path -LiteralPath '.venv/Scripts/python.exe')) {
    uv venv --python '>=3.12' .venv
    if ($LASTEXITCODE) { throw 'Python 环境创建失败' }
}
$extras = if ($SkipDsh) { '.' } else { '.[dsh]' }
$lockPath = Join-Path $projectRoot 'requirements.lock'
if ($SkipDsh) {
    New-Item -ItemType Directory -Force .cache | Out-Null
    $lockPath = Join-Path $projectRoot '.cache/requirements-core.lock'
    Get-Content -LiteralPath (Join-Path $projectRoot 'requirements.lock') | Where-Object { $_ -notmatch '^deepseek-harness-' } | Set-Content -Encoding utf8 -LiteralPath $lockPath
}
uv pip install --python .venv/Scripts/python.exe -r $lockPath -e $extras
if ($LASTEXITCODE) { throw '后端依赖安装失败' }
Push-Location apps/web
try {
    npm.cmd ci --no-audit --no-fund
    if ($LASTEXITCODE) { throw '前端依赖安装失败' }
    npm.cmd run build
    if ($LASTEXITCODE) { throw '前端构建失败' }
} finally { Pop-Location }
Write-Host '安装完成。运行 .venv/Scripts/python.exe scripts/launch.py'
