# Start Jarvis bot from repo root. Requires .env with TELEGRAM_BOT_TOKEN.
$ErrorActionPreference = "Stop"
$Root = $PSScriptRoot
Set-Location $Root

$envFile = Join-Path $Root ".env"
if (-not (Test-Path $envFile)) {
    $example = Join-Path $Root ".env.example"
    if (Test-Path $example) {
        Copy-Item $example $envFile
        Write-Host ""
        Write-Host "Created .env from .env.example. Edit and set:" -ForegroundColor Yellow
        Write-Host "  TELEGRAM_BOT_TOKEN=   (from @BotFather)" -ForegroundColor Yellow
        Write-Host "  AUTHORIZED_USER_ID=   (numeric id, e.g. @userinfobot)" -ForegroundColor Yellow
        Write-Host "Open: notepad $envFile"
        Write-Host "Then run again: .\start_bot.ps1"
        exit 1
    }
    Write-Host "Missing .env and .env.example" -ForegroundColor Red
    exit 1
}

$tokOk = $false
Get-Content $envFile -ErrorAction SilentlyContinue | ForEach-Object {
    if ($_ -match '^\s*TELEGRAM_BOT_TOKEN\s*=\s*(.+)$') {
        if ($matches[1].Trim().Length -ge 20) { $tokOk = $true }
    }
}
if (-not $tokOk) {
    Write-Host "Fill TELEGRAM_BOT_TOKEN in .env (real token from @BotFather), then run again." -ForegroundColor Red
    exit 1
}

Write-Host "Starting python run.py ..." -ForegroundColor Green
& python (Join-Path $Root "run.py")
