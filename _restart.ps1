# Restart Telegram / bot processes for this repo (worktree-safe).
param(
    [switch]$Gateway
)

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not (Test-Path (Join-Path $here "bot.py"))) {
    Write-Error "bot.py not found next to _restart.ps1: $here"
    exit 1
}

Get-CimInstance Win32_Process -Filter "Name='python.exe'" | ForEach-Object {
    $cl = $_.CommandLine
    if ($null -eq $cl) { return }
    if ($cl -like '*bot.py*' -or $cl -like '*run.py*' -or $cl -like '*gateway.telegram_bot*' -or $cl -like '*-m gateway.telegram_bot*') {
        Write-Host "Stopping PID $($_.ProcessId): $cl"
        Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    }
}
Start-Sleep -Seconds 2

if ($Gateway) {
    Write-Host "Starting gateway: python -m gateway.telegram_bot in $here"
    $proc = Start-Process python -ArgumentList '-X', 'utf8', '-m', 'gateway.telegram_bot' -WorkingDirectory $here -WindowStyle Normal -PassThru
} else {
    Write-Host "Starting bot.py in $here"
    $proc = Start-Process python -ArgumentList '-X', 'utf8', 'bot.py' -WorkingDirectory $here -WindowStyle Normal -PassThru
}
Write-Host "Started PID: $($proc.Id)"
