# Kill all bot.py processes
Get-WmiObject Win32_Process -Filter "Name='python.exe'" | ForEach-Object {
    if ($_.CommandLine -like '*bot.py*') {
        Write-Host "Killing PID $($_.ProcessId): $($_.CommandLine)"
        Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    }
}
Start-Sleep -Seconds 3

# Start fresh
$proc = Start-Process python -ArgumentList '-X','utf8','bot.py' -WorkingDirectory 'C:\Users\alexl\Desktop\claude tg bot' -WindowStyle Hidden -PassThru
Write-Host "Started new bot PID: $($proc.Id)"
