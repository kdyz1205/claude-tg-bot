"""
find_window.py - 精确找到并聚焦特定窗口
用法: python find_window.py claude_code
      python find_window.py crypto
"""
import sys
import subprocess
import time

def find_and_focus(target):
    """通过窗口标题找到并聚焦窗口"""
    keywords = {
        'claude_code': ['claude.ai/code', 'Claude Code'],
        'claude': ['claude.ai', 'Claude'],
        'crypto': ['crypto-analysis', 'localhost:800'],
        'tg_bot': ['claude tg bot', 'TG Bot'],
    }

    search_terms = keywords.get(target, [target])
    # Escape single quotes in search terms to prevent PowerShell injection
    safe_terms = [t.replace("'", "''").replace('"', '`"') for t in search_terms]

    script = f"""
Add-Type @"
using System;
using System.Runtime.InteropServices;
public class WinAPI {{
    [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr hWnd);
    [DllImport("user32.dll")] public static extern bool ShowWindow(IntPtr hWnd, int nCmdShow);
    [DllImport("user32.dll")] public static extern bool IsIconic(IntPtr hWnd);
}}
"@

$procs = Get-Process | Where-Object {{$_.MainWindowTitle -ne '' -and $_.MainWindowHandle -ne 0}}
$found = $null
foreach ($p in $procs) {{
    $title = $p.MainWindowTitle
    {chr(10).join(f'    if ($title -like "*{t}*") {{ $found = $p; break }}' for t in safe_terms)}
}}
if ($found) {{
    $hwnd = $found.MainWindowHandle
    if ([WinAPI]::IsIconic($hwnd)) {{
        [WinAPI]::ShowWindow($hwnd, 9)  # SW_RESTORE
    }}
    [WinAPI]::SetForegroundWindow($hwnd)
    Write-Output "FOCUSED:$($found.MainWindowTitle)"
}} else {{
    $titles = ($procs | Select-Object -ExpandProperty MainWindowTitle) -join "|"
    Write-Output "NOT_FOUND|$titles"
}}
"""

    result = subprocess.run(['powershell', '-Command', script],
                          capture_output=True, text=True, encoding='utf-8', errors='replace',
                          timeout=15)
    output = result.stdout.strip()
    print(output)
    return output.startswith('FOCUSED:')


def get_all_windows():
    """列出所有可见窗口"""
    script = """
$procs = Get-Process | Where-Object {$_.MainWindowTitle -ne '' -and $_.MainWindowHandle -ne 0}
foreach ($p in $procs) {
    Write-Output "$($p.Id)|$($p.ProcessName)|$($p.MainWindowTitle)"
}
"""
    result = subprocess.run(['powershell', '-Command', script],
                          capture_output=True, text=True, encoding='utf-8', errors='replace',
                          timeout=15)
    windows = []
    for line in result.stdout.strip().split('\n'):
        if '|' in line:
            parts = line.split('|', 2)
            if len(parts) == 3:
                windows.append({'pid': parts[0], 'proc': parts[1], 'title': parts[2].strip()})
    return windows


def focus_chrome_with_url(url_fragment):
    """找到含特定URL的Chrome窗口并聚焦"""
    # Chrome stores its window title as "[Page Title] - Google Chrome"
    # For claude.ai/code it would be "Claude Code - Google Chrome" or similar
    # Escape single quotes to prevent PowerShell injection
    safe_fragment = url_fragment.replace("'", "''").replace('"', '`"')
    script = f"""
Add-Type @"
using System;
using System.Runtime.InteropServices;
public class WinAPI2 {{
    [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr hWnd);
    [DllImport("user32.dll")] public static extern bool ShowWindow(IntPtr hWnd, int nCmdShow);
}}
"@

$chrome = Get-Process chrome -ErrorAction SilentlyContinue | Where-Object {{$_.MainWindowTitle -ne ''}}
$found = $null
foreach ($w in $chrome) {{
    if ($w.MainWindowTitle -like "*{safe_fragment}*") {{
        $found = $w
        break
    }}
}}
if ($found) {{
    [WinAPI2]::SetForegroundWindow($found.MainWindowHandle)
    Write-Output "FOCUSED:$($found.MainWindowTitle)"
}} else {{
    $titles = ($chrome | Select-Object -ExpandProperty MainWindowTitle) -join " || "
    Write-Output "NOT_FOUND. Chrome windows: $titles"
}}
"""
    result = subprocess.run(['powershell', '-Command', script],
                          capture_output=True, text=True, encoding='utf-8', errors='replace',
                          timeout=15)
    output = result.stdout.strip()
    print(output)
    return output.startswith('FOCUSED:')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python find_window.py <target>")
        print("Targets: claude_code, claude, crypto, tg_bot, or any keyword")
        print("\nAll windows:")
        for w in get_all_windows():
            print(f"  [{w['proc']}] {w['title']}")
        sys.exit(0)

    target = sys.argv[1]

    if target == 'list':
        for w in get_all_windows():
            print(f"  [{w['proc']}] {w['title']}")
    elif target == 'claude_code':
        # Try Chrome window with Claude Code
        success = focus_chrome_with_url('Claude Code')
        if not success:
            success = focus_chrome_with_url('claude.ai')
        if not success:
            print("Claude Code not found in Chrome. Opening new window...")
            subprocess.Popen(['start', 'chrome', '--new-window', 'https://claude.ai/code'], shell=True)
            time.sleep(3)
    else:
        find_and_focus(target)
