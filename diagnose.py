"""
diagnose.py — 发送完整诊断信息到 Telegram
运行: python diagnose.py
"""
import sys
import json
import subprocess
import urllib.request
from pathlib import Path
from datetime import datetime


def find_bot_dir():
    candidates = [
        Path.home() / "Desktop" / "claude tg bot",
        Path.home() / "Desktop" / "claude-tg-bot",
        Path.home() / "Desktop" / "claude_tg_bot",
    ]
    for p in candidates:
        if (p / "bot.py").exists():
            return p
    desktop = Path.home() / "Desktop"
    if desktop.exists():
        for d in desktop.iterdir():
            if d.is_dir() and (d / "bot.py").exists():
                return d
    return None


def find_claude_cmd():
    candidates = [
        Path.home() / "AppData" / "Roaming" / "npm" / "claude.cmd",
        Path(r"C:\Users\alexl\AppData\Roaming\npm\claude.cmd"),
    ]
    for p in candidates:
        if p.exists():
            return str(p)
    try:
        r = subprocess.run(["where", "claude.cmd"], capture_output=True, text=True, timeout=5)
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout.strip().split("\n")[0]
    except Exception:
        pass
    try:
        r = subprocess.run(["where", "claude"], capture_output=True, text=True, timeout=5)
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout.strip().split("\n")[0]
    except Exception:
        pass
    return None


def tg_send(token, chat_id, text):
    """Send message to Telegram, split if needed."""
    chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
    for chunk in chunks:
        try:
            data = json.dumps({"chat_id": chat_id, "text": chunk}).encode()
            req = urllib.request.Request(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data=data, headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10)
        except Exception as e:
            print(f"TG send failed: {e}")


def run_cli(claude_cmd, message, timeout=60):
    """Run Claude CLI and return full result."""
    try:
        r = subprocess.run(
            [claude_cmd, "-p", message, "--output-format", "json",
             "--dangerously-skip-permissions"],
            capture_output=True, text=True, timeout=timeout,
            cwd=str(Path.home()),
        )
        return {
            "returncode": r.returncode,
            "stdout": r.stdout[:2000] if r.stdout else "(empty)",
            "stderr": r.stderr[:1000] if r.stderr else "(empty)",
        }
    except subprocess.TimeoutExpired:
        return {"returncode": -1, "stdout": "(timeout)", "stderr": ""}
    except FileNotFoundError:
        return {"returncode": -1, "stdout": "", "stderr": f"NOT FOUND: {claude_cmd}"}
    except Exception as e:
        return {"returncode": -1, "stdout": "", "stderr": str(e)}


def main():
    print("=== DIAGNOSE ===")

    # Find bot dir
    bot_dir = find_bot_dir()
    if not bot_dir:
        print("Bot directory not found!")
        return

    # Read TG credentials
    env_file = bot_dir / ".env"
    if not env_file.exists():
        print(f"No .env file at {env_file}")
        return

    token = chat_id = None
    for line in env_file.read_text(encoding="utf-8").split("\n"):
        line = line.strip()
        if line.startswith("TELEGRAM_BOT_TOKEN="):
            token = line.split("=", 1)[1].strip().strip("\"'")
        if line.startswith("AUTHORIZED_USER_ID="):
            try:
                chat_id = int(line.split("=", 1)[1].strip().strip("\"'"))
            except ValueError:
                pass

    if not token or not chat_id:
        print("Missing TELEGRAM_BOT_TOKEN or AUTHORIZED_USER_ID in .env")
        return

    report = []
    report.append("=== DIAGNOSTIC REPORT ===")
    report.append(f"Time: {datetime.now()}")
    report.append(f"Bot dir: {bot_dir}")
    report.append(f"Python: {sys.version}")
    report.append("")

    # Check claude_agent.py
    ca = bot_dir / "claude_agent.py"
    if ca.exists():
        content = ca.read_text(encoding="utf-8")
        report.append(f"claude_agent.py: {len(content)} bytes")
        report.append(f"  Has 'DROP-IN REPLACEMENT': {'DROP-IN REPLACEMENT' in content}")
        report.append(f"  Has 'process_with_auto_fallback': {'process_with_auto_fallback' in content}")
        report.append(f"  Has '--append-system-prompt-file': {'--append-system-prompt-file' in content}")
        inline_check = 'append-system-prompt", _SYSTEM' in content
        report.append(f"  Has '--append-system-prompt' (inline): {inline_check}")
        report.append(f"  Has 'process_message': {'async def process_message' in content}")
        report.append(f"  Has '_process_with_claude_cli': {'_process_with_claude_cli' in content}")
    else:
        report.append(f"claude_agent.py: NOT FOUND!")

    # Check providers.py
    prov = bot_dir / "providers.py"
    prov_dis = bot_dir / "providers.py.disabled"
    report.append(f"providers.py exists: {prov.exists()}")
    report.append(f"providers.py.disabled exists: {prov_dis.exists()}")

    # Check __pycache__
    pycache = bot_dir / "__pycache__"
    if pycache.exists():
        pycs = list(pycache.glob("*.pyc"))
        report.append(f"__pycache__: {len(pycs)} files")
        for p in pycs[:5]:
            report.append(f"  {p.name} ({p.stat().st_size} bytes)")
    else:
        report.append("__pycache__: not present")

    # Check .system_prompt.txt
    sp = bot_dir / ".system_prompt.txt"
    report.append(f".system_prompt.txt exists: {sp.exists()}")
    if sp.exists():
        report.append(f"  Size: {sp.stat().st_size} bytes")

    # Check .harness_workspace/CLAUDE.md
    cm = bot_dir / ".harness_workspace" / "CLAUDE.md"
    report.append(f"CLAUDE.md exists: {cm.exists()}")

    report.append("")

    # Find Claude CLI
    claude_cmd = find_claude_cmd()
    report.append(f"Claude CLI: {claude_cmd}")
    if not claude_cmd:
        report.append("  CLAUDE CLI NOT FOUND!")
        tg_send(token, chat_id, "\n".join(report))
        return

    # Check Claude CLI version
    try:
        r = subprocess.run([claude_cmd, "--version"], capture_output=True, text=True, timeout=10)
        report.append(f"Claude version: {r.stdout.strip()}")
    except Exception as e:
        report.append(f"Claude version: error - {e}")

    report.append("")

    # Send Part 1
    tg_send(token, chat_id, "\n".join(report))
    report = []

    # Test 1: Basic response
    report.append("=== TEST 1: Basic Response ===")
    report.append("Command: claude -p '回复OK两个字母'")
    result = run_cli(claude_cmd, "回复OK两个字母", 30)
    report.append(f"Return code: {result['returncode']}")
    report.append(f"STDOUT:\n{result['stdout']}")
    if result['stderr'] != "(empty)":
        report.append(f"STDERR:\n{result['stderr']}")
    report.append("")

    tg_send(token, chat_id, "\n".join(report))
    report = []

    # Test 2: List files (requires tool use)
    report.append("=== TEST 2: List Desktop ===")
    report.append("Command: claude -p 'list files on desktop'")
    result = run_cli(claude_cmd, "list files on my desktop using dir command", 60)
    report.append(f"Return code: {result['returncode']}")
    report.append(f"STDOUT:\n{result['stdout']}")
    if result['stderr'] != "(empty)":
        report.append(f"STDERR:\n{result['stderr']}")
    report.append("")

    tg_send(token, chat_id, "\n".join(report))
    report = []

    # Test 3: Import test
    report.append("=== TEST 3: Import Test ===")
    try:
        r = subprocess.run(
            [sys.executable, "-c",
             "import sys; sys.path.insert(0, r'" + str(bot_dir) + "'); "
             "import config; import claude_agent; "
             "print('config OK'); print('claude_agent OK'); "
             "print(f'_PROMPT_FILE={claude_agent._PROMPT_FILE}'); "
             "print(f'exists={claude_agent._PROMPT_FILE.exists()}'); "
             "from providers import PROVIDER_DISPLAY; "
             "print(f'providers OK: {PROVIDER_DISPLAY}')"],
            capture_output=True, text=True, timeout=15,
            cwd=str(bot_dir))
        report.append(f"Return code: {r.returncode}")
        report.append(f"Output: {r.stdout}")
        if r.stderr:
            report.append(f"Errors: {r.stderr[:500]}")
    except Exception as e:
        report.append(f"Error: {e}")

    report.append("")

    # Test 4: Is bot process running?
    report.append("=== TEST 4: Bot Process ===")
    try:
        if sys.platform == "win32":
            r = subprocess.run(
                ["powershell", "-Command",
                 "Get-Process python* | Select-Object Id,ProcessName,CommandLine | Format-List"],
                capture_output=True, text=True, timeout=10)
            report.append(r.stdout[:1000] if r.stdout else "No python processes")
        else:
            report.append("(not on Windows)")
    except Exception as e:
        report.append(f"Error: {e}")

    tg_send(token, chat_id, "\n".join(report))
    print("Diagnostic sent to Telegram!")


if __name__ == "__main__":
    main()
