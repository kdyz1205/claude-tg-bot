"""
deploy_no_api.py — 一键部署：彻底删除所有 API 调用

在 PowerShell 运行:
  cd "C:\\Users\\alexl\\Desktop\\claude tg bot"
  python deploy_no_api.py
"""
import os
import sys
import re
import json
import shutil
import subprocess
import urllib.request
import time
from pathlib import Path
from datetime import datetime


GITHUB_BASE = "https://raw.githubusercontent.com/kdyz1205/multi-agent-coordinate-/claude/cross-session-collaboration-boSF9/tg_bot_integration"


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def find_bot_dir():
    candidates = [
        Path.cwd(),
        Path.home() / "Desktop" / "claude tg bot",
        Path.home() / "Desktop" / "claude-tg-bot",
    ]
    for p in candidates:
        if (p / "bot.py").exists():
            return p
    print("Bot directory not found. Enter full path:")
    return Path(input("> ").strip().strip('"'))


def main():
    print()
    print("=" * 55)
    print("  DEPLOY: Remove ALL API calls from bot")
    print("=" * 55)
    print()

    bot_dir = find_bot_dir()
    log(f"Bot dir: {bot_dir}")

    # Step 1: Kill all python processes (except self)
    log("\n--- STEP 1: Kill existing processes ---")
    if sys.platform == "win32":
        my_pid = os.getpid()
        try:
            subprocess.run(
                ["powershell", "-Command",
                 f"Get-Process python*,claude* -ErrorAction SilentlyContinue | "
                 f"Where-Object {{$_.Id -ne {my_pid}}} | "
                 f"Stop-Process -Force -ErrorAction SilentlyContinue"],
                capture_output=True, timeout=10)
        except Exception:
            pass
    time.sleep(3)
    log("  Done.")

    # Step 2: Download patched claude_agent.py
    log("\n--- STEP 2: Deploy patched claude_agent.py ---")
    target = bot_dir / "claude_agent.py"
    url = f"{GITHUB_BASE}/claude_agent_patched.py?t={int(time.time())}"
    try:
        urllib.request.urlretrieve(url, str(target))
        log(f"  Downloaded ({target.stat().st_size} bytes)")
    except Exception as e:
        log(f"  Download failed: {e}")
        return 1

    # Verify
    content = target.read_text(encoding="utf-8")
    if "process_with_auto_fallback" in content:
        log("  ERROR: File still has API fallback!")
        return 1
    if "DROP-IN REPLACEMENT" not in content:
        log("  ERROR: Wrong file downloaded!")
        return 1
    log("  Verified: no API fallback, correct file.")

    # Step 3: Patch bot.py to remove providers import
    log("\n--- STEP 3: Patch bot.py (remove API imports) ---")
    bot_py = bot_dir / "bot.py"
    bot_bak = bot_dir / "bot.py.bak"

    # Backup — copy FIRST, then read (safer order)
    if not bot_bak.exists():
        shutil.copy2(str(bot_py), str(bot_bak))
        log("  Backed up bot.py → bot.py.bak")
        bot_content = bot_py.read_text(encoding="utf-8")
    else:
        # Read from backup to ensure clean patching
        bot_content = bot_bak.read_text(encoding="utf-8")
        log("  Using existing bot.py.bak as base")

    changes = 0

    # Replace: from providers import PROVIDER_DISPLAY → inline dict
    if "from providers import PROVIDER_DISPLAY" in bot_content:
        bot_content = bot_content.replace(
            "from providers import PROVIDER_DISPLAY",
            '# providers.py removed — inline display names\n'
            'PROVIDER_DISPLAY = {"claude": "Claude (CLI)", "openai": "OpenAI", "gemini": "Gemini"}'
        )
        changes += 1
        log("  Removed: from providers import PROVIDER_DISPLAY")

    # Replace: from providers import anything_else
    bot_content = re.sub(
        r'^from providers import .*$',
        '# (providers import removed)',
        bot_content,
        flags=re.MULTILINE
    )

    # Replace: import providers
    bot_content = re.sub(
        r'^import providers\s*$',
        '# (import providers removed)',
        bot_content,
        flags=re.MULTILINE
    )

    # Write patched bot.py
    bot_py.write_text(bot_content, encoding="utf-8")
    log(f"  Patched bot.py written ({len(bot_content)} bytes, {changes} changes)")

    # Verify no providers import remains (check each line individually)
    final = bot_py.read_text(encoding="utf-8")
    active_imports = [
        line.strip() for line in final.splitlines()
        if "from providers import" in line
        and not line.strip().startswith("#")
    ]
    if active_imports:
        log(f"  WARNING: providers import might still be active: {active_imports}")
    else:
        log("  Verified: no active providers imports.")

    # Step 4: Clean up
    log("\n--- STEP 4: Clean up ---")

    # Delete __pycache__
    pycache = bot_dir / "__pycache__"
    if pycache.exists():
        shutil.rmtree(pycache, ignore_errors=True)
        log("  Deleted __pycache__/")

    # Delete .sessions.json
    sessions = bot_dir / ".sessions.json"
    if sessions.exists():
        sessions.unlink()
        log("  Deleted .sessions.json")

    # Clean PID files
    for pattern in ["*.pid", ".bot.pid", ".pid_lock"]:
        for f in bot_dir.glob(pattern):
            try:
                f.unlink()
                log(f"  Deleted {f.name}")
            except Exception:
                pass

    # Restore providers.py if it was disabled
    prov_bak = bot_dir / "providers.py.disabled"
    prov = bot_dir / "providers.py"
    if prov_bak.exists() and not prov.exists():
        prov_bak.rename(prov)
        log("  Restored providers.py (needed for other imports)")

    # Step 5: Test CLI auth
    log("\n--- STEP 5: Test Claude CLI ---")
    claude_cmd = None
    for p in [Path.home() / "AppData" / "Roaming" / "npm" / "claude.cmd"]:
        if p.exists():
            claude_cmd = str(p)
            break
    if not claude_cmd:
        try:
            r = subprocess.run(["where", "claude.cmd"], capture_output=True, text=True, timeout=5)
            if r.returncode == 0:
                claude_cmd = r.stdout.strip().split("\n")[0]
        except Exception:
            pass

    if claude_cmd:
        try:
            r = subprocess.run(
                [claude_cmd, "-p", "reply OK", "--output-format", "json"],
                capture_output=True, text=True, timeout=30,
                cwd=str(Path.home()))
            if not r.stdout.strip():
                log(f"  CLI returned no output (stderr: {r.stderr[:200] if r.stderr else 'none'})")
            else:
                data = json.loads(r.stdout)
                result = data.get("result", "")
                if "not logged in" in result.lower():
                    log(f"  CLI AUTH FAILED: {result}")
                    log("  Run 'claude /login' first!")
                    return 1
                elif data.get("is_error"):
                    log(f"  CLI ERROR: {result}")
                else:
                    log(f"  CLI OK: {result[:100]}")
        except Exception as e:
            log(f"  CLI test failed: {e}")
    else:
        log("  Claude CLI not found!")

    # Step 6: Start bot
    log("\n--- STEP 6: Start bot ---")
    kwargs = {}
    if sys.platform == "win32":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    proc = subprocess.Popen(
        [sys.executable, "run.py"],
        cwd=str(bot_dir),
        **kwargs,
    )
    log(f"  Bot started! PID: {proc.pid}")
    log("")
    log("=" * 55)
    log("  ALL DONE. Send a message in Telegram to test!")
    log("  Press Ctrl+C to stop.")
    log("=" * 55)

    try:
        proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
    return 0


if __name__ == "__main__":
    sys.exit(main())
