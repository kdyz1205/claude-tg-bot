"""
setup_and_start.py — One-click setup: fix auth + deploy + start bot

Run on Windows PowerShell:
  cd "C:\\Users\\alexl\\Desktop\\claude tg bot"
  python setup_and_start.py
"""
import os
import sys
import json
import time
import shutil
import subprocess
import urllib.request
from pathlib import Path
from datetime import datetime


GITHUB_BASE = "https://raw.githubusercontent.com/kdyz1205/multi-agent-coordinate-/claude/cross-session-collaboration-boSF9/tg_bot_integration"


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def find_bot_dir():
    candidates = [
        Path.home() / "Desktop" / "claude tg bot",
        Path.home() / "Desktop" / "claude-tg-bot",
        Path.home() / "Desktop" / "claude_tg_bot",
        Path.cwd(),
    ]
    for p in candidates:
        if (p / "bot.py").exists():
            return p
    print("Bot directory not found. Enter full path:")
    return Path(input("> ").strip().strip('"'))


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


def test_cli_auth(claude_cmd):
    """Test if claude -p mode is authenticated. Returns (ok, response)."""
    try:
        r = subprocess.run(
            [claude_cmd, "-p", "reply with just the word AUTHENTICATED",
             "--output-format", "json"],
            capture_output=True, text=True, timeout=30,
            cwd=str(Path.home()),
        )
        raw = r.stdout.strip()
        if not raw:
            return False, f"No output (stderr: {r.stderr[:200]})"
        try:
            data = json.loads(raw)
            result = data.get("result", "")
            if "not logged in" in result.lower():
                return False, "Not logged in"
            if data.get("is_error"):
                return False, result
            return True, result
        except json.JSONDecodeError:
            if "not logged in" in raw.lower():
                return False, "Not logged in"
            return False, raw[:200]
    except subprocess.TimeoutExpired:
        return False, "Timeout"
    except Exception as e:
        return False, str(e)


def fix_auth(claude_cmd):
    """Try to fix -p mode authentication."""
    log("")
    log("=" * 50)
    log("  STEP 1: Fix Claude CLI Authentication")
    log("=" * 50)
    log("")

    # Check credentials file
    cred_file = Path.home() / ".claude" / ".credentials.json"
    log(f"Checking {cred_file}...")
    if cred_file.exists():
        log(f"  Found! ({cred_file.stat().st_size} bytes)")
    else:
        log("  NOT FOUND (this is the known bug)")

    # List auth-related files
    claude_dir = Path.home() / ".claude"
    if claude_dir.exists():
        log(f"\nFiles in {claude_dir}:")
        for f in sorted(claude_dir.iterdir()):
            if f.is_file():
                log(f"  {f.name} ({f.stat().st_size} bytes)")

    # Test current auth
    log("\nTesting -p mode auth...")
    ok, msg = test_cli_auth(claude_cmd)
    if ok:
        log(f"  ALREADY WORKING! Response: {msg[:100]}")
        return True

    log(f"  FAILED: {msg}")
    log("")
    log("-" * 50)
    log("Claude CLI -p mode is not authenticated.")
    log("This is a known bug in Claude Code v2.1.76+")
    log("-" * 50)
    log("")
    log("Please complete the login now:")
    log("  1. A browser window will open")
    log("  2. Log in with your Claude account")
    log("  3. Come back here when done")
    log("")

    # Run claude /login
    try:
        subprocess.run([claude_cmd, "/login"], timeout=120)
    except subprocess.TimeoutExpired:
        log("Login timed out after 2 minutes")
    except Exception as e:
        log(f"Login error: {e}")

    # Test again
    log("\nRe-testing -p mode auth...")
    ok, msg = test_cli_auth(claude_cmd)
    if ok:
        log(f"  SUCCESS! Response: {msg[:100]}")
        return True

    log(f"  Still failing: {msg}")
    log("")
    log("If login completed but -p mode still fails, try:")
    log("  1. Close ALL Claude Code windows")
    log("  2. Run: claude /logout")
    log("  3. Run: claude /login")
    log("  4. Run this script again")
    return False


def deploy(bot_dir):
    """Download and deploy the patched claude_agent.py."""
    log("")
    log("=" * 50)
    log("  STEP 2: Deploy Patched Files")
    log("=" * 50)
    log("")

    target = bot_dir / "claude_agent.py"
    url = f"{GITHUB_BASE}/claude_agent_patched.py?t={int(time.time())}"

    log("Downloading claude_agent_patched.py...")
    try:
        urllib.request.urlretrieve(url, str(target))
    except Exception as e:
        log(f"  Download failed: {e}")
        try:
            subprocess.run(
                ["powershell", "-Command",
                 f'Invoke-WebRequest "{GITHUB_BASE}/claude_agent_patched.py" -OutFile "{target}"'],
                check=True, timeout=30)
        except Exception as e2:
            log(f"  PowerShell also failed: {e2}")
            return False

    # Verify content
    content = target.read_text(encoding="utf-8")
    checks = {
        "Has process_message": "async def process_message" in content,
        "Has _process_with_claude_cli": "_process_with_claude_cli" in content,
        "No API fallback": "process_with_auto_fallback" not in content,
        "Has DROP-IN REPLACEMENT": "DROP-IN REPLACEMENT" in content,
        "File size OK": len(content) > 5000,
    }
    all_ok = True
    for name, ok in checks.items():
        status = "OK" if ok else "FAIL"
        log(f"  {status}: {name}")
        if not ok:
            all_ok = False

    if not all_ok:
        log("  WARNING: Some checks failed!")
        return False

    # Clear __pycache__
    pycache = bot_dir / "__pycache__"
    if pycache.exists():
        shutil.rmtree(pycache, ignore_errors=True)
        log("  Cleared __pycache__/")

    # Restore providers.py if disabled
    prov_bak = bot_dir / "providers.py.disabled"
    prov = bot_dir / "providers.py"
    if prov_bak.exists() and not prov.exists():
        prov_bak.rename(prov)
        log("  Restored providers.py")

    # Clear sessions
    s = bot_dir / ".sessions.json"
    if s.exists():
        s.unlink()
        log("  Cleared old sessions")

    log("  Deployed successfully!")
    return True


def kill_existing(bot_dir):
    """Kill existing bot processes."""
    log("\nKilling existing bot processes...")
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

    # Clean PID files
    for pattern in ["*.pid", ".bot.pid", ".pid_lock"]:
        for f in bot_dir.glob(pattern):
            try:
                f.unlink()
            except Exception:
                pass
    log("  Done.")


def start_bot(bot_dir):
    """Start the bot."""
    log("")
    log("=" * 50)
    log("  STEP 3: Start Bot")
    log("=" * 50)
    log("")

    kill_existing(bot_dir)

    log("Starting bot (python run.py)...")
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
    log("=" * 50)
    log("  Bot is running. Send a message in Telegram!")
    log("  Press Ctrl+C to stop.")
    log("=" * 50)

    try:
        proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
        log("\nBot stopped.")


def main():
    print()
    print("=" * 60)
    print("  CLAUDE TG BOT — SETUP & START")
    print("  Fixes auth, deploys code, starts bot")
    print("=" * 60)
    print()

    bot_dir = find_bot_dir()
    log(f"Bot dir: {bot_dir}")

    claude_cmd = find_claude_cmd()
    if not claude_cmd:
        log("ERROR: Claude CLI not found!")
        log("Install: npm install -g @anthropic-ai/claude-code")
        return 1
    log(f"Claude CLI: {claude_cmd}")

    # Step 1: Fix auth
    auth_ok = fix_auth(claude_cmd)
    if not auth_ok:
        log("")
        log("Auth not fixed. Bot will show login instructions in Telegram.")
        log("Continuing with deployment anyway...")

    # Step 2: Deploy
    if not deploy(bot_dir):
        log("Deploy failed!")
        return 1

    # Step 3: Start
    start_bot(bot_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
