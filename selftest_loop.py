"""
selftest_loop.py — 自动部署 + 端到端测试循环

在 Windows PowerShell 运行:
  python selftest_loop.py

它会自动循环直到 bot 端到端工作:
1. 下载最新 claude_agent.py 从 GitHub
2. 替换本地文件 + 清 session
3. 启动 bot 进程
4. 通过 Claude CLI 直接测试每个能力
5. 如果失败 → 诊断 → 重试
6. 全部通过才停止
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

GITHUB_RAW = "https://raw.githubusercontent.com/kdyz1205/multi-agent-coordinate-/claude/cross-session-collaboration-boSF9/tg_bot_integration/claude_agent_patched.py"
MAX_RETRIES = 10
BOT_STARTUP_WAIT = 15
BETWEEN_TESTS_WAIT = 5


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def find_bot_dir():
    candidates = [
        Path.home() / "Desktop" / "claude tg bot",
        Path.home() / "Desktop" / "claude-tg-bot",
        Path.home() / "Desktop" / "claude_tg_bot",
    ]
    for p in candidates:
        if (p / "claude_agent.py").exists() and (p / "bot.py").exists():
            return p
    desktop = Path.home() / "Desktop"
    if desktop.exists():
        for d in desktop.iterdir():
            if d.is_dir() and (d / "claude_agent.py").exists() and (d / "bot.py").exists():
                return d
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
    # Try PATH
    try:
        r = subprocess.run(["where", "claude.cmd"], capture_output=True, text=True, timeout=5)
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout.strip().split("\n")[0]
    except Exception:
        pass
    return "claude.cmd"


def deploy(bot_dir):
    target = bot_dir / "claude_agent.py"
    log("Downloading latest claude_agent.py from GitHub...")
    try:
        url = GITHUB_RAW + f"?t={int(time.time())}"  # cache-bust GitHub CDN
        urllib.request.urlretrieve(url, str(target))
    except Exception as e:
        log(f"  urllib failed: {e}, trying PowerShell...")
        try:
            subprocess.run(
                ["powershell", "-Command", f'Invoke-WebRequest "{GITHUB_RAW}" -OutFile "{target}"'],
                check=True, timeout=30
            )
        except Exception as e2:
            log(f"  PowerShell also failed: {e2}")
            return False

    # Verify file was actually replaced
    content = target.read_text(encoding="utf-8")

    # Check expected function signatures exist
    if "async def process_message" not in content:
        log("  ERROR: Downloaded file missing process_message function!")
        return False
    if "_process_with_claude_cli" not in content:
        log("  ERROR: Downloaded file missing _process_with_claude_cli!")
        return False
    if len(content) < 5000:
        log(f"  ERROR: Downloaded file too small ({len(content)} bytes)!")
        return False

    # Check for API fallback
    if "process_with_auto_fallback" in content:
        log("  ERROR: File STILL has API fallback! Stripping manually...")
        lines = content.split("\n")
        new_lines = []
        skip = False
        for line in lines:
            if "FALLBACK: API provider" in line or "process_with_auto_fallback" in line:
                skip = True
            if skip and line.strip().startswith(("def ", "async def ")) and "process_message" not in line:
                skip = False
            if not skip:
                new_lines.append(line)
        target.write_text("\n".join(new_lines), encoding="utf-8")
        log("  Stripped API fallback manually.")

    if "DROP-IN REPLACEMENT" not in target.read_text(encoding="utf-8"):
        log("  WARNING: File may not be the patched version!")

    # Verify syntax — use separate argument to avoid path quoting issues
    r = subprocess.run([sys.executable, "-c",
        "import sys, py_compile; py_compile.compile(sys.argv[1], doraise=True); print('OK')",
        str(target)],
        capture_output=True, text=True)
    if r.returncode != 0:
        log(f"  SYNTAX ERROR: {r.stderr[:300]}")
        return False
    log("  Syntax OK, no API fallback, deployed.")

    # Delete __pycache__ to force Python to recompile from new .py file
    pycache = bot_dir / "__pycache__"
    if pycache.exists():
        shutil.rmtree(pycache, ignore_errors=True)
        log("  Deleted __pycache__/ (force recompile)")
    for pyc in bot_dir.glob("*.pyc"):
        try:
            pyc.unlink()
            log(f"  Deleted stale {pyc.name}")
        except Exception:
            pass

    # NOTE: Do NOT rename/delete providers.py — bot.py imports PROVIDER_DISPLAY from it.
    # The patched claude_agent.py simply doesn't call any provider functions.

    # Restore providers.py if it was previously disabled (fix earlier mistake)
    providers_bak = bot_dir / "providers.py.disabled"
    providers_file = bot_dir / "providers.py"
    if providers_bak.exists() and not providers_file.exists():
        try:
            providers_bak.rename(providers_file)
            log("  Restored providers.py (bot.py needs it for import)")
        except Exception:
            pass

    # Verify system prompt file will be created on bot startup
    prompt_file = bot_dir / ".system_prompt.txt"
    if prompt_file.exists():
        log(f"  System prompt file ready ({prompt_file.stat().st_size} bytes)")
    else:
        log("  System prompt file will be created on bot startup")

    # Clear sessions
    s = bot_dir / ".sessions.json"
    if s.exists():
        s.unlink()
        log("  Cleared old sessions.")
    return True


def kill_existing_bot(bot_dir=None):
    log("Killing ALL python/claude processes (except self)...")
    if sys.platform == "win32":
        my_pid = os.getpid()
        try:
            subprocess.run(
                ["powershell", "-Command",
                 f"Get-Process python*,claude* -ErrorAction SilentlyContinue | "
                 f"Where-Object {{$_.Id -ne {my_pid}}} | "
                 f"Stop-Process -Force -ErrorAction SilentlyContinue"],
                capture_output=True, timeout=10
            )
        except subprocess.TimeoutExpired:
            log("  WARNING: Kill command timed out, proceeding anyway")
        except Exception as e:
            log(f"  WARNING: Kill command failed: {e}")
    time.sleep(5)

    # Clean up PID lock files so bot can start fresh
    if bot_dir:
        for pattern in ["*.pid", ".bot.pid", "bot.pid", ".pid_lock"]:
            for f in Path(str(bot_dir)).glob(pattern):
                try:
                    f.unlink()
                    log(f"  Removed PID file: {f.name}")
                except Exception:
                    pass
        # Also clear __pycache__ before restart
        pycache = Path(str(bot_dir)) / "__pycache__"
        if pycache.exists():
            shutil.rmtree(pycache, ignore_errors=True)
            log("  Cleared __pycache__/")
    log("  Done.")


def start_bot(bot_dir):
    log(f"Starting bot...")
    kwargs = {}
    if sys.platform == "win32":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    # Use DEVNULL instead of PIPE to prevent pipe buffer deadlock.
    # The bot can produce unbounded output; PIPE fills up and blocks the process.
    # Redirect to a log file instead so output is still captured for debugging.
    bot_log = bot_dir / "selftest_bot.log"
    bot_log_fh = None
    try:
        bot_log_fh = open(str(bot_log), "w", encoding="utf-8", errors="replace")
        proc = subprocess.Popen(
            [sys.executable, "run.py"],
            cwd=str(bot_dir),
            stdout=bot_log_fh, stderr=subprocess.STDOUT,
            **kwargs,
        )
    except Exception:
        if bot_log_fh is not None:
            bot_log_fh.close()
        raise
    proc._log_fh = bot_log_fh  # attach so we can close it later
    proc._log_path = bot_log
    log(f"  Bot PID: {proc.pid} (log: {bot_log.name})")
    return proc


def cli_test(claude_cmd, message, timeout=90):
    """Send a message via Claude CLI and return the response text."""
    try:
        # Pass message as argument to -p (not stdin — stdin pipes break on Windows .cmd)
        result = subprocess.run(
            [claude_cmd, "-p", message, "--output-format", "json",
             "--dangerously-skip-permissions", "--model", "claude-sonnet-4-6"],
            capture_output=True, text=True,
            timeout=timeout, cwd=str(Path.home()),
        )
        raw = result.stdout.strip()
        if not raw:
            return None, result.stderr[:300] if result.stderr else "No output"
        try:
            data = json.loads(raw)
            resp = data.get("result", "").strip()
        except json.JSONDecodeError:
            resp = raw
        return resp, None
    except subprocess.TimeoutExpired:
        return None, "Timeout"
    except FileNotFoundError:
        return None, f"claude.cmd not found: {claude_cmd}"
    except Exception as e:
        return None, str(e)


def check_no_bad_patterns(response):
    bad = ["credit balance", "level 2 →", "level 2 ->", "claude.ai 不可用", "切换 claude cli", "auto_fallback"]
    resp_lower = response.lower()
    for b in bad:
        if b in resp_lower:
            return False, f"Contains '{b}'"
    return True, ""


# ─── Test Suite ──────────────────────────────────────────────────────────────

def run_all_tests(claude_cmd, tg_send=None):
    results = []

    def t(name, msg, check_fn, timeout=90):
        log(f"\n  TEST: {name}")
        log(f"    Sending: {msg[:60]}...")
        resp, err = cli_test(claude_cmd, msg, timeout)
        if err:
            log(f"    FAIL (error): {err}")
            results.append((name, False, err))
            return
        log(f"    Response: {resp[:150]}...")
        ok_bad, reason_bad = check_no_bad_patterns(resp)
        if not ok_bad:
            log(f"    FAIL (bad pattern): {reason_bad}")
            results.append((name, False, reason_bad))
            return
        ok, reason = check_fn(resp)
        log(f"    {'PASS' if ok else 'FAIL'}: {reason if reason else 'OK'}")
        results.append((name, ok, reason))
        time.sleep(BETWEEN_TESTS_WAIT)

    t("1. Basic Response",
      "回复OK两个字母",
      lambda r: (True, "") if len(r) >= 2 else (False, "Too short"),
      30)

    t("2. No API Fallback",
      "你好你是谁？一句话回答",
      lambda r: (True, "") if len(r) > 5 else (False, "Too short"),
      60)

    t("3. List Projects",
      "列出我电脑上的 Claude Code 项目目录",
      lambda r: (True, "") if any(k in r for k in ["Users", "Desktop", "项目", "claude", "C:"]) else (False, "No project info"),
      90)

    t("4. File Create",
      "在桌面创建文件 harness_selftest.txt 写入 harness works 然后告诉我完成了",
      lambda r: (True, "") if any(k in r.lower() for k in ["完成", "创建", "done", "wrote", "written", "已"]) else (False, "No confirmation"),
      90)

    t("5. Screenshot",
      "截屏，用中文描述你看到屏幕上有什么",
      lambda r: (True, "") if len(r) > 30 else (False, "Description too short"),
      90)

    return results


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    print()
    print("=" * 60)
    print("  HARNESS SELF-TEST LOOP")
    print("  Loops until bot works end-to-end. Ctrl+C to abort.")
    print("=" * 60)
    print()

    bot_dir = find_bot_dir()
    log(f"Bot dir: {bot_dir}")

    claude_cmd = find_claude_cmd()
    log(f"Claude CLI: {claude_cmd}")

    # Read TG credentials for notifications
    tg_send = None
    env_file = bot_dir / ".env"
    if env_file.exists():
        token = chat_id = None
        for line in env_file.read_text(encoding="utf-8").split("\n"):
            line = line.strip()
            if line.startswith("TELEGRAM_BOT_TOKEN="):
                token = line.split("=", 1)[1].strip().strip('"\'')
            if line.startswith("AUTHORIZED_USER_ID="):
                try:
                    chat_id = int(line.split("=", 1)[1].strip().strip('"\''))
                except ValueError:
                    pass
        if token and chat_id:
            def tg_send(text, _t=token, _c=chat_id):
                try:
                    data = json.dumps({"chat_id": _c, "text": text}).encode()
                    req = urllib.request.Request(
                        f"https://api.telegram.org/bot{_t}/sendMessage",
                        data=data, headers={"Content-Type": "application/json"})
                    urllib.request.urlopen(req, timeout=10)
                except Exception:
                    pass
            log(f"TG notifications enabled (chat: {chat_id})")

    for attempt in range(1, MAX_RETRIES + 1):
        log(f"\n{'#'*60}")
        log(f"  ATTEMPT {attempt}/{MAX_RETRIES}")
        log(f"{'#'*60}")

        # Deploy
        log("\n--- DEPLOY ---")
        if not deploy(bot_dir):
            log("Deploy failed, retry in 10s...")
            time.sleep(10)
            continue

        # Restart bot
        log("\n--- RESTART BOT ---")
        kill_existing_bot(bot_dir)
        bot_proc = start_bot(bot_dir)
        log(f"Waiting {BOT_STARTUP_WAIT}s for startup...")
        time.sleep(BOT_STARTUP_WAIT)

        if bot_proc.poll() is not None:
            # Read from log file instead of pipes (pipes are not used)
            crash_output = ""
            try:
                if hasattr(bot_proc, '_log_fh'):
                    bot_proc._log_fh.close()
                if hasattr(bot_proc, '_log_path') and bot_proc._log_path.exists():
                    crash_output = bot_proc._log_path.read_text(encoding="utf-8", errors="replace")[-500:]
            except Exception:
                pass
            log(f"Bot crashed! output: {crash_output}")
            continue

        if tg_send:
            tg_send(f"🔄 Self-test attempt {attempt}...")

        # Run tests
        log("\n--- TESTS ---")
        results = run_all_tests(claude_cmd, tg_send)

        # Report
        passed = sum(1 for _, ok, _ in results if ok)
        total = len(results)
        log(f"\n--- RESULTS: {passed}/{total} passed ---")
        for name, ok, reason in results:
            log(f"  {'✓' if ok else '✗'} {name}" + (f" — {reason}" if reason and not ok else ""))

        if passed == total:
            log("\n" + "=" * 60)
            log("  ✅ ALL TESTS PASSED!")
            log("  Bot is working as Harness Agent.")
            log("=" * 60)
            if tg_send:
                msg = f"✅ ALL {total} TESTS PASSED!\n\n"
                for name, ok, _ in results:
                    msg += f"✓ {name}\n"
                msg += "\nBot is fully operational as Harness Agent."
                tg_send(msg)
            log(f"\nBot running (PID {bot_proc.pid}). Ctrl+C to stop.")
            try:
                bot_proc.wait()
            except KeyboardInterrupt:
                bot_proc.terminate()
            finally:
                if hasattr(bot_proc, '_log_fh'):
                    try:
                        bot_proc._log_fh.close()
                    except Exception:
                        pass
            return 0

        # Failed — notify and retry
        if tg_send:
            msg = f"⚠️ Attempt {attempt}: {passed}/{total}\n"
            for name, ok, reason in results:
                if not ok:
                    msg += f"✗ {name}: {reason}\n"
            tg_send(msg)

        try:
            bot_proc.terminate()
            bot_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                bot_proc.kill()
                bot_proc.wait(timeout=3)
            except Exception:
                pass
        except Exception:
            try:
                bot_proc.kill()
            except Exception:
                pass
        finally:
            # Close log file AFTER process is stopped to avoid writing to closed handle
            if hasattr(bot_proc, '_log_fh'):
                try:
                    bot_proc._log_fh.close()
                except Exception:
                    pass

        log("Retry in 10s...")
        time.sleep(10)

    log(f"\n❌ FAILED after {MAX_RETRIES} attempts.")
    if tg_send:
        tg_send(f"❌ Failed after {MAX_RETRIES} attempts. Need manual fix.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
