"""
Evolve Watcher v2.0 - CLI-based session revival (no GUI clicking)

Uses `claude.cmd --resume <session_id>` to send revival prompts directly,
instead of unreliable pyautogui pixel detection and sidebar clicking.

Target session: "Build Telegram bot for re..."
  sessionId: e5ee4984-e0cd-4f93-b7d3-a8507bae99ac
  cwd: C:/Users/alexl/Desktop/claude tg bot
"""
import subprocess
import time
import os
import json
import sys
import logging
import shutil
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
USER_ID = os.getenv('AUTHORIZED_USER_ID')
BASE = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE, "_watcher_state.json")
LOG_FILE = os.path.join(BASE, "_watcher.log")
LOCK_FILE = os.path.join(BASE, "_watcher.lock")

# Target session configuration
TARGET_SESSION_ID = "e5ee4984-e0cd-4f93-b7d3-a8507bae99ac"
TARGET_CWD = r"C:\Users\alexl\Desktop\claude tg bot"
SESSIONS_DIR = Path.home() / ".claude" / "projects"

_logger = logging.getLogger("evolve_watcher")
_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S"))
_logger.addHandler(_handler)
_logger.setLevel(logging.INFO)

REVIVAL_PROMPTS = [
    "Continue evolving. Do not stop. Keep fixing bugs and adding capabilities until you surpass OpenClaw.",
    "CONTEXT LIMIT - resume evolution: scan code, fix bugs, add smart vision + adaptive control. Do not stop.",
    "Keep going. You are not done. Fix more bugs, add more capabilities. Surpass OpenClaw.",
]


def log(msg):
    _logger.info(msg)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def notify_tg(text):
    if not TOKEN or not USER_ID:
        return
    try:
        import requests
        requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={"chat_id": USER_ID, "text": text},
            timeout=10
        )
    except Exception as e:
        log(f"TG notify failed: {e}")


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {"idle_streak": 0, "total_revivals": 0, "revival_idx": 0,
            "last_revival_time": 0, "consecutive_failures": 0}


def save_state(s):
    tmp = STATE_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(s, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, STATE_FILE)
    except OSError as e:
        log(f"Failed to save state: {e}")
        try:
            os.remove(tmp)
        except OSError:
            pass


def _find_claude_cmd():
    """Find the claude.cmd executable."""
    # Check common locations
    candidates = [
        shutil.which("claude.cmd"),
        shutil.which("claude"),
    ]
    # Check well-known install paths
    home = Path.home()
    for p in [
        home / "AppData" / "Local" / "Programs" / "claude" / "claude.cmd",
        home / ".claude" / "local" / "claude.cmd",
        home / "AppData" / "Roaming" / "npm" / "claude.cmd",
    ]:
        candidates.append(str(p))

    for c in candidates:
        if c and os.path.isfile(c):
            log(f"Found claude.cmd at: {c}")
            return c
    return "claude.cmd"  # fallback, hope it's in PATH


def _find_target_session():
    """Find the target session by scanning session files.

    Looks for sessions whose cwd matches our target directory,
    or falls back to the hardcoded session ID.
    Returns the session ID string.
    """
    # Scan all session PID files for one matching our cwd
    sessions_pid_dir = Path.home() / ".claude" / "sessions"
    if sessions_pid_dir.is_dir():
        best_sid = None
        best_mtime = 0
        for sf in sessions_pid_dir.glob("*.json"):
            try:
                data = json.loads(sf.read_text(encoding="utf-8", errors="replace"))
                cwd = (data.get("cwd") or "").lower().replace("/", "\\")
                if "claude tg bot" in cwd:
                    sid = data.get("sessionId", "")
                    mtime = sf.stat().st_mtime
                    if sid and mtime > best_mtime:
                        best_sid = sid
                        best_mtime = mtime
            except Exception:
                continue
        if best_sid:
            log(f"Found session from PID files: {best_sid}")
            return best_sid

    # Scan project session files for one matching our cwd
    sessions_base = Path.home() / ".claude"
    for pattern in ["sessions/*.json", "projects/**/sessions/*.json"]:
        for sf in sessions_base.glob(pattern):
            try:
                data = json.loads(sf.read_text(encoding="utf-8", errors="replace"))
                cwd = (data.get("cwd") or "").lower().replace("/", "\\")
                if "claude tg bot" in cwd:
                    sid = data.get("sessionId", "")
                    if sid:
                        log(f"Found matching session by cwd: {sid} (cwd={data.get('cwd')})")
                        return sid
            except Exception:
                continue

    # Fallback to hardcoded
    log(f"Using hardcoded session ID: {TARGET_SESSION_ID}")
    return TARGET_SESSION_ID


def detect_session_status():
    """Detect if the target Claude session is actively running.

    Checks for recent file modifications in the project directory.
    If .py files were modified in the last 2 minutes, session is likely running.
    Returns: 'running' | 'idle'
    """
    try:
        recent_threshold = 120  # files modified in last 2 minutes = running
        now = time.time()
        project_dir = Path(TARGET_CWD)
        for f in project_dir.glob("*.py"):
            try:
                mtime = f.stat().st_mtime
                if now - mtime < recent_threshold:
                    return 'running'
            except OSError:
                continue
    except Exception:
        pass

    # Also check for active claude process
    try:
        if sys.platform == "win32":
            result = subprocess.run(
                ["powershell", "-NoProfile", "-Command",
                 "Get-Process claude -ErrorAction SilentlyContinue | "
                 "Where-Object { $_.CPU -gt 0 } | "
                 "Select-Object -ExpandProperty Id"],
                capture_output=True, text=True, timeout=5,
            )
            # If claude process exists, check if it's doing work
            # by looking at its CPU time change over 2 seconds
            pids = [p.strip() for p in result.stdout.strip().split("\n") if p.strip()]
            if pids:
                # Quick CPU delta check
                try:
                    pid = pids[0]
                    cpu1 = subprocess.run(
                        ["powershell", "-NoProfile", "-Command",
                         f"(Get-Process -Id {pid} -ErrorAction SilentlyContinue).CPU"],
                        capture_output=True, text=True, timeout=3,
                    )
                    time.sleep(2)
                    cpu2 = subprocess.run(
                        ["powershell", "-NoProfile", "-Command",
                         f"(Get-Process -Id {pid} -ErrorAction SilentlyContinue).CPU"],
                        capture_output=True, text=True, timeout=3,
                    )
                    c1 = float(cpu1.stdout.strip() or "0")
                    c2 = float(cpu2.stdout.strip() or "0")
                    if c2 - c1 > 0.5:  # CPU actively used
                        return 'running'
                except Exception:
                    pass
    except Exception:
        pass

    return 'idle'


def send_revival_cli(state):
    """Send a revival prompt via CLI --resume instead of GUI clicking."""
    session_id = _find_target_session()
    claude_cmd = _find_claude_cmd()

    msg = REVIVAL_PROMPTS[state["revival_idx"] % len(REVIVAL_PROMPTS)]
    state["revival_idx"] += 1
    state["total_revivals"] += 1
    state["last_revival_time"] = time.time()

    log(f"Sending revival #{state['total_revivals']} via CLI --resume {session_id[:12]}...")
    log(f"Prompt: {msg[:60]}...")

    try:
        result = subprocess.run(
            [claude_cmd, "--resume", session_id,
             "-p", msg,
             "--output-format", "json",
             "--dangerously-skip-permissions"],
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout for Claude to respond
            cwd=TARGET_CWD,
            encoding="utf-8",
            errors="replace",
        )

        if result.returncode == 0:
            log(f"Revival sent successfully (exit code 0)")
            try:
                output = result.stdout.strip()
                if output:
                    data = json.loads(output)
                    result_text = str(data.get("result", ""))[:200]
                    log(f"Claude response: {result_text}")
            except (json.JSONDecodeError, Exception):
                if result.stdout:
                    log(f"Raw output: {result.stdout[:200]}")
            state["consecutive_failures"] = 0
        else:
            log(f"Revival command failed (exit code {result.returncode})")
            if result.stderr:
                log(f"stderr: {result.stderr[:300]}")
            state["consecutive_failures"] = state.get("consecutive_failures", 0) + 1
    except subprocess.TimeoutExpired:
        log("Revival command timed out (5 min) -- Claude may still be processing")
        state["consecutive_failures"] = 0  # timeout is not a failure, Claude is working
    except FileNotFoundError:
        log(f"ERROR: claude.cmd not found at '{claude_cmd}'. Install Claude CLI or fix PATH.")
        state["consecutive_failures"] = state.get("consecutive_failures", 0) + 1
    except Exception as e:
        log(f"Revival error: {e}")
        state["consecutive_failures"] = state.get("consecutive_failures", 0) + 1

    return state


def _acquire_singleton_lock():
    """Prevent multiple watcher instances from running simultaneously.
    Returns the lock file handle (must stay open) or exits if another instance is running."""
    try:
        # Open lock file — keep handle open for entire process lifetime
        fh = open(LOCK_FILE, "w", encoding="utf-8")
        if sys.platform == "win32":
            import msvcrt
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fh.write(str(os.getpid()))
        fh.flush()
        return fh
    except (OSError, IOError):
        log("ERROR: Another watcher instance is already running. Exiting.")
        sys.exit(1)


def run_loop(interval=180):
    """Main monitoring loop -- checks every interval seconds (default 3 min)."""
    lock_fh = _acquire_singleton_lock()  # Exit if another instance running
    log(f"Watcher v2.0 (CLI-based) started. Check interval: {interval}s (PID {os.getpid()})")
    log(f"Target session: {TARGET_SESSION_ID}")
    notify_tg(f"Evolve Watcher v2.0 started (CLI mode), checking every {interval}s")
    state = load_state()

    try:
        while True:
            try:
                status = detect_session_status()
                log(f"Status: {status} | idle_streak: {state['idle_streak']} | revivals: {state['total_revivals']}")

                if status == 'running':
                    state["idle_streak"] = 0
                    state["consecutive_failures"] = 0
                    state["revivals_this_hour"] = 0  # Reset backoff when session active
                    log("Session running OK")

                else:
                    state["idle_streak"] += 1
                    log(f"Idle streak: {state['idle_streak']}")

                    # Exponential backoff: require more idle checks before each revival
                    # Revival 1: after 3 checks (9 min)
                    # Revival 2: after 6 checks (18 min)
                    # Revival 3: after 12 checks (36 min)
                    # Revival 4+: after 20 checks (60 min) — cap
                    recent_revivals = state.get("revivals_this_hour", 0)
                    if recent_revivals == 0:
                        idle_threshold = 3   # 9 min for first revival
                    elif recent_revivals == 1:
                        idle_threshold = 6   # 18 min
                    elif recent_revivals == 2:
                        idle_threshold = 12  # 36 min
                    else:
                        idle_threshold = 20  # 60 min cap

                    if state["idle_streak"] >= idle_threshold:
                        # Prevent rapid-fire revivals: minimum 5 min between attempts
                        last = state.get("last_revival_time", 0)
                        if time.time() - last < 300:
                            log("Skipping revival: too soon since last attempt (< 5 min)")
                        elif state.get("consecutive_failures", 0) >= 5:
                            log("Skipping revival: too many consecutive failures (5+). Manual intervention needed.")
                            notify_tg("Watcher: 5+ consecutive revival failures. Check CLI setup.")
                            state["consecutive_failures"] = 0  # reset to try again later
                        else:
                            state["idle_streak"] = 0
                            state = send_revival_cli(state)
                            # Track revivals in the last hour for backoff
                            state["revivals_this_hour"] = recent_revivals + 1
                            state["last_hour_reset"] = state.get("last_hour_reset", time.time())
                            # Reset hourly counter
                            if time.time() - state.get("last_hour_reset", 0) > 3600:
                                state["revivals_this_hour"] = 1
                                state["last_hour_reset"] = time.time()
                            # Only notify TG every 10th revival to avoid spam flood
                            total = state["total_revivals"]
                            if total <= 3 or total % 10 == 0:
                                notify_tg(f"Revival #{total}: session idle {idle_threshold*3}min, prodded via CLI")
                    elif state["idle_streak"] == 1:
                        log("First idle detection, waiting for confirmation...")

                save_state(state)
            except Exception as e:
                log(f"ERROR: {e}")

            time.sleep(interval)
    except KeyboardInterrupt:
        log("Watcher stopped by user.")


if __name__ == "__main__":
    interval = int(sys.argv[1]) if len(sys.argv) > 1 else 180
    run_loop(interval)
