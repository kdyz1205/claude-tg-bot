"""
Evolve Watcher v3.0 - CLI-based session revival with evolution task queue

Uses `claude.cmd --resume <session_id>` to send revival prompts directly.
Now supports structured evolution task queue: automatically advances to next
task after detecting task completion (session goes idle).

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
import logging.handlers
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
QUEUE_FILE = os.path.join(BASE, "_evolution_queue.json")

# Target session configuration
TARGET_SESSION_ID = "e5ee4984-e0cd-4f93-b7d3-a8507bae99ac"
TARGET_CWD = r"C:\Users\alexl\Desktop\claude tg bot"
SESSIONS_DIR = Path.home() / ".claude" / "projects"

_logger = logging.getLogger("evolve_watcher")
_handler = logging.handlers.RotatingFileHandler(
    LOG_FILE, encoding="utf-8", maxBytes=5_000_000, backupCount=2  # 5 MB cap with 2 backups
)
_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S"))
_logger.addHandler(_handler)
_logger.setLevel(logging.INFO)

_SAFETY_RULES = """⚠️ 安全规则（必须遵守）：
- 绝对不要杀死任何 python 进程（bot.py、run.py、smart_evolver.py）
- 绝对不要关闭 Claude Code / claude.cmd
- 绝对不要修改网络设置、防火墙、hosts文件
- 绝对不要删除 .env、.bot.pid、.bot.lock 等关键文件
- 绝对不要运行 taskkill、Stop-Process、shutdown、netsh
- 修改代码前先用 python -m py_compile 验证语法

"""

REVIVAL_PROMPTS = [
    _SAFETY_RULES + "Continue evolving. Do not stop. Keep fixing bugs and adding capabilities.",
    _SAFETY_RULES + "CONTEXT LIMIT - resume evolution: scan code, fix bugs, add smart vision + adaptive control. Do not stop.",
    _SAFETY_RULES + "Keep going. You are not done. Fix more bugs, add more capabilities.",
]

# ─── Evolution Task Queue ────────────────────────────────────────────────────

def load_queue():
    if os.path.exists(QUEUE_FILE):
        try:
            with open(QUEUE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError, FileNotFoundError, OSError):
            pass
    return None


def save_queue(q):
    tmp = QUEUE_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(q, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, QUEUE_FILE)
    except OSError as e:
        log(f"Failed to save queue: {e}")


def get_current_task(q):
    """Return the current pending task, or None if all done."""
    idx = q.get("current_task_index", 0)
    tasks = q.get("tasks", [])
    if idx < len(tasks):
        return tasks[idx]
    return None


def advance_queue(q, completed_task_name):
    """Mark current task done, move to next."""
    idx = q.get("current_task_index", 0)
    tasks = q.get("tasks", [])
    if idx < len(tasks):
        tasks[idx]["status"] = "completed"
        tasks[idx]["completed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        completed = q.setdefault("completed_tasks", [])
    completed.append(tasks[idx].get("name", "unknown"))
    # Cap completed_tasks to prevent unbounded list growth
    if len(completed) > 200:
        q["completed_tasks"] = completed[-200:]
    q["current_task_index"] = idx + 1

    # If all done, loop back
    if q["current_task_index"] >= len(tasks):
        q["loop_count"] = q.get("loop_count", 0) + 1
        q["current_task_index"] = 0
        for t in tasks:
            t["status"] = "pending"
        log(f"All tasks completed! Starting loop #{q['loop_count']}")
        notify_tg(f"🎉 所有7个进化任务完成！开始第{q['loop_count']}轮循环进化")

    save_queue(q)
    next_task = get_current_task(q)
    return next_task


def get_task_prompt(q, context_limit=False):
    """Get the prompt for the current task. Falls back to revival prompt if no queue."""
    task = get_current_task(q)
    if task is None:
        return REVIVAL_PROMPTS[0]
    if context_limit:
        return (
            _SAFETY_RULES +
            f"CONTEXT LIMIT DETECTED - 继续进化任务队列。\n\n"
            f"当前任务：{task.get('name', '?')} (任务{task.get('id', '?')}/7)\n\n"
            f"{task.get('prompt', '')}\n\n"
            f"注意：这是续命指令，请继续完成上面的任务，完成后说 ✅任务{task.get('id', '?')}完成。"
        )
    return _SAFETY_RULES + task.get("prompt", "")


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


def send_revival_cli(state, context_limit=False):
    """Send a revival prompt via CLI --resume instead of GUI clicking.
    Uses the evolution task queue to always advance to the next task.
    """
    session_id = _find_target_session()
    claude_cmd = _find_claude_cmd()

    # Use task queue if available
    q = load_queue()
    if q:
        msg = get_task_prompt(q, context_limit=context_limit)
        task = get_current_task(q)
        task_info = f"任务{task.get('id', '?')}/7: {task.get('name', '?')}" if task else "循环进化"
    else:
        msg = REVIVAL_PROMPTS[state.get("revival_idx", 0) % len(REVIVAL_PROMPTS)]
        task_info = "generic revival"

    state["revival_idx"] = state.get("revival_idx", 0) + 1
    state["total_revivals"] += 1
    state["last_revival_time"] = time.time()

    log(f"Sending revival #{state['total_revivals']} [{task_info}] via CLI --resume {session_id[:12]}...")
    log(f"Prompt: {msg[:80]}...")

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
            response_text = ""
            try:
                output = result.stdout.strip()
                if output:
                    data = json.loads(output)
                    response_text = str(data.get("result", ""))
                    log(f"Claude response: {response_text[:200]}")
            except (json.JSONDecodeError, Exception):
                if result.stdout:
                    response_text = result.stdout
                    log(f"Raw output: {response_text[:200]}")
            state["consecutive_failures"] = 0

            # Detect task completion in Claude's response → advance queue
            if q and response_text:
                task = get_current_task(q)
                completion_markers = ["✅", "任务完成", "task complete", "completed"]
                if task and any(m in response_text.lower() for m in completion_markers):
                    log(f"Task completion detected! Advancing from task {task.get('id', '?')}: {task.get('name', '?')}")
                    next_task = advance_queue(q, task.get("name", "unknown"))
                    if next_task:
                        notify_tg(f"✅ 任务{task.get('id', '?')} [{task.get('name', '?')}] 完成！\n下一个: 任务{next_task.get('id', '?')} [{next_task.get('name', '?')}]")
                        log(f"Next task: {next_task.get('id', '?')}: {next_task.get('name', '?')}")
                    state["_force_next_task"] = True  # immediately send next task
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
        fh = open(LOCK_FILE, "w", encoding="utf-8")  # noqa: SIM115 — intentionally not using `with`; handle must outlive this function
        # Write a byte first so Windows msvcrt.locking has data to lock
        fh.write(" ")
        fh.flush()
        fh.seek(0)
        if sys.platform == "win32":
            import msvcrt
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fh.seek(0)
        fh.write(str(os.getpid()))
        fh.flush()
        return fh
    except (OSError, IOError):
        log("ERROR: Another watcher instance is already running. Exiting.")
        sys.exit(1)


def _cleanup_lock():
    """Remove stale lock file on exit so restarts work after crash."""
    try:
        if os.path.exists(LOCK_FILE):
            os.unlink(LOCK_FILE)
    except Exception:
        pass

def run_loop(interval=180):
    """Main monitoring loop -- checks every interval seconds (default 3 min)."""
    lock_fh = _acquire_singleton_lock()  # Exit if another instance running
    import atexit
    atexit.register(_cleanup_lock)
    log(f"Watcher v2.0 (CLI-based) started. Check interval: {interval}s (PID {os.getpid()})")
    log(f"Target session: {TARGET_SESSION_ID}")
    # Suppress startup spam — only log locally, don't notify user every boot
    # notify_tg(f"Evolve Watcher v2.0 started (CLI mode), checking every {interval}s")
    state = load_state()

    try:
        while True:
            try:
                status = detect_session_status()
                log(f"Status: {status} | idle_streak: {state['idle_streak']} | revivals: {state['total_revivals']}")

                # Force-send next task immediately after task completion detected
                if state.pop("_force_next_task", False):
                    log("Force-sending next evolution task after completion...")
                    time.sleep(10)  # brief pause before next task
                    state = send_revival_cli(state, context_limit=False)
                    q = load_queue()
                    if q:
                        task = get_current_task(q)
                        if task:
                            notify_tg(f"🚀 开始任务{task['id']}/7: {task['name']}")
                    save_state(state)
                    continue

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
                            # context_limit heuristic: many revivals in short time
                            is_ctx = state.get("revivals_this_hour", 0) >= 2
                            state = send_revival_cli(state, context_limit=is_ctx)
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
    if len(sys.argv) > 1 and sys.argv[1] == "queue":
        import io, sys as _sys
        _sys.stdout = io.TextIOWrapper(_sys.stdout.buffer, encoding='utf-8', errors='replace')
        q = load_queue()
        if q:
            task = get_current_task(q)
            print(f"Loop #{q.get('loop_count', 0)} | Current task index: {q['current_task_index']}")
            if task:
                print(f"  Current: [{task.get('id', '?')}] {task.get('name', '?')} ({task.get('status', '?')})")
            else:
                print("  All tasks done!")
            print(f"  Completed: {q.get('completed_tasks', [])}")
        else:
            print("No queue file found.")
    elif len(sys.argv) > 1 and sys.argv[1] == "advance":
        q = load_queue()
        if q:
            t = get_current_task(q)
            name = t.get("name", "unknown") if t else "unknown"
            next_t = advance_queue(q, name)
            print(f"Advanced past '{name}' → next: {next_t.get('name', '?') if next_t else 'all done'}")
    elif len(sys.argv) > 1 and sys.argv[1] == "reset":
        q = load_queue()
        if q:
            q["current_task_index"] = 0
            for t in q["tasks"]:
                t["status"] = "pending"
                t.pop("completed_at", None)
            q["completed_tasks"] = []
            save_queue(q)
            print("Queue reset to task 1")
    elif len(sys.argv) > 1:
        try:
            interval = int(sys.argv[1])
        except ValueError:
            print(f"Unknown command: {sys.argv[1]}")
            print("Usage: evolve_watcher.py [status|advance|reset|<interval_seconds>]")
            sys.exit(1)
        run_loop(interval)
    else:
        run_loop(180)
