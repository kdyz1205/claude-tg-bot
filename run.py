"""
run.py — Auto-restart wrapper for the Telegram bot.

Usage: python run.py
- Automatically restarts bot if it crashes
- Waits 5 seconds between restarts
- Stops after 10 consecutive rapid failures
- Uses PID file + lockfile to prevent multiple bot instances
- Logs all crashes to _error_log.txt
- Watchdog hot-reload: automatically restarts bot when .py files change
"""
import subprocess
import sys
import time
import os
import platform
import signal
import traceback
import threading
from datetime import datetime
if sys.platform == "win32":
    import msvcrt

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BOT_SCRIPT = os.path.join(BASE_DIR, "bot.py")
LOG_FILE = os.path.join(BASE_DIR, "bot.log")
PID_FILE = os.path.join(BASE_DIR, ".bot.pid")
LOCK_FILE = os.path.join(BASE_DIR, ".bot.lock")
ERROR_LOG_FILE = os.path.join(BASE_DIR, "_error_log.txt")
_lock_fh = None  # file handle kept open to hold the exclusive lock
MAX_RAPID_FAILURES = 10
RAPID_FAILURE_WINDOW = 60  # seconds
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
ERROR_LOG_MAX_BYTES = 5 * 1024 * 1024  # 5 MB

# Hot-reload: set to True by watchdog when .py files change
_hot_reload_pending = threading.Event()
_current_proc = None  # current bot subprocess, for watchdog to kill


def _write_error_log(message: str):
    """Append a timestamped entry to _error_log.txt, rotating if it gets too large."""
    try:
        # Rotate if too large
        if os.path.exists(ERROR_LOG_FILE) and os.path.getsize(ERROR_LOG_FILE) > ERROR_LOG_MAX_BYTES:
            backup = ERROR_LOG_FILE + ".1"
            try:
                if os.path.exists(backup):
                    os.remove(backup)
                os.rename(ERROR_LOG_FILE, backup)
            except Exception:
                pass
        with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}\n")
    except Exception:
        pass


def _start_hot_reload_watcher():
    """Start a watchdog thread that signals hot-reload when any .py file changes."""
    try:
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler

        # Files that should NOT trigger hot-reload (evolution/background tasks modify these)
        _RELOAD_IGNORE = {
            "__pycache__", ".skill_library", "skills", ".bot_memory",
            "_evolution_queue.json", "_infinite_evolver_state.json",
            "_vital_signs.json", "_signal_history.json", "_performance_stats.json",
            ".bot_scores.jsonl", ".self_heal.jsonl", ".user_language.json",
            "action_memory.json", "_smart_evolver_state.json",
        }
        # Only reload for CORE files that actually affect bot behavior
        _RELOAD_CORE = {
            "bot.py", "claude_agent.py", "config.py", "run.py", "tools.py",
            "harness_learn.py", "skill_library.py", "providers.py",
        }
        _last_reload_time = [0.0]  # mutable container for closure

        class _BotReloadHandler(FileSystemEventHandler):
            def on_modified(self, event):
                if event.is_directory or not event.src_path.endswith(".py"):
                    return
                rel = os.path.relpath(event.src_path, BASE_DIR)
                basename = os.path.basename(event.src_path)
                # Skip files in ignored directories/patterns
                for ignore in _RELOAD_IGNORE:
                    if ignore in rel:
                        return
                # Only reload for core files, skip evolution/background scripts
                if basename not in _RELOAD_CORE:
                    print(f"[HotReload] Ignoring non-core change: {rel}")
                    return
                # Debounce: no more than 1 reload per 30 seconds
                import time as _t
                now = _t.time()
                if now - _last_reload_time[0] < 30:
                    print(f"[HotReload] Debounced: {rel} (too soon after last reload)")
                    return
                _last_reload_time[0] = now
                print(f"[HotReload] Core file changed: {rel} — scheduling restart...")
                _write_error_log(f"HOT-RELOAD: {rel} changed, restarting bot")
                _hot_reload_pending.set()
                # Kill current bot process so the main loop can restart it
                if _current_proc is not None and _current_proc.poll() is None:
                    try:
                        _current_proc.terminate()
                    except Exception:
                        pass

        observer = Observer()
        observer.schedule(_BotReloadHandler(), path=BASE_DIR, recursive=False)
        observer.daemon = True
        observer.start()
        print("[HotReload] Watchdog started — will auto-restart on .py changes")
        return observer
    except ImportError:
        print("[HotReload] watchdog not installed — hot-reload disabled (pip install watchdog)")
        return None
    except Exception as e:
        print(f"[HotReload] Failed to start watchdog: {e}")
        return None


def _is_pid_alive(pid):
    """Check if a process with given PID is alive and is a Python process.
    Uses tasklist on Windows instead of os.kill(pid, 0) which sends CTRL_C_EVENT."""
    if sys.platform == "win32":
        try:
            # First check if PID exists at all (fast, no PowerShell)
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                capture_output=True, text=True, timeout=5,
            )
            if str(pid) not in result.stdout:
                return False
            # Then verify it's a python process
            result2 = subprocess.run(
                ["powershell", "-NoProfile", "-Command",
                 f"(Get-Process -Id {pid} -ErrorAction SilentlyContinue).ProcessName"],
                capture_output=True, text=True, timeout=5,
            )
            proc_name = (result2.stdout or "").strip().lower()
            return "python" in proc_name
        except Exception:
            return False
    else:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False


def _kill_existing_bot():
    """Kill any existing bot.py process found via PID file or process scan."""
    killed = False

    # 1. Check PID file
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, "r", encoding="utf-8") as f:
                old_pid = int(f.read().strip())
            if _is_pid_alive(old_pid):
                print(f"Found running bot instance (PID {old_pid}), killing it...")
                if sys.platform == "win32":
                    subprocess.run(["taskkill", "/PID", str(old_pid), "/F"],
                                   capture_output=True, timeout=5)
                else:
                    os.kill(old_pid, signal.SIGTERM)
                time.sleep(3)
                # Verify it died
                if _is_pid_alive(old_pid):
                    print(f"PID {old_pid} still alive after SIGTERM, force killing...")
                    if sys.platform == "win32":
                        subprocess.run(["taskkill", "/PID", str(old_pid), "/F", "/T"],
                                       capture_output=True, timeout=5)
                    else:
                        os.kill(old_pid, signal.SIGKILL)
                    time.sleep(2)
                killed = True
        except (ValueError, IOError):
            pass

    # 2. Scan for any other bot.py python processes (belt and suspenders)
    try:
        if sys.platform == "win32":
            # Find all python processes running bot.py
            result = subprocess.run(
                ["powershell", "-NoProfile", "-Command",
                 "Get-WmiObject Win32_Process | Where-Object { $_.Name -like 'python*' -and $_.CommandLine -like '*bot.py*' } | Select-Object ProcessId, CommandLine | Format-List"],
                capture_output=True, text=True, timeout=10,
            )
            my_pid = os.getpid()
            for line in result.stdout.split("\n"):
                if "ProcessId" in line:
                    try:
                        pid = int(line.split(":")[-1].strip())
                        if pid != my_pid and _is_pid_alive(pid):
                            print(f"Found orphan bot.py process (PID {pid}), killing...")
                            subprocess.run(["taskkill", "/PID", str(pid), "/F"],
                                           capture_output=True, timeout=5)
                            killed = True
                    except (ValueError, TypeError):
                        pass
    except Exception as e:
        print(f"Warning: process scan failed: {e}")

    # Clean up stale PID file
    try:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
    except OSError:
        pass

    if killed:
        # Wait for Telegram's getUpdates long-poll to expire
        print("Waiting 10s for Telegram polling to expire after killing old instance...")
        time.sleep(10)

    return killed


def _acquire_lock():
    """Atomically acquire an exclusive lock using msvcrt.locking to prevent
    concurrent run.py instances (fixes TOCTOU race condition)."""
    global _lock_fh
    try:
        # Open with O_CREAT|O_RDWR — non-blocking exclusive lock via locking()
        fd = os.open(LOCK_FILE, os.O_CREAT | os.O_RDWR)
        _lock_fh = os.fdopen(fd, "r+")
        # Try to lock the first byte exclusively (non-blocking)
        try:
            if sys.platform == "win32":
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        except OSError:
            # Another process holds the lock
            try:
                _lock_fh.seek(0)
                lock_pid = int(_lock_fh.read().strip())
            except (ValueError, IOError):
                lock_pid = "?"
            print(f"ERROR: Another run.py instance already holds the lock (PID {lock_pid}).")
            print("If this is wrong, delete .bot.lock and try again.")
            _lock_fh.close()
            _lock_fh = None
            sys.exit(1)
        # We hold the lock — write our PID
        _lock_fh.seek(0)
        _lock_fh.write(str(os.getpid()))
        _lock_fh.flush()
        _lock_fh.truncate()
    except Exception as e:
        print(f"Warning: could not acquire lock file: {e}")


def _release_lock():
    """Release the exclusive lock and remove lockfile on exit."""
    global _lock_fh
    if _lock_fh is not None:
        try:
            fd = _lock_fh.fileno()
            if sys.platform == "win32":
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            _lock_fh.close()
        except Exception:
            pass
        _lock_fh = None
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
    except Exception:
        pass


def rotate_log():
    """Rotate bot.log -> bot.log.1 if it exceeds LOG_MAX_BYTES."""
    try:
        if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > LOG_MAX_BYTES:
            backup = LOG_FILE + ".1"
            try:
                if os.path.exists(backup):
                    os.remove(backup)
                os.rename(LOG_FILE, backup)
                print(f"Rotated {LOG_FILE} -> {backup}")
            except PermissionError:
                # File is locked by another process (e.g., bot.py logging)
                # Try truncation instead of rename
                try:
                    with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
                        # Keep last 1MB
                        f.seek(0, 2)
                        size = f.tell()
                        if size > 1_000_000:
                            f.seek(-1_000_000, 2)
                            f.readline()  # skip partial line
                            tail = f.read()
                        else:
                            tail = None
                    if tail is not None:
                        tmp_path = LOG_FILE + ".tmp"
                        with open(tmp_path, "w", encoding="utf-8") as f:
                            f.write(tail)
                        os.replace(tmp_path, LOG_FILE)
                    print(f"Truncated {LOG_FILE} (file was locked)")
                except Exception as e2:
                    print(f"Warning: Could not rotate log: {e2}")
            except OSError as e:
                print(f"Warning: Log rotation failed: {e}")
    except OSError:
        pass  # File doesn't exist or can't stat it


def main():
    failures = []
    print(f"Python {platform.python_version()} on {platform.system()} {platform.release()}")
    print(f"Auto-restart wrapper for: {BOT_SCRIPT}")
    print("Press Ctrl+C to stop.\n")

    # Prevent multiple run.py instances
    _acquire_lock()

    # Kill any existing bot.py before starting
    _kill_existing_bot()

    # Start hot-reload watchdog
    _start_hot_reload_watcher()

    try:
        _main_loop(failures)
    finally:
        _release_lock()


def _main_loop(failures):
    global _current_proc
    while True:
        rotate_log()
        _hot_reload_pending.clear()
        start_time = time.time()
        exit_code = 1  # Default in case of exception
        is_hot_reload = False
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Starting bot...")

        # Kill any lingering bot.py before each restart (belt and suspenders)
        if os.path.exists(PID_FILE):
            try:
                with open(PID_FILE, "r", encoding="utf-8") as f:
                    old_pid = int(f.read().strip())
                if _is_pid_alive(old_pid) and old_pid != os.getpid():
                    print(f"Stale bot.py still running (PID {old_pid}), killing before restart...")
                    if sys.platform == "win32":
                        subprocess.run(["taskkill", "/PID", str(old_pid), "/F"],
                                       capture_output=True, timeout=5)
                    else:
                        os.kill(old_pid, signal.SIGTERM)
                    time.sleep(3)
            except (ValueError, IOError, OSError):
                pass

        proc = None
        try:
            proc = subprocess.Popen(
                [sys.executable, BOT_SCRIPT],
                cwd=os.path.dirname(BOT_SCRIPT),
            )
            _current_proc = proc
            # Healthcheck: wait up to 5 seconds; if it dies quickly, flag it
            try:
                exit_code = proc.wait(timeout=5)
                # Process exited within 5 seconds — will be handled below
            except subprocess.TimeoutExpired:
                # Still running after 5s — healthy start, wait for it to finish
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Bot healthcheck OK (still running after 5s)")
                exit_code = proc.wait()
            is_hot_reload = _hot_reload_pending.is_set()
        except KeyboardInterrupt:
            print("\nStopped by user.")
            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
            break
        except SystemExit:
            print("\nSystemExit caught. Shutting down.")
            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
            break
        except Exception as e:
            print(f"Error starting bot: {e}")
            _write_error_log(f"STARTUP ERROR: {e}\n{traceback.format_exc()}")
            exit_code = 1
            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
        finally:
            _current_proc = None

        elapsed = time.time() - start_time
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Bot exited (code {exit_code}) after {elapsed:.0f}s")

        # Log crash to _error_log.txt (skip clean exits and hot-reloads)
        if exit_code != 0 and not is_hot_reload:
            _write_error_log(
                f"CRASH: exit_code={exit_code}, elapsed={elapsed:.0f}s"
            )

        # Hot-reload: .py file changed — restart immediately (3s for file flush)
        if is_hot_reload:
            print("[HotReload] Restarting due to .py file change (3s)...")
            try:
                time.sleep(3)
            except (KeyboardInterrupt, SystemExit):
                break
            continue

        # Exit code 0 means clean shutdown — don't restart
        if exit_code == 0 and elapsed > 10:
            print("Bot exited cleanly (code 0). Not restarting.")
            break

        # Exit code 42 = Conflict error, bot detected another instance.
        # Wait longer before retry to let the other instance fully release.
        if exit_code == 42:
            print("Bot exited with Conflict (code 42). Another instance may be polling.")
            print("Killing all bot.py processes and waiting 20s for Telegram to release...")
            _kill_existing_bot()
            try:
                time.sleep(10)  # extra wait on top of _kill_existing_bot's 10s
            except (KeyboardInterrupt, SystemExit):
                break
            continue

        # Track rapid failures
        now = time.time()
        failures = [t for t in failures if now - t < RAPID_FAILURE_WINDOW]
        if elapsed < 10:  # Crashed within 10 seconds = rapid failure
            count = 3 if elapsed < 3 else 1  # Immediate exits count 3x
            failures.extend([now] * count)
            if elapsed < 3:
                print(f"Bot exited almost immediately (<3s) — counting as {count} failures")
            if len(failures) >= MAX_RAPID_FAILURES:
                print(f"Too many rapid failures ({MAX_RAPID_FAILURES} in {RAPID_FAILURE_WINDOW}s). Stopping.")
                _write_error_log(f"GIVING UP: {MAX_RAPID_FAILURES} rapid failures in {RAPID_FAILURE_WINDOW}s")
                break

        # Wait 15s before restart — Telegram's getUpdates long-poll takes ~10s to expire,
        # so a shorter delay causes "Conflict: terminated by other getUpdates request" errors.
        restart_wait = 3 if elapsed < 3 else 15
        print(f"Restarting in {restart_wait} seconds...")
        try:
            time.sleep(restart_wait)
        except KeyboardInterrupt:
            print("\nStopped by user.")
            break
        except SystemExit:
            print("\nSystemExit caught. Shutting down.")
            break


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        print("Exiting.")
    except KeyboardInterrupt:
        print("\nStopped by user.")
