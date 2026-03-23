"""
run.py — Auto-restart wrapper for the Telegram bot.

Usage: python run.py
- Automatically restarts bot if it crashes
- Waits 5 seconds between restarts
- Stops after 10 consecutive rapid failures
"""
import subprocess
import sys
import time
import os
import platform

BOT_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.py")
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.log")
MAX_RAPID_FAILURES = 10
RAPID_FAILURE_WINDOW = 60  # seconds
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB


def rotate_log():
    """Rotate bot.log -> bot.log.1 if it exceeds LOG_MAX_BYTES."""
    if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > LOG_MAX_BYTES:
        backup = LOG_FILE + ".1"
        if os.path.exists(backup):
            os.remove(backup)
        os.rename(LOG_FILE, backup)
        print(f"Rotated {LOG_FILE} -> {backup}")


def main():
    failures = []
    print(f"Python {platform.python_version()} on {platform.system()} {platform.release()}")
    print(f"Auto-restart wrapper for: {BOT_SCRIPT}")
    print("Press Ctrl+C to stop.\n")

    while True:
        rotate_log()
        start_time = time.time()
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Starting bot...")

        try:
            proc = subprocess.Popen(
                [sys.executable, BOT_SCRIPT],
                cwd=os.path.dirname(BOT_SCRIPT),
            )
            # Healthcheck: wait up to 5 seconds; if it dies quickly, flag it
            try:
                exit_code = proc.wait(timeout=5)
                # Process exited within 5 seconds — will be handled below
            except subprocess.TimeoutExpired:
                # Still running after 5s — healthy start, wait for it to finish
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Bot healthcheck OK (still running after 5s)")
                exit_code = proc.wait()
        except KeyboardInterrupt:
            print("\nStopped by user.")
            if 'proc' in dir() and proc.poll() is None:
                proc.terminate()
            break
        except SystemExit:
            print("\nSystemExit caught. Shutting down.")
            if 'proc' in dir() and proc.poll() is None:
                proc.terminate()
            break
        except Exception as e:
            print(f"Error starting bot: {e}")
            exit_code = 1

        elapsed = time.time() - start_time
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Bot exited (code {exit_code}) after {elapsed:.0f}s")

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
                break

        print("Restarting in 5 seconds...")
        try:
            time.sleep(5)
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
