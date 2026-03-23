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

BOT_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.py")
MAX_RAPID_FAILURES = 10
RAPID_FAILURE_WINDOW = 60  # seconds


def main():
    failures = []
    print(f"Auto-restart wrapper for: {BOT_SCRIPT}")
    print("Press Ctrl+C to stop.\n")

    while True:
        start_time = time.time()
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Starting bot...")

        try:
            result = subprocess.run(
                [sys.executable, BOT_SCRIPT],
                cwd=os.path.dirname(BOT_SCRIPT),
            )
            exit_code = result.returncode
        except KeyboardInterrupt:
            print("\nStopped by user.")
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
            failures.append(now)
            if len(failures) >= MAX_RAPID_FAILURES:
                print(f"Too many rapid failures ({MAX_RAPID_FAILURES} in {RAPID_FAILURE_WINDOW}s). Stopping.")
                break

        print("Restarting in 5 seconds...")
        try:
            time.sleep(5)
        except KeyboardInterrupt:
            print("\nStopped by user.")
            break


if __name__ == "__main__":
    main()
