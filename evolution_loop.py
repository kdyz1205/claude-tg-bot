"""
进化循环驱动器
检查local session是否空闲，空闲时自动发下一个进化任务
"""
import json
import os
import time
import sys
import subprocess
import pyperclip
import pyautogui
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

QUEUE_FILE = Path(__file__).parent / ".evolution_queue.json"
IDLE_KEYWORDS = ["Reply...", "Reply…"]
BUSY_KEYWORDS = ["Processing", "Thinking", "Crafting", "Tinkering", "Coalescing", "Reflecting", "Pondering", "Synthesizing", "Creating", "Creati"]

def screenshot():
    try:
        result = subprocess.run(
            ["python", "pc_control.py", "screenshot"],
            capture_output=True, text=True, cwd=str(Path(__file__).parent),
            timeout=30,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None
    # Find screenshot path
    for line in result.stdout.split("\n"):
        if "Screenshot saved:" in line:
            return line.split("Screenshot saved: ")[1].strip()
    return None

def is_session_idle():
    """Check if tg bot session is idle by looking for busy/idle indicators"""
    try:
        # Take screenshot and check pixels around bottom input area
        img = pyautogui.screenshot()
        # Sample the status area around y=375 (where "Processing..." typically shows)
        # Also check y=440 area (Reply box)

        # Simple approach: screenshot and check with PIL
        from PIL import Image
        import numpy as np

        arr = np.array(img)
        # Check if the Reply box area is visible (light gray around 350-450 y, 200-800 x)
        # Reply box bg is usually white/light
        reply_area = arr[430:460, 200:800]
        avg_brightness = reply_area.mean()

        # High brightness = white reply box = idle
        # But also need to check there's no spinner
        return avg_brightness > 200
    except Exception as e:
        print(f"Idle check error: {e}")
        return False

def get_next_task():
    """Get the next pending task from the queue"""
    try:
        with open(QUEUE_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"Error reading queue: {e}")
        return None, {"tasks": []}

    for task in data.get("tasks", []):
        if task.get("status") == "pending":
            return task, data
    return None, data

def mark_task_sent(task_id):
    """Mark a task as sent in the queue"""
    try:
        with open(QUEUE_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"Error reading queue for mark: {e}")
        return

    for task in data.get("tasks", []):
        if task.get("id") == task_id:
            task["status"] = "sent"
            task["sent_at"] = time.strftime("%Y-%m-%d %H:%M:%S")

    tmp = str(QUEUE_FILE) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, str(QUEUE_FILE))

def send_to_session(message):
    """Send a message to the tg bot session"""
    # Click reply box
    pyautogui.click(400, 441)
    time.sleep(0.5)

    # Paste message
    pyperclip.copy(message)
    pyautogui.hotkey("ctrl", "v")
    time.sleep(0.5)

    # Send
    pyautogui.press("enter")
    print(f"[OK] Sent task to session")

def check_and_send():
    """Main check: if idle, send next task"""
    # Get next task
    task, _ = get_next_task()
    if not task:
        print("[DONE] All evolution tasks complete!")
        return False

    # Check if session is idle
    if not is_session_idle():
        print(f"⏳ Session busy, waiting... (Next task: {task.get('name', '?')})")
        return True  # continue loop

    # Session is idle, send the task
    print(f"\n[SENDING] evolution task #{task.get('id', '?')}: {task.get('name', '?')}")

    evolution_prompt = f"""
**[进化任务 #{task.get('id', '?')}: {task.get('name', '?')}]**

{task.get('prompt', '')}

完成后：
1. 总结你做了什么
2. 学到了什么新的？发现了哪些新的弱点？
3. 推荐下一步进化方向
""".strip()

    send_to_session(evolution_prompt)
    mark_task_sent(task.get("id", 0))

    print(f"[SENT] Task #{task.get('id', '?')} sent. Waiting for completion...")
    return True

def main():
    print("[EVOLUTION LOOP STARTED]")
    print(f"Queue: {QUEUE_FILE}")

    # Show next task
    task, data = get_next_task()
    if task:
        print(f"Next task: #{task['id']} - {task['name']}")

    # Single check mode (called by cron)
    if "--once" in sys.argv:
        check_and_send()
        return

    # Loop mode
    check_interval = 60  # seconds
    while True:
        should_continue = check_and_send()
        if not should_continue:
            break
        time.sleep(check_interval)

if __name__ == "__main__":
    main()
