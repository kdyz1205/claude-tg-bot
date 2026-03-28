"""
pc_control.py — Computer control toolkit for Claude CLI.

Usage from Claude CLI via Bash:
  python pc_control.py screenshot              → takes screenshot, saves to ~/Desktop/screenshot.png
  python pc_control.py screenshot --region 0,0,800,600  → partial screenshot
  python pc_control.py click 500 300           → left click at (500, 300)
  python pc_control.py doubleclick 500 300     → double click
  python pc_control.py rightclick 500 300      → right click
  python pc_control.py type "hello world"      → type text
  python pc_control.py hotkey ctrl c           → press Ctrl+C
  python pc_control.py scroll 3               → scroll up 3 clicks (negative = down)
  python pc_control.py moveto 500 300          → move mouse to position
  python pc_control.py drag 100 200 500 400    → drag from (100,200) to (500,400)
  python pc_control.py locate "image.png"      → find image on screen, return coordinates
  python pc_control.py getpos                  → get current mouse position
  python pc_control.py screensize              → get screen dimensions
"""
import sys
import os
import time
import subprocess

TAKEOVER_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "takeover.py")

# Commands that take over mouse/keyboard (need countdown)
TAKEOVER_COMMANDS = {"click", "doubleclick", "rightclick", "type", "hotkey", "scroll", "moveto", "drag"}
# Safe commands (no takeover needed)
SAFE_COMMANDS = {"screenshot", "locate", "getpos", "screensize", "wait"}


def request_takeover(action_desc: str = "") -> bool:
    """Show takeover countdown. Returns True if approved, False if cancelled."""
    # Skip if --no-takeover flag is present
    if "--no-takeover" in sys.argv:
        return True
    msg = f"Bot 即将操控: {action_desc}" if action_desc else "Bot 即将接管鼠标键盘"
    try:
        result = subprocess.run(
            [sys.executable, TAKEOVER_SCRIPT, "3", msg],
            timeout=10,
        )
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        return True  # If overlay hangs, proceed anyway
    except Exception:
        return True  # If overlay fails, proceed anyway


def _check_coords(x, y):
    """Validate coordinates are within screen bounds."""
    import pyautogui
    size = pyautogui.size()
    if x < 0 or y < 0 or x > size.width or y > size.height:
        print(f"WARNING: ({x}, {y}) outside screen ({size.width}x{size.height}), clamping")
        x = max(0, min(x, size.width - 1))
        y = max(0, min(y, size.height - 1))
    return x, y


def main():
    import pyautogui
    pyautogui.FAILSAFE = False  # Don't abort on corner moves
    pyautogui.PAUSE = 0.05  # 50ms between operations (was 100ms)

    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1].lower()

    # Takeover countdown for mouse/keyboard commands
    if cmd in TAKEOVER_COMMANDS:
        action_desc = " ".join(sys.argv[1:])[:60]
        if not request_takeover(action_desc):
            print("CANCELLED by user (right-click)")
            sys.exit(1)

    if cmd == "screenshot":
        from PIL import ImageGrab
        # Save to TG forwarding directory so bot auto-sends to Telegram
        tg_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_tg_screenshots")
        os.makedirs(tg_dir, exist_ok=True)
        ts = time.strftime("%Y%m%d_%H%M%S")
        save_path = os.path.join(tg_dir, f"screenshot_{ts}.jpg")
        region = None
        if "--region" in sys.argv:
            idx = sys.argv.index("--region")
            if idx + 1 < len(sys.argv):
                parts = sys.argv[idx + 1].split(",")
                try:
                    region = tuple(int(x) for x in parts)
                    if len(region) != 4:
                        print("Error: --region needs exactly 4 values: left,top,right,bottom")
                        sys.exit(1)
                except ValueError:
                    print("Error: --region values must be integers, e.g. --region 0,0,800,600")
                    sys.exit(1)
        img = ImageGrab.grab(bbox=region)
        # Save as JPEG for smaller size (TG friendly)
        img.save(save_path, format="JPEG", quality=80)
        print(f"Screenshot saved: {save_path}")
        print(f"Size: {img.size[0]}x{img.size[1]}")
        print(f"[TG_FORWARD] This screenshot will be auto-sent to Telegram.")

    elif cmd == "click":
        if len(sys.argv) < 4:
            print("Usage: pc_control.py click X Y")
            sys.exit(1)
        x, y = _check_coords(int(sys.argv[2]), int(sys.argv[3]))
        pyautogui.click(x, y)
        print(f"Clicked ({x}, {y})")

    elif cmd == "doubleclick":
        if len(sys.argv) < 4:
            print("Usage: pc_control.py doubleclick X Y")
            sys.exit(1)
        x, y = _check_coords(int(sys.argv[2]), int(sys.argv[3]))
        pyautogui.doubleClick(x, y)
        print(f"Double-clicked ({x}, {y})")

    elif cmd == "rightclick":
        if len(sys.argv) < 4:
            print("Usage: pc_control.py rightclick X Y")
            sys.exit(1)
        x, y = _check_coords(int(sys.argv[2]), int(sys.argv[3]))
        pyautogui.rightClick(x, y)
        print(f"Right-clicked ({x}, {y})")

    elif cmd == "type":
        if len(sys.argv) < 3:
            print("Usage: pc_control.py type TEXT")
            sys.exit(1)
        text = sys.argv[2]
        # Use clipboard for non-ASCII (Chinese, etc)
        if any(ord(c) > 127 for c in text):
            try:
                import tempfile
                # Write to temp file, then read via PowerShell to avoid escaping issues
                tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", encoding="utf-8", delete=False)
                tmp.write(text)
                tmp.close()
                subprocess.run(["powershell", "-NoProfile", "-Command",
                              f"Set-Clipboard -Value (Get-Content -Raw -Encoding UTF8 '{tmp.name}')"],
                              check=True, capture_output=True)
                os.unlink(tmp.name)
            except Exception:
                # Fallback: direct escaping (single quotes in PS don't interpolate)
                safe = text.replace("'", "''")
                subprocess.run(["powershell", "-NoProfile", "-Command",
                              f"Set-Clipboard -Value '{safe}'"], check=True, capture_output=True)
            pyautogui.hotkey("ctrl", "v")
            time.sleep(0.1)  # Let clipboard paste complete
            print(f"Typed (via clipboard): {text}")
        else:
            pyautogui.typewrite(text, interval=0.005)  # Fast ASCII typing
            print(f"Typed: {text}")

    elif cmd == "hotkey":
        if len(sys.argv) < 3:
            print("Usage: pc_control.py hotkey KEY1 [KEY2 ...]")
            sys.exit(1)
        keys = [k.lower() for k in sys.argv[2:] if not k.startswith("--")]  # Filter flags, normalize case
        if not keys:
            print("Error: no keys specified")
            sys.exit(1)
        pyautogui.hotkey(*keys)
        print(f"Hotkey: {'+'.join(keys)}")

    elif cmd == "scroll":
        if len(sys.argv) < 3:
            print("Usage: pc_control.py scroll CLICKS [X Y]")
            sys.exit(1)
        clicks = int(sys.argv[2])
        x = int(sys.argv[3]) if len(sys.argv) > 3 else None
        y = int(sys.argv[4]) if len(sys.argv) > 4 else None
        pyautogui.scroll(clicks, x=x, y=y)
        print(f"Scrolled {clicks} at ({x}, {y})")

    elif cmd == "moveto":
        if len(sys.argv) < 4:
            print("Usage: pc_control.py moveto X Y")
            sys.exit(1)
        x, y = _check_coords(int(sys.argv[2]), int(sys.argv[3]))
        pyautogui.moveTo(x, y)
        print(f"Moved to ({x}, {y})")

    elif cmd == "drag":
        if len(sys.argv) < 6:
            print("Usage: pc_control.py drag X1 Y1 X2 Y2 [DURATION]")
            sys.exit(1)
        x1, y1 = _check_coords(int(sys.argv[2]), int(sys.argv[3]))
        x2, y2 = _check_coords(int(sys.argv[4]), int(sys.argv[5]))
        duration = float(sys.argv[6]) if len(sys.argv) > 6 else 0.5
        pyautogui.moveTo(x1, y1)
        pyautogui.drag(x2 - x1, y2 - y1, duration=duration)
        print(f"Dragged ({x1},{y1}) -> ({x2},{y2})")

    elif cmd == "locate":
        if len(sys.argv) < 3:
            print("Usage: pc_control.py locate IMAGE_PATH")
            sys.exit(1)
        image_path = sys.argv[2]
        try:
            loc = pyautogui.locateOnScreen(image_path, confidence=0.8)
            if loc:
                center = pyautogui.center(loc)
                print(f"Found at: ({center.x}, {center.y})")
                print(f"Region: {loc}")
            else:
                print("Not found on screen")
        except Exception as e:
            print(f"Error: {e}")

    elif cmd == "getpos":
        pos = pyautogui.position()
        print(f"Mouse position: ({pos.x}, {pos.y})")

    elif cmd == "screensize":
        size = pyautogui.size()
        print(f"Screen size: {size.width}x{size.height}")

    elif cmd == "wait":
        secs = float(sys.argv[2]) if len(sys.argv) > 2 else 1
        time.sleep(secs)
        print(f"Waited {secs}s")

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)

if __name__ == "__main__":
    main()
