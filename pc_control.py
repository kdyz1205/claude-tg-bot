"""
pc_control.py — Computer control toolkit for Claude CLI.

Usage from Claude CLI via Bash:
  python pc_control.py screenshot              → takes screenshot, saves to ~/Desktop/screenshot.png
  python pc_control.py screenshot --region 0,0,800,600  → partial screenshot
  python pc_control.py click 500 300           → left click at (500, 300)
  python pc_control.py doubleclick 500 300     → double click
  python pc_control.py rightclick 500 300      → right click
  python pc_control.py type "hello world"      → type text
  python pc_control.py smarttype "hello world" → type text and verify it appeared on screen
  python pc_control.py hotkey ctrl c           → press Ctrl+C
  python pc_control.py scroll 3               → scroll up 3 clicks (negative = down)
  python pc_control.py moveto 500 300          → move mouse to position
  python pc_control.py drag 100 200 500 400    → drag from (100,200) to (500,400)
  python pc_control.py locate "image.png"      → find image on screen, return coordinates
  python pc_control.py findcolor R,G,B [tol]   → find UI elements matching a color (tolerance 0-255)
  python pc_control.py verifyclick X Y         → click and verify screen changed
  python pc_control.py getpos                  → get current mouse position
  python pc_control.py screensize              → get screen dimensions
  python pc_control.py findtext                → find text-like regions on screen
  python pc_control.py findinput               → find input fields on screen
  python pc_control.py focusedwindow           → get info about the focused window
  python pc_control.py windowlist              → list all visible windows
  python pc_control.py smartclick X Y          → click with verification and retry
  python pc_control.py findbuttonnear X Y [R]  → find button near coordinates
"""
import sys
import os
import time
import subprocess
import json

BOT_DIR = os.path.dirname(os.path.abspath(__file__))
TAKEOVER_SCRIPT = os.path.join(BOT_DIR, "takeover.py")

# --- Emergency stop ---
# Create this file to immediately halt all PC control actions.
# Delete it to resume. This is the global kill switch.
EMERGENCY_STOP_FILE = os.path.join(BOT_DIR, "_EMERGENCY_STOP")


def check_emergency_stop():
    """Check if emergency stop is engaged. Raises RuntimeError if so."""
    if os.path.exists(EMERGENCY_STOP_FILE):
        raise RuntimeError(
            "EMERGENCY STOP is active. PC control is halted. "
            f"Delete {EMERGENCY_STOP_FILE} to resume."
        )


# --- Takeover warning configuration ---
TAKEOVER_WARNING_ENABLED = True   # Set to False to skip the countdown entirely
TAKEOVER_WARNING_SECONDS = 3      # Countdown duration in seconds

# Commands that take over mouse/keyboard (need countdown)
TAKEOVER_COMMANDS = {"click", "doubleclick", "rightclick", "type", "smarttype", "hotkey", "scroll", "moveto", "drag", "verifyclick", "smartclick"}
# Safe commands (no takeover needed)
SAFE_COMMANDS = {"screenshot", "locate", "findcolor", "getpos", "screensize", "wait", "findtext", "findinput", "focusedwindow", "windowlist", "findbuttonnear"}

# Track whether a takeover warning was already shown recently (to avoid
# repeated popups when one high-level action triggers many low-level clicks).
_last_takeover_time = 0
_TAKEOVER_DEDUP_SECONDS = 5  # Suppress duplicate warnings within this window


def show_takeover_warning(seconds: int = None, message: str = "Bot taking control...") -> bool:
    """Show a takeover countdown overlay before the bot takes control.

    Tries these display methods in order:
      A) Tkinter overlay via takeover.py (topmost auto-closing window)
      B) ctypes MessageBoxTimeoutW (auto-closing message box)
      C) System beep + console print

    The warning is non-blocking for the countdown display (auto-closes).
    Returns True to proceed, False if user cancelled (right-click).

    Respects TAKEOVER_WARNING_ENABLED and deduplication window so
    callers can invoke this freely without spamming popups.
    """
    global _last_takeover_time

    check_emergency_stop()

    if not TAKEOVER_WARNING_ENABLED:
        return True

    if seconds is None:
        seconds = TAKEOVER_WARNING_SECONDS

    # Deduplication: skip if a warning was shown very recently
    now = time.time()
    if now - _last_takeover_time < _TAKEOVER_DEDUP_SECONDS:
        return True
    _last_takeover_time = now

    # --- Option A: Tkinter overlay via takeover.py (best experience) ---
    try:
        result = subprocess.run(
            [sys.executable, TAKEOVER_SCRIPT, str(seconds), message],
            timeout=seconds + 7,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    except Exception:
        pass

    # --- Option B: ctypes auto-closing MessageBox (Windows only) ---
    try:
        import ctypes
        MB_ICONWARNING = 0x30
        MB_TOPMOST = 0x40000
        # MessageBoxTimeoutW: auto-closes after timeout_ms
        ctypes.windll.user32.MessageBoxTimeoutW(
            0, f"{message}\n\n{seconds}... 2... 1... GO", "Bot Takeover",
            MB_ICONWARNING | MB_TOPMOST, 0, seconds * 1000
        )
        return True
    except Exception:
        pass

    # --- Option C: System beep + console countdown ---
    try:
        import ctypes
        for i in range(seconds, 0, -1):
            print(f"[TAKEOVER] {message} — {i}...")
            ctypes.windll.user32.MessageBeep(0xFFFFFFFF)  # simple beep
            time.sleep(1)
        print("[TAKEOVER] GO")
        return True
    except Exception:
        # Absolute last resort: just print and sleep
        for i in range(seconds, 0, -1):
            print(f"[TAKEOVER] {message} — {i}...")
            time.sleep(1)
        print("[TAKEOVER] GO")
        return True


def request_takeover(action_desc: str = "") -> bool:
    """Show takeover countdown. Returns True if approved, False if cancelled.

    This is a convenience wrapper around show_takeover_warning() that
    also honours the --no-takeover CLI flag and emergency stop.
    """
    check_emergency_stop()
    # Skip if --no-takeover flag is present
    if "--no-takeover" in sys.argv:
        return True
    msg = f"Bot taking control: {action_desc}" if action_desc else "Bot taking control of mouse/keyboard"
    return show_takeover_warning(seconds=TAKEOVER_WARNING_SECONDS, message=msg)


def _get_virtual_screen_bounds():
    """Get the virtual screen bounds covering all monitors (handles multi-monitor).

    Returns (left, top, right, bottom) of the virtual desktop.
    On single-monitor, this is (0, 0, width, height).
    On multi-monitor, left/top can be negative.
    """
    try:
        import ctypes
        user32 = ctypes.windll.user32
        # SM_XVIRTUALSCREEN=76, SM_YVIRTUALSCREEN=77
        # SM_CXVIRTUALSCREEN=78, SM_CYVIRTUALSCREEN=79
        left = user32.GetSystemMetrics(76)
        top = user32.GetSystemMetrics(77)
        width = user32.GetSystemMetrics(78)
        height = user32.GetSystemMetrics(79)
        if width > 0 and height > 0:
            return left, top, left + width, top + height
    except Exception:
        pass
    # Fallback: single monitor via pyautogui
    import pyautogui
    size = pyautogui.size()
    return 0, 0, size.width, size.height


def _ensure_dpi_aware():
    """Enable DPI awareness so coordinates match physical pixels on scaled displays."""
    try:
        import ctypes
        # Try per-monitor DPI awareness (Windows 10 1703+)
        try:
            ctypes.windll.shcore.SetProcessDpiAwareness(2)  # PROCESS_PER_MONITOR_DPI_AWARE
        except Exception:
            # Fallback: system DPI awareness
            ctypes.windll.user32.SetProcessDPIAware()
    except Exception:
        pass

# Call once at module level
_ensure_dpi_aware()


def _check_coords(x, y):
    """Validate coordinates are within virtual screen bounds (multi-monitor safe).

    On multi-monitor setups, coordinates can be negative (monitor to the left/above
    the primary). This function clamps to the full virtual desktop area.
    """
    vl, vt, vr, vb = _get_virtual_screen_bounds()
    if x < vl or y < vt or x >= vr or y >= vb:
        print(f"WARNING: ({x}, {y}) outside virtual screen ({vl},{vt})-({vr},{vb}), clamping")
        x = max(vl, min(x, vr - 1))
        y = max(vt, min(y, vb - 1))
    return x, y


def _grab_screenshot_array(region=None):
    """Capture screenshot as a numpy array (RGB). Used by analysis functions.

    Handles RGBA -> RGB conversion and multi-monitor capture via all_screens.
    """
    from PIL import ImageGrab
    import numpy as np
    img = ImageGrab.grab(bbox=region, all_screens=True)
    # Ensure RGB (ImageGrab can return RGBA on some Windows configs)
    if img.mode != "RGB":
        img = img.convert("RGB")
    return np.array(img)


def find_element_by_color(color_rgb, tolerance=20, min_area=100):
    """Scan the screen for contiguous UI elements matching a color range.

    Args:
        color_rgb: (R, G, B) tuple of the target color.
        tolerance: How far each channel can deviate (0-255).
        min_area: Minimum pixel area to count as a UI element.

    Returns:
        List of dicts with keys: x, y (center), width, height, area.
    """
    import numpy as np
    screen = _grab_screenshot_array()
    target = np.array(color_rgb, dtype=np.int16)
    diff = np.abs(screen.astype(np.int16) - target)
    mask = np.all(diff <= tolerance, axis=2).astype(np.uint8)

    # Connected-component labeling without OpenCV: simple row-scan approach
    # Use scipy if available, else fall back to a bounding-box scan
    try:
        from scipy import ndimage
        labeled, num_features = ndimage.label(mask)
        results = []
        for i in range(1, num_features + 1):
            ys, xs = np.where(labeled == i)
            area = len(xs)
            if area < min_area:
                continue
            x_min, x_max = int(xs.min()), int(xs.max())
            y_min, y_max = int(ys.min()), int(ys.max())
            results.append({
                "x": (x_min + x_max) // 2,
                "y": (y_min + y_max) // 2,
                "width": x_max - x_min + 1,
                "height": y_max - y_min + 1,
                "area": area,
            })
        results.sort(key=lambda r: r["area"], reverse=True)
        return results[:50]  # cap at 50 largest
    except ImportError:
        # Fallback: scan for bounding box of all matching pixels
        ys, xs = np.where(mask > 0)
        if len(xs) == 0:
            return []
        return [{
            "x": int((xs.min() + xs.max()) // 2),
            "y": int((ys.min() + ys.max()) // 2),
            "width": int(xs.max() - xs.min() + 1),
            "height": int(ys.max() - ys.min() + 1),
            "area": int(len(xs)),
        }]


def verify_click_result(before_screenshot, after_screenshot, click_region=None):
    """Compare two screenshot numpy arrays to verify a click had an effect.

    Args:
        before_screenshot: numpy array (RGB) captured before the click.
        after_screenshot:  numpy array (RGB) captured after the click.
        click_region: Optional (x, y, radius) tuple to focus comparison around.

    Returns:
        dict with keys:
          changed (bool): whether a meaningful change was detected,
          change_percent (float): percentage of pixels that changed,
          changed_region (dict|None): bounding box of the changed area.
    """
    import numpy as np
    b = before_screenshot.astype(np.int16)
    a = after_screenshot.astype(np.int16)

    # If sizes differ, can't compare meaningfully
    if b.shape != a.shape:
        return {"changed": True, "change_percent": 100.0, "changed_region": None}

    # If click_region given, crop both to a region around the click point
    if click_region:
        cx, cy, radius = click_region
        h, w = b.shape[:2]
        y1 = max(0, cy - radius)
        y2 = min(h, cy + radius)
        x1 = max(0, cx - radius)
        x2 = min(w, cx + radius)
        b = b[y1:y2, x1:x2]
        a = a[y1:y2, x1:x2]
    else:
        x1, y1 = 0, 0

    diff = np.abs(a - b)
    pixel_changed = np.any(diff > 12, axis=2)  # threshold per channel
    change_count = int(pixel_changed.sum())
    total = pixel_changed.size
    pct = (change_count / total * 100) if total > 0 else 0.0

    changed_region = None
    if change_count > 0:
        ys, xs = np.where(pixel_changed)
        changed_region = {
            "x": int(xs.min()) + x1,
            "y": int(ys.min()) + y1,
            "width": int(xs.max() - xs.min()) + 1,
            "height": int(ys.max() - ys.min()) + 1,
        }

    return {
        "changed": pct > 0.05,  # more than 0.05% of region changed
        "change_percent": round(pct, 3),
        "changed_region": changed_region,
    }


def smart_type(text, verify=True):
    """Type text and optionally verify it appeared on screen.

    Uses OCR-free approach: takes before/after screenshots and checks that
    pixels changed in the area around the cursor (typing region).

    Args:
        text: The text to type.
        verify: If True, take screenshots before/after and verify change.

    Returns:
        dict with keys: typed (bool), verified (bool|None), change_percent (float|None).
    """
    import pyautogui
    import numpy as np

    before = None
    cursor_pos = pyautogui.position()

    if verify:
        before = _grab_screenshot_array()

    # Type using the same logic as the main 'type' command
    if any(ord(c) > 127 for c in text):
        tmp_name = None
        try:
            import tempfile
            tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", encoding="utf-8", delete=False)
            tmp_name = tmp.name
            tmp.write(text)
            tmp.close()
            subprocess.run(
                ["powershell", "-NoProfile", "-Command",
                 f"Set-Clipboard -Value (Get-Content -Raw -Encoding UTF8 '{tmp_name}')"],
                check=True, capture_output=True, timeout=10,
            )
        except Exception:
            safe = text.replace("'", "''")
            subprocess.run(
                ["powershell", "-NoProfile", "-Command",
                 f"Set-Clipboard -Value '{safe}'"],
                check=True, capture_output=True, timeout=10,
            )
        finally:
            if tmp_name:
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass
        pyautogui.hotkey("ctrl", "v")
        time.sleep(0.1)
    else:
        pyautogui.typewrite(text, interval=0.005)

    result = {"typed": True, "verified": None, "change_percent": None}

    if verify and before is not None:
        time.sleep(0.3)  # let rendering settle
        after = _grab_screenshot_array()
        # Check around cursor position with generous radius
        vr = verify_click_result(before, after, click_region=(cursor_pos.x, cursor_pos.y, 300))
        result["verified"] = vr["changed"]
        result["change_percent"] = vr["change_percent"]

        # If verification failed and we used typewrite (ASCII path), retry via clipboard
        if not vr["changed"] and not any(ord(c) > 127 for c in text):
            try:
                # Select-all and delete what we may have typed, then paste fresh
                pyautogui.hotkey("ctrl", "a")
                time.sleep(0.05)
                pyautogui.press("delete")
                time.sleep(0.05)
                # Use clipboard paste as fallback
                tmp_name = None
                import tempfile
                tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", encoding="utf-8", delete=False)
                tmp_name = tmp.name
                tmp.write(text)
                tmp.close()
                subprocess.run(
                    ["powershell", "-NoProfile", "-Command",
                     f"Set-Clipboard -Value (Get-Content -Raw -Encoding UTF8 '{tmp_name}')"],
                    check=True, capture_output=True, timeout=5,
                )
                if tmp_name:
                    try:
                        os.unlink(tmp_name)
                    except OSError:
                        pass
                before2 = _grab_screenshot_array()
                pyautogui.hotkey("ctrl", "v")
                time.sleep(0.3)
                after2 = _grab_screenshot_array()
                vr2 = verify_click_result(before2, after2, click_region=(cursor_pos.x, cursor_pos.y, 300))
                result["verified"] = vr2["changed"]
                result["change_percent"] = vr2["change_percent"]
                result["method"] = "clipboard_fallback"
            except Exception:
                pass  # clipboard fallback is best-effort

    return result


def find_text_region(text_pattern=None, screenshot=None):
    """Find rectangular text-like regions on screen by detecting horizontal
    runs of dark pixels on light backgrounds.  This is NOT OCR — it locates
    WHERE text regions are by pixel-pattern analysis.

    Args:
        text_pattern: Unused placeholder (reserved for future filtering).
        screenshot: Optional numpy RGB array.  Captured automatically if None.

    Returns:
        List of dicts with keys: x, y, width, height, center_x, center_y.
        Sorted by vertical position (top to bottom).
    """
    import numpy as np

    if screenshot is None:
        screenshot = _grab_screenshot_array()

    # Convert to grayscale
    gray = np.dot(screenshot[..., :3], [0.299, 0.587, 0.114]).astype(np.uint8)
    h, w = gray.shape

    # Text = dark pixels on light background.  Threshold: pixel < 100 on a
    # background that averages > 180 in the surrounding row-strip.
    # Compute per-row mean brightness
    row_mean = gray.mean(axis=1)

    # Binary mask: dark pixels in bright rows
    bright_rows = row_mean > 160
    dark_px = gray < 120
    text_mask = np.zeros_like(dark_px, dtype=np.uint8)
    for r in range(h):
        if bright_rows[r]:
            text_mask[r, :] = dark_px[r, :].astype(np.uint8)

    # Horizontal run-length merging: dilate horizontally to connect chars
    # Simple approach: for each row, expand each True pixel by 4px left/right
    dilated = text_mask.copy()
    for shift in range(1, 6):
        dilated[:, shift:] |= text_mask[:, :-shift]
        dilated[:, :-shift] |= text_mask[:, shift:]

    # Vertical dilation by 2px to merge lines
    vert = dilated.copy()
    for shift in range(1, 3):
        vert[shift:, :] |= dilated[:-shift, :]
        vert[:-shift, :] |= dilated[shift:, :]
    dilated = vert

    # Connected components
    try:
        from scipy import ndimage
        labeled, num_features = ndimage.label(dilated)
    except ImportError:
        ys, xs = np.where(dilated > 0)
        if len(xs) == 0:
            return []
        return [{
            "x": int(xs.min()), "y": int(ys.min()),
            "width": int(xs.max() - xs.min()) + 1,
            "height": int(ys.max() - ys.min()) + 1,
            "center_x": int((xs.min() + xs.max()) // 2),
            "center_y": int((ys.min() + ys.max()) // 2),
        }]

    results = []
    for i in range(1, num_features + 1):
        ys, xs = np.where(labeled == i)
        bw = int(xs.max() - xs.min()) + 1
        bh = int(ys.max() - ys.min()) + 1
        # Text regions: width > 20, height between 8 and 80, aspect > 1.5
        if bw < 20 or bh < 6 or bh > 80:
            continue
        if bw / max(bh, 1) < 1.2:
            continue
        x_min, y_min = int(xs.min()), int(ys.min())
        results.append({
            "x": x_min, "y": y_min,
            "width": bw, "height": bh,
            "center_x": x_min + bw // 2,
            "center_y": y_min + bh // 2,
        })

    results.sort(key=lambda r: (r["y"], r["x"]))
    return results[:200]


def find_input_field(screenshot=None):
    """Scan screenshot for input-field patterns: rectangular regions with
    uniform background, thin border (1-2px dark outline), typical input
    dimensions (width > 100, height 20-40).

    Searches the bottom 300px first (common for chat inputs), then full screen.

    Args:
        screenshot: Optional numpy RGB array.  Captured automatically if None.

    Returns:
        List of dicts with keys: x, y, width, height, center_x, center_y,
        confidence (float 0-1).  Sorted by confidence descending.
    """
    import numpy as np

    if screenshot is None:
        screenshot = _grab_screenshot_array()

    gray = np.dot(screenshot[..., :3], [0.299, 0.587, 0.114]).astype(np.uint8)
    img_h, img_w = gray.shape

    def _scan_region(arr, y_offset=0):
        """Detect input-field candidates in a grayscale array."""
        h, w = arr.shape
        # Edge detection: simple horizontal and vertical gradient
        gx = np.abs(arr[:, 1:].astype(np.int16) - arr[:, :-1].astype(np.int16))
        gy = np.abs(arr[1:, :].astype(np.int16) - arr[:-1, :].astype(np.int16))
        # Pad to original size
        gx = np.pad(gx, ((0, 0), (0, 1)), mode='constant')
        gy = np.pad(gy, ((0, 1), (0, 0)), mode='constant')
        edges = np.maximum(gx, gy).astype(np.uint8)

        # Strong edges = border pixels
        border_mask = (edges > 40).astype(np.uint8)

        # Dilate border mask slightly
        dilated = border_mask.copy()
        for s in range(1, 3):
            dilated[:, s:] |= border_mask[:, :-s]
            dilated[:, :-s] |= border_mask[:, s:]
            dilated[s:, :] |= border_mask[:-s, :]
            dilated[:-s, :] |= border_mask[s:, :]

        try:
            from scipy import ndimage
            labeled, num_features = ndimage.label(dilated)
        except ImportError:
            return []

        candidates = []
        for i in range(1, num_features + 1):
            ys, xs = np.where(labeled == i)
            bw = int(xs.max() - xs.min()) + 1
            bh = int(ys.max() - ys.min()) + 1
            x_min, y_min = int(xs.min()), int(ys.min())

            # Input field heuristics
            if bw < 100 or bh < 18 or bh > 60:
                continue
            if bw / max(bh, 1) < 2.5:
                continue

            # Check interior uniformity (should be mostly uniform background)
            inner_y1 = min(y_min + 3, y_min + bh - 1)
            inner_y2 = max(y_min + bh - 3, inner_y1 + 1)
            inner_x1 = min(x_min + 3, x_min + bw - 1)
            inner_x2 = max(x_min + bw - 3, inner_x1 + 1)
            interior = arr[inner_y1:inner_y2, inner_x1:inner_x2]
            if interior.size == 0:
                continue
            uniformity = 1.0 - (float(interior.std()) / 128.0)
            uniformity = max(0.0, min(1.0, uniformity))

            # Confidence based on aspect ratio, size, and uniformity
            aspect_score = min(1.0, (bw / max(bh, 1)) / 10.0)
            size_score = 1.0 if 22 <= bh <= 45 else 0.5
            conf = (uniformity * 0.5 + aspect_score * 0.25 + size_score * 0.25)

            candidates.append({
                "x": x_min, "y": y_min + y_offset,
                "width": bw, "height": bh,
                "center_x": x_min + bw // 2,
                "center_y": y_min + y_offset + bh // 2,
                "confidence": round(conf, 3),
            })
        return candidates

    # Search bottom 300px first
    bottom_start = max(0, img_h - 300)
    results = _scan_region(gray[bottom_start:, :], y_offset=bottom_start)

    # If nothing found in bottom region, scan full screen
    if not results:
        results = _scan_region(gray, y_offset=0)

    results.sort(key=lambda r: r["confidence"], reverse=True)
    return results[:50]


def detect_focused_window():
    """Get the currently focused window's title, position, size, and process name.

    Uses ctypes/win32 API calls.  Windows only.

    Returns:
        dict with keys: hwnd, title, x, y, width, height, process_name, pid.
        Returns None on failure.
    """
    import ctypes
    import ctypes.wintypes

    try:
        user32 = ctypes.windll.user32
        kernel32 = ctypes.windll.kernel32

        hwnd = user32.GetForegroundWindow()
        if not hwnd:
            return None

        # Window title
        length = user32.GetWindowTextLengthW(hwnd)
        buf = ctypes.create_unicode_buffer(length + 1)
        user32.GetWindowTextW(hwnd, buf, length + 1)
        title = buf.value

        # Window rect
        rect = ctypes.wintypes.RECT()
        user32.GetWindowRect(hwnd, ctypes.byref(rect))
        x, y = rect.left, rect.top
        width = rect.right - rect.left
        height = rect.bottom - rect.top

        # Process ID and name
        pid = ctypes.wintypes.DWORD()
        user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
        pid_val = pid.value

        process_name = ""
        try:
            PROCESS_QUERY_INFORMATION = 0x0400
            PROCESS_VM_READ = 0x0010
            h_process = kernel32.OpenProcess(
                PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, False, pid_val
            )
            if h_process:
                try:
                    psapi = ctypes.windll.psapi
                    exe_buf = ctypes.create_unicode_buffer(260)
                    psapi.GetModuleBaseNameW(h_process, None, exe_buf, 260)
                    process_name = exe_buf.value
                finally:
                    kernel32.CloseHandle(h_process)
        except Exception:
            pass

        return {
            "hwnd": hwnd,
            "title": title,
            "x": x, "y": y,
            "width": width, "height": height,
            "process_name": process_name,
            "pid": pid_val,
        }
    except Exception as e:
        return {"error": str(e)}


def get_window_list():
    """List ALL visible windows with their titles, positions, sizes, and
    whether they are focused.

    Uses EnumWindows via ctypes.  Windows only.

    Returns:
        List of dicts with keys: hwnd, title, x, y, width, height,
        is_focused, pid, process_name.
    """
    import ctypes
    import ctypes.wintypes

    try:
        user32 = ctypes.windll.user32
        kernel32 = ctypes.windll.kernel32

        foreground_hwnd = user32.GetForegroundWindow()
        windows = []

        WNDENUMPROC = ctypes.WINFUNCTYPE(
            ctypes.wintypes.BOOL, ctypes.wintypes.HWND, ctypes.wintypes.LPARAM
        )

        def enum_callback(hwnd, lparam):
            if not user32.IsWindowVisible(hwnd):
                return True

            length = user32.GetWindowTextLengthW(hwnd)
            if length == 0:
                return True

            buf = ctypes.create_unicode_buffer(length + 1)
            user32.GetWindowTextW(hwnd, buf, length + 1)
            title = buf.value
            if not title.strip():
                return True

            rect = ctypes.wintypes.RECT()
            user32.GetWindowRect(hwnd, ctypes.byref(rect))

            pid = ctypes.wintypes.DWORD()
            user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
            pid_val = pid.value

            process_name = ""
            try:
                PROCESS_QUERY_INFORMATION = 0x0400
                PROCESS_VM_READ = 0x0010
                h_process = kernel32.OpenProcess(
                    PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, False, pid_val
                )
                if h_process:
                    try:
                        psapi = ctypes.windll.psapi
                        exe_buf = ctypes.create_unicode_buffer(260)
                        psapi.GetModuleBaseNameW(h_process, None, exe_buf, 260)
                        process_name = exe_buf.value
                    finally:
                        kernel32.CloseHandle(h_process)
            except Exception:
                pass

            windows.append({
                "hwnd": hwnd,
                "title": title,
                "x": rect.left, "y": rect.top,
                "width": rect.right - rect.left,
                "height": rect.bottom - rect.top,
                "is_focused": hwnd == foreground_hwnd,
                "pid": pid_val,
                "process_name": process_name,
            })
            return True

        user32.EnumWindows(WNDENUMPROC(enum_callback), 0)
        return windows
    except Exception as e:
        return [{"error": str(e)}]


def smart_click(x, y, verify=True, max_retries=2):
    """Click at coordinates with optional verification and retry.

    If verify=True, takes before/after screenshots and compares the click
    region.  If no change is detected, retries with small offsets (+/- 3px).

    Args:
        x, y: Click coordinates.
        verify: Whether to verify the click had an effect.
        max_retries: Maximum number of retry attempts with offsets.

    Returns:
        dict with keys: success (bool), x, y (final coords), attempts,
        change_percent (float).
    """
    import pyautogui
    import numpy as np

    # Takeover warning before controlling mouse
    if not show_takeover_warning(message=f"Bot 即将点击 ({x}, {y})"):
        return {"success": False, "x": x, "y": y, "attempts": 0,
                "change_percent": None, "cancelled": True}

    x, y = _check_coords(x, y)

    if not verify:
        pyautogui.click(x, y)
        return {"success": True, "x": x, "y": y, "attempts": 1,
                "change_percent": None}

    # Offset patterns for retries: center, then nudge in 4 directions
    offsets = [(0, 0)] + [
        (dx, dy)
        for dx in (-3, 0, 3)
        for dy in (-3, 0, 3)
        if (dx, dy) != (0, 0)
    ]

    for attempt in range(1, max_retries + 2):  # +2 because first attempt counts
        ox, oy = offsets[min(attempt - 1, len(offsets) - 1)]
        cx, cy = _check_coords(x + ox, y + oy)

        before = _grab_screenshot_array()
        pyautogui.click(cx, cy)
        time.sleep(0.35)
        after = _grab_screenshot_array()

        vr = verify_click_result(before, after, click_region=(cx, cy, 120))
        if vr["changed"]:
            return {
                "success": True, "x": cx, "y": cy,
                "attempts": attempt,
                "change_percent": vr["change_percent"],
            }

        if attempt > max_retries:
            break

        # Wait between retries to let UI animations settle
        time.sleep(0.15)

    return {
        "success": False, "x": cx, "y": cy,
        "attempts": max_retries + 1,
        "change_percent": vr["change_percent"],
        "message": f"No visible change after {max_retries + 1} attempts at ({x}, {y}). "
                   "Target may be inactive, already selected, or coordinates are off.",
    }


def find_button_near(x, y, radius=100, screenshot=None):
    """Search a circular region around (x, y) for button-like elements.

    Looks for rectangular regions with distinct color from background and
    text-like content inside.

    Args:
        x, y: Center of the search region.
        radius: Search radius in pixels.
        screenshot: Optional numpy RGB array.  Captured automatically if None.

    Returns:
        dict with keys: found (bool), x, y (center of nearest button),
        width, height.  Returns found=False if none detected.
    """
    import numpy as np

    if screenshot is None:
        screenshot = _grab_screenshot_array()

    img_h, img_w = screenshot.shape[:2]

    # Crop to circular bounding box
    x1 = max(0, x - radius)
    y1 = max(0, y - radius)
    x2 = min(img_w, x + radius)
    y2 = min(img_h, y + radius)
    crop = screenshot[y1:y2, x1:x2]

    if crop.size == 0:
        return {"found": False}

    gray = np.dot(crop[..., :3], [0.299, 0.587, 0.114]).astype(np.uint8)
    ch, cw = gray.shape

    # Edge detection
    gx = np.abs(gray[:, 1:].astype(np.int16) - gray[:, :-1].astype(np.int16))
    gy = np.abs(gray[1:, :].astype(np.int16) - gray[:-1, :].astype(np.int16))
    gx = np.pad(gx, ((0, 0), (0, 1)), mode='constant')
    gy = np.pad(gy, ((0, 1), (0, 0)), mode='constant')
    edges = np.maximum(gx, gy).astype(np.uint8)

    binary = (edges > 30).astype(np.uint8)

    # Dilate
    dilated = binary.copy()
    for s in range(1, 4):
        dilated[:, s:] |= binary[:, :-s]
        dilated[:, :-s] |= binary[:, s:]
        dilated[s:, :] |= binary[:-s, :]
        dilated[:-s, :] |= binary[s:, :]

    try:
        from scipy import ndimage
        labeled, num_features = ndimage.label(dilated)
    except ImportError:
        return {"found": False}

    best = None
    best_dist = float('inf')

    for i in range(1, num_features + 1):
        ys, xs = np.where(labeled == i)
        bw = int(xs.max() - xs.min()) + 1
        bh = int(ys.max() - ys.min()) + 1

        # Button heuristics: reasonable size, aspect ratio
        if bw < 30 or bh < 15 or bh > 70:
            continue
        if bw / max(bh, 1) < 1.2 or bw / max(bh, 1) > 12:
            continue

        bx_min, by_min = int(xs.min()), int(ys.min())
        cx_btn = bx_min + bw // 2 + x1
        cy_btn = by_min + bh // 2 + y1

        # Check interior has text-like content (some dark pixels inside)
        inner = gray[by_min + 2:by_min + bh - 2, bx_min + 2:bx_min + bw - 2]
        if inner.size > 0:
            dark_ratio = (inner < 120).sum() / inner.size
            if dark_ratio < 0.02:  # no text-like content inside
                # Also check for light text on dark bg
                light_ratio = (inner > 180).sum() / inner.size
                if light_ratio < 0.02:
                    continue

        # Distance from search center
        dist = ((cx_btn - x) ** 2 + (cy_btn - y) ** 2) ** 0.5
        if dist <= radius and dist < best_dist:
            best_dist = dist
            best = {
                "found": True,
                "x": cx_btn, "y": cy_btn,
                "width": bw, "height": bh,
            }

    return best if best else {"found": False}


def main():
    import pyautogui
    pyautogui.FAILSAFE = True  # Allow emergency stop by moving mouse to corner
    pyautogui.PAUSE = 0.05  # 50ms between operations (was 100ms)

    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1].lower()

    # Emergency stop check
    try:
        check_emergency_stop()
    except RuntimeError as e:
        print(f"BLOCKED: {e}")
        sys.exit(2)

    # Takeover countdown for mouse/keyboard commands
    if cmd in TAKEOVER_COMMANDS:
        action_desc = " ".join(sys.argv[1:])[:60]
        if not request_takeover(action_desc):
            print("CANCELLED by user")
            sys.exit(1)

    if cmd == "screenshot":
        from PIL import ImageGrab
        # Save to TG forwarding directory so bot auto-sends to Telegram
        tg_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_tg_screenshots")
        os.makedirs(tg_dir, exist_ok=True)
        ts = time.strftime("%Y%m%d_%H%M%S")
        # Use PNG for --region (lossless detail), JPEG for full screen (size-efficient)
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
        img = ImageGrab.grab(bbox=region, all_screens=True)
        # Ensure RGB for JPEG compatibility (ImageGrab can return RGBA)
        if img.mode != "RGB":
            img = img.convert("RGB")
        if region:
            save_path = os.path.join(tg_dir, f"screenshot_{ts}.png")
            img.save(save_path, format="PNG")
        else:
            save_path = os.path.join(tg_dir, f"screenshot_{ts}.jpg")
            img.save(save_path, format="JPEG", quality=85)
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
            tmp_name = None
            try:
                import tempfile
                # Write to temp file, then read via PowerShell to avoid escaping issues
                tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", encoding="utf-8", delete=False)
                tmp_name = tmp.name
                tmp.write(text)
                tmp.close()
                subprocess.run(["powershell", "-NoProfile", "-Command",
                              f"Set-Clipboard -Value (Get-Content -Raw -Encoding UTF8 '{tmp_name}')"],
                              check=True, capture_output=True, timeout=10)
            except Exception:
                # Fallback: direct escaping (single quotes in PS don't interpolate)
                safe = text.replace("'", "''")
                subprocess.run(["powershell", "-NoProfile", "-Command",
                              f"Set-Clipboard -Value '{safe}'"], check=True, capture_output=True, timeout=10)
            finally:
                if tmp_name:
                    try:
                        os.unlink(tmp_name)
                    except OSError:
                        pass
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
        if x is not None and y is not None:
            x, y = _check_coords(x, y)
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
        if not os.path.isfile(image_path):
            print(f"Error: image file not found: {image_path}")
            sys.exit(1)
        try:
            loc = pyautogui.locateOnScreen(image_path, confidence=0.8)
            if loc:
                center = pyautogui.center(loc)
                print(f"Found at: ({center.x}, {center.y})")
                print(f"Region: {loc}")
            else:
                print("Not found on screen")
        except ImportError:
            # opencv-python is required for confidence parameter
            print("Error: opencv-python is required for image matching. Install with: pip install opencv-python")
            sys.exit(1)
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

    elif cmd == "findcolor":
        if len(sys.argv) < 3:
            print("Usage: pc_control.py findcolor R,G,B [tolerance]")
            sys.exit(1)
        parts = sys.argv[2].split(",")
        if len(parts) != 3:
            print("Error: color must be R,G,B (e.g. 255,0,0)")
            sys.exit(1)
        color = tuple(int(c) for c in parts)
        tol = int(sys.argv[3]) if len(sys.argv) > 3 else 20
        results = find_element_by_color(color, tolerance=tol)
        if results:
            print(f"Found {len(results)} element(s) matching RGB{color} (tolerance={tol}):")
            print(json.dumps(results, indent=2))
        else:
            print(f"No elements found matching RGB{color} (tolerance={tol})")

    elif cmd == "verifyclick":
        if len(sys.argv) < 4:
            print("Usage: pc_control.py verifyclick X Y")
            sys.exit(1)
        x, y = _check_coords(int(sys.argv[2]), int(sys.argv[3]))
        before = _grab_screenshot_array()
        pyautogui.click(x, y)
        time.sleep(0.4)  # let UI react
        after = _grab_screenshot_array()
        vr = verify_click_result(before, after, click_region=(x, y, 150))
        print(f"Clicked ({x}, {y})")
        if vr["changed"]:
            print(f"VERIFIED: Screen changed ({vr['change_percent']:.2f}% pixels in region)")
            if vr["changed_region"]:
                r = vr["changed_region"]
                print(f"Changed region: ({r['x']},{r['y']}) {r['width']}x{r['height']}")
        else:
            print(f"WARNING: No visible change detected ({vr['change_percent']:.2f}% pixels). Click may have missed.")

    elif cmd == "smarttype":
        if len(sys.argv) < 3:
            print("Usage: pc_control.py smarttype TEXT")
            sys.exit(1)
        text = sys.argv[2]
        result = smart_type(text, verify=True)
        if result["verified"]:
            print(f"Typed and VERIFIED: '{text}' ({result['change_percent']:.2f}% change)")
        elif result["verified"] is False:
            print(f"WARNING: Typed '{text}' but NO visible change detected. Text may not have landed in a focused input.")
        else:
            print(f"Typed: '{text}' (verification skipped)")

    elif cmd == "findtext":
        results = find_text_region()
        if results:
            print(f"Found {len(results)} text-like region(s):")
            print(json.dumps(results, indent=2))
        else:
            print("No text-like regions found")

    elif cmd == "findinput":
        results = find_input_field()
        if results:
            print(f"Found {len(results)} input field(s):")
            print(json.dumps(results, indent=2))
        else:
            print("No input fields found")

    elif cmd == "focusedwindow":
        result = detect_focused_window()
        if result and "error" not in result:
            print(json.dumps(result, indent=2, ensure_ascii=False))
        elif result and "error" in result:
            print(f"Error: {result['error']}")
        else:
            print("No focused window detected")

    elif cmd == "windowlist":
        results = get_window_list()
        if results and "error" not in results[0]:
            print(f"Found {len(results)} visible window(s):")
            print(json.dumps(results, indent=2, ensure_ascii=False))
        else:
            print(f"Error: {results[0].get('error', 'unknown')}" if results else "No windows found")

    elif cmd == "smartclick":
        if len(sys.argv) < 4:
            print("Usage: pc_control.py smartclick X Y")
            sys.exit(1)
        x, y = int(sys.argv[2]), int(sys.argv[3])
        result = smart_click(x, y, verify=True)
        if result["success"]:
            print(f"Smart-clicked ({result['x']}, {result['y']}) — VERIFIED after {result['attempts']} attempt(s), {result['change_percent']:.2f}% change")
        else:
            print(f"WARNING: Smart-clicked ({result['x']}, {result['y']}) but NO change detected after {result['attempts']} attempt(s)")

    elif cmd == "findbuttonnear":
        if len(sys.argv) < 4:
            print("Usage: pc_control.py findbuttonnear X Y [RADIUS]")
            sys.exit(1)
        x, y = int(sys.argv[2]), int(sys.argv[3])
        radius = int(sys.argv[4]) if len(sys.argv) > 4 else 100
        result = find_button_near(x, y, radius=radius)
        if result.get("found"):
            print(f"Button found at ({result['x']}, {result['y']}), size {result['width']}x{result['height']}")
            print(json.dumps(result, indent=2))
        else:
            print(f"No button found near ({x}, {y}) within radius {radius}")

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)

if __name__ == "__main__":
    main()
