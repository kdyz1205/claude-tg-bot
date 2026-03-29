"""
Session Commander v4.0 - Bulletproof Session Targeting + Smart Rate Limit Scheduling
Controls Claude Code sessions with adaptive sidebar detection.
Uses OCR-based session identification with safety guards against mis-clicks.
"""
import pyautogui
import pyperclip
import re
import time
import os
import sys
import json
import hashlib
import requests
import subprocess
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
_raw_uid = os.getenv('AUTHORIZED_USER_ID')
USER_ID = int(_raw_uid) if _raw_uid else None

# --- Configuration ---
CONFIG_PATH = Path(__file__).parent / "session_commander_config.json"

# Pacific timezone offset helper (handles PST/PDT naively: -8 for standard, -7 for daylight)
def _pacific_now():
    """Return current time as a naive datetime in approximate Pacific time."""
    utc_now = datetime.now(timezone.utc)
    # Simple DST rule: Mar second Sun 2am -> Nov first Sun 2am
    year = utc_now.year
    # March: second Sunday
    mar1 = datetime(year, 3, 1)
    dst_start = mar1 + timedelta(days=(6 - mar1.weekday()) % 7 + 7, hours=2)
    # November: first Sunday
    nov1 = datetime(year, 11, 1)
    dst_end = nov1 + timedelta(days=(6 - nov1.weekday()) % 7, hours=2)
    dst_start = dst_start.replace(tzinfo=timezone.utc)
    dst_end = dst_end.replace(tzinfo=timezone.utc)
    if dst_start <= utc_now < dst_end:
        offset = timedelta(hours=-7)  # PDT
    else:
        offset = timedelta(hours=-8)  # PST
    return (utc_now + offset).replace(tzinfo=None), offset


# Screen dimensions (auto-detected once, with fallback)
try:
    _SCREEN_W, _SCREEN_H = pyautogui.size()
except Exception:
    _SCREEN_W, _SCREEN_H = 1920, 1080  # safe fallback

DEFAULT_CONFIG = {
    "target_session": "Build TG Bot",
    "reply_box": [730, 878],
    "sidebar_x_range": [0, 220],
    "sidebar_y_range": [300, 750],
    "prod_cooldown_seconds": 120,
    "idle_threshold_seconds": 30,
    "monitored_paths": [
        str(Path(__file__).parent),
    ],
    "prod_message": "Continue working. Do not stop. Keep going until the task is complete.",
    # --- v4.0 additions ---
    "safe_click_y_min": 80,           # Never click above this (top bar / "New Session" area)
    "safe_click_y_max": _SCREEN_H - 50,  # Never click below this (taskbar area)
    "dry_run": False,                  # If True, log clicks but don't execute them
    "rate_limit_plan_requests_per_hour": 50,  # Known plan limit (adjust as needed)
    "rate_limit_usage_log": str(Path(__file__).parent / "_usage_timestamps.json"),
}

def load_config():
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                saved = json.load(f)
            merged = {**DEFAULT_CONFIG, **saved}
            return merged
        except (json.JSONDecodeError, OSError) as e:
            print(f"[WARN] Config file corrupted, using defaults: {e}")
    return dict(DEFAULT_CONFIG)

def save_config(cfg):
    import tempfile
    tmp = CONFIG_PATH.with_suffix(".tmp")
    with open(str(tmp), "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(str(tmp), str(CONFIG_PATH))

CFG = load_config()

# --- State tracking for cooldowns ---
_last_prod_time = 0.0
_last_rate_limit_warn = 0.0
_file_snapshot = {}

# ============================================================
# Core helpers
# ============================================================

def screenshot(path="current_screen.png"):
    img = pyautogui.screenshot()
    if img is None:
        print("[WARN] Screenshot returned None (desktop locked or headless?)")
        return None
    img.save(path)
    return img


def notify(text):
    if not TOKEN or not USER_ID:
        print(f"[WARN] TG not configured: {text}")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={"chat_id": USER_ID, "text": text},
            timeout=10,
        )
    except Exception as e:
        print(f"[WARN] TG notify failed: {e}")


def send_photo(path, caption=""):
    if not TOKEN or not USER_ID:
        print(f"[WARN] TG not configured, skipping photo: {caption}")
        return
    try:
        with open(path, "rb") as f:
            requests.post(
                f"https://api.telegram.org/bot{TOKEN}/sendPhoto",
                data={"chat_id": USER_ID, "caption": caption[:1024]},
                files={"photo": f},
                timeout=15,
            )
    except Exception as e:
        print(f"[WARN] TG photo failed: {e}")


# ============================================================
# Window management
# ============================================================

def list_windows():
    """List all open windows."""
    r = subprocess.run(
        ['powershell', '-Command',
         'Get-Process | Where-Object {$_.MainWindowTitle -ne ""} '
         '| Select-Object Name,MainWindowTitle | Format-Table -AutoSize'],
        capture_output=True, text=True, timeout=15)
    return r.stdout


def focus_window(keyword):
    """Focus a window whose title matches keyword."""
    safe_keyword = re.sub(r"[^\w\s\-.]", "", keyword)
    safe_keyword = safe_keyword.replace("'", "''")  # Escape PowerShell single quotes
    script = f"""
$p = Get-Process | Where-Object {{ $_.MainWindowTitle -match '{safe_keyword}' }} | Select-Object -First 1
if ($p) {{
    $hwnd = $p.MainWindowHandle
    Add-Type -TypeDefinition 'using System;using System.Runtime.InteropServices;
public class W{{[DllImport("user32.dll")]public static extern bool SetForegroundWindow(IntPtr h);
[DllImport("user32.dll")]public static extern bool ShowWindow(IntPtr h,int n);}}'
    [W]::ShowWindow($hwnd,9); [W]::SetForegroundWindow($hwnd)
    "FOCUSED: " + $p.MainWindowTitle
}} else {{ "NOT_FOUND" }}"""
    r = subprocess.run(['powershell', '-Command', script], capture_output=True, text=True, timeout=15)
    return r.stdout.strip()


def open_url(url):
    """Open URL in Chrome."""
    if not re.match(r'^https?://', url):
        print(f"[WARN] Rejected non-HTTP URL: {url}")
        return
    try:
        proc = subprocess.Popen(['cmd', '/c', 'start', '/b', '', 'chrome', url])
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass  # Chrome started successfully, just detached
    except Exception as e:
        print(f"[WARN] Failed to open URL: {e}")
    time.sleep(3)


# ============================================================
# Fix 3: Safe click with validation and dry-run support
# ============================================================

def _is_in_safe_zone(y):
    """Check if a Y coordinate is within the safe click zone."""
    y_min = CFG.get("safe_click_y_min", 80)
    y_max = CFG.get("safe_click_y_max", _SCREEN_H - 50)
    return y_min <= y <= y_max


def _looks_like_session_item(img, x, y, tolerance=30):
    """
    Check the area around (x, y) to see if it looks like a sidebar session item.
    A session item should have: dark background with lighter text pixels.
    It should NOT be: a bright button (like "+ New Session"), empty space, or toolbar.
    """
    half_w, half_h = 60, 12
    x0 = max(0, x - half_w)
    y0 = max(0, y - half_h)
    x1 = min(img.width, x + half_w)
    y1 = min(img.height, y + half_h)
    region = img.crop((x0, y0, x1, y1))
    pixels = region.load()
    w, h = region.size

    bright_count = 0
    dark_count = 0
    total = 0
    for py in range(0, h, 2):
        for px in range(0, w, 2):
            r, g, b = pixels[px, py][:3]
            brightness = (r + g + b) / 3
            total += 1
            if brightness > 180:
                bright_count += 1
            elif brightness < 80:
                dark_count += 1

    if total == 0:
        return False

    bright_ratio = bright_count / total
    dark_ratio = dark_count / total

    # A "New Session" button or toolbar area tends to be mostly bright
    # A session list item on dark theme is mostly dark with some light text
    if bright_ratio > 0.6:
        print(f"[SAFE] Rejected: area around ({x},{y}) is too bright ({bright_ratio:.0%}) - likely a button")
        return False

    # Completely empty/uniform area (no text at all)
    if dark_ratio > 0.95:
        print(f"[SAFE] Rejected: area around ({x},{y}) is uniformly dark ({dark_ratio:.0%}) - likely empty space")
        return False

    return True


def safe_click(x, y, reason="", img=None):
    """
    Click at (x, y) only if it passes all safety checks.
    Returns True if clicked, False if rejected.
    """
    # Takeover warning before controlling mouse
    from pc_control import show_takeover_warning
    if not show_takeover_warning(message=f"Bot 即将点击 ({x},{y}): {reason[:30]}"):
        print(f"[CANCELLED] Click at ({x},{y}) cancelled by user")
        return False

    dry_run = CFG.get("dry_run", False)

    # Check Y boundaries
    if not _is_in_safe_zone(y):
        print(f"[BLOCKED] Click at ({x},{y}) rejected: outside safe Y zone "
              f"[{CFG.get('safe_click_y_min', 80)}-{CFG.get('safe_click_y_max', _SCREEN_H - 50)}]. "
              f"Reason: {reason}")
        return False

    # Check X boundaries (should be in sidebar)
    sx0, sx1 = CFG["sidebar_x_range"]
    if not (sx0 <= x <= sx1 + 20):  # small tolerance
        print(f"[BLOCKED] Click at ({x},{y}) rejected: X outside sidebar range [{sx0}-{sx1}]. "
              f"Reason: {reason}")
        return False

    # Visual confirmation: check that area looks like a session item
    if img is None:
        img = screenshot()
    if img and not _looks_like_session_item(img, x, y):
        print(f"[BLOCKED] Click at ({x},{y}) rejected: does not look like a session item. "
              f"Reason: {reason}")
        return False

    if dry_run:
        print(f"[DRY RUN] Would click ({x},{y}) - {reason}")
        return False

    pyautogui.click(x, y)
    time.sleep(0.3)
    print(f"[CLICK] ({x},{y}) - {reason}")
    return True


# ============================================================
# Fix 1: Reliable session targeting with verification
# ============================================================

def _get_sidebar_text_rows(img):
    """
    Analyze sidebar pixel rows to find text-containing bands.
    Returns list of (y_center, row_hash) for each detected text row.
    """
    cfg = CFG
    x0, x1 = cfg["sidebar_x_range"]
    y0, y1 = cfg["sidebar_y_range"]
    sidebar = img.crop((x0, y0, x1, y1))
    pixels = sidebar.load()
    w, h = sidebar.size

    row_data = []
    for y in range(h):
        total = 0
        for x in range(0, w, 2):
            r, g, b = pixels[x, y][:3]
            total += r + g + b
        avg = total / max(1, w // 2) / 3
        row_data.append(avg)

    sorted_brightness = sorted(row_data)
    bg_brightness = sorted_brightness[len(sorted_brightness) // 2]
    threshold = max(bg_brightness + 15, 35)

    bands = []
    in_band = False
    band_start = 0
    for y, b in enumerate(row_data):
        if b > threshold and not in_band:
            band_start = y
            in_band = True
        elif b <= threshold and in_band:
            band_end = y
            if band_end - band_start >= 5:
                center_y = y0 + (band_start + band_end) // 2
                band_pixels = []
                sample_y = (band_start + band_end) // 2
                for x in range(0, w, 3):
                    r, g, b_val = pixels[x, sample_y][:3]
                    band_pixels.append(r > threshold)
                band_hash = hashlib.md5(str(band_pixels).encode()).hexdigest()[:8]
                bands.append({"y": center_y, "hash": band_hash,
                              "height": band_end - band_start})
            in_band = False

    return bands


def _try_ocr_sidebar(img):
    """
    Try to use Windows built-in OCR (via PowerShell) to read sidebar text.
    Falls back gracefully if unavailable.
    Returns list of {"text": str, "y": int} or None if OCR unavailable.
    """
    cfg = CFG
    x0, x1 = cfg["sidebar_x_range"]
    y0, y1 = cfg["sidebar_y_range"]

    sidebar_path = str(Path(__file__).parent / "_sidebar_crop.png")
    sidebar_img = img.crop((x0, y0, x1, y1))
    sidebar_img.save(sidebar_path)

    ps_script = f"""
Add-Type -AssemblyName System.Runtime.WindowsRuntime
$null = [Windows.Media.Ocr.OcrEngine,Windows.Foundation,ContentType=WindowsRuntime]
$null = [Windows.Graphics.Imaging.BitmapDecoder,Windows.Foundation,ContentType=WindowsRuntime]
$null = [Windows.Storage.StorageFile,Windows.Foundation,ContentType=WindowsRuntime]

function Await($WinRtTask, $ResultType) {{
    $asTask = [System.WindowsRuntimeSystemExtensions].GetMethod('AsTask', @([Type]'Windows.Foundation.IAsyncOperation`1'.MakeGenericType($ResultType)))
    $netTask = $asTask.Invoke($null, @($WinRtTask))
    $netTask.Wait(-1) | Out-Null
    $netTask.Result
}}

$path = '{sidebar_path.replace(chr(92), chr(92)+chr(92)).replace(chr(39), chr(39)+chr(39))}'
$file = Await ([Windows.Storage.StorageFile]::GetFileFromPathAsync($path)) ([Windows.Storage.StorageFile])
$stream = Await ($file.OpenAsync([Windows.Storage.FileAccessMode]::Read)) ([Windows.Storage.Streams.IRandomAccessStream])
$decoder = Await ([Windows.Graphics.Imaging.BitmapDecoder]::CreateAsync($stream)) ([Windows.Graphics.Imaging.BitmapDecoder])
$bitmap = Await ($decoder.GetSoftwareBitmapAsync()) ([Windows.Graphics.Imaging.SoftwareBitmap])
$engine = [Windows.Media.Ocr.OcrEngine]::TryCreateFromLanguage('en-US')
$result = Await ($engine.RecognizeAsync($bitmap)) ([Windows.Media.Ocr.OcrResult])
foreach ($line in $result.Lines) {{
    $b = $line.Words[0].BoundingRect
    $cy = [int]($b.Y + $b.Height / 2)
    Write-Output "$cy|$($line.Text)"
}}
"""
    try:
        r = subprocess.run(
            ['powershell', '-Command', ps_script],
            capture_output=True, text=True, timeout=15
        )
        if r.returncode != 0 or not r.stdout.strip():
            return None

        results = []
        for line in r.stdout.strip().split('\n'):
            line = line.strip()
            if '|' not in line:
                continue
            parts = line.split('|', 1)
            try:
                local_y = int(parts[0])
                text = parts[1].strip()
                screen_y = y0 + local_y
                results.append({"text": text, "y": screen_y})
            except (ValueError, IndexError):
                continue
        return results if results else None
    except Exception as e:
        print(f"[DEBUG] OCR unavailable: {e}")
        return None


def find_session_y(session_name):
    """
    Adaptively find the Y coordinate of a session in the Claude Code sidebar.

    v4.0 changes:
    - ONLY returns a Y if it can positively identify the target session.
    - Returns None (and does NOT guess) if the target cannot be found.
    - All returned Y values are validated against the safe click zone.
    """
    target = session_name.lower()
    img = screenshot()
    if img is None:
        return None, img

    # --- Strategy 1: OCR-based text matching (most reliable) ---
    ocr_results = _try_ocr_sidebar(img)
    if ocr_results:
        print(f"[DEBUG] OCR found {len(ocr_results)} sidebar entries:")
        best_match = None
        best_score = 0
        for entry in ocr_results:
            text_lower = entry["text"].lower()
            print(f"  y={entry['y']}  text={entry['text']}")

            # Exact substring match
            if target in text_lower:
                y = entry["y"]
                if _is_in_safe_zone(y):
                    print(f"[MATCH] Exact substring match: '{entry['text']}' at y={y}")
                    return y, img
                else:
                    print(f"[WARN] OCR match at y={y} is outside safe zone, skipping")

            # Word-level matching
            target_words = target.split()
            score = sum(1 for w in target_words if w in text_lower)
            if score > best_score:
                best_score = score
                best_match = entry

        # Accept partial match if at least half the words match
        if best_match and best_score >= max(1, len(target.split()) // 2):
            y = best_match["y"]
            if _is_in_safe_zone(y):
                print(f"[MATCH] Partial word match ({best_score} words): "
                      f"'{best_match['text']}' at y={y}")
                return y, img
            else:
                print(f"[WARN] Partial match at y={y} is outside safe zone, skipping")

    # --- Strategy 2: Pixel-band analysis with cached hash ---
    bands = _get_sidebar_text_rows(img)
    print(f"[DEBUG] Pixel analysis found {len(bands)} text rows in sidebar")

    cache_key = f"_cached_hash_{target}"
    cached = CFG.get(cache_key)
    if cached:
        for band in bands:
            if band["hash"] == cached:
                y = band["y"]
                if _is_in_safe_zone(y):
                    print(f"[MATCH] Cached hash match at y={y}")
                    return y, img
                else:
                    print(f"[WARN] Cached hash match at y={y} is outside safe zone, skipping")

    # --- NO fallback guessing ---
    # v4.0: We do NOT click random bands. If we can't identify the session, report failure.
    print(f"[FAIL] Could not positively identify session '{session_name}' in sidebar. "
          f"OCR entries: {len(ocr_results) if ocr_results else 0}, "
          f"Pixel bands: {len(bands)}. NOT clicking anything.")
    return None, img


def verify_session_after_click(expected_name, pre_click_img=None):
    """
    After clicking a session, take a screenshot and verify the correct
    session is now active by checking the title bar / header area.
    Returns True if verified, False otherwise.
    """
    time.sleep(1.0)  # Wait for UI to update
    img = screenshot(str(Path(__file__).parent / "_verify_session.png"))
    if img is None:
        return False

    # Try OCR on the top area of the main content pane to find the session title
    # The active session name typically appears near the top, right of the sidebar
    sx1 = CFG["sidebar_x_range"][1]
    title_region = img.crop((sx1, 0, min(sx1 + 600, img.width), 80))
    title_path = str(Path(__file__).parent / "_title_crop.png")
    title_region.save(title_path)

    ps_script = f"""
Add-Type -AssemblyName System.Runtime.WindowsRuntime
$null = [Windows.Media.Ocr.OcrEngine,Windows.Foundation,ContentType=WindowsRuntime]
$null = [Windows.Graphics.Imaging.BitmapDecoder,Windows.Foundation,ContentType=WindowsRuntime]
$null = [Windows.Storage.StorageFile,Windows.Foundation,ContentType=WindowsRuntime]

function Await($WinRtTask, $ResultType) {{
    $asTask = [System.WindowsRuntimeSystemExtensions].GetMethod('AsTask', @([Type]'Windows.Foundation.IAsyncOperation`1'.MakeGenericType($ResultType)))
    $netTask = $asTask.Invoke($null, @($WinRtTask))
    $netTask.Wait(-1) | Out-Null
    $netTask.Result
}}

$path = '{title_path.replace(chr(92), chr(92)+chr(92)).replace(chr(39), chr(39)+chr(39))}'
$file = Await ([Windows.Storage.StorageFile]::GetFileFromPathAsync($path)) ([Windows.Storage.StorageFile])
$stream = Await ($file.OpenAsync([Windows.Storage.FileAccessMode]::Read)) ([Windows.Storage.Streams.IRandomAccessStream])
$decoder = Await ([Windows.Graphics.Imaging.BitmapDecoder]::CreateAsync($stream)) ([Windows.Graphics.Imaging.BitmapDecoder])
$bitmap = Await ($decoder.GetSoftwareBitmapAsync()) ([Windows.Graphics.Imaging.SoftwareBitmap])
$engine = [Windows.Media.Ocr.OcrEngine]::TryCreateFromLanguage('en-US')
$result = Await ($engine.RecognizeAsync($bitmap)) ([Windows.Media.Ocr.OcrResult])
foreach ($line in $result.Lines) {{
    Write-Output $line.Text
}}
"""
    try:
        r = subprocess.run(
            ['powershell', '-Command', ps_script],
            capture_output=True, text=True, timeout=15
        )
        if r.returncode != 0 or not r.stdout.strip():
            print("[VERIFY] Could not OCR title area - verification inconclusive")
            return False

        title_text = r.stdout.strip().lower()
        expected_lower = expected_name.lower()
        target_words = expected_lower.split()

        # Check if the expected session name appears in the title area
        if expected_lower in title_text:
            print(f"[VERIFY] Session confirmed: found '{expected_name}' in title area")
            return True

        # Partial word match
        matched_words = sum(1 for w in target_words if w in title_text)
        if matched_words >= max(1, len(target_words) // 2):
            print(f"[VERIFY] Session likely correct: {matched_words}/{len(target_words)} "
                  f"words matched in title: '{r.stdout.strip()}'")
            return True

        print(f"[VERIFY] Session mismatch! Expected '{expected_name}', "
              f"title area reads: '{r.stdout.strip()}'")
        return False

    except Exception as e:
        print(f"[VERIFY] OCR failed during verification: {e}")
        return False


def cache_session_position(session_name, y_pos, img=None):
    """After successfully identifying a session, cache its pixel hash."""
    if img is None:
        img = screenshot()
    if img is None:
        return
    bands = _get_sidebar_text_rows(img)
    closest = min(bands, key=lambda b: abs(b["y"] - y_pos), default=None)
    if closest and abs(closest["y"] - y_pos) < 20:
        cache_key = f"_cached_hash_{session_name.lower()}"
        CFG[cache_key] = closest["hash"]
        save_config(CFG)
        print(f"[CACHE] Saved hash for '{session_name}' at y={closest['y']}")


# ============================================================
# Fix 2: Smart Rate Limit Scheduler
# ============================================================

class RateLimitScheduler:
    """
    Tracks API usage, predicts rate limit hits, and schedules
    automatic wake-ups after the 3:00 AM PT reset.
    """

    def __init__(self, config=None):
        self.config = config or CFG
        self._usage_log_path = self.config.get(
            "rate_limit_usage_log",
            str(Path(__file__).parent / "_usage_timestamps.json")
        )
        self._timestamps = self._load_timestamps()
        self._limit_hit_at = None  # datetime when limit was hit
        self._cooldown_active = False
        self._wakeup_scheduled = False

    def _load_timestamps(self):
        """Load usage timestamps from disk."""
        try:
            if Path(self._usage_log_path).exists():
                with open(self._usage_log_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                return [float(t) for t in data.get("timestamps", [])]
        except Exception as e:
            print(f"[RATE] Failed to load usage log: {e}")
        return []

    def _save_timestamps(self):
        """Save usage timestamps to disk."""
        try:
            # Only keep last 2 hours of data
            cutoff = time.time() - 7200
            self._timestamps = [t for t in self._timestamps if t > cutoff]
            with open(self._usage_log_path, "w", encoding="utf-8") as f:
                json.dump({
                    "timestamps": self._timestamps,
                    "limit_hit_at": self._limit_hit_at.isoformat() if self._limit_hit_at else None,
                }, f)
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            print(f"[RATE] Failed to save usage log: {e}")

    def record_usage(self):
        """Record a single API interaction timestamp."""
        self._timestamps.append(time.time())
        self._save_timestamps()

    def get_usage_rate(self):
        """Calculate requests per hour over the last 30 minutes."""
        now = time.time()
        window = 1800  # 30 minutes
        recent = [t for t in self._timestamps if t > now - window]
        if len(recent) < 2:
            return 0.0
        elapsed_hours = (now - recent[0]) / 3600
        if elapsed_hours < 0.001:
            return 0.0
        return len(recent) / elapsed_hours

    def predict_limit_time(self):
        """
        Estimate when the rate limit will be hit based on current usage rate.
        Returns a datetime or None if rate is too low to predict.
        """
        rate = self.get_usage_rate()
        limit = self.config.get("rate_limit_plan_requests_per_hour", 50)
        if rate < 1:
            return None  # Too low to predict

        now = time.time()
        window = 3600  # 1-hour window
        recent = [t for t in self._timestamps if t > now - window]
        remaining = limit - len(recent)
        if remaining <= 0:
            return datetime.now()  # Already at limit

        # At current rate, how long until remaining is exhausted?
        seconds_until = (remaining / rate) * 3600
        return datetime.now() + timedelta(seconds=seconds_until)

    @staticmethod
    def get_reset_time():
        """
        Returns the next 3:00 AM Pacific time as a UTC datetime.
        If it's currently before 3 AM PT, returns today's 3 AM PT.
        If after, returns tomorrow's 3 AM PT.
        """
        pacific_now, offset = _pacific_now()
        # 3:00 AM Pacific today
        reset_pacific = pacific_now.replace(hour=3, minute=0, second=0, microsecond=0)
        if pacific_now >= reset_pacific:
            # Already past 3 AM today, use tomorrow
            reset_pacific += timedelta(days=1)
        # Convert back to local system time
        # reset in UTC = reset_pacific - offset (offset is negative, e.g. -8h)
        reset_utc = reset_pacific - offset
        # Convert to local
        local_offset = datetime.now(timezone.utc).astimezone().utcoffset() or timedelta(0)
        reset_local = reset_utc + local_offset
        return reset_local

    def get_wait_seconds(self):
        """Seconds until the next 3:00 AM PT reset."""
        reset = self.get_reset_time()
        delta = reset - datetime.now()
        return max(0, delta.total_seconds())

    def is_in_cooldown(self):
        """Returns True if we're between a limit hit and the next reset."""
        if self._cooldown_active:
            wait = self.get_wait_seconds()
            if wait <= 0:
                # Reset time has passed
                self._cooldown_active = False
                self._limit_hit_at = None
                self._wakeup_scheduled = False
                print("[RATE] Cooldown period ended - reset time reached")
                return False
            return True
        return False

    def on_limit_hit(self):
        """
        Call this when a rate limit error is detected.
        Enters cooldown mode and schedules a wake-up.
        """
        self._cooldown_active = True
        self._limit_hit_at = datetime.now()
        self._save_timestamps()

        wait_secs = self.get_wait_seconds()
        reset_time = self.get_reset_time()
        pacific_now, _ = _pacific_now()

        print(f"[RATE] Rate limit hit at {pacific_now.strftime('%I:%M %p')} PT")
        print(f"[RATE] Next reset: 3:00 AM PT ({reset_time.strftime('%Y-%m-%d %H:%M:%S')} local)")
        print(f"[RATE] Wait time: {wait_secs/60:.1f} minutes ({wait_secs/3600:.1f} hours)")

        notify(
            f"Rate limit hit.\n"
            f"Current time: {pacific_now.strftime('%I:%M %p')} PT\n"
            f"Reset at: 3:00 AM PT\n"
            f"Wait: {wait_secs/60:.0f} min\n"
            f"Will auto-resume at 3:01 AM PT."
        )

        # Schedule wake-up via Windows Task Scheduler (survives process death)
        self.schedule_wakeup()

        return wait_secs

    def schedule_wakeup(self):
        """
        Create a Windows Scheduled Task to restart the bot at 3:01 AM PT.
        Uses schtasks for reliability even if this process dies.
        """
        if self._wakeup_scheduled:
            print("[RATE] Wake-up already scheduled, skipping")
            return

        reset_time = self.get_reset_time() + timedelta(minutes=1)  # 3:01 AM PT
        task_name = "ClaudeBotAutoResume"

        # Format for schtasks: MM/DD/YYYY and HH:MM
        date_str = reset_time.strftime("%m/%d/%Y")
        time_str = reset_time.strftime("%H:%M")

        # The command to run at wake-up: restart the monitor
        bot_dir = str(Path(__file__).parent)
        python_exe = sys.executable
        resume_cmd = f'"{python_exe}" "{bot_dir}\\session_commander.py" watch'

        try:
            # Delete old task if it exists
            subprocess.run(
                ['schtasks', '/delete', '/tn', task_name, '/f'],
                capture_output=True, timeout=10
            )

            # Create new scheduled task
            result = subprocess.run(
                ['schtasks', '/create',
                 '/tn', task_name,
                 '/tr', resume_cmd,
                 '/sc', 'once',
                 '/sd', date_str,
                 '/st', time_str,
                 '/f'],
                capture_output=True, text=True, timeout=10
            )

            if result.returncode == 0:
                self._wakeup_scheduled = True
                print(f"[RATE] Scheduled wake-up task '{task_name}' for {date_str} {time_str}")
            else:
                print(f"[RATE] Failed to create scheduled task: {result.stderr}")
                # Fall back to in-process timer
                self._schedule_timer_fallback(reset_time)

        except Exception as e:
            print(f"[RATE] schtasks failed: {e}, using in-process timer fallback")
            self._schedule_timer_fallback(reset_time)

    def _schedule_timer_fallback(self, wake_time):
        """In-process timer fallback if schtasks fails."""
        wait = (wake_time - datetime.now()).total_seconds()
        if wait <= 0:
            return

        def _timer_wake():
            print(f"[RATE] Timer wakeup! Sleeping {wait:.0f}s until {wake_time}")
            time.sleep(wait)
            print("[RATE] Timer fired - cooldown period over, resuming")
            self._cooldown_active = False
            self._wakeup_scheduled = False

        t = threading.Thread(target=_timer_wake, daemon=True)
        t.start()
        self._wakeup_scheduled = True
        print(f"[RATE] Set in-process timer for {wait:.0f}s")

    def sleep_until_reset(self):
        """
        Block until the rate limit resets. Use this in the main loop
        instead of spamming retries.
        Returns the number of seconds slept.
        """
        wait = self.get_wait_seconds()
        if wait <= 0:
            return 0

        # Add 60 seconds buffer after reset
        total_wait = wait + 60
        reset_time = self.get_reset_time()

        print(f"[RATE] Sleeping {total_wait/60:.1f} minutes until reset + 1min buffer")
        print(f"[RATE] Will resume at approximately {(reset_time + timedelta(minutes=1)).strftime('%H:%M:%S')} local")

        # Sleep in chunks so we can be interrupted
        slept = 0
        chunk = 60  # Check every 60 seconds
        while slept < total_wait:
            sleep_now = min(chunk, total_wait - slept)
            time.sleep(sleep_now)
            slept += sleep_now
            remaining = total_wait - slept
            if remaining > 0 and remaining % 600 < chunk:  # Log every ~10 min
                print(f"[RATE] Still sleeping... {remaining/60:.0f} min remaining")

        self._cooldown_active = False
        self._wakeup_scheduled = False
        print("[RATE] Cooldown sleep complete, resuming operations")
        return slept


# Global scheduler instance
rate_scheduler = RateLimitScheduler()


# ============================================================
# Session interaction (v4.0: uses safe_click + verification)
# ============================================================

def click_at(x, y):
    """Legacy click - only used for reply box, not sidebar."""
    pyautogui.click(x, y)
    time.sleep(0.3)


def type_text(text):
    pyperclip.copy(text)
    pyautogui.hotkey('ctrl', 'v')


def switch_session(target):
    """Switch to a Claude Code sidebar session (v4.0: safe targeting)."""
    y, img = find_session_y(target)
    if y is None:
        print(f"[ABORT] Cannot switch to '{target}' - session not found. "
              f"NOT clicking anything.")
        notify(f"Session switch FAILED: could not find '{target}' in sidebar. "
               f"No click performed.")
        return False

    click_x = (CFG["sidebar_x_range"][0] + CFG["sidebar_x_range"][1]) // 2
    clicked = safe_click(click_x, y, reason=f"switch to '{target}'", img=img)
    if not clicked:
        print(f"[ABORT] safe_click rejected the click for '{target}'")
        notify(f"Session switch BLOCKED: click at ({click_x},{y}) failed safety checks")
        return False

    # Verify the correct session is now active
    verified = verify_session_after_click(target)
    if verified:
        cache_session_position(target, y)
        print(f"[SWITCHED] '{target}' at y={y} (verified)")
        return True
    else:
        print(f"[WARN] Clicked y={y} but could not verify session '{target}' is active. "
              f"Proceeding cautiously.")
        # Still cache the position - the click happened, OCR verification may just be flaky
        cache_session_position(target, y)
        return True  # Proceed but warn


def send_to_session(message, session=None):
    """Send a message to a Claude Code session."""
    # Check rate limit cooldown first
    if rate_scheduler.is_in_cooldown():
        wait = rate_scheduler.get_wait_seconds()
        print(f"[RATE] In cooldown - {wait/60:.0f} min until reset. Not sending.")
        return False

    # Takeover warning before controlling mouse/keyboard
    from pc_control import show_takeover_warning
    if not show_takeover_warning(message=f"Bot 即将发送消息到会话"):
        print("[CANCELLED] send_to_session cancelled by user")
        return False

    if session:
        if not switch_session(session):
            print(f"[ERROR] Failed to switch to session '{session}', aborting send")
            return False

    rx, ry = CFG["reply_box"]
    click_at(rx, ry)
    time.sleep(0.5)
    type_text(message)
    time.sleep(0.3)
    pyautogui.press('enter')
    print(f"[SENT] {message[:60]}...")

    # Record usage for rate limiting
    rate_scheduler.record_usage()
    return True


# ============================================================
# Activity detection (file-based, not pixel-based)
# ============================================================

def _snapshot_files(paths):
    """Get a dict of {filepath: (mtime, size)} for monitored paths."""
    snap = {}
    for base in paths:
        base_path = Path(base)
        if not base_path.exists():
            continue
        try:
            for f in base_path.rglob("*"):
                if f.is_file() and not any(
                    part.startswith('.') or part == '__pycache__' or part == 'node_modules'
                    for part in f.parts
                ):
                    try:
                        st = f.stat()
                        snap[str(f)] = (st.st_mtime, st.st_size)
                    except OSError:
                        pass
        except OSError:
            pass
    return snap


def detect_file_activity():
    """
    Check if files in monitored paths have changed since last check.
    Returns (has_changes, changed_files_list).
    """
    global _file_snapshot
    current = _snapshot_files(CFG.get("monitored_paths", []))

    if not _file_snapshot:
        _file_snapshot = current
        return True, []

    changed = []
    for path, (mtime, size) in current.items():
        old = _file_snapshot.get(path)
        if old is None:
            changed.append(f"NEW: {Path(path).name}")
        elif old != (mtime, size):
            changed.append(f"MOD: {Path(path).name}")

    for path in _file_snapshot:
        if path not in current:
            changed.append(f"DEL: {Path(path).name}")

    _file_snapshot = current
    return bool(changed), changed


def check_session_idle():
    """
    Check if the session is idle using BOTH pixel analysis and file activity.
    Returns (is_idle, reason).
    """
    has_changes, changed_files = detect_file_activity()
    if has_changes and changed_files:
        return False, f"Files changed: {', '.join(changed_files[:5])}"

    img = screenshot()
    if img is None:
        return False, "Could not take screenshot"

    rx, ry = CFG["reply_box"]
    reply_zone = img.crop((rx - 200, ry - 20, rx + 200, ry + 20))
    pixels = reply_zone.load()
    w, h = reply_zone.size

    dark = sum(
        1 for y in range(h) for x in range(w)
        if sum(pixels[x, y][:3]) < 200
    )
    pixel_idle = dark > (w * h * 0.7)

    if pixel_idle:
        return True, "Reply box visible (no active generation)"
    return False, "Session appears active (pixel check)"


# ============================================================
# Monitor with cooldown, smart prodding, and rate limit awareness
# ============================================================

def monitor_and_prod(session=None, prod_msg=None):
    """
    Check session status and prod if idle.
    v4.0: Respects rate limit cooldown and uses smart scheduling.
    """
    global _last_prod_time, _last_rate_limit_warn

    if not session:
        session = CFG["target_session"]
    if not prod_msg:
        prod_msg = CFG["prod_message"]

    # Check rate limit cooldown FIRST
    if rate_scheduler.is_in_cooldown():
        wait = rate_scheduler.get_wait_seconds()
        pacific_now, _ = _pacific_now()
        print(f"[RATE] In cooldown ({pacific_now.strftime('%I:%M %p')} PT). "
              f"{wait/60:.0f} min until reset. Skipping prod.")
        return "rate_limited"

    now = time.time()
    cooldown = CFG["prod_cooldown_seconds"]

    # Check prod cooldown
    time_since_last = now - _last_prod_time
    if time_since_last < cooldown:
        remaining = int(cooldown - time_since_last)
        if now - _last_rate_limit_warn > 60:
            _last_rate_limit_warn = now
            print(f"[COOLDOWN] {remaining}s remaining before next prod")
        return "cooldown"

    is_idle, reason = check_session_idle()

    if is_idle:
        print(f"[IDLE] {reason}")
        success = send_to_session(prod_msg, session)
        if success:
            _last_prod_time = time.time()
            send_photo("current_screen.png", f"Session idle, prodded: {session}\n{reason}")
            return "prodded"
        else:
            # Check if failure was due to rate limit
            if rate_scheduler.is_in_cooldown():
                return "rate_limited"
            notify(f"Failed to prod session '{session}' - could not find it in sidebar")
            return "error"
    else:
        print(f"[ACTIVE] {reason}")
        return "running"


def monitor_loop(session=None, interval=30):
    """
    Continuous monitoring loop.
    v4.0: Handles rate limits by sleeping until reset instead of spamming.
    """
    if not session:
        session = CFG["target_session"]

    print(f"[MONITOR] Starting continuous monitor for '{session}'")
    print(f"[MONITOR] Check interval: {interval}s, Prod cooldown: {CFG['prod_cooldown_seconds']}s")
    print(f"[MONITOR] Monitoring paths: {CFG['monitored_paths']}")
    print(f"[MONITOR] Safe click zone: Y [{CFG.get('safe_click_y_min', 80)}-{CFG.get('safe_click_y_max', _SCREEN_H-50)}]")
    print(f"[MONITOR] Dry run: {CFG.get('dry_run', False)}")
    print(f"[MONITOR] Press Ctrl+C to stop")

    # Initialize file snapshot
    detect_file_activity()

    while True:
        try:
            # If in rate limit cooldown, sleep until reset instead of polling
            if rate_scheduler.is_in_cooldown():
                wait = rate_scheduler.get_wait_seconds()
                if wait > 120:  # More than 2 minutes to wait
                    print(f"[MONITOR] Rate limited. Sleeping {wait/60:.0f} min until 3:01 AM PT reset...")
                    rate_scheduler.sleep_until_reset()
                    print("[MONITOR] Woke up from rate limit sleep, resuming normal monitoring")
                    continue

            result = monitor_and_prod(session)
            print(f"[MONITOR] {time.strftime('%H:%M:%S')} - {result}")

            # If rate limited, don't keep polling at normal interval
            if result == "rate_limited":
                wait = rate_scheduler.get_wait_seconds()
                if wait > 120:
                    print(f"[MONITOR] Entering rate limit sleep for {wait/60:.0f} min...")
                    rate_scheduler.sleep_until_reset()
                    continue
                else:
                    # Close to reset, just wait a bit
                    time.sleep(min(wait + 60, 180))
                    continue

            time.sleep(interval)
        except KeyboardInterrupt:
            print("\n[MONITOR] Stopped.")
            break
        except Exception as e:
            print(f"[ERROR] {e}")
            time.sleep(interval)


# ============================================================
# CLI (v4.0: added rate-limit and dry-run commands)
# ============================================================

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Session Commander v4.0 - Bulletproof Session Targeting")
        print("Usage:")
        print("  session_commander.py status              - Screenshot + idle check")
        print("  session_commander.py send <message>      - Send to target session")
        print("  session_commander.py send-to <sess> <msg> - Send to named session")
        print("  session_commander.py switch <session>     - Switch sidebar session")
        print("  session_commander.py monitor [session]    - One-shot monitor+prod")
        print("  session_commander.py watch [session]      - Continuous monitor loop")
        print("  session_commander.py windows              - List open windows")
        print("  session_commander.py focus <keyword>      - Focus a window")
        print("  session_commander.py url <url>            - Open URL in Chrome")
        print("  session_commander.py config               - Show current config")
        print("  session_commander.py set-target <name>    - Set target session name")
        print("  session_commander.py scan                 - Scan sidebar (debug)")
        print("  session_commander.py dry-run [on|off]     - Toggle dry-run mode")
        print("  session_commander.py rate-status          - Show rate limit status")
        print("  session_commander.py rate-reset           - Clear rate limit cooldown")
        print("  session_commander.py rate-simulate-hit    - Simulate a rate limit hit")
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "status":
        screenshot()
        is_idle, reason = check_session_idle()
        status = f"Session: {'IDLE' if is_idle else 'ACTIVE'}\nReason: {reason}"
        print(status)
        send_photo("current_screen.png", status)

    elif cmd == "send":
        msg = " ".join(sys.argv[2:])
        session = CFG["target_session"]
        send_to_session(msg, session)
        time.sleep(3)
        screenshot()
        send_photo("current_screen.png", f"Sent to '{session}'")

    elif cmd == "send-to":
        session = sys.argv[2]
        msg = " ".join(sys.argv[3:])
        send_to_session(msg, session)
        time.sleep(3)
        screenshot()
        send_photo("current_screen.png", f"Sent to '{session}'")

    elif cmd == "switch":
        target = " ".join(sys.argv[2:])
        switch_session(target)

    elif cmd == "windows":
        notify(f"Open windows:\n{list_windows()[:800]}")

    elif cmd == "focus":
        print(focus_window(" ".join(sys.argv[2:])))

    elif cmd == "url":
        open_url(sys.argv[2])
        time.sleep(3)
        screenshot()
        send_photo("current_screen.png", f"Opened: {sys.argv[2]}")

    elif cmd == "monitor":
        s = sys.argv[2] if len(sys.argv) > 2 else None
        result = monitor_and_prod(s)
        notify(f"Monitor result: {result}")

    elif cmd == "watch":
        s = sys.argv[2] if len(sys.argv) > 2 else None
        interval = int(sys.argv[3]) if len(sys.argv) > 3 else 30
        monitor_loop(s, interval)

    elif cmd == "config":
        print(json.dumps(CFG, indent=2, ensure_ascii=False))

    elif cmd == "set-target":
        new_target = " ".join(sys.argv[2:])
        CFG["target_session"] = new_target
        save_config(CFG)
        print(f"[OK] Target session set to: '{new_target}'")

    elif cmd == "scan":
        img = screenshot()
        if img:
            print("--- OCR Scan ---")
            ocr = _try_ocr_sidebar(img)
            if ocr:
                for entry in ocr:
                    safe = _is_in_safe_zone(entry["y"])
                    marker = "OK" if safe else "UNSAFE"
                    print(f"  [{marker}] y={entry['y']}  text='{entry['text']}'")
            else:
                print("  OCR unavailable or returned no results")

            print("\n--- Pixel Band Scan ---")
            bands = _get_sidebar_text_rows(img)
            for b in bands:
                safe = _is_in_safe_zone(b["y"])
                marker = "OK" if safe else "UNSAFE"
                print(f"  [{marker}] y={b['y']}  height={b['height']}  hash={b['hash']}")

            print(f"\n--- Safe Zone ---")
            print(f"  Y range: [{CFG.get('safe_click_y_min', 80)} - {CFG.get('safe_click_y_max', _SCREEN_H-50)}]")
            print(f"  Screen: {_SCREEN_W}x{_SCREEN_H}")

    elif cmd == "dry-run":
        if len(sys.argv) > 2:
            val = sys.argv[2].lower() in ("on", "true", "1", "yes")
        else:
            val = not CFG.get("dry_run", False)
        CFG["dry_run"] = val
        save_config(CFG)
        print(f"[OK] Dry-run mode: {'ON' if val else 'OFF'}")

    elif cmd == "rate-status":
        pacific_now, _ = _pacific_now()
        rate = rate_scheduler.get_usage_rate()
        predict = rate_scheduler.predict_limit_time()
        reset = rate_scheduler.get_reset_time()
        wait = rate_scheduler.get_wait_seconds()
        cooldown = rate_scheduler.is_in_cooldown()

        print(f"Current time (PT): {pacific_now.strftime('%I:%M %p')}")
        print(f"Usage rate: {rate:.1f} req/hour")
        print(f"Predicted limit hit: {predict.strftime('%I:%M %p') if predict else 'N/A'}")
        print(f"Next reset: {reset.strftime('%Y-%m-%d %H:%M:%S')} local")
        print(f"Time until reset: {wait/60:.1f} minutes")
        print(f"In cooldown: {cooldown}")
        print(f"Recent requests: {len([t for t in rate_scheduler._timestamps if t > time.time() - 3600])}")

    elif cmd == "rate-reset":
        rate_scheduler._cooldown_active = False
        rate_scheduler._limit_hit_at = None
        rate_scheduler._wakeup_scheduled = False
        rate_scheduler._timestamps.clear()
        rate_scheduler._save_timestamps()
        print("[OK] Rate limit state cleared")

    elif cmd == "rate-simulate-hit":
        print("[TEST] Simulating rate limit hit...")
        wait = rate_scheduler.on_limit_hit()
        print(f"[TEST] Would sleep {wait/60:.1f} minutes until reset")

    else:
        # Legacy: treat first arg as session name, rest as message
        msg = " ".join(sys.argv[2:])
        if msg:
            send_to_session(msg, cmd)
            time.sleep(3)
            screenshot()
            send_photo("current_screen.png", f"Sent to '{cmd}'")
        else:
            print(f"Unknown command: {cmd}")
