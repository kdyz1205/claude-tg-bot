"""
Adaptive Controller — Intelligent PC control that learns and self-heals.
Builds on pc_control.py with adaptive targeting, failure recovery, and context awareness.
"""

import asyncio
import hashlib
import json
import logging
import os
import time
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

BOT_DIR = os.path.dirname(os.path.abspath(__file__))
PATTERNS_FILE = os.path.join(BOT_DIR, "adaptive_patterns.json")

# ---------------------------------------------------------------------------
# Lazy imports — only pulled in when needed to keep module load fast
# ---------------------------------------------------------------------------

def _import_pyautogui():
    import pyautogui
    pyautogui.FAILSAFE = True  # Allow emergency stop by moving mouse to corner
    pyautogui.PAUSE = 0.05
    return pyautogui

def _import_numpy():
    import numpy as np
    return np

def _import_pil():
    from PIL import Image
    return Image


# ---------------------------------------------------------------------------
# Common UI color signatures for color-based element detection
# ---------------------------------------------------------------------------

COMMON_UI_COLORS = {
    "blue_button":    {"rgb": (0, 120, 215), "tolerance": 35, "description": "Windows/web blue button"},
    "green_button":   {"rgb": (40, 167, 69),  "tolerance": 30, "description": "Success/confirm green button"},
    "red_button":     {"rgb": (220, 53, 69),  "tolerance": 30, "description": "Danger/delete red button"},
    "white_input":    {"rgb": (255, 255, 255), "tolerance": 15, "description": "White input field background"},
    "gray_input":     {"rgb": (240, 240, 240), "tolerance": 20, "description": "Gray input field background"},
    "link_blue":      {"rgb": (0, 102, 204),  "tolerance": 30, "description": "Hyperlink blue text"},
    "send_blue":      {"rgb": (0, 132, 255),  "tolerance": 30, "description": "Messenger/chat send button"},
    "telegram_blue":  {"rgb": (42, 171, 238), "tolerance": 30, "description": "Telegram accent blue"},
}

# Keyword-to-color mapping for description-based lookups
DESCRIPTION_COLOR_HINTS = {
    "send":    ["send_blue", "telegram_blue", "blue_button"],
    "submit":  ["blue_button", "green_button"],
    "confirm": ["green_button", "blue_button"],
    "delete":  ["red_button"],
    "cancel":  ["gray_input"],
    "ok":      ["blue_button", "green_button"],
    "url":     ["white_input", "gray_input"],
    "input":   ["white_input", "gray_input"],
    "search":  ["white_input", "gray_input"],
}

# Window title keywords to detected state mapping
STATE_DETECTION_RULES = {
    "browser_page": [
        "chrome", "firefox", "edge", "brave", "opera", "safari", "vivaldi",
    ],
    "chat_input": [
        "telegram", "whatsapp", "discord", "slack", "messenger", "teams", "signal",
    ],
    "file_dialog": [
        "open", "save as", "select folder", "browse", "file explorer", "explorer",
    ],
    "text_editor": [
        "notepad", "sublime", "vscode", "code", "vim", "nano", "atom", "notepad++",
    ],
    "terminal": [
        "cmd", "powershell", "terminal", "command prompt", "bash", "wt.exe",
        "windows terminal", "windowsterminal",
    ],
}


# ---------------------------------------------------------------------------
# AdaptiveController
# ---------------------------------------------------------------------------

class AdaptiveController:
    """Intelligent PC control that learns UI patterns, self-heals on failure,
    and provides context-aware automation."""

    # Hard caps to prevent unbounded memory growth over days of operation
    _MAX_PATTERNS = 2000
    _MAX_OFFSETS = 500

    def __init__(self):
        # Learned click offsets per application
        self.click_offset_map: dict[str, dict[str, int]] = {}
        # Cached UI pattern locations: pattern_name -> {x, y, app_title, hash, hits, last_used}
        self.known_ui_patterns: dict[str, dict[str, Any]] = {}
        # Rolling failure log (last 500 failures)
        self.failure_log: list[dict] = []
        self._max_failures = 500

        # Temp screenshot directory
        self._screenshot_dir = os.path.join(BOT_DIR, "_adaptive_screenshots")
        try:
            os.makedirs(self._screenshot_dir, exist_ok=True)
        except OSError as exc:
            logger.warning("AdaptiveController: cannot create screenshot dir: %s", exc)

        # Load persisted patterns
        self._load_patterns()

    # =====================================================================
    # Persistence
    # =====================================================================

    def _load_patterns(self) -> None:
        """Load learned UI patterns from disk."""
        try:
            if os.path.exists(PATTERNS_FILE):
                with open(PATTERNS_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if not isinstance(data, dict):
                    logger.warning("AdaptiveController: patterns file has unexpected type, resetting")
                    return
                patterns = data.get("patterns", {})
                offsets = data.get("offsets", {})
                # Validate types to guard against corrupted data
                if isinstance(patterns, dict):
                    self.known_ui_patterns = patterns
                if isinstance(offsets, dict):
                    self.click_offset_map = offsets
                # Enforce size caps on load
                self._prune_patterns()
                logger.info("AdaptiveController: loaded %d patterns, %d offset maps",
                            len(self.known_ui_patterns), len(self.click_offset_map))
        except (json.JSONDecodeError, OSError, ValueError, TypeError) as exc:
            logger.warning("AdaptiveController: failed to load patterns (will start fresh): %s", exc)
            self.known_ui_patterns = {}
            self.click_offset_map = {}

    def _prune_patterns(self) -> None:
        """Evict least-recently-used patterns and offsets to stay within caps."""
        if len(self.known_ui_patterns) > self._MAX_PATTERNS:
            # Keep the most recently used patterns
            sorted_items = sorted(
                self.known_ui_patterns.items(),
                key=lambda kv: kv[1].get("last_used", ""),
                reverse=True,
            )
            self.known_ui_patterns = dict(sorted_items[:self._MAX_PATTERNS])
        if len(self.click_offset_map) > self._MAX_OFFSETS:
            # Keep only the most recent offset entries (no timestamp, so just truncate)
            keys = list(self.click_offset_map.keys())
            for k in keys[: len(keys) - self._MAX_OFFSETS]:
                del self.click_offset_map[k]

    def _save_patterns(self) -> None:
        """Persist learned UI patterns to disk."""
        try:
            self._prune_patterns()
            data = {
                "patterns": self.known_ui_patterns,
                "offsets": self.click_offset_map,
                "saved_at": datetime.now().isoformat(),
            }
            tmp = PATTERNS_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, PATTERNS_FILE)
        except OSError as exc:
            logger.warning("AdaptiveController: failed to save patterns: %s", exc)
            # Clean up stale tmp file
            try:
                os.unlink(PATTERNS_FILE + ".tmp")
            except OSError:
                pass

    # =====================================================================
    # Internal helpers
    # =====================================================================

    _MAX_SCREENSHOTS = 50  # keep at most this many screenshots on disk

    def _take_screenshot(self, path: str = None) -> str:
        """Capture a screenshot and return the file path."""
        if path is None:
            ts = time.strftime("%Y%m%d_%H%M%S")
            path = os.path.join(self._screenshot_dir, f"adaptive_{ts}.png")
        try:
            from screenshots import save_screenshot
            save_screenshot(path)
        except Exception:
            # Fallback: use PIL directly
            try:
                from PIL import ImageGrab
                img = ImageGrab.grab(all_screens=True)
                if img.mode != "RGB":
                    img = img.convert("RGB")
                img.save(path)
            except Exception as exc:
                logger.error("AdaptiveController: screenshot failed: %s", exc)
                raise RuntimeError(f"Cannot capture screenshot: {exc}") from exc

        # Prune old screenshots to prevent unbounded disk growth
        self._prune_screenshots()
        return path

    def _prune_screenshots(self) -> None:
        """Remove oldest screenshots when over the cap."""
        try:
            if not os.path.isdir(self._screenshot_dir):
                return
            files = sorted(
                (os.path.join(self._screenshot_dir, f)
                 for f in os.listdir(self._screenshot_dir)
                 if f.endswith(".png")),
                key=lambda p: os.path.getmtime(p),
            )
            while len(files) > self._MAX_SCREENSHOTS:
                oldest = files.pop(0)
                try:
                    os.remove(oldest)
                except OSError:
                    pass
        except OSError:
            pass

    def _screenshot_hash(self, path: str) -> str:
        """Compute a perceptual hash of a screenshot (downscaled grayscale mean hash)."""
        try:
            np = _import_numpy()
            Image = _import_pil()
            img = Image.open(path).convert("L").resize((16, 16), Image.LANCZOS)
            arr = np.array(img)
            avg = arr.mean()
            bits = (arr > avg).flatten()
            hex_str = "".join("1" if b else "0" for b in bits)
            return hashlib.md5(hex_str.encode()).hexdigest()[:12]
        except Exception:
            return "unknown"

    def _grab_array(self, path: str = None):
        """Grab screenshot as numpy RGB array, optionally from a file path."""
        np = _import_numpy()
        if path and os.path.exists(path):
            Image = _import_pil()
            img = Image.open(path).convert("RGB")
            return np.array(img)
        # Live capture
        from pc_control import _grab_screenshot_array
        return _grab_screenshot_array()

    def _record_failure(self, action: str, target: str, details: dict) -> None:
        """Append to rolling failure log."""
        entry = {
            "ts": datetime.now().isoformat(),
            "action": action,
            "target": target,
            **details,
        }
        self.failure_log.append(entry)
        if len(self.failure_log) > self._max_failures:
            self.failure_log = self.failure_log[-self._max_failures:]

    def _get_focused_window(self) -> dict:
        """Get the currently focused window info, with error handling."""
        try:
            from pc_control import detect_focused_window
            result = detect_focused_window()
            if result and "error" not in result:
                return result
        except Exception as exc:
            logger.debug("detect_focused_window failed: %s", exc)
        return {"title": "", "process_name": "", "hwnd": 0, "x": 0, "y": 0,
                "width": 0, "height": 0, "pid": 0}

    def _pattern_cache_key(self, target_description: str, app_title: str) -> str:
        """Build a cache key for a UI pattern."""
        desc_normalized = target_description.strip().lower()
        app_normalized = app_title.strip().lower()[:60]
        return f"{app_normalized}::{desc_normalized}"

    # =====================================================================
    # 1. adaptive_click
    # =====================================================================

    async def adaptive_click(self, target_description: str,
                             screenshot_path: str = None) -> dict:
        """Click a UI element described in natural language, using multiple
        detection strategies with caching and self-healing.

        Args:
            target_description: Human description like "the Send button" or "the URL bar".
            screenshot_path: Optional path to an existing screenshot. Taken automatically
                             if not provided.

        Returns:
            dict with keys: success, x, y, method_used, attempts, cached.
        """
        # Takeover warning before controlling mouse
        from pc_control import show_takeover_warning
        if not await asyncio.to_thread(
            show_takeover_warning,
            message=f"Bot 即将点击: {target_description[:40]}"
        ):
            return {"success": False, "x": 0, "y": 0,
                    "method_used": "cancelled", "attempts": 0, "cached": False}

        pyautogui = _import_pyautogui()

        # Take screenshot if needed
        if screenshot_path is None or not os.path.exists(screenshot_path):
            screenshot_path = await asyncio.to_thread(self._take_screenshot)

        window_info = await asyncio.to_thread(self._get_focused_window)
        app_title = window_info.get("title", "")
        cache_key = self._pattern_cache_key(target_description, app_title)

        result = {
            "success": False, "x": 0, "y": 0,
            "method_used": "none", "attempts": 0, "cached": False,
        }

        # ------------------------------------------------------------------
        # Strategy A: Check cached pattern
        # ------------------------------------------------------------------
        cached = self.known_ui_patterns.get(cache_key)
        if cached:
            cx, cy = cached["x"], cached["y"]
            # Apply learned offsets if any
            offsets = self.click_offset_map.get(app_title, {})
            cx += offsets.get("x_offset", 0)
            cy += offsets.get("y_offset", 0)

            click_result = await self._try_click(cx, cy, screenshot_path)
            result["attempts"] += 1
            if click_result["changed"]:
                cached["hits"] = cached.get("hits", 0) + 1
                cached["last_used"] = datetime.now().isoformat()
                await asyncio.to_thread(self._save_patterns)
                result.update(success=True, x=cx, y=cy,
                              method_used="cached_pattern", cached=True)
                return result

        # ------------------------------------------------------------------
        # Strategy B: Use find_button_near / find_input_field from pc_control
        # ------------------------------------------------------------------
        desc_lower = target_description.lower()
        screen_array = await asyncio.to_thread(self._grab_array, screenshot_path)

        found_element = None
        method = "none"

        if any(kw in desc_lower for kw in ("button", "send", "submit", "ok",
                                            "cancel", "confirm", "next", "back",
                                            "close", "apply", "save")):
            # Try button detection — scan promising regions
            found_element = await self._find_button_by_scan(
                screen_array, target_description
            )
            if found_element:
                method = "button_scan"
        elif any(kw in desc_lower for kw in ("input", "field", "url", "address",
                                              "search", "bar", "text box", "textbox")):
            # Try input field detection
            found_element = await self._find_input_by_scan(
                screen_array, target_description
            )
            if found_element:
                method = "input_scan"

        # If specific detection failed, try generic button scan
        if not found_element:
            found_element = await self._find_button_by_scan(
                screen_array, target_description
            )
            if found_element:
                method = "generic_button_scan"

        if found_element:
            cx, cy = found_element["x"], found_element["y"]
            click_result = await self._try_click(cx, cy, screenshot_path)
            result["attempts"] += 1
            if click_result["changed"]:
                s_hash = await asyncio.to_thread(self._screenshot_hash, screenshot_path)
                await asyncio.to_thread(
                    self.learn_ui_pattern, cache_key, cx, cy, app_title, s_hash
                )
                result.update(success=True, x=cx, y=cy,
                              method_used=method, cached=False)
                return result

        # ------------------------------------------------------------------
        # Strategy C: Color-based detection for common UI elements
        # ------------------------------------------------------------------
        color_result = await self._find_by_color(desc_lower, screen_array)
        if color_result:
            cx, cy = color_result["x"], color_result["y"]
            click_result = await self._try_click(cx, cy, screenshot_path)
            result["attempts"] += 1
            if click_result["changed"]:
                s_hash = await asyncio.to_thread(self._screenshot_hash, screenshot_path)
                await asyncio.to_thread(
                    self.learn_ui_pattern, cache_key, cx, cy, app_title, s_hash
                )
                result.update(success=True, x=cx, y=cy,
                              method_used="color_detection", cached=False)
                return result

        # ------------------------------------------------------------------
        # Strategy D: Coordinate estimation from description + retries
        # ------------------------------------------------------------------
        estimated = self._estimate_coords_from_description(
            desc_lower, window_info, screen_array.shape
        )
        if estimated:
            for ex, ey in estimated:
                click_result = await self._try_click(ex, ey, screenshot_path)
                result["attempts"] += 1
                if click_result["changed"]:
                    s_hash = await asyncio.to_thread(self._screenshot_hash, screenshot_path)
                    await asyncio.to_thread(
                        self.learn_ui_pattern, cache_key, ex, ey, app_title, s_hash
                    )
                    result.update(success=True, x=ex, y=ey,
                                  method_used="coordinate_estimation", cached=False)
                    return result

        # All strategies failed
        self._record_failure("adaptive_click", target_description, {
            "app_title": app_title,
            "attempts": result["attempts"],
            "strategies_tried": ["cached", "button_scan", "input_scan",
                                 "color_detection", "coordinate_estimation"],
        })
        return result

    async def _try_click(self, x: int, y: int, screenshot_path: str) -> dict:
        """Click at (x, y) and verify the screen changed."""
        try:
            from pc_control import smart_click
            sc_result = await asyncio.to_thread(smart_click, x, y, True, 1)
            return {"changed": sc_result.get("success", False),
                    "change_percent": sc_result.get("change_percent", 0)}
        except Exception as exc:
            logger.debug("_try_click(%d, %d) failed: %s", x, y, exc)
            return {"changed": False, "change_percent": 0}

    async def _find_button_by_scan(self, screen_array, description: str) -> dict | None:
        """Scan the screen for buttons, pick the best match for the description."""
        try:
            from pc_control import find_button_near
            np = _import_numpy()
            img_h, img_w = screen_array.shape[:2]

            # Determine likely regions based on description
            desc_lower = description.lower()
            scan_points = []

            if any(kw in desc_lower for kw in ("send", "submit", "post")):
                # Typically bottom-right of chat windows
                scan_points = [
                    (img_w - 80, img_h - 60),
                    (img_w - 120, img_h - 80),
                    (img_w - 60, img_h - 100),
                    (img_w // 2, img_h - 60),
                ]
            elif any(kw in desc_lower for kw in ("close", "cancel", "back")):
                # Top-right or bottom of dialogs
                scan_points = [
                    (img_w - 40, 20),
                    (img_w // 2 + 100, img_h // 2 + 150),
                    (img_w // 2 - 100, img_h // 2 + 150),
                ]
            elif any(kw in desc_lower for kw in ("ok", "confirm", "apply", "save")):
                # Center-bottom of dialogs
                scan_points = [
                    (img_w // 2, img_h // 2 + 100),
                    (img_w // 2 + 80, img_h // 2 + 100),
                    (img_w // 2, img_h - 80),
                ]
            else:
                # General scan: center area and common button locations
                scan_points = [
                    (img_w // 2, img_h // 2),
                    (img_w // 2, img_h - 80),
                    (img_w - 100, img_h // 2),
                    (100, img_h // 2),
                    (img_w // 2, 60),
                ]

            for sx, sy in scan_points:
                sx = max(0, min(sx, img_w - 1))
                sy = max(0, min(sy, img_h - 1))
                btn = await asyncio.to_thread(
                    find_button_near, sx, sy, radius=150, screenshot=screen_array
                )
                if btn and btn.get("found"):
                    return {"x": btn["x"], "y": btn["y"],
                            "width": btn.get("width", 0),
                            "height": btn.get("height", 0)}

        except Exception as exc:
            logger.debug("_find_button_by_scan failed: %s", exc)
        return None

    async def _find_input_by_scan(self, screen_array, description: str) -> dict | None:
        """Scan for input fields and return the best match."""
        try:
            from pc_control import find_input_field
            results = await asyncio.to_thread(find_input_field, screen_array)
            if not results:
                return None

            desc_lower = description.lower()

            # Heuristic: URL bar is usually near the top
            if any(kw in desc_lower for kw in ("url", "address")):
                top_results = [r for r in results if r.get("y", r.get("center_y", 999)) < 120]
                if top_results:
                    best = max(top_results, key=lambda r: r["confidence"])
                    return {"x": best["center_x"], "y": best["center_y"]}

            # Chat input is usually near the bottom
            if any(kw in desc_lower for kw in ("chat", "message", "type", "send")):
                np = _import_numpy()
                img_h = screen_array.shape[0]
                bottom_results = [r for r in results if r.get("y", r.get("center_y", 0)) > img_h - 300]
                if bottom_results:
                    best = max(bottom_results, key=lambda r: r["confidence"])
                    return {"x": best["center_x"], "y": best["center_y"]}

            # Search bar: usually wide and near the top
            if any(kw in desc_lower for kw in ("search",)):
                wide_top = [r for r in results
                            if r.get("width", 0) > 200 and r.get("y", r.get("center_y", 999)) < screen_array.shape[0] // 3]
                if wide_top:
                    best = max(wide_top, key=lambda r: r["confidence"])
                    return {"x": best["center_x"], "y": best["center_y"]}

            # Default: highest confidence
            best = max(results, key=lambda r: r["confidence"])
            return {"x": best["center_x"], "y": best["center_y"]}

        except Exception as exc:
            logger.debug("_find_input_by_scan failed: %s", exc)
        return None

    async def _find_by_color(self, desc_lower: str, screen_array) -> dict | None:
        """Find a UI element by matching known color signatures."""
        try:
            from pc_control import find_element_by_color
            np = _import_numpy()

            # Determine which colors to try based on description
            color_keys_to_try = []
            for keyword, keys in DESCRIPTION_COLOR_HINTS.items():
                if keyword in desc_lower:
                    color_keys_to_try.extend(keys)

            if not color_keys_to_try:
                # No specific hint — try common button colors
                color_keys_to_try = ["blue_button", "green_button", "send_blue"]

            # Deduplicate preserving order
            seen = set()
            unique_keys = []
            for k in color_keys_to_try:
                if k not in seen:
                    seen.add(k)
                    unique_keys.append(k)

            for color_key in unique_keys:
                color_spec = COMMON_UI_COLORS.get(color_key)
                if not color_spec:
                    continue
                elements = await asyncio.to_thread(
                    find_element_by_color,
                    color_spec["rgb"],
                    color_spec["tolerance"],
                    min_area=80,
                )
                if elements:
                    # Pick the element closest to expected position
                    # For buttons: prefer reasonable sizes
                    for elem in elements:
                        w = elem.get("width", 0)
                        h = elem.get("height", 0)
                        if 20 <= w <= 400 and 15 <= h <= 80:
                            return {"x": elem["x"], "y": elem["y"]}
                    # If no size-filtered match, return the largest
                    return {"x": elements[0]["x"], "y": elements[0]["y"]}

        except Exception as exc:
            logger.debug("_find_by_color failed: %s", exc)
        return None

    def _estimate_coords_from_description(self, desc_lower: str,
                                          window_info: dict,
                                          screen_shape: tuple) -> list[tuple[int, int]]:
        """Estimate screen coordinates based on UI conventions and description.

        Returns a list of (x, y) candidates to try, ordered by likelihood.
        """
        img_h, img_w = screen_shape[:2]
        win_x = window_info.get("x", 0)
        win_y = window_info.get("y", 0)
        win_w = window_info.get("width", img_w)
        win_h = window_info.get("height", img_h)

        # Center of the active window
        wcx = win_x + win_w // 2
        wcy = win_y + win_h // 2

        candidates = []

        if any(kw in desc_lower for kw in ("url", "address bar")):
            # Browser URL bar: top of window, horizontally centered
            candidates = [
                (wcx, win_y + 52),
                (wcx, win_y + 65),
                (wcx - 100, win_y + 52),
            ]
        elif any(kw in desc_lower for kw in ("send", "submit")):
            # Send button: bottom-right of window
            candidates = [
                (win_x + win_w - 60, win_y + win_h - 50),
                (win_x + win_w - 80, win_y + win_h - 40),
                (win_x + win_w - 50, win_y + win_h - 70),
            ]
        elif any(kw in desc_lower for kw in ("close",)):
            # Close button: top-right corner
            candidates = [
                (win_x + win_w - 25, win_y + 12),
                (win_x + win_w - 20, win_y + 8),
            ]
        elif any(kw in desc_lower for kw in ("minimize",)):
            candidates = [
                (win_x + win_w - 75, win_y + 12),
            ]
        elif any(kw in desc_lower for kw in ("maximize",)):
            candidates = [
                (win_x + win_w - 50, win_y + 12),
            ]
        elif any(kw in desc_lower for kw in ("search",)):
            candidates = [
                (wcx, win_y + 80),
                (wcx, win_y + 50),
            ]
        elif any(kw in desc_lower for kw in ("ok", "confirm", "apply")):
            # Dialog OK button: center-bottom
            candidates = [
                (wcx + 50, win_y + win_h - 50),
                (wcx, win_y + win_h - 50),
            ]
        elif any(kw in desc_lower for kw in ("cancel",)):
            candidates = [
                (wcx - 50, win_y + win_h - 50),
                (wcx + 120, win_y + win_h - 50),
            ]
        elif any(kw in desc_lower for kw in ("input", "field", "text box")):
            # Generic input: center of window, slightly below middle
            candidates = [
                (wcx, wcy + 20),
                (wcx, wcy),
            ]
        else:
            # Fallback: center and common positions
            candidates = [
                (wcx, wcy),
                (wcx, win_y + win_h - 60),
                (wcx, win_y + 60),
            ]

        # Clamp all candidates to screen bounds
        clamped = []
        for cx, cy in candidates:
            cx = max(0, min(cx, img_w - 1))
            cy = max(0, min(cy, img_h - 1))
            clamped.append((cx, cy))
        return clamped

    # =====================================================================
    # 2. find_and_type
    # =====================================================================

    async def find_and_type(self, text: str,
                            target_description: str = "input field") -> dict:
        """Find a target input field, click it, clear it, type text, and verify.

        Args:
            text: The text to type.
            target_description: Description of the target input field.

        Returns:
            dict with keys: success, target_found, typed, verified.
        """
        result = {"success": False, "target_found": False,
                  "typed": False, "verified": False}

        # Takeover warning before controlling mouse/keyboard
        from pc_control import show_takeover_warning
        if not await asyncio.to_thread(
            show_takeover_warning,
            message=f"Bot 即将输入文字到: {target_description[:40]}"
        ):
            return result

        try:
            # Step 1: Find and click the target
            click_result = await self.adaptive_click(target_description)
            result["target_found"] = click_result["success"]

            if not click_result["success"]:
                logger.warning("find_and_type: could not find target '%s'",
                               target_description)
                # Try clicking the most likely input field anyway
                try:
                    from pc_control import find_input_field
                    fields = await asyncio.to_thread(find_input_field)
                    if fields:
                        best = max(fields, key=lambda f: f["confidence"])
                        pyautogui = _import_pyautogui()
                        await asyncio.to_thread(
                            pyautogui.click, best["center_x"], best["center_y"]
                        )
                        result["target_found"] = True
                        await asyncio.sleep(0.2)
                except Exception:
                    pass

            if not result["target_found"]:
                return result

            await asyncio.sleep(0.15)

            # Step 2: Clear existing content
            pyautogui = _import_pyautogui()
            await asyncio.to_thread(pyautogui.hotkey, "ctrl", "a")
            await asyncio.sleep(0.05)
            await asyncio.to_thread(pyautogui.press, "delete")
            await asyncio.sleep(0.1)

            # Step 3: Type the text
            before_screenshot = await asyncio.to_thread(self._take_screenshot)
            try:
                from pc_control import smart_type
                type_result = await asyncio.to_thread(smart_type, text, True)
                result["typed"] = type_result.get("typed", False)
                result["verified"] = type_result.get("verified", False)
            except Exception as exc:
                logger.warning("find_and_type: smart_type failed, fallback: %s", exc)
                # Fallback: plain typing
                if any(ord(c) > 127 for c in text):
                    import subprocess
                    # Use stdin piping to avoid shell injection via special chars
                    await asyncio.to_thread(
                        subprocess.run,
                        ["powershell", "-NoProfile", "-Command",
                         "[Console]::InputEncoding=[Text.Encoding]::UTF8;"
                         "$t=[Console]::In.ReadToEnd();"
                         "Set-Clipboard -Value $t"],
                        input=text.encode("utf-8"),
                        check=True, capture_output=True,
                    )
                    await asyncio.to_thread(pyautogui.hotkey, "ctrl", "v")
                else:
                    await asyncio.to_thread(pyautogui.typewrite, text, interval=0.005)
                result["typed"] = True

            # Step 4: Verify by screenshot comparison if not already verified
            if result["typed"] and not result["verified"]:
                await asyncio.sleep(0.3)
                after_screenshot = await asyncio.to_thread(self._take_screenshot)
                try:
                    from screenshots import detect_changes
                    changes = detect_changes(before_screenshot, after_screenshot,
                                             threshold=12, min_region_area=50)
                    result["verified"] = len(changes) > 0
                except Exception:
                    # If detect_changes unavailable, assume typed = verified
                    result["verified"] = True

            result["success"] = result["typed"]
            return result

        except Exception as exc:
            logger.error("find_and_type failed: %s", exc, exc_info=True)
            result["error"] = str(exc)
            return result

    # =====================================================================
    # 3. execute_task_sequence
    # =====================================================================

    async def execute_task_sequence(self, steps: list[dict]) -> list[dict]:
        """Execute a sequence of UI automation steps with self-healing.

        Each step is a dict with at least an "action" key:
            {"action": "click", "target": "Send button"}
            {"action": "type", "text": "hello", "target": "message input"}
            {"action": "hotkey", "keys": ["ctrl", "c"]}
            {"action": "wait", "seconds": 1.0}
            {"action": "wait_for_change", "timeout": 5}
            {"action": "screenshot"}

        Returns:
            List of result dicts, one per step.
        """
        results = []
        context = {}  # Accumulated context from previous steps

        for i, step in enumerate(steps):
            action = step.get("action", "").lower()
            step_result = {
                "step": i,
                "action": action,
                "success": False,
                "error": None,
            }

            try:
                if action == "click":
                    target = step.get("target", "button")
                    cr = await self.adaptive_click(target)
                    step_result["success"] = cr["success"]
                    step_result["details"] = cr
                    context["last_click"] = cr

                elif action == "type":
                    text = step.get("text", "")
                    target = step.get("target", "input field")
                    if context.get("last_click", {}).get("success"):
                        # Previous click already focused an element — just type
                        from pc_control import smart_type
                        pyautogui = _import_pyautogui()
                        await asyncio.to_thread(pyautogui.hotkey, "ctrl", "a")
                        await asyncio.sleep(0.05)
                        await asyncio.to_thread(pyautogui.press, "delete")
                        await asyncio.sleep(0.05)
                        tr = await asyncio.to_thread(smart_type, text, True)
                        step_result["success"] = tr.get("typed", False)
                        step_result["details"] = tr
                    else:
                        tr = await self.find_and_type(text, target)
                        step_result["success"] = tr["success"]
                        step_result["details"] = tr

                elif action == "hotkey":
                    keys = step.get("keys", [])
                    if keys:
                        pyautogui = _import_pyautogui()
                        await asyncio.to_thread(pyautogui.hotkey, *keys)
                        step_result["success"] = True
                    else:
                        step_result["error"] = "No keys specified"

                elif action == "wait":
                    seconds = float(step.get("seconds", 1.0))
                    await asyncio.sleep(seconds)
                    step_result["success"] = True

                elif action == "wait_for_change":
                    timeout = float(step.get("timeout", 10))
                    region = step.get("region")
                    wfc = await self.wait_for_change(region=region, timeout=timeout)
                    step_result["success"] = wfc.get("changed", False)
                    step_result["details"] = wfc

                elif action == "screenshot":
                    path = await asyncio.to_thread(self._take_screenshot)
                    step_result["success"] = True
                    step_result["path"] = path
                    context["last_screenshot"] = path

                elif action == "scroll":
                    clicks = int(step.get("clicks", 3))
                    pyautogui = _import_pyautogui()
                    await asyncio.to_thread(pyautogui.scroll, clicks)
                    step_result["success"] = True

                else:
                    step_result["error"] = f"Unknown action: {action}"

            except Exception as exc:
                step_result["error"] = str(exc)
                logger.warning("execute_task_sequence step %d (%s) failed: %s",
                               i, action, exc)

            # Self-healing: on failure, try once more with a fresh screenshot
            if not step_result["success"] and step_result.get("error") is None:
                if action in ("click", "type") and step.get("_retried") is not True:
                    logger.info("Step %d failed, attempting self-heal retry", i)
                    retry_step = dict(step)
                    retry_step["_retried"] = True
                    await asyncio.sleep(0.5)
                    retry_results = await self.execute_task_sequence([retry_step])
                    if retry_results and retry_results[0].get("success"):
                        step_result = retry_results[0]
                        step_result["self_healed"] = True

            results.append(step_result)

            # Short delay between steps for UI to settle
            if action != "wait":
                await asyncio.sleep(0.15)

        return results

    # =====================================================================
    # 4. learn_ui_pattern
    # =====================================================================

    def learn_ui_pattern(self, pattern_name: str, x: int, y: int,
                         app_title: str, screenshot_hash: str) -> None:
        """Cache a UI element location for future fast lookups.

        Args:
            pattern_name: Unique key for this pattern (usually cache_key).
            x, y: Screen coordinates of the element center.
            app_title: Title of the window where this pattern was found.
            screenshot_hash: Perceptual hash of the screenshot context.
        """
        self.known_ui_patterns[pattern_name] = {
            "x": x,
            "y": y,
            "app_title": app_title,
            "screenshot_hash": screenshot_hash,
            "hits": 1,
            "created": datetime.now().isoformat(),
            "last_used": datetime.now().isoformat(),
        }
        self._save_patterns()
        logger.debug("Learned UI pattern '%s' at (%d, %d) for '%s'",
                      pattern_name, x, y, app_title)

    # =====================================================================
    # 5. get_learned_patterns
    # =====================================================================

    def get_learned_patterns(self) -> dict:
        """Return all learned UI patterns with their hit counts and metadata.

        Returns:
            dict mapping pattern_name -> {x, y, app_title, hits, last_used, ...}
        """
        return {
            name: {**info}
            for name, info in self.known_ui_patterns.items()
        }

    # =====================================================================
    # 6. detect_context
    # =====================================================================

    async def detect_context(self) -> dict:
        """Analyze the current screen to determine what app is active and what
        state it is in.

        Returns:
            dict with keys: app_name, window_title, detected_state,
            available_actions.
        """
        window_info = await asyncio.to_thread(self._get_focused_window)
        title = window_info.get("title", "").lower()
        process = window_info.get("process_name", "").lower()

        # Determine state from window title and process name
        detected_state = "desktop"
        combined = f"{title} {process}"

        for state, keywords in STATE_DETECTION_RULES.items():
            for kw in keywords:
                if kw in combined:
                    detected_state = state
                    break
            if detected_state != "desktop":
                break

        # If no window is focused or it's the desktop itself
        if not title or "program manager" in title:
            detected_state = "desktop"

        # Determine available actions per state
        action_map = {
            "browser_page": [
                {"action": "click", "target": "URL bar"},
                {"action": "type", "target": "URL bar"},
                {"action": "click", "target": "link"},
                {"action": "scroll", "clicks": -3},
                {"action": "hotkey", "keys": ["ctrl", "t"]},
                {"action": "hotkey", "keys": ["ctrl", "w"]},
                {"action": "hotkey", "keys": ["ctrl", "l"]},
                {"action": "hotkey", "keys": ["f5"]},
            ],
            "chat_input": [
                {"action": "type", "target": "message input"},
                {"action": "click", "target": "send button"},
                {"action": "scroll", "clicks": -3},
                {"action": "hotkey", "keys": ["enter"]},
            ],
            "file_dialog": [
                {"action": "type", "target": "file name input"},
                {"action": "click", "target": "open button"},
                {"action": "click", "target": "cancel button"},
            ],
            "text_editor": [
                {"action": "type", "target": "editor area"},
                {"action": "hotkey", "keys": ["ctrl", "s"]},
                {"action": "hotkey", "keys": ["ctrl", "z"]},
                {"action": "hotkey", "keys": ["ctrl", "c"]},
                {"action": "hotkey", "keys": ["ctrl", "v"]},
            ],
            "terminal": [
                {"action": "type", "target": "terminal input"},
                {"action": "hotkey", "keys": ["enter"]},
                {"action": "hotkey", "keys": ["ctrl", "c"]},
            ],
            "desktop": [
                {"action": "click", "target": "desktop icon"},
                {"action": "hotkey", "keys": ["win"]},
                {"action": "click", "target": "taskbar"},
            ],
        }

        return {
            "app_name": window_info.get("process_name", "unknown"),
            "window_title": window_info.get("title", ""),
            "detected_state": detected_state,
            "available_actions": action_map.get(detected_state, []),
            "window_info": {
                "x": window_info.get("x", 0),
                "y": window_info.get("y", 0),
                "width": window_info.get("width", 0),
                "height": window_info.get("height", 0),
                "pid": window_info.get("pid", 0),
            },
        }

    # =====================================================================
    # 7. wait_for_change
    # =====================================================================

    async def wait_for_change(self, region: tuple = None,
                              timeout: float = 10,
                              poll_interval: float = 0.5,
                              _max_timeout: float = 300) -> dict:
        """Wait until the screen (or a region) changes, or until timeout.

        Args:
            region: Optional (x, y, width, height) tuple to monitor.
                    If None, monitors the full screen.
            timeout: Maximum seconds to wait.
            poll_interval: Seconds between polls.

        Returns:
            dict with keys: changed (bool), elapsed (float), change_region (dict|None).
        """
        np = _import_numpy()
        # Clamp timeout to prevent effectively-infinite loops
        timeout = min(max(0.1, timeout), _max_timeout)
        poll_interval = max(0.1, poll_interval)
        start = time.monotonic()

        # Baseline screenshot
        baseline = await asyncio.to_thread(self._grab_array)
        if region:
            rx, ry, rw, rh = region
            img_h, img_w = baseline.shape[:2]
            rx = max(0, min(rx, img_w - 1))
            ry = max(0, min(ry, img_h - 1))
            rx2 = min(img_w, rx + rw)
            ry2 = min(img_h, ry + rh)
            baseline_region = baseline[ry:ry2, rx:rx2].copy()
        else:
            baseline_region = baseline

        while True:
            elapsed = time.monotonic() - start
            if elapsed >= timeout:
                return {"changed": False, "elapsed": round(elapsed, 2),
                        "change_region": None}

            await asyncio.sleep(poll_interval)

            current = await asyncio.to_thread(self._grab_array)
            if region:
                current_region = current[ry:ry2, rx:rx2]
            else:
                current_region = current

            # Quick comparison: check if shapes match first
            if baseline_region.shape != current_region.shape:
                elapsed = time.monotonic() - start
                return {"changed": True, "elapsed": round(elapsed, 2),
                        "change_region": None,
                        "reason": "resolution_changed"}

            diff = np.abs(
                current_region.astype(np.int16) - baseline_region.astype(np.int16)
            )
            pixel_changed = np.any(diff > 15, axis=2)
            change_count = int(pixel_changed.sum())
            total = pixel_changed.size
            pct = (change_count / total * 100) if total > 0 else 0.0

            if pct > 0.1:  # More than 0.1% of pixels changed
                elapsed = time.monotonic() - start
                change_region_info = None
                if change_count > 0:
                    ys, xs = np.where(pixel_changed)
                    x_off = region[0] if region else 0
                    y_off = region[1] if region else 0
                    change_region_info = {
                        "x": int(xs.min()) + x_off,
                        "y": int(ys.min()) + y_off,
                        "width": int(xs.max() - xs.min()) + 1,
                        "height": int(ys.max() - ys.min()) + 1,
                    }
                return {
                    "changed": True,
                    "elapsed": round(elapsed, 2),
                    "change_percent": round(pct, 3),
                    "change_region": change_region_info,
                }

    # =====================================================================
    # 8. suggest_action
    # =====================================================================

    def suggest_action(self, context: dict, goal: str) -> list[dict]:
        """Given the current context and a goal, suggest a sequence of actions.

        Args:
            context: Output from detect_context() or similar dict with
                     app_name, detected_state, window_title.
            goal: Natural language description of what to accomplish
                  (e.g. "open google.com", "send a message saying hello").

        Returns:
            List of step dicts suitable for execute_task_sequence().
        """
        state = context.get("detected_state", "desktop")
        goal_lower = goal.lower()
        steps: list[dict] = []

        # --- Goal: open a URL ---
        if any(kw in goal_lower for kw in ("open ", "go to ", "navigate to ", "visit ")):
            # Extract the URL-like part
            url = goal_lower
            for prefix in ("open ", "go to ", "navigate to ", "visit "):
                if url.startswith(prefix):
                    url = goal[len(prefix):].strip()
                    break

            if state == "browser_page":
                steps = [
                    {"action": "hotkey", "keys": ["ctrl", "l"]},
                    {"action": "wait", "seconds": 0.2},
                    {"action": "type", "text": url, "target": "URL bar"},
                    {"action": "hotkey", "keys": ["enter"]},
                    {"action": "wait_for_change", "timeout": 8},
                ]
            else:
                # Need to open browser first
                steps = [
                    {"action": "hotkey", "keys": ["win", "r"]},
                    {"action": "wait", "seconds": 0.5},
                    {"action": "type", "text": url, "target": "run dialog input"},
                    {"action": "hotkey", "keys": ["enter"]},
                    {"action": "wait_for_change", "timeout": 10},
                ]

        # --- Goal: send a message ---
        elif any(kw in goal_lower for kw in ("send ", "type ", "write ", "message ")):
            # Extract the message text
            message = goal
            for prefix in ("send a message saying ", "send message ", "send ",
                           "type ", "write "):
                if goal_lower.startswith(prefix):
                    message = goal[len(prefix):].strip().strip("'\"")
                    break

            if state == "chat_input":
                steps = [
                    {"action": "click", "target": "message input field"},
                    {"action": "type", "text": message, "target": "message input"},
                    {"action": "hotkey", "keys": ["enter"]},
                ]
            else:
                steps = [
                    {"action": "type", "text": message, "target": "input field"},
                    {"action": "hotkey", "keys": ["enter"]},
                ]

        # --- Goal: save / close ---
        elif "save" in goal_lower:
            steps = [
                {"action": "hotkey", "keys": ["ctrl", "s"]},
                {"action": "wait", "seconds": 0.5},
            ]
            # Handle potential "Save As" dialog
            if "as" in goal_lower:
                filename = goal_lower.split("as")[-1].strip().strip("'\"")
                if filename:
                    steps.extend([
                        {"action": "wait_for_change", "timeout": 3},
                        {"action": "type", "text": filename, "target": "file name input"},
                        {"action": "hotkey", "keys": ["enter"]},
                    ])

        elif "close" in goal_lower:
            steps = [
                {"action": "hotkey", "keys": ["alt", "f4"]},
            ]

        # --- Goal: copy / paste ---
        elif "copy" in goal_lower:
            steps = [
                {"action": "hotkey", "keys": ["ctrl", "c"]},
            ]
        elif "paste" in goal_lower:
            steps = [
                {"action": "hotkey", "keys": ["ctrl", "v"]},
            ]

        # --- Goal: search ---
        elif "search" in goal_lower:
            query = goal_lower.replace("search for ", "").replace("search ", "").strip()
            if state == "browser_page":
                steps = [
                    {"action": "hotkey", "keys": ["ctrl", "l"]},
                    {"action": "wait", "seconds": 0.2},
                    {"action": "type", "text": query, "target": "URL bar"},
                    {"action": "hotkey", "keys": ["enter"]},
                    {"action": "wait_for_change", "timeout": 8},
                ]
            else:
                steps = [
                    {"action": "hotkey", "keys": ["win", "s"]},
                    {"action": "wait", "seconds": 0.5},
                    {"action": "type", "text": query, "target": "search input"},
                    {"action": "wait", "seconds": 1.0},
                ]

        # --- Goal: scroll ---
        elif "scroll" in goal_lower:
            direction = -3 if "down" in goal_lower else 3
            steps = [
                {"action": "scroll", "clicks": direction},
            ]

        # --- Goal: click something ---
        elif "click" in goal_lower:
            target = goal_lower.replace("click on ", "").replace("click ", "").strip()
            steps = [
                {"action": "click", "target": target},
            ]

        # --- Fallback: use learned patterns if any match ---
        else:
            # Check if any learned pattern matches the goal
            for pattern_name, info in self.known_ui_patterns.items():
                if any(word in pattern_name for word in goal_lower.split()):
                    steps = [
                        {"action": "click", "target": goal},
                    ]
                    break

            if not steps:
                # Generic: take screenshot for context
                steps = [
                    {"action": "screenshot"},
                ]
                logger.info("suggest_action: no specific strategy for goal '%s'", goal)

        return steps


# ---------------------------------------------------------------------------
# Module-level singleton (lazy to avoid import-time crashes)
# ---------------------------------------------------------------------------

_adaptive_controller: AdaptiveController | None = None


def get_adaptive_controller() -> AdaptiveController:
    """Get or create the global AdaptiveController instance."""
    global _adaptive_controller
    if _adaptive_controller is None:
        try:
            _adaptive_controller = AdaptiveController()
        except Exception as exc:
            logger.error("AdaptiveController init failed: %s", exc)
            raise
    return _adaptive_controller


# Backwards-compatible alias -- lazy property via module __getattr__
def __getattr__(name: str):
    if name == "adaptive_controller":
        return get_adaptive_controller()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
