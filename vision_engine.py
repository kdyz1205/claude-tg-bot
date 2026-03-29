"""
vision_engine.py — Precision Computer Control Engine

Three-tier approach to solve imprecise coordinate-based clicking:

Tier 1: Set-of-Mark (SoM) — annotate screenshots with numbered bounding boxes
         so Claude picks element #7 instead of guessing x=542, y=318.

Tier 2: OmniParser — ONNX-based UI element detection model for higher accuracy
         element detection than edge-based heuristics.

Tier 3: Windows Accessibility Tree — extract the UI automation tree for desktop
         apps (like DOM for the desktop). Click by automation ID, zero guessing.
"""
import asyncio
import json
import logging
import os
import re
import subprocess
import threading
import time
from io import BytesIO
from pathlib import Path
from typing import Any

import cv2
import numpy as np
from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

BOT_DIR = Path(__file__).parent
_SOM_COLORS = [
    (255, 0, 0), (0, 180, 0), (0, 100, 255), (255, 165, 0),
    (148, 0, 211), (0, 206, 209), (255, 20, 147), (50, 205, 50),
    (255, 215, 0), (30, 144, 255), (220, 20, 60), (0, 128, 128),
    (255, 127, 80), (106, 90, 205), (60, 179, 113), (199, 21, 133),
    (255, 99, 71), (0, 191, 255), (173, 255, 47), (186, 85, 211),
]

# OmniParser model paths (downloaded on demand)
_OMNI_DIR = BOT_DIR / ".omniparser"
_OMNI_DET_MODEL = _OMNI_DIR / "icon_detect.onnx"
_OMNI_CLS_MODEL = _OMNI_DIR / "icon_classify.onnx"


# ═════════════════════════════════════════════════════════════════════════════
# TIER 1 — Set-of-Mark (SoM) Annotation
# ═════════════════════════════════════════════════════════════════════════════

def _detect_ui_elements_cv(image: np.ndarray, min_area: int = 300) -> list[dict]:
    """Detect UI elements using OpenCV contour analysis.

    More robust than pure edge detection — combines:
    1. Canny edge detection with adaptive thresholds
    2. Morphological closing to form solid bounding regions
    3. Contour hierarchy analysis to skip inner contours
    4. Aspect ratio / size / position heuristics
    """
    h, w = image.shape[:2]
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Adaptive threshold for varying background brightness
    blurred = cv2.GaussianBlur(gray, (3, 3), 0)
    edges = cv2.Canny(blurred, 30, 120)

    # Close gaps to form solid regions (buttons, inputs, cards)
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (15, 7))
    closed = cv2.morphologyEx(edges, cv2.MORPH_CLOSE, kernel)

    # Additional dilation to merge nearby elements
    kernel2 = cv2.getStructuringElement(cv2.MORPH_RECT, (5, 3))
    dilated = cv2.dilate(closed, kernel2, iterations=1)

    contours, hierarchy = cv2.findContours(
        dilated, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE,
    )

    elements = []
    seen_rects = set()

    for i, cnt in enumerate(contours):
        # Skip inner contours (children of other contours)
        if hierarchy is not None and hierarchy[0][i][3] != -1:
            # Allow one level of nesting
            parent = hierarchy[0][i][3]
            if hierarchy[0][parent][3] != -1:
                continue

        x, y, bw, bh = cv2.boundingRect(cnt)
        area = bw * bh

        # Filter by size
        if area < min_area:
            continue
        if bw < 15 or bh < 10:
            continue
        if bw > w * 0.95 and bh > h * 0.95:
            continue  # Skip full-screen contour

        # Deduplicate overlapping rectangles
        key = (x // 10, y // 10, bw // 10, bh // 10)
        if key in seen_rects:
            continue
        seen_rects.add(key)

        aspect = bw / max(bh, 1)

        # Classify element type
        if 1.5 < aspect < 10 and 20 <= bh <= 55:
            kind = "button"
        elif aspect > 3 and 15 <= bh <= 50:
            kind = "input"
        elif bw > 150 and bh > 100:
            kind = "card"
        elif bw > 80 and 12 <= bh <= 35:
            kind = "text"
        else:
            kind = "element"

        elements.append({
            "x": x, "y": y, "w": bw, "h": bh,
            "cx": x + bw // 2, "cy": y + bh // 2,
            "area": area, "kind": kind,
        })

    # Sort: top-to-bottom, left-to-right
    elements.sort(key=lambda e: (e["y"] // 20, e["x"]))
    return elements[:80]  # Cap at 80 elements


def _get_font(size: int = 14):
    """Get a font for annotation, with fallback."""
    font_paths = [
        "C:/Windows/Fonts/consola.ttf",
        "C:/Windows/Fonts/arial.ttf",
        "C:/Windows/Fonts/segoeui.ttf",
    ]
    for fp in font_paths:
        if os.path.exists(fp):
            try:
                return ImageFont.truetype(fp, size)
            except Exception:
                pass
    return ImageFont.load_default()


def annotate_screenshot_som(
    image: Image.Image | np.ndarray | BytesIO | str,
    elements: list[dict] | None = None,
    max_elements: int = 50,
) -> tuple[Image.Image, list[dict]]:
    """Annotate a screenshot with numbered Set-of-Mark bounding boxes.

    Args:
        image: PIL Image, numpy array, BytesIO buffer, or file path
        elements: Pre-detected elements. If None, auto-detects.
        max_elements: Maximum elements to annotate

    Returns:
        (annotated_image, element_list) where element_list has id/x/y/w/h/cx/cy/kind
    """
    # Convert input to PIL Image and numpy array
    if isinstance(image, str):
        pil_img = Image.open(image).convert("RGB")
    elif isinstance(image, BytesIO):
        image.seek(0)
        pil_img = Image.open(image).convert("RGB")
    elif isinstance(image, np.ndarray):
        pil_img = Image.fromarray(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))
    else:
        pil_img = image.convert("RGB")

    cv_img = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)

    # Auto-detect elements if not provided
    if elements is None:
        elements = _detect_ui_elements_cv(cv_img)

    elements = elements[:max_elements]

    # Number each element
    for idx, elem in enumerate(elements):
        elem["id"] = idx + 1

    # Draw annotations on a copy
    annotated = pil_img.copy()
    draw = ImageDraw.Draw(annotated)
    font = _get_font(11)

    for elem in elements:
        eid = elem["id"]
        color = _SOM_COLORS[(eid - 1) % len(_SOM_COLORS)]
        x, y, bw, bh = elem["x"], elem["y"], elem["w"], elem["h"]

        # Draw bounding box (2px border)
        draw.rectangle([x, y, x + bw, y + bh], outline=color, width=2)

        # Draw numbered label (top-left corner, small filled badge)
        label = str(eid)
        lw = len(label) * 8 + 6
        lh = 16
        lx = max(0, x - 1)
        ly = max(0, y - lh - 1)

        draw.rectangle([lx, ly, lx + lw, ly + lh], fill=color)
        draw.text((lx + 3, ly + 1), label, fill=(255, 255, 255), font=font)

    return annotated, elements


def som_screenshot(region: list | None = None) -> tuple[BytesIO, BytesIO, list[dict]]:
    """Take a screenshot and return both raw and SoM-annotated versions.

    Returns:
        (raw_buffer, annotated_buffer, elements)
    """
    from screenshots import capture_screenshot

    raw_buf = capture_screenshot(region=region)
    raw_buf.seek(0)
    pil_img = Image.open(raw_buf).convert("RGB")

    annotated_img, elements = annotate_screenshot_som(pil_img)

    # Save annotated to buffer
    ann_buf = BytesIO()
    annotated_img.save(ann_buf, format="JPEG", quality=85)
    ann_buf.seek(0)
    ann_buf.name = "screenshot_annotated.jpg"

    raw_buf.seek(0)
    return raw_buf, ann_buf, elements


def som_click(element_id: int, elements: list[dict], button: str = "left", clicks: int = 1) -> str:
    """Click the center of a SoM-identified element by its ID number.

    Args:
        element_id: The number shown on the annotated screenshot
        elements: Element list from annotate_screenshot_som or som_screenshot
        button: left/right/middle
        clicks: 1 for single, 2 for double

    Returns:
        Result string
    """
    import pyautogui

    target = None
    for e in elements:
        if e.get("id") == element_id:
            target = e
            break

    if not target:
        return f"Error: Element #{element_id} not found. Available: {[e['id'] for e in elements[:20]]}"

    cx, cy = target["cx"], target["cy"]
    pyautogui.click(cx, cy, button=button, clicks=clicks)

    return (
        f"Clicked element #{element_id} ({target['kind']}) at ({cx}, {cy}), "
        f"size {target['w']}x{target['h']}"
    )


# ═════════════════════════════════════════════════════════════════════════════
# TIER 2 — OmniParser Integration (ONNX-based UI Element Detection)
# ═════════════════════════════════════════════════════════════════════════════

class OmniParser:
    """ONNX-based UI element detection.

    Uses a YOLO-style object detection model trained on UI elements.
    Falls back to OpenCV detection if ONNX model is not available.
    """

    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get(cls) -> "OmniParser":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def __init__(self):
        self._det_session = None
        self._cls_session = None
        self._initialized = False

    def is_available(self) -> bool:
        """Check if OmniParser models are downloaded."""
        return _OMNI_DET_MODEL.exists()

    def _load(self):
        """Load ONNX models. Called lazily."""
        if self._initialized:
            return

        if not self.is_available():
            logger.info("OmniParser models not found, using OpenCV fallback")
            self._initialized = True
            return

        try:
            import onnxruntime as ort
            self._det_session = ort.InferenceSession(
                str(_OMNI_DET_MODEL),
                providers=["CUDAExecutionProvider", "CPUExecutionProvider"],
            )
            if _OMNI_CLS_MODEL.exists():
                self._cls_session = ort.InferenceSession(
                    str(_OMNI_CLS_MODEL),
                    providers=["CUDAExecutionProvider", "CPUExecutionProvider"],
                )
            logger.info("OmniParser ONNX models loaded")
        except Exception as e:
            logger.warning(f"OmniParser ONNX load failed: {e}")
            self._det_session = None

        self._initialized = True

    def detect(self, image: np.ndarray, conf_threshold: float = 0.3) -> list[dict]:
        """Detect UI elements using ONNX model or OpenCV fallback.

        Args:
            image: BGR numpy array
            conf_threshold: Confidence threshold for ONNX detection

        Returns:
            List of element dicts with x, y, w, h, cx, cy, kind, confidence
        """
        self._load()

        if self._det_session is not None:
            return self._detect_onnx(image, conf_threshold)

        # Fallback: enhanced OpenCV detection
        return self._detect_opencv_enhanced(image)

    def _detect_onnx(self, image: np.ndarray, conf_threshold: float) -> list[dict]:
        """ONNX YOLO-style detection."""
        h, w = image.shape[:2]

        # Preprocess: resize to 640x640, normalize
        input_size = 640
        scale_x = w / input_size
        scale_y = h / input_size

        resized = cv2.resize(image, (input_size, input_size))
        blob = resized.astype(np.float32) / 255.0
        blob = blob.transpose(2, 0, 1)  # HWC -> CHW
        blob = np.expand_dims(blob, axis=0)  # Add batch dim

        input_name = self._det_session.get_inputs()[0].name
        outputs = self._det_session.run(None, {input_name: blob})

        # Parse YOLO output: [batch, num_boxes, 5+classes]
        # or [batch, 5+classes, num_boxes] depending on model
        raw = outputs[0]
        if raw.ndim == 3:
            if raw.shape[1] < raw.shape[2]:
                raw = raw.transpose(0, 2, 1)
            raw = raw[0]  # Remove batch dim

        elements = []
        for det in raw:
            if len(det) < 5:
                continue

            # Could be [cx, cy, w, h, conf, ...classes] or [x1, y1, x2, y2, conf, ...]
            if len(det) >= 6:
                cx, cy, dw, dh = det[0], det[1], det[2], det[3]
                conf = det[4]
                class_scores = det[5:]
                if len(class_scores) > 0:
                    conf *= float(np.clip(max(class_scores), 0, 1))
            else:
                cx, cy, dw, dh, conf = det[:5]

            if conf < conf_threshold:
                continue

            # Scale back to original image coords
            x1 = int((cx - dw / 2) * scale_x)
            y1 = int((cy - dh / 2) * scale_y)
            bw = int(dw * scale_x)
            bh = int(dh * scale_y)

            # Clamp to image bounds
            x1 = max(0, min(x1, w - 1))
            y1 = max(0, min(y1, h - 1))
            bw = min(bw, w - x1)
            bh = min(bh, h - y1)

            if bw < 10 or bh < 8:
                continue

            kind = self._classify_element(bw, bh)

            elements.append({
                "x": x1, "y": y1, "w": bw, "h": bh,
                "cx": x1 + bw // 2, "cy": y1 + bh // 2,
                "area": bw * bh, "kind": kind,
                "confidence": round(float(conf), 3),
            })

        # NMS: remove overlapping detections
        elements = self._nms(elements, iou_threshold=0.4)
        elements.sort(key=lambda e: (e["y"] // 20, e["x"]))
        return elements[:80]

    def _detect_opencv_enhanced(self, image: np.ndarray) -> list[dict]:
        """Enhanced OpenCV detection combining multiple strategies."""
        h, w = image.shape[:2]
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

        all_elements = []

        # Strategy 1: Canny edges + contours (general UI elements)
        all_elements.extend(_detect_ui_elements_cv(image, min_area=200))

        # Strategy 2: Color-based button detection
        # Detect saturated colored regions (typically buttons)
        hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
        # High saturation = colored UI elements
        sat_mask = hsv[:, :, 1] > 80
        val_mask = hsv[:, :, 2] > 60
        color_mask = (sat_mask & val_mask).astype(np.uint8) * 255

        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (10, 5))
        color_closed = cv2.morphologyEx(color_mask, cv2.MORPH_CLOSE, kernel)

        contours, _ = cv2.findContours(
            color_closed, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE,
        )
        for cnt in contours:
            x, y, bw, bh = cv2.boundingRect(cnt)
            if bw * bh < 400 or bw < 20 or bh < 15:
                continue
            if bw > w * 0.9 and bh > h * 0.9:
                continue
            all_elements.append({
                "x": x, "y": y, "w": bw, "h": bh,
                "cx": x + bw // 2, "cy": y + bh // 2,
                "area": bw * bh, "kind": "button",
            })

        # Strategy 3: Input field detection via horizontal line patterns
        # Input fields often have a bottom border or full rectangular border
        h_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (40, 1))
        h_edges = cv2.morphologyEx(
            cv2.Canny(gray, 50, 150), cv2.MORPH_CLOSE, h_kernel,
        )
        h_contours, _ = cv2.findContours(
            h_edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE,
        )
        for cnt in h_contours:
            x, y, bw, bh = cv2.boundingRect(cnt)
            if bw > 100 and 2 <= bh <= 50:
                # Check if there's a uniform region above (the input field body)
                input_top = max(0, y - 30)
                region = gray[input_top:y, x:x + bw]
                if region.size > 0 and np.std(region) < 30:
                    all_elements.append({
                        "x": x, "y": input_top, "w": bw, "h": y - input_top + bh,
                        "cx": x + bw // 2, "cy": (input_top + y + bh) // 2,
                        "area": bw * (y - input_top + bh), "kind": "input",
                    })

        # Deduplicate
        all_elements = self._nms(all_elements, iou_threshold=0.3)
        all_elements.sort(key=lambda e: (e["y"] // 20, e["x"]))
        return all_elements[:80]

    @staticmethod
    def _classify_element(w: int, h: int) -> str:
        aspect = w / max(h, 1)
        if 1.5 < aspect < 10 and 20 <= h <= 55:
            return "button"
        if aspect > 3 and 15 <= h <= 50:
            return "input"
        if w > 150 and h > 100:
            return "card"
        if w > 80 and 12 <= h <= 35:
            return "text"
        return "element"

    @staticmethod
    def _nms(elements: list[dict], iou_threshold: float = 0.4) -> list[dict]:
        """Non-maximum suppression: remove overlapping detections."""
        if not elements:
            return []

        # Sort by area descending (keep larger elements) — copy to avoid mutating input
        elements = sorted(elements, key=lambda e: e.get("area", 0), reverse=True)
        keep = []

        for elem in elements:
            overlaps = False
            for kept in keep:
                iou = _compute_iou(elem, kept)
                if iou > iou_threshold:
                    overlaps = True
                    break
            if not overlaps:
                keep.append(elem)

        return keep


def _compute_iou(a: dict, b: dict) -> float:
    """Compute intersection-over-union between two element rects."""
    ax1, ay1 = a["x"], a["y"]
    ax2, ay2 = ax1 + a["w"], ay1 + a["h"]
    bx1, by1 = b["x"], b["y"]
    bx2, by2 = bx1 + b["w"], by1 + b["h"]

    ix1 = max(ax1, bx1)
    iy1 = max(ay1, by1)
    ix2 = min(ax2, bx2)
    iy2 = min(ay2, by2)

    if ix1 >= ix2 or iy1 >= iy2:
        return 0.0

    inter = (ix2 - ix1) * (iy2 - iy1)
    area_a = a["w"] * a["h"]
    area_b = b["w"] * b["h"]
    union = area_a + area_b - inter

    return inter / max(union, 1)


async def download_omniparser_models():
    """Download OmniParser ONNX models from HuggingFace.

    This is a one-time download (~100MB). Models are cached in .omniparser/.
    """
    _OMNI_DIR.mkdir(exist_ok=True)

    # HuggingFace model URLs for OmniParser
    models = {
        "icon_detect.onnx": "https://huggingface.co/microsoft/OmniParser/resolve/main/icon_detect/model.onnx",
    }

    import urllib.request
    for filename, url in models.items():
        dest = _OMNI_DIR / filename
        if dest.exists():
            logger.info(f"OmniParser model already exists: {filename}")
            continue

        logger.info(f"Downloading OmniParser model: {filename} ...")
        try:
            urllib.request.urlretrieve(url, str(dest))
            if dest.exists():
                logger.info(f"Downloaded: {filename} ({dest.stat().st_size // 1024}KB)")
            else:
                logger.warning(f"Download failed for {filename}")
        except Exception as e:
            logger.warning(f"Download error for {filename}: {e}")


# ═════════════════════════════════════════════════════════════════════════════
# TIER 3 — Windows Accessibility Tree (UIAutomation)
# ═════════════════════════════════════════════════════════════════════════════

class AccessibilityTree:
    """Extract Windows UI Automation tree for precise element targeting.

    Uses pywinauto to access the Windows UIAutomation framework,
    which provides a DOM-like tree for desktop apps.
    """

    @staticmethod
    def get_tree(
        window_title: str | None = None,
        max_depth: int = 4,
        max_elements: int = 100,
    ) -> dict:
        """Get the accessibility tree for a window or the foreground app.

        Args:
            window_title: Partial window title to find. None = foreground window.
            max_depth: Maximum tree depth to traverse
            max_elements: Maximum elements to return

        Returns:
            dict with: window_title, process, elements (list of element dicts)
        """
        try:
            from pywinauto import Desktop, Application
            from pywinauto.findwindows import find_windows

            desktop = Desktop(backend="uia")

            if window_title:
                # Find window by partial title match
                windows = desktop.windows()
                target = None
                title_lower = window_title.lower()
                for w in windows:
                    try:
                        wt = w.window_text().lower()
                        if title_lower in wt:
                            target = w
                            break
                    except Exception:
                        continue
                if not target:
                    return {"error": f"Window '{window_title}' not found",
                            "available_windows": [
                                w.window_text() for w in windows
                                if w.window_text().strip()
                            ][:20]}
            else:
                # Use foreground window
                import ctypes
                hwnd = ctypes.windll.user32.GetForegroundWindow()
                try:
                    app = Application(backend="uia").connect(handle=hwnd)
                    target = app.top_window()
                except Exception:
                    return {"error": "Cannot connect to foreground window"}

            # Extract element tree
            elements = []
            AccessibilityTree._walk_tree(target, elements, 0, max_depth, max_elements)

            return {
                "window_title": target.window_text()[:200],
                "element_count": len(elements),
                "elements": elements,
            }

        except ImportError:
            return {"error": "pywinauto not installed. Run: pip install pywinauto"}
        except Exception as e:
            return {"error": f"Accessibility tree error: {str(e)[:300]}"}

    @staticmethod
    def _walk_tree(
        control,
        elements: list,
        depth: int,
        max_depth: int,
        max_elements: int,
    ):
        """Recursively walk the UI Automation tree."""
        if depth > max_depth or len(elements) >= max_elements:
            return

        try:
            ctrl_type = control.element_info.control_type or "Unknown"
            name = control.element_info.name or ""
            auto_id = control.element_info.automation_id or ""
            class_name = control.element_info.class_name or ""

            # Skip invisible and empty elements
            if not name and not auto_id and ctrl_type in ("Pane", "Custom", "Group"):
                # Still walk children
                pass
            else:
                # Get bounding rectangle
                try:
                    rect = control.element_info.rectangle
                    x, y = rect.left, rect.top
                    w, h = rect.width(), rect.height()
                    cx, cy = x + w // 2, y + h // 2
                    is_visible = w > 0 and h > 0
                except Exception:
                    x = y = w = h = cx = cy = 0
                    is_visible = False

                if is_visible or (name and ctrl_type not in ("Pane",)):
                    elem = {
                        "id": len(elements) + 1,
                        "type": ctrl_type,
                        "name": name[:100] if name else "",
                        "auto_id": auto_id[:80] if auto_id else "",
                        "depth": depth,
                    }
                    if is_visible:
                        elem.update({"x": x, "y": y, "w": w, "h": h, "cx": cx, "cy": cy})

                    # Add state info for interactive elements
                    if ctrl_type in ("Button", "CheckBox", "RadioButton",
                                     "MenuItem", "TabItem", "ListItem",
                                     "Edit", "ComboBox", "Slider"):
                        try:
                            elem["enabled"] = control.is_enabled()
                        except Exception:
                            pass
                        try:
                            if ctrl_type in ("Edit", "ComboBox"):
                                val = control.get_value()
                                if val:
                                    elem["value"] = str(val)[:100]
                        except Exception:
                            pass
                        try:
                            if ctrl_type in ("CheckBox", "RadioButton"):
                                elem["checked"] = control.get_toggle_state()
                        except Exception:
                            pass

                    elements.append(elem)

            # Walk children
            try:
                children = control.children()
                for child in children:
                    if len(elements) >= max_elements:
                        break
                    AccessibilityTree._walk_tree(
                        child, elements, depth + 1, max_depth, max_elements,
                    )
            except Exception:
                pass

        except Exception as e:
            logger.debug(f"Tree walk error at depth {depth}: {e}")

    @staticmethod
    def click_element(
        window_title: str | None,
        element_name: str | None = None,
        element_auto_id: str | None = None,
        element_type: str | None = None,
    ) -> str:
        """Click a UI element using the accessibility tree (no coordinates needed).

        Args:
            window_title: Window to search in (None = foreground)
            element_name: Element name/text to click
            element_auto_id: Automation ID to click (more reliable than name)
            element_type: Control type filter (Button, Edit, MenuItem, etc.)

        Returns:
            Result string
        """
        try:
            from pywinauto import Desktop, Application

            if window_title:
                desktop = Desktop(backend="uia")
                windows = desktop.windows()
                target = None
                title_lower = window_title.lower()
                for w in windows:
                    try:
                        if title_lower in w.window_text().lower():
                            target = w
                            break
                    except Exception:
                        continue
                if not target:
                    return f"Window '{window_title}' not found"
            else:
                import ctypes
                hwnd = ctypes.windll.user32.GetForegroundWindow()
                app = Application(backend="uia").connect(handle=hwnd)
                target = app.top_window()

            # Build search criteria
            criteria = {}
            if element_name:
                criteria["title"] = element_name
            if element_auto_id:
                criteria["auto_id"] = element_auto_id
            if element_type:
                criteria["control_type"] = element_type

            if not criteria:
                return "Error: Must specify element_name, element_auto_id, or element_type"

            # Find and click
            try:
                child = target.child_window(**criteria)
                child.click_input()
                return f"Clicked: {criteria} in '{target.window_text()[:50]}'"
            except Exception as e:
                # Try partial name match
                if element_name:
                    try:
                        child = target.child_window(title_re=f".*{re.escape(element_name)}.*")
                        child.click_input()
                        return f"Clicked (regex match): '{element_name}' in '{target.window_text()[:50]}'"
                    except Exception:
                        pass
                return f"Element not found: {criteria}. Error: {str(e)[:200]}"

        except ImportError:
            return "pywinauto not installed. Run: pip install pywinauto"
        except Exception as e:
            return f"Accessibility click error: {str(e)[:300]}"

    @staticmethod
    def type_into(
        window_title: str | None,
        text: str,
        element_name: str | None = None,
        element_auto_id: str | None = None,
        clear_first: bool = True,
    ) -> str:
        """Type text into a UI element found by accessibility tree.

        Args:
            window_title: Window to search in
            text: Text to type
            element_name: Name of the input element
            element_auto_id: Automation ID of the input element
            clear_first: Whether to clear existing text first

        Returns:
            Result string
        """
        try:
            from pywinauto import Desktop, Application
            import pyautogui

            if window_title:
                desktop = Desktop(backend="uia")
                target = None
                for w in desktop.windows():
                    try:
                        if window_title.lower() in w.window_text().lower():
                            target = w
                            break
                    except Exception:
                        continue
                if not target:
                    return f"Window '{window_title}' not found"
            else:
                import ctypes
                hwnd = ctypes.windll.user32.GetForegroundWindow()
                app = Application(backend="uia").connect(handle=hwnd)
                target = app.top_window()

            # Find input element
            criteria = {"control_type": "Edit"}
            if element_name:
                criteria["title"] = element_name
            if element_auto_id:
                criteria["auto_id"] = element_auto_id

            try:
                child = target.child_window(**criteria)
                child.click_input()
                time.sleep(0.1)

                if clear_first:
                    pyautogui.hotkey("ctrl", "a")
                    time.sleep(0.05)

                # Handle non-ASCII via clipboard (escape quotes to prevent injection)
                if any(ord(c) > 127 for c in text):
                    safe_text = text.replace("'", "''")
                    subprocess.run(
                        ["powershell", "-Command", f"Set-Clipboard -Value '{safe_text}'"],
                        capture_output=True, timeout=5,
                    )
                    pyautogui.hotkey("ctrl", "v")
                else:
                    pyautogui.typewrite(text, interval=0.02)

                return f"Typed '{text[:50]}' into {criteria}"
            except Exception as e:
                return f"Type failed: {str(e)[:200]}"

        except ImportError:
            return "pywinauto not installed"
        except Exception as e:
            return f"Accessibility type error: {str(e)[:300]}"


# ═════════════════════════════════════════════════════════════════════════════
# TIER 1C — GUI Task Planner
# ═════════════════════════════════════════════════════════════════════════════

def build_gui_plan_prompt(task_description: str, screen_context: dict) -> str:
    """Build a prompt that makes Claude plan multi-step GUI tasks before executing.

    Instead of screenshot → click → screenshot → click (slow loop),
    Claude plans the entire sequence first, then executes in batch.

    Args:
        task_description: What the user wants to do
        screen_context: Current screen state from detect_context()

    Returns:
        Enhanced prompt with planning instructions
    """
    return f"""TASK: {task_description}

CURRENT SCREEN STATE:
{json.dumps(screen_context, indent=2, default=str)}

PLAN YOUR ACTIONS FIRST. List every step you will take:
1. What element to interact with (name, type, location)
2. What action (click, type, hotkey)
3. What to verify after each action

Then execute the plan using the most precise method available:
- For web apps: use browser_click/browser_type (CSS selectors, no coordinates)
- For desktop apps: use ui_tree to find elements by name/ID, then ui_click_element
- For unknown UIs: use som_screenshot to get numbered elements, then som_click #N
- LAST RESORT: use mouse_click with coordinates

NEVER guess coordinates from memory. Always verify with a screenshot or ui_tree first.
"""


# ═════════════════════════════════════════════════════════════════════════════
# TIER 1B — Smart Tool Router
# ═════════════════════════════════════════════════════════════════════════════

def suggest_best_tool(action: str, context: dict | None = None) -> dict:
    """Given a user action, suggest the most precise tool to use.

    Args:
        action: What to do, e.g. "click the Submit button"
        context: Optional screen context from detect_context()

    Returns:
        dict with: tool_name, reason, params
    """
    action_lower = action.lower()

    # Determine if we're in a browser context
    is_browser = False
    if context:
        state = context.get("detected_state", "")
        app = context.get("app_name", "").lower()
        is_browser = state == "browser_page" or app in (
            "chrome", "firefox", "edge", "brave", "opera",
        )

    # Web apps: always prefer selector-based tools
    if is_browser:
        if any(w in action_lower for w in ["click", "press", "tap", "select"]):
            return {
                "tool": "browser_click" if "button" in action_lower or "link" in action_lower
                        else "web_click",
                "reason": "Browser detected — CSS selector click is more reliable than coordinates",
                "fallback": "som_click",
            }
        if any(w in action_lower for w in ["type", "enter", "input", "write", "fill"]):
            return {
                "tool": "browser_type",
                "reason": "Browser detected — selector-based typing is precise",
                "fallback": "ui_type_element",
            }

    # Desktop apps: prefer accessibility tree
    if any(w in action_lower for w in ["click", "press", "tap", "select"]):
        return {
            "tool": "ui_click_element",
            "reason": "Desktop app — accessibility tree click by name/ID is precise",
            "fallback": "som_click",
        }
    if any(w in action_lower for w in ["type", "enter", "input", "write", "fill"]):
        return {
            "tool": "ui_type_element",
            "reason": "Desktop app — accessibility tree finds input fields reliably",
            "fallback": "smart_action",
        }

    return {
        "tool": "smart_action",
        "reason": "General action — using adaptive detection",
        "fallback": "mouse_click",
    }


# ═════════════════════════════════════════════════════════════════════════════
# Cached SoM State — persists element list between screenshot and click
# ═════════════════════════════════════════════════════════════════════════════

_last_som_elements: list[dict] = []
_last_som_time: float = 0
_som_lock = threading.Lock()


def set_som_elements(elements: list[dict]):
    global _last_som_elements, _last_som_time
    with _som_lock:
        _last_som_elements = elements
        _last_som_time = time.time()


def get_som_elements() -> list[dict]:
    """Get cached SoM elements. Valid for 30 seconds."""
    with _som_lock:
        if time.time() - _last_som_time > 30:
            return []
        return list(_last_som_elements)  # Return copy for thread safety
