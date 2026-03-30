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
import difflib
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

try:
    import cv2
except ImportError:
    cv2 = None
try:
    import numpy as np
except ImportError:
    np = None
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
        if hierarchy is None:
            continue
        if hierarchy[0][i][3] != -1:
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


def _try_ocr_text(img_rgb: np.ndarray, elem: dict) -> str:
    """Extract text from a SoM element's bounding box region.

    Uses pytesseract if installed; silently returns "" otherwise.
    Scales up small regions for better accuracy.
    """
    x, y, w, h = elem["x"], elem["y"], elem["w"], elem["h"]
    pad = 3
    arr_h, arr_w = img_rgb.shape[:2]
    x1, y1 = max(0, x - pad), max(0, y - pad)
    x2, y2 = min(arr_w, x + w + pad), min(arr_h, y + h + pad)
    crop = img_rgb[y1:y2, x1:x2]
    if crop.size == 0:
        return ""
    try:
        import pytesseract
        pil_crop = Image.fromarray(crop)
        cw, ch = pil_crop.size
        if cw < 80 and cw > 0:
            factor = max(2, 80 // cw)
            pil_crop = pil_crop.resize(
                (cw * factor, ch * factor), Image.NEAREST,
            )
        return pytesseract.image_to_string(
            pil_crop, config="--psm 7 --oem 3",
        ).strip()
    except Exception:
        return ""


def annotate_screenshot_som(
    image: Image.Image | np.ndarray | BytesIO | str,
    elements: list[dict] | None = None,
    max_elements: int = 50,
    annotate_text: bool = True,
) -> tuple[Image.Image, list[dict]]:
    """Annotate a screenshot with numbered Set-of-Mark bounding boxes.

    Args:
        image: PIL Image, numpy array, BytesIO buffer, or file path
        elements: Pre-detected elements. If None, auto-detects.
        max_elements: Maximum elements to annotate
        annotate_text: If True, run OCR on each element and show extracted text
                       next to its badge (requires pytesseract).

    Returns:
        (annotated_image, element_list) where element_list has id/x/y/w/h/cx/cy/kind
        Elements also get an "ocr_text" key when annotate_text=True.
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

    # OCR pass: extract text from each element region (if pytesseract available)
    img_rgb = np.array(pil_img)
    if annotate_text:
        for elem in elements:
            if "ocr_text" not in elem:
                elem["ocr_text"] = _try_ocr_text(img_rgb, elem)

    # Draw annotations on a copy
    annotated = pil_img.copy()
    draw = ImageDraw.Draw(annotated)
    font = _get_font(13)
    font_small = _get_font(10)

    # Kind abbreviations for compact text labels
    _KIND_ABBR = {"button": "btn", "edit": "edit", "text": "txt",
                  "checkbox": "chk", "radio": "rad", "combo": "cmb",
                  "list": "lst", "menu": "mnu", "element": "el"}

    for elem in elements:
        eid = elem["id"]
        color = _SOM_COLORS[(eid - 1) % len(_SOM_COLORS)]
        x, y, bw, bh = elem["x"], elem["y"], elem["w"], elem["h"]

        # Draw bounding box (3px border, more visible)
        draw.rectangle([x, y, x + bw, y + bh], outline=color, width=3)

        # Sublabel: prefer OCR text (truncated), fall back to kind abbreviation
        ocr_text = elem.get("ocr_text", "").strip()
        if ocr_text:
            sublabel = ocr_text[:20]  # Keep it readable
        else:
            kind_abbr = _KIND_ABBR.get(elem.get("kind", "element"), elem.get("kind", "el")[:3])
            sublabel = kind_abbr

        label = f"{eid}"

        # Badge dimensions — wider to fit kind text
        char_w = 8
        lw = max(len(label) * char_w + 6, 22)
        lh = 20
        lx = max(0, x)
        ly = max(0, y - lh - 2)
        if ly < 0:
            ly = y + 2  # place inside box if no room above

        # Dark outline for contrast, then fill
        draw.rectangle([lx - 1, ly - 1, lx + lw + 1, ly + lh + 1], fill=(0, 0, 0))
        draw.rectangle([lx, ly, lx + lw, ly + lh], fill=color)

        # Number in bold white
        draw.text((lx + 3, ly + 2), label, fill=(255, 255, 255), font=font)

        # OCR text / kind in small text to the right of badge
        draw.text((lx + lw + 3, ly + 4), sublabel, fill=color, font=font_small)

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


def smart_ui_click(
    element_name: str,
    window_title: str | None = None,
    element_type: str | None = None,
    fallback_x: int | None = None,
    fallback_y: int | None = None,
) -> dict:
    """Click a UI element using a fallback chain for maximum reliability.

    Tries in order:
      1. ui_click_element (Windows Accessibility Tree — most precise)
      2. som_screenshot + find element by label text → som_click
      3. smartclick at fallback coordinates (if provided)

    Args:
        element_name: The element's name/text to find
        window_title: Window to search in (None = foreground)
        element_type: Control type filter (Button, Edit, etc.)
        fallback_x: X coordinate for smartclick fallback
        fallback_y: Y coordinate for smartclick fallback

    Returns:
        dict with keys: success (bool), method (str), message (str), attempts (list)
    """
    attempts = []

    # --- Step 1: Accessibility Tree click ---
    try:
        result1 = AccessibilityTree.click_element(
            window_title=window_title,
            element_name=element_name,
            element_type=element_type,
        )
        if "not found" not in result1.lower() and "error" not in result1.lower():
            return {"success": True, "method": "ui_click_element", "message": result1, "attempts": [result1]}
        attempts.append(f"ui_click: {result1}")
    except Exception as e:
        attempts.append(f"ui_click: exception: {e}")

    # --- Step 2: SoM screenshot + OCR text matching ---
    try:
        raw_buf, ann_buf, elements = som_screenshot()
        set_som_elements(elements)

        if elements:
            # Load raw image for OCR text extraction on each element
            raw_buf.seek(0)
            img_rgb = np.array(Image.open(raw_buf).convert("RGB"))
            name_lower = element_name.lower().strip()

            best_elem = None
            best_score = 0.0

            for elem in elements:
                # Use cached OCR text if already computed during annotation, else compute now
                ocr_text = elem.get("ocr_text") or _try_ocr_text(img_rgb, elem)
                elem["ocr_text"] = ocr_text
                text_lower = ocr_text.lower().strip()

                if text_lower:
                    # Score by text similarity
                    if name_lower == text_lower:
                        score = 1.0
                    elif name_lower in text_lower:
                        score = 0.85
                    elif text_lower in name_lower:
                        score = 0.75
                    else:
                        # Word-level overlap
                        name_words = set(name_lower.split())
                        text_words = set(text_lower.split())
                        overlap = len(name_words & text_words)
                        score = overlap / max(len(name_words), 1) * 0.6
                else:
                    # No OCR text — fall back to kind hint if element_type given
                    kind = elem.get("kind", "")
                    if element_type and element_type.lower() in kind.lower():
                        score = 0.2
                    else:
                        score = 0.0

                if score > best_score:
                    best_score = score
                    best_elem = elem

            if best_elem and best_score >= 0.2:
                result2 = som_click(best_elem["id"], elements)
                attempts.append(
                    f"som_click (ocr_score={best_score:.0%}, text='{best_elem.get('ocr_text','')[:30]}'): {result2}",
                )
                return {"success": True, "method": "som_click_ocr", "message": result2, "attempts": attempts}
            else:
                attempts.append(
                    f"som_click: no element matched '{element_name}' "
                    f"(best_score={best_score:.0%}, {len(elements)} elements checked)",
                )
        else:
            attempts.append("som_click: no elements detected on screen")
    except Exception as e:
        attempts.append(f"som_click: exception: {e}")

    # --- Step 3: smartclick at fallback coordinates ---
    if fallback_x is not None and fallback_y is not None:
        try:
            import pyautogui
            import time as _time
            from PIL import ImageGrab

            before = ImageGrab.grab()
            pyautogui.click(fallback_x, fallback_y)
            _time.sleep(0.15)
            after = ImageGrab.grab()

            import numpy as _np
            diff = _np.abs(_np.array(before).astype(int) - _np.array(after).astype(int))
            changed = bool(diff.mean() > 0.5)
            msg = f"smartclick at ({fallback_x},{fallback_y}), screen_changed={changed}"
            attempts.append(f"smartclick: {msg}")
            return {"success": True, "method": "smartclick", "message": msg, "attempts": attempts}
        except Exception as e:
            attempts.append(f"smartclick: exception: {e}")

    return {
        "success": False,
        "method": "none",
        "message": f"All click methods failed for '{element_name}'",
        "attempts": attempts,
    }


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
            if element_auto_id:
                criteria["auto_id"] = element_auto_id
            if element_type:
                criteria["control_type"] = element_type

            if not element_name and not criteria:
                return "Error: Must specify element_name, element_auto_id, or element_type"

            # --- Attempt 1: exact match by automation ID / type (most reliable) ---
            if criteria:
                try:
                    child = target.child_window(**criteria)
                    child.click_input()
                    return f"Clicked: {criteria} in '{target.window_text()[:50]}'"
                except Exception:
                    pass

            # --- Attempt 2: exact title + any other criteria ---
            if element_name:
                exact_criteria = dict(criteria)
                exact_criteria["title"] = element_name
                try:
                    child = target.child_window(**exact_criteria)
                    child.click_input()
                    return f"Clicked (exact name): '{element_name}' in '{target.window_text()[:50]}'"
                except Exception:
                    pass

                # --- Attempt 3: case-insensitive partial regex match ---
                try:
                    child = target.child_window(title_re=f"(?i).*{re.escape(element_name)}.*")
                    child.click_input()
                    return f"Clicked (fuzzy name): '{element_name}' in '{target.window_text()[:50]}'"
                except Exception:
                    pass

                # --- Attempt 4: walk all children, fuzzy match via SequenceMatcher ---
                name_lower = element_name.lower()
                try:
                    all_children = target.descendants()
                    best = None
                    best_score = 0.0
                    for child in all_children:
                        try:
                            ctext = child.window_text().lower()
                            if not ctext:
                                continue
                            # Exact substring → score 1.0
                            if name_lower in ctext or ctext in name_lower:
                                score = 1.0
                            else:
                                # Fuzzy match using SequenceMatcher (order-aware)
                                score = difflib.SequenceMatcher(None, name_lower, ctext).ratio()
                            if score > best_score:
                                best_score = score
                                best = child
                        except Exception:
                            continue
                    if best and best_score > 0.4:
                        best.click_input()
                        return f"Clicked (best-match={best_score:.0%}): '{best.window_text()}' for query '{element_name}'"
                except Exception:
                    pass

            return f"Element not found: name='{element_name}', id='{element_auto_id}', type='{element_type}'"

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


# ═════════════════════════════════════════════════════════════════════════════
# MULTIMODAL TELEGRAM IMAGE ANALYSIS
# p3_19: 多模态任务理解升级
# ═════════════════════════════════════════════════════════════════════════════

def _extract_full_ocr(image_path: str) -> tuple[str, float]:
    """Run full-image OCR and return (text, confidence 0-1).

    Uses pytesseract if available; returns ("", 0.0) otherwise.
    Confidence is the mean character confidence from tesseract data.
    """
    try:
        import pytesseract
        from PIL import Image as _PIL_Image
        pil = _PIL_Image.open(image_path).convert("RGB")
        # Scale up if small for better OCR accuracy
        w, h = pil.size
        if w < 800:
            scale = 800 / w
            pil = pil.resize((int(w * scale), int(h * scale)), _PIL_Image.LANCZOS)

        data = pytesseract.image_to_data(
            pil, output_type=pytesseract.Output.DICT, config="--oem 3 --psm 6",
        )
        words = [
            (t, int(c))
            for t, c in zip(data.get("text", []), data.get("conf", []))
            if t.strip() and int(c) > 0
        ]
        if not words:
            return "", 0.0
        text = " ".join(w for w, _ in words)
        conf = sum(c for _, c in words) / (len(words) * 100)
        return text, min(conf, 1.0)
    except Exception as e:
        logger.debug(f"OCR failed: {e}")
        return "", 0.0


def _classify_image_type(img_bgr: np.ndarray, ocr_text: str) -> tuple[str, float]:
    """Classify image into one of four categories.

    Returns: (type_str, confidence 0-1)
    type_str: "kline_chart" | "ui_screenshot" | "task_screenshot" | "general"
    """
    h, w = img_bgr.shape[:2]
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)

    scores: dict[str, float] = {
        "kline_chart": 0.0,
        "ui_screenshot": 0.0,
        "task_screenshot": 0.0,
        "general": 0.1,
    }

    # --- Kline chart heuristics ---
    # 1. Detect many thin vertical bars (candlesticks) via column variance
    col_variance = np.var(gray, axis=0)
    high_var_cols = np.sum(col_variance > np.mean(col_variance) * 1.5)
    bar_density = high_var_cols / max(w, 1)
    if bar_density > 0.3:
        scores["kline_chart"] += 0.3

    # 2. Detect red/green pixel clusters (candlestick colors)
    hsv = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2HSV)
    red_mask = (
        cv2.inRange(hsv, np.array([0, 100, 100]), np.array([10, 255, 255])) |
        cv2.inRange(hsv, np.array([160, 100, 100]), np.array([180, 255, 255]))
    )
    green_mask = cv2.inRange(hsv, np.array([40, 80, 80]), np.array([90, 255, 255]))
    red_ratio = np.sum(red_mask > 0) / (h * w)
    green_ratio = np.sum(green_mask > 0) / (h * w)
    if 0.01 < red_ratio < 0.35 and 0.01 < green_ratio < 0.35:
        scores["kline_chart"] += 0.35

    # 3. Grid lines (horizontal/vertical lines) — common in charts
    edges = cv2.Canny(gray, 50, 150)
    lines = cv2.HoughLinesP(edges, 1, np.pi / 180, threshold=60, minLineLength=w // 4, maxLineGap=10)
    if lines is not None:
        h_lines = sum(1 for l in lines if abs(l[0][1] - l[0][3]) < 5)
        v_lines = sum(1 for l in lines if abs(l[0][0] - l[0][2]) < 5)
        if h_lines >= 2 and v_lines >= 2:
            scores["kline_chart"] += 0.25

    # 4. OCR keywords for chart context
    ocr_lower = ocr_text.lower()
    chart_keywords = ["btc", "eth", "usd", "usdt", "sol", "bnb", "open", "close",
                      "high", "low", "volume", "ma", "rsi", "macd", "%", "k", "m"]
    chart_hits = sum(1 for kw in chart_keywords if kw in ocr_lower)
    scores["kline_chart"] += min(chart_hits * 0.05, 0.2)

    # --- UI screenshot heuristics ---
    # Detect many rectangular UI elements
    elements = _detect_ui_elements_cv(img_bgr)
    elem_density = len(elements) / 30  # normalize to ~30 elements = full score
    scores["ui_screenshot"] += min(elem_density * 0.4, 0.4)

    ui_keywords = ["button", "click", "menu", "settings", "ok", "cancel", "file",
                   "edit", "view", "help", "window", "search"]
    ui_hits = sum(1 for kw in ui_keywords if kw in ocr_lower)
    scores["ui_screenshot"] += min(ui_hits * 0.08, 0.3)

    # UI screenshots tend to have many distinct colors (icons, buttons)
    if img_bgr.size > 0:
        unique_colors = len(np.unique(img_bgr.reshape(-1, 3), axis=0))
        if unique_colors > 5000:
            scores["ui_screenshot"] += 0.15

    # --- Task screenshot heuristics ---
    # Dense text with limited color variation
    word_count = len(ocr_text.split()) if ocr_text else 0
    if word_count > 20:
        scores["task_screenshot"] += min(word_count / 100, 0.5)
    task_keywords = ["todo", "task", "step", "please", "需要", "请", "完成", "任务",
                     "目标", "要求", "实现", "功能", "bug", "fix", "error", "implement"]
    task_hits = sum(1 for kw in task_keywords if kw in ocr_lower)
    scores["task_screenshot"] += min(task_hits * 0.1, 0.4)

    # Low color diversity → text-heavy document
    if img_bgr.size > 0:
        unique_colors = len(np.unique(img_bgr[::4, ::4].reshape(-1, 3), axis=0))
        if unique_colors < 1000:
            scores["task_screenshot"] += 0.15

    best_type = max(scores, key=lambda k: scores[k])
    best_conf = scores[best_type]
    total = sum(scores.values())
    normalized_conf = best_conf / max(total, 1)

    return best_type, min(normalized_conf, 1.0)


def _analyze_kline_patterns(img_bgr: np.ndarray, ocr_text: str) -> dict:
    """Analyze K-line chart: detect trend, patterns, support/resistance.

    Returns dict with:
        trend: "uptrend" | "downtrend" | "sideways"
        trend_confidence: float
        patterns: list of detected candlestick patterns
        support_levels: list of approximate price levels (from OCR)
        resistance_levels: list
        price_numbers: list of numbers found via OCR
        summary: human-readable analysis string
    """
    h, w = img_bgr.shape[:2]
    result: dict[str, Any] = {
        "trend": "unknown",
        "trend_confidence": 0.0,
        "patterns": [],
        "support_levels": [],
        "resistance_levels": [],
        "price_numbers": [],
        "summary": "",
    }

    # --- Extract price numbers from OCR ---
    price_numbers = []
    if ocr_text:
        number_pattern = re.findall(r"\b\d{1,8}(?:[.,]\d{1,6})?\b", ocr_text)
        for num_str in number_pattern:
            try:
                val = float(num_str.replace(",", ""))
                if val > 0:
                    price_numbers.append(val)
            except ValueError:
                pass
    result["price_numbers"] = sorted(price_numbers)

    # --- Detect overall trend via green/red candle distribution ---
    hsv = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2HSV)
    red_mask = (
        cv2.inRange(hsv, np.array([0, 100, 100]), np.array([10, 255, 255])) |
        cv2.inRange(hsv, np.array([160, 100, 100]), np.array([180, 255, 255]))
    )
    green_mask = cv2.inRange(hsv, np.array([40, 80, 80]), np.array([90, 255, 255]))

    # Divide image into left and right halves for trend comparison
    mid = w // 2
    red_left = np.sum(red_mask[:, :mid] > 0)
    red_right = np.sum(red_mask[:, mid:] > 0)
    green_left = np.sum(green_mask[:, :mid] > 0)
    green_right = np.sum(green_mask[:, mid:] > 0)

    total_red = np.sum(red_mask > 0)
    total_green = np.sum(green_mask > 0)
    total_color = total_red + total_green

    # Simple trend: more green recently = uptrend
    if total_color > 100:
        green_ratio_right = green_right / max(green_right + red_right, 1)
        green_ratio_total = total_green / max(total_color, 1)

        if green_ratio_right > 0.6 and green_ratio_total > 0.5:
            result["trend"] = "uptrend"
            result["trend_confidence"] = min(green_ratio_right, 0.95)
        elif green_ratio_right < 0.4 and green_ratio_total < 0.5:
            result["trend"] = "downtrend"
            result["trend_confidence"] = min(1.0 - green_ratio_right, 0.95)
        else:
            result["trend"] = "sideways"
            result["trend_confidence"] = 0.6
    else:
        # Fallback: analyze luminosity gradient (right brighter = uptrend for line charts)
        gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
        # Look at the main chart area (middle 60% height)
        chart_h1, chart_h2 = int(h * 0.2), int(h * 0.8)
        left_mean = float(np.mean(gray[chart_h1:chart_h2, :mid]))
        right_mean = float(np.mean(gray[chart_h1:chart_h2, mid:]))
        if abs(left_mean - right_mean) < 5:
            result["trend"] = "sideways"
            result["trend_confidence"] = 0.5
        else:
            result["trend"] = "unknown"
            result["trend_confidence"] = 0.3

    # --- Detect dominant moving average lines ---
    # Look for smooth curves (low curvature edges) spanning most of the width
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
    blurred = cv2.GaussianBlur(gray, (5, 5), 0)
    edges = cv2.Canny(blurred, 20, 60)
    ma_lines = cv2.HoughLinesP(edges, 1, np.pi / 180, threshold=50,
                                minLineLength=w // 3, maxLineGap=30)
    ma_count = len(ma_lines) if ma_lines is not None else 0
    if ma_count >= 2:
        result["patterns"].append(f"检测到{ma_count}条均线")

    # --- Detect doji candles (open ≈ close, long wicks) ---
    # Simplified: look for thin vertical color segments
    col_means = []
    step = max(1, w // 50)
    for x in range(0, w, step):
        col = img_bgr[:, x]
        r_px = np.sum(red_mask[:, x] > 0)
        g_px = np.sum(green_mask[:, x] > 0)
        col_means.append((r_px, g_px))

    doji_count = sum(1 for r, g in col_means if abs(r - g) < 3 and (r + g) > 2)
    if doji_count > len(col_means) * 0.1:
        result["patterns"].append("可能含十字星(Doji)形态")

    # --- Support/resistance from price_numbers ---
    if price_numbers and len(price_numbers) >= 2:
        sorted_prices = sorted(price_numbers)
        result["support_levels"] = sorted_prices[:2]
        result["resistance_levels"] = sorted_prices[-2:]

    # --- Build human-readable summary ---
    trend_emoji = {"uptrend": "📈", "downtrend": "📉", "sideways": "➡️", "unknown": "❓"}
    trend_cn = {"uptrend": "上升趋势", "downtrend": "下降趋势", "sideways": "横盘整理", "unknown": "趋势不明"}
    t = result["trend"]
    conf_pct = int(result["trend_confidence"] * 100)

    lines = [
        f"{trend_emoji.get(t, '❓')} 趋势: {trend_cn.get(t, t)} (置信度 {conf_pct}%)",
    ]
    if result["patterns"]:
        lines.append(f"形态: {', '.join(result['patterns'])}")
    if result["price_numbers"]:
        lines.append(f"识别价格区间: {min(result['price_numbers']):.4g} ~ {max(result['price_numbers']):.4g}")
    if result["support_levels"]:
        lines.append(f"支撑位参考: {result['support_levels']}")
    if result["resistance_levels"]:
        lines.append(f"阻力位参考: {result['resistance_levels']}")

    result["summary"] = "\n".join(lines)
    return result


def analyze_telegram_image(image_path: str, caption: str = "") -> dict:
    """Main multimodal analysis pipeline for Telegram images.

    Args:
        image_path: Path to saved image file
        caption: User caption/text accompanying the image

    Returns dict:
        type: image type classification
        type_confidence: float (0-1)
        ocr_text: extracted text
        ocr_confidence: float (0-1)
        analysis: type-specific analysis dict
        ui_elements: list of SoM elements (for UI screenshots)
        suggested_actions: list of action strings
        needs_confirmation: bool (True when overall confidence < 0.7)
        claude_context: formatted string to prepend to Claude's prompt
    """
    result: dict[str, Any] = {
        "type": "general",
        "type_confidence": 0.0,
        "ocr_text": "",
        "ocr_confidence": 0.0,
        "analysis": {},
        "ui_elements": [],
        "suggested_actions": [],
        "needs_confirmation": True,
        "claude_context": "",
    }

    try:
        # Load image
        img_bgr = cv2.imread(image_path)
        if img_bgr is None:
            result["claude_context"] = "⚠️ 图片读取失败"
            return result

        # Step 1: OCR full image
        ocr_text, ocr_conf = _extract_full_ocr(image_path)
        result["ocr_text"] = ocr_text
        result["ocr_confidence"] = ocr_conf

        # Step 2: Classify image type
        combined_text = (ocr_text + " " + caption).strip()
        img_type, type_conf = _classify_image_type(img_bgr, combined_text)
        result["type"] = img_type
        result["type_confidence"] = type_conf

        # Step 3: Type-specific deep analysis
        if img_type == "kline_chart":
            analysis = _analyze_kline_patterns(img_bgr, combined_text)
            result["analysis"] = analysis
            result["suggested_actions"] = [
                "根据趋势分析生成交易建议",
                "识别关键支撑位和阻力位",
                "结合成交量给出买卖信号",
            ]

        elif img_type == "ui_screenshot":
            # SoM annotation for UI elements
            try:
                pil_img, elements = annotate_screenshot_som(image_path)
                set_som_elements(elements)
                result["ui_elements"] = elements
                result["analysis"] = {
                    "element_count": len(elements),
                    "elements_preview": [
                        {
                            "id": e["id"],
                            "kind": e["kind"],
                            "ocr_text": e.get("ocr_text", "")[:30],
                            "cx": e["cx"], "cy": e["cy"],
                        }
                        for e in elements[:15]
                    ],
                }
                result["suggested_actions"] = [
                    f"点击元素#{e['id']} ({e.get('ocr_text','') or e['kind']})"
                    for e in elements[:5]
                    if e.get("ocr_text") or e["kind"] in ("button", "input")
                ]
            except Exception as e:
                logger.warning(f"UI SoM analysis failed: {e}")
                result["analysis"] = {"error": str(e)}

        elif img_type == "task_screenshot":
            result["analysis"] = {
                "extracted_text": ocr_text,
                "word_count": len(ocr_text.split()),
            }
            # Parse task keywords
            task_lines = [l.strip() for l in ocr_text.splitlines() if l.strip()]
            result["suggested_actions"] = task_lines[:5] if task_lines else []

        # Step 4: Confidence check
        overall_conf = (type_conf + ocr_conf) / 2
        result["needs_confirmation"] = overall_conf < 0.7

        # Step 5: Build claude_context string
        ctx_parts = [
            f"【图片分析结果】",
            f"图片类型: {_TYPE_NAMES.get(img_type, img_type)} (置信度 {int(type_conf * 100)}%)",
        ]

        if ocr_text:
            ocr_preview = ocr_text[:300] + ("..." if len(ocr_text) > 300 else "")
            ctx_parts.append(f"OCR识别文字 (置信度 {int(ocr_conf * 100)}%):\n{ocr_preview}")

        if img_type == "kline_chart" and result["analysis"].get("summary"):
            ctx_parts.append(f"\n{result['analysis']['summary']}")
            ctx_parts.append("\n请根据以上K线分析给出具体交易建议（入场点、止损、止盈）")

        elif img_type == "ui_screenshot":
            elems = result["ui_elements"]
            if elems:
                ctx_parts.append(f"检测到 {len(elems)} 个UI元素，已缓存至SoM引擎")
                ctx_parts.append("可使用 som_click #N 点击对应编号元素")

        elif img_type == "task_screenshot":
            ctx_parts.append("图中包含任务/指令文字，请解析并执行")

        if caption:
            ctx_parts.append(f"用户说明: {caption}")

        if result["needs_confirmation"]:
            ctx_parts.append(
                f"\n⚠️ 识别置信度较低({int(overall_conf * 100)}% < 70%)，"
                "请向用户确认理解是否正确后再执行操作"
            )

        result["claude_context"] = "\n".join(ctx_parts)

    except Exception as e:
        logger.error(f"analyze_telegram_image error: {e}", exc_info=True)
        result["claude_context"] = f"图片分析出错: {e}"

    return result


_TYPE_NAMES = {
    "kline_chart": "K线/交易图表",
    "ui_screenshot": "UI截图",
    "task_screenshot": "任务/文字截图",
    "general": "普通图片",
}
