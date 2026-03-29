"""
screenshots.py — Screen capture and analysis utility.

Captures screenshots, resizes for Telegram, returns as BytesIO JPEG buffer.
Also provides change detection and basic UI element detection for
screenshot-based automation verification.
"""
import logging
import os
from io import BytesIO

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy numpy/PIL imports (only when analysis functions are called)
# ---------------------------------------------------------------------------

def _load_image_as_array(img_path: str):
    """Load an image file into a numpy RGB array."""
    import numpy as np
    from PIL import Image
    img = Image.open(img_path).convert("RGB")
    return np.array(img)


def detect_changes(img1_path: str, img2_path: str, threshold: int = 15,
                   min_region_area: int = 200) -> list:
    """Find rectangular regions that changed between two screenshots.

    Args:
        img1_path: Path to the first (before) screenshot.
        img2_path: Path to the second (after) screenshot.
        threshold: Per-channel difference threshold (0-255) to count a pixel
                   as changed.  Default 15 filters out compression artefacts.
        min_region_area: Minimum pixel area for a changed region to be
                         reported.  Prevents noise.

    Returns:
        List of dicts, each with keys:
          x, y          - top-left corner of the changed region,
          width, height - size,
          area          - number of changed pixels inside the bounding box,
          center_x, center_y - centre of the bounding box.
        Sorted largest-area first.  Empty list when the images are identical
        (within threshold) or have different dimensions.
    """
    import numpy as np
    a = _load_image_as_array(img1_path).astype(np.int16)
    b = _load_image_as_array(img2_path).astype(np.int16)

    if a.shape != b.shape:
        logger.warning("detect_changes: image dimensions differ (%s vs %s)", a.shape, b.shape)
        return []

    diff = np.abs(b - a)
    changed_mask = np.any(diff > threshold, axis=2).astype(np.uint8)

    # Connected-component labeling
    try:
        from scipy import ndimage
        labeled, num_features = ndimage.label(changed_mask)
    except ImportError:
        # Lightweight fallback: return one bounding box over all changes
        ys, xs = np.where(changed_mask > 0)
        if len(xs) == 0:
            return []
        area = int(len(xs))
        if area < min_region_area:
            return []
        x_min, x_max = int(xs.min()), int(xs.max())
        y_min, y_max = int(ys.min()), int(ys.max())
        return [{
            "x": x_min, "y": y_min,
            "width": x_max - x_min + 1,
            "height": y_max - y_min + 1,
            "area": area,
            "center_x": (x_min + x_max) // 2,
            "center_y": (y_min + y_max) // 2,
        }]

    results = []
    for i in range(1, num_features + 1):
        ys, xs = np.where(labeled == i)
        area = int(len(xs))
        if area < min_region_area:
            continue
        x_min, x_max = int(xs.min()), int(xs.max())
        y_min, y_max = int(ys.min()), int(ys.max())
        results.append({
            "x": x_min, "y": y_min,
            "width": x_max - x_min + 1,
            "height": y_max - y_min + 1,
            "area": area,
            "center_x": (x_min + x_max) // 2,
            "center_y": (y_min + y_max) // 2,
        })

    results.sort(key=lambda r: r["area"], reverse=True)
    return results[:100]


def find_ui_elements(img_path: str, min_width: int = 20, min_height: int = 10,
                     max_elements: int = 80) -> list:
    """Detect probable clickable UI elements (buttons, inputs, cards) using
    edge detection and contour-finding heuristics.

    This is a lightweight, OpenCV-optional approach:
    1. Convert to grayscale.
    2. Detect edges with a Sobel filter.
    3. Threshold and dilate to close gaps.
    4. Label connected components and filter by aspect ratio / size.

    Args:
        img_path: Path to the screenshot image.
        min_width:  Minimum bounding-box width to keep (pixels).
        min_height: Minimum bounding-box height to keep (pixels).
        max_elements: Cap on returned elements.

    Returns:
        List of dicts sorted by vertical position, each with keys:
          x, y, width, height, center_x, center_y, area,
          kind  - heuristic guess: "button", "input", "card", or "element".
    """
    import numpy as np
    from PIL import Image, ImageFilter

    img = Image.open(img_path).convert("L")  # grayscale
    arr = np.array(img, dtype=np.float32)

    # Sobel edge detection (horizontal + vertical)
    # Simple 3x3 kernels applied via PIL convolution
    edges_img = img.filter(ImageFilter.FIND_EDGES)
    edges = np.array(edges_img, dtype=np.uint8)

    # Threshold: keep strong edges
    binary = (edges > 30).astype(np.uint8)

    # Dilate to merge nearby edges into solid regions
    # Use a simple box-blur + threshold trick as a fast morphological close
    from PIL import ImageFilter as IF
    dilated_img = Image.fromarray(binary * 255).filter(IF.MaxFilter(5))
    dilated = (np.array(dilated_img) > 127).astype(np.uint8)

    # Connected-component labeling
    try:
        from scipy import ndimage
        labeled, num_features = ndimage.label(dilated)
    except ImportError:
        # Without scipy we cannot do multi-component analysis
        logger.warning("find_ui_elements: scipy not available, returning empty")
        return []

    results = []
    img_h, img_w = arr.shape
    for i in range(1, num_features + 1):
        ys, xs = np.where(labeled == i)
        x_min, x_max = int(xs.min()), int(xs.max())
        y_min, y_max = int(ys.min()), int(ys.max())
        w = x_max - x_min + 1
        h = y_max - y_min + 1
        if w < min_width or h < min_height:
            continue
        # Skip elements that span nearly the whole image (backgrounds)
        if w > img_w * 0.95 and h > img_h * 0.95:
            continue

        area = int(len(xs))
        aspect = w / max(h, 1)

        # Heuristic classification
        if 1.5 < aspect < 8 and 20 <= h <= 60:
            kind = "button"
        elif aspect > 3 and 18 <= h <= 50:
            kind = "input"
        elif 0.5 < aspect < 2.5 and w > 80 and h > 60:
            kind = "card"
        else:
            kind = "element"

        results.append({
            "x": x_min, "y": y_min,
            "width": w, "height": h,
            "center_x": (x_min + x_max) // 2,
            "center_y": (y_min + y_max) // 2,
            "area": area,
            "kind": kind,
        })

    # Sort top-to-bottom, left-to-right
    results.sort(key=lambda r: (r["y"], r["x"]))
    return results[:max_elements]


def highlight_regions(img_path: str, regions: list, output_path: str = None,
                      color=(255, 0, 0), thickness: int = 2) -> str:
    """Draw rectangles on a screenshot to highlight detected regions.

    Args:
        img_path:    Source screenshot path.
        regions:     List of dicts with x, y, width, height keys.
        output_path: Where to save.  Defaults to img_path with '_highlighted' suffix.
        color:       RGB tuple for rectangle outlines.
        thickness:   Line thickness in pixels.

    Returns:
        Path to the saved highlighted image.
    """
    from PIL import Image, ImageDraw

    img = Image.open(img_path).convert("RGB")
    draw = ImageDraw.Draw(img)
    for r in regions:
        x, y, w, h = r["x"], r["y"], r["width"], r["height"]
        for t in range(thickness):
            draw.rectangle([x - t, y - t, x + w + t, y + h + t], outline=color)
        # Label if kind is present
        label = r.get("kind", "")
        if label:
            draw.text((x + 2, y - 12 if y > 14 else y + 2), label, fill=color)

    if output_path is None:
        base, ext = os.path.splitext(img_path)
        output_path = f"{base}_highlighted{ext}"
    img.save(output_path)
    return output_path


def analyze_region(img_path: str, x: int, y: int, w: int, h: int) -> dict:
    """Analyze a specific region of a screenshot.

    Args:
        img_path: Path to the screenshot image.
        x, y: Top-left corner of the region.
        w, h: Width and height of the region.

    Returns:
        dict with keys:
          dominant_color   - (R, G, B) tuple of the most common color cluster,
          contrast_level   - float 0-1 (0 = flat, 1 = maximum contrast),
          has_text_like_content - bool, whether region likely contains text,
          edge_density     - float 0-1 (proportion of pixels that are edges),
          uniformity_score - float 0-1 (1 = perfectly uniform).
    """
    import numpy as np
    from PIL import Image

    try:
        img = Image.open(img_path).convert("RGB")
        arr = np.array(img)
    except Exception as e:
        logger.error("analyze_region: cannot open %s: %s", img_path, e)
        return {"error": str(e)}

    img_h, img_w = arr.shape[:2]
    # Clamp region to image bounds
    x1 = max(0, x)
    y1 = max(0, y)
    x2 = min(img_w, x + w)
    y2 = min(img_h, y + h)
    if x2 <= x1 or y2 <= y1:
        return {"error": "region is out of bounds or empty"}

    region = arr[y1:y2, x1:x2]
    gray = np.dot(region[..., :3], [0.299, 0.587, 0.114]).astype(np.uint8)

    # Dominant color: mean of all pixels (fast approximation)
    dominant_color = tuple(int(c) for c in region.mean(axis=(0, 1)))

    # Contrast level: standard deviation of grayscale, normalized to 0-1
    gray_std = float(gray.std())
    contrast_level = round(min(1.0, gray_std / 128.0), 4)

    # Uniformity: inverse of normalized std
    uniformity_score = round(max(0.0, 1.0 - gray_std / 128.0), 4)

    # Edge density: proportion of strong gradient pixels
    gx = np.abs(gray[:, 1:].astype(np.int16) - gray[:, :-1].astype(np.int16))
    gy = np.abs(gray[1:, :].astype(np.int16) - gray[:-1, :].astype(np.int16))
    gx_full = np.pad(gx, ((0, 0), (0, 1)), mode='constant')
    gy_full = np.pad(gy, ((0, 1), (0, 0)), mode='constant')
    edges = np.maximum(gx_full, gy_full)
    edge_pixels = (edges > 25).sum()
    edge_density = round(float(edge_pixels) / max(gray.size, 1), 4)

    # Text-like content heuristic: moderate edge density + high contrast
    # on a mostly uniform background
    bg_ratio = (gray > 180).sum() / max(gray.size, 1)
    dark_ratio = (gray < 100).sum() / max(gray.size, 1)
    has_text = bool(
        (0.02 < edge_density < 0.5)
        and (
            (bg_ratio > 0.5 and dark_ratio > 0.01)   # dark text on light
            or (dark_ratio > 0.5 and (gray > 180).sum() / max(gray.size, 1) > 0.01)  # light text on dark
        )
    )

    return {
        "dominant_color": dominant_color,
        "contrast_level": contrast_level,
        "has_text_like_content": has_text,
        "edge_density": edge_density,
        "uniformity_score": uniformity_score,
    }


def compare_regions(img1_path: str, img2_path: str,
                    x: int, y: int, w: int, h: int) -> dict:
    """Compare the same region across two screenshots.

    Args:
        img1_path: Path to the first (before) screenshot.
        img2_path: Path to the second (after) screenshot.
        x, y: Top-left corner of the region.
        w, h: Width and height of the region.

    Returns:
        dict with keys:
          changed             - bool, whether the region changed meaningfully,
          change_percentage   - float 0-100,
          dominant_change_color - (R, G, B) mean color of changed pixels in img2
                                 (None if no change).
    """
    import numpy as np

    try:
        a = _load_image_as_array(img1_path)
        b = _load_image_as_array(img2_path)
    except Exception as e:
        logger.error("compare_regions: %s", e)
        return {"changed": False, "change_percentage": 0.0,
                "dominant_change_color": None, "error": str(e)}

    # Handle size mismatch
    if a.shape != b.shape:
        return {"changed": True, "change_percentage": 100.0,
                "dominant_change_color": None,
                "note": "image dimensions differ"}

    img_h, img_w = a.shape[:2]
    x1 = max(0, x)
    y1 = max(0, y)
    x2 = min(img_w, x + w)
    y2 = min(img_h, y + h)
    if x2 <= x1 or y2 <= y1:
        return {"changed": False, "change_percentage": 0.0,
                "dominant_change_color": None, "error": "region out of bounds"}

    ra = a[y1:y2, x1:x2].astype(np.int16)
    rb = b[y1:y2, x1:x2].astype(np.int16)

    diff = np.abs(rb - ra)
    pixel_changed = np.any(diff > 15, axis=2)
    change_count = int(pixel_changed.sum())
    total = pixel_changed.size
    pct = round((change_count / max(total, 1)) * 100.0, 3)

    dominant_change_color = None
    if change_count > 0:
        # Mean color of changed pixels in the "after" image
        changed_pixels = b[y1:y2, x1:x2][pixel_changed]
        dominant_change_color = tuple(int(c) for c in changed_pixels.mean(axis=0))

    return {
        "changed": pct > 0.05,
        "change_percentage": pct,
        "dominant_change_color": dominant_change_color,
    }


def capture_screenshot(region=None):
    """Capture a screenshot and return it as a BytesIO JPEG buffer.

    Args:
        region: Optional (x, y, width, height) tuple to capture a specific area.

    Returns:
        BytesIO buffer containing the JPEG screenshot.

    Raises:
        RuntimeError if screenshot capture fails (e.g., no display).
    """
    from PIL import Image, ImageGrab
    import config

    try:
        # Use ImageGrab directly for multi-monitor support
        if region:
            x, y, w, h = region
            img = ImageGrab.grab(bbox=(x, y, x + w, y + h), all_screens=True)
        else:
            img = ImageGrab.grab(all_screens=True)
    except Exception as e:
        logger.error("Screenshot capture failed: %s", e)
        raise RuntimeError(f"Cannot capture screenshot: {e}") from e

    # Ensure RGB for JPEG (ImageGrab can return RGBA on some Windows setups)
    if img.mode != "RGB":
        img = img.convert("RGB")

    # Resize if wider than max width (for 4K/ultrawide displays)
    if img.width > config.MAX_SCREENSHOT_WIDTH:
        ratio = config.MAX_SCREENSHOT_WIDTH / img.width
        new_size = (config.MAX_SCREENSHOT_WIDTH, int(img.height * ratio))
        img = img.resize(new_size, Image.LANCZOS)

    buffer = BytesIO()
    # Use quality 85 for clearer text in screenshots (overrides config if too low)
    quality = max(config.SCREENSHOT_QUALITY, 80)
    img.save(buffer, format="JPEG", quality=quality)
    img.close()  # Release PIL resources
    buffer.seek(0)
    buffer.name = "screenshot.jpg"
    return buffer


def save_screenshot(path: str, region=None):
    """Capture and save screenshot to a file path. Returns the path.

    Handles RGBA->RGB conversion and multi-monitor via all_screens.
    """
    from PIL import Image, ImageGrab
    import config

    try:
        if region:
            x, y, w, h = region
            img = ImageGrab.grab(bbox=(x, y, x + w, y + h), all_screens=True)
        else:
            img = ImageGrab.grab(all_screens=True)
    except Exception as e:
        raise RuntimeError(f"Cannot capture screenshot: {e}") from e

    # Ensure RGB for JPEG compatibility (ImageGrab can return RGBA)
    if img.mode != "RGB":
        img = img.convert("RGB")

    if img.width > config.MAX_SCREENSHOT_WIDTH:
        ratio = config.MAX_SCREENSHOT_WIDTH / img.width
        new_size = (config.MAX_SCREENSHOT_WIDTH, int(img.height * ratio))
        img = img.resize(new_size, Image.LANCZOS)

    # Determine format from extension; default to JPEG
    ext = os.path.splitext(path)[1].lower()
    save_kwargs = {}
    quality = max(config.SCREENSHOT_QUALITY, 80)
    if ext in ('.jpg', '.jpeg'):
        save_kwargs = {"format": "JPEG", "quality": quality}
    elif ext == '.png':
        save_kwargs = {"format": "PNG"}
    else:
        save_kwargs = {"format": "JPEG", "quality": quality}
    img.save(path, **save_kwargs)
    return path
