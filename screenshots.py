"""
screenshots.py — Screen capture utility.

Captures screenshots, resizes for Telegram, returns as BytesIO JPEG buffer.
"""
import logging
from io import BytesIO

logger = logging.getLogger(__name__)


def capture_screenshot(region=None):
    """Capture a screenshot and return it as a BytesIO JPEG buffer.

    Args:
        region: Optional (x, y, width, height) tuple to capture a specific area.

    Returns:
        BytesIO buffer containing the JPEG screenshot.

    Raises:
        Exception if screenshot capture fails (e.g., no display).
    """
    import pyautogui
    from PIL import Image
    import config

    try:
        img = pyautogui.screenshot(region=tuple(region) if region else None)
    except Exception as e:
        logger.error(f"Screenshot capture failed: {e}")
        raise RuntimeError(f"Cannot capture screenshot: {e}") from e

    # Resize if wider than max width (for 4K/ultrawide displays)
    if img.width > config.MAX_SCREENSHOT_WIDTH:
        ratio = config.MAX_SCREENSHOT_WIDTH / img.width
        new_size = (config.MAX_SCREENSHOT_WIDTH, int(img.height * ratio))
        img = img.resize(new_size, Image.LANCZOS)

    buffer = BytesIO()
    img.save(buffer, format="JPEG", quality=config.SCREENSHOT_QUALITY)
    buffer.seek(0)
    buffer.name = "screenshot.jpg"
    return buffer


def save_screenshot(path: str, region=None):
    """Capture and save screenshot to a file path. Returns the path."""
    import pyautogui
    from PIL import Image
    import config

    try:
        img = pyautogui.screenshot(region=tuple(region) if region else None)
    except Exception as e:
        raise RuntimeError(f"Cannot capture screenshot: {e}") from e

    if img.width > config.MAX_SCREENSHOT_WIDTH:
        ratio = config.MAX_SCREENSHOT_WIDTH / img.width
        new_size = (config.MAX_SCREENSHOT_WIDTH, int(img.height * ratio))
        img = img.resize(new_size, Image.LANCZOS)

    img.save(path, quality=config.SCREENSHOT_QUALITY)
    return path
