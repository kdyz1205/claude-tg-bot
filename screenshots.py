import pyautogui
from io import BytesIO
from PIL import Image
import config


def capture_screenshot(region=None):
    """Capture a screenshot and return it as a BytesIO JPEG buffer."""
    img = pyautogui.screenshot(region=tuple(region) if region else None)

    # Resize if wider than max width (for 4K displays)
    if img.width > config.MAX_SCREENSHOT_WIDTH:
        ratio = config.MAX_SCREENSHOT_WIDTH / img.width
        new_size = (config.MAX_SCREENSHOT_WIDTH, int(img.height * ratio))
        img = img.resize(new_size, Image.LANCZOS)

    buffer = BytesIO()
    img.save(buffer, format="JPEG", quality=config.SCREENSHOT_QUALITY)
    buffer.seek(0)
    buffer.name = "screenshot.jpg"
    return buffer
