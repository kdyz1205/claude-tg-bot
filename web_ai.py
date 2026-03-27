"""
web_ai.py — Browser-based free AI fallback.

When Claude Code CLI is rate-limited, routes tasks to free AI web interfaces
via Playwright browser automation. Zero API cost.

Supported platforms:
- ChatGPT (chatgpt.com) — free tier
- Claude.ai (claude.ai) — free tier
- Gemini (gemini.google.com) — free tier

Requirements: User must be logged into these sites in Chrome.

Parallel mode: query_web_ai_parallel() fires all platforms simultaneously
and returns results as they complete.
"""
import asyncio
import logging
import os
import time
from io import BytesIO

logger = logging.getLogger(__name__)

# Use the user's Chrome profile so we're already logged in
CHROME_USER_DATA = os.path.join(os.path.expanduser("~"), "AppData", "Local", "Google", "Chrome", "User Data")

# Singleton browser (separate from browser_agent.py's Playwright instance)
_pw = None
_browser = None
_contexts: dict[str, object] = {}  # platform_name -> context
_pages: dict[str, object] = {}     # platform_name -> page

# Per-platform locks — allows concurrent queries across different platforms
_browser_init_lock = asyncio.Lock()  # only for browser init
_platform_locks: dict[str, asyncio.Lock] = {}  # lazily created per platform


def _get_platform_lock(platform: str) -> asyncio.Lock:
    if platform not in _platform_locks:
        _platform_locks[platform] = asyncio.Lock()
    return _platform_locks[platform]


async def _get_page(platform: str, url: str):
    """Get or create a browser page for a platform, using Chrome profile."""
    global _pw, _browser

    if platform in _pages:
        page = _pages[platform]
        if not page.is_closed():
            return page

    # Connect to running Chrome via CDP (reuses all login sessions)
    # Falls back to persistent context if CDP not available
    async with _browser_init_lock:
        if _browser is None or not _browser.is_connected():
            from playwright.async_api import async_playwright
            _pw = await async_playwright().start()

            # Try CDP first (connects to user's running Chrome)
            try:
                import socket
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(1)
                s.connect(("127.0.0.1", 9222))
                s.close()
                _browser = await _pw.chromium.connect_over_cdp("http://127.0.0.1:9222")
                logger.info("web_ai: connected to Chrome via CDP")
            except Exception:
                # Fallback: launch with persistent context
                logger.info("web_ai: CDP not available, launching persistent context")
                _browser = await _pw.chromium.launch_persistent_context(
                    user_data_dir=CHROME_USER_DATA,
                    headless=False,
                    channel="chrome",
                    args=["--start-maximized", "--no-first-run"],
                    no_viewport=True,
                    timeout=30000,
                )

    # Create new page for this platform (outside init lock — each platform gets its own page)
    # CDP browser has contexts; persistent context IS the context
    if hasattr(_browser, 'contexts') and _browser.contexts:
        page = await _browser.contexts[0].new_page()
    else:
        page = await _browser.new_page()
    await page.goto(url, wait_until="domcontentloaded", timeout=20000)
    _pages[platform] = page
    return page


async def _wait_and_get_response(page, response_selector: str, timeout: float = 120.0) -> str:
    """Wait for AI response to finish streaming, then extract text."""
    start = time.time()

    # Wait for response element to appear
    try:
        await page.wait_for_selector(response_selector, timeout=15000)
    except Exception:
        return None

    # Wait for streaming to finish (text stops changing)
    last_text = ""
    stable_count = 0
    while time.time() - start < timeout:
        await asyncio.sleep(2)
        try:
            elements = await page.query_selector_all(response_selector)
            if not elements:
                continue
            # Get the LAST response element (newest reply)
            current_text = await elements[-1].inner_text()
            if current_text == last_text and len(current_text) > 0:
                stable_count += 1
                if stable_count >= 2:  # Text stable for 4 seconds
                    return current_text.strip()
            else:
                stable_count = 0
                last_text = current_text
        except Exception as e:
            logger.debug(f"Response poll error: {e}")
            continue

    # Timeout — return whatever we have
    return last_text.strip() if last_text else None


# ─── ChatGPT ─────────────────────────────────────────────────────────────────

async def query_chatgpt(message: str) -> str | None:
    """Send a message to ChatGPT web and get response. Returns None on failure."""
    async with _get_platform_lock("chatgpt"):
        try:
            page = await _get_page("chatgpt", "https://chatgpt.com")
            await asyncio.sleep(1)

            # Find the message input
            # ChatGPT uses a contenteditable div or textarea
            input_sel = "#prompt-textarea, [data-testid='send-button']"
            try:
                textarea = await page.wait_for_selector("#prompt-textarea", timeout=10000)
                await textarea.click()
                await asyncio.sleep(0.3)

                # Clear and type message
                await textarea.fill("")
                await textarea.fill(message)
                await asyncio.sleep(0.5)

                # Press Enter or click send button
                await textarea.press("Enter")
            except Exception as e:
                logger.warning(f"ChatGPT input failed: {e}")
                return None

            # Wait for response
            # ChatGPT response selector (may need updating as UI changes)
            response = await _wait_and_get_response(
                page,
                "[data-message-author-role='assistant']",
                timeout=120
            )
            return response

        except Exception as e:
            logger.error(f"ChatGPT query failed: {e}")
            return None


# ─── Claude.ai ───────────────────────────────────────────────────────────────

async def query_claude_web(message: str) -> str | None:
    """Send a message to Claude.ai web and get response."""
    async with _get_platform_lock("claude"):
        try:
            page = await _get_page("claude", "https://claude.ai/new")
            await asyncio.sleep(2)

            # Find input field
            try:
                # Claude.ai uses contenteditable div
                input_area = await page.wait_for_selector(
                    "[contenteditable='true'], .ProseMirror", timeout=10000
                )
                await input_area.click()
                await asyncio.sleep(0.3)

                # Type message
                await page.keyboard.type(message, delay=10)
                await asyncio.sleep(0.5)

                # Press Enter to send
                await page.keyboard.press("Enter")
            except Exception as e:
                logger.warning(f"Claude.ai input failed: {e}")
                return None

            # Wait for response
            response = await _wait_and_get_response(
                page,
                "[data-is-streaming], .font-claude-message, .prose",
                timeout=120
            )
            return response

        except Exception as e:
            logger.error(f"Claude.ai query failed: {e}")
            return None


# ─── Gemini Web ──────────────────────────────────────────────────────────────

async def query_gemini_web(message: str) -> str | None:
    """Send a message to Gemini web and get response."""
    async with _get_platform_lock("gemini"):
        try:
            page = await _get_page("gemini", "https://gemini.google.com")
            await asyncio.sleep(2)

            try:
                # Gemini uses a rich text editor
                input_area = await page.wait_for_selector(
                    ".ql-editor, [contenteditable='true'], rich-textarea", timeout=10000
                )
                await input_area.click()
                await asyncio.sleep(0.3)
                await page.keyboard.type(message, delay=10)
                await asyncio.sleep(0.5)
                await page.keyboard.press("Enter")
            except Exception as e:
                logger.warning(f"Gemini web input failed: {e}")
                return None

            # Wait for response
            response = await _wait_and_get_response(
                page,
                ".model-response-text, .response-container, message-content",
                timeout=120
            )
            return response

        except Exception as e:
            logger.error(f"Gemini web query failed: {e}")
            return None


# ─── Unified Router ──────────────────────────────────────────────────────────

_PLATFORMS = [
    ("chatgpt", query_chatgpt),
    ("claude_web", query_claude_web),
    ("gemini_web", query_gemini_web),
]

PLATFORM_DISPLAY = {
    "chatgpt": "ChatGPT",
    "claude_web": "Claude.ai",
    "gemini_web": "Gemini",
}

# Track which platforms failed recently (avoid retrying broken ones)
_platform_failures: dict[str, float] = {}
_FAILURE_COOLDOWN = 300  # 5 minutes before retrying a failed platform


def _available_platforms() -> list:
    """Return platforms not in failure cooldown."""
    now = time.time()
    return [
        (name, fn) for name, fn in _PLATFORMS
        if now - _platform_failures.get(name, 0) >= _FAILURE_COOLDOWN
    ]


async def _try_platform(name: str, fn, message: str) -> tuple[str | None, str]:
    """Query one platform, return (response, name) or (None, name) on failure."""
    try:
        response = await asyncio.wait_for(fn(message), timeout=150)
        if response and len(response) > 5:
            logger.info(f"Web AI [{name}]: got {len(response)} chars")
            return response, name
        logger.warning(f"Web AI [{name}]: empty/short response")
        _platform_failures[name] = time.time()
        return None, name
    except asyncio.TimeoutError:
        logger.warning(f"Web AI [{name}]: timed out")
        _platform_failures[name] = time.time()
        return None, name
    except Exception as e:
        logger.error(f"Web AI [{name}]: error: {e}")
        _platform_failures[name] = time.time()
        return None, name


async def query_web_ai(message: str, preferred: str = None) -> tuple[str | None, str]:
    """Query free web AI sequentially. Returns (response, platform_name) or (None, "")."""
    platforms = _available_platforms()
    if preferred:
        platforms.sort(key=lambda p: 0 if p[0] == preferred else 1)

    for name, fn in platforms:
        logger.info(f"Web AI: trying {name}")
        response, _ = await _try_platform(name, fn, message)
        if response:
            return response, name

    return None, ""


async def query_web_ai_parallel(message: str) -> list[tuple[str, str]]:
    """Query ALL available platforms simultaneously.

    Returns list of (response, platform_name) sorted by response length (longest first).
    Fires all platforms at the same time — much faster than sequential.
    """
    platforms = _available_platforms()
    if not platforms:
        return []

    logger.info(f"Web AI parallel: querying {[n for n, _ in platforms]} simultaneously")

    tasks = [
        asyncio.create_task(_try_platform(name, fn, message))
        for name, fn in platforms
    ]

    results = []
    for coro in asyncio.as_completed([asyncio.shield(t) for t in tasks]):
        try:
            response, name = await coro
            if response:
                results.append((response, name))
        except Exception:
            pass

    # Sort by response length (most detailed first)
    results.sort(key=lambda r: len(r[0]), reverse=True)
    return results


async def query_web_ai_race(message: str) -> tuple[str | None, str]:
    """Query all platforms simultaneously, return the FIRST one that responds.

    Cancels remaining tasks once a winner is found.
    """
    platforms = _available_platforms()
    if not platforms:
        return None, ""

    logger.info(f"Web AI race: {[n for n, _ in platforms]}")

    tasks = {
        asyncio.create_task(_try_platform(name, fn, message)): name
        for name, fn in platforms
    }

    winner = (None, "")
    pending = set(tasks.keys())

    while pending:
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            try:
                response, name = task.result()
                if response:
                    winner = (response, name)
                    # Cancel remaining tasks
                    for t in pending:
                        t.cancel()
                    pending.clear()
                    break
            except Exception:
                pass

    return winner


async def close_web_ai():
    """Close all web AI browser pages."""
    global _pw, _browser
    async with _browser_init_lock:
        for name, page in _pages.items():
            try:
                if not page.is_closed():
                    await page.close()
            except Exception:
                pass
        _pages.clear()

        try:
            if _browser:
                await _browser.close()
            if _pw:
                await _pw.stop()
        except Exception:
            pass
        _browser = None
        _pw = None
