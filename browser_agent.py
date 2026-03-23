"""
Browser automation via Playwright.
Provides high-level browser control without needing screenshot+click.
"""
import asyncio
import logging
from io import BytesIO
from playwright.async_api import async_playwright, Browser, Page, BrowserContext

logger = logging.getLogger(__name__)

# Singleton browser instance
_playwright = None
_browser: Browser = None
_context: BrowserContext = None
_page: Page = None


async def _ensure_browser():
    """Ensure browser is launched. Reuse existing instance."""
    global _playwright, _browser, _context, _page
    if _browser and _browser.is_connected():
        if _page and not _page.is_closed():
            return _page
        # Page closed, make a new one
        _page = await _context.new_page()
        return _page

    _playwright = await async_playwright().start()
    _browser = await _playwright.chromium.launch(
        headless=False,  # Visible browser so user can see
        args=["--start-maximized"],
    )
    _context = await _browser.new_context(
        viewport=None,  # Use full window size
        no_viewport=True,
    )
    _page = await _context.new_page()
    return _page


async def browser_navigate(url: str) -> str:
    """Navigate to a URL. Returns page title."""
    try:
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        page = await _ensure_browser()
        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        title = await page.title()
        return f"Navigated to: {url}\nPage title: {title}"
    except Exception as e:
        return f"Navigation error: {e}"


async def browser_click(selector: str) -> str:
    """Click an element by CSS selector or text content."""
    try:
        page = await _ensure_browser()
        # Try CSS selector first
        try:
            await page.click(selector, timeout=5000)
            return f"Clicked element: {selector}"
        except Exception:
            pass
        # Try by text
        try:
            await page.get_by_text(selector, exact=False).first.click(timeout=5000)
            return f"Clicked text: {selector}"
        except Exception:
            pass
        # Try by role
        try:
            await page.get_by_role("button", name=selector).first.click(timeout=5000)
            return f"Clicked button: {selector}"
        except Exception:
            pass
        # Try by placeholder
        try:
            await page.get_by_placeholder(selector).first.click(timeout=5000)
            return f"Clicked placeholder: {selector}"
        except Exception:
            pass
        return f"Could not find element: {selector}. Try a different selector or use take_screenshot + mouse_click."
    except Exception as e:
        return f"Click error: {e}"


async def browser_type(selector: str, text: str, press_enter: bool = False) -> str:
    """Type text into an input field identified by selector or placeholder text."""
    try:
        page = await _ensure_browser()
        element = None
        # Try CSS selector
        try:
            element = page.locator(selector).first
            await element.click(timeout=3000)
        except Exception:
            pass
        # Try placeholder
        if not element:
            try:
                element = page.get_by_placeholder(selector).first
                await element.click(timeout=3000)
            except Exception:
                pass
        # Try label
        if not element:
            try:
                element = page.get_by_label(selector).first
                await element.click(timeout=3000)
            except Exception:
                pass
        # Try role
        if not element:
            try:
                element = page.get_by_role("textbox", name=selector).first
                await element.click(timeout=3000)
            except Exception:
                pass

        if element:
            await element.fill(text)
            if press_enter:
                await element.press("Enter")
            return f"Typed '{text}' into {selector}" + (" and pressed Enter" if press_enter else "")
        return f"Could not find input: {selector}"
    except Exception as e:
        return f"Type error: {e}"


async def browser_screenshot() -> BytesIO:
    """Take a screenshot of the current browser page."""
    try:
        page = await _ensure_browser()
        img_bytes = await page.screenshot(type="jpeg", quality=80, full_page=False)
        buf = BytesIO(img_bytes)
        buf.name = "browser_screenshot.jpg"
        return buf
    except Exception as e:
        logger.error(f"Browser screenshot error: {e}")
        return None


async def browser_get_text() -> str:
    """Get visible text content of the current page."""
    try:
        page = await _ensure_browser()
        text = await page.inner_text("body")
        # Truncate
        if len(text) > 8000:
            text = text[:8000] + "\n... (truncated)"
        return f"URL: {page.url}\nTitle: {await page.title()}\n\nPage text:\n{text}"
    except Exception as e:
        return f"Error getting page text: {e}"


async def browser_get_elements(selector: str = None) -> str:
    """Get information about elements on the page. Useful for finding what to click."""
    try:
        page = await _ensure_browser()
        if selector:
            elements = await page.query_selector_all(selector)
        else:
            # Get all interactive elements
            elements = await page.query_selector_all(
                "a, button, input, select, textarea, [role='button'], [onclick]"
            )

        results = []
        for i, el in enumerate(elements[:50]):  # Limit to 50
            tag = await el.evaluate("e => e.tagName.toLowerCase()")
            text = (await el.inner_text()).strip()[:100] if await el.inner_text() else ""
            href = await el.get_attribute("href") or ""
            placeholder = await el.get_attribute("placeholder") or ""
            role = await el.get_attribute("role") or ""
            name = await el.get_attribute("name") or ""
            el_id = await el.get_attribute("id") or ""

            info = f"[{i}] <{tag}"
            if el_id:
                info += f' id="{el_id}"'
            if name:
                info += f' name="{name}"'
            if role:
                info += f' role="{role}"'
            if placeholder:
                info += f' placeholder="{placeholder}"'
            info += ">"
            if text:
                info += f" {text[:60]}"
            if href:
                info += f" → {href[:80]}"
            results.append(info)

        if not results:
            return "No elements found."
        return f"Found {len(results)} elements:\n" + "\n".join(results)
    except Exception as e:
        return f"Error: {e}"


async def browser_scroll(direction: str = "down", amount: int = 3) -> str:
    """Scroll the browser page."""
    try:
        page = await _ensure_browser()
        pixels = amount * 300
        if direction == "up":
            pixels = -pixels
        await page.evaluate(f"window.scrollBy(0, {pixels})")
        return f"Scrolled {direction} by {abs(pixels)}px"
    except Exception as e:
        return f"Scroll error: {e}"


async def browser_go_back() -> str:
    """Go back to the previous page."""
    try:
        page = await _ensure_browser()
        await page.go_back(timeout=10000)
        return f"Went back. Now at: {page.url}"
    except Exception as e:
        return f"Go back error: {e}"


async def browser_tabs() -> str:
    """List all open browser tabs."""
    try:
        if not _context:
            return "No browser open."
        pages = _context.pages
        lines = []
        for i, p in enumerate(pages):
            active = " (active)" if p == _page else ""
            lines.append(f"[{i}] {await p.title()} - {p.url}{active}")
        return "\n".join(lines) if lines else "No tabs open."
    except Exception as e:
        return f"Error: {e}"


async def browser_switch_tab(index: int) -> str:
    """Switch to a specific tab by index."""
    global _page
    try:
        if not _context:
            return "No browser open."
        pages = _context.pages
        if 0 <= index < len(pages):
            _page = pages[index]
            await _page.bring_to_front()
            return f"Switched to tab {index}: {await _page.title()}"
        return f"Invalid tab index. Available: 0-{len(pages)-1}"
    except Exception as e:
        return f"Error: {e}"


async def browser_new_tab(url: str = None) -> str:
    """Open a new tab, optionally with a URL."""
    global _page
    try:
        if not _context:
            await _ensure_browser()
        _page = await _context.new_page()
        if url:
            if not url.startswith(("http://", "https://")):
                url = "https://" + url
            await _page.goto(url, wait_until="domcontentloaded", timeout=15000)
            return f"New tab opened: {url}"
        return "New empty tab opened."
    except Exception as e:
        return f"Error: {e}"


async def browser_close_tab() -> str:
    """Close the current tab."""
    global _page
    try:
        if _page and not _page.is_closed():
            await _page.close()
        if _context and _context.pages:
            _page = _context.pages[-1]
            return f"Tab closed. Active: {await _page.title()}"
        return "Tab closed. No more tabs."
    except Exception as e:
        return f"Error: {e}"


async def browser_eval_js(code: str) -> str:
    """Execute JavaScript in the browser and return result."""
    try:
        page = await _ensure_browser()
        result = await page.evaluate(code)
        return f"JS result: {result}"
    except Exception as e:
        return f"JS error: {e}"


async def browser_wait_for(selector: str, timeout: int = 10000) -> str:
    """Wait for an element to appear on the page."""
    try:
        page = await _ensure_browser()
        await page.wait_for_selector(selector, timeout=timeout)
        return f"Element found: {selector}"
    except Exception as e:
        return f"Wait timeout: {e}"


async def close_browser():
    """Cleanup browser resources."""
    global _playwright, _browser, _context, _page
    try:
        if _browser:
            await _browser.close()
        if _playwright:
            await _playwright.stop()
    except Exception:
        pass
    _playwright = _browser = _context = _page = None
