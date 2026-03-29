"""
Browser automation via Playwright.
Provides high-level browser control without needing screenshot+click.
"""
import asyncio
import logging
from io import BytesIO
from playwright.async_api import async_playwright, Browser, Page, BrowserContext, TimeoutError as PlaywrightTimeoutError

logger = logging.getLogger(__name__)

# Singleton browser instance
_playwright = None
_browser: Browser = None
_context: BrowserContext = None
_page: Page = None

# Global lock to prevent concurrent browser operations
_browser_lock: asyncio.Lock | None = None


def _get_browser_lock() -> asyncio.Lock:
    """Get or create the browser lock for the current event loop."""
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _ensure_browser():
    """Ensure browser is launched. Reuse existing instance. Timeout after 30s."""
    global _playwright, _browser, _context, _page

    async def _do_ensure():
        global _playwright, _browser, _context, _page
        if _browser and _browser.is_connected():
            if _page and not _page.is_closed():
                return _page
            # Page closed, make a new one — but verify context is still alive
            if _context:
                try:
                    _page = await _context.new_page()
                    return _page
                except Exception:
                    logger.warning("Context broken — will re-launch browser")
            # Fall through to full re-launch below

        # Clean up stale instances before re-launching
        if _browser is not None:
            logger.warning("Browser disconnected — cleaning up before restart")
            for obj, action in [
                (_page, None), (_context, "close"), (_browser, "close"), (_playwright, "stop")
            ]:
                try:
                    if obj is not None and action:
                        await getattr(obj, action)()
                except Exception:
                    pass
            _page = _context = _browser = _playwright = None

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

    try:
        return await asyncio.wait_for(_do_ensure(), timeout=30.0)
    except asyncio.TimeoutError:
        raise RuntimeError("Browser launch timed out after 30 seconds")


async def browser_navigate(url: str) -> str:
    """Navigate to a URL. Returns page title."""
    async with _get_browser_lock():
        try:
            if not url.startswith(("http://", "https://")):
                url = "https://" + url
            page = await _ensure_browser()
        except Exception as e:
            return f"Browser init error: {e}"
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=15000)
            title = await page.title()
            # Detect certificate / blocked-page errors
            _error_indicators = [
                "privacy error", "your connection is not private",
                "err_", "this site can", "not secure",
                "certificate error", "security warning",
            ]
            title_lower = title.lower()
            if any(ind in title_lower for ind in _error_indicators):
                return (
                    f"WARNING: Possible certificate/security error at {url}\n"
                    f"Page title: {title}\n"
                    "The page may be blocked or showing a security warning."
                )
            return f"Navigated to: {url}\nPage title: {title}"
        except PlaywrightTimeoutError:
            # Page may have partially loaded; report what we have
            title = "unknown"
            try:
                if page and not page.is_closed():
                    title = await page.title()
            except Exception:
                pass
            return f"Navigation timed out for: {url} (page may be partially loaded)\nPage title: {title}"
        except Exception as e:
            return f"Navigation error: {e}"


async def browser_click(selector: str) -> str:
    """Click an element by CSS selector or text content."""
    async with _get_browser_lock():
        try:
            page = await _ensure_browser()
        except Exception as e:
            return f"Browser init error: {e}"
        try:
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
    async with _get_browser_lock():
        try:
            page = await _ensure_browser()
        except Exception as e:
            return f"Browser init error: {e}"
        try:
            element = None
            found = False
            # Try CSS selector
            try:
                loc = page.locator(selector).first
                await loc.click(timeout=3000)
                element = loc
                found = True
            except Exception:
                pass
            # Try placeholder
            if not found:
                try:
                    loc = page.get_by_placeholder(selector).first
                    await loc.click(timeout=3000)
                    element = loc
                    found = True
                except Exception:
                    pass
            # Try label
            if not found:
                try:
                    loc = page.get_by_label(selector).first
                    await loc.click(timeout=3000)
                    element = loc
                    found = True
                except Exception:
                    pass
            # Try role
            if not found:
                try:
                    loc = page.get_by_role("textbox", name=selector).first
                    await loc.click(timeout=3000)
                    element = loc
                    found = True
                except Exception:
                    pass

            if found and element:
                await element.fill(text)
                if press_enter:
                    await element.press("Enter")
                return f"Typed '{text}' into {selector}" + (" and pressed Enter" if press_enter else "")
            return f"Could not find input: {selector}"
        except Exception as e:
            return f"Type error: {e}"


async def browser_screenshot() -> BytesIO:
    """Take a screenshot of the current browser page."""
    async with _get_browser_lock():
        try:
            page = await _ensure_browser()
        except Exception as e:
            logger.error(f"Browser init error: {e}")
            return None
        try:
            img_bytes = await page.screenshot(type="jpeg", quality=80, full_page=False)
            buf = BytesIO(img_bytes)
            buf.name = "browser_screenshot.jpg"
            return buf
        except Exception as e:
            logger.error(f"Browser screenshot error: {e}")
            return None


async def browser_get_text() -> str:
    """Get visible text content of the current page."""
    async with _get_browser_lock():
        try:
            page = await _ensure_browser()
        except Exception as e:
            return f"Browser init error: {e}"
        try:
            text = await asyncio.wait_for(page.inner_text("body"), timeout=5.0)
            # Truncate
            if len(text) > 8000:
                text = text[:8000] + "\n... (truncated)"
            return f"URL: {page.url}\nTitle: {await page.title()}\n\nPage text:\n{text}"
        except Exception as e:
            return f"Error getting page text: {e}"


async def browser_get_elements(selector: str = None) -> str:
    """Get information about elements on the page. Useful for finding what to click."""
    async with _get_browser_lock():
        try:
            page = await _ensure_browser()
        except Exception as e:
            return f"Browser init error: {e}"
        try:
            if selector:
                elements = await page.query_selector_all(selector)
            else:
                # Get all interactive elements
                elements = await page.query_selector_all(
                    "a, button, input, select, textarea, [role='button'], [onclick]"
                )

            results = []
            for i, el in enumerate(elements[:50]):  # Limit to 50
                try:
                    tag = await el.evaluate("e => e.tagName.toLowerCase()")
                    try:
                        raw_text = await el.inner_text()
                        text = raw_text.strip()[:100] if raw_text else ""
                    except Exception:
                        text = ""
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
                except Exception:
                    # Element became stale (detached from DOM), skip it
                    continue

            if not results:
                return "No elements found."
            return f"Found {len(results)} elements:\n" + "\n".join(results)
        except Exception as e:
            return f"Error: {e}"


async def browser_scroll(direction: str = "down", amount: int = 3) -> str:
    """Scroll the browser page."""
    async with _get_browser_lock():
        try:
            page = await _ensure_browser()
        except Exception as e:
            return f"Browser init error: {e}"
        try:
            amount = max(1, min(abs(amount), 20))  # Clamp to 1-20, always positive
            pixels = amount * 300
            if direction == "up":
                pixels = -pixels
            await page.evaluate(f"window.scrollBy(0, {pixels})")
            return f"Scrolled {direction} by {abs(pixels)}px"
        except Exception as e:
            return f"Scroll error: {e}"


async def browser_go_back() -> str:
    """Go back to the previous page."""
    async with _get_browser_lock():
        try:
            page = await _ensure_browser()
        except Exception as e:
            return f"Browser init error: {e}"
        try:
            await page.go_back(timeout=10000)
            return f"Went back. Now at: {page.url}"
        except Exception as e:
            return f"Go back error: {e}"


async def browser_tabs() -> str:
    """List all open browser tabs."""
    async with _get_browser_lock():
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
    async with _get_browser_lock():
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
    async with _get_browser_lock():
        try:
            if not _context:
                await _ensure_browser()
            if not _context:
                return "Browser failed to initialize — no context available."
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
    async with _get_browser_lock():
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
    async with _get_browser_lock():
        try:
            page = await _ensure_browser()
        except Exception as e:
            return f"Browser init error: {e}"
        try:
            result = await page.evaluate(code)
            return f"JS result: {result}"
        except Exception as e:
            return f"JS error: {e}"


async def browser_wait_for(selector: str, timeout: int = 10000) -> str:
    """Wait for an element to appear on the page."""
    async with _get_browser_lock():
        try:
            page = await _ensure_browser()
        except Exception as e:
            return f"Browser init error: {e}"
        try:
            await page.wait_for_selector(selector, timeout=timeout)
            return f"Element found: {selector}"
        except Exception as e:
            return f"Wait timeout: {e}"


async def browser_current_url() -> str:
    """Return the current page URL (useful for debugging/status)."""
    async with _get_browser_lock():
        try:
            page = await _ensure_browser()
        except Exception as e:
            return f"Browser init error: {e}"
        try:
            return page.url
        except Exception as e:
            return f"Error getting URL: {e}"


async def browser_close_all() -> str:
    """Close all pages and browser contexts, then shut down the browser."""
    global _playwright, _browser, _context, _page
    async with _get_browser_lock():
        try:
            closed_count = 0
            if _context:
                for p in list(_context.pages):
                    if not p.is_closed():
                        await p.close()
                        closed_count += 1
                await _context.close()
            if _browser:
                await _browser.close()
            if _playwright:
                await _playwright.stop()
        except Exception as e:
            logger.error(f"Error during browser_close_all: {e}")
        finally:
            _playwright = _browser = _context = _page = None
        return f"Browser closed. {closed_count} page(s) cleaned up."


async def smart_navigate(url: str, wait_for_selector: str = None, timeout: int = 30000) -> str:
    """Navigate to URL, wait for page load, and auto-dismiss popups/cookie banners."""
    async with _get_browser_lock():
        try:
            if not url.startswith(("http://", "https://")):
                url = "https://" + url
            page = await _ensure_browser()
        except Exception as e:
            return f"Browser init error: {e}"
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)

            # Auto-dismiss cookie banners and common popups
            _dismiss_selectors = [
                # Cookie consent buttons (common patterns)
                'button:has-text("Accept")',
                'button:has-text("Accept all")',
                'button:has-text("Accept All")',
                'button:has-text("I agree")',
                'button:has-text("Got it")',
                'button:has-text("OK")',
                'button:has-text("Allow all")',
                'button:has-text("Agree")',
                '[id*="cookie"] button',
                '[class*="cookie"] button',
                '[id*="consent"] button',
                '[class*="consent"] button',
                '[id*="gdpr"] button',
                '[class*="gdpr"] button',
                # Close buttons on modals/overlays
                '[aria-label="Close"]',
                '[aria-label="Dismiss"]',
                'button.close',
                '.modal-close',
                '[class*="dismiss"]',
                '[class*="banner"] [class*="close"]',
            ]
            dismissed = []
            for sel in _dismiss_selectors:
                try:
                    loc = page.locator(sel).first
                    if await loc.is_visible(timeout=500):
                        await loc.click(timeout=1500)
                        dismissed.append(sel)
                        await asyncio.sleep(0.3)
                        break  # One dismiss is usually enough
                except Exception:
                    continue

            # Optionally wait for a specific selector
            if wait_for_selector:
                try:
                    await page.wait_for_selector(wait_for_selector, timeout=timeout)
                except PlaywrightTimeoutError:
                    pass  # Non-fatal: page loaded but selector not found

            title = await page.title()
            msg = f"Navigated to: {url}\nPage title: {title}"
            if dismissed:
                msg += f"\nAuto-dismissed popup/banner."
            return msg

        except PlaywrightTimeoutError:
            title = "unknown"
            try:
                if page and not page.is_closed():
                    title = await page.title()
            except Exception:
                pass
            return f"Navigation timed out for: {url} (page may be partially loaded)\nPage title: {title}"
        except Exception as e:
            return f"Navigation error: {e}"


async def extract_page_data(selectors: dict = None) -> str:
    """Extract structured data from the current page.

    If selectors is None, auto-extracts title, meta description, headings,
    links, images, and main text content. If selectors is provided, it should
    be a dict of {name: css_selector} and text content will be extracted for each.
    """
    async with _get_browser_lock():
        try:
            page = await _ensure_browser()
        except Exception as e:
            return f"Browser init error: {e}"
        try:
            import json

            if selectors:
                # Custom extraction
                data = {}
                for name, sel in selectors.items():
                    try:
                        elements = await page.query_selector_all(sel)
                        texts = []
                        for el in elements[:100]:
                            try:
                                t = await el.inner_text()
                                if t and t.strip():
                                    texts.append(t.strip()[:500])
                            except Exception:
                                pass
                        data[name] = texts
                    except Exception as e:
                        data[name] = f"Error: {e}"
                return json.dumps(data, indent=2, ensure_ascii=False)

            # Auto-extraction
            data = await page.evaluate("""() => {
                const result = {};

                // Title
                result.title = document.title || '';

                // Meta description
                const metaDesc = document.querySelector('meta[name="description"]');
                result.meta_description = metaDesc ? metaDesc.getAttribute('content') || '' : '';

                // URL
                result.url = window.location.href;

                // Headings
                result.headings = [];
                for (const tag of ['h1', 'h2', 'h3']) {
                    document.querySelectorAll(tag).forEach(el => {
                        const text = el.innerText.trim();
                        if (text) result.headings.push({level: tag, text: text.substring(0, 200)});
                    });
                }
                result.headings = result.headings.slice(0, 50);

                // Links
                result.links = [];
                document.querySelectorAll('a[href]').forEach(el => {
                    const text = el.innerText.trim();
                    const href = el.getAttribute('href');
                    if (text && href && !href.startsWith('javascript:')) {
                        result.links.push({text: text.substring(0, 100), href: href.substring(0, 300)});
                    }
                });
                result.links = result.links.slice(0, 100);

                // Images
                result.images = [];
                document.querySelectorAll('img[src]').forEach(el => {
                    result.images.push({
                        src: el.getAttribute('src').substring(0, 300),
                        alt: (el.getAttribute('alt') || '').substring(0, 200),
                    });
                });
                result.images = result.images.slice(0, 50);

                // Main text content (try article/main first, fallback to body)
                const mainEl = document.querySelector('article') ||
                               document.querySelector('main') ||
                               document.querySelector('[role="main"]') ||
                               document.body;
                result.text_content = mainEl ? mainEl.innerText.substring(0, 8000) : '';

                return result;
            }""")

            return json.dumps(data, indent=2, ensure_ascii=False)

        except Exception as e:
            return f"Error extracting page data: {e}"


async def fill_form(fields: dict) -> str:
    """Smart form filling. fields = {label_or_placeholder: value}.

    For each field, tries to locate the input by label text, placeholder,
    aria-label, name attribute, or nearby text, then fills it.
    """
    async with _get_browser_lock():
        try:
            page = await _ensure_browser()
        except Exception as e:
            return f"Browser init error: {e}"

        results = []
        for key, value in fields.items():
            filled = False
            strategies = [
                ("label", lambda k=key: page.get_by_label(k).first),
                ("placeholder", lambda k=key: page.get_by_placeholder(k).first),
                ("role textbox", lambda k=key: page.get_by_role("textbox", name=k).first),
                ("role combobox", lambda k=key: page.get_by_role("combobox", name=k).first),
                ("css name", lambda k=key: page.locator(f'[name="{k}"]').first),
                ("css id", lambda k=key: page.locator(f'#{k}').first),
                ("css selector", lambda k=key: page.locator(k).first),
            ]

            for strategy_name, get_locator in strategies:
                try:
                    loc = get_locator()
                    # Check the element exists and is visible
                    if await loc.is_visible(timeout=1500):
                        tag = await loc.evaluate("el => el.tagName.toLowerCase()")
                        input_type = await loc.evaluate("el => el.type || ''")

                        if tag == "select":
                            # For select elements, select by visible text or value
                            try:
                                await loc.select_option(label=value)
                            except Exception:
                                await loc.select_option(value=value)
                        elif input_type == "checkbox" or input_type == "radio":
                            should_check = str(value).lower() in ("true", "1", "yes", "on")
                            if should_check:
                                await loc.check()
                            else:
                                await loc.uncheck()
                        else:
                            await loc.click(timeout=2000)
                            await loc.fill(str(value))

                        results.append(f"Filled '{key}' = '{value}' (found via {strategy_name})")
                        filled = True
                        break
                except Exception:
                    continue

            if not filled:
                results.append(f"FAILED to find field: '{key}'")

        return "\n".join(results) if results else "No fields provided."


async def wait_and_click(text: str = None, selector: str = None, timeout: int = 10000) -> str:
    """Find element by visible text OR CSS selector, scroll into view, and click.

    Retries once if the first click fails (e.g. element was obscured by an overlay).
    """
    async with _get_browser_lock():
        try:
            page = await _ensure_browser()
        except Exception as e:
            return f"Browser init error: {e}"

        if not text and not selector:
            return "Error: must provide either 'text' or 'selector'."

        for attempt in range(2):
            try:
                loc = None
                found_via = ""

                if selector:
                    try:
                        loc = page.locator(selector).first
                        await loc.wait_for(state="visible", timeout=timeout if attempt == 0 else 3000)
                        found_via = f"selector '{selector}'"
                    except Exception:
                        loc = None

                if not loc and text:
                    # Try get_by_text
                    try:
                        loc = page.get_by_text(text, exact=False).first
                        await loc.wait_for(state="visible", timeout=timeout if attempt == 0 else 3000)
                        found_via = f"text '{text}'"
                    except Exception:
                        loc = None

                    # Try get_by_role button
                    if not loc:
                        try:
                            loc = page.get_by_role("button", name=text).first
                            await loc.wait_for(state="visible", timeout=3000)
                            found_via = f"button role '{text}'"
                        except Exception:
                            loc = None

                    # Try get_by_role link
                    if not loc:
                        try:
                            loc = page.get_by_role("link", name=text).first
                            await loc.wait_for(state="visible", timeout=3000)
                            found_via = f"link role '{text}'"
                        except Exception:
                            loc = None

                if not loc:
                    if attempt == 0:
                        continue
                    return f"Could not find element with text='{text}' selector='{selector}'. Try browser_get_elements to see what's available."

                # Scroll into view and click
                await loc.scroll_into_view_if_needed(timeout=3000)
                await loc.click(timeout=5000)
                return f"Clicked element found via {found_via}." + (f" (retry #{attempt+1})" if attempt > 0 else "")

            except Exception as e:
                if attempt == 0:
                    await asyncio.sleep(0.5)
                    continue
                return f"Click failed after retry: {e}"

        return "Click failed after 2 attempts."


async def screenshot_element(selector: str, path: str = None) -> BytesIO:
    """Screenshot a specific element (not the whole page).

    Returns a BytesIO buffer with the screenshot. If path is provided,
    also saves to that file path.
    """
    async with _get_browser_lock():
        try:
            page = await _ensure_browser()
        except Exception as e:
            logger.error(f"Browser init error: {e}")
            return None
        try:
            # Try CSS selector first
            element = None
            try:
                loc = page.locator(selector).first
                await loc.wait_for(state="visible", timeout=5000)
                element = loc
            except Exception:
                pass

            # Try by text
            if element is None:
                try:
                    loc = page.get_by_text(selector, exact=False).first
                    await loc.wait_for(state="visible", timeout=3000)
                    element = loc
                except Exception:
                    pass

            if element is None:
                logger.error(f"Element not found: {selector}")
                return None

            await element.scroll_into_view_if_needed(timeout=3000)

            screenshot_opts = {"type": "jpeg", "quality": 85}
            if path:
                screenshot_opts["path"] = path

            img_bytes = await element.screenshot(**screenshot_opts)
            buf = BytesIO(img_bytes)
            buf.name = "element_screenshot.jpg"
            return buf

        except Exception as e:
            logger.error(f"Element screenshot error: {e}")
            return None


async def close_browser():
    """Cleanup browser resources."""
    global _playwright, _browser, _context, _page
    async with _get_browser_lock():
        try:
            if _context:
                await _context.close()
        except Exception:
            pass
        try:
            if _browser:
                await _browser.close()
        except Exception:
            pass
        try:
            if _playwright:
                await _playwright.stop()
        except Exception:
            pass
        _playwright = _browser = _context = _page = None
