"""
Base Browser Agent — abstract interface all platform controllers implement.

Uses Playwright for browser automation. Headless by default,
but can run headed for debugging.
"""

from __future__ import annotations

import asyncio
import time
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class BrowserConfig:
    """Configuration for browser automation."""
    headless: bool = False       # Show browser for debugging; True for production
    user_data_dir: str = ""      # Chrome profile dir (to reuse login sessions)
    timeout_ms: int = 120_000    # Max wait time for AI response (2 min)
    slow_mo: int = 50            # Milliseconds between actions (human-like)
    screenshot_on_error: bool = True
    screenshot_dir: str = "./screenshots"
    chrome_path: str = ""        # Custom Chrome/Edge path if needed


@dataclass
class AgentResult:
    """Result from a browser agent execution."""
    success: bool
    output: str                  # The AI's response text
    code_blocks: list[str] = field(default_factory=list)  # Extracted code
    platform: str = ""
    duration_seconds: float = 0
    error: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


class BrowserAgent(ABC):
    """
    Abstract base class for all browser-based AI agents.

    Subclasses implement platform-specific selectors and interaction patterns.
    """

    PLATFORM_NAME: str = "base"
    URL: str = ""

    def __init__(self, config: BrowserConfig | None = None):
        self.config = config or BrowserConfig()
        self._browser = None
        self._context = None
        self._page = None

    async def launch(self):
        """Launch the browser."""
        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()

        launch_args = {
            "headless": self.config.headless,
            "slow_mo": self.config.slow_mo,
        }
        if self.config.chrome_path:
            launch_args["executable_path"] = self.config.chrome_path

        # Use persistent context to reuse login sessions
        if self.config.user_data_dir:
            self._context = await self._playwright.chromium.launch_persistent_context(
                self.config.user_data_dir,
                **launch_args,
            )
            self._page = self._context.pages[0] if self._context.pages else await self._context.new_page()
        else:
            self._browser = await self._playwright.chromium.launch(**launch_args)
            self._context = await self._browser.new_context()
            self._page = await self._context.new_page()

        logger.info(f"[{self.PLATFORM_NAME}] Browser launched")

    async def close(self):
        """Close browser and cleanup."""
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        logger.info(f"[{self.PLATFORM_NAME}] Browser closed")

    async def navigate(self):
        """Navigate to the AI platform URL."""
        await self._page.goto(self.URL, wait_until="networkidle")
        logger.info(f"[{self.PLATFORM_NAME}] Navigated to {self.URL}")

    @abstractmethod
    async def find_input(self):
        """Find and return the chat input element."""
        ...

    @abstractmethod
    async def send_prompt(self, prompt: str):
        """Type the prompt and send it."""
        ...

    @abstractmethod
    async def wait_for_response(self) -> str:
        """Wait for the AI to finish responding and return the text."""
        ...

    @abstractmethod
    async def extract_code_blocks(self) -> list[str]:
        """Extract code blocks from the response."""
        ...

    async def execute(self, prompt: str) -> AgentResult:
        """
        Full execution cycle:
        1. Launch browser
        2. Navigate to platform
        3. Send prompt
        4. Wait for response
        5. Extract code
        6. Return result
        """
        start = time.time()
        try:
            await self.launch()
            await self.navigate()
            await self.check_login()
            await self.send_prompt(prompt)
            output = await self.wait_for_response()
            code_blocks = await self.extract_code_blocks()

            return AgentResult(
                success=True,
                output=output,
                code_blocks=code_blocks,
                platform=self.PLATFORM_NAME,
                duration_seconds=time.time() - start,
            )

        except Exception as e:
            logger.error(f"[{self.PLATFORM_NAME}] Error: {e}")
            if self.config.screenshot_on_error and self._page:
                ss_dir = Path(self.config.screenshot_dir)
                ss_dir.mkdir(parents=True, exist_ok=True)
                ss_path = ss_dir / f"{self.PLATFORM_NAME}_error_{int(time.time())}.png"
                await self._page.screenshot(path=str(ss_path))
                logger.info(f"Error screenshot saved: {ss_path}")

            return AgentResult(
                success=False,
                output="",
                platform=self.PLATFORM_NAME,
                duration_seconds=time.time() - start,
                error=str(e),
            )

        finally:
            await self.close()

    async def check_login(self):
        """Check if logged in. Override per platform."""
        pass

    async def _type_human_like(self, selector: str, text: str):
        """Type text character by character (more human-like, avoids detection)."""
        element = await self._page.wait_for_selector(selector, timeout=10_000)
        # Use clipboard paste for speed — Playwright fill() is too fast
        await element.click()
        await self._page.keyboard.insert_text(text)

    async def _wait_for_idle(self, selector: str, check_interval: float = 2.0, max_checks: int = 60):
        """
        Wait for the AI to stop generating.
        Polls the response element until its text stops changing.
        """
        last_text = ""
        stable_count = 0

        for _ in range(max_checks):
            await asyncio.sleep(check_interval)
            try:
                elements = await self._page.query_selector_all(selector)
                if not elements:
                    continue
                current_text = await elements[-1].inner_text()

                if current_text == last_text and current_text:
                    stable_count += 1
                    if stable_count >= 2:  # Stable for 2 consecutive checks
                        return current_text
                else:
                    stable_count = 0
                    last_text = current_text
            except Exception:
                continue

        return last_text  # Return whatever we have after timeout
