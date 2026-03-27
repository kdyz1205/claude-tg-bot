"""
Grok Browser Agent — automates grok.com (X/Twitter's AI)

Grok is free to use with an X account.
Good for quick Q&A and moderate reasoning tasks.
"""

from __future__ import annotations

import asyncio
import logging

from browser_agents.base import BrowserAgent, BrowserConfig

logger = logging.getLogger(__name__)


class GrokAgent(BrowserAgent):
    PLATFORM_NAME = "grok"
    URL = "https://grok.com"

    SELECTORS = {
        "input": 'textarea, div[contenteditable="true"]',
        "send_button": 'button[type="submit"], button[aria-label*="Send"], button[aria-label*="send"]',
        "response": 'div[class*="message"], div[class*="response"], div[class*="markdown"]',
        "code_block": "pre code",
        "login_check": 'textarea, div[contenteditable="true"]',
    }

    async def check_login(self):
        """Verify logged in to Grok."""
        try:
            await self._page.wait_for_selector(
                self.SELECTORS["login_check"],
                timeout=15_000,
            )
            logger.info("[grok] Login verified")
        except Exception:
            raise RuntimeError(
                "Not logged in to Grok. Log in to grok.com manually first."
            )

    async def find_input(self):
        return await self._page.wait_for_selector(
            self.SELECTORS["input"],
            timeout=10_000,
        )

    async def send_prompt(self, prompt: str):
        input_el = await self.find_input()
        await input_el.click()
        await self._page.keyboard.insert_text(prompt)
        await asyncio.sleep(0.5)

        # Try send button, fallback to Enter key
        try:
            send_btn = await self._page.wait_for_selector(
                self.SELECTORS["send_button"],
                timeout=3_000,
            )
            await send_btn.click()
        except Exception:
            await self._page.keyboard.press("Enter")

        logger.info(f"[grok] Prompt sent ({len(prompt)} chars)")

    async def wait_for_response(self) -> str:
        await asyncio.sleep(3)
        return await self._wait_for_idle(self.SELECTORS["response"])

    async def extract_code_blocks(self) -> list[str]:
        code_elements = await self._page.query_selector_all(self.SELECTORS["code_block"])
        blocks = []
        for el in code_elements:
            code = await el.inner_text()
            if code.strip():
                blocks.append(code.strip())
        return blocks
