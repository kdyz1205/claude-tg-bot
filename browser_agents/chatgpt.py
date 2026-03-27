"""
ChatGPT Browser Agent — automates chatgpt.com

Handles:
- Standard ChatGPT conversations
- GPT-4o, GPT-4, o1 models
- Code block extraction

IMPORTANT: You must be logged in to chatgpt.com in the browser profile.
Set user_data_dir in BrowserConfig to your Chrome profile path.

Chrome profile paths:
- Windows: C:/Users/<user>/AppData/Local/Google/Chrome/User Data
- macOS: ~/Library/Application Support/Google/Chrome
- Linux: ~/.config/google-chrome
"""

from __future__ import annotations

import asyncio
import re
import logging

from browser_agents.base import BrowserAgent, BrowserConfig

logger = logging.getLogger(__name__)


class ChatGPTAgent(BrowserAgent):
    PLATFORM_NAME = "chatgpt"
    URL = "https://chatgpt.com"

    # CSS Selectors — these may change as OpenAI updates their UI.
    # Update these if automation breaks.
    SELECTORS = {
        "input": "div#prompt-textarea",
        "send_button": 'button[data-testid="send-button"]',
        "response": "div.markdown",
        "code_block": "div.markdown pre code",
        "stop_button": 'button[aria-label="Stop generating"]',
        "login_check": 'div#prompt-textarea',  # Present when logged in
    }

    async def check_login(self):
        """Verify we're logged in to ChatGPT."""
        try:
            await self._page.wait_for_selector(
                self.SELECTORS["login_check"],
                timeout=15_000,
            )
            logger.info("[chatgpt] Login verified")
        except Exception:
            raise RuntimeError(
                "Not logged in to ChatGPT. Please log in manually first, "
                "then set user_data_dir to your Chrome profile path."
            )

    async def find_input(self):
        """Find the prompt input textarea."""
        return await self._page.wait_for_selector(
            self.SELECTORS["input"],
            timeout=10_000,
        )

    async def send_prompt(self, prompt: str):
        """Type prompt and click send."""
        input_el = await self.find_input()
        await input_el.click()
        await self._page.keyboard.insert_text(prompt)
        await asyncio.sleep(0.5)

        # Click send button
        send_btn = await self._page.wait_for_selector(
            self.SELECTORS["send_button"],
            timeout=5_000,
        )
        await send_btn.click()
        logger.info(f"[chatgpt] Prompt sent ({len(prompt)} chars)")

    async def wait_for_response(self) -> str:
        """Wait for ChatGPT to finish generating."""
        # Wait for response to start appearing
        await asyncio.sleep(3)

        # Wait until the stop button disappears (generation complete)
        try:
            for _ in range(60):  # Max 2 minutes
                stop_btn = await self._page.query_selector(self.SELECTORS["stop_button"])
                if stop_btn is None:
                    break
                await asyncio.sleep(2)
        except Exception:
            pass

        await asyncio.sleep(1)  # Let DOM settle

        # Get the last response
        responses = await self._page.query_selector_all(self.SELECTORS["response"])
        if not responses:
            return ""

        text = await responses[-1].inner_text()
        logger.info(f"[chatgpt] Response received ({len(text)} chars)")
        return text

    async def extract_code_blocks(self) -> list[str]:
        """Extract all code blocks from the last response."""
        code_elements = await self._page.query_selector_all(self.SELECTORS["code_block"])
        blocks = []
        for el in code_elements:
            code = await el.inner_text()
            if code.strip():
                blocks.append(code.strip())
        return blocks
