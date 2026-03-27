"""
Claude Web Browser Agent — automates claude.ai

This is for standard Claude conversations on the web.
Good for moderate-to-heavy code tasks.

With a Pro subscription, you get generous usage of Sonnet/Opus.
"""

from __future__ import annotations

import asyncio
import logging

from browser_agents.base import BrowserAgent, BrowserConfig

logger = logging.getLogger(__name__)


class ClaudeWebAgent(BrowserAgent):
    PLATFORM_NAME = "claude_web"
    URL = "https://claude.ai/new"

    SELECTORS = {
        "input": 'div.ProseMirror[contenteditable="true"], div[contenteditable="true"]',
        "send_button": 'button[aria-label="Send Message"], button[aria-label*="Send"]',
        "response": 'div[class*="font-claude-message"], div[data-is-streaming]',
        "response_complete": 'div[class*="font-claude-message"]',
        "code_block": 'div[class*="code-block"] pre code, pre code',
        "stop_button": 'button[aria-label="Stop Response"]',
        "login_check": 'div.ProseMirror[contenteditable="true"], div[contenteditable="true"]',
    }

    async def check_login(self):
        try:
            await self._page.wait_for_selector(
                self.SELECTORS["login_check"],
                timeout=15_000,
            )
            logger.info("[claude_web] Login verified")
        except Exception:
            raise RuntimeError(
                "Not logged in to claude.ai. Log in manually first."
            )

    async def find_input(self):
        return await self._page.wait_for_selector(
            self.SELECTORS["input"],
            timeout=10_000,
        )

    async def send_prompt(self, prompt: str):
        input_el = await self.find_input()
        await input_el.click()

        # Claude uses ProseMirror, need to handle paste differently
        await self._page.keyboard.insert_text(prompt)
        await asyncio.sleep(0.5)

        # Click send or use keyboard shortcut
        try:
            send_btn = await self._page.wait_for_selector(
                self.SELECTORS["send_button"],
                timeout=3_000,
            )
            await send_btn.click()
        except Exception:
            # Fallback: Enter sends in Claude
            await self._page.keyboard.press("Enter")

        logger.info(f"[claude_web] Prompt sent ({len(prompt)} chars)")

    async def wait_for_response(self) -> str:
        await asyncio.sleep(3)

        # Wait for stop button to disappear (streaming complete)
        try:
            for _ in range(90):  # Max 3 minutes for long responses
                stop_btn = await self._page.query_selector(self.SELECTORS["stop_button"])
                if stop_btn is None:
                    # Double check streaming is done
                    streaming = await self._page.query_selector('div[data-is-streaming="true"]')
                    if streaming is None:
                        break
                await asyncio.sleep(2)
        except Exception:
            pass

        await asyncio.sleep(1)

        # Get last response
        responses = await self._page.query_selector_all(self.SELECTORS["response_complete"])
        if not responses:
            return ""

        text = await responses[-1].inner_text()
        logger.info(f"[claude_web] Response received ({len(text)} chars)")
        return text

    async def extract_code_blocks(self) -> list[str]:
        code_elements = await self._page.query_selector_all(self.SELECTORS["code_block"])
        blocks = []
        for el in code_elements:
            code = await el.inner_text()
            if code.strip():
                blocks.append(code.strip())
        return blocks
