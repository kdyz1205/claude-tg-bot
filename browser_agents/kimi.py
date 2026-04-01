"""
Kimi (Moonshot) — browser automation for kimi.moonshot.cn

Requires an active login in the browser profile (CDP or user_data_dir).
Selectors are intentionally broad; site UI changes may require updates.
"""

from __future__ import annotations

import asyncio
import logging

from browser_agents.base import BrowserAgent

logger = logging.getLogger(__name__)


class KimiAgent(BrowserAgent):
    PLATFORM_NAME = "kimi"
    URL = "https://kimi.moonshot.cn/"

    SELECTORS = {
        "input": 'textarea, div[contenteditable="true"]',
        "send_button": (
            'button[type="submit"], button[aria-label*="发送"], '
            'button[aria-label*="Send"], svg[class*="send"]'
        ),
        "response": (
            'div[class*="message"], div[class*="Message"], '
            'div.markdown, article, [class*="markdown"]'
        ),
        "code_block": "pre code",
        "login_check": 'textarea, div[contenteditable="true"]',
    }

    async def check_login(self):
        try:
            await self._page.wait_for_selector(
                self.SELECTORS["login_check"],
                timeout=20_000,
            )
            logger.info("[kimi] input area ready")
        except Exception:
            raise RuntimeError(
                "Kimi: 未检测到输入框。请在浏览器中登录 kimi.moonshot.cn 后重试，"
                "并配置 JARVIS_BROWSER_CDP_URL 或 JARVIS_BROWSER_USER_DATA_DIR。"
            )

    async def find_input(self):
        return await self._page.wait_for_selector(
            self.SELECTORS["input"],
            timeout=15_000,
        )

    async def send_prompt(self, prompt: str):
        input_el = await self.find_input()
        await input_el.click()
        await self._page.keyboard.insert_text(prompt)
        await asyncio.sleep(0.4)
        try:
            send_btn = await self._page.wait_for_selector(
                self.SELECTORS["send_button"],
                timeout=4_000,
            )
            await send_btn.click()
        except Exception:
            await self._page.keyboard.press("Enter")
        logger.info("[kimi] prompt sent (%d chars)", len(prompt))

    async def wait_for_response(self) -> str:
        await asyncio.sleep(2.5)
        return await self._wait_for_idle(self.SELECTORS["response"], check_interval=2.0, max_checks=90)

    async def extract_code_blocks(self) -> list[str]:
        code_elements = await self._page.query_selector_all(self.SELECTORS["code_block"])
        blocks: list[str] = []
        for el in code_elements:
            code = await el.inner_text()
            if code.strip():
                blocks.append(code.strip())
        return blocks
