"""
Claude Code Web Browser Agent — automates claude.ai/code

This is the heavy-duty agent for complex coding tasks.
Claude Code web sessions can read/write files, run commands, etc.

This agent opens claude.ai/code, sends the task, and waits for
the full coding session to complete.
"""

from __future__ import annotations

import asyncio
import logging

from browser_agents.base import BrowserAgent, BrowserConfig

logger = logging.getLogger(__name__)


class ClaudeCodeWebAgent(BrowserAgent):
    PLATFORM_NAME = "claude_code"
    URL = "https://claude.ai/code"

    SELECTORS = {
        "input": 'textarea, div[contenteditable="true"], div.ProseMirror',
        "send_button": 'button[aria-label*="Send"], button[type="submit"]',
        "response": 'div[class*="message"], div[class*="response"]',
        "code_block": "pre code",
        "stop_button": 'button[aria-label*="Stop"], button[aria-label*="Cancel"]',
        "login_check": 'textarea, div[contenteditable="true"]',
        # Claude Code specific
        "file_tree": 'div[class*="file-tree"], nav[class*="sidebar"]',
        "terminal": 'div[class*="terminal"], div[class*="console"]',
        "accept_button": 'button:has-text("Accept"), button:has-text("approve")',
    }

    async def check_login(self):
        try:
            await self._page.wait_for_selector(
                self.SELECTORS["login_check"],
                timeout=20_000,
            )
            logger.info("[claude_code] Login verified")
        except Exception:
            raise RuntimeError(
                "Not logged in to claude.ai/code. Log in manually first."
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

        try:
            send_btn = await self._page.wait_for_selector(
                self.SELECTORS["send_button"],
                timeout=3_000,
            )
            await send_btn.click()
        except Exception:
            await self._page.keyboard.press("Enter")

        logger.info(f"[claude_code] Task sent ({len(prompt)} chars)")

    async def auto_approve(self, max_approvals: int = 20):
        """
        Auto-approve tool calls in Claude Code.
        Claude Code asks for permission before running commands/editing files.
        This auto-clicks approve buttons.
        """
        approved = 0
        for _ in range(max_approvals * 10):  # Check every 2s
            try:
                accept_btn = await self._page.query_selector(self.SELECTORS["accept_button"])
                if accept_btn:
                    await accept_btn.click()
                    approved += 1
                    logger.info(f"[claude_code] Auto-approved action #{approved}")
            except Exception:
                pass
            await asyncio.sleep(2)

            # Check if Claude Code is done (no more activity)
            stop_btn = await self._page.query_selector(self.SELECTORS["stop_button"])
            if stop_btn is None and approved > 0:
                # Wait a bit more to make sure
                await asyncio.sleep(5)
                stop_btn = await self._page.query_selector(self.SELECTORS["stop_button"])
                if stop_btn is None:
                    break

        return approved

    async def wait_for_response(self) -> str:
        """Wait for Claude Code to finish the entire coding session."""
        await asyncio.sleep(5)

        # Auto-approve tool calls while waiting
        await self.auto_approve()

        await asyncio.sleep(2)

        # Get all response text
        responses = await self._page.query_selector_all(self.SELECTORS["response"])
        if not responses:
            return ""

        # Concatenate all response parts
        full_text = ""
        for resp in responses:
            text = await resp.inner_text()
            full_text += text + "\n"

        logger.info(f"[claude_code] Session complete ({len(full_text)} chars)")
        return full_text.strip()

    async def extract_code_blocks(self) -> list[str]:
        code_elements = await self._page.query_selector_all(self.SELECTORS["code_block"])
        blocks = []
        for el in code_elements:
            code = await el.inner_text()
            if code.strip():
                blocks.append(code.strip())
        return blocks

    async def execute(self, prompt: str):
        """
        Override execute to add Claude Code specific instructions.
        Appends git push instructions to the prompt so Claude Code
        automatically pushes results.
        """
        enhanced_prompt = (
            f"{prompt}\n\n"
            "When you're done, please:\n"
            "1. Commit all changes with a descriptive message\n"
            "2. Push to the current branch\n"
        )
        return await super().execute(enhanced_prompt)
