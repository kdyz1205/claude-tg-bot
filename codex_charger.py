"""
codex_charger.py — Codex自充能模块 (Task 7/7)

当 CLI 触达 rate limit / credits 耗尽时，自动切换到 claude.ai/code
浏览器会话继续执行进化任务，实现永续运行。

使用方式:
    from codex_charger import CodexCharger
    charger = CodexCharger()
    result = await charger.run_task(task_prompt)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

BASE = Path(__file__).parent
STATE_FILE = BASE / "_codex_state.json"

# How long to wait after a failed Codex attempt before retrying (seconds)
CODEX_RETRY_DELAY = 30


def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "mode": "cli",           # "cli" | "codex"
        "codex_runs": 0,
        "codex_successes": 0,
        "last_codex_run": None,
        "cli_exhausted_at": None,
    }


def _save_state(s: dict):
    tmp = str(STATE_FILE) + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(s, f, indent=2, ensure_ascii=False)
        os.replace(tmp, str(STATE_FILE))
    except Exception as e:
        logger.error(f"codex_charger save_state error: {e}")


def get_status() -> str:
    """Return a human-readable status string."""
    s = _load_state()
    mode = s.get("mode", "cli")
    codex_runs = s.get("codex_runs", 0)
    codex_successes = s.get("codex_successes", 0)
    last = s.get("last_codex_run") or "never"
    cli_exhausted = s.get("cli_exhausted_at") or "never"

    icon = "🌐" if mode == "codex" else "💻"
    lines = [
        f"{icon} 当前模式: **{mode.upper()}**",
        f"🔄 Codex执行次数: {codex_runs} (成功: {codex_successes})",
        f"⏱ 上次Codex运行: {last}",
        f"💀 CLI耗尽时间: {cli_exhausted}",
    ]
    return "\n".join(lines)


def set_mode(mode: str):
    """Switch between 'cli' and 'codex' mode."""
    s = _load_state()
    s["mode"] = mode
    if mode == "codex":
        s["cli_exhausted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _save_state(s)
    logger.info(f"[codex_charger] Mode switched to: {mode}")


class CodexCharger:
    """
    Runs evolution tasks via claude.ai/code browser session.

    Falls back to this when CLI credits are exhausted. Uses the same
    Claude Max subscription, just via the web interface instead of CLI.
    """

    def __init__(self, cdp_url: str = "http://localhost:9222"):
        self.cdp_url = cdp_url

    async def run_task(self, prompt: str, timeout_seconds: int = 600) -> dict:
        """
        Run an evolution task via claude.ai/code.

        Returns:
            {"success": bool, "output": str, "duration": float, "error": str}
        """
        from browser_agents.claude_code_web import ClaudeCodeWebAgent
        from browser_agents.base import BrowserConfig

        s = _load_state()
        s["codex_runs"] += 1
        s["last_codex_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _save_state(s)

        logger.info(f"[codex_charger] Starting Codex run #{s['codex_runs']} ({len(prompt)} char prompt)")
        start = time.time()

        config = BrowserConfig(
            headless=False,       # Show browser so user can see progress
            cdp_url=self.cdp_url,
            timeout_ms=timeout_seconds * 1000,
            slow_mo=30,
        )

        agent = ClaudeCodeWebAgent(config=config)

        try:
            # Enhanced prompt: tell Claude Code to work in the project dir
            project_dir = str(BASE)
            full_prompt = (
                f"You are running an autonomous evolution task for a Telegram bot project.\n"
                f"Project directory: {project_dir}\n\n"
                f"{prompt}\n\n"
                f"After completing the task, output exactly: ✅任务完成"
            )

            result = await asyncio.wait_for(
                agent.execute(full_prompt),
                timeout=timeout_seconds,
            )

            duration = time.time() - start

            if result.success:
                s2 = _load_state()
                s2["codex_successes"] += 1
                _save_state(s2)
                logger.info(f"[codex_charger] Task succeeded in {duration:.1f}s")
                return {
                    "success": True,
                    "output": result.output,
                    "duration": duration,
                    "error": "",
                }
            else:
                logger.warning(f"[codex_charger] Task failed: {result.error}")
                return {
                    "success": False,
                    "output": result.output,
                    "duration": duration,
                    "error": result.error,
                }

        except asyncio.TimeoutError:
            duration = time.time() - start
            logger.warning(f"[codex_charger] Task timed out after {duration:.1f}s")
            return {
                "success": False,
                "output": "",
                "duration": duration,
                "error": f"Timeout after {timeout_seconds}s",
            }
        except Exception as e:
            duration = time.time() - start
            logger.error(f"[codex_charger] Unexpected error: {e}")
            return {
                "success": False,
                "output": "",
                "duration": duration,
                "error": str(e),
            }

    async def run_task_with_retry(self, prompt: str, max_retries: int = 2) -> dict:
        """Run task with automatic retry on failure."""
        for attempt in range(max_retries + 1):
            if attempt > 0:
                logger.info(f"[codex_charger] Retry {attempt}/{max_retries} after {CODEX_RETRY_DELAY}s")
                await asyncio.sleep(CODEX_RETRY_DELAY)

            result = await self.run_task(prompt)
            if result["success"]:
                return result

        return result  # Return last failure

    def run_task_sync(self, prompt: str) -> dict:
        """Synchronous wrapper for use in non-async contexts (e.g. smart_evolver)."""
        try:
            # Always use asyncio.run() in a thread — safe whether or not a loop exists
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    lambda: asyncio.run(self.run_task(prompt))
                )
                return future.result(timeout=700)
        except Exception as e:
            logger.error(f"[codex_charger] run_task_sync error: {e}")
            return {"success": False, "output": "", "duration": 0, "error": str(e)}


# ─── Monitor: auto-detect CLI exhaustion ─────────────────────────────────────

def is_cli_rate_limited(error_text: str) -> bool:
    """Check if text indicates CLI rate limit / credit exhaustion."""
    patterns = [
        "rate limit", "out of credits", "exceeded your", "billing",
        "quota exceeded", "too many requests", "credit balance",
        "usage limit", "error 429", "429", "overloaded",
    ]
    lower = error_text.lower()
    return any(p in lower for p in patterns)


def should_use_codex() -> bool:
    """Check if we should currently route tasks to Codex."""
    s = _load_state()
    return s.get("mode") == "codex"


def mark_cli_exhausted():
    """Call this when CLI hits limits. Switches mode to 'codex'."""
    set_mode("codex")
    logger.info("[codex_charger] CLI exhausted — switched to Codex mode")


def mark_cli_recovered():
    """Call this when CLI is working again. Switches back to CLI mode."""
    set_mode("cli")
    logger.info("[codex_charger] CLI recovered — switched back to CLI mode")


# ─── CLI entry point ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    if len(sys.argv) < 2:
        print("Usage: python codex_charger.py <status|test|mode=cli|mode=codex>")
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "status":
        print(get_status())

    elif cmd.startswith("mode="):
        new_mode = cmd.split("=", 1)[1]
        set_mode(new_mode)
        print(f"Mode set to: {new_mode}")

    elif cmd == "test":
        prompt = "Say exactly: ✅任务完成 — this is a test"
        print(f"Running test task via Codex...")
        charger = CodexCharger()
        result = charger.run_task_sync(prompt)
        print(f"Result: success={result['success']}, duration={result['duration']:.1f}s")
        print(f"Output preview: {result['output'][:200]}")
        if result['error']:
            print(f"Error: {result['error']}")
