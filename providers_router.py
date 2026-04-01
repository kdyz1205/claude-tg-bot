"""
Multi-Model Provider Router — uses LOCAL CLI subscriptions only (zero API cost).

Routes requests across local CLIs:
- Claude CLI (Claude Pro/Max subscription — primary, best reasoning)
- Codex CLI (OpenAI subscription — secondary, fast)
- Web AI (browser automation fallback — free)

NO paid API calls. All models run through your existing subscriptions.

Routing modes:
- "auto": Claude CLI primary, Codex CLI secondary, Web AI fallback
- "claude": force Claude CLI only
- "codex": force Codex CLI only
- "round_robin": alternate between Claude CLI and Codex CLI
"""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import time
from dataclasses import dataclass, field
from typing import Any

import config

log = logging.getLogger(__name__)


@dataclass
class ProviderStats:
    success_count: int = 0
    failure_count: int = 0
    total_latency_ms: float = 0.0
    last_failure_time: float = 0.0
    last_success_time: float = 0.0
    consecutive_failures: int = 0

    @property
    def avg_latency_ms(self) -> float:
        total = self.success_count + self.failure_count
        if total == 0:
            return 0
        return self.total_latency_ms / total

    @property
    def success_rate(self) -> float:
        total = self.success_count + self.failure_count
        if total == 0:
            return 1.0
        return self.success_count / total

    @property
    def is_healthy(self) -> bool:
        if self.consecutive_failures >= 3:
            cooldown = min(300, 60 * self.consecutive_failures)
            if time.time() - self.last_failure_time < cooldown:
                return False
        return True


PROVIDERS = ["claude_cli", "codex_cli", "web_ai"]


def _find_codex_cmd() -> str | None:
    """Find the codex CLI executable path."""
    import shutil
    # shutil.which works with .cmd on Windows
    path = shutil.which("codex")
    if path:
        return path
    # Manual fallback: npm global bin
    npm_path = os.path.join(os.environ.get("APPDATA", ""), "npm", "codex.cmd")
    if os.path.isfile(npm_path):
        return npm_path
    return None


_CODEX_CMD: str | None = _find_codex_cmd()


def _check_codex_available() -> bool:
    """Check if codex CLI is installed and accessible."""
    if not _CODEX_CMD:
        return False
    try:
        r = subprocess.run([_CODEX_CMD, "--version"], capture_output=True, timeout=5, text=True)
        return r.returncode == 0
    except Exception:
        return False


class ModelRouter:
    """Local-CLI-only router. Zero API costs."""

    def __init__(self):
        self._mode: str = os.environ.get("ROUTER_MODE", "auto")
        self._stats: dict[str, ProviderStats] = {p: ProviderStats() for p in PROVIDERS}
        self._robin_idx: int = 0
        self._lock = asyncio.Lock()
        self._codex_available: bool = _check_codex_available()
        if self._codex_available:
            log.info("Codex CLI detected — available as secondary provider")
        else:
            log.info("Codex CLI not found — Claude CLI only")

    @property
    def mode(self) -> str:
        return self._mode

    @mode.setter
    def mode(self, value: str):
        valid = ("auto", "claude", "codex", "round_robin", "web_ai")
        if value in valid:
            self._mode = value

    def available_providers(self) -> list[str]:
        available = ["claude_cli"]
        if self._codex_available:
            available.append("codex_cli")
        available.append("web_ai")
        return available

    def healthy_providers(self) -> list[str]:
        return [p for p in self.available_providers() if self._stats[p].is_healthy]

    def select_provider(self, task_hint: str = "") -> str:
        """Select the best provider for a given task."""
        healthy = self.healthy_providers()
        if not healthy:
            return "claude_cli"

        if self._mode == "claude":
            return "claude_cli"
        elif self._mode == "codex" and "codex_cli" in healthy:
            return "codex_cli"
        elif self._mode == "web_ai":
            return "web_ai"
        elif self._mode == "round_robin":
            cli_providers = [p for p in healthy if p != "web_ai"]
            if cli_providers:
                self._robin_idx = (self._robin_idx + 1) % len(cli_providers)
                return cli_providers[self._robin_idx]
            return healthy[0]

        # Auto mode: Claude CLI primary (best reasoning)
        # Codex CLI as fallback if Claude is unhealthy
        if "claude_cli" in healthy:
            return "claude_cli"
        if "codex_cli" in healthy:
            return "codex_cli"
        return healthy[0]

    def record_success(self, provider: str, latency_ms: float):
        stats = self._stats.get(provider)
        if stats:
            stats.success_count += 1
            stats.total_latency_ms += latency_ms
            stats.last_success_time = time.time()
            stats.consecutive_failures = 0

    def record_failure(self, provider: str):
        stats = self._stats.get(provider)
        if stats:
            stats.failure_count += 1
            stats.last_failure_time = time.time()
            stats.consecutive_failures += 1

    async def route_message(
        self,
        user_message: str,
        chat_id: int,
        context: Any,
        task_hint: str = "",
    ) -> tuple[bool, str]:
        """Route a message through the provider chain. Returns (success, provider_used)."""
        primary = self.select_provider(task_hint or user_message)
        providers_to_try = [primary]
        for p in self.healthy_providers():
            if p not in providers_to_try:
                providers_to_try.append(p)

        for provider in providers_to_try:
            t0 = time.time()
            try:
                success = await self._call_provider(provider, user_message, chat_id, context)
                latency = (time.time() - t0) * 1000
                if success:
                    self.record_success(provider, latency)
                    return True, provider
                else:
                    self.record_failure(provider)
            except Exception as e:
                self.record_failure(provider)
                log.warning("Provider %s failed: %s", provider, str(e)[:200])

        return False, "none"

    async def _call_provider(
        self, provider: str, message: str, chat_id: int, context: Any
    ) -> bool:
        """Call a specific provider. Returns True if successful."""
        if provider == "claude_cli":
            from claude_agent import _process_with_claude_cli
            return await _process_with_claude_cli(message, chat_id, context)

        elif provider == "codex_cli":
            return await self._call_codex_cli(message, chat_id, context)

        elif provider == "web_ai":
            from claude_agent import _fallback_to_web_ai
            return await _fallback_to_web_ai(message, chat_id, context)

        return False

    async def _call_codex_cli(
        self, message: str, chat_id: int, context: Any
    ) -> bool:
        """Call Codex CLI (OpenAI subscription, local, no API cost)."""
        if not _CODEX_CMD:
            return False
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    subprocess.run,
                    [_CODEX_CMD, "--quiet", "--full-auto", "-m", "o4-mini", message],
                    capture_output=True, text=True, timeout=120,
                    cwd=os.path.dirname(os.path.abspath(__file__)),
                ),
                timeout=130,
            )
            response = (result.stdout or "").strip()
            if not response and result.stderr:
                response = result.stderr.strip()
            if response:
                from claude_agent import _send_response
                await _send_response(chat_id, f"[Codex] {response}", context)
                return True
            return False
        except Exception as e:
            log.warning("Codex CLI error: %s", str(e)[:200])
            return False

    def get_status(self) -> dict:
        """Return router status for display."""
        available = self.available_providers()
        healthy = self.healthy_providers()
        return {
            "mode": self._mode,
            "available": available,
            "healthy": healthy,
            "stats": {
                p: {
                    "success": s.success_count,
                    "failure": s.failure_count,
                    "avg_latency_ms": round(s.avg_latency_ms, 0),
                    "success_rate": f"{s.success_rate:.0%}",
                    "healthy": s.is_healthy,
                    "consecutive_failures": s.consecutive_failures,
                }
                for p, s in self._stats.items()
                if p in available
            },
        }

    def format_status(self) -> str:
        """Format router status for Telegram display."""
        status = self.get_status()
        lines = [
            "━━ AI Model Router ━━",
            f"模式: {status['mode'].upper()}",
            f"💰 成本: $0 (全部本地订阅)",
            "",
            "可用模型:",
        ]
        provider_names = {
            "claude_cli": "Claude CLI (Pro/Max订阅)",
            "codex_cli": "Codex CLI (OpenAI订阅)",
            "web_ai": "Web AI (免费浏览器)",
        }
        for p, s in status["stats"].items():
            health = "🟢" if s["healthy"] else "🔴"
            name = provider_names.get(p, p)
            lines.append(
                f"  {health} {name}\n"
                f"     {s['success']}✓ {s['failure']}✗ "
                f"({s['success_rate']}) ~{s['avg_latency_ms']:.0f}ms"
            )
        lines.append(f"\n/provider auto|claude|codex|round_robin")
        return "\n".join(lines)


# Module-level singleton
model_router = ModelRouter()
