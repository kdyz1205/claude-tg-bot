"""
Multi-model router: local CLI chain + optional HTTP API tier routing.

Local (zero API $): Claude CLI → Codex CLI → Web AI — see `ModelRouter`.

HTTP API tier (`execute_api_routed_turn`): when Anthropic/OpenAI keys are set,
classifies each turn as FAST (cheap/fast models) vs HEAVY (strong reasoning /
multimodal), then runs `providers.process_tiered_api_fallback` with transient
retries and Claude → OpenAI → Gemini degradation.
"""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import config

log = logging.getLogger(__name__)


class TaskTier(str, Enum):
    """API compute tier for Anthropic/OpenAI model selection."""

    FAST = "fast"
    HEAVY = "heavy"


# Signals for routing (Chinese + English). FAST = short judgments / extraction.
_FAST_SIGNALS = (
    "格式化", "提取", "抽取", "判断", "分类", "打标签", "转json", "parse", "regex",
    "总结以下", "一句话", "只需", "简短", "yes or no", "true/false", "extract",
    "format as", "bullet list", "列表", "关键词", "是否", "json",
)
_HEAVY_SIGNALS = (
    "策略", "推演", "架构", "多模态", "回测", "实盘", "链上", "debug",
    "analyze", "解释", "为什么", "对比", "设计", "实现", "重构", "优化",
    "screenshot", "截图", "图片", "图像", "vision", "trade", "portfolio",
)


def classify_task_tier(
    user_message: str,
    *,
    has_image: bool = False,
    length_heavy_threshold: int = 900,
) -> TaskTier:
    """Route HIGH-frequency/light tasks to FAST tier; complex / vision to HEAVY."""
    if has_image:
        return TaskTier.HEAVY
    t = (user_message or "").strip()
    if len(t) >= length_heavy_threshold:
        return TaskTier.HEAVY
    low = t.lower()
    if any(s.lower() in low for s in _HEAVY_SIGNALS):
        return TaskTier.HEAVY
    if any(s.lower() in low for s in _FAST_SIGNALS) and len(t) < 420:
        return TaskTier.FAST
    if len(t) > 360:
        return TaskTier.HEAVY
    return TaskTier.FAST


def resolve_models_for_tier(tier: TaskTier) -> tuple[str, str]:
    """Return (claude_model_id, openai_model_id) for the tier."""
    if tier == TaskTier.FAST:
        return config.TASK_TIER_FAST_CLAUDE, config.TASK_TIER_FAST_OPENAI
    hc = config.TASK_TIER_HEAVY_CLAUDE or config.CLAUDE_MODEL
    ho = config.TASK_TIER_HEAVY_OPENAI or config.OPENAI_MODEL
    return hc, ho


def _last_user_plain_text(messages: list | None) -> str:
    for m in reversed(messages or []):
        if m.get("role") != "user":
            continue
        c = m.get("content")
        if isinstance(c, str):
            return c
        if isinstance(c, list):
            for p in c:
                if isinstance(p, dict) and p.get("type") == "text":
                    return (p.get("text") or "").strip()
    return ""


async def execute_api_routed_turn(
    messages: list,
    chat_id: int,
    context: Any,
    *,
    user_message_hint: str = "",
    image_data: str | None = None,
) -> bool:
    """Classify tier, pick Haiku/Mini vs Sonnet-class stack, run tiered API fallback."""
    from providers import _select_tools, process_tiered_api_fallback

    hint = (user_message_hint or _last_user_plain_text(messages)).strip()
    tier = classify_task_tier(hint, has_image=bool(image_data))
    claude_m, openai_m = resolve_models_for_tier(tier)
    log.info(
        "execute_api_routed_turn: tier=%s claude=%s openai=%s",
        tier.value,
        claude_m,
        openai_m,
    )
    selected = _select_tools(hint)
    return await process_tiered_api_fallback(
        messages,
        chat_id,
        context,
        selected_tools=selected,
        image_data=image_data,
        claude_model=claude_m,
        openai_model=openai_m,
    )


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
        cli_codex = _check_codex_available()
        api_mini = bool(
            (getattr(config, "OPENAI_API_KEY", "") or "").strip()
            or (getattr(config, "ANTHROPIC_API_KEY", "") or "").strip()
        )
        self._codex_available: bool = cli_codex or api_mini
        if cli_codex:
            log.info("Codex CLI detected — available as secondary provider")
        elif api_mini:
            log.info("HTTP mini-model fallback enabled (no Codex CLI subprocess)")
        else:
            log.info("Codex CLI not found — Claude primary only")

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
        """HTTP coding fallback (aiohttp via ``llm_http_client``) — no Codex subprocess."""
        try:
            import llm_http_client

            model = getattr(config, "TASK_TIER_FAST_OPENAI", None) or getattr(
                config, "OPENAI_MODEL", None
            )
            text, err = await llm_http_client.complete_stateless(
                system_prompt=(
                    "You are a coding assistant. Answer concisely; user is on Telegram."
                ),
                user_text=(message or "")[:120_000],
                model_hint=model,
                timeout_sec=120.0,
                state_key=-7700 - (abs(chat_id) % 10_000),
            )
            response = (text or "").strip()
            if not response and err:
                response = err.strip()
            if response:
                from claude_agent import _send_response

                await _send_response(chat_id, f"[API] {response}", context)
                return True
            return False
        except Exception as e:
            log.warning("HTTP codex fallback error: %s", str(e)[:200])
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
