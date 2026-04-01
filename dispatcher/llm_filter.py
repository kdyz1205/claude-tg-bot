"""
dispatcher/llm_filter.py — Zero-Trust LLM output interceptor.

ANY trade directive from an agent/LLM must pass through
LLMHallucinationFilter.sanitize_trade_directive() before reaching the
execution layer.  Rejects:
  - Non-whitelisted trading pairs
  - Notional value > MAX_NOTIONAL_USD
  - Missing / unparse-able fields
  - Action not in allowed set
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Awaitable, Callable, Dict, Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator

from self_monitor import trigger_alert

logger = logging.getLogger(__name__)


# ── Configurable constants ───────────────────────────────────────────────────

ALLOWED_PAIRS: frozenset[str] = frozenset({
    "BTC/USDT", "ETH/USDT", "SOL/USDT",
    "BNB/USDT", "XRP/USDT", "AVAX/USDT",
    "MATIC/USDT", "LINK/USDT", "DOT/USDT",
})

ALLOWED_ACTIONS: frozenset[str] = frozenset({"BUY", "SELL", "CLOSE"})

MAX_NOTIONAL_USD: float = 5_000.0
MAX_PRICE_USD: float = 10_000_000.0   # sanity cap on per-unit price
MAX_AMOUNT: float = 1_000_000.0       # sanity cap on raw amount

# JSON fence pattern for extracting directives from free text
_JSON_FENCE = re.compile(r"```json\s*([\s\S]*?)```", re.I)

# 20 条「举一反三」纠错提示 — 重试时轮换，避免模型对同一措辞麻木
TRADE_JSON_REMINDERS: tuple[str, ...] = (
    "格式错误：请只输出一个 JSON 对象，包含 action, pair, amount, price 四个键；不要 Markdown、不要解释。",
    "OUTPUT RULE: Single JSON object only. Keys: action (BUY|SELL|CLOSE), pair (e.g. BTC/USDT), amount, price. No prose.",
    "你输出了闲聊内容。请删除所有非 JSON 文本，仅保留合法 JSON。",
    "Invalid format. Reply with exactly: {\"action\":\"...\",\"pair\":\"...\",\"amount\":...,\"price\":...}",
    "禁止代码块。不要 ```json。直接一行裸 JSON。",
    "System: previous reply was not valid trade JSON. Fix: emit raw JSON matching the schema, nothing else.",
    "请用英文键名：action, pair, amount, price。pair 必须用斜杠，如 ETH/USDT。",
    "若上一段是分析文字，请忽略分析，只输出最终交易指令 JSON。",
    "Retry: 必须可被 json.loads 解析；字符串用双引号；数字不要用引号。",
    "格式错误。请只输出 JSON，不要「好的」「以下是」等前缀。",
    "Hallucination guard: no storytelling. JSON dictionary with four required keys only.",
    "再次强调：不要列表套 JSON；顶层就是一个对象 {}。",
    "If you added commentary, delete it. Final message = one JSON object.",
    "错误：检测到非 JSON。请重发，且 total output length 建议 < 500 字符。",
    "pair 白名单示例：BTC/USDT、ETH/USDT、SOL/USDT；请从中选择或同格式。",
    "amount 与 price 必须是数字；action 必须大写 BUY/SELL/CLOSE。",
    "不要输出多个 JSON。合并为一个对象或只保留最后一笔指令。",
    "上一次输出无法解析。请检查尾随逗号、单引号、注释 — JSON 标准不允许。",
    "FORMAT ERROR. Respond with JSON only — zero natural language characters outside the object.",
    "最终机会：仅 JSON。键：action, pair, amount, price。否则系统丢弃。",
)

TradeJsonReaskFn = Callable[[int, str], Awaitable[str]]


class TradeDirectiveModel(BaseModel):
    """Strict schema for LLM trade JSON — rejects garbage before execution."""

    action: str
    pair: str
    amount: float = Field(ge=0, le=MAX_AMOUNT)
    price: float = Field(ge=0, le=MAX_PRICE_USD)

    @field_validator("action", "pair", mode="before")
    @classmethod
    def strip_str(cls, v: Any) -> str:
        if v is None:
            return ""
        return str(v).strip()

    @field_validator("action")
    @classmethod
    def upper_action(cls, v: str) -> str:
        return v.upper()

    @field_validator("pair")
    @classmethod
    def upper_pair(cls, v: str) -> str:
        return v.upper()

    @model_validator(mode="after")
    def whitelist(self):
        action = self.action
        if action not in ALLOWED_ACTIONS:
            raise ValueError(f"action not allowed: {action}")
        normalized = re.sub(r"[-_]", "/", self.pair)
        if normalized not in ALLOWED_PAIRS:
            raise ValueError(f"pair not allowed: {self.pair}")
        object.__setattr__(self, "pair", normalized)
        notional = self.amount * self.price
        if notional > MAX_NOTIONAL_USD:
            raise ValueError(f"notional {notional} exceeds cap")
        return self


# ── Internal helpers ─────────────────────────────────────────────────────────

def _extract_json(raw: str) -> Optional[Dict[str, Any]]:
    """Extract the first valid JSON object from arbitrary LLM output."""
    m = _JSON_FENCE.search(raw)
    if m:
        try:
            return json.loads(m.group(1).strip())
        except (json.JSONDecodeError, ValueError):
            pass

    decoder = json.JSONDecoder()
    start = 0
    while True:
        i = raw.find("{", start)
        if i < 0:
            break
        try:
            obj, _ = decoder.raw_decode(raw, i)
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            pass
        start = i + 1
    return None


# ── Public API ───────────────────────────────────────────────────────────────

class LLMHallucinationFilter:
    """Stateless class-level interceptor for LLM-generated trade directives."""

    allowed_pairs: frozenset[str] = ALLOWED_PAIRS
    max_notional_usd: float = MAX_NOTIONAL_USD

    @classmethod
    async def sanitize_trade_directive(
        cls,
        raw_llm_output: str | Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Parse and strictly validate a trade directive from LLM output.

        Args:
            raw_llm_output: free-text LLM response *or* already-parsed dict.

        Returns:
            Validated directive dict on success, None on rejection.
        """
        # ── 1. Parse ────────────────────────────────────────────────────────
        if isinstance(raw_llm_output, dict):
            parsed: Optional[Dict[str, Any]] = raw_llm_output
        elif isinstance(raw_llm_output, str):
            parsed = _extract_json(raw_llm_output)
        else:
            await trigger_alert(
                "HallucinationFilter",
                f"Unexpected input type {type(raw_llm_output).__name__}",
                severity="warning",
            )
            return None

        if not isinstance(parsed, dict):
            await trigger_alert(
                "HallucinationFilter",
                "No parseable JSON object in LLM output.",
                severity="warning",
            )
            return None

        # ── 2. Pydantic strict validation (pair/action/notional/amount/price) ─
        try:
            model = TradeDirectiveModel.model_validate(parsed)
        except Exception as e:
            await trigger_alert(
                "HallucinationFilter",
                f"Pydantic rejected directive: {e}",
                severity="warning",
            )
            return None

        notional = model.amount * model.price
        if notional > cls.max_notional_usd:
            await trigger_alert(
                "HallucinationFilter",
                f"Notional ${notional:,.2f} > limit ${cls.max_notional_usd:,.2f} "
                f"(pair={model.pair} amount={model.amount} price={model.price})",
                severity="warning",
            )
            return None

        validated: Dict[str, Any] = {
            "action": model.action,
            "pair": model.pair,
            "amount": model.amount,
            "price": model.price,
            "notional_usd": round(notional, 4),
        }
        logger.info(
            "LLMFilter: approved %s %s x%.4f @ %.4f = $%.2f",
            model.action, model.pair, model.amount, model.price, notional,
        )
        return validated


async def sanitize_trade_directive_with_retries(
    raw_llm_output: Union[str, Dict[str, Any]],
    reask: TradeJsonReaskFn,
    *,
    max_retries: int = 3,
) -> Optional[Dict[str, Any]]:
    """
    Parse + Pydantic-validate; on failure call ``reask(round_index, previous_text)``
    up to ``max_retries`` times. ``reask`` must return new model output text.
    """
    cur: Union[str, Dict[str, Any]] = raw_llm_output
    for attempt in range(max_retries + 1):
        out = await LLMHallucinationFilter.sanitize_trade_directive(cur)
        if out is not None:
            return out
        if attempt >= max_retries:
            break
        prev = json.dumps(cur, ensure_ascii=False) if isinstance(cur, dict) else str(cur)
        nxt = await reask(attempt, prev)
        cur = (nxt or "").strip()
        if not cur:
            break
    return None
