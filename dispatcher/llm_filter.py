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
from typing import Any, Dict, Optional

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
_JSON_BARE = re.compile(r"\{[\s\S]*\}", re.S)


# ── Internal helpers ─────────────────────────────────────────────────────────

def _extract_json(raw: str) -> Optional[Dict[str, Any]]:
    """Try to extract the first valid JSON object from arbitrary LLM output."""
    for pattern in (_JSON_FENCE, _JSON_BARE):
        m = pattern.search(raw)
        if m:
            try:
                candidate = m.group(1) if pattern is _JSON_FENCE else m.group(0)
                return json.loads(candidate)
            except (json.JSONDecodeError, ValueError):
                continue
    return None


def _coerce_float(val: Any, field: str) -> Optional[float]:
    try:
        v = float(val)
        if v < 0:
            return None
        return v
    except (TypeError, ValueError):
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

        # ── 2. Required fields ──────────────────────────────────────────────
        action = str(parsed.get("action", "")).strip().upper()
        pair = str(parsed.get("pair", "")).strip().upper()
        raw_amount = parsed.get("amount")
        raw_price = parsed.get("price")

        missing = [f for f, v in [("action", action), ("pair", pair),
                                   ("amount", raw_amount), ("price", raw_price)]
                   if not v and v != 0]
        if missing:
            await trigger_alert(
                "HallucinationFilter",
                f"Missing required fields: {missing}",
                severity="warning",
            )
            return None

        # ── 3. Action whitelist ─────────────────────────────────────────────
        if action not in ALLOWED_ACTIONS:
            await trigger_alert(
                "HallucinationFilter",
                f"Blocked action '{action}' (allowed: {sorted(ALLOWED_ACTIONS)})",
                severity="warning",
            )
            return None

        # ── 4. Pair whitelist ───────────────────────────────────────────────
        # Normalize separators: ETHUSDT → ETH/USDT, eth-usdt → ETH/USDT
        normalized_pair = re.sub(r"[-_]", "/", pair)
        if normalized_pair not in cls.allowed_pairs:
            await trigger_alert(
                "HallucinationFilter",
                f"Blocked non-whitelisted pair '{pair}'",
                severity="warning",
            )
            return None

        # ── 5. Numeric sanity ───────────────────────────────────────────────
        amount = _coerce_float(raw_amount, "amount")
        price = _coerce_float(raw_price, "price")

        if amount is None or amount > MAX_AMOUNT:
            await trigger_alert(
                "HallucinationFilter",
                f"Invalid or extreme amount={raw_amount}",
                severity="warning",
            )
            return None

        if price is None or price > MAX_PRICE_USD:
            await trigger_alert(
                "HallucinationFilter",
                f"Invalid or extreme price={raw_price}",
                severity="warning",
            )
            return None

        # ── 6. Notional cap ─────────────────────────────────────────────────
        notional = amount * price
        if notional > cls.max_notional_usd:
            await trigger_alert(
                "HallucinationFilter",
                f"Notional ${notional:,.2f} > limit ${cls.max_notional_usd:,.2f} "
                f"(pair={normalized_pair} amount={amount} price={price})",
                severity="warning",
            )
            return None

        validated: Dict[str, Any] = {
            "action": action,
            "pair": normalized_pair,
            "amount": amount,
            "price": price,
            "notional_usd": round(notional, 4),
        }
        logger.info(
            "LLMFilter: approved %s %s x%.4f @ %.4f = $%.2f",
            action, normalized_pair, amount, price, notional,
        )
        return validated
