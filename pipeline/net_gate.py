"""
pipeline/net_gate.py — Shared async infrastructure for the execution layer.

Components
----------
AsyncRateLimiter   : Token-bucket HTTP throttle (no lock-while-sleeping).
TradeMemoryGate    : Pre-trade RAG check against JSONL failure ledger.
                     Blocks tokens whose current features closely match
                     a previously recorded losing trade pattern.
alpha_http_limiter : Global 5 req/s gate consumed by alpha_engine.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

BOT_DIR = Path(__file__).resolve().parent.parent

# ── AsyncRateLimiter ─────────────────────────────────────────────────────────


class AsyncRateLimiter:
    """Token bucket; does not hold the lock while sleeping."""

    def __init__(self, rate_limit: int, time_window: float = 1.0) -> None:
        self.rate_limit = rate_limit
        self.time_window = time_window
        self.tokens = float(rate_limit)
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self.last_update
                rate = self.rate_limit / self.time_window
                self.tokens = min(self.rate_limit, self.tokens + elapsed * rate)
                self.last_update = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
            await asyncio.sleep(0.05)


# Global gate: max 5 HTTP requests per second across alpha_engine external calls
alpha_http_limiter = AsyncRateLimiter(5, 1.0)


# ── TradeMemoryGate ──────────────────────────────────────────────────────────

FAILURE_LEDGER = BOT_DIR / "intelligence_data" / "failures.json"
SIMILARITY_THRESHOLD = 0.70   # cosine-like sim above which trade is blocked
MAX_LEDGER_ENTRIES = 1000
_LEDGER_RELOAD_INTERVAL = 300  # seconds between ledger hot-reloads


@dataclass
class FailureRecord:
    symbol: str
    reason: str
    score_at_loss: float = 0.0
    liquidity_usd: float = 0.0
    volume_24h_usd: float = 0.0
    price_change_24h: float = 0.0
    top10_concentration_pct: float = 0.0
    timestamp: float = field(default_factory=time.time)

    def feature_vector(self) -> List[float]:
        """Normalised [0,1] feature vector for similarity computation."""
        return [
            min(1.0, self.score_at_loss / 100.0),
            min(1.0, math.log10(max(1.0, self.liquidity_usd)) / 8.0),
            min(1.0, math.log10(max(1.0, self.volume_24h_usd)) / 8.0),
            min(1.0, max(0.0, (self.price_change_24h + 100.0) / 200.0)),
            min(1.0, self.top10_concentration_pct / 100.0),
        ]


def _cosine_sim(a: List[float], b: List[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    mag_a = math.sqrt(sum(x * x for x in a))
    mag_b = math.sqrt(sum(x * x for x in b))
    if mag_a == 0 or mag_b == 0:
        return 0.0
    return dot / (mag_a * mag_b)


class TradeMemoryGate:
    """
    Loads a JSONL / JSON failure ledger and gates new trades by similarity.

    Usage in alpha_engine:
        gate = TradeMemoryGate()
        blocked, reason = await gate.check(candidate_features_dict)
        if blocked:
            return  # skip this token
    """

    def __init__(
        self,
        ledger_path: Optional[Path] = None,
        threshold: float = SIMILARITY_THRESHOLD,
    ) -> None:
        self._ledger_path = ledger_path or FAILURE_LEDGER
        self._threshold = threshold
        self._records: List[FailureRecord] = []
        self._last_load: float = 0.0
        self._lock = asyncio.Lock()

    # ── Ledger I/O ────────────────────────────────────────────────────────────

    def _load_ledger_sync(self) -> None:
        path = self._ledger_path
        if not path.exists():
            self._records = []
            return
        try:
            with open(path, encoding="utf-8") as f:
                raw = json.load(f)
            if not isinstance(raw, list):
                raw = []
            records: List[FailureRecord] = []
            for item in raw[-MAX_LEDGER_ENTRIES:]:
                if not isinstance(item, dict):
                    continue
                try:
                    records.append(FailureRecord(
                        symbol=str(item.get("symbol", "")),
                        reason=str(item.get("reason", "")),
                        score_at_loss=float(item.get("score_at_loss", 0)),
                        liquidity_usd=float(item.get("liquidity_usd", 0)),
                        volume_24h_usd=float(item.get("volume_24h_usd", 0)),
                        price_change_24h=float(item.get("price_change_24h", 0)),
                        top10_concentration_pct=float(item.get("top10_concentration_pct", 0)),
                    ))
                except (TypeError, ValueError):
                    continue
            self._records = records
            logger.info("TradeMemoryGate: loaded %d failure records", len(self._records))
        except Exception as e:
            logger.warning("TradeMemoryGate: ledger load error: %s", e)
            self._records = []

    async def _ensure_loaded(self) -> None:
        now = time.monotonic()
        if now - self._last_load > _LEDGER_RELOAD_INTERVAL:
            async with self._lock:
                if now - self._last_load > _LEDGER_RELOAD_INTERVAL:
                    await asyncio.to_thread(self._load_ledger_sync)
                    self._last_load = now

    async def append_failure(self, record: FailureRecord) -> None:
        """Write a new failure record back to the ledger."""
        async with self._lock:
            self._records.append(record)
            try:
                path = self._ledger_path
                path.parent.mkdir(parents=True, exist_ok=True)
                existing: List[Dict[str, Any]] = []
                if path.exists():
                    try:
                        with open(path, encoding="utf-8") as f:
                            existing = json.load(f)
                        if not isinstance(existing, list):
                            existing = []
                    except Exception:
                        existing = []
                existing.append({
                    "symbol": record.symbol,
                    "reason": record.reason,
                    "score_at_loss": record.score_at_loss,
                    "liquidity_usd": record.liquidity_usd,
                    "volume_24h_usd": record.volume_24h_usd,
                    "price_change_24h": record.price_change_24h,
                    "top10_concentration_pct": record.top10_concentration_pct,
                    "timestamp": record.timestamp,
                })
                tmp = str(path) + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(existing[-MAX_LEDGER_ENTRIES:], f, ensure_ascii=False, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, str(path))
            except Exception as e:
                logger.error("TradeMemoryGate: append_failure error: %s", e)

    # ── Pre-trade gate ────────────────────────────────────────────────────────

    async def check(self, features: Dict[str, Any]) -> tuple[bool, str]:
        """
        Gate a prospective trade.

        Args:
            features: dict with keys matching FailureRecord fields.

        Returns:
            (blocked: bool, reason: str)
        """
        await self._ensure_loaded()
        if not self._records:
            return False, ""

        candidate = FailureRecord(
            symbol=str(features.get("symbol", "")),
            reason="",
            score_at_loss=float(features.get("score", 0)),
            liquidity_usd=float(features.get("liquidity_usd", 0)),
            volume_24h_usd=float(features.get("volume_24h_usd", 0)),
            price_change_24h=float(features.get("price_change_24h", 0)),
            top10_concentration_pct=float(features.get("top10_concentration_pct", 0)),
        )
        cv = candidate.feature_vector()

        best_sim = 0.0
        best_reason = ""
        for rec in self._records:
            sim = _cosine_sim(cv, rec.feature_vector())
            if sim > best_sim:
                best_sim = sim
                best_reason = rec.reason

        if best_sim >= self._threshold:
            msg = (
                f"Pre-trade RAG gate blocked {features.get('symbol','')} "
                f"(sim={best_sim:.2f} ≥ {self._threshold}): {best_reason}"
            )
            logger.warning(msg)
            return True, msg

        return False, ""


# Module-level singleton — import and reuse across alpha_engine runs
trade_memory_gate = TradeMemoryGate()
