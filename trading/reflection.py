"""
Self-Reflection Loop — Post-trade analysis and lesson-driven mutation.

Based on TradingGroup 2026 paper patterns:
1. Every trade (win or lose) feeds back into the system
2. Post-trade analysis: Why did this trade win/lose? What market conditions?
3. Lesson extraction: Store structured lessons in TradeMemoryGate
4. Pre-trade RAG check: Block trades resembling past losing scenarios
5. Strategy parameter adjustment: driven by structured reflection, not random mutation

Integrates with:
- pipeline/net_gate.py (TradeMemoryGate) for failure pattern storage
- trading/strategy_brain.py (LessonsLedger) for cross-symbol learning
- trading/strategy_arena.py for bias-correction of arena genomes
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from .indicators import sma, ema, atr
from .okx_executor import TradeRecord

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
REFLECTION_LOG = PROJECT_ROOT / "intelligence_data" / "reflection_log.jsonl"
REFLECTION_LOG.parent.mkdir(parents=True, exist_ok=True)


@dataclass
class TradeReflection:
    """Structured post-trade analysis."""
    symbol: str
    side: str
    pnl_pct: float
    pnl_usd: float
    entry_price: float
    exit_price: float
    reason: str
    duration_hours: float

    # Market conditions at trade time
    market_regime: str = "unknown"
    volatility_regime: str = "unknown"
    atr_pct: float = 0.0
    trend_strength: float = 0.0
    volume_ratio: float = 0.0

    # Analysis
    was_correct_direction: bool = False
    was_stopped_out: bool = False
    was_take_profit: bool = False
    max_favorable_excursion: float = 0.0
    max_adverse_excursion: float = 0.0

    # Extracted lesson
    lesson_category: str = ""
    lesson_text: str = ""
    param_adjustments: dict = field(default_factory=dict)

    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "pnl_pct": round(self.pnl_pct, 4),
            "pnl_usd": round(self.pnl_usd, 4),
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "reason": self.reason,
            "duration_hours": round(self.duration_hours, 2),
            "market_regime": self.market_regime,
            "volatility_regime": self.volatility_regime,
            "atr_pct": round(self.atr_pct, 4),
            "trend_strength": round(self.trend_strength, 4),
            "was_correct_direction": self.was_correct_direction,
            "was_stopped_out": self.was_stopped_out,
            "was_take_profit": self.was_take_profit,
            "lesson_category": self.lesson_category,
            "lesson_text": self.lesson_text,
            "param_adjustments": self.param_adjustments,
            "timestamp": self.timestamp,
        }


class ReflectionEngine:
    """Post-trade analysis and lesson-driven strategy improvement."""

    def __init__(self):
        self._reflections: list[TradeReflection] = []
        self._param_adjustment_buffer: list[dict] = []

    async def analyze_trade(
        self,
        trade: TradeRecord,
        market_data: np.ndarray | None = None,
        market_regime: str = "unknown",
        volatility_regime: str = "unknown",
    ) -> TradeReflection:
        """Perform post-trade analysis on a completed trade."""
        duration_hours = (trade.exit_time - trade.entry_time) / 3600

        reflection = TradeReflection(
            symbol=trade.symbol,
            side=trade.side,
            pnl_pct=trade.pnl_pct,
            pnl_usd=trade.pnl_usd,
            entry_price=trade.entry_price,
            exit_price=trade.exit_price,
            reason=trade.reason,
            duration_hours=duration_hours,
            market_regime=market_regime,
            volatility_regime=volatility_regime,
        )

        # Classify trade outcome
        reflection.was_stopped_out = "SL" in trade.reason or "stop" in trade.reason.lower()
        reflection.was_take_profit = "TP" in trade.reason or "profit" in trade.reason.lower()
        if trade.side == "long":
            reflection.was_correct_direction = trade.exit_price > trade.entry_price
        else:
            reflection.was_correct_direction = trade.exit_price < trade.entry_price

        # Compute market conditions if data available
        if market_data is not None and len(market_data) >= 60:
            close = market_data[:, 4] if market_data.ndim == 2 else market_data
            high = market_data[:, 2] if market_data.ndim == 2 else close
            low = market_data[:, 3] if market_data.ndim == 2 else close

            atr_arr = atr(high, low, close, 14)
            i = len(close) - 1
            if not np.isnan(atr_arr[i]) and close[i] > 0:
                reflection.atr_pct = float(atr_arr[i] / close[i] * 100)

            ma20 = sma(close, 20)
            if not np.isnan(ma20[i]) and not np.isnan(ma20[max(0, i - 10)]):
                reflection.trend_strength = float(
                    (ma20[i] - ma20[max(0, i - 10)]) / ma20[max(0, i - 10)] * 100
                )

        # Extract lesson
        self._extract_lesson(reflection)

        self._reflections.append(reflection)
        self._log_reflection(reflection)

        return reflection

    def _extract_lesson(self, reflection: TradeReflection):
        """Extract a structured lesson from the trade analysis."""
        pnl = reflection.pnl_pct

        if pnl > 2.0:
            reflection.lesson_category = "success_pattern"
            reflection.lesson_text = (
                f"{reflection.side} in {reflection.market_regime}/{reflection.volatility_regime} "
                f"yielded +{pnl:.1f}% over {reflection.duration_hours:.1f}h — favorable setup"
            )
        elif pnl < -1.5:
            reflection.lesson_category = "failure_pattern"
            if reflection.was_stopped_out:
                reflection.lesson_text = (
                    f"SL hit on {reflection.side} in {reflection.market_regime}: "
                    f"lost {pnl:.1f}% — SL may be too tight or trend reversed"
                )
                if reflection.atr_pct > 3.0:
                    reflection.param_adjustments = {"bb_std_dev": "+0.2", "dist_ema21_ma55": "+0.5"}
                    reflection.lesson_text += ". High volatility suggests wider BB/distance thresholds."
            elif reflection.duration_hours < 2:
                reflection.lesson_text = (
                    f"Quick loss ({reflection.duration_hours:.1f}h) on {reflection.side}: "
                    f"possibly entered at wrong time in {reflection.volatility_regime} volatility"
                )
                reflection.param_adjustments = {"slope_threshold": "+0.02"}
            else:
                reflection.lesson_text = (
                    f"{reflection.side} in {reflection.market_regime} slowly bled {pnl:.1f}% "
                    f"over {reflection.duration_hours:.1f}h — regime mismatch"
                )
        elif abs(pnl) < 0.5:
            reflection.lesson_category = "breakeven"
            reflection.lesson_text = (
                f"Breakeven trade ({pnl:+.2f}%) on {reflection.side}: "
                f"no clear edge in {reflection.market_regime} market"
            )
        else:
            reflection.lesson_category = "marginal"
            reflection.lesson_text = (
                f"Marginal {'win' if pnl > 0 else 'loss'} ({pnl:+.2f}%) on {reflection.side}"
            )

    def _log_reflection(self, reflection: TradeReflection):
        """Append to reflection log (JSONL format)."""
        try:
            with open(REFLECTION_LOG, "a", encoding="utf-8") as f:
                f.write(json.dumps(reflection.to_dict(), default=str) + "\n")
        except Exception as e:
            log.warning("Failed to write reflection log: %s", e)

    async def feed_to_memory_gate(self, reflection: TradeReflection):
        """Push failure patterns into TradeMemoryGate for pre-trade blocking."""
        if reflection.lesson_category != "failure_pattern":
            return

        try:
            from pipeline.net_gate import trade_memory_gate, FailureRecord
            record = FailureRecord(
                symbol=reflection.symbol,
                reason=reflection.lesson_text[:200],
                score_at_loss=abs(reflection.pnl_pct),
                price_change_24h=reflection.trend_strength,
            )
            await trade_memory_gate.append_failure(record)
            log.info("Failure pattern stored for %s: %s", reflection.symbol, reflection.lesson_text[:80])
        except Exception as e:
            log.warning("Failed to feed memory gate: %s", e)

    def suggest_param_adjustments(self) -> dict[str, float]:
        """Aggregate parameter adjustment suggestions from recent reflections.

        Returns a dict of param_name -> suggested_delta based on recent
        failure patterns. Only suggests changes with 3+ supporting reflections.
        """
        recent = self._reflections[-20:]
        adjustments: dict[str, list[float]] = {}

        for r in recent:
            if r.lesson_category != "failure_pattern":
                continue
            for param, delta_str in r.param_adjustments.items():
                try:
                    delta = float(delta_str.replace("+", ""))
                except ValueError:
                    continue
                adjustments.setdefault(param, []).append(delta)

        suggestions: dict[str, float] = {}
        for param, deltas in adjustments.items():
            if len(deltas) >= 3:
                suggestions[param] = float(np.mean(deltas))

        return suggestions

    def get_stats(self) -> dict:
        """Reflection statistics for display."""
        if not self._reflections:
            return {"total_reflections": 0}

        recent = self._reflections[-50:]
        wins = [r for r in recent if r.pnl_pct > 0]
        losses = [r for r in recent if r.pnl_pct < 0]

        # Regime analysis
        regime_perf: dict[str, list[float]] = {}
        for r in recent:
            regime_perf.setdefault(r.market_regime, []).append(r.pnl_pct)

        return {
            "total_reflections": len(self._reflections),
            "recent_50": {
                "wins": len(wins),
                "losses": len(losses),
                "avg_win_pct": round(float(np.mean([w.pnl_pct for w in wins])), 2) if wins else 0,
                "avg_loss_pct": round(float(np.mean([l.pnl_pct for l in losses])), 2) if losses else 0,
                "avg_duration_h": round(float(np.mean([r.duration_hours for r in recent])), 1),
                "stopped_out_pct": round(
                    sum(1 for r in losses if r.was_stopped_out) / max(len(losses), 1) * 100, 1
                ),
            },
            "regime_performance": {
                regime: {
                    "count": len(pnls),
                    "avg_pnl": round(float(np.mean(pnls)), 2),
                    "win_rate": round(sum(1 for p in pnls if p > 0) / max(len(pnls), 1) * 100, 1),
                }
                for regime, pnls in regime_perf.items()
            },
            "pending_adjustments": self.suggest_param_adjustments(),
        }

    def format_report(self) -> str:
        """Format reflection stats for Telegram."""
        stats = self.get_stats()
        if stats["total_reflections"] == 0:
            return "No trade reflections yet."

        recent = stats.get("recent_50", {})
        lines = [
            "━━ Trade Reflection Report ━━",
            f"Total Reflections: {stats['total_reflections']}",
            f"Recent 50: {recent.get('wins', 0)}W / {recent.get('losses', 0)}L",
            f"Avg Win: {recent.get('avg_win_pct', 0):+.2f}% | Avg Loss: {recent.get('avg_loss_pct', 0):+.2f}%",
            f"Avg Duration: {recent.get('avg_duration_h', 0):.1f}h",
            f"Stopped Out: {recent.get('stopped_out_pct', 0):.1f}%",
        ]

        regime_perf = stats.get("regime_performance", {})
        if regime_perf:
            lines.append("\n📊 Performance by Regime:")
            for regime, perf in regime_perf.items():
                lines.append(
                    f"  {regime}: {perf['count']} trades, "
                    f"avg PnL {perf['avg_pnl']:+.2f}%, WR {perf['win_rate']:.0f}%"
                )

        adjustments = stats.get("pending_adjustments", {})
        if adjustments:
            lines.append("\n🔧 Suggested Adjustments:")
            for param, delta in adjustments.items():
                lines.append(f"  {param}: {delta:+.3f}")

        return "\n".join(lines)


# Module singleton
reflection_engine = ReflectionEngine()
