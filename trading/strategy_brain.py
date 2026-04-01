"""
Strategy Brain — V6 Self-Evolving Trading Agent with 举一反三 (Learn-by-Analogy).

Closed-loop phases: OBSERVE → LEARN → SCAN → VALIDATE → EXECUTE → EVOLVE → CHECKPOINT

Adapted from crypto-analysis agent_brain.py and integrated with the telegram bot
pipeline (trade memory gate, LLM hallucination filter, self-monitor alerts).
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from .indicators import sma, ema, atr, bb_upper, bb_lower, slope
from .okx_executor import OKXExecutor, Position, TradeRecord

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TRADE_AUDIT_LOG = PROJECT_ROOT / "trade_audit.jsonl"
LESSONS_FILE = PROJECT_ROOT / "lessons_ledger.json"

WATCH_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
SIGNAL_INTERVAL = "4H"
TICK_INTERVAL_SEC = 60
EVOLVE_EVERY_N_TRADES = 10
MIN_TRADES_FOR_EVAL = 5
SIGNAL_DEDUP_WINDOW_SEC = 14_400


def _audit_log(event: dict):
    try:
        event["timestamp"] = datetime.now(timezone.utc).isoformat()
        TRADE_AUDIT_LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(TRADE_AUDIT_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, default=str) + "\n")
    except Exception as e:
        log.warning("Audit log write failed: %s", e)


class LessonsLedger:
    """举一反三 — Learn from one trade and generalize across all symbols."""

    MAX_LESSONS = 50

    def __init__(self):
        self.lessons: list[dict] = []
        self.market_regime: str = "unknown"
        self.regime_confidence: float = 0.0
        self.cycle: int = 0
        self._load()

    def _load(self):
        if LESSONS_FILE.exists():
            try:
                data = json.loads(LESSONS_FILE.read_text(encoding="utf-8"))
                self.lessons = data.get("lessons", [])
                self.market_regime = data.get("market_regime", "unknown")
                self.regime_confidence = data.get("regime_confidence", 0.0)
                self.cycle = data.get("cycle", 0)
            except Exception as e:
                log.warning("Failed to load lessons ledger: %s", e)

    def save(self):
        try:
            LESSONS_FILE.parent.mkdir(parents=True, exist_ok=True)
            LESSONS_FILE.write_text(json.dumps({
                "lessons": self.lessons[-self.MAX_LESSONS:],
                "market_regime": self.market_regime,
                "regime_confidence": self.regime_confidence,
                "cycle": self.cycle,
            }, indent=2, default=str), encoding="utf-8")
        except Exception as e:
            log.warning("Lessons save failed: %s", e)

    def add(self, category: str, lesson: str, symbol: str = "ALL", data: dict | None = None):
        entry = {
            "cycle": self.cycle,
            "time": datetime.now(timezone.utc).isoformat(),
            "category": category,
            "symbol": symbol,
            "lesson": lesson,
            "data": data or {},
        }
        self.lessons.append(entry)
        if len(self.lessons) > self.MAX_LESSONS:
            self.lessons = self.lessons[-self.MAX_LESSONS:]
        log.info("[举一反三] %s: %s", category, lesson)

    def get_applicable(self, symbol: str) -> list[dict]:
        return [le for le in self.lessons if le["symbol"] in (symbol, "ALL")]

    def has_recent_warning(self, category: str, lookback: int = 5) -> bool:
        recent = self.lessons[-lookback:] if self.lessons else []
        return any(le["category"] == category for le in recent)

    def detect_regime(self, close: np.ndarray, atr_arr: np.ndarray) -> str:
        if len(close) < 60:
            return "unknown"
        ma20 = sma(close, 20)
        i = len(close) - 1
        if np.isnan(ma20[i]):
            return "unknown"
        slope_20 = slope(ma20, 10, i)
        atr_val = atr_arr[i] if not np.isnan(atr_arr[i]) else 0
        atr_pct = atr_val / close[i] * 100 if close[i] > 0 else 0

        if abs(slope_20) > 0.5 and atr_pct < 4.0:
            regime = "trending"
            confidence = min(abs(slope_20) / 2.0, 1.0)
        elif atr_pct > 4.0:
            regime = "volatile"
            confidence = min(atr_pct / 8.0, 1.0)
        elif abs(slope_20) < 0.2:
            regime = "ranging"
            confidence = 1.0 - abs(slope_20) / 0.2
        else:
            regime = "mixed"
            confidence = 0.5

        self.market_regime = regime
        self.regime_confidence = round(confidence, 2)
        return regime

    def learn_from_trade(
        self, symbol: str, side: str, pnl_pct: float,
        entry_price: float, exit_price: float,
        regime: str, vol_regime: str,
    ):
        if pnl_pct > 2.0:
            self.add(
                "pattern",
                f"{side} in {regime}/{vol_regime} yielded +{pnl_pct:.1f}%",
                symbol="ALL",
                data={"regime": regime, "vol": vol_regime, "side": side, "pnl": pnl_pct},
            )
        elif pnl_pct < -1.5:
            self.add(
                "risk",
                f"{side} in {regime}/{vol_regime} lost {pnl_pct:.1f}%",
                symbol="ALL",
                data={"regime": regime, "vol": vol_regime, "side": side, "pnl": pnl_pct},
            )
        losses_in_regime = [
            le for le in self.lessons[-20:]
            if le["category"] == "risk"
            and le.get("data", {}).get("regime") == regime
            and le.get("data", {}).get("side") == side
        ]
        if len(losses_in_regime) >= 3:
            self.add(
                "regime",
                f"3+ losses in {regime} for {side} — pausing",
                symbol="ALL",
                data={"regime": regime, "side": side, "loss_count": len(losses_in_regime)},
            )

    def should_skip_regime(self, side: str) -> bool:
        recent_warnings = [
            le for le in self.lessons[-10:]
            if le["category"] == "regime"
            and le.get("data", {}).get("side") == side
            and le.get("data", {}).get("regime") == self.market_regime
        ]
        return len(recent_warnings) > 0

    def get_summary(self) -> dict:
        cats: dict[str, int] = {}
        for le in self.lessons:
            c = le["category"]
            cats[c] = cats.get(c, 0) + 1
        return {
            "cycle": self.cycle,
            "total_lessons": len(self.lessons),
            "by_category": cats,
            "market_regime": self.market_regime,
            "regime_confidence": self.regime_confidence,
            "recent": self.lessons[-5:] if self.lessons else [],
        }


class PreTradeChecklist:
    """Institutional-grade pre-trade validation."""

    @staticmethod
    def validate(brain: "StrategyBrain", symbol: str, signal: dict) -> tuple[bool, list[str]]:
        failures: list[str] = []
        required = ["action", "confidence", "reason", "sl", "tp", "price"]
        for fld in required:
            if fld not in signal or signal[fld] is None:
                failures.append(f"Missing field: {fld}")
        if signal.get("confidence", 0) < 0.6:
            failures.append(f"Low confidence: {signal.get('confidence', 0):.2f} < 0.60")
        if not brain.executor.state.is_alive:
            failures.append(f"Agent shutdown: {brain.executor.state.shutdown_reason}")
        can, reason = brain.executor.can_trade(symbol)
        if not can:
            failures.append(f"Risk check failed: {reason}")
        price = signal.get("price", 0)
        sl = signal.get("sl", 0)
        action = signal.get("action", "")
        if action in ("long", "short") and (not sl or sl <= 0):
            failures.append("Missing or zero stop loss")
        elif price > 0 and sl > 0:
            sl_dist = abs(price - sl) / price * 100
            if sl_dist > 10:
                failures.append(f"SL too far: {sl_dist:.1f}%")
            if sl_dist < 0.1:
                failures.append(f"SL too tight: {sl_dist:.3f}%")
            if action == "long" and sl >= price:
                failures.append(f"SL above price for long")
            if action == "short" and sl <= price:
                failures.append(f"SL below price for short")
        last_sig = brain._last_signals.get(symbol)
        if last_sig and last_sig.get("action") == signal.get("action"):
            last_time = last_sig.get("_ts", 0)
            if time.time() - last_time < SIGNAL_DEDUP_WINDOW_SEC:
                failures.append("Duplicate signal suppressed")
        if symbol in brain.executor.state.positions:
            failures.append(f"Already in position for {symbol}")
        recent = brain.executor.state.trade_history[-5:]
        consec_losses = 0
        for t in reversed(recent):
            if t.pnl_usd < 0:
                consec_losses += 1
            else:
                break
        if consec_losses >= 3:
            failures.append(f"Consecutive loss protection: {consec_losses} losses")
        return len(failures) == 0, failures


class StrategyBrain:
    """V6 self-evolving trading agent integrated with the telegram bot."""

    def __init__(self, executor: OKXExecutor | None = None):
        self.executor = executor or OKXExecutor()
        self.executor.load_state()
        self._running = False
        self._task: asyncio.Task | None = None
        self._last_signals: dict[str, dict] = {}
        self._signal_history: list[dict] = []
        self.lessons = LessonsLedger()
        self._cycle_phase: str = "idle"
        self._last_evolved_at: int = 0
        self._send_callback: Any = None

    async def _notify(self, text: str):
        if self._send_callback:
            try:
                await self._send_callback(text[:4096])
            except Exception:
                pass

    # ── V6 signal generation ──────────────────────────────────────────────

    async def generate_signal(self, symbol: str) -> dict | None:
        """V6 four-layer filtered trend following with dynamic thresholds."""
        try:
            candles = await self.executor.get_ohlcv(symbol, SIGNAL_INTERVAL, limit=300)
            if len(candles) < 60:
                return None
        except Exception as e:
            log.warning("Data error for %s: %s", symbol, e)
            return None

        p = self.executor.state.strategy_params
        close = np.array([c[4] for c in candles])
        high = np.array([c[2] for c in candles])
        low = np.array([c[3] for c in candles])
        vol = np.array([c[5] for c in candles])

        atr_temp = atr(high, low, close, 14)
        self.lessons.detect_regime(close, atr_temp)

        ma5 = sma(close, p["ma5_len"])
        ma8 = sma(close, p["ma8_len"])
        ema21 = ema(close, p["ema21_len"])
        ma55 = sma(close, p["ma55_len"])
        bb_up = bb_upper(close, p["bb_length"], p["bb_std_dev"])
        bb_lo = bb_lower(close, p["bb_length"], p["bb_std_dev"])
        atr_arr = atr(high, low, close, p["atr_period"])

        i = len(close) - 1
        indicators = [ma5, ma8, ema21, ma55, bb_up, bb_lo, atr_arr]
        if any(np.isnan(x[i]) for x in indicators):
            return None

        price = close[i]
        if np.isnan(price) or price <= 0:
            return None

        slope_len = p["slope_len"]
        slope_thresh = p["slope_threshold"]

        atr_pct = atr_arr[i] / price * 100
        atr_dist_scale = max(1.0, atr_pct / 2.0)
        atr_slope_scale = max(1.0, atr_pct / 1.5)

        adapted_dist_5_8 = p["dist_ma5_ma8"] * atr_dist_scale
        adapted_dist_8_21 = p["dist_ma8_ema21"] * atr_dist_scale
        adapted_dist_21_55 = p["dist_ema21_ma55"] * atr_dist_scale
        adapted_slope_thresh = slope_thresh * atr_slope_scale

        if atr_pct < 1.5:
            volatility_regime = "low"
        elif atr_pct < 4.0:
            volatility_regime = "normal"
        else:
            volatility_regime = "high"

        # Check existing position exits
        if symbol in self.executor.state.positions:
            pos = self.executor.state.positions[symbol]
            if pos.side == "long":
                if price < ma55[i]:
                    return {"action": "close", "confidence": 1.0, "reason": "Long SL: price < MA55", "volatility_regime": volatility_regime}
                if price >= bb_up[i]:
                    return {"action": "close", "confidence": 0.9, "reason": "Long TP: BB_upper", "volatility_regime": volatility_regime}
            elif pos.side == "short":
                if price > ma55[i]:
                    return {"action": "close", "confidence": 1.0, "reason": "Short SL: price > MA55", "volatility_regime": volatility_regime}
                if price <= bb_lo[i]:
                    return {"action": "close", "confidence": 0.9, "reason": "Short TP: BB_lower", "volatility_regime": volatility_regime}
            return None

        # Layer 1: Trend ordering
        long_order = price > ma5[i] > ma8[i] > ema21[i] > ma55[i]
        short_order = price < ma5[i] < ma8[i] < ema21[i] < ma55[i]
        if not long_order and not short_order:
            return None

        # Layer 2: Fanning distance
        def pct_dist(a: float, b: float) -> float:
            return abs(a - b) / max(abs(b), 1e-10) * 100

        dist_5_8 = pct_dist(ma5[i], ma8[i])
        dist_8_21 = pct_dist(ma8[i], ema21[i])
        dist_21_55 = pct_dist(ema21[i], ma55[i])
        if not (dist_5_8 < adapted_dist_5_8 and dist_8_21 < adapted_dist_8_21 and dist_21_55 < adapted_dist_21_55):
            return None

        # Layer 3: Slope momentum
        s_ma5 = slope(ma5, slope_len, i)
        s_ma8 = slope(ma8, slope_len, i)
        s_ema21 = slope(ema21, slope_len, i)
        s_ma55 = slope(ma55, slope_len, i)
        slopes = [s_ma5, s_ma8, s_ema21, s_ma55]
        if long_order:
            if not all(s > adapted_slope_thresh for s in slopes):
                return None
        else:
            if not all(s < -adapted_slope_thresh for s in slopes):
                return None

        # Layer 4: BB position filter
        if long_order and price >= bb_up[i]:
            return None
        if short_order and price <= bb_lo[i]:
            return None

        # Confidence from ordering consistency
        stack_count = 0
        for j in range(max(0, i - 5), i + 1):
            if any(np.isnan(x[j]) for x in [ma5, ma8, ema21, ma55]):
                continue
            if long_order and close[j] > ma5[j] > ma8[j] > ema21[j] > ma55[j]:
                stack_count += 1
            elif short_order and close[j] < ma5[j] < ma8[j] < ema21[j] < ma55[j]:
                stack_count += 1
        confidence = min(stack_count / 5.0, 1.0)
        if volatility_regime in ("normal", "high"):
            confidence = min(confidence + 0.1, 1.0)

        # Layer 5: Volume confirmation
        if len(vol) > 20:
            vol_slice = vol[max(0, i - 20) : i] if i >= 20 else vol[: i + 1]
            vol_ma = np.nanmean(vol_slice) if len(vol_slice) > 0 else 0
            if vol_ma > 0 and not np.isnan(vol[i]):
                vol_ratio = vol[i] / vol_ma
                if vol_ratio < 0.5:
                    return None
                if vol_ratio > 1.5:
                    confidence = min(confidence + 0.1, 1.0)

        if confidence < 0.4:
            return None

        action = "long" if long_order else "short"
        return {
            "action": action,
            "confidence": round(confidence, 2),
            "reason": (
                f"V6 {action.title()}: dist={dist_5_8:.1f}/{dist_8_21:.1f}/{dist_21_55:.1f}%, "
                f"slopes={s_ma5:.2f}/{s_ma8:.2f}/{s_ema21:.2f}/{s_ma55:.2f}%, "
                f"vol={volatility_regime} atr={atr_pct:.2f}%"
            ),
            "sl": round(float(ma55[i]), 6),
            "tp": round(float(bb_up[i] if long_order else bb_lo[i]), 6),
            "price": round(float(price), 6),
            "volatility_regime": volatility_regime,
        }

    # ── Position management ───────────────────────────────────────────────

    async def manage_positions(self):
        for symbol in list(self.executor.state.positions.keys()):
            pos = self.executor.state.positions[symbol]
            signal = await self.generate_signal(symbol)
            if signal and signal["action"] == "close":
                result = await self.executor.close_position(symbol, signal["reason"])
                if result["ok"]:
                    pnl_pct = result.get("pnl_pct", 0)
                    pnl_usd = result.get("pnl_usd", 0)
                    log.info("Closed %s: %s PnL=%.2f%%", symbol, signal["reason"], pnl_pct)
                    self.lessons.learn_from_trade(
                        symbol=symbol, side=pos.side, pnl_pct=pnl_pct,
                        entry_price=pos.entry_price,
                        exit_price=result.get("exit_price", 0),
                        regime=self.lessons.market_regime,
                        vol_regime=signal.get("volatility_regime", "unknown"),
                    )

                    # Self-reflection: post-trade analysis
                    try:
                        from .reflection import reflection_engine
                        trade_rec = TradeRecord(
                            symbol=symbol, side=pos.side,
                            entry_price=pos.entry_price,
                            exit_price=result.get("exit_price", 0),
                            size=pos.size, pnl_pct=pnl_pct, pnl_usd=pnl_usd,
                            entry_time=pos.entry_time, exit_time=time.time(),
                            reason=signal["reason"],
                        )
                        reflection = await reflection_engine.analyze_trade(
                            trade_rec,
                            market_regime=self.lessons.market_regime,
                            volatility_regime=signal.get("volatility_regime", "unknown"),
                        )
                        await reflection_engine.feed_to_memory_gate(reflection)

                        # Apply suggested param adjustments if enough evidence
                        adjustments = reflection_engine.suggest_param_adjustments()
                        if adjustments:
                            for param, delta in adjustments.items():
                                if param in self.executor.state.strategy_params:
                                    old_val = self.executor.state.strategy_params[param]
                                    self.executor.state.strategy_params[param] = old_val + delta
                                    log.info("Reflection adjustment: %s %.3f → %.3f",
                                             param, old_val, old_val + delta)
                    except Exception as e:
                        log.debug("Reflection failed: %s", e)

                    emoji = "🟢" if pnl_usd >= 0 else "🔴"
                    await self._notify(
                        f"{emoji} Closed {symbol} {pos.side.upper()}\n"
                        f"PnL: {pnl_pct:+.2f}% (${pnl_usd:+.2f})\n"
                        f"Reason: {signal['reason']}"
                    )
                    _audit_log({
                        "event": "close_position", "symbol": symbol,
                        "reason": signal["reason"], "pnl_pct": pnl_pct,
                        "pnl_usd": pnl_usd,
                        "mode": self.executor.state.mode,
                        "equity_after": self.executor.state.equity,
                    })
                    self.executor.save_state()
                    self.lessons.save()

    # ── Evolution ─────────────────────────────────────────────────────────

    def _evaluate_recent(self) -> dict:
        trades = self.executor.state.trade_history[-EVOLVE_EVERY_N_TRADES:]
        if len(trades) < MIN_TRADES_FOR_EVAL:
            return {"ready": False}
        pnls = [t.pnl_pct for t in trades]
        win_rate = sum(1 for p in pnls if p > 0) / len(pnls) * 100
        avg_pnl = float(np.mean(pnls))
        std_pnl = float(np.std(pnls)) if len(pnls) > 1 else 0.0
        sharpe_val = avg_pnl / max(std_pnl, 1e-8)
        return {"ready": True, "win_rate": win_rate, "avg_pnl": avg_pnl, "sharpe": sharpe_val, "num_trades": len(trades)}

    def evolve(self):
        perf = self._evaluate_recent()
        if not perf["ready"]:
            return
        params = self.executor.state.strategy_params
        mutation_rate = 0.05 if perf["sharpe"] > 0 else 0.10
        int_params = ["ma5_len", "ma8_len", "ema21_len", "ma55_len", "bb_length", "atr_period", "slope_len"]
        float_params = ["bb_std_dev", "dist_ma5_ma8", "dist_ma8_ema21", "dist_ema21_ma55", "slope_threshold"]
        all_params = int_params + float_params
        to_mutate = random.sample(all_params, min(random.randint(1, 2), len(all_params)))
        bounds = {
            "ma5_len": (3, 8), "ma8_len": (6, 12), "ema21_len": (15, 30),
            "ma55_len": (40, 80), "bb_length": (15, 30), "bb_std_dev": (1.5, 4.0),
            "dist_ma5_ma8": (0.5, 3.0), "dist_ma8_ema21": (1.0, 5.0),
            "dist_ema21_ma55": (2.0, 8.0), "slope_len": (2, 5),
            "slope_threshold": (0.02, 0.5), "atr_period": (7, 21),
        }
        for key in to_mutate:
            val = params[key]
            delta = val * mutation_rate * random.choice([-1, 1])
            new_val = max(bounds[key][0], min(bounds[key][1], val + delta))
            params[key] = int(round(new_val)) if key in int_params else new_val
        if params["ma5_len"] >= params["ma8_len"]:
            params["ma8_len"] = params["ma5_len"] + 2
        if params["ma8_len"] >= params["ema21_len"]:
            params["ema21_len"] = params["ma8_len"] + 5
        if params["ema21_len"] >= params["ma55_len"]:
            params["ma55_len"] = params["ema21_len"] + 15
        self.executor.state.generation += 1
        log.info("Evolution gen=%d: mutated %s", self.executor.state.generation, to_mutate)
        self.executor.save_state()

    # ── Main loop ─────────────────────────────────────────────────────────

    async def tick(self):
        if not self.executor.state.is_alive:
            return
        self.lessons.cycle += 1
        self._cycle_phase = "observe"
        self.executor.check_daily_reset()
        await self.executor.update_positions()

        self._cycle_phase = "manage"
        await self.manage_positions()

        self._cycle_phase = "learn"
        self._learn_from_recent()

        recent = self.executor.state.trade_history[-3:]
        if len(recent) >= 3 and all(t.pnl_usd < 0 for t in recent):
            if not self.lessons.has_recent_warning("risk", lookback=3):
                self.lessons.add("risk", "3 consecutive losses — auto-paused", symbol="ALL")
        else:
            self._cycle_phase = "scan"
            for symbol in WATCH_SYMBOLS:
                if symbol in self.executor.state.positions:
                    continue
                signal = await self.generate_signal(symbol)
                if signal is None or signal.get("action") not in ("long", "short"):
                    if signal:
                        self._last_signals[symbol] = signal
                    continue
                signal["_ts"] = time.time()
                signal["market_regime"] = self.lessons.market_regime

                if self.lessons.should_skip_regime(signal["action"]):
                    self._last_signals[symbol] = {**signal, "blocked": True}
                    continue

                # Pre-trade RAG gate (from pipeline.net_gate)
                try:
                    from pipeline import trade_memory_gate
                    blocked, reason = await trade_memory_gate.check({
                        "symbol": symbol,
                        "side": signal["action"],
                        "volatility": signal.get("volatility_regime", ""),
                        "confidence": signal.get("confidence", 0),
                    })
                    if blocked:
                        log.info("RAG gate blocked %s: %s", symbol, reason)
                        self._last_signals[symbol] = {**signal, "blocked": True, "block_reasons": [reason]}
                        continue
                except Exception:
                    pass

                self._cycle_phase = "validate"
                passed, failures = PreTradeChecklist.validate(self, symbol, signal)
                if not passed:
                    _audit_log({"event": "signal_blocked", "symbol": symbol, "failures": failures})
                    self._last_signals[symbol] = {**signal, "blocked": True, "block_reasons": failures}
                    continue

                self._cycle_phase = "execute"
                size = signal["confidence"] * self.executor.risk.max_position_pct * self.executor.state.equity
                result = await self.executor.open_position(symbol, signal["action"], size)
                if result["ok"]:
                    log.info(
                        "Opened %s %s @ %.2f size=$%.0f conf=%.2f",
                        signal["action"].upper(), symbol, result["price"], size, signal["confidence"],
                    )
                    await self._notify(
                        f"📊 Opened {signal['action'].upper()} {symbol}\n"
                        f"Price: {result['price']}\nSize: ${size:.0f}\n"
                        f"Conf: {signal['confidence']}\n"
                        f"SL: {signal.get('sl')} | TP: {signal.get('tp')}"
                    )
                    _audit_log({
                        "event": "open_position", "symbol": symbol,
                        "side": signal["action"], "price": result["price"],
                        "size_usd": size, "confidence": signal["confidence"],
                        "mode": self.executor.state.mode,
                    })
                    self._last_signals[symbol] = signal
                    self._signal_history.append({
                        "symbol": symbol, "signal": signal,
                        "time": time.time(), "executed": True,
                    })
                    self.executor.save_state()

        self._cycle_phase = "evolve"
        tt = self.executor.state.total_trades
        if tt > 0 and tt % EVOLVE_EVERY_N_TRADES == 0 and tt != self._last_evolved_at:
            self.evolve()
            self._last_evolved_at = tt

        self._cycle_phase = "checkpoint"
        self.lessons.save()
        self._cycle_phase = "idle"

    def _learn_from_recent(self):
        trades = self.executor.state.trade_history
        if len(trades) < 5:
            return
        recent = trades[-10:]
        streak, streak_dir = 0, None
        for t in reversed(recent):
            if streak_dir is None:
                streak_dir = "win" if t.pnl_pct > 0 else "loss"
                streak = 1
            elif (t.pnl_pct > 0) == (streak_dir == "win"):
                streak += 1
            else:
                break
        if streak >= 4 and streak_dir == "win":
            self.lessons.add("pattern", f"Win streak {streak}", symbol="ALL")
        elif streak >= 3 and streak_dir == "loss":
            self.lessons.add("risk", f"Loss streak {streak}", symbol="ALL")
        avg_pnl = np.mean([t.pnl_pct for t in recent])
        if avg_pnl < -0.5 and len(recent) >= 8:
            self.lessons.add("param", f"Avg PnL drifting negative ({avg_pnl:.2f}%)", symbol="ALL")

    async def run_loop(self):
        self._running = True
        log.info(
            "Strategy brain started. Mode=%s Equity=$%.2f Gen=%d",
            self.executor.state.mode, self.executor.state.equity,
            self.executor.state.generation,
        )
        while self._running:
            try:
                await self.tick()
            except Exception as e:
                log.error("Tick error: %s", e, exc_info=True)
            await asyncio.sleep(TICK_INTERVAL_SEC)

    def start(self):
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self.run_loop())

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None
        self.executor.save_state()
        log.info("Strategy brain stopped.")

    def get_status(self) -> dict:
        return {
            **self.executor.state.to_dict(),
            "running": self._running,
            "cycle_phase": self._cycle_phase,
            "watch_symbols": WATCH_SYMBOLS,
            "signal_interval": SIGNAL_INTERVAL,
            "tick_interval_sec": TICK_INTERVAL_SEC,
            "last_signals": {
                k: {
                    "action": v.get("action"),
                    "confidence": v.get("confidence"),
                    "blocked": v.get("blocked", False),
                }
                for k, v in self._last_signals.items()
            },
            "risk_limits": {
                "max_position_pct": self.executor.risk.max_position_pct,
                "max_total_exposure_pct": self.executor.risk.max_total_exposure_pct,
                "max_daily_loss_pct": self.executor.risk.max_daily_loss_pct,
                "max_drawdown_pct": self.executor.risk.max_drawdown_pct,
                "max_positions": self.executor.risk.max_positions,
                "cooldown_seconds": self.executor.risk.cooldown_seconds,
            },
            "harness": self.lessons.get_summary(),
        }
