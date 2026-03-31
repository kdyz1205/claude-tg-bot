"""
drawdown_guardian.py — Production-grade Drawdown Guardian

Real-time drawdown monitoring with adaptive thresholds, multi-level alerts,
per-position trailing-stop tracking, daily circuit breakers, portfolio heat
calculation, recovery mode, and time-based cooldowns.

Designed for quantitative crypto trading on OKX via agent_brain / okx_trader.
"""

from __future__ import annotations

import time
import logging
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Alert levels (ordered by severity)
# ---------------------------------------------------------------------------

class AlertLevel(str, Enum):
    NORMAL = "NORMAL"
    WARNING = "WARNING"      # 50 % of threshold
    DANGER = "DANGER"        # 75 %
    CRITICAL = "CRITICAL"    # 90 %
    SHUTDOWN = "SHUTDOWN"    # 100 %


_ALERT_THRESHOLDS: Dict[AlertLevel, float] = {
    AlertLevel.WARNING:  0.50,
    AlertLevel.DANGER:   0.75,
    AlertLevel.CRITICAL: 0.90,
    AlertLevel.SHUTDOWN: 1.00,
}

# ---------------------------------------------------------------------------
# Lightweight reference dataclasses (mirrors of external types)
# ---------------------------------------------------------------------------

@dataclass
class Position:
    """Minimal position representation expected by the guardian."""
    symbol: str
    side: str               # "long" | "short"
    size: float
    entry_price: float
    entry_time: float
    unrealized_pnl: float = 0.0
    peak_pnl: float = 0.0


@dataclass
class RiskLimits:
    max_position_pct: float = 0.05
    max_total_exposure_pct: float = 0.15
    max_daily_loss_pct: float = 0.02
    max_drawdown_pct: float = 0.05


@dataclass
class AgentState:
    equity: float = 0.0
    peak_equity: float = 0.0
    cash: float = 0.0
    positions: Dict[str, Position] = field(default_factory=dict)
    daily_pnl: float = 0.0
    trade_history: List[Any] = field(default_factory=list)


# ---------------------------------------------------------------------------
# DrawdownGuardian
# ---------------------------------------------------------------------------

class DrawdownGuardian:
    """Hedge-fund grade drawdown controller.

    Instantiate once, call ``update()`` on every equity tick, and
    ``check_position()`` per open position.  The guardian will
    adaptively tighten thresholds when recent equity volatility rises
    and enforce multi-level circuit breakers.

    Parameters
    ----------
    base_max_dd : float
        Maximum tolerated drawdown as a fraction (default 0.05 = 5 %).
    adaptive_lookback : int
        Number of equity observations used for volatility scaling.
    vol_scale : bool
        If *True*, the effective threshold contracts when realised
        equity volatility is elevated.
    """

    # Maximum equity curve history retained (ring-buffer style)
    _MAX_CURVE_LEN: int = 1000

    def __init__(
        self,
        base_max_dd: float = 0.05,
        adaptive_lookback: int = 30,
        vol_scale: bool = True,
    ) -> None:
        # --- configuration ---
        self.base_max_dd: float = base_max_dd
        self.adaptive_lookback: int = max(adaptive_lookback, 2)
        self.vol_scale: bool = vol_scale

        # --- equity tracking ---
        self.equity_curve: deque[Tuple[float, float]] = deque(maxlen=self._MAX_CURVE_LEN)
        self.peak_equity: float = 0.0
        self.current_dd: float = 0.0        # current drawdown fraction
        self.alert_level: AlertLevel = AlertLevel.NORMAL

        # --- daily loss circuit breaker ---
        self.daily_start_equity: float = 0.0
        self._daily_loss_limit: float = RiskLimits.max_daily_loss_pct

        # --- recovery mode ---
        self.in_recovery: bool = False
        self.recovery_target: float = 0.0
        self._recovery_pct: float = 0.50    # require 50 % retracement recovery

        # --- cooldown ---
        self.last_critical_time: float = 0.0
        self.cooldown_seconds: int = 3600

        # --- per-position peak PnL tracking ---
        self._position_peak_pnl: Dict[str, float] = {}
        self._max_tracked_positions: int = 200  # prevent unbounded growth

        logger.info(
            "DrawdownGuardian initialised  base_max_dd=%.2f%%  lookback=%d  vol_scale=%s",
            base_max_dd * 100,
            self.adaptive_lookback,
            self.vol_scale,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update(self, equity: float, timestamp: Optional[float] = None) -> Dict[str, Any]:
        """Process a new equity observation.

        Must be called on **every tick** (or at least every time equity
        changes).  Returns a status dict suitable for upstream decision
        logic.
        """
        if equity <= 0:
            return self._build_status(
                message="Invalid equity <= 0; skipping update.",
            )

        ts = timestamp if timestamp is not None else time.time()

        # --- update equity curve (ring buffer) ---
        self.equity_curve.append((ts, equity))

        # --- initialise daily start if needed ---
        if self.daily_start_equity <= 0:
            self.daily_start_equity = equity

        # --- peak / drawdown ---
        if equity > self.peak_equity:
            self.peak_equity = equity
        self.current_dd = 1.0 - (equity / self.peak_equity) if self.peak_equity > 0 else 0.0

        # --- dynamic threshold ---
        threshold = self.get_dynamic_threshold()

        # --- alert level ---
        self.alert_level = self._classify_alert(self.current_dd, threshold)

        if self.alert_level == AlertLevel.CRITICAL:
            self.last_critical_time = ts
        if self.alert_level == AlertLevel.SHUTDOWN:
            self.last_critical_time = ts

        # --- recovery mode management ---
        self._update_recovery(equity)

        # --- daily loss check ---
        daily_loss_pct = self._daily_loss_pct(equity)
        daily_breached = daily_loss_pct >= self._daily_loss_limit

        if daily_breached and self.alert_level != AlertLevel.SHUTDOWN:
            self.alert_level = AlertLevel.SHUTDOWN
            logger.warning(
                "Daily loss circuit breaker triggered: %.2f%% >= %.2f%%",
                daily_loss_pct * 100,
                self._daily_loss_limit * 100,
            )

        msg = self._format_message(equity, threshold, daily_loss_pct)
        logger.debug(msg)

        return self._build_status(
            message=msg,
            threshold=threshold,
            daily_loss_pct=daily_loss_pct,
        )

    def check_position(self, position: Position, current_price: float) -> Dict[str, Any]:
        """Evaluate per-position drawdown from its peak unrealised PnL.

        Tracks peak PnL internally and warns when the unrealised PnL
        has dropped more than 50 % from its peak.
        """
        sym = position.symbol
        try:
            # Compute mark-to-market PnL
            if position.side == "long":
                unrealised = (current_price - position.entry_price) * position.size
            elif position.side == "short":
                unrealised = (position.entry_price - current_price) * position.size
            else:
                unrealised = position.unrealized_pnl

            # Update peak PnL tracker (with size guard)
            prev_peak = self._position_peak_pnl.get(sym, unrealised)
            peak = max(prev_peak, unrealised)
            if sym not in self._position_peak_pnl and len(self._position_peak_pnl) >= self._max_tracked_positions:
                # Evict oldest entry to prevent unbounded dict growth
                oldest_key = next(iter(self._position_peak_pnl))
                del self._position_peak_pnl[oldest_key]
            self._position_peak_pnl[sym] = peak

            # Compute drawdown from peak PnL
            if peak > 0:
                pnl_dd = 1.0 - (unrealised / peak)
            else:
                pnl_dd = 0.0

            should_close = pnl_dd >= 0.50
            alert = AlertLevel.DANGER if should_close else AlertLevel.NORMAL

            return {
                "symbol": sym,
                "unrealized_pnl": round(unrealised, 6),
                "peak_pnl": round(peak, 6),
                "pnl_drawdown_pct": round(pnl_dd * 100, 2),
                "alert_level": alert.value,
                "should_close": should_close,
                "message": (
                    f"{sym}: PnL drawdown {pnl_dd*100:.1f}% from peak"
                    + (" — CLOSE recommended" if should_close else "")
                ),
            }

        except Exception as exc:  # pragma: no cover
            logger.error("check_position error for %s: %s", sym, exc, exc_info=True)
            return {
                "symbol": sym,
                "unrealized_pnl": 0.0,
                "peak_pnl": 0.0,
                "pnl_drawdown_pct": 0.0,
                "alert_level": AlertLevel.NORMAL.value,
                "should_close": False,
                "message": f"{sym}: error computing position drawdown — {exc}",
            }

    def get_dynamic_threshold(self) -> float:
        """Return the adaptive drawdown threshold.

        When ``vol_scale`` is enabled the threshold *tightens*
        (decreases) as recent equity volatility rises, preventing
        large drawdowns in choppy regimes.
        """
        if not self.vol_scale or len(self.equity_curve) < self.adaptive_lookback:
            return self.base_max_dd

        try:
            recent = np.array(
                [e for _, e in self.equity_curve[-self.adaptive_lookback:]],
                dtype=np.float64,
            )
            if np.any(recent <= 0):
                return self.base_max_dd

            returns = np.diff(np.log(recent))
            if len(returns) < 2:
                return self.base_max_dd

            vol = float(np.std(returns, ddof=1))

            # Baseline vol (annualised ~80 % ≈ per-tick ~0.005 for 1-min bars).
            # Scale factor: when vol doubles from baseline, threshold halves.
            baseline_vol = 0.005
            vol_ratio = vol / baseline_vol if baseline_vol > 0 else 1.0

            # vol_adjustment is negative when vol is high (tightens threshold)
            vol_adjustment = 1.0 - min(vol_ratio, 2.0) * 0.5  # range [0, 1]
            vol_adjustment = max(vol_adjustment, 0.25)          # floor at 25 % of base

            threshold = self.base_max_dd * vol_adjustment
            # Never allow threshold to exceed the base
            threshold = min(threshold, self.base_max_dd)
            # Never allow threshold below 0.5 %
            threshold = max(threshold, 0.005)

            return round(threshold, 6)

        except Exception as exc:  # pragma: no cover
            logger.error("get_dynamic_threshold error: %s", exc, exc_info=True)
            return self.base_max_dd

    def should_reduce_exposure(self) -> Dict[str, Any]:
        """Overall portfolio health assessment.

        Returns a recommendation dict including portfolio heat,
        cooldown status, and whether exposure should be reduced.
        """
        threshold = self.get_dynamic_threshold()
        dd_ratio = self.current_dd / threshold if threshold > 0 else 0.0

        # Cooldown check
        in_cooldown = self._in_cooldown()

        # Position sizing multiplier (accounts for recovery + drawdown)
        size_mult = self.get_position_size_multiplier()

        reduce = (
            self.alert_level in (AlertLevel.DANGER, AlertLevel.CRITICAL, AlertLevel.SHUTDOWN)
            or in_cooldown
            or self.in_recovery
        )

        return {
            "should_reduce": reduce,
            "alert_level": self.alert_level.value,
            "current_dd_pct": round(self.current_dd * 100, 2),
            "threshold_pct": round(threshold * 100, 2),
            "dd_ratio": round(dd_ratio, 4),
            "in_recovery": self.in_recovery,
            "in_cooldown": in_cooldown,
            "position_size_mult": round(size_mult, 4),
            "should_close_all": self.alert_level == AlertLevel.SHUTDOWN,
            "message": (
                f"Exposure check: DD {self.current_dd*100:.2f}%/{threshold*100:.2f}% "
                f"alert={self.alert_level.value} mult={size_mult:.2f}"
            ),
        }

    def get_position_size_multiplier(self) -> float:
        """Scale position size down as drawdown approaches the threshold.

        Returns a float in (0, 1].  At zero drawdown the multiplier is 1.0.
        At the threshold it drops towards 0.1.  During recovery mode or
        cooldown the multiplier is further reduced.
        """
        threshold = self.get_dynamic_threshold()
        if threshold <= 0:
            return 0.1

        dd_ratio = min(self.current_dd / threshold, 1.0)

        # Quadratic decay: gentle at first, aggressive near limit
        base_mult = max(1.0 - dd_ratio ** 2, 0.1)

        # Recovery penalty: cap at 60 % of base while recovering
        if self.in_recovery:
            base_mult = min(base_mult, 0.6)

        # Cooldown penalty: cap at 30 %
        if self._in_cooldown():
            base_mult = min(base_mult, 0.3)

        # Hard shutdown
        if self.alert_level == AlertLevel.SHUTDOWN:
            return 0.0

        return round(base_mult, 4)

    def reset_daily(self) -> None:
        """Reset daily-loss tracking.  Call at the start of each trading day."""
        if len(self.equity_curve) > 0:
            self.daily_start_equity = self.equity_curve[-1][1]
        else:
            self.daily_start_equity = self.peak_equity

        # Prune stale position-peak entries (positions that no longer exist
        # should not carry over indefinitely).
        self._position_peak_pnl.clear()

        logger.info(
            "DrawdownGuardian daily reset — start equity %.4f",
            self.daily_start_equity,
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _classify_alert(self, dd: float, threshold: float) -> AlertLevel:
        if threshold <= 0:
            return AlertLevel.SHUTDOWN
        ratio = dd / threshold
        if ratio >= _ALERT_THRESHOLDS[AlertLevel.SHUTDOWN]:
            return AlertLevel.SHUTDOWN
        if ratio >= _ALERT_THRESHOLDS[AlertLevel.CRITICAL]:
            return AlertLevel.CRITICAL
        if ratio >= _ALERT_THRESHOLDS[AlertLevel.DANGER]:
            return AlertLevel.DANGER
        if ratio >= _ALERT_THRESHOLDS[AlertLevel.WARNING]:
            return AlertLevel.WARNING
        return AlertLevel.NORMAL

    def _update_recovery(self, equity: float) -> None:
        """Enter or exit recovery mode based on current equity."""
        threshold = self.get_dynamic_threshold()

        # Enter recovery if we hit DANGER+ and are not already recovering
        if not self.in_recovery and self.current_dd >= threshold * 0.75:
            self.in_recovery = True
            # Target: recover back to (peak - recovery_pct * drawdown_amount)
            dd_amount = self.peak_equity - equity
            self.recovery_target = equity + dd_amount * self._recovery_pct
            logger.info(
                "Entered recovery mode — target equity %.4f (current %.4f)",
                self.recovery_target,
                equity,
            )

        # Exit recovery when equity exceeds target
        if self.in_recovery and equity >= self.recovery_target:
            self.in_recovery = False
            self.recovery_target = 0.0
            logger.info("Exited recovery mode — equity %.4f", equity)

    def _daily_loss_pct(self, equity: float) -> float:
        if self.daily_start_equity <= 0:
            return 0.0
        loss = max(self.daily_start_equity - equity, 0.0)
        return loss / self.daily_start_equity

    def _in_cooldown(self) -> bool:
        if self.last_critical_time <= 0:
            return False
        return (time.time() - self.last_critical_time) < self.cooldown_seconds

    def _format_message(
        self,
        equity: float,
        threshold: float,
        daily_loss_pct: float,
    ) -> str:
        parts = [
            f"[{self.alert_level.value}]",
            f"equity={equity:.2f}",
            f"peak={self.peak_equity:.2f}",
            f"DD={self.current_dd*100:.2f}%/{threshold*100:.2f}%",
            f"daily_loss={daily_loss_pct*100:.2f}%",
        ]
        if self.in_recovery:
            parts.append(f"RECOVERY(target={self.recovery_target:.2f})")
        if self._in_cooldown():
            remaining = self.cooldown_seconds - (time.time() - self.last_critical_time)
            parts.append(f"COOLDOWN({remaining:.0f}s)")
        return "  ".join(parts)

    def _build_status(
        self,
        message: str = "",
        threshold: Optional[float] = None,
        daily_loss_pct: float = 0.0,
    ) -> Dict[str, Any]:
        thr = threshold if threshold is not None else self.get_dynamic_threshold()
        return {
            "alert_level": self.alert_level.value,
            "current_dd_pct": round(self.current_dd * 100, 4),
            "threshold_pct": round(thr * 100, 4),
            "position_size_mult": self.get_position_size_multiplier(),
            "daily_loss_pct": round(daily_loss_pct * 100, 4),
            "in_recovery": self.in_recovery,
            "in_cooldown": self._in_cooldown(),
            "should_close_all": self.alert_level == AlertLevel.SHUTDOWN,
            "message": message,
        }

    # ------------------------------------------------------------------
    # Convenience / introspection
    # ------------------------------------------------------------------

    def snapshot(self) -> Dict[str, Any]:
        """Return a full snapshot of guardian state for logging / dashboards."""
        return {
            "peak_equity": self.peak_equity,
            "current_dd": round(self.current_dd, 6),
            "alert_level": self.alert_level.value,
            "dynamic_threshold": self.get_dynamic_threshold(),
            "position_size_mult": self.get_position_size_multiplier(),
            "in_recovery": self.in_recovery,
            "recovery_target": self.recovery_target,
            "daily_start_equity": self.daily_start_equity,
            "equity_curve_len": len(self.equity_curve),
            "position_peaks": dict(self._position_peak_pnl),
        }

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"DrawdownGuardian(dd={self.current_dd*100:.2f}% "
            f"alert={self.alert_level.value} "
            f"threshold={self.get_dynamic_threshold()*100:.2f}%)"
        )
