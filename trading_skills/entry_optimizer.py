"""
Entry Timing Optimizer — production-grade limit-order entry refinement.

Given a raw trade signal (long/short with price, SL, TP) and recent OHLCV data,
this module decides whether to enter via market order or a limit order at a
statistically advantageous price.  It synthesises VWAP distance, inferred
support/resistance, recent volatility, volume-profile nodes, candle momentum,
and time-of-day seasonality into a single actionable recommendation.

Dependencies: polars, numpy (no TA-Lib).
Expected DataFrame columns: open_time, open, high, low, close, volume
Signal schema: {"action": "long"|"short", "confidence": 0-1.0,
                "price": float, "sl": float, "tp": float}
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

import numpy as np
import polars as pl

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TOD_PEAK_HOURS_UTC = (0, 8, 16)  # typical crypto volatility spikes
_MIN_BARS_REQUIRED = 20           # absolute minimum for any analysis
_VOLUME_PROFILE_BINS = 30         # histogram resolution
_ATR_PERIOD = 14
_SUPPORT_RESISTANCE_LOOKBACK = 50
_MOMENTUM_EMA_SPAN = 5


# ---------------------------------------------------------------------------
# Helper: ATR (no TA-Lib)
# ---------------------------------------------------------------------------

def _compute_atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
                 period: int = _ATR_PERIOD) -> float:
    """Average True Range over the last *period* bars."""
    if len(highs) == 0:
        return 0.0
    if len(highs) < period + 1:
        mean_range = float(np.mean(highs - lows))
        return max(mean_range, 1e-12)  # prevent zero ATR
    tr = np.maximum(
        highs[1:] - lows[1:],
        np.maximum(
            np.abs(highs[1:] - closes[:-1]),
            np.abs(lows[1:] - closes[:-1]),
        ),
    )
    result = float(np.mean(tr[-period:]))
    return max(result, 1e-12)  # prevent zero ATR


# ---------------------------------------------------------------------------
# EntryTimingOptimizer
# ---------------------------------------------------------------------------

class EntryTimingOptimizer:
    """Optimises trade entries by recommending limit orders when profitable.

    Parameters
    ----------
    patience_bars : int
        Maximum bars to wait for a limit fill before cancelling.
    spread_estimate_pct : float
        Estimated bid-ask spread in percent of price (default 0.05 %).
    max_slippage_pct : float
        Maximum tolerable slippage for a market order (default 0.10 %).
    """

    def __init__(
        self,
        patience_bars: int = 5,
        spread_estimate_pct: float = 0.05,
        max_slippage_pct: float = 0.1,
    ) -> None:
        self.patience_bars = patience_bars
        self.spread_estimate_pct = spread_estimate_pct
        self.max_slippage_pct = max_slippage_pct

    # --------------------------------------------------------------------- #
    #  Public API                                                            #
    # --------------------------------------------------------------------- #

    def optimize_entry(self, signal: Dict[str, Any], df: pl.DataFrame) -> Dict[str, Any]:
        """Return an optimised order recommendation for *signal*.

        Parameters
        ----------
        signal : dict
            Must contain keys ``action``, ``confidence``, ``price``,
            ``sl``, ``tp``.
        df : pl.DataFrame
            Recent OHLCV bars (oldest first).

        Returns
        -------
        dict
            Order recommendation with keys ``order_type``, ``limit_price``,
            ``market_price``, ``expected_improvement_bps``,
            ``fill_probability``, ``time_limit_bars``, ``reasoning``,
            ``microstructure``, ``alternative``.
        """
        for key in ("action", "confidence", "price", "sl", "tp"):
            if key not in signal:
                raise ValueError(f"Signal missing required key: '{key}'")
        action = signal["action"].lower()
        confidence = float(signal["confidence"])
        market_price = float(signal["price"])
        sl = float(signal["sl"])
        tp = float(signal["tp"])

        required_cols = {"open", "high", "low", "close", "volume"}
        missing = required_cols - set(df.columns)
        if missing:
            logger.warning(
                "DataFrame missing columns %s; falling back to market order.", missing,
            )
            return self._market_order_result(
                market_price, action, confidence,
                reason=f"DataFrame missing columns: {missing}",
            )

        if len(df) < _MIN_BARS_REQUIRED:
            logger.warning(
                "Only %d bars available (need %d); falling back to market order.",
                len(df), _MIN_BARS_REQUIRED,
            )
            return self._market_order_result(
                market_price, action, confidence,
                reason="Insufficient data for microstructure analysis",
            )

        # -- Microstructure ------------------------------------------------
        micro = self._analyze_microstructure(df)

        # -- Momentum direction check --------------------------------------
        momentum = micro["momentum_strength"]
        momentum_aligned = (
            (action == "long" and momentum > 0.3)
            or (action == "short" and momentum < -0.3)
        )

        # -- Decision: market vs limit -------------------------------------
        # High confidence + aligned momentum → market order (don't miss it)
        if confidence > 0.85 and momentum_aligned:
            return self._market_order_result(
                market_price, action, confidence,
                micro=micro,
                reason=(
                    f"High confidence ({confidence:.2f}) with strong aligned "
                    f"momentum ({momentum:+.2f}); market order to avoid missing entry."
                ),
            )

        # -- Compute limit price -------------------------------------------
        limit_price = self._find_optimal_limit_price(signal, df, micro)

        # Sanity: limit must be on the correct side of market price
        if action == "long" and limit_price >= market_price:
            limit_price = market_price * (1 - self.spread_estimate_pct / 100)
        elif action == "short" and limit_price <= market_price:
            limit_price = market_price * (1 + self.spread_estimate_pct / 100)

        # -- Fill probability & expected improvement -----------------------
        fill_prob = self._estimate_fill_probability(limit_price, df, action)
        improvement_bps = self._compute_expected_improvement(
            market_price, limit_price, fill_prob,
        )

        # -- Determine patience (confidence-scaled) ------------------------
        if confidence < 0.5:
            time_limit = self.patience_bars + 2  # more patient
        elif confidence > 0.75:
            time_limit = max(1, self.patience_bars - 1)  # less patient
        else:
            time_limit = self.patience_bars

        # -- If improvement is negligible, just go market ------------------
        if improvement_bps < 2.0 or fill_prob < 0.15:
            return self._market_order_result(
                market_price, action, confidence,
                micro=micro,
                reason=(
                    f"Limit would save only {improvement_bps:.1f} bps with "
                    f"{fill_prob:.0%} fill probability; market order preferred."
                ),
            )

        # -- Build limit recommendation -----------------------------------
        reasoning_parts: List[str] = []  # capped at join via truncation below
        reasoning_parts.append(
            f"{'LONG' if action == 'long' else 'SHORT'} limit at "
            f"{limit_price:.6g} (market {market_price:.6g})."
        )
        reasoning_parts.append(
            f"Expected saving {improvement_bps:.1f} bps, "
            f"fill probability {fill_prob:.0%}."
        )
        if abs(micro["vwap_distance_pct"]) > 0.3:
            side = "above" if micro["vwap_distance_pct"] > 0 else "below"
            reasoning_parts.append(
                f"Price is {abs(micro['vwap_distance_pct']):.2f}% {side} VWAP "
                f"({micro['vwap']:.6g}); mean-reversion tailwind."
            )
        if confidence < 0.5:
            reasoning_parts.append(
                "Low confidence — wider limit for better risk/reward or skip."
            )

        alternative = self._market_order_result(
            market_price, action, confidence,
            micro=micro,
            reason="Fallback market order if limit expires unfilled.",
        )

        reasoning_text = " ".join(reasoning_parts)
        # Cap reasoning string length to prevent unbounded growth
        if len(reasoning_text) > 2000:
            reasoning_text = reasoning_text[:2000] + "..."

        return {
            "order_type": "limit",
            "limit_price": round(limit_price, 8),
            "market_price": round(market_price, 8),
            "expected_improvement_bps": round(improvement_bps, 2),
            "fill_probability": round(fill_prob, 4),
            "time_limit_bars": time_limit,
            "reasoning": reasoning_text,
            "microstructure": micro,
            "alternative": alternative,
        }

    # --------------------------------------------------------------------- #
    #  Microstructure analysis                                               #
    # --------------------------------------------------------------------- #

    def _analyze_microstructure(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Compute a microstructure snapshot from recent OHLCV bars.

        Returns a dict with ``vwap``, ``vwap_distance_pct``, ``recent_vol``,
        ``support``, ``resistance``, ``volume_node``, and
        ``momentum_strength``.
        """
        closes = df["close"].to_numpy(zero_copy_only=False).astype(np.float64)
        highs = df["high"].to_numpy(zero_copy_only=False).astype(np.float64)
        lows = df["low"].to_numpy(zero_copy_only=False).astype(np.float64)
        volumes = df["volume"].to_numpy(zero_copy_only=False).astype(np.float64)

        last_close = float(closes[-1])

        # VWAP
        vwap = self._compute_vwap(closes, highs, lows, volumes)
        vwap_dist = self._compute_vwap_distance(df, vwap=vwap)

        # Volatility (ATR-based, normalised to %)
        atr = _compute_atr(highs, lows, closes)
        recent_vol = (atr / last_close) * 100 if last_close > 0 else 0.0

        # Support / resistance
        support, resistance = self._find_support_resistance(highs, lows, closes)

        # Volume profile node
        volume_node = self._find_volume_node(highs, lows, volumes)

        # Momentum strength (-1 … +1)
        momentum = self._compute_momentum(closes)

        return {
            "vwap": round(vwap, 8),
            "vwap_distance_pct": round(vwap_dist, 4),
            "recent_vol": round(recent_vol, 4),
            "support": round(support, 8),
            "resistance": round(resistance, 8),
            "volume_node": round(volume_node, 8),
            "momentum_strength": round(momentum, 4),
        }

    # --------------------------------------------------------------------- #
    #  VWAP helpers                                                          #
    # --------------------------------------------------------------------- #

    @staticmethod
    def _compute_vwap(
        closes: np.ndarray,
        highs: np.ndarray,
        lows: np.ndarray,
        volumes: np.ndarray,
    ) -> float:
        """Session VWAP using typical price (H+L+C)/3."""
        typical = (highs + lows + closes) / 3.0
        cum_vol = np.sum(volumes)
        if cum_vol == 0:
            return float(closes[-1])
        return float(np.sum(typical * volumes) / cum_vol)

    def _compute_vwap_distance(
        self,
        df: pl.DataFrame,
        *,
        vwap: Optional[float] = None,
    ) -> float:
        """Percentage distance of last close from VWAP.

        Positive means price is *above* VWAP; negative means below.
        """
        closes = df["close"].to_numpy(zero_copy_only=False).astype(np.float64)
        if vwap is None:
            highs = df["high"].to_numpy(zero_copy_only=False).astype(np.float64)
            lows = df["low"].to_numpy(zero_copy_only=False).astype(np.float64)
            volumes = df["volume"].to_numpy(zero_copy_only=False).astype(np.float64)
            vwap = self._compute_vwap(closes, highs, lows, volumes)
        if vwap == 0:
            return 0.0
        return ((float(closes[-1]) - vwap) / vwap) * 100.0

    # --------------------------------------------------------------------- #
    #  Order-book pressure proxy                                             #
    # --------------------------------------------------------------------- #

    def _compute_order_book_pressure(self, df: pl.DataFrame) -> float:
        """Inferred buy/sell pressure from volume-weighted price movement.

        Returns a value in ``(-1, +1)`` where positive is buy pressure.
        No real order-book data is required.
        """
        closes = df["close"].to_numpy(zero_copy_only=False).astype(np.float64)
        opens = df["open"].to_numpy(zero_copy_only=False).astype(np.float64)
        volumes = df["volume"].to_numpy(zero_copy_only=False).astype(np.float64)

        lookback = min(len(closes), 10)
        c = closes[-lookback:]
        o = opens[-lookback:]
        v = volumes[-lookback:]

        # Direction: +1 for bullish bar, -1 for bearish
        direction = np.sign(c - o)
        total_vol = np.sum(v)
        if total_vol == 0:
            return 0.0
        pressure = float(np.sum(direction * v) / total_vol)
        return float(np.clip(pressure, -1.0, 1.0))

    # --------------------------------------------------------------------- #
    #  Optimal limit price                                                   #
    # --------------------------------------------------------------------- #

    def _find_optimal_limit_price(
        self,
        signal: Dict[str, Any],
        df: pl.DataFrame,
        micro: Optional[Dict[str, Any]] = None,
    ) -> float:
        """Compute the best limit price for *signal*.

        For **long**: ``limit = min(price - atr_fraction, support, vwap)``
        adjusted toward market price when confidence is high.

        For **short**: mirrored logic with resistance.
        """
        if micro is None:
            micro = self._analyze_microstructure(df)

        action = signal["action"].lower()
        confidence = float(signal["confidence"])
        market_price = float(signal["price"])

        closes = df["close"].to_numpy(zero_copy_only=False).astype(np.float64)
        highs = df["high"].to_numpy(zero_copy_only=False).astype(np.float64)
        lows = df["low"].to_numpy(zero_copy_only=False).astype(np.float64)

        atr = _compute_atr(highs, lows, closes)

        # Confidence-based aggressiveness: high confidence → small offset
        # low confidence → large offset (want a bargain or skip)
        aggression = 1.0 - confidence  # 0 = very aggressive, 1 = very passive
        atr_fraction = atr * (0.2 + 0.8 * aggression)  # 0.2×ATR … 1.0×ATR

        # Time-of-day adjustment
        tod_factor = self._time_of_day_factor(df)
        atr_fraction *= tod_factor

        # Candle-chase guard: if last candle moved strongly in signal direction
        # widen the limit to avoid chasing
        chase_penalty = self._candle_chase_penalty(closes, highs, lows, atr, action)
        atr_fraction *= (1.0 + chase_penalty)

        if action == "long":
            candidates = [
                market_price - atr_fraction,
                micro["support"],
                micro["vwap"],
                micro["volume_node"],
            ]
            # Filter out candidates that are absurdly far (> 2×ATR from market)
            valid = [p for p in candidates
                     if 0 < p < market_price and (market_price - p) < 2.0 * atr]
            if not valid:
                return market_price - atr_fraction
            # Pick the *highest* valid candidate (closest to market → best fill)
            # but blend toward the lowest when confidence is low
            valid_sorted = sorted(valid)
            idx = int(round(confidence * (len(valid_sorted) - 1)))
            return valid_sorted[idx]

        else:  # short
            candidates = [
                market_price + atr_fraction,
                micro["resistance"],
                micro["vwap"],
                micro["volume_node"],
            ]
            valid = [p for p in candidates
                     if p > market_price and (p - market_price) < 2.0 * atr]
            if not valid:
                return market_price + atr_fraction
            # Sort ascending: index 0 = closest to market (tightest), last = farthest
            # High confidence -> pick closer to market (higher fill prob)
            # Low confidence -> pick farther from market (better price)
            valid_sorted = sorted(valid)
            idx = int(round(confidence * (len(valid_sorted) - 1)))
            return valid_sorted[idx]

    # --------------------------------------------------------------------- #
    #  Fill probability                                                      #
    # --------------------------------------------------------------------- #

    def _estimate_fill_probability(
        self,
        limit_price: float,
        df: pl.DataFrame,
        action: Optional[str] = None,
    ) -> float:
        """Estimate the probability that *limit_price* gets filled.

        Uses the empirical distribution of recent lows (for longs) or
        highs (for shorts) over the patience window.
        """
        highs = df["high"].to_numpy(zero_copy_only=False).astype(np.float64)
        lows = df["low"].to_numpy(zero_copy_only=False).astype(np.float64)

        lookback = min(len(highs), max(self.patience_bars * 4, 20))

        if action == "short":
            # For short limit above market: what fraction of recent bars
            # had highs >= limit_price?
            recent = highs[-lookback:]
            filled_count = np.sum(recent >= limit_price)
        else:
            # For long limit below market: fraction of bars whose lows
            # reached down to limit_price
            recent = lows[-lookback:]
            filled_count = np.sum(recent <= limit_price)

        raw_prob = float(filled_count) / lookback

        # Scale by patience: more bars waiting → higher cumulative probability
        # P(fill in N bars) ≈ 1 - (1 - p_single)^N
        p_single = max(raw_prob, 0.01)
        cumulative = 1.0 - (1.0 - p_single) ** self.patience_bars

        return float(np.clip(cumulative, 0.0, 1.0))

    # --------------------------------------------------------------------- #
    #  Expected improvement                                                  #
    # --------------------------------------------------------------------- #

    @staticmethod
    def _compute_expected_improvement(
        market_price: float,
        limit_price: float,
        fill_prob: float,
    ) -> float:
        """Expected saving in basis points, accounting for fill probability.

        ``E[improvement] = |market - limit| / market * 10_000 * fill_prob``
        """
        if market_price == 0:
            return 0.0
        raw_bps = abs(market_price - limit_price) / market_price * 10_000
        return raw_bps * fill_prob

    # --------------------------------------------------------------------- #
    #  Support / resistance (pivot-based)                                    #
    # --------------------------------------------------------------------- #

    @staticmethod
    def _find_support_resistance(
        highs: np.ndarray,
        lows: np.ndarray,
        closes: np.ndarray,
    ) -> tuple[float, float]:
        """Identify nearest support and resistance from recent swing points.

        Uses a simple rolling-window pivot detection (local minima / maxima
        with a 3-bar confirmation on each side).
        """
        lookback = min(len(highs), _SUPPORT_RESISTANCE_LOOKBACK)
        h = highs[-lookback:]
        l = lows[-lookback:]
        last = float(closes[-1])

        pivot_highs: List[float] = []
        pivot_lows: List[float] = []
        order = 3  # bars on each side

        for i in range(order, len(h) - order):
            if h[i] == np.max(h[i - order : i + order + 1]):
                pivot_highs.append(float(h[i]))
            if l[i] == np.min(l[i - order : i + order + 1]):
                pivot_lows.append(float(l[i]))

        # Nearest support below current price
        supports_below = [p for p in pivot_lows if p < last]
        support = max(supports_below) if supports_below else last - (last * 0.005)

        # Nearest resistance above current price
        resistances_above = [p for p in pivot_highs if p > last]
        resistance = min(resistances_above) if resistances_above else last + (last * 0.005)

        return support, resistance

    # --------------------------------------------------------------------- #
    #  Volume profile (poor-man's histogram)                                 #
    # --------------------------------------------------------------------- #

    @staticmethod
    def _find_volume_node(
        highs: np.ndarray,
        lows: np.ndarray,
        volumes: np.ndarray,
    ) -> float:
        """Price level with highest accumulated volume (value area POC).

        Distributes each bar's volume uniformly across its high-low range
        into a histogram, then returns the bin centre with the most volume.
        """
        lookback = min(len(highs), _SUPPORT_RESISTANCE_LOOKBACK)
        h = highs[-lookback:]
        l = lows[-lookback:]
        v = volumes[-lookback:]

        price_min = float(np.min(l))
        price_max = float(np.max(h))
        if price_max == price_min:
            return float(price_min)

        bins = np.linspace(price_min, price_max, _VOLUME_PROFILE_BINS + 1)
        profile = np.zeros(_VOLUME_PROFILE_BINS, dtype=np.float64)

        for i in range(len(h)):
            bar_lo, bar_hi, bar_vol = float(l[i]), float(h[i]), float(v[i])
            if bar_hi == bar_lo or bar_vol == 0:
                continue
            # Determine which bins this bar spans
            lo_idx = max(0, int((bar_lo - price_min) / (price_max - price_min) * _VOLUME_PROFILE_BINS))
            hi_idx = min(_VOLUME_PROFILE_BINS - 1, int((bar_hi - price_min) / (price_max - price_min) * _VOLUME_PROFILE_BINS))
            if hi_idx < lo_idx:
                continue
            n_bins = max(1, hi_idx - lo_idx + 1)
            profile[lo_idx : hi_idx + 1] += bar_vol / n_bins

        poc_idx = int(np.argmax(profile))
        bin_centre = (bins[poc_idx] + bins[poc_idx + 1]) / 2.0
        return float(bin_centre)

    # --------------------------------------------------------------------- #
    #  Momentum                                                              #
    # --------------------------------------------------------------------- #

    @staticmethod
    def _compute_momentum(closes: np.ndarray) -> float:
        """Momentum strength in ``(-1, +1)`` using EMA slope + ROC.

        Combines a short EMA slope (direction) with rate-of-change magnitude,
        then clips to [-1, 1].
        """
        n = len(closes)
        if n < _MOMENTUM_EMA_SPAN + 1:
            return 0.0

        # EMA
        alpha = 2.0 / (_MOMENTUM_EMA_SPAN + 1)
        ema = np.empty(n, dtype=np.float64)
        ema[0] = closes[0]
        for i in range(1, n):
            ema[i] = alpha * closes[i] + (1 - alpha) * ema[i - 1]

        # Normalised slope of last EMA segment
        ema_tail = ema[-_MOMENTUM_EMA_SPAN:]
        ema_base = ema_tail[0] if ema_tail[0] != 0 else 1e-12
        slope = (ema_tail[-1] - ema_tail[0]) / ema_base

        # 5-bar ROC
        roc_base = closes[-_MOMENTUM_EMA_SPAN] if closes[-_MOMENTUM_EMA_SPAN] != 0 else 1e-12
        roc = (closes[-1] - closes[-_MOMENTUM_EMA_SPAN]) / roc_base

        combined = 0.6 * slope + 0.4 * roc
        # Scale so that ±2 % move ≈ ±1.0
        scaled = combined / 0.02
        return float(np.clip(scaled, -1.0, 1.0))

    # --------------------------------------------------------------------- #
    #  Time-of-day seasonality factor                                        #
    # --------------------------------------------------------------------- #

    @staticmethod
    def _time_of_day_factor(df: pl.DataFrame) -> float:
        """Return a volatility-scaling factor based on hour-of-day.

        Near ``_TOD_PEAK_HOURS_UTC`` (session opens) volatility is typically
        elevated → widen limit offsets (factor > 1).  During off-peak hours
        volatility compresses → tighter limits (factor < 1).
        """
        try:
            last_time = df["open_time"].to_list()[-1]
            if isinstance(last_time, (int, float)):
                # Heuristic: values > 1e12 are milliseconds, else seconds
                ts = last_time / 1000 if last_time > 1e12 else float(last_time)
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            elif isinstance(last_time, datetime):
                dt = last_time if last_time.tzinfo else last_time.replace(tzinfo=timezone.utc)
            else:
                return 1.0
            hour = dt.hour
        except Exception:
            return 1.0

        # Distance to nearest peak hour
        min_dist = min(abs(hour - ph) % 24 for ph in _TOD_PEAK_HOURS_UTC)
        min_dist = min(min_dist, 24 - min_dist)

        # Gaussian-ish bump: peaks at 0 distance, baseline at far distance
        # factor range: [0.85, 1.25]
        factor = 0.85 + 0.40 * math.exp(-(min_dist ** 2) / 8.0)
        return factor

    # --------------------------------------------------------------------- #
    #  Candle-chase penalty                                                  #
    # --------------------------------------------------------------------- #

    @staticmethod
    def _candle_chase_penalty(
        closes: np.ndarray,
        highs: np.ndarray,
        lows: np.ndarray,
        atr: float,
        action: str,
    ) -> float:
        """Extra offset multiplier when the last candle was a large move
        in the signal direction (avoid chasing momentum bars).

        Returns 0.0 (no penalty) to 1.0 (double the ATR offset).
        """
        if len(closes) < 2 or atr == 0:
            return 0.0

        body = closes[-1] - closes[-2]  # positive = bullish
        bar_range = highs[-1] - lows[-1]

        if bar_range == 0:
            return 0.0

        body_ratio = abs(body) / bar_range  # how "full" the candle is
        size_ratio = bar_range / atr         # how large relative to ATR

        # Only penalise when the candle is in the same direction as signal
        if action == "long" and body <= 0:
            return 0.0
        if action == "short" and body >= 0:
            return 0.0

        # Penalty scales with body fullness and relative size
        penalty = body_ratio * min(size_ratio, 2.0) * 0.5
        return float(np.clip(penalty, 0.0, 1.0))

    # --------------------------------------------------------------------- #
    #  Market-order result builder                                           #
    # --------------------------------------------------------------------- #

    def _market_order_result(
        self,
        market_price: float,
        action: str,
        confidence: float,
        *,
        micro: Optional[Dict[str, Any]] = None,
        reason: str = "",
    ) -> Dict[str, Any]:
        """Build a standardised market-order recommendation dict."""
        return {
            "order_type": "market",
            "limit_price": None,
            "market_price": round(market_price, 8),
            "expected_improvement_bps": 0.0,
            "fill_probability": 1.0,
            "time_limit_bars": 0,
            "reasoning": reason or "Market order — immediate fill.",
            "microstructure": micro or {},
            "alternative": None,
        }
