"""
Signal Confidence Scorer for quantitative crypto trading.

Replaces boolean pass/fail signal filtering with continuous 0-100 scoring
across six orthogonal components: trend alignment, momentum, volume profile,
volatility context, price action, and risk/reward.

Designed for integration with Polars DataFrames and the standard signal dict
format used throughout the trading pipeline.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import polars as pl

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default component weights -- must sum to 1.0
# ---------------------------------------------------------------------------
DEFAULT_WEIGHTS: dict[str, float] = {
    "trend": 0.25,
    "momentum": 0.20,
    "volume": 0.15,
    "volatility": 0.15,
    "price_action": 0.15,
    "risk_reward": 0.10,
}

# ---------------------------------------------------------------------------
# Default strategy parameters used when the caller does not supply overrides
# ---------------------------------------------------------------------------
DEFAULT_PARAMS: dict[str, Any] = {
    "ma5_len": 5,
    "ma8_len": 8,
    "ema21_len": 21,
    "ma55_len": 55,
    "bb_length": 20,
    "bb_std_dev": 2.0,
    "atr_period": 14,
    "slope_len": 5,
    "slope_threshold": 0.0005,
}

# Grade boundaries (lower inclusive)
_GRADE_MAP: list[tuple[float, str]] = [
    (90.0, "A+"),
    (80.0, "A"),
    (65.0, "B"),
    (50.0, "C"),
    (35.0, "D"),
]


def _grade_from_score(score: float) -> str:
    for threshold, grade in _GRADE_MAP:
        if score >= threshold:
            return grade
    return "F"


def _recommendation(grade: str, side: str) -> str:
    if grade in ("A+", "A"):
        return f"Strong {side} setup -- high conviction entry."
    if grade == "B":
        return f"Decent {side} setup -- consider reduced size."
    if grade == "C":
        return f"Marginal {side} setup -- wait for confirmation or skip."
    if grade == "D":
        return f"Weak {side} setup -- avoid or paper-trade only."
    return "No edge detected -- do not trade."


class SignalConfidenceScorer:
    """Continuous 0-100 signal confidence scorer.

    Parameters
    ----------
    weights : dict, optional
        Component weight overrides.  Keys must be a subset of
        ``DEFAULT_WEIGHTS``.  Values are re-normalised to sum to 1.0.
    """

    def __init__(self, weights: dict[str, float] | None = None) -> None:
        if weights is None:
            self.weights = dict(DEFAULT_WEIGHTS)
        else:
            # Validate keys
            unknown = set(weights) - set(DEFAULT_WEIGHTS)
            if unknown:
                raise ValueError(f"Unknown weight keys: {unknown}")
            merged = {**DEFAULT_WEIGHTS, **weights}
            total = sum(merged.values())
            if total <= 0:
                raise ValueError("Weight sum must be positive.")
            self.weights = {k: v / total for k, v in merged.items()}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def score(
        self,
        signal: dict[str, Any],
        df: pl.DataFrame,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Score a trading signal against recent OHLCV data.

        Parameters
        ----------
        signal : dict
            Must contain ``action`` (``"long"`` or ``"short"``), ``price``,
            and optionally ``sl``, ``tp``.
        df : pl.DataFrame
            OHLCV DataFrame with columns: ``open_time``, ``open``, ``high``,
            ``low``, ``close``, ``volume``.  Must have at least 55 rows for
            full scoring; fewer rows degrade gracefully.
        params : dict, optional
            Strategy parameter overrides.

        Returns
        -------
        dict
            Detailed score breakdown including ``total_score``,
            ``components``, ``weights``, ``confidence``, ``grade``,
            ``recommendation``, and ``flags``.
        """
        params = {**DEFAULT_PARAMS, **(params or {})}
        side = signal.get("action", "long")
        if side not in ("long", "short"):
            raise ValueError(f"Invalid action: {side!r}. Expected 'long' or 'short'.")

        required_cols = {"open", "high", "low", "close", "volume"}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"DataFrame missing columns: {missing}")

        if df.height < 2:
            raise ValueError("DataFrame must contain at least 2 rows.")

        flags: list[str] = []

        # Compute individual component scores
        trend = self._score_trend_alignment(df, side, params, flags)
        momentum = self._score_momentum(df, side, params, flags)
        volume = self._score_volume_profile(df, flags)
        volatility = self._score_volatility_context(df, params, flags)
        price_action = self._score_price_action(df, side, flags)
        risk_reward = self._score_risk_reward(signal, df, params, flags)

        components = {
            "trend": round(trend, 2),
            "momentum": round(momentum, 2),
            "volume": round(volume, 2),
            "volatility": round(volatility, 2),
            "price_action": round(price_action, 2),
            "risk_reward": round(risk_reward, 2),
        }

        total = sum(components[k] * self.weights[k] for k in self.weights)
        total = round(np.clip(total, 0.0, 100.0), 2)
        grade = _grade_from_score(total)

        return {
            "total_score": total,
            "components": components,
            "weights": dict(self.weights),
            "confidence": round(total / 100.0, 4),
            "grade": grade,
            "recommendation": _recommendation(grade, side),
            "flags": flags,
        }

    # ------------------------------------------------------------------
    # Component scorers
    # ------------------------------------------------------------------

    def _score_trend_alignment(
        self,
        df: pl.DataFrame,
        side: str,
        params: dict[str, Any],
        flags: list[str],
    ) -> float:
        """Score 0-100 based on moving-average stack ordering quality.

        A perfect long stack is MA5 > MA8 > EMA21 > MA55; a perfect short
        stack is the reverse.  Partial alignment scores proportionally.
        """
        closes = df["close"].to_numpy(zero_copy_only=False).astype(np.float64)
        n = len(closes)

        ma_specs: list[tuple[str, int, bool]] = [
            ("ma5", params["ma5_len"], False),
            ("ma8", params["ma8_len"], False),
            ("ema21", params["ema21_len"], True),
            ("ma55", params["ma55_len"], False),
        ]

        last_values: list[float | None] = []
        for _name, length, is_ema in ma_specs:
            if n < length:
                last_values.append(None)
                continue
            if is_ema:
                last_values.append(float(self._ema(closes, length)[-1]))
            else:
                last_values.append(float(self._sma(closes, length)[-1]))

        # Only score MAs that could be computed
        available = [(i, v) for i, v in enumerate(last_values) if v is not None]
        if len(available) < 2:
            flags.append("insufficient_data_for_trend")
            return 50.0  # neutral

        vals = [v for _, v in available]

        # Count correctly ordered pairs
        total_pairs = 0
        correct = 0
        for i in range(len(vals)):
            for j in range(i + 1, len(vals)):
                total_pairs += 1
                if side == "long" and vals[i] > vals[j]:
                    correct += 1
                elif side == "short" and vals[i] < vals[j]:
                    correct += 1

        if total_pairs == 0:
            return 50.0

        ratio = correct / total_pairs

        # Bonus: check slope of fastest MA confirms direction
        ma5_len = params["ma5_len"]
        slope_len = params["slope_len"]
        if n >= ma5_len + slope_len:
            sma5 = self._sma(closes, ma5_len)
            slope = (sma5[-1] - sma5[-slope_len]) / (sma5[-slope_len] + 1e-12)
            slope_ok = (side == "long" and slope > 0) or (side == "short" and slope < 0)
            if not slope_ok:
                flags.append("ma5_slope_against_trade")
                ratio *= 0.8

        return float(np.clip(ratio * 100.0, 0.0, 100.0))

    def _score_momentum(
        self,
        df: pl.DataFrame,
        side: str,
        params: dict[str, Any],
        flags: list[str],
    ) -> float:
        """Score 0-100 based on RSI zone, MACD histogram, and price slope."""
        closes = df["close"].to_numpy(zero_copy_only=False).astype(np.float64)
        n = len(closes)
        sub_scores: list[tuple[str, float, float]] = []

        # --- RSI component (weight 0.4) ---
        if n >= 15:
            rsi_arr = self._compute_rsi(closes, period=14)
            rsi = float(rsi_arr[-1])

            if side == "long":
                # Ideal zone 40-70
                if 40 <= rsi <= 70:
                    rsi_score = 100.0
                elif rsi < 40:
                    rsi_score = max(0.0, 100.0 - (40.0 - rsi) * 3.0)
                else:
                    rsi_score = max(0.0, 100.0 - (rsi - 70.0) * 4.0)
                    if rsi > 80:
                        flags.append("rsi_overbought")
            else:
                # Ideal zone 30-60
                if 30 <= rsi <= 60:
                    rsi_score = 100.0
                elif rsi > 60:
                    rsi_score = max(0.0, 100.0 - (rsi - 60.0) * 3.0)
                else:
                    rsi_score = max(0.0, 100.0 - (30.0 - rsi) * 4.0)
                    if rsi < 20:
                        flags.append("rsi_oversold")

            sub_scores.append(("rsi", 0.4, rsi_score))
        else:
            sub_scores.append(("rsi", 0.4, 50.0))

        # --- MACD component (weight 0.35) ---
        if n >= 35:
            macd_line, signal_line, histogram = self._compute_macd(closes)
            hist_val = float(histogram[-1])
            hist_prev = float(histogram[-2]) if len(histogram) >= 2 else 0.0

            macd_score = 50.0
            if side == "long":
                if hist_val > 0:
                    macd_score = 70.0
                    if hist_val > hist_prev:
                        macd_score = 100.0  # accelerating
                else:
                    macd_score = 30.0
                    if hist_val > hist_prev:
                        macd_score = 50.0  # converging
                        flags.append("macd_converging_bullish")
            else:
                if hist_val < 0:
                    macd_score = 70.0
                    if hist_val < hist_prev:
                        macd_score = 100.0
                else:
                    macd_score = 30.0
                    if hist_val < hist_prev:
                        macd_score = 50.0
                        flags.append("macd_converging_bearish")

            # Detect divergence
            if n >= 40:
                price_higher = closes[-1] > closes[-10]
                hist_higher = hist_val > float(histogram[-10]) if len(histogram) >= 10 else True
                if side == "long" and price_higher and not hist_higher:
                    flags.append("divergence_detected")
                    macd_score *= 0.7
                elif side == "short" and not price_higher and hist_higher:
                    flags.append("divergence_detected")
                    macd_score *= 0.7

            sub_scores.append(("macd", 0.35, macd_score))
        else:
            sub_scores.append(("macd", 0.35, 50.0))

        # --- Slope component (weight 0.25) ---
        slope_len = params["slope_len"]
        if n >= slope_len + 1:
            slope = (closes[-1] - closes[-slope_len]) / (closes[-slope_len] + 1e-12)
            threshold = params["slope_threshold"]

            if side == "long":
                if slope > threshold * 2:
                    slope_score = 100.0
                elif slope > threshold:
                    slope_score = 75.0
                elif slope > 0:
                    slope_score = 55.0
                else:
                    slope_score = max(0.0, 40.0 + slope / threshold * 40.0)
            else:
                slope = -slope
                if slope > threshold * 2:
                    slope_score = 100.0
                elif slope > threshold:
                    slope_score = 75.0
                elif slope > 0:
                    slope_score = 55.0
                else:
                    slope_score = max(0.0, 40.0 + slope / threshold * 40.0)

            sub_scores.append(("slope", 0.25, slope_score))
        else:
            sub_scores.append(("slope", 0.25, 50.0))

        # Weighted combination
        total_weight = sum(w for _, w, _ in sub_scores)
        if total_weight == 0:
            return 50.0
        result = sum(w * s for _, w, s in sub_scores) / total_weight
        return float(np.clip(result, 0.0, 100.0))

    def _score_volume_profile(
        self,
        df: pl.DataFrame,
        flags: list[str],
    ) -> float:
        """Score 0-100 based on volume confirmation strength.

        Compares recent volume to 20-bar average.  Increasing volume on
        the prevailing trend direction is rewarded.
        """
        vol = df["volume"].to_numpy(zero_copy_only=False).astype(np.float64)
        n = len(vol)
        lookback = min(20, n)

        avg_vol = np.mean(vol[-lookback:])
        if avg_vol <= 0:
            flags.append("zero_average_volume")
            return 0.0

        current_vol = float(vol[-1])
        ratio = current_vol / avg_vol

        if ratio < 0.5:
            flags.append("low_volume")
            score = ratio * 40.0  # 0-20
        elif ratio < 0.8:
            score = 20.0 + (ratio - 0.5) * 100.0  # 20-50
        elif ratio < 1.2:
            score = 50.0 + (ratio - 0.8) * 100.0  # 50-90
        elif ratio < 2.0:
            score = 90.0 + (ratio - 1.2) * 12.5  # 90-100
        else:
            score = 95.0  # very high volume can be climactic -- cap it
            flags.append("climactic_volume")

        # Bonus: check if last 3 bars show increasing volume
        if n >= 4:
            recent = vol[-3:]
            if all(recent[i] > recent[i - 1] for i in range(1, len(recent))):
                score = min(100.0, score + 10.0)

        return float(np.clip(score, 0.0, 100.0))

    def _score_volatility_context(
        self,
        df: pl.DataFrame,
        params: dict[str, Any],
        flags: list[str],
    ) -> float:
        """Score 0-100 for favorable volatility environment.

        Neither too high (choppy / whipsaw risk) nor too low (no momentum).
        Sweet-spot is moderate ATR relative to recent median.
        """
        highs = df["high"].to_numpy(zero_copy_only=False).astype(np.float64)
        lows = df["low"].to_numpy(zero_copy_only=False).astype(np.float64)
        closes = df["close"].to_numpy(zero_copy_only=False).astype(np.float64)
        n = len(closes)
        atr_period = min(params["atr_period"], n - 1)

        if atr_period < 2:
            return 50.0

        atr = self._compute_atr(highs, lows, closes, atr_period)
        if len(atr) == 0 or np.all(np.isnan(atr)):
            return 50.0
        current_atr = float(atr[-1]) if np.isfinite(atr[-1]) else 0.0
        atr_window = atr[-min(50, len(atr)):]
        valid_atr = atr_window[np.isfinite(atr_window)]
        median_atr = float(np.median(valid_atr)) if len(valid_atr) > 0 else 0.0

        if median_atr <= 0:
            return 50.0

        ratio = current_atr / median_atr

        # Bollinger Band width as secondary measure
        bb_len = min(params["bb_length"], n)
        if bb_len >= 2:
            sma = self._sma(closes, bb_len)
            std = self._rolling_std(closes, bb_len)
            bb_width = (2 * params["bb_std_dev"] * std[-1]) / (sma[-1] + 1e-12)
        else:
            bb_width = 0.02  # neutral default

        # Sweet spot: ratio near 1.0, BB width moderate
        if ratio < 0.5:
            flags.append("very_low_volatility")
            atr_score = ratio * 60.0
        elif ratio < 0.8:
            atr_score = 30.0 + (ratio - 0.5) * 166.67  # 30-80
        elif ratio <= 1.3:
            atr_score = 80.0 + (1.0 - abs(ratio - 1.05)) * 40.0  # peak near 1.05
            atr_score = min(100.0, atr_score)
        elif ratio <= 2.0:
            atr_score = max(20.0, 80.0 - (ratio - 1.3) * 85.7)
        else:
            flags.append("extreme_volatility")
            atr_score = 10.0

        # BB width penalty/bonus
        if bb_width > 0.08:
            flags.append("wide_bollinger_bands")
            atr_score *= 0.85
        elif bb_width < 0.005:
            flags.append("squeeze_detected")
            atr_score = min(100.0, atr_score * 1.1)  # squeeze can be opportunity

        return float(np.clip(atr_score, 0.0, 100.0))

    def _score_price_action(
        self,
        df: pl.DataFrame,
        side: str,
        flags: list[str],
    ) -> float:
        """Score 0-100 based on recent candle patterns and structure.

        Checks for:
        - Candle body direction alignment with trade side
        - Rejection wicks against the trade
        - Recent candle size consistency
        """
        opens = df["open"].to_numpy(zero_copy_only=False).astype(np.float64)
        highs = df["high"].to_numpy(zero_copy_only=False).astype(np.float64)
        lows = df["low"].to_numpy(zero_copy_only=False).astype(np.float64)
        closes = df["close"].to_numpy(zero_copy_only=False).astype(np.float64)
        n = len(closes)

        lookback = min(5, n)
        score = 50.0  # start neutral

        # --- Candle direction alignment (last N candles) ---
        bullish_count = 0
        for i in range(-lookback, 0):
            if closes[i] > opens[i]:
                bullish_count += 1

        if side == "long":
            direction_ratio = bullish_count / lookback
        else:
            direction_ratio = (lookback - bullish_count) / lookback

        score += (direction_ratio - 0.5) * 40.0  # +/- 20 points

        # --- Rejection wick analysis on last candle ---
        o, h, l, c = opens[-1], highs[-1], lows[-1], closes[-1]
        body = abs(c - o)
        full_range = h - l
        if full_range > 0:
            upper_wick = h - max(o, c)
            lower_wick = min(o, c) - l
            body_ratio = body / full_range

            if side == "long":
                # Long upper wick = rejection = bad for longs
                if upper_wick > body * 1.5:
                    flags.append("upper_rejection_wick")
                    score -= 15.0
                # Long lower wick = buying pressure = good for longs
                if lower_wick > body * 1.5 and c > o:
                    score += 10.0
            else:
                # Long lower wick = rejection = bad for shorts
                if lower_wick > body * 1.5:
                    flags.append("lower_rejection_wick")
                    score -= 15.0
                if upper_wick > body * 1.5 and c < o:
                    score += 10.0

            # Doji detection (indecision)
            if body_ratio < 0.1:
                flags.append("doji_candle")
                score -= 5.0

        # --- Consecutive same-direction candles ---
        if n >= 3:
            last3_bull = all(closes[i] > opens[i] for i in range(-3, 0))
            last3_bear = all(closes[i] < opens[i] for i in range(-3, 0))
            if (side == "long" and last3_bull) or (side == "short" and last3_bear):
                score += 10.0
            elif (side == "long" and last3_bear) or (side == "short" and last3_bull):
                score -= 10.0

        # --- Proximity to recent high/low (support/resistance) ---
        if n >= 20:
            recent_high = float(np.max(highs[-20:]))
            recent_low = float(np.min(lows[-20:]))
            price_range = recent_high - recent_low
            if price_range > 0:
                position = (closes[-1] - recent_low) / price_range
                if side == "long" and position > 0.95:
                    flags.append("near_resistance")
                    score -= 10.0
                elif side == "short" and position < 0.05:
                    flags.append("near_support")
                    score -= 10.0

        return float(np.clip(score, 0.0, 100.0))

    def _score_risk_reward(
        self,
        signal: dict[str, Any],
        df: pl.DataFrame,
        params: dict[str, Any],
        flags: list[str],
    ) -> float:
        """Score 0-100 based on TP/SL ratio quality.

        If signal lacks explicit SL/TP, estimates them using ATR.
        """
        price = signal.get("price", float(df["close"].to_numpy()[-1]))
        sl = signal.get("sl")
        tp = signal.get("tp")
        side = signal.get("action", "long")

        # Estimate SL/TP from ATR if not provided
        if sl is None or tp is None:
            closes = df["close"].to_numpy(zero_copy_only=False).astype(np.float64)
            highs = df["high"].to_numpy(zero_copy_only=False).astype(np.float64)
            lows = df["low"].to_numpy(zero_copy_only=False).astype(np.float64)
            atr_period = min(params["atr_period"], len(closes) - 1)
            if atr_period >= 2:
                atr = self._compute_atr(highs, lows, closes, atr_period)
                current_atr = float(atr[-1])
            else:
                current_atr = abs(float(closes[-1]) - float(closes[-2])) if len(closes) >= 2 else price * 0.01

            if sl is None:
                if side == "long":
                    sl = price - 1.5 * current_atr
                else:
                    sl = price + 1.5 * current_atr
                flags.append("sl_estimated_from_atr")

            if tp is None:
                if side == "long":
                    tp = price + 2.5 * current_atr
                else:
                    tp = price - 2.5 * current_atr
                flags.append("tp_estimated_from_atr")

        # Calculate risk/reward ratio
        if side == "long":
            risk = abs(price - sl) if sl is not None else 1e-12
            reward = abs(tp - price) if tp is not None else 1e-12
        else:
            risk = abs(sl - price) if sl is not None else 1e-12
            reward = abs(price - tp) if tp is not None else 1e-12

        if risk <= 0:
            flags.append("zero_risk_distance")
            return 0.0

        rr_ratio = reward / risk

        # Score mapping: 2:1 = 100, 1.5:1 = 70, 1:1 = 40, < 1:1 = 0-40
        if rr_ratio >= 3.0:
            score = 100.0
        elif rr_ratio >= 2.0:
            score = 80.0 + (rr_ratio - 2.0) * 20.0  # 80-100
        elif rr_ratio >= 1.5:
            score = 60.0 + (rr_ratio - 1.5) * 40.0  # 60-80
        elif rr_ratio >= 1.0:
            score = 30.0 + (rr_ratio - 1.0) * 60.0  # 30-60
        elif rr_ratio >= 0.5:
            score = rr_ratio * 60.0  # 0-30
        else:
            flags.append("terrible_risk_reward")
            score = 0.0

        return float(np.clip(score, 0.0, 100.0))

    # ------------------------------------------------------------------
    # Technical indicator helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_rsi(closes: np.ndarray, period: int = 14) -> np.ndarray:
        """Compute RSI using exponential moving average of gains/losses.

        Parameters
        ----------
        closes : np.ndarray
            1-D array of close prices.
        period : int
            RSI look-back period (default 14).

        Returns
        -------
        np.ndarray
            RSI values; first ``period`` elements are NaN.
        """
        if len(closes) < 2:
            return np.full(len(closes), np.nan)
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)

        rsi = np.full(len(closes), np.nan)
        if len(gains) < period:
            return rsi

        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])

        # Fill the first valid RSI at index=period BEFORE the loop mutates avg_gain/avg_loss
        if avg_loss == 0:
            rsi[period] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[period] = 100.0 - (100.0 / (1.0 + rs))

        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period

            if avg_loss == 0:
                rsi[i + 1] = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi[i + 1] = 100.0 - (100.0 / (1.0 + rs))

        return rsi

    @staticmethod
    def _compute_macd(
        closes: np.ndarray,
        fast: int = 12,
        slow: int = 26,
        signal_period: int = 9,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Compute MACD line, signal line, and histogram.

        Parameters
        ----------
        closes : np.ndarray
            1-D array of close prices.
        fast : int
            Fast EMA period (default 12).
        slow : int
            Slow EMA period (default 26).
        signal_period : int
            Signal line EMA period (default 9).

        Returns
        -------
        tuple[np.ndarray, np.ndarray, np.ndarray]
            ``(macd_line, signal_line, histogram)``
        """
        ema_fast = SignalConfidenceScorer._ema(closes, fast)
        ema_slow = SignalConfidenceScorer._ema(closes, slow)
        macd_line = ema_fast - ema_slow
        signal_line = SignalConfidenceScorer._ema(macd_line, signal_period)
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram

    @staticmethod
    def _ema(data: np.ndarray, period: int) -> np.ndarray:
        """Exponential moving average."""
        if len(data) == 0:
            return np.array([], dtype=np.float64)
        alpha = 2.0 / (period + 1)
        out = np.empty_like(data, dtype=np.float64)
        out[0] = data[0]
        for i in range(1, len(data)):
            out[i] = alpha * data[i] + (1.0 - alpha) * out[i - 1]
        return out

    @staticmethod
    def _sma(data: np.ndarray, period: int) -> np.ndarray:
        """Simple moving average (last ``period`` values only filled)."""
        n = len(data)
        if n < period or period < 1:
            return np.full(n, np.nan, dtype=np.float64)
        cumsum = np.empty(n + 1, dtype=np.float64)
        cumsum[0] = 0.0
        np.cumsum(data, out=cumsum[1:])
        sma = np.full(n, np.nan, dtype=np.float64)
        sma[period - 1:] = (cumsum[period:] - cumsum[:n - period + 1]) / period
        return sma

    @staticmethod
    def _rolling_std(data: np.ndarray, period: int) -> np.ndarray:
        """Rolling standard deviation."""
        if len(data) == 0:
            return np.array([], dtype=np.float64)
        out = np.full_like(data, np.nan, dtype=np.float64)
        for i in range(period - 1, len(data)):
            out[i] = np.std(data[i - period + 1 : i + 1], ddof=0)
        return out

    @staticmethod
    def _compute_atr(
        highs: np.ndarray,
        lows: np.ndarray,
        closes: np.ndarray,
        period: int,
    ) -> np.ndarray:
        """Average True Range via Wilder smoothing."""
        n = len(closes)
        if n == 0:
            return np.array([], dtype=np.float64)
        tr = np.empty(n, dtype=np.float64)
        tr[0] = highs[0] - lows[0]
        for i in range(1, n):
            tr[i] = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )

        atr = np.empty(n, dtype=np.float64)
        atr[:period] = np.nan
        atr[period - 1] = np.mean(tr[:period])
        for i in range(period, n):
            atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period
        return atr
