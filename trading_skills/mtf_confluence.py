"""
Multi-Timeframe Confluence Filter
==================================

Production-grade multi-timeframe analysis engine for quantitative crypto trading.
Evaluates trend bias, momentum, market structure, and key levels across multiple
timeframes to gate incoming trade signals via confluence agreement.

Integrates with:
    - Polars DataFrames with schema: open_time, open, high, low, close, volume
    - Signal format: {"action": "long"|"short", "confidence": 0-1.0, "reason": str, "price": float}
    - Async data fetcher: async def fetch(symbol, interval, days) -> pl.DataFrame

No TA-Lib dependency. Pure numpy/polars implementation.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Awaitable, Optional

import numpy as np
import polars as pl

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_TIMEFRAMES = ("15m", "1h", "4h")

_TF_WEIGHT_MAP: dict[str, float] = {
    "1m": 0.05,
    "5m": 0.10,
    "15m": 0.20,
    "30m": 0.30,
    "1h": 0.45,
    "2h": 0.55,
    "4h": 0.70,
    "8h": 0.80,
    "12h": 0.85,
    "1d": 1.00,
}

_TF_LOOKBACK_DAYS: dict[str, int] = {
    "1m": 1,
    "5m": 3,
    "15m": 7,
    "30m": 14,
    "1h": 30,
    "2h": 45,
    "4h": 60,
    "8h": 90,
    "12h": 120,
    "1d": 180,
}

_MA_FAST = 5
_MA_MID = 21
_MA_SLOW = 55

_RSI_PERIOD = 14
_MACD_FAST = 12
_MACD_SLOW = 26
_MACD_SIGNAL = 9

_SWING_LOOKBACK = 5  # bars each side for swing detection
_PIVOT_PERIOD = 20   # bars for pivot-point level calculation


# ---------------------------------------------------------------------------
# Helpers – pure numpy, no TA-Lib
# ---------------------------------------------------------------------------

def _ema(values: np.ndarray, period: int) -> np.ndarray:
    """Exponential moving average via recursive formula. Returns array of same length (NaN-padded)."""
    if len(values) == 0:
        return np.array([], dtype=np.float64)
    out = np.full_like(values, np.nan, dtype=np.float64)
    if len(values) < period:
        return out
    k = 2.0 / (period + 1)
    out[period - 1] = np.mean(values[:period])
    for i in range(period, len(values)):
        out[i] = values[i] * k + out[i - 1] * (1.0 - k)
    return out


def _sma(values: np.ndarray, period: int) -> np.ndarray:
    """Simple moving average. Returns array of same length (NaN-padded)."""
    if len(values) == 0:
        return np.array([], dtype=np.float64)
    out = np.full_like(values, np.nan, dtype=np.float64)
    if len(values) < period:
        return out
    cs = np.cumsum(values)
    padded = np.concatenate([[0.0], cs])  # length n+1
    out[period - 1:] = (cs[period - 1:] - padded[:len(values) - period + 1]) / period
    return out


def _rsi(close: np.ndarray, period: int = _RSI_PERIOD) -> np.ndarray:
    """Wilder-smoothed RSI. Returns array of same length (NaN-padded)."""
    if len(close) == 0:
        return np.array([], dtype=np.float64)
    out = np.full_like(close, np.nan, dtype=np.float64)
    if len(close) < period + 1:
        return out
    delta = np.diff(close)
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)

    avg_gain = np.mean(gain[:period])
    avg_loss = np.mean(loss[:period])

    # Fill the first valid RSI value BEFORE the loop mutates avg_gain/avg_loss
    if avg_loss == 0.0:
        out[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        out[period] = 100.0 - 100.0 / (1.0 + rs)

    for i in range(period, len(delta)):
        avg_gain = (avg_gain * (period - 1) + gain[i]) / period
        avg_loss = (avg_loss * (period - 1) + loss[i]) / period
        if avg_loss == 0.0:
            out[i + 1] = 100.0
        else:
            rs = avg_gain / avg_loss
            out[i + 1] = 100.0 - 100.0 / (1.0 + rs)
    return out


def _macd(
    close: np.ndarray,
    fast: int = _MACD_FAST,
    slow: int = _MACD_SLOW,
    signal: int = _MACD_SIGNAL,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """MACD line, signal line, histogram. All NaN-padded to input length."""
    ema_fast = _ema(close, fast)
    ema_slow = _ema(close, slow)
    macd_line = ema_fast - ema_slow
    signal_line = _ema(macd_line, signal)
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def _vwap(high: np.ndarray, low: np.ndarray, close: np.ndarray, volume: np.ndarray) -> np.ndarray:
    """Cumulative VWAP over the entire array."""
    typical = (high + low + close) / 3.0
    cum_vol = np.cumsum(volume)
    cum_tp_vol = np.cumsum(typical * volume)
    with np.errstate(divide="ignore", invalid="ignore"):
        vwap = np.where(cum_vol > 0, cum_tp_vol / cum_vol, np.nan)
    return vwap


def _floor_pivots(
    high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = _PIVOT_PERIOD
) -> dict[str, float]:
    """
    Standard floor pivot points from the last *period* bars.

    PP = (H + L + C) / 3
    R1 = 2*PP - L,  S1 = 2*PP - H
    R2 = PP + (H - L), S2 = PP - (H - L)
    """
    if len(high) == 0 or len(low) == 0 or len(close) == 0:
        return {"PP": 0.0, "R1": 0.0, "S1": 0.0, "R2": 0.0, "S2": 0.0}
    h = np.nanmax(high[-period:])
    l = np.nanmin(low[-period:])
    c = close[-1]
    if not (np.isfinite(h) and np.isfinite(l) and np.isfinite(c)):
        return {"PP": 0.0, "R1": 0.0, "S1": 0.0, "R2": 0.0, "S2": 0.0}
    pp = (h + l + c) / 3.0
    return {
        "PP": pp,
        "R1": 2.0 * pp - l,
        "S1": 2.0 * pp - h,
        "R2": pp + (h - l),
        "S2": pp - (h - l),
    }


def _detect_swings(
    high: np.ndarray, low: np.ndarray, lookback: int = _SWING_LOOKBACK
) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
    """
    Detect swing highs and swing lows.

    A swing high at index *i* requires high[i] >= all highs in [i-lookback, i+lookback].
    Returns (swing_highs, swing_lows) as lists of (index, price).
    """
    swing_highs: list[tuple[int, float]] = []
    swing_lows: list[tuple[int, float]] = []
    n = len(high)
    for i in range(lookback, n - lookback):
        window_h = high[max(0, i - lookback): i + lookback + 1]
        if high[i] == np.nanmax(window_h):
            swing_highs.append((i, float(high[i])))
        window_l = low[max(0, i - lookback): i + lookback + 1]
        if low[i] == np.nanmin(window_l):
            swing_lows.append((i, float(low[i])))
    return swing_highs, swing_lows


# ---------------------------------------------------------------------------
# Core class
# ---------------------------------------------------------------------------

class MultiTimeframeConfluence:
    """
    Multi-timeframe confluence filter that gates trade signals based on
    agreement of trend bias, momentum, and market structure across configurable
    timeframes.

    Parameters
    ----------
    timeframes : tuple[str, ...]
        Ordered list of timeframes to analyse (e.g. ``("15m", "1h", "4h")``).
    min_agreement : int
        Minimum number of timeframes that must agree with the signal direction
        for the signal to pass.
    weights : dict[str, float] | None
        Per-timeframe weight overrides.  Falls back to ``_TF_WEIGHT_MAP``.
    """

    def __init__(
        self,
        timeframes: tuple[str, ...] = _DEFAULT_TIMEFRAMES,
        min_agreement: int = 2,
        weights: Optional[dict[str, float]] = None,
    ) -> None:
        self.timeframes = timeframes
        self.min_agreement = min_agreement
        self.weights: dict[str, float] = weights or {
            tf: _TF_WEIGHT_MAP.get(tf, 0.5) for tf in timeframes
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def analyze(
        self,
        symbol: str,
        signal: dict[str, Any],
        data_fetcher: Optional[Callable[..., Awaitable[pl.DataFrame]]] = None,
    ) -> dict[str, Any]:
        """
        Fetch data for each timeframe and run full confluence analysis.

        Parameters
        ----------
        symbol : str
            Trading pair, e.g. ``"BTCUSDT"``.
        signal : dict
            Incoming signal with keys ``action``, ``confidence``, ``reason``, ``price``.
        data_fetcher : async callable, optional
            ``async def fetch(symbol, interval, days) -> pl.DataFrame``.
            Required when calling this method (use ``analyze_with_data`` otherwise).

        Returns
        -------
        dict
            Confluence analysis result (see module docstring for schema).

        Raises
        ------
        ValueError
            If *data_fetcher* is ``None``.
        """
        if data_fetcher is None:
            raise ValueError(
                "data_fetcher is required for analyze(). "
                "Use analyze_with_data() to pass pre-loaded DataFrames."
            )

        tasks = []
        for tf in self.timeframes:
            days = _TF_LOOKBACK_DAYS.get(tf, 30)
            tasks.append(data_fetcher(symbol, tf, days))

        raw = await asyncio.gather(*tasks, return_exceptions=True)
        dfs: dict[str, pl.DataFrame] = {}
        for tf, result in zip(self.timeframes, raw):
            if isinstance(result, Exception):
                logger.warning("Failed to fetch %s data for %s: %s", tf, symbol, result)
                continue
            dfs[tf] = result
        if not dfs:
            return {"confluence_score": 0.0, "reason": "all timeframe fetches failed"}
        return self.analyze_with_data(dfs, signal)

    def analyze_with_data(
        self,
        dfs: dict[str, pl.DataFrame],
        signal: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Run confluence analysis with pre-loaded DataFrames.

        Parameters
        ----------
        dfs : dict[str, pl.DataFrame]
            Mapping of timeframe label to OHLCV DataFrame.
        signal : dict
            Incoming trade signal.

        Returns
        -------
        dict
            Full confluence result.
        """
        action = signal.get("action", "").lower()
        base_confidence = float(signal.get("confidence", 0.0))
        price = float(signal.get("price", 0.0))

        tf_details: dict[str, dict[str, Any]] = {}
        agreement_count = 0
        weighted_score = 0.0
        total_weight = 0.0
        flags: list[str] = []

        for tf in self.timeframes:
            df = dfs.get(tf)
            if df is None or df.is_empty():
                logger.warning("No data for timeframe %s – skipping", tf)
                continue

            detail = self._analyze_single_tf(df, tf)
            tf_details[tf] = detail
            weight = self.weights.get(tf, 0.5)
            total_weight += weight

            # Determine agreement
            agrees = self._direction_agrees(action, detail["bias"])
            if agrees:
                agreement_count += 1
                weighted_score += weight
            elif detail["bias"] != "neutral":
                # Active disagreement – penalise proportionally
                weighted_score -= weight * 0.5

            # Key-level proximity flags (cap to prevent unbounded growth)
            if len(flags) < 50:
                self._check_key_level_flags(tf, detail, price, flags)

        total_timeframes = len(tf_details)
        if total_timeframes == 0:
            return self._empty_result(signal, "No timeframe data available")

        # Normalise weighted score to 0-1
        confluence_score = max(0.0, min(1.0, (weighted_score / total_weight + 1.0) / 2.0)) if total_weight > 0 else 0.0

        passed = agreement_count >= self.min_agreement
        boosted_confidence = base_confidence
        if passed and total_timeframes > 0:
            boost = (agreement_count / total_timeframes) * 0.2
            boosted_confidence = min(1.0, base_confidence + boost)

        message = self._build_message(action, agreement_count, total_timeframes, passed, flags)

        return {
            "passed": passed,
            "agreement_count": agreement_count,
            "total_timeframes": total_timeframes,
            "confluence_score": round(confluence_score, 4),
            "timeframe_details": tf_details,
            "boosted_confidence": round(boosted_confidence, 4),
            "flags": flags,
            "message": message,
        }

    def gate_signal(
        self,
        signal: dict[str, Any],
        analysis: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        """
        Gate and optionally modify a signal based on confluence analysis.

        Returns
        -------
        dict or None
            The (possibly modified) signal if confluence passes, otherwise ``None``.
        """
        if not analysis.get("passed", False):
            logger.info(
                "Signal blocked by MTF confluence: %s (%s)",
                signal.get("action"),
                analysis.get("message", ""),
            )
            return None

        gated = dict(signal)
        gated["confidence"] = analysis["boosted_confidence"]
        gated["mtf_confluence"] = {
            "score": analysis["confluence_score"],
            "agreement": f"{analysis['agreement_count']}/{analysis['total_timeframes']}",
            "flags": analysis["flags"],
        }
        return gated

    # ------------------------------------------------------------------
    # Per-timeframe analysis
    # ------------------------------------------------------------------

    def _analyze_single_tf(self, df: pl.DataFrame, tf: str) -> dict[str, Any]:
        """
        Analyse a single timeframe DataFrame.

        Returns dict with keys: bias, momentum, structure, key_levels.
        """
        required_cols = {"open", "high", "low", "close", "volume"}
        missing = required_cols - set(df.columns)
        if missing:
            logger.warning("TF %s missing columns %s — returning neutral", tf, missing)
            return {
                "bias": "neutral",
                "momentum": 0.0,
                "structure": "undefined",
                "key_levels": {"pivots": {}, "nearest_support": None, "nearest_resistance": None},
            }
        bias = self._compute_tf_bias(df)
        momentum = self._compute_tf_momentum(df)
        structure = self._compute_tf_structure(df)
        key_levels = self._compute_key_levels(df)

        return {
            "bias": bias,
            "momentum": round(momentum, 4),
            "structure": structure.get("pattern", "undefined"),
            "key_levels": key_levels,
        }

    def _compute_tf_bias(self, df: pl.DataFrame) -> str:
        """
        Determine trend bias from moving-average structure.

        - **bullish**: MA5 > MA21 > MA55
        - **bearish**: MA5 < MA21 < MA55
        - **neutral**: otherwise

        Returns
        -------
        str
            ``"bullish"``, ``"bearish"``, or ``"neutral"``.
        """
        close = df["close"].to_numpy().astype(np.float64)
        if len(close) < _MA_SLOW:
            return "neutral"

        ma_fast = _sma(close, _MA_FAST)
        ma_mid = _sma(close, _MA_MID)
        ma_slow = _sma(close, _MA_SLOW)

        f, m, s = ma_fast[-1], ma_mid[-1], ma_slow[-1]
        if np.isnan(f) or np.isnan(m) or np.isnan(s):
            return "neutral"

        if f > m > s:
            return "bullish"
        if f < m < s:
            return "bearish"
        return "neutral"

    def _compute_tf_momentum(self, df: pl.DataFrame) -> float:
        """
        Composite momentum score in [-1, +1].

        Components (equally weighted):
            1. RSI zone: >60 → +1, <40 → -1, else linear interpolation
            2. MACD histogram sign: >0 → +1, <0 → -1
            3. Price vs VWAP: above → +1, below → -1

        Returns
        -------
        float
            Momentum score in ``[-1.0, +1.0]``.
        """
        close = df["close"].to_numpy().astype(np.float64)
        high = df["high"].to_numpy().astype(np.float64)
        low = df["low"].to_numpy().astype(np.float64)
        volume = df["volume"].to_numpy().astype(np.float64)

        scores: list[float] = []

        # 1. RSI
        rsi_arr = _rsi(close)
        rsi_val = rsi_arr[-1] if not np.isnan(rsi_arr[-1]) else 50.0
        if rsi_val >= 60.0:
            scores.append(1.0)
        elif rsi_val <= 40.0:
            scores.append(-1.0)
        else:
            scores.append((rsi_val - 50.0) / 10.0)  # linear: 40→-1, 50→0, 60→+1

        # 2. MACD histogram
        _, _, hist = _macd(close)
        hist_val = hist[-1] if not np.isnan(hist[-1]) else 0.0
        scores.append(1.0 if hist_val > 0 else (-1.0 if hist_val < 0 else 0.0))

        # 3. Price vs VWAP
        vwap_arr = _vwap(high, low, close, volume)
        vwap_val = vwap_arr[-1] if not np.isnan(vwap_arr[-1]) else close[-1]
        scores.append(1.0 if close[-1] > vwap_val else (-1.0 if close[-1] < vwap_val else 0.0))

        return float(np.mean(scores))

    def _compute_tf_structure(self, df: pl.DataFrame) -> dict[str, Any]:
        """
        Determine market structure from swing highs/lows.

        Patterns:
            - ``"uptrend"``: most recent swings show Higher High + Higher Low (HH/HL)
            - ``"downtrend"``: most recent swings show Lower High + Lower Low (LH/LL)
            - ``"range"``: mixed or insufficient data

        Returns
        -------
        dict
            ``{"pattern": str, "swing_highs": int, "swing_lows": int, "detail": str}``
        """
        high = df["high"].to_numpy().astype(np.float64)
        low = df["low"].to_numpy().astype(np.float64)

        swing_highs, swing_lows = _detect_swings(high, low)

        if len(swing_highs) < 2 or len(swing_lows) < 2:
            return {
                "pattern": "range",
                "swing_highs": len(swing_highs),
                "swing_lows": len(swing_lows),
                "detail": "insufficient swings",
            }

        # Compare last two swing highs and lows
        sh1, sh2 = swing_highs[-2][1], swing_highs[-1][1]
        sl1, sl2 = swing_lows[-2][1], swing_lows[-1][1]

        hh = sh2 > sh1  # Higher High
        hl = sl2 > sl1  # Higher Low
        lh = sh2 < sh1  # Lower High
        ll = sl2 < sl1  # Lower Low

        if hh and hl:
            pattern = "uptrend"
            detail = "HH/HL"
        elif lh and ll:
            pattern = "downtrend"
            detail = "LH/LL"
        elif hh and ll:
            pattern = "range"
            detail = "HH/LL (expanding)"
        elif lh and hl:
            pattern = "range"
            detail = "LH/HL (contracting)"
        else:
            pattern = "range"
            detail = "mixed"

        return {
            "pattern": pattern,
            "swing_highs": len(swing_highs),
            "swing_lows": len(swing_lows),
            "detail": detail,
        }

    def _compute_key_levels(self, df: pl.DataFrame) -> dict[str, Any]:
        """
        Compute key support/resistance levels from floor pivot points and
        recent swing extremes.

        Returns
        -------
        dict
            ``{"pivots": {PP, R1, R2, S1, S2}, "nearest_support": float, "nearest_resistance": float}``
        """
        high = df["high"].to_numpy().astype(np.float64)
        low = df["low"].to_numpy().astype(np.float64)
        close = df["close"].to_numpy().astype(np.float64)

        pivots = _floor_pivots(high, low, close)
        current_price = close[-1]

        # Collect all candidate levels
        levels = sorted(pivots.values())

        # Add recent swing levels
        swing_highs, swing_lows = _detect_swings(high, low)
        for _, price in swing_highs[-5:]:
            levels.append(price)
        for _, price in swing_lows[-5:]:
            levels.append(price)

        levels = sorted(set(levels))

        # Find nearest support (below price) and resistance (above price)
        supports = [lv for lv in levels if lv < current_price]
        resistances = [lv for lv in levels if lv > current_price]

        nearest_support = supports[-1] if supports else float("nan")
        nearest_resistance = resistances[0] if resistances else float("nan")

        return {
            "pivots": {k: round(v, 6) for k, v in pivots.items()},
            "nearest_support": round(nearest_support, 6) if not np.isnan(nearest_support) else None,
            "nearest_resistance": round(nearest_resistance, 6) if not np.isnan(nearest_resistance) else None,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _direction_agrees(action: str, bias: str) -> bool:
        """Check whether a timeframe bias agrees with the signal direction."""
        if bias == "neutral":
            return False
        if action == "long" and bias == "bullish":
            return True
        if action == "short" and bias == "bearish":
            return True
        return False

    @staticmethod
    def _check_key_level_flags(
        tf: str,
        detail: dict[str, Any],
        price: float,
        flags: list[str],
    ) -> None:
        """Append flags when price is near a key level on this timeframe."""
        levels = detail.get("key_levels", {})
        support = levels.get("nearest_support")
        resistance = levels.get("nearest_resistance")
        threshold = 0.005  # 0.5% proximity

        if support is not None and price > 0:
            pct_from_support = abs(price - support) / price
            if pct_from_support <= threshold:
                flags.append(f"near_support_{tf} ({support:.2f})")

        if resistance is not None and price > 0:
            pct_from_resistance = abs(price - resistance) / price
            if pct_from_resistance <= threshold:
                flags.append(f"near_resistance_{tf} ({resistance:.2f})")

    @staticmethod
    def _build_message(
        action: str, agreement: int, total: int, passed: bool, flags: list[str]
    ) -> str:
        """Build a human-readable summary message."""
        status = "PASS" if passed else "BLOCKED"
        parts = [f"MTF Confluence {status}: {agreement}/{total} timeframes agree with {action}"]
        if flags:
            parts.append(f"Flags: {', '.join(flags)}")
        return " | ".join(parts)

    @staticmethod
    def _empty_result(signal: dict[str, Any], message: str) -> dict[str, Any]:
        """Return a blocked result when no data is available."""
        return {
            "passed": False,
            "agreement_count": 0,
            "total_timeframes": 0,
            "confluence_score": 0.0,
            "timeframe_details": {},
            "boosted_confidence": float(signal.get("confidence", 0.0)),
            "flags": [],
            "message": message,
        }
