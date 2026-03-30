"""
Market Regime Detector — production-grade market state classifier.

Classifies crypto markets into one of six regimes using ADX, ATR-based
volatility percentiles, Hurst exponent (simplified R/S), and moving-average
slope variance.  Provides a gate function that modifies or blocks trade
signals based on the detected regime.

Dependencies: polars, numpy (no TA-Lib).
Expected DataFrame columns: open_time, open, high, low, close, volume
"""

from __future__ import annotations

import enum
import logging
from typing import Any, Dict, List, Optional

import numpy as np
import polars as pl

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Regime enum
# ---------------------------------------------------------------------------


class MarketRegime(enum.Enum):
    """Six mutually-exclusive market regimes."""

    TRENDING_UP = "trending_up"
    TRENDING_DOWN = "trending_down"
    RANGING = "ranging"
    VOLATILE = "volatile"
    MEAN_REVERTING = "mean_reverting"
    BREAKOUT = "breakout"


# ---------------------------------------------------------------------------
# Detector
# ---------------------------------------------------------------------------


class MarketRegimeDetector:
    """Stateless regime detector operating on OHLCV polars DataFrames.

    Parameters
    ----------
    atr_period : int
        Lookback for Average True Range (default 14).
    adx_period : int
        Smoothing period for ADX / DI+/- (default 14).
    vol_lookback : int
        Window for volatility percentile ranking (default 100).
    slope_period : int
        Bars used to compute linear-regression slope of MAs (default 10).
    """

    # Minimum rows required *before* any indicator can be computed.
    _MIN_ROWS = 60

    def __init__(
        self,
        atr_period: int = 14,
        adx_period: int = 14,
        vol_lookback: int = 100,
        slope_period: int = 10,
    ) -> None:
        self.atr_period = atr_period
        self.adx_period = adx_period
        self.vol_lookback = vol_lookback
        self.slope_period = slope_period

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def detect(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Detect the current market regime.

        Returns
        -------
        dict with keys:
            regime        : MarketRegime
            adx           : float
            di_plus       : float
            di_minus      : float
            vol_pct       : float   (0-100 percentile)
            hurst         : float
            slope_var     : float
            volatility_regime : "low" | "normal" | "high"
            description   : str     (human-readable summary)
        """
        required_cols = {"high", "low", "close", "volume"}
        missing = required_cols - set(df.columns)
        if missing:
            logger.warning("DataFrame missing columns %s — returning default.", missing)
            return self._default_result()

        n = len(df)
        if n < self._MIN_ROWS:
            logger.warning(
                "Insufficient data for regime detection (%d rows, need %d). "
                "Returning RANGING as default.",
                n,
                self._MIN_ROWS,
            )
            return self._default_result()

        # --- Indicator computation ---
        adx, di_plus, di_minus = self._compute_adx(df)
        vol_pct = self._compute_volatility_percentile(df)
        slope_var = self._compute_ma_slope_variance(df)
        hurst = self._compute_hurst_exponent(df)

        # --- Previous-bar ADX for breakout detection ---
        # We need at least two valid ADX values; fall back gracefully.
        prev_adx = self._compute_adx_series(df)
        adx_rising_from_low = False
        if prev_adx is not None and len(prev_adx) >= 2:
            adx_rising_from_low = (prev_adx[-2] < 20.0) and (prev_adx[-1] >= 25.0)

        # --- Classification hierarchy (order matters) ---
        regime = self._classify(
            adx, di_plus, di_minus, vol_pct, hurst, slope_var, adx_rising_from_low
        )

        volatility_regime = self._vol_regime_label(vol_pct)

        return {
            "regime": regime,
            "adx": round(float(adx), 4),
            "di_plus": round(float(di_plus), 4),
            "di_minus": round(float(di_minus), 4),
            "vol_pct": round(float(vol_pct), 2),
            "hurst": round(float(hurst), 4),
            "slope_var": round(float(slope_var), 8),
            "volatility_regime": volatility_regime,
            "description": self._describe(regime, adx, vol_pct, hurst),
        }

    def gate_signal(
        self, signal: Dict[str, Any], df: pl.DataFrame
    ) -> Optional[Dict[str, Any]]:
        """Gate / modify a trade signal based on the current regime.

        Parameters
        ----------
        signal : dict
            Must contain at least ``action`` and ``confidence``.
        df : pl.DataFrame
            OHLCV data used for regime detection.

        Returns
        -------
        dict or None
            Modified signal, or ``None`` if the signal should be blocked.
        """
        if signal is None:
            return None

        info = self.detect(df)
        regime: MarketRegime = info["regime"]

        # Attach regime metadata to every signal that passes.
        signal = {**signal, "volatility_regime": info["volatility_regime"]}

        action = signal.get("action", "")
        confidence = float(signal.get("confidence", 0.5))

        # --- TRENDING_UP / TRENDING_DOWN ---
        if regime in (MarketRegime.TRENDING_UP, MarketRegime.TRENDING_DOWN):
            trend_dir = "long" if regime == MarketRegime.TRENDING_UP else "short"
            if action == trend_dir:
                # Trend-following: boost confidence.
                signal["confidence"] = min(1.0, round(confidence + 0.1, 4))
                signal["reason"] = (
                    signal.get("reason", "")
                    + f" [regime: {regime.value}, confidence boosted +0.1]"
                )
                return signal
            if action == "close":
                return signal  # always allow closing
            # Counter-trend signal in a trending market — block.
            logger.info(
                "Blocking counter-trend %s signal in %s regime.", action, regime.value
            )
            return None

        # --- RANGING ---
        if regime == MarketRegime.RANGING:
            # Only allow mean-reversion style trades (counter-trend) and closes.
            if action == "close":
                return signal
            # Heuristic: if the signal reason mentions "mean-rev" or confidence
            # is moderate, assume mean-reversion.  Otherwise block.
            reason_lower = signal.get("reason", "").lower()
            is_mean_reversion = any(
                kw in reason_lower
                for kw in ("mean", "revert", "reversion", "oversold", "overbought", "rsi", "bb")
            )
            if is_mean_reversion:
                signal["reason"] = (
                    signal.get("reason", "") + " [regime: ranging, mean-reversion OK]"
                )
                return signal
            logger.info(
                "Blocking trend signal '%s' in RANGING regime (no mean-reversion keywords).",
                action,
            )
            return None

        # --- VOLATILE ---
        if regime == MarketRegime.VOLATILE:
            signal["confidence"] = round(confidence * 0.75, 4)
            signal["reason"] = (
                signal.get("reason", "")
                + " [regime: volatile, confidence reduced 25%, use smaller size]"
            )
            signal.setdefault("warnings", [])
            if isinstance(signal["warnings"], list):
                signal["warnings"].append(
                    "High volatility regime — consider reducing position size by 50%."
                )
            return signal

        # --- MEAN_REVERTING ---
        if regime == MarketRegime.MEAN_REVERTING:
            if action == "close":
                return signal
            # In mean-reverting markets, trend-following is risky.
            # Allow the signal but add a note.
            signal["reason"] = (
                signal.get("reason", "")
                + " [regime: mean_reverting, tighten stops]"
            )
            return signal

        # --- BREAKOUT ---
        if regime == MarketRegime.BREAKOUT:
            # Allow but require volume confirmation.
            if "volume" not in df.columns:
                return signal
            vol_series = df["volume"].to_numpy().astype(np.float64)
            if len(vol_series) >= 20 and not np.all(np.isnan(vol_series[-20:])):
                recent_vol = vol_series[-1]
                avg_vol = np.nanmean(vol_series[-20:])
                if avg_vol > 0 and recent_vol < avg_vol * 1.2:
                    signal["reason"] = (
                        signal.get("reason", "")
                        + " [regime: breakout, WARNING: volume not confirmed]"
                    )
                    signal["confidence"] = round(confidence * 0.8, 4)
                else:
                    signal["reason"] = (
                        signal.get("reason", "")
                        + " [regime: breakout, volume confirmed]"
                    )
                    signal["confidence"] = min(1.0, round(confidence + 0.05, 4))
            return signal

        # Fallback — pass through unchanged.
        return signal

    # ------------------------------------------------------------------
    # Internal: ADX / DI+/- calculation (Wilder smoothing)
    # ------------------------------------------------------------------

    def _compute_adx(self, df: pl.DataFrame) -> tuple[float, float, float]:
        """Return the latest (ADX, DI+, DI-) values.

        Uses Wilder's smoothing (exponential with alpha = 1/period).
        """
        high, low, close = self._hlc_arrays(df)
        n = len(close)
        period = self.adx_period

        if n < period + 1:
            return (0.0, 0.0, 0.0)

        # True Range
        tr = self._true_range(high, low, close)

        # Directional Movement
        up_move = np.diff(high)  # length n-1
        down_move = -np.diff(low)

        dm_plus = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        dm_minus = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

        # Wilder smoothing (first value = SMA, then EMA with alpha=1/period)
        alpha = 1.0 / period

        atr = self._wilder_smooth(tr, period, alpha)
        smooth_dm_plus = self._wilder_smooth(dm_plus, period, alpha)
        smooth_dm_minus = self._wilder_smooth(dm_minus, period, alpha)

        # Avoid division by zero
        safe_atr = np.where(atr == 0, 1e-10, atr)

        di_plus_arr = 100.0 * smooth_dm_plus / safe_atr
        di_minus_arr = 100.0 * smooth_dm_minus / safe_atr

        di_sum = di_plus_arr + di_minus_arr
        safe_di_sum = np.where(di_sum == 0, 1e-10, di_sum)
        dx = 100.0 * np.abs(di_plus_arr - di_minus_arr) / safe_di_sum

        # ADX = Wilder-smoothed DX
        adx_arr = self._wilder_smooth(dx, period, alpha)

        return (
            float(adx_arr[-1]),
            float(di_plus_arr[-1]),
            float(di_minus_arr[-1]),
        )

    def _compute_adx_series(self, df: pl.DataFrame) -> Optional[np.ndarray]:
        """Return the full ADX array (for breakout detection).

        Returns None if data is insufficient.
        """
        high, low, close = self._hlc_arrays(df)
        n = len(close)
        period = self.adx_period

        if n < period + 1:
            return None

        tr = self._true_range(high, low, close)
        up_move = np.diff(high)
        down_move = -np.diff(low)

        dm_plus = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        dm_minus = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

        alpha = 1.0 / period
        atr = self._wilder_smooth(tr, period, alpha)
        smooth_dm_plus = self._wilder_smooth(dm_plus, period, alpha)
        smooth_dm_minus = self._wilder_smooth(dm_minus, period, alpha)

        safe_atr = np.where(atr == 0, 1e-10, atr)
        di_plus_arr = 100.0 * smooth_dm_plus / safe_atr
        di_minus_arr = 100.0 * smooth_dm_minus / safe_atr

        di_sum = di_plus_arr + di_minus_arr
        safe_di_sum = np.where(di_sum == 0, 1e-10, di_sum)
        dx = 100.0 * np.abs(di_plus_arr - di_minus_arr) / safe_di_sum

        adx_arr = self._wilder_smooth(dx, period, alpha)
        return adx_arr

    # ------------------------------------------------------------------
    # Internal: Volatility percentile
    # ------------------------------------------------------------------

    def _compute_volatility_percentile(self, df: pl.DataFrame) -> float:
        """ATR as percentage of price, then percentile-ranked over lookback.

        Returns a value in [0, 100].
        """
        high, low, close = self._hlc_arrays(df)
        n = len(close)

        tr = self._true_range(high, low, close)

        period = self.atr_period
        if len(tr) < period:
            return 50.0  # neutral default

        # Rolling ATR via simple moving average of TR
        atr_series = np.empty(len(tr) - period + 1)
        for i in range(len(atr_series)):
            atr_series[i] = np.mean(tr[i : i + period])

        # Normalise by close price to get ATR% (use close aligned with atr)
        # atr_series[i] corresponds to close[period - 1 + i] in the diff'd array.
        # The TR array has length n-1 (from diff); ATR starts at index period-1 of TR,
        # which aligns with close index period.
        close_aligned = close[period:]  # same length as atr_series
        if len(close_aligned) != len(atr_series):
            # Edge alignment: trim to min length
            min_len = min(len(close_aligned), len(atr_series))
            close_aligned = close_aligned[:min_len]
            atr_series = atr_series[:min_len]

        safe_close = np.where(close_aligned == 0, 1e-10, close_aligned)
        atr_pct = atr_series / safe_close * 100.0

        if len(atr_pct) == 0:
            return 50.0

        # Percentile rank of latest value within the lookback window
        lookback = min(self.vol_lookback, len(atr_pct))
        window = atr_pct[-lookback:]
        current = atr_pct[-1]

        # Filter out NaN values before ranking
        valid_window = window[np.isfinite(window)]
        if len(valid_window) == 0 or not np.isfinite(current):
            return 50.0

        # Edge case: all values identical (e.g. zero volatility) -> neutral
        if np.max(valid_window) == np.min(valid_window):
            return 50.0

        pct_rank = float(np.sum(valid_window <= current)) / float(len(valid_window)) * 100.0
        return pct_rank

    # ------------------------------------------------------------------
    # Internal: MA slope variance
    # ------------------------------------------------------------------

    def _compute_ma_slope_variance(self, df: pl.DataFrame) -> float:
        """Variance of normalised slopes across MA5, MA8, EMA21, MA55.

        Low variance = MAs aligned (trending).  High variance = mixed signals.
        """
        close = df["close"].to_numpy().astype(np.float64)
        n = len(close)
        period = self.slope_period

        if n < 55 + period:
            return 0.0

        ma5 = self._sma(close, 5)
        ma8 = self._sma(close, 8)
        ema21 = self._ema(close, 21)
        ma55 = self._sma(close, 55)

        slopes: List[float] = []
        for ma in (ma5, ma8, ema21, ma55):
            if len(ma) < period:
                continue
            tail = ma[-period:]
            # Normalised linear-regression slope (per bar, % of mean)
            x = np.arange(period, dtype=np.float64)
            mean_y = np.mean(tail)
            if mean_y == 0:
                slopes.append(0.0)
                continue
            # OLS slope = cov(x,y)/var(x)
            slope = (np.mean(x * tail) - np.mean(x) * np.mean(tail)) / (
                np.var(x) + 1e-15
            )
            # Normalise to percentage-per-bar
            slopes.append(slope / mean_y)

        if len(slopes) < 2:
            return 0.0

        return float(np.var(slopes))

    # ------------------------------------------------------------------
    # Internal: Hurst exponent (simplified R/S analysis)
    # ------------------------------------------------------------------

    def _compute_hurst_exponent(
        self, df: pl.DataFrame, max_lag: int = 20
    ) -> float:
        """Simplified Hurst exponent via rescaled-range (R/S) analysis.

        H < 0.5 -> mean-reverting
        H ~ 0.5 -> random walk
        H > 0.5 -> trending / persistent

        Uses log-returns and sub-series of varying size.
        """
        close = df["close"].to_numpy().astype(np.float64)
        n = len(close)

        if n < max_lag * 2:
            return 0.5  # neutral

        # Log returns (suppress numpy warnings for edge cases)
        with np.errstate(divide="ignore", invalid="ignore"):
            safe_close = np.where(close <= 0, 1e-10, close)
            log_ret = np.diff(np.log(safe_close))

        lags: List[int] = []
        rs_values: List[float] = []

        for lag in range(2, max_lag + 1):
            # Split log_ret into non-overlapping sub-series of length `lag`
            num_subseries = len(log_ret) // lag
            if num_subseries < 1:
                continue

            rs_list: List[float] = []
            for i in range(num_subseries):
                subseries = log_ret[i * lag : (i + 1) * lag]
                mean_sub = np.mean(subseries)
                deviate = np.cumsum(subseries - mean_sub)
                r = float(np.max(deviate) - np.min(deviate))
                s = float(np.std(subseries, ddof=1))
                if s > 1e-15:
                    rs_list.append(r / s)

            if len(rs_list) > 0:
                lags.append(lag)
                rs_values.append(np.mean(rs_list))

        if len(lags) < 3:
            return 0.5

        # OLS on log(lag) vs log(R/S) -> slope is Hurst exponent
        log_lags = np.log(np.array(lags, dtype=np.float64))
        log_rs = np.log(np.array(rs_values, dtype=np.float64))

        # Filter out any NaN/Inf
        mask = np.isfinite(log_lags) & np.isfinite(log_rs)
        log_lags = log_lags[mask]
        log_rs = log_rs[mask]

        if len(log_lags) < 2:
            return 0.5

        slope = (np.mean(log_lags * log_rs) - np.mean(log_lags) * np.mean(log_rs)) / (
            np.var(log_lags) + 1e-15
        )

        # Clamp to [0, 1]
        return float(np.clip(slope, 0.0, 1.0))

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------

    def _classify(
        self,
        adx: float,
        di_plus: float,
        di_minus: float,
        vol_pct: float,
        hurst: float,
        slope_var: float,
        adx_rising_from_low: bool,
    ) -> MarketRegime:
        """Hierarchical classification with clear priority ordering."""

        # 1. BREAKOUT: vol expansion + ADX crossing from <20 to >=25
        if adx_rising_from_low and vol_pct > 60:
            return MarketRegime.BREAKOUT

        # 2. VOLATILE: extreme volatility dominates
        if vol_pct > 80:
            return MarketRegime.VOLATILE

        # 3. TRENDING: ADX strong + DI alignment
        if adx > 25:
            if di_plus > di_minus:
                return MarketRegime.TRENDING_UP
            else:
                return MarketRegime.TRENDING_DOWN

        # 4. MEAN_REVERTING: Hurst significantly below 0.5
        if hurst < 0.4:
            return MarketRegime.MEAN_REVERTING

        # 5. RANGING: weak ADX + low volatility
        if adx < 20 and vol_pct < 50:
            return MarketRegime.RANGING

        # 6. Default to RANGING for ambiguous states
        return MarketRegime.RANGING

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _hlc_arrays(df: pl.DataFrame) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Extract high/low/close as float64 numpy arrays, replacing NaN with forward-fill."""
        high = df["high"].to_numpy().astype(np.float64)
        low = df["low"].to_numpy().astype(np.float64)
        close = df["close"].to_numpy().astype(np.float64)
        # Replace NaN with previous value (forward fill); remaining leading NaN -> 0
        for arr in (high, low, close):
            mask = np.isnan(arr)
            if mask.any():
                for i in range(len(arr)):
                    if mask[i]:
                        arr[i] = arr[i - 1] if i > 0 else 0.0
        return high, low, close

    @staticmethod
    def _true_range(
        high: np.ndarray, low: np.ndarray, close: np.ndarray
    ) -> np.ndarray:
        """Compute True Range array (length n-1)."""
        prev_close = close[:-1]
        h = high[1:]
        l_ = low[1:]
        tr1 = h - l_
        tr2 = np.abs(h - prev_close)
        tr3 = np.abs(l_ - prev_close)
        return np.maximum(tr1, np.maximum(tr2, tr3))

    @staticmethod
    def _wilder_smooth(
        data: np.ndarray, period: int, alpha: float
    ) -> np.ndarray:
        """Wilder-style smoothing: first value = SMA, then EMA(alpha=1/period).

        Returns an array of length ``len(data) - period + 1``.
        """
        n = len(data)
        if n < period:
            return np.array([], dtype=np.float64)

        out = np.empty(n - period + 1, dtype=np.float64)
        out[0] = np.mean(data[:period])

        for i in range(1, len(out)):
            out[i] = out[i - 1] * (1.0 - alpha) + data[period - 1 + i] * alpha

        return out

    @staticmethod
    def _sma(data: np.ndarray, period: int) -> np.ndarray:
        """Simple moving average using cumsum trick."""
        if len(data) < period:
            return np.array([], dtype=np.float64)
        cs = np.cumsum(data)
        cs = np.insert(cs, 0, 0.0)
        return (cs[period:] - cs[:-period]) / period

    @staticmethod
    def _ema(data: np.ndarray, period: int) -> np.ndarray:
        """Exponential moving average (span = period)."""
        if len(data) < period:
            return np.array([], dtype=np.float64)
        alpha = 2.0 / (period + 1)
        out = np.empty(len(data), dtype=np.float64)
        out[0] = data[0]
        for i in range(1, len(data)):
            out[i] = alpha * data[i] + (1.0 - alpha) * out[i - 1]
        return out

    @staticmethod
    def _vol_regime_label(vol_pct: float) -> str:
        """Map volatility percentile to low/normal/high label."""
        if vol_pct < 25:
            return "low"
        if vol_pct > 75:
            return "high"
        return "normal"

    @staticmethod
    def _default_result() -> Dict[str, Any]:
        """Return a safe default when data is insufficient."""
        return {
            "regime": MarketRegime.RANGING,
            "adx": 0.0,
            "di_plus": 0.0,
            "di_minus": 0.0,
            "vol_pct": 50.0,
            "hurst": 0.5,
            "slope_var": 0.0,
            "volatility_regime": "normal",
            "description": "Insufficient data — defaulting to RANGING.",
        }

    @staticmethod
    def _describe(
        regime: MarketRegime, adx: float, vol_pct: float, hurst: float
    ) -> str:
        """Human-readable one-line summary."""
        return (
            f"{regime.value.upper()} | ADX={adx:.1f} | "
            f"VolPct={vol_pct:.0f} | Hurst={hurst:.2f}"
        )
