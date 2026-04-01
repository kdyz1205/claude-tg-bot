"""Pure-NumPy technical indicators used by V6 strategy and backtester."""

import numpy as np


def sma(x: np.ndarray, period: int) -> np.ndarray:
    out = np.full(len(x), np.nan)
    for i in range(period - 1, len(x)):
        out[i] = np.mean(x[i - period + 1 : i + 1])
    return out


def ema(x: np.ndarray, span: int) -> np.ndarray:
    a = 2.0 / (span + 1)
    out = np.empty_like(x, dtype=float)
    out[:] = np.nan
    first_valid = next((i for i, v in enumerate(x) if not np.isnan(v)), None)
    if first_valid is None:
        return out
    out[first_valid] = x[first_valid]
    for i in range(first_valid + 1, len(x)):
        if np.isnan(x[i]):
            out[i] = out[i - 1]
        else:
            out[i] = a * x[i] + (1 - a) * out[i - 1]
    return out


def atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray:
    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))
    atr_conv = np.convolve(tr, np.ones(period) / period, mode="valid")
    out = np.full(len(close), np.nan)
    out[period - 1 : period - 1 + len(atr_conv)] = atr_conv
    return out


def bb_upper(close: np.ndarray, period: int = 21, num_std: float = 2.2) -> np.ndarray:
    out = np.full(len(close), np.nan)
    for i in range(period - 1, len(close)):
        w = close[i - period + 1 : i + 1]
        out[i] = np.mean(w) + num_std * np.std(w)
    return out


def bb_lower(close: np.ndarray, period: int = 21, num_std: float = 2.2) -> np.ndarray:
    s = sma(close, period)
    out = np.full(len(close), np.nan)
    for i in range(period - 1, len(close)):
        w = close[i - period + 1 : i + 1]
        out[i] = s[i] - num_std * np.std(w)
    return out


def slope(arr: np.ndarray, length: int, i: int) -> float:
    if i < length or np.isnan(arr[i]) or np.isnan(arr[i - length]):
        return 0.0
    prev = arr[i - length]
    if prev == 0:
        return 0.0
    return (arr[i] - prev) / prev * 100


# ── Fractal pivots & symmetrical triangle breakout (pure NumPy window slices) ─


def find_pivots(
    high: np.ndarray,
    low: np.ndarray,
    left_bars: int = 5,
    right_bars: int = 5,
) -> tuple[np.ndarray, np.ndarray]:
    """
    分形高点 (Pivot Highs) 与分形低点 (Pivot Lows)。
    返回与 ``high`` 等长的布尔数组，True 表示该下标为窗口内极值。
    """
    high = np.asarray(high, dtype=np.float64)
    low = np.asarray(low, dtype=np.float64)
    n = len(high)
    pivot_highs = np.zeros(n, dtype=bool)
    pivot_lows = np.zeros(n, dtype=bool)
    if n != len(low) or n < left_bars + right_bars + 1:
        return pivot_highs, pivot_lows

    for i in range(left_bars, n - right_bars):
        w_h = high[i - left_bars : i + right_bars + 1]
        w_l = low[i - left_bars : i + right_bars + 1]
        if high[i] == np.max(w_h):
            pivot_highs[i] = True
        if low[i] == np.min(w_l):
            pivot_lows[i] = True

    return pivot_highs, pivot_lows


def detect_triangle_breakout(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    volume: np.ndarray,
    *,
    lookback: int = 60,
    vol_mult: float = 1.5,
    vol_avg_bars: int = 20,
) -> dict:
    """
    检测最近窗口内是否出现「收敛三角形 + 收盘价上破阻力 + 放量」。

    铁律：阻力斜率 ``m_R < 0``（lower highs），支撑斜率 ``m_S > 0``（higher lows），
    且 ``close[-1]`` 高于阻力延长线，``volume[-1] > vol_mult * mean(volume[:-1])``。
    """
    high = np.asarray(high, dtype=np.float64)
    low = np.asarray(low, dtype=np.float64)
    close = np.asarray(close, dtype=np.float64)
    volume = np.asarray(volume, dtype=np.float64)
    n = min(len(high), len(low), len(close), len(volume))
    if n < lookback:
        return {"signal": False, "reason": "short_history"}

    hb = high[-lookback:]
    lb = low[-lookback:]
    cb = close[-lookback:]
    vb = volume[-lookback:]

    ph, pl = find_pivots(hb, lb)
    high_idx = np.where(ph)[0]
    low_idx = np.where(pl)[0]

    if len(high_idx) < 2 or len(low_idx) < 2:
        return {"signal": False, "reason": "insufficient_pivots"}

    x1_h, x2_h = int(high_idx[-2]), int(high_idx[-1])
    y1_h, y2_h = float(hb[x1_h]), float(hb[x2_h])
    x1_l, x2_l = int(low_idx[-2]), int(low_idx[-1])
    y1_l, y2_l = float(lb[x1_l]), float(lb[x2_l])

    dx_h = x2_h - x1_h
    dx_l = x2_l - x1_l
    if dx_h == 0 or dx_l == 0:
        return {"signal": False, "reason": "degenerate_pivot_spacing"}

    slope_res = (y2_h - y1_h) / dx_h
    slope_sup = (y2_l - y1_l) / dx_l

    if slope_res >= 0 or slope_sup <= 0:
        return {"signal": False, "reason": "not_converging_triangle"}

    current_idx = lookback - 1
    current_res_line_price = slope_res * (current_idx - x2_h) + y2_h
    is_breakout = float(cb[-1]) > current_res_line_price

    past = max(2, min(vol_avg_bars, len(vb) - 1))
    avg_vol = float(np.mean(vb[-past - 1 : -1])) if past > 0 else 0.0
    if avg_vol <= 0:
        return {"signal": False, "reason": "no_volume_baseline"}
    is_vol_confirmed = float(vb[-1]) > avg_vol * vol_mult

    if is_breakout and is_vol_confirmed:
        opening_height = abs(y1_h - y1_l)
        return {
            "signal": True,
            "confidence": 0.85,
            "stop_loss": y2_l,
            "target": float(cb[-1]) + opening_height,
            "resistance_line_price": current_res_line_price,
            "slope_resistance": slope_res,
            "slope_support": slope_sup,
            "avg_volume": avg_vol,
            "volume_ratio": float(vb[-1]) / avg_vol,
        }

    return {
        "signal": False,
        "reason": "no_breakout_or_volume",
        "is_breakout": is_breakout,
        "is_vol_confirmed": is_vol_confirmed,
    }
