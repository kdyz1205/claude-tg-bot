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
