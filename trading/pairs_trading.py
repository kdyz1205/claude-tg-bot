"""
统计套利 / 配对交易（轻量版）：对数价格 OLS 残差作为价差，滚动 Z-score 触发均值回归信号。

完整 Engle-Granger 检验依赖 ``statsmodels``；此处用 **最小二乘残差 + 波动率标准化**，
与 ``trading_skills/correlation_monitor`` 互补：相关矩阵看集中度，本模块看 **价差是否偏离**。

Delta-中性语义：signal 给出「多弱势腿 / 空强势腿」的方向提示；实盘需两笔对冲执行（由执行层实现）。
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


def log_price_ols_spread(p1: np.ndarray, p2: np.ndarray) -> tuple[float, float, np.ndarray]:
    """
    ``log(p1) = α + β log(p2) + ε``，返回 ``(beta, alpha, spread=ε)``。
    """
    l1 = np.log(np.clip(np.asarray(p1, dtype=np.float64), 1e-12, None))
    l2 = np.log(np.clip(np.asarray(p2, dtype=np.float64), 1e-12, None))
    n = min(len(l1), len(l2))
    if n < 30:
        return 0.0, 0.0, np.array([])
    l1, l2 = l1[-n:], l2[-n:]
    X = np.column_stack([np.ones(n), l2])
    coef, _, _, _ = np.linalg.lstsq(X, l1, rcond=None)
    alpha, beta = float(coef[0]), float(coef[1])
    spread = l1 - (alpha + beta * l2)
    return beta, alpha, spread


def spread_zscore(spread: np.ndarray, window: int = 60) -> float:
    """最后一根 K 对应的 Z：``(s - μ) / σ``。"""
    if len(spread) < window:
        return 0.0
    w = spread[-window:]
    mu = float(np.mean(w))
    sd = float(np.std(w))
    if sd < 1e-12:
        return 0.0
    return float((spread[-1] - mu) / sd)


def pairs_trading_signal(
    close_a: np.ndarray,
    close_b: np.ndarray,
    *,
    z_entry: float = 2.0,
    z_exit: float = 0.5,
    window: int = 60,
    name_a: str = "A",
    name_b: str = "B",
) -> dict[str, Any]:
    """
    价差 Z 过大：A 相对 B **高估** → 做空 A / 做多 B（均值回归押注）。
    Z 过小：反向。
    """
    beta, alpha, spread = log_price_ols_spread(close_a, close_b)
    if spread.size == 0:
        return {
            "signal": "none",
            "reason": "short_history",
            "z": 0.0,
            "beta": beta,
            "alpha": alpha,
        }
    z = spread_zscore(spread, window=window)
    out: dict[str, Any] = {
        "signal": "none",
        "z": z,
        "beta": beta,
        "alpha": alpha,
        "spread_last": float(spread[-1]),
        "leg_long": None,
        "leg_short": None,
        "delta_neutral_hint": True,
    }
    if z > z_entry:
        out["signal"] = "short_a_long_b"
        out["leg_short"] = name_a
        out["leg_long"] = name_b
        out["reason"] = "spread_high_a_rich_vs_b"
    elif z < -z_entry:
        out["signal"] = "long_a_short_b"
        out["leg_long"] = name_a
        out["leg_short"] = name_b
        out["reason"] = "spread_low_a_cheap_vs_b"
    elif abs(z) < z_exit:
        out["signal"] = "flat"
        out["reason"] = "inside_band"
    else:
        out["reason"] = "between_entry_exit"
    return out
