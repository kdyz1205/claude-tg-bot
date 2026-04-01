"""
凯利分数 + 波动率缩放 — 将「固定 SOL」改为随胜率/赔率与波动率调整的上限。

经典：:math:`f^* = (b p - q) / b`，其中 ``q = 1-p``，``b`` 为净赔率（平均盈利/平均亏损）。
半凯利、硬顶 cap 为实务必备；可选按目标年化波动率缩放。
"""

from __future__ import annotations

import math
from typing import Any


def kelly_fraction(
    win_prob: float,
    win_loss_ratio: float,
    *,
    half_kelly: bool = True,
    cap: float = 0.25,
) -> float:
    """
    返回应投入 **本金比例** 的理论凯利份额（已裁剪到 ``[0, cap]``）。

    ``win_loss_ratio`` = b：若平均赚 1.2 单位、平均亏 1 单位，则 b=1.2。
    """
    p = float(win_prob)
    b = float(win_loss_ratio)
    if not (0 < p < 1) or b <= 0:
        return 0.0
    q = 1.0 - p
    raw = (b * p - q) / b
    if raw <= 0:
        return 0.0
    if half_kelly:
        raw *= 0.5
    cap_f = max(0.0, min(float(cap), 1.0))
    return float(min(raw, cap_f))


def volatility_scale(
    realized_ann_vol: float | None,
    target_ann_vol: float | None,
) -> float:
    """波动率目标：``scale = target / realized``，裁剪到 [0.25, 2.0]。"""
    if not realized_ann_vol or not target_ann_vol:
        return 1.0
    rv = float(realized_ann_vol)
    tv = float(target_ann_vol)
    if rv <= 1e-9:
        return 1.0
    s = tv / rv
    return float(max(0.25, min(2.0, s)))


def clamped_kelly_max_sol(
    balance_sol: float,
    cfg: dict[str, Any],
    *,
    signal_data: dict[str, Any] | None = None,
) -> float | None:
    """
    若 ``cfg["kelly_sizing_enabled"]`` 为真，返回本次允许使用的 **最大 SOL**（可与 pct 上限取 min）。

    ``cfg`` 可选键：
      - ``kelly_win_rate`` / ``kelly_b``（赔率 b）
      - ``kelly_cap``（凯利份额顶，默认 0.2）
      - ``kelly_half``（默认 True）
      - ``kelly_vol_realized_ann`` / ``kelly_vol_target_ann`` — 波动率缩放
      - ``kelly_min_sol`` / ``kelly_max_sol`` — 绝对夹紧
    ``signal_data`` 可覆盖：``kelly_win_rate``, ``kelly_b``（雷达/回测注入）。
    """
    if not cfg.get("kelly_sizing_enabled"):
        return None
    sig = signal_data or {}
    p = float(sig.get("kelly_win_rate") or cfg.get("kelly_win_rate") or 0.52)
    b = float(sig.get("kelly_b") or cfg.get("kelly_b") or 1.15)
    cap = float(cfg.get("kelly_cap") or 0.2)
    half = bool(cfg.get("kelly_half", True))
    frac = kelly_fraction(p, b, half_kelly=half, cap=cap)
    if frac <= 0:
        return float(cfg.get("kelly_min_sol") or 0.0) or None

    sc = volatility_scale(
        cfg.get("kelly_vol_realized_ann"),
        cfg.get("kelly_vol_target_ann"),
    )
    raw_sol = float(balance_sol) * frac * sc
    lo = float(cfg.get("kelly_min_sol") or 0.0)
    hi = float(cfg.get("kelly_max_sol") or math.inf)
    out = max(lo, min(raw_sol, hi))
    if out <= 0 or not math.isfinite(out):
        return None
    return out
