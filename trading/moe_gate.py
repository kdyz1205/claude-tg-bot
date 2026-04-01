"""
Mixture-of-Experts (MoE) pre-trade gate — live radar signal vs three local experts.

Runs after V6 / radar has produced a long/short intent and passed earlier checks,
and before ``OKXExecutor.open_position``:

1. Fixed **debate cooldown** (``MOE_DEBATE_SEC``) on the event loop.
2. Three **local** rule-based experts share the same OHLCV window:
   - 激进多头、保守空头、守门员（风险一票否决）。
3. **Quorum**: at least two of the three cast a non-veto “同意执行本次信号”,
   and the goalkeeper must not hard-veto.

Offline evolutionary competition lives in ``trading.strategy_arena``; this module
is for **live** execution only.
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field
from typing import Any, List, Sequence

import numpy as np

from trading.indicators import atr, sma

MOE_DEBATE_SEC = 1.0
# Minimum relative volume vs 20-bar mean for goalkeeper to allow (illiquidity proxy)
_MIN_VOLUME_RATIO = 0.45
# ATR% above this is “stressed” unless confidence is high
_ATR_PCT_STRESS = 5.5
_CONF_UNDER_STRESS = 0.82
# Spread hint from signal (bps); veto if above
_MAX_SPREAD_BPS = 55.0


def _moe_enabled() -> bool:
    v = (os.environ.get("MOE_GATE_ENABLED") or "1").strip().lower()
    return v not in ("0", "false", "no", "off")


@dataclass
class MoEVoteDetail:
    aggressive_bull: bool = False
    conservative_bear: bool = False
    goalkeeper_ok: bool = False
    goalkeeper_veto: bool = False
    veto_reason: str = ""
    debate_sec: float = MOE_DEBATE_SEC
    approvals: int = 0
    atr_pct: float = 0.0
    vol_ratio: float = 0.0


def _ohlcv_arrays(
    candles: Sequence[Sequence[float]],
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """candles: [ts, o, h, l, c, vol, ...] per row."""
    a = np.asarray(candles, dtype=np.float64)
    if a.ndim != 2 or a.shape[1] < 6:
        raise ValueError("candles must be 2D with >=6 columns (OHLCV)")
    high, low, close, vol = a[:, 2], a[:, 3], a[:, 4], a[:, 5]
    return high, low, close, vol


def expert_aggressive_bull(
    action: str,
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    vol: np.ndarray,
) -> bool:
    """激进多头：偏多进攻；仅在顺势或强趋势反转时认可空头开仓。"""
    if len(close) < 25:
        return False
    ma20 = sma(close, 20)
    i = len(close) - 1
    if np.isnan(ma20[i]):
        return False
    price = close[i]
    mom = (price - close[max(0, i - 3)]) / max(abs(close[max(0, i - 3)]), 1e-12)

    if action == "long":
        return bool(price > ma20[i] and mom > -0.001)

    if action == "short":
        # 强弱势：价在均线下方且短期动量向下
        return bool(price < ma20[i] * 0.997 and mom < -0.002)

    return False


def expert_conservative_bear(
    action: str,
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    vol: np.ndarray,
) -> bool:
    """保守空头：偏谨慎；认可做空在偏弱结构，认可做多仅在回撤/非追高。"""
    if len(close) < 25:
        return False
    ma20 = sma(close, 20)
    i = len(close) - 1
    if np.isnan(ma20[i]):
        return False
    price = close[i]

    if action == "short":
        return bool(price < ma20[i])

    if action == "long":
        # 不在极端追高：收盘价不高于近 12 根最高价的 99.5% 且仍在均线上方或刚回踩
        recent_high = float(np.nanmax(high[max(0, i - 12) : i + 1]))
        chasing = recent_high > 0 and price >= recent_high * 0.998
        return bool(not chasing and price >= ma20[i] * 0.995)

    return False


def expert_goalkeeper(
    signal: dict[str, Any],
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    vol: np.ndarray,
) -> tuple[bool, str]:
    """
    极度厌恶风险的守门员：返回 (允许通过, "") 或 (False, 否决原因)。
    一票否决：流动性过低、波动与置信不匹配、点差过大、成交量枯竭等。
    """
    reasons: List[str] = []

    liq = float(signal.get("liquidity_ratio") or 1.0)
    if liq < 0.35:
        reasons.append(f"liquidity_ratio={liq:.2f}<0.35")

    spread_bps = float(signal.get("spread_bps") or 0.0)
    if spread_bps > _MAX_SPREAD_BPS:
        reasons.append(f"spread_bps={spread_bps:.1f}>{_MAX_SPREAD_BPS}")

    if len(close) < 20:
        reasons.append("insufficient_bars")
        return False, "; ".join(reasons) if reasons else "insufficient_bars"

    atr_arr = atr(high, low, close, 14)
    i = len(close) - 1
    atr_pct = float(atr_arr[i] / close[i] * 100) if close[i] > 0 else 0.0
    conf = float(signal.get("confidence") or 0.0)

    if atr_pct > _ATR_PCT_STRESS and conf < _CONF_UNDER_STRESS:
        reasons.append(
            f"high_atr_pct={atr_pct:.2f}%_with_conf={conf:.2f}<{_CONF_UNDER_STRESS}"
        )

    vol_slice = vol[max(0, i - 20) : i] if i >= 20 else vol[: max(1, i)]
    vol_ma = float(np.nanmean(vol_slice)) if len(vol_slice) else 0.0
    vol_ratio = float(vol[i] / vol_ma) if vol_ma > 0 and not np.isnan(vol[i]) else 1.0
    if vol_ratio < _MIN_VOLUME_RATIO:
        reasons.append(f"volume_ratio={vol_ratio:.2f}<{_MIN_VOLUME_RATIO}")

    if reasons:
        return False, "; ".join(reasons)
    return True, ""


def evaluate_moe_sync(
    symbol: str,
    signal: dict[str, Any],
    candles: Sequence[Sequence[float]],
) -> tuple[bool, MoEVoteDetail]:
    """Pure sync evaluation (call from thread or directly after debate sleep)."""
    detail = MoEVoteDetail()
    action = str(signal.get("action") or "")
    if action not in ("long", "short"):
        detail.veto_reason = "not_entry_action"
        return False, detail

    high, low, close, vol = _ohlcv_arrays(candles)
    i = len(close) - 1
    atr_arr = atr(high, low, close, 14)
    detail.atr_pct = float(atr_arr[i] / close[i] * 100) if close[i] > 0 else 0.0
    vol_slice = vol[max(0, i - 20) : i] if i >= 20 else vol[: max(1, i)]
    vol_ma = float(np.nanmean(vol_slice)) if len(vol_slice) else 0.0
    detail.vol_ratio = (
        float(vol[i] / vol_ma) if vol_ma > 0 and not np.isnan(vol[i]) else 1.0
    )

    ok_gk, gk_reason = expert_goalkeeper(signal, close, high, low, vol)
    detail.goalkeeper_veto = not ok_gk
    detail.veto_reason = gk_reason if not ok_gk else ""
    detail.goalkeeper_ok = ok_gk

    b_bull = expert_aggressive_bull(action, close, high, low, vol)
    b_bear = expert_conservative_bear(action, close, high, low, vol)
    detail.aggressive_bull = b_bull
    detail.conservative_bear = b_bear

    votes = [b_bull, b_bear, ok_gk]
    detail.approvals = sum(1 for v in votes if v)

    if detail.goalkeeper_veto:
        return False, detail
    if detail.approvals >= 2:
        return True, detail
    detail.veto_reason = (
        f"quorum_fail approvals={detail.approvals} "
        f"(bull={b_bull} bear={b_bear} gk_ok={ok_gk})"
    )
    return False, detail


async def run_moe_gate(
    *,
    symbol: str,
    signal: dict[str, Any],
    candles: Sequence[Sequence[float]],
    debate_sec: float | None = None,
) -> tuple[bool, dict[str, Any]]:
    """
    1s 辩论期（可配置），然后并行专家在同步核心里跑完（避免阻塞用 to_thread）。
    返回 (是否放行, 可审计 dict)。
    """
    if not _moe_enabled():
        return True, {"skipped": True, "reason": "MOE_GATE_DISABLED"}

    sec = float(MOE_DEBATE_SEC if debate_sec is None else debate_sec)
    t0 = time.monotonic()
    await asyncio.sleep(sec)
    elapsed_sleep = time.monotonic() - t0

    allowed, detail = await asyncio.to_thread(
        evaluate_moe_sync, symbol, signal, candles
    )

    out: dict[str, Any] = {
        "allowed": allowed,
        "symbol": symbol,
        "action": signal.get("action"),
        "debate_sec_requested": sec,
        "debate_sleep_elapsed": round(elapsed_sleep, 3),
        "aggressive_bull": detail.aggressive_bull,
        "conservative_bear": detail.conservative_bear,
        "goalkeeper_ok": detail.goalkeeper_ok,
        "goalkeeper_veto": detail.goalkeeper_veto,
        "approvals": detail.approvals,
        "veto_reason": detail.veto_reason,
        "atr_pct": round(detail.atr_pct, 4),
        "vol_ratio": round(detail.vol_ratio, 4),
    }
    return allowed, out


__all__ = [
    "MOE_DEBATE_SEC",
    "MoEVoteDetail",
    "evaluate_moe_sync",
    "expert_aggressive_bull",
    "expert_conservative_bear",
    "expert_goalkeeper",
    "run_moe_gate",
]
