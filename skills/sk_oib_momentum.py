"""
skills/sk_oib_momentum.py — Order-book imbalance (OIB) momentum micro-factor.

Vectorized on time × depth: at each tick, OIB = (bid_depth − ask_depth) / (bid_depth + ask_depth + ε).
Momentum = change in smoothed OIB (or z-scored delta). Maps to directional confidence for radar / MoE.

Payload expects aggregate top-of-book or multi-level sums (caller flattens L2 → bid_depth/ask_depth series).
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np

from skills.base_skill import BaseSkill

logger = logging.getLogger(__name__)


def _rolling_mean(a: np.ndarray, window: int) -> np.ndarray:
    w = max(1, int(window))
    n = len(a)
    if n == 0:
        return np.zeros(0, dtype=np.float64)
    if w == 1:
        return a.astype(np.float64, copy=True)
    cum = np.cumsum(np.insert(a.astype(np.float64, copy=False), 0, 0.0))
    out = (cum[w:] - cum[:-w]) / w
    pad = np.full(w - 1, np.nan, dtype=np.float64)
    return np.concatenate([pad, out])


def compute_oib_momentum(
    bid_depth: np.ndarray,
    ask_depth: np.ndarray,
    *,
    smooth_window: int = 5,
    momentum_lag: int = 3,
    z_eps: float = 1e-9,
) -> dict[str, Any]:
    bid = np.asarray(bid_depth, dtype=np.float64).ravel()
    ask = np.asarray(ask_depth, dtype=np.float64).ravel()
    m = min(len(bid), len(ask))
    if m < 4:
        return {
            "oib": 0.0,
            "oib_smoothed": 0.0,
            "momentum": 0.0,
            "z_momentum": 0.0,
            "buy_confidence": 0.0,
            "sell_confidence": 0.0,
            "reason": "insufficient_ticks",
        }
    bid = bid[-m:]
    ask = ask[-m:]
    denom = bid + ask + z_eps
    oib = (bid - ask) / denom
    sw = max(2, min(smooth_window, m))
    sm = _rolling_mean(oib, sw)
    valid = sm[np.isfinite(sm)]
    if len(valid) < 2:
        return {
            "oib": float(oib[-1]),
            "oib_smoothed": float(oib[-1]),
            "momentum": 0.0,
            "z_momentum": 0.0,
            "buy_confidence": 0.0,
            "sell_confidence": 0.0,
            "reason": "smooth_degenerate",
        }
    lag = max(1, min(momentum_lag, len(valid) - 1))
    mom = float(valid[-1] - valid[-1 - lag])
    hist = valid[-min(64, len(valid)) :]
    sd = float(np.std(hist, ddof=1)) if len(hist) > 1 else 0.0
    z = mom / (sd + 1e-6) if sd > 0 else np.sign(mom) * min(abs(mom) * 10.0, 3.0)
    z = float(np.clip(z, -4.0, 4.0))
    # Confidence: squash z to [0,1]; direction from smoothed OIB + momentum agreement
    strength = float(np.tanh(abs(z) / 2.0))
    bias = float(valid[-1])
    if bias > 0 and mom >= 0:
        buy = strength
        sell = 0.0
    elif bias < 0 and mom <= 0:
        buy = 0.0
        sell = strength
    elif abs(bias) > abs(mom) * 0.5:
        buy = strength if bias > 0 else 0.0
        sell = strength if bias < 0 else 0.0
    else:
        buy = sell = 0.0
    return {
        "oib": float(oib[-1]),
        "oib_smoothed": float(valid[-1]),
        "momentum": mom,
        "z_momentum": z,
        "buy_confidence": round(buy, 6),
        "sell_confidence": round(sell, 6),
        "reason": "ok",
    }


class OIBMomentumSkill(BaseSkill):
    skill_id = "sk_oib_momentum"
    default_timeout_sec = 30.0

    def analyze(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        bid = payload.get("bid_depth") or payload.get("bids")
        ask = payload.get("ask_depth") or payload.get("asks")
        if bid is None or ask is None:
            return {
                "buy_confidence": 0.0,
                "sell_confidence": 0.0,
                "reason": "missing_bid_ask_depth",
            }
        params = dict(payload.get("params") or {})
        try:
            sw = int(params.get("smooth_window", payload.get("smooth_window", 5)))
            lag = int(params.get("momentum_lag", payload.get("momentum_lag", 3)))
        except (TypeError, ValueError):
            sw, lag = 5, 3
        out = compute_oib_momentum(
            np.asarray(bid, dtype=np.float64),
            np.asarray(ask, dtype=np.float64),
            smooth_window=sw,
            momentum_lag=lag,
        )
        out["skill_id"] = self.skill_id
        return out

    async def _execute(self, payload: dict[str, Any]) -> Any:
        return self.analyze(payload)


SKILL_CLASS = OIBMomentumSkill
