"""
skills/sk_triangle_sniper.py — Converging triangle breakout with volume confirmation.

Uses vectorized bar geometry: narrowing range (high-low) trend, opposing slopes of highs vs lows,
breakout of recent range with volume > vol_mult × rolling mean volume. No SciPy dependency.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np

from skills.base_skill import BaseSkill

logger = logging.getLogger(__name__)


def compute_triangle_breakout(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    vol: np.ndarray,
    *,
    lookback: int = 20,
    vol_mult: float = 1.35,
    min_compress_ratio: float = 0.55,
) -> dict[str, Any]:
    h = np.asarray(high, dtype=np.float64).ravel()
    l = np.asarray(low, dtype=np.float64).ravel()
    c = np.asarray(close, dtype=np.float64).ravel()
    v = np.asarray(vol, dtype=np.float64).ravel()
    n = min(len(h), len(l), len(c), len(v))
    if n < lookback + 2:
        return {
            "buy_confidence": 0.0,
            "sell_confidence": 0.0,
            "compress_ratio": 1.0,
            "breakout_up": False,
            "breakout_down": False,
            "reason": "insufficient_bars",
        }
    h, l, c, v = h[-n:], l[-n:], c[-n:], v[-n:]
    lb = max(8, min(lookback, n - 1))

    span = slice(-lb, None)
    hi0, hi1 = float(h[span][0]), float(h[span][-1])
    lo0, lo1 = float(l[span][0]), float(l[span][-1])
    hi_slope = (hi1 - hi0) / max(lb - 1, 1)
    lo_slope = (lo1 - lo0) / max(lb - 1, 1)

    ranges = h - l
    r_early = float(np.mean(ranges[-lb : -lb // 2])) if lb >= 4 else float(np.mean(ranges))
    r_late = float(np.mean(ranges[-lb // 2 :])) if lb >= 4 else float(ranges[-1])
    compress_ratio = (r_late / r_early) if r_early > 1e-12 else 1.0

    prior_high = float(np.max(h[-lb:-1]))
    prior_low = float(np.min(l[-lb:-1]))
    last_c = float(c[-1])
    last_v = float(v[-1])
    v_base = float(np.mean(v[-lb:-1])) if lb > 2 else float(np.mean(v))
    v_base = max(v_base, 1e-12)
    vol_ok = last_v >= float(vol_mult) * v_base

    converging = hi_slope < -1e-12 and lo_slope > 1e-12
    compressed = compress_ratio <= float(min_compress_ratio) or (
        converging and compress_ratio <= min(1.0, min_compress_ratio + 0.25)
    )

    breakout_up = last_c > prior_high and vol_ok and compressed
    breakout_down = last_c < prior_low and vol_ok and compressed

    conf = 0.0
    if breakout_up and not breakout_down:
        conf = float(np.tanh((last_c / prior_high - 1.0) * 80.0) * min(last_v / v_base / vol_mult, 2.0))
        conf = max(0.0, min(1.0, abs(conf)))
        return {
            "buy_confidence": round(conf, 6),
            "sell_confidence": 0.0,
            "compress_ratio": round(compress_ratio, 6),
            "hi_slope": hi_slope,
            "lo_slope": lo_slope,
            "breakout_up": True,
            "breakout_down": False,
            "vol_ratio": round(last_v / v_base, 4),
            "reason": "breakout_up",
        }
    if breakout_down and not breakout_up:
        conf = float(np.tanh((prior_low / max(last_c, 1e-12) - 1.0) * 80.0) * min(last_v / v_base / vol_mult, 2.0))
        conf = max(0.0, min(1.0, abs(conf)))
        return {
            "buy_confidence": 0.0,
            "sell_confidence": round(conf, 6),
            "compress_ratio": round(compress_ratio, 6),
            "hi_slope": hi_slope,
            "lo_slope": lo_slope,
            "breakout_up": False,
            "breakout_down": True,
            "vol_ratio": round(last_v / v_base, 4),
            "reason": "breakout_down",
        }

    return {
        "buy_confidence": 0.0,
        "sell_confidence": 0.0,
        "compress_ratio": round(compress_ratio, 6),
        "hi_slope": hi_slope,
        "lo_slope": lo_slope,
        "breakout_up": False,
        "breakout_down": False,
        "vol_ratio": round(last_v / v_base, 4),
        "reason": "no_confirmed_breakout",
    }


class TriangleSniperSkill(BaseSkill):
    skill_id = "sk_triangle_sniper"
    default_timeout_sec = 30.0

    def analyze(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        c = payload.get("close")
        if c is None or len(np.asarray(c)) < 10:
            return {
                "buy_confidence": 0.0,
                "sell_confidence": 0.0,
                "reason": "missing_or_short_close",
            }
        c = np.asarray(c, dtype=np.float64).ravel()
        h = np.asarray(payload.get("high") or c, dtype=np.float64).ravel()
        l = np.asarray(payload.get("low") or c, dtype=np.float64).ravel()
        v = np.asarray(payload.get("vol") or payload.get("volume") or np.ones_like(c), dtype=np.float64).ravel()
        params = dict(payload.get("params") or {})
        try:
            lb = int(params.get("lookback", payload.get("lookback", 20)))
            vm = float(params.get("vol_mult", payload.get("vol_mult", 1.35)))
            mc = float(params.get("min_compress_ratio", 0.55))
        except (TypeError, ValueError):
            lb, vm, mc = 20, 1.35, 0.55
        out = compute_triangle_breakout(h, l, c, v, lookback=lb, vol_mult=vm, min_compress_ratio=mc)
        out["skill_id"] = self.skill_id
        return out

    async def _execute(self, payload: dict[str, Any]) -> Any:
        return self.analyze(payload)


SKILL_CLASS = TriangleSniperSkill
