"""
skills/sk_triangle_sniper.py — 收敛三角形爆量向上突破（分形极值 + 趋势线 + 成交量护栏）。

数据来自 ``trading.local_ohlcv_cache.load_offline_ohlcv``（磁盘 1m / parquet / npy），
数学核心在 ``trading.indicators.detect_triangle_breakout``。
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict

import numpy as np

from skills.base_skill import BaseSkill
from trading.indicators import detect_triangle_breakout
from trading.local_ohlcv_cache import load_offline_ohlcv


class TriangleSniperSkill(BaseSkill):
    skill_id = "sk_triangle_sniper"
    default_timeout_sec = 45.0

    async def _execute(self, payload: Dict[str, Any]) -> Any:
        inst_id = str(payload.get("inst_id") or payload.get("okx_inst") or "").strip()
        bar = str(payload.get("bar") or "1m").strip()
        limit = int(payload.get("limit") or 100)
        lookback = int(payload.get("lookback") or 60)
        vol_mult = float(payload.get("vol_mult") or 1.8)

        if payload.get("high") is not None and payload.get("low") is not None:
            high = np.asarray(payload["high"], dtype=np.float64)
            low = np.asarray(payload["low"], dtype=np.float64)
            close = np.asarray(
                payload.get("close") if payload.get("close") is not None else high,
                dtype=np.float64,
            )
            vol_raw = payload.get("volume")
            if vol_raw is None:
                vol_raw = payload.get("vol")
            if vol_raw is None:
                volume = np.ones_like(close, dtype=np.float64)
            else:
                volume = np.asarray(vol_raw, dtype=np.float64)
        else:
            if not inst_id:
                return {
                    "buy_confidence": 0.0,
                    "sell_confidence": 0.0,
                    "reason": "missing_inst_id_or_arrays",
                }

            def _load() -> np.ndarray | None:
                return load_offline_ohlcv(inst_id, bar, limit)

            ohlcv = await asyncio.to_thread(_load)
            if ohlcv is None or len(ohlcv) < lookback:
                return {
                    "buy_confidence": 0.0,
                    "sell_confidence": 0.0,
                    "reason": "no_offline_ohlcv_or_short",
                    "inst_id": inst_id,
                }

            high = ohlcv[:, 2]
            low = ohlcv[:, 3]
            close = ohlcv[:, 4]
            volume = ohlcv[:, 5]

        def _detect() -> dict:
            return detect_triangle_breakout(
                high,
                low,
                close,
                volume,
                lookback=lookback,
                vol_mult=vol_mult,
            )

        result = await asyncio.to_thread(_detect)

        if result.get("signal"):
            conf = float(result.get("confidence") or 0.85)
            return {
                "buy_confidence": conf,
                "sell_confidence": 0.0,
                "reason": "symmetrical_triangle_breakout",
                "metadata": {
                    "pattern": "Symmetrical Triangle Breakout",
                    "suggested_sl": result.get("stop_loss"),
                    "suggested_tp": result.get("target"),
                    "resistance_line_price": result.get("resistance_line_price"),
                    "volume_ratio": result.get("volume_ratio"),
                },
            }

        return {
            "buy_confidence": 0.0,
            "sell_confidence": 0.0,
            "reason": str(result.get("reason") or "hold"),
            "debug": {k: result[k] for k in result if k != "signal"},
        }


SKILL_CLASS = TriangleSniperSkill
