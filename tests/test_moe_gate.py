import os

import numpy as np
import pytest

from trading.moe_gate import (
    evaluate_moe_sync,
    expert_goalkeeper,
    run_moe_gate,
)


def _candles_from_close(close: np.ndarray) -> list[list[float]]:
    high = close + 0.15
    low = close - 0.15
    vol = np.full(len(close), 5000.0, dtype=np.float64)
    out: list[list[float]] = []
    for i in range(len(close)):
        out.append(
            [float(i), float(close[i]), float(high[i]), float(low[i]), float(close[i]), float(vol[i])]
        )
    return out


def test_goalkeeper_vetoes_low_liquidity():
    n = 50
    close = np.linspace(100.0, 110.0, n, dtype=np.float64)
    high = close + 0.2
    low = close - 0.2
    vol = np.full(n, 8000.0)
    sig = {
        "action": "long",
        "confidence": 0.9,
        "liquidity_ratio": 0.2,
        "spread_bps": 10.0,
    }
    ok, reason = expert_goalkeeper(sig, close, high, low, vol)
    assert ok is False
    assert "liquidity" in reason.lower()


def test_evaluate_moe_sync_quorum_long():
    """Pullback-style long: bull + bear + GK can all approve."""
    n = 60
    close = np.full(n, 118.0, dtype=np.float64)
    close[-15:] = np.linspace(118.0, 120.0, 15)
    close[-1] = 119.0
    candles = _candles_from_close(close)
    sig = {
        "action": "long",
        "confidence": 0.92,
        "liquidity_ratio": 1.0,
        "spread_bps": 8.0,
    }
    allowed, detail = evaluate_moe_sync("BTCUSDT", sig, candles)
    assert detail.goalkeeper_veto is False
    assert allowed is True
    assert detail.approvals >= 2


def test_evaluate_moe_sync_blocked_when_goalkeeper_vetoes():
    n = 60
    close = np.linspace(100.0, 112.0, n, dtype=np.float64)
    candles = _candles_from_close(close)
    sig = {
        "action": "long",
        "confidence": 0.99,
        "liquidity_ratio": 0.1,
        "spread_bps": 8.0,
    }
    allowed, detail = evaluate_moe_sync("ETHUSDT", sig, candles)
    assert detail.goalkeeper_veto is True
    assert allowed is False


@pytest.mark.asyncio
async def test_run_moe_gate_skipped_when_disabled():
    old = os.environ.get("MOE_GATE_ENABLED")
    os.environ["MOE_GATE_ENABLED"] = "0"
    try:
        ok, d = await run_moe_gate(
            symbol="BTCUSDT",
            signal={"action": "long"},
            candles=[],
            debate_sec=0,
        )
        assert ok is True
        assert d.get("skipped") is True
    finally:
        if old is None:
            os.environ.pop("MOE_GATE_ENABLED", None)
        else:
            os.environ["MOE_GATE_ENABLED"] = old


@pytest.mark.asyncio
async def test_run_moe_gate_respects_debate_sec_zero():
    n = 60
    close = np.full(n, 118.0, dtype=np.float64)
    close[-15:] = np.linspace(118.0, 120.0, 15)
    close[-1] = 119.0
    candles = _candles_from_close(close)
    sig = {
        "action": "long",
        "confidence": 0.92,
        "liquidity_ratio": 1.0,
        "spread_bps": 8.0,
    }
    ok, d = await run_moe_gate(
        symbol="BTCUSDT", signal=sig, candles=candles, debate_sec=0
    )
    assert ok is True
    assert d.get("debate_sec_requested") == 0
    assert d.get("allowed") is True
