"""Fast chaos battery (short blackout via env)."""

import os

import pytest

from gateway.jarvis_semantic import classify_intent


@pytest.fixture(autouse=True)
def _fast_chaos_blackout(monkeypatch):
    monkeypatch.setenv("CHAOS_API_BLACKOUT_SEC", "0.2")


@pytest.mark.asyncio
async def test_run_chaos_immunity_battery_summary():
    from harness.chaos_trading_immunity import run_chaos_immunity_battery

    rep = await run_chaos_immunity_battery()
    assert rep.get("kind") == "chaos_immunity_battery"
    scen = rep.get("scenarios") or []
    assert len(scen) >= 4
    names = {s.get("name") for s in scen}
    assert "flash_crash_90pct_then_hard_kill" in names
    assert "okx_price_api_blackout_then_hard_kill" in names
    assert rep["summary"]["passed"] == rep["summary"]["total"]


@pytest.mark.asyncio
async def test_classify_chaos_immunity_intent():
    row = await classify_intent("启动混沌测试", uid=1)
    assert row["intent"] == "CHAOS_IMMUNITY"


@pytest.mark.asyncio
async def test_classify_chaos_before_wallet_clone():
    row = await classify_intent("启动混沌测试 追踪并破解地址 0x1234567890123456789012345678901234567890", uid=1)
    assert row["intent"] == "CHAOS_IMMUNITY"
