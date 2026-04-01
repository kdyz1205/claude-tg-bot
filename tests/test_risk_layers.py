"""
tests/test_risk_layers.py — Integration-level risk layer tests.

Covers:
  - LLMHallucinationFilter: all edge-case paths
  - EvolveSandbox: boundary mutations
  - ConsciousnessStateManager: concurrent patch isolation
  - AsyncRateLimiter: token exhaustion + replenish
"""

from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path
from typing import Any

import pytest

# ── Shared mocks ─────────────────────────────────────────────────────────────

@pytest.fixture
def no_alert(monkeypatch):
    async def _noop(*_a: Any, **_k: Any) -> None:
        return None
    monkeypatch.setattr("dispatcher.llm_filter.trigger_alert", _noop)
    monkeypatch.setattr("auto_evolve.trigger_alert", _noop)
    monkeypatch.setattr("adaptive_controller.trigger_alert", _noop)


# ═══════════════════════════════════════════════════════════════════════════════
# LLMHallucinationFilter
# ═══════════════════════════════════════════════════════════════════════════════

class TestLLMHallucinationFilter:

    @pytest.mark.asyncio
    async def test_accepts_valid_dict(self, no_alert):
        from dispatcher.llm_filter import LLMHallucinationFilter
        d = await LLMHallucinationFilter.sanitize_trade_directive(
            {"action": "BUY", "pair": "BTC/USDT", "amount": 0.01, "price": 50000}
        )
        assert d is not None
        assert d["notional_usd"] == pytest.approx(500.0)

    @pytest.mark.asyncio
    async def test_accepts_json_embedded_in_prose(self, no_alert):
        from dispatcher.llm_filter import LLMHallucinationFilter
        raw = 'Sure! ```json\n{"action":"SELL","pair":"ETH/USDT","amount":0.1,"price":3000}\n```'
        d = await LLMHallucinationFilter.sanitize_trade_directive(raw)
        assert d is not None and d["action"] == "SELL"

    @pytest.mark.asyncio
    async def test_normalises_pair_separators(self, no_alert):
        from dispatcher.llm_filter import LLMHallucinationFilter
        d = await LLMHallucinationFilter.sanitize_trade_directive(
            {"action": "BUY", "pair": "BTC-USDT", "amount": 0.01, "price": 50000}
        )
        assert d is not None and d["pair"] == "BTC/USDT"

    @pytest.mark.asyncio
    async def test_rejects_non_whitelisted_pair(self, no_alert):
        from dispatcher.llm_filter import LLMHallucinationFilter
        d = await LLMHallucinationFilter.sanitize_trade_directive(
            {"action": "BUY", "pair": "DOGE/USDT", "amount": 1.0, "price": 0.1}
        )
        assert d is None

    @pytest.mark.asyncio
    async def test_rejects_over_notional(self, no_alert):
        from dispatcher.llm_filter import LLMHallucinationFilter
        # 100 ETH × $3000 = $300,000 >> $5,000 cap
        d = await LLMHallucinationFilter.sanitize_trade_directive(
            {"action": "BUY", "pair": "ETH/USDT", "amount": 100.0, "price": 3000.0}
        )
        assert d is None

    @pytest.mark.asyncio
    async def test_rejects_hallucinated_action(self, no_alert):
        from dispatcher.llm_filter import LLMHallucinationFilter
        d = await LLMHallucinationFilter.sanitize_trade_directive(
            {"action": "YOLO_BUY", "pair": "BTC/USDT", "amount": 0.001, "price": 50000}
        )
        assert d is None

    @pytest.mark.asyncio
    async def test_rejects_negative_amount(self, no_alert):
        from dispatcher.llm_filter import LLMHallucinationFilter
        d = await LLMHallucinationFilter.sanitize_trade_directive(
            {"action": "BUY", "pair": "SOL/USDT", "amount": -1, "price": 100}
        )
        assert d is None

    @pytest.mark.asyncio
    async def test_rejects_missing_fields(self, no_alert):
        from dispatcher.llm_filter import LLMHallucinationFilter
        d = await LLMHallucinationFilter.sanitize_trade_directive(
            {"action": "BUY", "pair": "BTC/USDT"}  # amount + price missing
        )
        assert d is None

    @pytest.mark.asyncio
    async def test_rejects_garbage_input(self, no_alert):
        from dispatcher.llm_filter import LLMHallucinationFilter
        d = await LLMHallucinationFilter.sanitize_trade_directive(
            "The market is looking great today!"
        )
        assert d is None

    @pytest.mark.asyncio
    async def test_close_action_allowed(self, no_alert):
        from dispatcher.llm_filter import LLMHallucinationFilter
        d = await LLMHallucinationFilter.sanitize_trade_directive(
            {"action": "CLOSE", "pair": "BTC/USDT", "amount": 0.001, "price": 50000}
        )
        assert d is not None

    @pytest.mark.asyncio
    async def test_ten_sol_point_zero_one_micro_trades_stress(self, no_alert):
        """Regression: ten consecutive 0.01 SOL notional-style directives must all sanitize."""
        from dispatcher.llm_filter import LLMHallucinationFilter

        for i in range(10):
            d = await LLMHallucinationFilter.sanitize_trade_directive(
                {
                    "action": "BUY",
                    "pair": "SOL/USDT",
                    "amount": 0.01,
                    "price": 150.0,
                }
            )
            assert d is not None, f"iteration {i}"
            assert d["pair"] == "SOL/USDT"
            assert d["amount"] == pytest.approx(0.01)


# ═══════════════════════════════════════════════════════════════════════════════
# EvolveSandbox
# ═══════════════════════════════════════════════════════════════════════════════

class TestEvolveSandbox:

    @pytest.mark.asyncio
    async def test_valid_genome_passes(self, no_alert):
        from auto_evolve import EvolveSandbox, EvolutionRiskBoundary
        s = EvolveSandbox(EvolutionRiskBoundary(max_leverage_mutation=2.0))
        assert await s.validate_mutation({"leverage": 1.5, "stop_loss_pct": 0.1})

    @pytest.mark.asyncio
    async def test_leverage_exactly_at_boundary(self, no_alert):
        from auto_evolve import EvolveSandbox, EvolutionRiskBoundary
        s = EvolveSandbox(EvolutionRiskBoundary(max_leverage_mutation=2.0))
        assert await s.validate_mutation({"leverage": 2.0, "stop_loss_pct": 0.05})

    @pytest.mark.asyncio
    async def test_leverage_one_tick_above_boundary(self, no_alert):
        from auto_evolve import EvolveSandbox, EvolutionRiskBoundary
        s = EvolveSandbox(EvolutionRiskBoundary(max_leverage_mutation=2.0))
        assert not await s.validate_mutation({"leverage": 2.01, "stop_loss_pct": 0.05})

    @pytest.mark.asyncio
    async def test_stop_loss_too_wide(self, no_alert):
        from auto_evolve import EvolveSandbox
        s = EvolveSandbox()
        assert not await s.validate_mutation({"leverage": 1.0, "stop_loss_pct": 0.20})

    @pytest.mark.asyncio
    async def test_bad_type_leverage_rejected(self, no_alert):
        from auto_evolve import EvolveSandbox
        s = EvolveSandbox()
        assert not await s.validate_mutation({"leverage": "ten", "stop_loss_pct": 0.05})

    @pytest.mark.asyncio
    async def test_propose_evolution_win_rate_gate(self, monkeypatch, no_alert):
        from auto_evolve import EvolveSandbox, EvolutionRiskBoundary

        async def _low(*_a: Any, **_k: Any) -> float:
            return 0.10

        s = EvolveSandbox(EvolutionRiskBoundary(min_win_rate_required=0.45))
        monkeypatch.setattr(s, "run_backtest_simulation", _low)
        ok = await s.propose_evolution({"leverage": 1.0, "stop_loss_pct": 0.05}, [])
        assert ok is False

    @pytest.mark.asyncio
    async def test_propose_evolution_passes_when_win_rate_ok(self, monkeypatch, no_alert):
        from auto_evolve import EvolveSandbox, EvolutionRiskBoundary

        async def _high(*_a: Any, **_k: Any) -> float:
            return 0.60

        s = EvolveSandbox(EvolutionRiskBoundary(min_win_rate_required=0.45))
        monkeypatch.setattr(s, "run_backtest_simulation", _high)
        ok = await s.propose_evolution({"leverage": 1.0, "stop_loss_pct": 0.05}, [])
        assert ok is True


# ═══════════════════════════════════════════════════════════════════════════════
# ConsciousnessStateManager — concurrent safety
# ═══════════════════════════════════════════════════════════════════════════════

class TestConsciousnessStateManager:

    @pytest.mark.asyncio
    async def test_atomic_write_and_read(self, no_alert, tmp_path, monkeypatch):
        from adaptive_controller import ConsciousnessStateManager
        # Point at a temp file for isolation
        ConsciousnessStateManager._instance = None
        mgr = ConsciousnessStateManager()
        mgr._state_file = tmp_path / ".test_cs.json"

        ok = await mgr.commit_state_patch({"foo": "bar"}, "test")
        assert ok
        data = await mgr.load_state()
        assert data["foo"] == "bar"
        assert data["_last_modifier"] == "test"
        ConsciousnessStateManager._instance = None  # reset singleton

    @pytest.mark.asyncio
    async def test_concurrent_patches_no_corruption(self, no_alert, tmp_path):
        from adaptive_controller import ConsciousnessStateManager
        ConsciousnessStateManager._instance = None
        mgr = ConsciousnessStateManager()
        mgr._state_file = tmp_path / ".test_cs2.json"

        await asyncio.gather(*[
            mgr.commit_state_patch({f"key_{i}": i}, f"caller_{i}")
            for i in range(10)
        ])
        data = await mgr.load_state()
        assert isinstance(data, dict)
        ConsciousnessStateManager._instance = None

    @pytest.mark.asyncio
    async def test_corrupt_json_returns_cache(self, no_alert, tmp_path):
        from adaptive_controller import ConsciousnessStateManager
        ConsciousnessStateManager._instance = None
        mgr = ConsciousnessStateManager()
        state_file = tmp_path / ".test_cs3.json"
        mgr._state_file = state_file
        mgr.memory_cache = {"cached": True}
        state_file.write_text("{ not valid json !!!", encoding="utf-8")

        data = await mgr.load_state()
        assert data.get("cached") is True
        ConsciousnessStateManager._instance = None


# ═══════════════════════════════════════════════════════════════════════════════
# AsyncRateLimiter
# ═══════════════════════════════════════════════════════════════════════════════

class TestAsyncRateLimiter:

    @pytest.mark.asyncio
    async def test_burst_within_limit(self):
        from pipeline.net_gate import AsyncRateLimiter
        lim = AsyncRateLimiter(rate_limit=5, time_window=1.0)
        # Should all complete instantly (tokens pre-filled)
        await asyncio.gather(*[lim.acquire() for _ in range(5)])

    @pytest.mark.asyncio
    async def test_tokens_exhaust_then_replenish(self):
        from pipeline.net_gate import AsyncRateLimiter
        lim = AsyncRateLimiter(rate_limit=2, time_window=0.2)
        await lim.acquire()
        await lim.acquire()
        # Next acquire must wait for replenishment (< 1s in test)
        start = asyncio.get_event_loop().time()
        await asyncio.wait_for(lim.acquire(), timeout=2.0)
        assert asyncio.get_event_loop().time() - start >= 0.05
