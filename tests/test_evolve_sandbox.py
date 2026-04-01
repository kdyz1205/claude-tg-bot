import pytest


@pytest.fixture
def no_alert(monkeypatch):
    async def _na(*_a, **_k):
        return None

    monkeypatch.setattr("auto_evolve.trigger_alert", _na)


@pytest.mark.asyncio
async def test_leverage_boundary(no_alert):
    from auto_evolve import EvolveSandbox, EvolutionRiskBoundary

    s = EvolveSandbox(EvolutionRiskBoundary(max_leverage_mutation=2.0))
    assert await s.validate_mutation({"leverage": 2.0, "stop_loss_pct": 0.05})
    assert not await s.validate_mutation({"leverage": 2.01, "stop_loss_pct": 0.05})


@pytest.mark.asyncio
async def test_stop_loss_too_wide(no_alert):
    from auto_evolve import EvolveSandbox, EvolutionRiskBoundary

    s = EvolveSandbox(EvolutionRiskBoundary())
    assert not await s.validate_mutation({"leverage": 1, "stop_loss_pct": 0.20})


@pytest.mark.asyncio
async def test_propose_evolution_requires_backtest(monkeypatch, no_alert):
    from auto_evolve import EvolveSandbox, EvolutionRiskBoundary

    async def low_win(*_a, **_k):
        return 0.1

    s = EvolveSandbox(EvolutionRiskBoundary(min_win_rate_required=0.9))
    monkeypatch.setattr(s, "run_backtest_simulation", low_win)
    ok = await s.propose_evolution({"leverage": 1, "stop_loss_pct": 0.05}, [])
    assert ok is False
