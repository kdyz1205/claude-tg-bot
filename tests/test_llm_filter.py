import pytest


@pytest.mark.asyncio
async def test_llm_filter_accepts_safe_directive(monkeypatch):
    async def fake_alert(*_a, **_k):
        return None

    monkeypatch.setattr("dispatcher.llm_filter.trigger_alert", fake_alert)
    from dispatcher.llm_filter import LLMHallucinationFilter

    raw = '{"action":"BUY","pair":"BTC/USDT","amount":0.01,"price":50000}'
    d = await LLMHallucinationFilter.sanitize_trade_directive(raw)
    assert d is not None
    assert d["pair"] == "BTC/USDT"
    assert d["notional_usd"] == 500.0


@pytest.mark.asyncio
async def test_llm_filter_rejects_over_notional(monkeypatch):
    async def fake_alert(*_a, **_k):
        return None

    monkeypatch.setattr("dispatcher.llm_filter.trigger_alert", fake_alert)
    from dispatcher.llm_filter import LLMHallucinationFilter

    raw = '{"action":"BUY","pair":"ETH/USDT","amount":10,"price":3000}'
    assert await LLMHallucinationFilter.sanitize_trade_directive(raw) is None


@pytest.mark.asyncio
async def test_llm_filter_invalid_pair(monkeypatch):
    async def fake_alert(*_a, **_k):
        return None

    monkeypatch.setattr("dispatcher.llm_filter.trigger_alert", fake_alert)
    from dispatcher.llm_filter import LLMHallucinationFilter

    raw = '{"action":"BUY","pair":"DOGE/USDT","amount":1000,"price":0.1}'
    assert await LLMHallucinationFilter.sanitize_trade_directive(raw) is None
