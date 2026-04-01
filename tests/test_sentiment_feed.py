"""gateway.sentiment_feed — URL 检测与阈值逻辑。"""

import pytest

from gateway import sentiment_feed as sf


def test_extract_urls_dedup():
    t = "see https://x.com/a and https://x.com/a ok"
    assert sf.extract_urls(t) == ["https://x.com/a"]


def test_is_single_url_message():
    assert sf.is_single_url_message("https://x.com/foo/status/1")
    assert not sf.is_single_url_message("https://a.com\nhttps://b.com")
    assert not sf.is_single_url_message("hello https://x.com/a")


@pytest.mark.asyncio
async def test_process_sentiment_feed_no_trade_below_threshold(monkeypatch):
    async def fake_build(_u):
        return "some text", "inline_text"

    async def fake_llm(_b):
        return {
            "sentiment": 0.5,
            "primary_symbol": "BONK",
            "primary_mint": None,
            "coin_notes": "test",
        }

    async def fake_resolve(_t):
        return {"address": "So11111111111111111111111111111111111111112", "liquidity_usd": 100_000, "price_usd": 1.0, "symbol": "BONK", "name": "Bonk", "chain": "solana", "pair_url": ""}

    monkeypatch.setattr(sf, "build_analysis_text", fake_build)
    monkeypatch.setattr(sf, "analyze_sentiment_json", fake_llm)
    monkeypatch.setattr(sf, "resolve_solana_token", fake_resolve)

    out = await sf.process_sentiment_feed("x", user_mode="paper")
    assert "0.500" in out or "+0.500" in out
    assert "未超过" in out or "不触发" in out


@pytest.mark.asyncio
async def test_process_sentiment_feed_triggers_paper_buy(monkeypatch):
    calls: list[float] = []

    async def fake_build(_u):
        return "mega bullish", "inline_text"

    async def fake_llm(_b):
        return {
            "sentiment": 0.95,
            "primary_symbol": "BONK",
            "primary_mint": None,
            "coin_notes": "moon",
        }

    async def fake_resolve(_t):
        return {
            "address": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
            "liquidity_usd": 80_000,
            "price_usd": 0.00002,
            "symbol": "BONK",
            "name": "Bonk",
            "chain": "solana",
            "pair_url": "https://dexscreener.com",
        }

    def fake_buy(info, amount_sol, mode="paper"):
        calls.append(amount_sol)
        return {
            "id": "t",
            "address": info["address"],
            "symbol": info["symbol"],
            "name": info["name"],
            "chain": "solana",
            "pair_url": "",
            "entry_price": info["price_usd"],
            "current_price": info["price_usd"],
            "amount_sol": amount_sol,
            "tokens": 1.0,
            "entry_time": 0.0,
            "last_buy_time": 0.0,
            "last_updated": 0.0,
            "status": "open",
            "pnl_pct": 0,
            "current_value_sol": amount_sol,
            "peak_pnl": 0,
            "trough_pnl": 0,
            "tp_pct": 100,
            "sl_pct": -30,
            "mode": mode,
        }

    import dex_trader as dex

    monkeypatch.setattr(sf, "build_analysis_text", fake_build)
    monkeypatch.setattr(sf, "analyze_sentiment_json", fake_llm)
    monkeypatch.setattr(sf, "resolve_solana_token", fake_resolve)
    monkeypatch.setattr(dex, "execute_buy", fake_buy)
    monkeypatch.setattr(dex, "save_positions", lambda _p: None)
    monkeypatch.setattr(dex, "get_positions", lambda: [])
    monkeypatch.setattr(dex, "format_buy_result", lambda pos, amt: "ok")

    out = await sf.process_sentiment_feed("x", user_mode="paper")
    assert calls and abs(calls[0] - 0.05) < 1e-9
    assert "事件驱动" in out
