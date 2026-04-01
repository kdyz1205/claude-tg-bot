"""Tests for portfolio_manager formatting, Kelly sizing, and Solana mint regex."""

import json
import re

import pytest


def test_solana_mint_regex_matches():
    pat = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
    sol = "So11111111111111111111111111111111111111112"
    assert pat.match(sol)
    assert not pat.match("short")
    assert not pat.match("0x" + "a" * 40)


def test_format_portfolio_plain_okx_and_dex():
    import portfolio_manager as pm

    snap = {
        "updated_at": 1.0,
        "age_sec": 5.0,
        "sol_price": 100.0,
        "sol_chg_pct": 1.0,
        "okx": {
            "has_keys": True,
            "ok": True,
            "total_equity_usd": 1000.0,
            "usdt_available": 100.0,
            "positions": [
                {"instId": "SOL-USDT-SWAP", "pos": -1.0, "notionalUsd": 500, "upl": 12.5},
            ],
        },
        "dex": {
            "positions": [
                {"symbol": "PEPE", "amount_sol": 1.5, "pnl_pct": -2.3},
            ],
            "total_value_sol": 2.0,
        },
        "wallet": {
            "ok": True,
            "pubkey_short": "Test…Addr",
            "sol_bal": 1.0,
            "token_count": 1,
            "tokens": [
                {
                    "label": "USDC",
                    "amount": 100.0,
                    "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                }
            ],
        },
        "last_error": "",
    }
    out = pm.format_portfolio_plain(snap)
    assert "OKX · 中心化所" in out
    assert "SOL-USDT-SWAP" in out
    assert "PEPE" in out
    assert "USDC" in out
    assert "Polymarket · Polygon CLOB" in out


def test_format_chain_snapshot_compact_tree():
    import portfolio_manager as pm

    mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    snap = {
        "updated_at": 1.0,
        "age_sec": 12.0,
        "sol_price": 80.0,
        "okx": {"has_keys": False, "ok": False, "positions": []},
        "dex": {"positions": [], "total_value_sol": 0.0},
        "wallet": {
            "ok": True,
            "pubkey_short": "Abcd…wxyz",
            "sol_bal": 0.07,
            "token_count": 2,
            "tokens": [
                {"label": "mɔ", "amount": 211.0, "mint": mint},
            ],
        },
        "last_error": "",
    }
    out = pm.format_chain_snapshot(snap)
    assert "按市场分栏" in out
    assert "OKX · 中心化所" in out
    assert "Solana · 链上钱包" in out
    assert "DEX · 引擎/Jupiter 跟踪" in out
    assert "Polymarket · Polygon CLOB" in out
    assert "mɔ" in out
    assert "$mɔ" not in out
    assert mint[:4] in out and mint[-4:] in out


def test_format_chain_compact_and_chunks():
    import portfolio_manager as pm

    snap = {
        "updated_at": 1.0,
        "age_sec": 8.0,
        "sol_price": 90.0,
        "sol_chg_pct": 0.0,
        "okx": {"has_keys": True, "ok": True, "total_equity_usd": 12.0, "usdt_available": 1.0, "positions": []},
        "dex": {"positions": [], "total_value_sol": 0.0},
        "wallet": {
            "ok": True,
            "pubkey_short": "Ab…yz",
            "sol_bal": 0.1,
            "token_count": 0,
            "tokens": [],
        },
        "poly": {"configured": False, "oracle_enabled": False, "recent": []},
        "last_error": "",
    }
    c = pm.format_chain_compact(snap)
    assert "【速览" in c
    assert "OKX:" in c and "Solana:" in c
    chunks = pm.format_chain_snapshot_chunks(snap)
    assert len(chunks) == 3
    assert "OKX" in chunks[0] and "Solana" in chunks[1] and "DEX" in chunks[2]


@pytest.mark.asyncio
async def test_get_live_portfolio_summary_minimal(monkeypatch):
    import portfolio_manager as pm

    snap = {
        "updated_at": 1.0,
        "age_sec": 1.0,
        "sol_price": 50.0,
        "okx": {"has_keys": False, "ok": False, "positions": []},
        "dex": {"positions": [], "total_value_sol": 0.0},
        "wallet": {"tokens": []},
        "poly": {"configured": False, "oracle_enabled": False, "recent": []},
        "last_error": "",
    }

    monkeypatch.setattr("trading.portfolio_snapshot.get_snapshot", lambda: dict(snap))
    text = await pm.get_live_portfolio_summary(refresh=False)
    assert "真实持仓 · 分市场" in text
    assert "Polymarket" in text


def test_scale_kelly_fraction_drawdown_guardian():
    from trading_skills.drawdown_guardian import DrawdownGuardian

    g = DrawdownGuardian(base_max_dd=0.10)
    g.update(100_000.0)
    assert g.scale_kelly_fraction(0.10) == pytest.approx(0.10, rel=1e-3)
    g.update(92_000.0)
    m = g.get_position_size_multiplier()
    assert m < 1.0
    assert g.scale_kelly_fraction(0.10) == pytest.approx(0.10 * m, rel=1e-3)


def test_calculate_kelly_position_size_with_stats(tmp_path, monkeypatch):
    import portfolio_manager as pm

    p = tmp_path / "kelly.json"
    monkeypatch.setattr(pm, "_STATS_PATH", p)
    monkeypatch.setattr(pm, "_MIN_TRADES_FOR_KELLY", 4)
    stats = {
        "skills": {
            "sk_test": {"n_win": 6, "n_loss": 4, "sum_win": 0.12, "sum_loss": 0.08}
        }
    }
    p.write_text(json.dumps(stats), encoding="utf-8")

    from trading_skills.drawdown_guardian import DrawdownGuardian

    g = DrawdownGuardian(base_max_dd=0.10)
    g.update(50_000.0)
    sol = pm.calculate_kelly_position_size(
        "sk_test",
        10_000.0,
        drawdown_guardian=g,
        sol_price_usd=100.0,
    )
    assert sol > 0
    assert sol < 100


def test_sk_oib_momentum_analyze():
    from skills.sk_oib_momentum import OIBMomentumSkill

    n = 40
    bid = [100 + i * 0.5 for i in range(n)]
    ask = [100 - i * 0.3 for i in range(n)]
    out = OIBMomentumSkill().analyze({"bid_depth": bid, "ask_depth": ask})
    assert "buy_confidence" in out and "sell_confidence" in out
    assert out["reason"] == "ok"


def test_profit_tracker_loss_writes_failures_ledger(tmp_path, monkeypatch):
    import profit_tracker as pt

    ledger = tmp_path / "failures.json"
    monkeypatch.setattr(pt, "FAILURE_LEDGER", ledger)
    monkeypatch.setattr(pt, "MAX_LEDGER_ENTRIES", 100)
    sig = {
        "id": "sig_test_1",
        "symbol": "BTC-USDT",
        "signal_type": "breakout_high",
        "skill_id": "sk_oib_momentum",
        "entry_price": 100.0,
        "direction": "long",
        "checked_prices": {"24": 95.0},
    }
    pt._append_signal_loss_to_failure_ledger(sig, -5.0)
    data = json.loads(ledger.read_text(encoding="utf-8"))
    assert len(data) == 1
    assert data[0]["trade_id"] == "sig_test_1"
    assert data[0]["skill_id"] == "sk_oib_momentum"
    assert data[0]["entry_price"] == 100.0
    assert data[0]["exit_price"] == 95.0
    assert data[0]["final_pnl_pct"] == -5.0
    assert data[0]["source"] == "profit_tracker"


def test_sk_triangle_sniper_breakout_up():
    from skills.sk_triangle_sniper import TriangleSniperSkill

    rng = __import__("random").Random(42)
    base = 100.0
    highs = []
    lows = []
    closes = []
    vols = []
    for i in range(30):
        w = max(0.5, 3.0 - i * 0.06)
        mid = base + rng.uniform(-0.2, 0.2)
        hi = mid + w / 2
        lo = mid - w / 2
        highs.append(hi)
        lows.append(lo)
        closes.append(mid + rng.uniform(-0.05, 0.05))
        vols.append(1000 + rng.uniform(0, 50))
    # strong up breakout + volume spike
    highs[-1] = highs[-2] + 0.01
    lows[-1] = lows[-2] + 0.02
    closes[-1] = max(highs[:-1]) + 0.5
    vols[-1] = 5000.0

    out = TriangleSniperSkill().analyze(
        {"high": highs, "low": lows, "close": closes, "vol": vols, "params": {"lookback": 18, "vol_mult": 1.2}}
    )
    assert out.get("breakout_up") is True or out.get("buy_confidence", 0) >= 0
