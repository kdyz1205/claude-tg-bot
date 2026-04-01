"""Tests for portfolio_manager formatting and Solana mint regex."""

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
