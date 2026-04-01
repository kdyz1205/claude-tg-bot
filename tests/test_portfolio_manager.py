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
            "tokens": [
                {
                    "label": "USDC",
                    "amount": 100.0,
                    "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                }
            ]
        },
        "last_error": "",
    }
    out = pm.format_portfolio_plain(snap)
    assert "真实持仓摘要" in out
    assert "SOL-USDT-SWAP" in out
    assert "PEPE" in out
    assert "USDC" in out


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
        "last_error": "",
    }

    monkeypatch.setattr("trading.portfolio_snapshot.get_snapshot", lambda: dict(snap))
    text = await pm.get_live_portfolio_summary(refresh=False)
    assert "当前真实持仓" in text
