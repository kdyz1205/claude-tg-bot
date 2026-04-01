"""Unit tests for onchain_wallet_panel (no real RPC)."""

from __future__ import annotations

import pytest

from onchain_wallet_panel import build_wallet_snapshot, format_wallet_message


class _NoWallet:
    def wallet_exists(self):
        return False


class _FakeWallet:
    def wallet_exists(self):
        return True

    def get_public_key(self):
        return "So11111111111111111111111111111111111111112"

    async def get_sol_balance(self):
        return 1.5

    async def get_token_balances(self):
        return [{"mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "amount": 10.0, "decimals": 6}]


@pytest.mark.asyncio
async def test_snapshot_not_configured():
    snap = await build_wallet_snapshot(_NoWallet(), chain_cache={"sol_price": 100.0})
    assert snap["ok"] is True
    assert snap["configured"] is False


@pytest.mark.asyncio
async def test_snapshot_configured_no_lookup():
    snap = await build_wallet_snapshot(_FakeWallet(), chain_cache={"sol_price": 50.0})
    assert snap["configured"] is True
    assert snap["sol"] == 1.5
    assert snap["sol_usd"] == 75.0
    assert len(snap["tokens"]) == 1
    text = format_wallet_message(snap, panel="chain")
    assert "1.500000" in text or "1.5" in text
    assert "So1111" in text


def test_format_module_unavailable():
    t = format_wallet_message({"ok": False, "configured": False, "error": "module_unavailable"})
    assert "不可用" in t or "unavailable" in t.lower()
