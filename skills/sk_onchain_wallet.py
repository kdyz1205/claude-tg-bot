"""
skills/sk_onchain_wallet.py — Agent-callable Solana wallet snapshot (no key material).

Payload (optional):
  chain_cache: dict with sol_price, sol_bal hints (same keys as bot._chain_cache)
"""

from __future__ import annotations

import logging
from typing import Any

from skills.base_skill import BaseSkill

logger = logging.getLogger(__name__)


class OnchainWalletSkill(BaseSkill):
    skill_id = "sk_onchain_wallet"
    default_timeout_sec = 30.0

    async def _execute(self, payload: dict[str, Any] | None) -> dict[str, Any]:
        payload = payload or {}
        try:
            import secure_wallet as sw
        except ImportError as e:
            logger.warning("sk_onchain_wallet: secure_wallet missing: %s", e)
            return {"ok": False, "error": "secure_wallet_import", "summary_text": ""}

        if sw.wallet_exists():
            print(
                "[Cspace] 钥匙加载成功，正在尝试连接 Solana Mainnet...",
                flush=True,
            )
            logger.info("[Cspace] 钥匙加载成功，正在尝试连接 Solana Mainnet...")

        from onchain_wallet_panel import build_wallet_snapshot, format_wallet_message

        chain_cache = payload.get("chain_cache")
        if not isinstance(chain_cache, dict):
            chain_cache = {}

        lookup = None
        if payload.get("with_dex_labels"):
            try:
                import dex_trader as dex

                lookup = dex.lookup_token
            except Exception:
                lookup = None

        snap = await build_wallet_snapshot(sw, chain_cache=chain_cache, lookup_token=lookup)
        text = format_wallet_message(snap, panel="trade")
        return {
            "ok": bool(snap.get("ok")),
            "configured": bool(snap.get("configured")),
            "snapshot": snap,
            "summary_text": text,
        }


SKILL_CLASS = OnchainWalletSkill
