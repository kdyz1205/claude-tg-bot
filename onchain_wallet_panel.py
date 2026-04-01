"""
onchain_wallet_panel — Solana on-chain wallet summary for Telegram + skills.

Shared by /trade (Wallet button), /chain (🔐 钱包), and skills.sk_onchain_wallet.
Does not store keys; uses secure_wallet for signing policy elsewhere.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)

DEFAULT_SOL_PRICE = 83.0

LookupFn = Optional[Callable[[str], Awaitable[Optional[dict]]]]


async def _timed(fn: Awaitable[Any], timeout: float, default: Any) -> Any:
    try:
        return await asyncio.wait_for(fn, timeout=timeout)
    except Exception as e:
        logger.debug("onchain_wallet_panel timed/failed: %s", e)
        return default


async def _resolve_labels(
    tokens: list[dict],
    *,
    lookup_token: LookupFn,
    max_tokens: int = 14,
) -> list[dict]:
    if not tokens:
        return []

    async def _one(t: dict) -> dict:
        mint = (t.get("mint") or "").strip()
        amt = float(t.get("amount") or 0)
        short = f"{mint[:4]}…{mint[-4:]}" if len(mint) > 12 else (mint or "?")
        label = short
        if mint and lookup_token:
            try:
                info = await asyncio.wait_for(lookup_token(mint), timeout=2.0)
                if info:
                    label = (info.get("symbol") or info.get("name") or short)[:16]
            except Exception:
                pass
        return {"mint": mint, "amount": amt, "decimals": t.get("decimals", 0), "label": label}

    capped = tokens[:max_tokens]
    return list(await asyncio.gather(*[_one(t) for t in capped]))


async def build_wallet_snapshot(
    wallet_mod: Any,
    *,
    chain_cache: Optional[dict[str, Any]] = None,
    lookup_token: LookupFn = None,
    sol_timeout: float = 4.5,
    token_timeout: float = 6.0,
) -> dict[str, Any]:
    """
    Returns a plain dict safe to JSON-log (no secrets):
    ok, configured, pubkey, sol, sol_usd, sol_price, tokens, error?
    """
    chain_cache = chain_cache or {}
    if wallet_mod is None:
        return {"ok": False, "configured": False, "error": "module_unavailable"}

    if not wallet_mod.wallet_exists():
        return {
            "ok": True,
            "configured": False,
            "pubkey": None,
            "sol": None,
            "sol_usd": None,
            "sol_price": float(chain_cache.get("sol_price") or 0) or DEFAULT_SOL_PRICE,
            "tokens": [],
        }

    pubkey = wallet_mod.get_public_key() or "?"

    bal_raw, raw_toks = await asyncio.gather(
        _timed(wallet_mod.get_sol_balance(), sol_timeout, None),
        _timed(wallet_mod.get_token_balances(), token_timeout, []),
    )
    sol_price = float(chain_cache.get("sol_price") or 0) or DEFAULT_SOL_PRICE
    sol = float(bal_raw) if bal_raw is not None else float(chain_cache.get("sol_bal") or 0)
    rows = await _resolve_labels(list(raw_toks or []), lookup_token=lookup_token)

    return {
        "ok": True,
        "configured": True,
        "pubkey": pubkey,
        "sol": sol,
        "sol_usd": sol * sol_price,
        "sol_price": sol_price,
        "tokens": rows,
    }


def format_wallet_message(snapshot: dict[str, Any], *, panel: str = "trade") -> str:
    """Plain text for Telegram (no parse_mode). panel: 'trade' | 'chain'."""
    err = snapshot.get("error")
    if err == "module_unavailable":
        return (
            "💼 链上钱包\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "❌ 钱包模块不可用（需 live_trader + secure_wallet）。"
        )

    if not snapshot.get("configured"):
        return (
            "💼 ON-CHAIN WALLET · 链上钱包\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🔓 尚未绑定 Solana 钱包\n\n"
            "📝 /wallet_setup <私钥或助记词>\n"
            "🔐 密钥由 WALLET_PASSWORD 加密存储；请勿在群组泄露。\n"
            "🛡 本机签名：仅白名单 Jupiter swap（禁止裸 SOL/SPL 转账指令）。"
        )

    pubkey = str(snapshot.get("pubkey") or "?")
    sol = float(snapshot.get("sol") or 0)
    usd = float(snapshot.get("sol_usd") or 0)
    lines = [
        "💼 ON-CHAIN WALLET · 链上钱包",
        "━━━━━━━━━━━━━━━━━━━━",
        f"🔑 Address · 充值地址\n{pubkey}",
        "",
        f"💰 SOL: {sol:.6f}  (~${usd:.2f})",
    ]
    tokens = snapshot.get("tokens") or []
    if tokens:
        lines.append("")
        lines.append(f"🪙 SPL · {len(tokens)} 种")
        for t in tokens[:12]:
            label = str(t.get("label") or "?")
            amt = float(t.get("amount") or 0)
            lines.append(f" · {label} — {amt:,.4f}")
            if panel == "trade":
                m = (t.get("mint") or "").strip()
                if m:
                    lines.append(f"   {m}")

    lines.extend(
        [
            "",
            "⚙️ /wallet_setup 更换  ·  /wallet_delete 删除",
        ]
    )
    if panel == "trade":
        lines.append("🔗 聚合持仓/策略：发送 /chain")
    return "\n".join(lines)
