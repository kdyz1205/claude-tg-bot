"""
Live portfolio summary for Telegram (MarkdownV2) and plain chain dashboard text.

Uses trading.portfolio_snapshot for OKX + wallet + DEX; optional Jupiter prices for SPL.
"""

from __future__ import annotations

import asyncio
from typing import Any

try:
    from telegram.helpers import escape_markdown
except ImportError:

    def escape_markdown(s: str, version: int = 2) -> str:
        if version != 2:
            return s
        for ch in r"_*[]()~`>#+-=|{}.!":
            s = s.replace(ch, "\\" + ch)
        return s


def _fmt_usd(n: float) -> str:
    return f"{n:,.2f}"


def _fmt_qty(n: float) -> str:
    if abs(n) >= 1_000_000:
        return f"{n/1e6:.2f}M"
    if abs(n) >= 1_000:
        return f"{n/1e3:.2f}K"
    if abs(n) >= 1:
        return f"{n:.4f}"
    return f"{n:.6f}"


def format_portfolio_plain(snapshot: dict[str, Any]) -> str:
    """Plain-text block for embedding in /chain dashboard (no MarkdownV2)."""
    sp = float(snapshot.get("sol_price") or 0)
    ox = snapshot.get("okx") or {}
    dx = snapshot.get("dex") or {}
    w = snapshot.get("wallet") or {}
    lines: list[str] = []
    okx_eq = float(ox.get("total_equity_usd") or 0)
    dex_v = float(dx.get("total_value_sol") or 0)
    dex_usd = dex_v * sp if sp > 0 else 0.0
    total_nav = okx_eq + dex_usd
    age = float(snapshot.get("age_sec") or 0)
    lines.append("━━ 真实持仓摘要 ━━")
    lines.append(f"总资产净值(估): ~${_fmt_usd(total_nav)}  |  快照 {age:.0f}s 前")
    if ox.get("has_keys") and ox.get("ok"):
        lines.append(f"OKX 权益 ${okx_eq:,.2f}  ·  可用 USDT {float(ox.get('usdt_available') or 0):,.2f}")
        for i, row in enumerate((ox.get("positions") or [])[:8], 1):
            inst = row.get("instId") or "?"
            upl = float(row.get("upl") or 0)
            nu = float(row.get("notionalUsd") or 0)
            em = "🟢" if upl >= 0 else "🔴"
            side = "空" if float(row.get("pos") or 0) < 0 else "多"
            lines.append(f"{i}. ${inst} (OKX{side}保) | 名义 ${nu:,.0f} | 浮盈 {em} ${upl:+,.2f}")
    elif ox.get("has_keys") and not ox.get("ok"):
        lines.append(f"OKX: {str(ox.get('error') or '?')[:80]}")
    for i, p in enumerate((dx.get("positions") or [])[:6], 1):
        sym = (p.get("symbol") or p.get("name") or "?")[:14]
        amt = float(p.get("amount_sol", 0) or 0)
        pnl = float(p.get("pnl_pct", 0) or 0)
        em = "🟢" if pnl >= 0 else "🔴"
        lines.append(f"{i}. ${sym} (DEX) | {amt:.4f} SOL 等 | 浮亏 {em} {pnl:+.1f}%")
    for i, t in enumerate((w.get("tokens") or [])[:6], 1):
        lab = (t.get("label") or "?")[:12]
        amt = float(t.get("amount") or 0)
        lines.append(f"{i}. ${lab} (钱包SPL) | 数量 {_fmt_qty(amt)} | 浮亏 —")
    err = (snapshot.get("last_error") or "").strip()
    if err:
        lines.append(f"⚠ 同步: {err[:120]}")
    return "\n".join(lines)


async def _wallet_token_prices_usd(mints: list[str]) -> dict[str, float]:
    out: dict[str, float] = {}
    if not mints:
        return out
    try:
        import live_trader as lt
    except ImportError:
        return out

    async def _one(m: str) -> tuple[str, float]:
        try:
            p = await asyncio.wait_for(lt._get_token_price_usd(m), timeout=2.5)
            return m, float(p or 0)
        except Exception:
            return m, 0.0

    pairs = await asyncio.gather(*[_one(m) for m in mints[:8]], return_exceptions=True)
    for pr in pairs:
        if isinstance(pr, BaseException):
            continue
        m, px = pr
        if px > 0:
            out[m] = px
    return out


async def get_live_portfolio_summary(*, refresh: bool = False) -> str:
    """
    MarkdownV2 string for Telegram. Call with parse_mode='MarkdownV2'.
    If refresh=True, runs portfolio_snapshot.refresh_once() first.
    """
    if refresh:
        try:
            from trading.portfolio_snapshot import refresh_once as _rf

            await _rf()
        except Exception:
            pass
    from trading.portfolio_snapshot import get_snapshot as _gs

    snap = _gs()
    sp = float(snap.get("sol_price") or 0)
    ox = snap.get("okx") or {}
    dx = snap.get("dex") or {}
    w = snap.get("wallet") or {}

    mints = [(t.get("mint") or "").strip() for t in (w.get("tokens") or [])[:8]]
    mints = [m for m in mints if m]
    px_map = await _wallet_token_prices_usd(mints)

    okx_eq = float(ox.get("total_equity_usd") or 0)
    dex_v = float(dx.get("total_value_sol") or 0)
    dex_usd = dex_v * sp if sp > 0 else 0.0
    spl_usd = 0.0
    for t in (w.get("tokens") or [])[:8]:
        m = (t.get("mint") or "").strip()
        amt = float(t.get("amount") or 0)
        p = px_map.get(m, 0.0)
        if p > 0 and amt > 0:
            spl_usd += amt * p
    total_nav = okx_eq + dex_usd + spl_usd
    age = float(snap.get("age_sec") or 0)

    lines: list[str] = []
    lines.append("💼 " + escape_markdown("【当前真实持仓】", version=2))
    lines.append(
        escape_markdown(f"总资产净值: ${_fmt_usd(total_nav)}", version=2)
    )
    lines.append(escape_markdown(f"快照 {age:.0f}s 前", version=2))
    lines.append("")

    idx = 0
    if ox.get("has_keys") and ox.get("ok"):
        for row in (ox.get("positions") or [])[:8]:
            idx += 1
            inst = str(row.get("instId") or "?")
            upl = float(row.get("upl") or 0)
            nu = float(row.get("notionalUsd") or 0)
            em = "🟢" if upl >= 0 else "🔴"
            pos = float(row.get("pos") or 0)
            tag = "OKX空单保护" if pos < 0 else "OKX多单"
            core = (
                f"{idx}. ${inst} ({tag}) | 名义 ${nu:,.0f} | "
                f"浮动盈亏: {upl:+.2f} USD"
            )
            lines.append(escape_markdown(core, version=2) + f" {em}")
    for p in (dx.get("positions") or [])[:6]:
        idx += 1
        sym = str(p.get("symbol") or p.get("name") or "?")[:14]
        amt = float(p.get("amount_sol", 0) or 0)
        pnl = float(p.get("pnl_pct", 0) or 0)
        em = "🟢" if pnl >= 0 else "🔴"
        core = f"{idx}. ${sym} (DEX现货) | {amt:.4f} SOL 等 | 浮动盈亏: {pnl:+.1f}%"
        lines.append(escape_markdown(core, version=2) + f" {em}")
    for t in (w.get("tokens") or [])[:6]:
        idx += 1
        lab = str(t.get("label") or "?")[:12]
        amt = float(t.get("amount") or 0)
        m = (t.get("mint") or "").strip()
        p = px_map.get(m, 0.0)
        if p > 0 and amt > 0:
            mv = amt * p
            core = f"{idx}. ${lab} (钱包SPL) | 数量 {_fmt_qty(amt)} | 市值 ~${_fmt_usd(mv)}"
        else:
            core = f"{idx}. ${lab} (钱包SPL) | 数量 {_fmt_qty(amt)} | 浮动盈亏: —"
        lines.append(escape_markdown(core, version=2))

    if idx == 0:
        lines.append(escape_markdown("暂无持仓或等待首次同步。", version=2))

    err = (snap.get("last_error") or "").strip()
    if err:
        lines.append("")
        lines.append(escape_markdown(f"⚠ {err[:200]}", version=2))

    return "\n".join(lines)


def snapshot_to_dashboard_portfolio(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Structured slice for web UI extensions (same numbers as plain summary)."""
    sp = float(snapshot.get("sol_price") or 0)
    ox = snapshot.get("okx") or {}
    dx = snapshot.get("dex") or {}
    okx_eq = float(ox.get("total_equity_usd") or 0)
    dex_usd = float(dx.get("total_value_sol") or 0) * sp if sp > 0 else 0.0
    return {
        "total_nav_usd_approx": okx_eq + dex_usd,
        "okx_equity_usd": okx_eq,
        "dex_value_usd_approx": dex_usd,
        "sol_price": sp,
        "age_sec": snapshot.get("age_sec"),
        "okx_positions_preview": (ox.get("positions") or [])[:8],
        "dex_positions_preview": (dx.get("positions") or [])[:8],
    }
