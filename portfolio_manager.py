"""
Core risk & dynamic sizing engine + live portfolio presentation.

This module is the **canonical entry** for production DEX leg sizing: Kelly-style
fractions per ``skill_id`` (persisted trade stats) are merged with
``trading_skills.drawdown_guardian.DrawdownGuardian.scale_kelly_fraction`` so that
drawdown, recovery mode, cooldown, and shutdown states shrink or zero the bet **before**
converting USD notional to absolute SOL.

The functions below (``calculate_kelly_position_size``, ``kelly_fraction_for_skill``, etc.)
implement the risk engine. The remainder of the file preserves the original Telegram /
dashboard formatters unchanged.

Live portfolio summary for Telegram (MarkdownV2) and plain chain dashboard text.

Uses trading.portfolio_snapshot for OKX + wallet + DEX; optional Jupiter prices for SPL.

OKX live vs local ledger sync: ``trading.reconciliation_daemon`` (default every 15 min;
``RECONCILE_INTERVAL_SEC``). See ``OKXExecutor.reconcile_state_with_exchange``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from trading_skills.drawdown_guardian import DrawdownGuardian

logger = logging.getLogger(__name__)

# Persisted per-skill stats for Kelly (wins/losses and gross PnL fractions).
_STATS_LOCK = threading.Lock()
_STATS_PATH = Path(
    os.environ.get(
        "PORTFOLIO_KELLY_STATS_PATH",
        str(Path(__file__).resolve().parent / "_skill_kelly_stats.json"),
    )
)
_MIN_TRADES_FOR_KELLY = max(3, int(os.environ.get("PORTFOLIO_KELLY_MIN_TRADES", "5")))
_DEFAULT_FRACTION_NO_STATS = float(os.environ.get("PORTFOLIO_KELLY_DEFAULT_FRAC", "0.02"))
_MAX_KELLY_CAP = float(os.environ.get("PORTFOLIO_KELLY_MAX_FRAC", "0.25"))
_KELLY_SCALAR = float(os.environ.get("PORTFOLIO_KELLY_SCALAR", "0.5"))  # half-Kelly default


def _load_kelly_stats() -> dict[str, Any]:
    try:
        if _STATS_PATH.is_file():
            raw = _STATS_PATH.read_text(encoding="utf-8")
            data = json.loads(raw)
            return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("Kelly stats load failed: %s", e)
    return {}


def _save_kelly_stats(obj: dict[str, Any]) -> None:
    try:
        _STATS_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = str(_STATS_PATH) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, str(_STATS_PATH))
    except OSError as e:
        logger.error("Kelly stats save failed: %s", e)


def _skill_bucket(stats: dict[str, Any], skill_id: str) -> dict[str, Any]:
    skills = stats.setdefault("skills", {})
    if not isinstance(skills, dict):
        skills = {}
        stats["skills"] = skills
    sid = str(skill_id or "").strip() or "default"
    cur = skills.get(sid)
    if not isinstance(cur, dict):
        cur = {"n_win": 0, "n_loss": 0, "sum_win": 0.0, "sum_loss": 0.0}
        skills[sid] = cur
    return cur


def record_skill_trade_outcome(skill_id: str, pnl_fraction: float) -> None:
    """
    Feed realized PnL fraction (e.g. 0.03 = +3%) to update Kelly inputs for ``skill_id``.
    Thread-safe; persists to ``_skill_kelly_stats.json`` (or ``PORTFOLIO_KELLY_STATS_PATH``).
    """
    try:
        x = float(pnl_fraction)
    except (TypeError, ValueError):
        return
    with _STATS_LOCK:
        stats = _load_kelly_stats()
        b = _skill_bucket(stats, skill_id)
        if x >= 0:
            b["n_win"] = int(b.get("n_win", 0)) + 1
            b["sum_win"] = float(b.get("sum_win", 0.0)) + x
        else:
            b["n_loss"] = int(b.get("n_loss", 0)) + 1
            b["sum_loss"] = float(b.get("sum_loss", 0.0)) + abs(x)
        _save_kelly_stats(stats)


def _half_kelly_fraction(p: float, avg_win: float, avg_loss: float) -> float:
    """Classic Kelly f* = (p*b - q)/b with b = avg_win/avg_loss; return conservative scalar * f*."""
    if p <= 0 or p >= 1:
        return 0.0
    if avg_win <= 0 or avg_loss <= 0:
        return 0.0
    b = avg_win / avg_loss
    q = 1.0 - p
    k = (p * b - q) / b if b > 1e-12 else 0.0
    if k <= 0:
        return 0.0
    return max(0.0, min(_MAX_KELLY_CAP, k * _KELLY_SCALAR))


def kelly_fraction_for_skill(skill_id: str) -> float:
    """
    Raw Kelly-based **fraction of equity** (before drawdown guardian), ∈ [0, _MAX_KELLY_CAP].
    Uses persisted wins/losses; if below ``_MIN_TRADES_FOR_KELLY``, falls back to
    ``_DEFAULT_FRACTION_NO_STATS``.
    """
    with _STATS_LOCK:
        stats = _load_kelly_stats()
        b = _skill_bucket(stats, skill_id)
        nw = int(b.get("n_win", 0))
        nl = int(b.get("n_loss", 0))
        sw = float(b.get("sum_win", 0.0))
        sl = float(b.get("sum_loss", 0.0))
    n = nw + nl
    if n < _MIN_TRADES_FOR_KELLY:
        return max(0.0, min(_MAX_KELLY_CAP, _DEFAULT_FRACTION_NO_STATS))
    p = nw / n if n else 0.0
    avg_win = sw / nw if nw else 0.0
    avg_loss = sl / nl if nl else 0.0
    if nw == 0 or nl == 0:
        # One-sided history: stay defensive
        return max(0.0, min(_MAX_KELLY_CAP, _DEFAULT_FRACTION_NO_STATS))
    return _half_kelly_fraction(p, avg_win, avg_loss)


def calculate_kelly_position_size(
    skill_id: str,
    current_equity: float,
    *,
    drawdown_guardian: DrawdownGuardian | None = None,
    sol_price_usd: float | None = None,
) -> float:
    """
    Production sizing: Kelly fraction for ``skill_id`` → ``DrawdownGuardian.scale_kelly_fraction``
    → USD notional → **absolute SOL** for the DEX leg.

    Parameters
    ----------
    skill_id:
        Key for persisted stats (e.g. ``sk_oib_momentum``).
    current_equity:
        Account equity in **USD** (same numeraire as Kelly fraction).
    drawdown_guardian:
        Live guardian updated on equity ticks; if ``None``, a fresh guardian is updated once
        with ``current_equity`` (neutral multiplier when no history — tests / cold start).
    sol_price_usd:
        SOL/USD; if omitted, uses ``trading.portfolio_snapshot`` then env
        ``PORTFOLIO_FALLBACK_SOL_PRICE_USD``.
    """
    try:
        eq = float(current_equity)
    except (TypeError, ValueError):
        return 0.0
    if eq <= 0:
        return 0.0

    base_f = kelly_fraction_for_skill(skill_id)
    if drawdown_guardian is not None:
        eff_f = drawdown_guardian.scale_kelly_fraction(base_f)
    else:
        from trading_skills.drawdown_guardian import DrawdownGuardian

        g = DrawdownGuardian()
        g.update(eq)
        eff_f = g.scale_kelly_fraction(base_f)

    notional_usd = eq * eff_f
    px = _resolve_sol_price_usd(sol_price_usd)
    if px <= 0:
        logger.warning("calculate_kelly_position_size: invalid SOL/USD price, returning 0 SOL")
        return 0.0
    sol = notional_usd / px
    return max(0.0, float(sol))


def _resolve_sol_price_usd(hint: float | None) -> float:
    if hint is not None:
        try:
            h = float(hint)
            if h > 0:
                return h
        except (TypeError, ValueError):
            pass
    try:
        from trading.portfolio_snapshot import get_snapshot

        s = get_snapshot()
        p = float(s.get("sol_price") or 0)
        if p > 0:
            return p
    except Exception:
        pass
    try:
        env = float(os.environ.get("PORTFOLIO_FALLBACK_SOL_PRICE_USD") or 0)
        return env if env > 0 else 0.0
    except (TypeError, ValueError):
        return 0.0

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


def format_chain_snapshot(snapshot: dict[str, Any]) -> str:
    """
    /chain 资金块：按交易市场分栏（OKX / Solana 链上 / DEX 记账 / Polymarket）。
    不把各所混成一条「总净值」；SPL 标签不加前导 $。
    """
    sp = float(snapshot.get("sol_price") or 0)
    ox = snapshot.get("okx") or {}
    dx = snapshot.get("dex") or {}
    w = snapshot.get("wallet") or {}
    poly = snapshot.get("poly") or {}
    lines: list[str] = []
    age = float(snapshot.get("age_sec") or 0)
    lines.append(f"快照 {age:.0f}s 前 · 按市场分栏（OKX ≠ 链上 ≠ DEX ≠ Polymarket）")
    lines.append("")

    lines.append("━━ OKX · 中心化所 ━━")
    okx_eq = float(ox.get("total_equity_usd") or 0)
    if ox.get("has_keys") and ox.get("ok"):
        lines.append(f"权益 ~${_fmt_usd(okx_eq)} · 可用 USDT {float(ox.get('usdt_available') or 0):,.2f}")
        for row in (ox.get("positions") or [])[:8]:
            inst = row.get("instId") or "?"
            upl = float(row.get("upl") or 0)
            nu = float(row.get("notionalUsd") or 0)
            em = "🟢" if upl >= 0 else "🔴"
            sd = "空" if float(row.get("pos") or 0) < 0 else "多"
            lines.append(f" · {inst} {sd} 名义${nu:,.0f} 浮盈{em}${upl:+,.2f}")
        if not (ox.get("positions") or []):
            lines.append(" · 无挂单持仓")
    elif ox.get("has_keys") and not ox.get("ok"):
        lines.append(f"✗ {str(ox.get('error') or '?')[:80]}")
    else:
        lines.append("未配置 API 密钥")
    lines.append("")

    lines.append("━━ Solana · 链上钱包 ━━")
    sol_bal = float(w.get("sol_bal") or 0)
    tc = int(w.get("token_count") or 0)
    usd_sol = sol_bal * sp if sp > 0 else 0.0
    pk = (w.get("pubkey_short") or "").strip() or "?"
    if w.get("ok"):
        lines.append(f"{pk} · {sol_bal:.4f} SOL (~${_fmt_usd(usd_sol)}) · SPL {tc} 种")
        for t in (w.get("tokens") or [])[:10]:
            lab = str(t.get("label") or "?")[:18].strip()
            amt = float(t.get("amount") or 0)
            m = (t.get("mint") or "").strip()
            tail = f" ({m[:4]}…{m[-4:]})" if len(m) > 10 else ""
            lines.append(f" · {lab}{tail}  {_fmt_qty(amt)}")
        if not (w.get("tokens") or []):
            lines.append(" · 无 SPL（或仅 SOL）")
    else:
        lines.append("未就绪 → /wallet_setup")
    lines.append("")

    dex_v = float(dx.get("total_value_sol") or 0)
    dex_pos = list(dx.get("positions") or [])
    lines.append("━━ DEX · 引擎/Jupiter 跟踪 ━━")
    lines.append("（本地策略仓位；与 OKX 订单、与上方 SPL 余额不是同一套账）")
    if dex_pos:
        lines.append(f"共 {len(dex_pos)} 笔 · 约 {dex_v:.3f} SOL 敞口")
        for p in sorted(dex_pos, key=lambda x: float(x.get("amount_sol", 0) or 0), reverse=True)[:8]:
            sym = (p.get("symbol") or p.get("name") or "?")[:14]
            amt = float(p.get("amount_sol", 0) or 0)
            pnl = float(p.get("pnl_pct", 0) or 0)
            em = "🟢" if pnl >= 0 else "🔴"
            lines.append(f" · {sym} {amt:.2f}SOL {em}{pnl:+.1f}%")
    else:
        lines.append("无跟踪仓")
    lines.append("")

    lines.append("━━ Polymarket · Polygon CLOB ━━")
    lines.append("（Polygon 条件代币；与 Solana 链、OKX、上方 DEX 无关）")
    if poly.get("configured"):
        oe = "开" if poly.get("oracle_enabled") else "关"
        lines.append(f"密钥: 已配置 · live 神谕: {oe}")
    else:
        lines.append("密钥: 未配置（POLYMARKET_PRIVATE_KEY / POLY_PRIVATE_KEY）")
    rec = list(poly.get("recent") or [])
    if rec:
        lines.append("最近执行:")
        for r in rec:
            ts = float(r.get("ts") or 0)
            tss = time.strftime("%m-%d %H:%M", time.localtime(ts)) if ts > 0 else "—"
            okm = "✓" if r.get("ok") else "✗"
            tid = str(r.get("token_id") or "?")
            lines.append(f" · [{tss}] ~${float(r.get('stake_usd') or 0):.2f} {okm} {tid}")
    else:
        lines.append("最近执行: 无")
    perr = str(poly.get("error") or "").strip()
    if perr:
        lines.append(f"⚠ Poly 腿: {perr[:80]}")

    err = (snapshot.get("last_error") or "").strip()
    if err:
        lines.append("")
        lines.append(f"⚠ {err[:100]}")
    return "\n".join(lines)


def format_chain_compact(snapshot: dict[str, Any]) -> str:
    """
    Short overview for /chain first message (callbacks + keyboard).
    No per-token enumeration — details go in format_chain_snapshot_chunks.
    """
    sp = float(snapshot.get("sol_price") or 0)
    ox = snapshot.get("okx") or {}
    dx = snapshot.get("dex") or {}
    w = snapshot.get("wallet") or {}
    poly = snapshot.get("poly") or {}
    age = float(snapshot.get("age_sec") or 0)
    lines: list[str] = ["【速览·四市场】", f"快照约 {age:.0f}s 前 · OKX ≠ 链上 ≠ DEX ≠ Poly"]

    if ox.get("has_keys") and ox.get("ok"):
        eq = float(ox.get("total_equity_usd") or 0)
        u = float(ox.get("usdt_available") or 0)
        np = len(ox.get("positions") or [])
        lines.append(f"OKX: ~${_fmt_usd(eq)} · 可用 {u:.2f} USDT · 挂单/持仓 {np} 笔")
    elif ox.get("has_keys") and not ox.get("ok"):
        lines.append(f"OKX: 异常 {str(ox.get('error') or '?')[:72]}")
    else:
        lines.append("OKX: 未配置 API 密钥")

    sol_bal = float(w.get("sol_bal") or 0)
    tc = int(w.get("token_count") or 0)
    pk = (w.get("pubkey_short") or "").strip() or "?"
    usd_sol = sol_bal * sp if sp > 0 else 0.0
    if w.get("ok"):
        lines.append(f"Solana: {pk} · {sol_bal:.4f} SOL (~${_fmt_usd(usd_sol)}) · SPL {tc} 种")
    else:
        lines.append("Solana: 未就绪 → /wallet_setup")

    dex_pos = list(dx.get("positions") or [])
    dex_v = float(dx.get("total_value_sol") or 0)
    if dex_pos:
        lines.append(f"DEX 记账: {len(dex_pos)} 笔 · 约 {dex_v:.3f} SOL 敞口")
    else:
        lines.append("DEX 记账: 无跟踪仓")

    if poly.get("configured"):
        oe = "开" if poly.get("oracle_enabled") else "关"
        lines.append(f"Polymarket: 已配置 · live 神谕 {oe}")
    else:
        lines.append("Polymarket: 未配置（可忽略）")

    err = (snapshot.get("last_error") or "").strip()
    if err:
        lines.append(f"⚠ {err[:90]}")
    return "\n".join(lines)


def format_chain_snapshot_chunks(
    snapshot: dict[str, Any], *, max_spl: int = 6
) -> list[str]:
    """
    Split the chain snapshot into several Telegram-sized messages (headers OKX / Sol / DEX+Poly).
    """
    sp = float(snapshot.get("sol_price") or 0)
    ox = snapshot.get("okx") or {}
    dx = snapshot.get("dex") or {}
    w = snapshot.get("wallet") or {}
    poly = snapshot.get("poly") or {}
    age = float(snapshot.get("age_sec") or 0)

    chunks: list[str] = []

    okx_lines: list[str] = [
        "📊 明细 ①/③ · OKX · 中心化所",
        f"（{age:.0f}s 前 · 与链上/DEX 不同账）",
        "",
    ]
    okx_eq = float(ox.get("total_equity_usd") or 0)
    if ox.get("has_keys") and ox.get("ok"):
        okx_lines.append(f"权益 ~${_fmt_usd(okx_eq)} · 可用 USDT {float(ox.get('usdt_available') or 0):,.2f}")
        for row in (ox.get("positions") or [])[:8]:
            inst = row.get("instId") or "?"
            upl = float(row.get("upl") or 0)
            nu = float(row.get("notionalUsd") or 0)
            em = "🟢" if upl >= 0 else "🔴"
            sd = "空" if float(row.get("pos") or 0) < 0 else "多"
            okx_lines.append(f" · {inst} {sd} 名义${nu:,.0f} 浮盈{em}${upl:+,.2f}")
        if not (ox.get("positions") or []):
            okx_lines.append(" · 无挂单持仓")
    elif ox.get("has_keys") and not ox.get("ok"):
        okx_lines.append(f"✗ {str(ox.get('error') or '?')[:120]}")
    else:
        okx_lines.append("未配置 API 密钥")
    chunks.append("\n".join(okx_lines))

    sol_lines: list[str] = [
        "📊 明细 ②/③ · Solana · 链上钱包",
        "（SPL 为钱包真实余额，非 DEX 策略账）",
        "",
    ]
    sol_bal = float(w.get("sol_bal") or 0)
    tc = int(w.get("token_count") or 0)
    usd_sol = sol_bal * sp if sp > 0 else 0.0
    pk = (w.get("pubkey_short") or "").strip() or "?"
    if w.get("ok"):
        sol_lines.append(f"{pk} · {sol_bal:.4f} SOL (~${_fmt_usd(usd_sol)}) · SPL {tc} 种")
        for t in (w.get("tokens") or [])[:max_spl]:
            lab = str(t.get("label") or "?")[:18].strip()
            amt = float(t.get("amount") or 0)
            m = (t.get("mint") or "").strip()
            tail = f" ({m[:4]}…{m[-4:]})" if len(m) > 10 else ""
            sol_lines.append(f" · {lab}{tail}  {_fmt_qty(amt)}")
        if tc > max_spl:
            sol_lines.append(f" · … 另有 {tc - max_spl} 种未列出")
        if not (w.get("tokens") or []):
            sol_lines.append(" · 无 SPL（或仅 SOL）")
    else:
        sol_lines.append("未就绪 → /wallet_setup")
    chunks.append("\n".join(sol_lines))

    dex_lines: list[str] = [
        "📊 明细 ③/③ · DEX + Polymarket",
        "",
        "━━ DEX · 引擎/Jupiter 跟踪 ━━",
        "（本地策略仓位；与 OKX、上方 SPL 不是同一套账）",
    ]
    dex_v = float(dx.get("total_value_sol") or 0)
    dex_pos = list(dx.get("positions") or [])
    if dex_pos:
        dex_lines.append(f"共 {len(dex_pos)} 笔 · 约 {dex_v:.3f} SOL 敞口")
        for p in sorted(dex_pos, key=lambda x: float(x.get("amount_sol", 0) or 0), reverse=True)[:8]:
            sym = (p.get("symbol") or p.get("name") or "?")[:14]
            amt = float(p.get("amount_sol", 0) or 0)
            pnl = float(p.get("pnl_pct", 0) or 0)
            em = "🟢" if pnl >= 0 else "🔴"
            dex_lines.append(f" · {sym} {amt:.2f}SOL {em}{pnl:+.1f}%")
    else:
        dex_lines.append("无跟踪仓")
    dex_lines.extend(["", "━━ Polymarket · Polygon CLOB ━━"])
    if poly.get("configured"):
        oe = "开" if poly.get("oracle_enabled") else "关"
        dex_lines.append(f"密钥已配置 · live 神谕: {oe}")
    else:
        dex_lines.append("未配置（POLYMARKET_PRIVATE_KEY / POLY_PRIVATE_KEY）— 可忽略")
    rec = list(poly.get("recent") or [])
    if rec:
        dex_lines.append("最近执行:")
        for r in rec[:5]:
            ts = float(r.get("ts") or 0)
            tss = time.strftime("%m-%d %H:%M", time.localtime(ts)) if ts > 0 else "—"
            okm = "✓" if r.get("ok") else "✗"
            tid = str(r.get("token_id") or "?")
            dex_lines.append(f" · [{tss}] ~${float(r.get('stake_usd') or 0):.2f} {okm} {tid[:20]}")
    else:
        dex_lines.append("最近执行: 无")
    perr = str(poly.get("error") or "").strip()
    if perr:
        dex_lines.append(f"⚠ Poly: {perr[:100]}")
    chunks.append("\n".join(dex_lines))

    err = (snapshot.get("last_error") or "").strip()
    if err:
        chunks.append(f"⚠ 快照提示\n{err[:200]}")
    return chunks


def format_portfolio_plain(snapshot: dict[str, Any]) -> str:
    """Plain-text fallback; mirrors format_chain_snapshot 分栏."""
    return format_chain_snapshot(snapshot)


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
    poly = snap.get("poly") or {}

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
    lines.append("💼 " + escape_markdown("【真实持仓 · 分市场】", version=2))
    lines.append(
        escape_markdown(
            f"参考合计(粗): ${_fmt_usd(total_nav)} · 快照 {age:.0f}s 前",
            version=2,
        )
    )
    lines.append(
        escape_markdown("OKX / Solana / DEX / Polymarket 分栏；勿与 paper 混读", version=2)
    )
    lines.append("")

    idx = 0
    lines.append(escape_markdown("━━ OKX · 中心化 ━━", version=2))
    if ox.get("has_keys") and ox.get("ok"):
        lines.append(
            escape_markdown(
                f"权益 ~${_fmt_usd(okx_eq)} · 可用 USDT {float(ox.get('usdt_available') or 0):,.2f}",
                version=2,
            )
        )
        if not (ox.get("positions") or []):
            lines.append(escape_markdown("无挂单持仓", version=2))
        else:
            for row in (ox.get("positions") or [])[:8]:
                idx += 1
                inst = str(row.get("instId") or "?")
                upl = float(row.get("upl") or 0)
                nu = float(row.get("notionalUsd") or 0)
                em = "🟢" if upl >= 0 else "🔴"
                pos = float(row.get("pos") or 0)
                tag = "OKX空单保护" if pos < 0 else "OKX多单"
                core = (
                    f"{idx}. {inst} ({tag}) | 名义 ${nu:,.0f} | "
                    f"浮动盈亏: {upl:+.2f} USD"
                )
                lines.append(escape_markdown(core, version=2) + f" {em}")
    elif ox.get("has_keys") and not ox.get("ok"):
        lines.append(escape_markdown(str(ox.get("error") or "?")[:100], version=2))
    else:
        lines.append(escape_markdown("未配置 API 密钥", version=2))
    lines.append("")

    lines.append(escape_markdown("━━ Solana · 链上钱包 ━━", version=2))
    if w.get("ok"):
        sol_bal = float(w.get("sol_bal") or 0)
        usd_sol = sol_bal * sp if sp > 0 else 0.0
        pk = (w.get("pubkey_short") or "").strip() or "?"
        lines.append(
            escape_markdown(
                f"{pk} · {sol_bal:.4f} SOL (~${_fmt_usd(usd_sol)})",
                version=2,
            )
        )
        if not (w.get("tokens") or []):
            lines.append(escape_markdown("无 SPL 代币行", version=2))
        else:
            for t in (w.get("tokens") or [])[:8]:
                idx += 1
                lab = str(t.get("label") or "?")[:14]
                amt = float(t.get("amount") or 0)
                m = (t.get("mint") or "").strip()
                p = px_map.get(m, 0.0)
                if p > 0 and amt > 0:
                    mv = amt * p
                    core = (
                        f"{idx}. {lab} (SPL) | {_fmt_qty(amt)} | 估市值 ~${_fmt_usd(mv)}"
                    )
                else:
                    core = f"{idx}. {lab} (SPL) | {_fmt_qty(amt)} | 估价 —"
                lines.append(escape_markdown(core, version=2))
    else:
        lines.append(escape_markdown("钱包未就绪", version=2))
    lines.append("")

    lines.append(escape_markdown("━━ DEX · 引擎跟踪 ━━", version=2))
    dex_rows = dx.get("positions") or []
    if not dex_rows:
        lines.append(escape_markdown("无", version=2))
    else:
        for p in dex_rows[:8]:
            idx += 1
            sym = str(p.get("symbol") or p.get("name") or "?")[:14]
            amt = float(p.get("amount_sol", 0) or 0)
            pnl = float(p.get("pnl_pct", 0) or 0)
            em = "🟢" if pnl >= 0 else "🔴"
            core = f"{idx}. {sym} | {amt:.4f} SOL | {pnl:+.1f}%"
            lines.append(escape_markdown(core, version=2) + f" {em}")
    lines.append("")

    lines.append(escape_markdown("━━ Polymarket · Polygon ━━", version=2))
    if poly.get("configured"):
        oe = "开" if poly.get("oracle_enabled") else "关"
        lines.append(escape_markdown(f"密钥已配置 · 神谕 {oe}", version=2))
    else:
        lines.append(escape_markdown("未配置 Polymarket 密钥", version=2))
    rec = list(poly.get("recent") or [])
    if not rec:
        lines.append(escape_markdown("最近执行: 无", version=2))
    else:
        for r in rec:
            ts = float(r.get("ts") or 0)
            tss = time.strftime("%m-%d %H:%M", time.localtime(ts)) if ts > 0 else "—"
            okm = "✓" if r.get("ok") else "✗"
            tid = str(r.get("token_id") or "?")
            core = f"[{tss}] ~${float(r.get('stake_usd') or 0):.2f} {okm} {tid}"
            lines.append(escape_markdown(core, version=2))

    if idx == 0 and not dex_rows and not rec and not (ox.get("positions") or []):
        lines.append("")
        lines.append(escape_markdown("各所暂无仓位或等待首次同步。", version=2))

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
