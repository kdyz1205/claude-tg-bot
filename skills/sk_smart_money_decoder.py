"""
skills/sk_smart_money_decoder.py — Smart-money concentration decoder.

Algorithm
---------
1. DexScreener  → pool liquidity + price + 24h volume
2. Ethplorer    → top-holder distribution (EVM only, free key available)
3. Heuristic scoring:
   - Top-10 concentration pct  → 0-40 pts  (higher = more suspicious)
   - Liquidity depth            → 0-30 pts  (deeper = safer)
   - Volume / MCap ratio        → 0-30 pts  (healthier ratio = safer)
4. Outputs a Telegram MarkdownV2-safe report + raw dict.

Complexity: O(H) where H ≤ 100 top-holders.  Results cached in-process for
CACHE_TTL seconds to avoid RPC hammering.

Resilience: exponential back-off with circuit breaker (3 consecutive fails →
  open circuit for CIRCUIT_OPEN_SECS seconds, return degraded result).
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import aiohttp

from self_monitor import trigger_alert

logger = logging.getLogger(__name__)

CACHE_TTL = 120  # seconds
CIRCUIT_OPEN_SECS = 120

_DEXSCREENER_BASE = "https://api.dexscreener.com/latest/dex/tokens"
_ETHPLORER_BASE = "https://api.ethplorer.io"

# ── In-process cache + circuit state ─────────────────────────────────────────

_cache: Dict[str, tuple[float, Any]] = {}
_circuit: Dict[str, dict] = {}  # key → {fails, open_until}


def _is_circuit_open(key: str) -> bool:
    c = _circuit.get(key, {})
    return time.monotonic() < c.get("open_until", 0)


def _record_fail(key: str) -> None:
    c = _circuit.setdefault(key, {"fails": 0, "open_until": 0})
    c["fails"] += 1
    if c["fails"] >= 3:
        c["open_until"] = time.monotonic() + CIRCUIT_OPEN_SECS
        c["fails"] = 0
        logger.warning("SmartMoneyDecoder: circuit OPEN for %s (%ds)", key, CIRCUIT_OPEN_SECS)


def _reset_circuit(key: str) -> None:
    _circuit.pop(key, None)


# ── Fetch helpers ─────────────────────────────────────────────────────────────

async def _fetch_json(
    session: aiohttp.ClientSession,
    url: str,
    circuit_key: str,
    *,
    params: Optional[Dict[str, str]] = None,
    retries: int = 3,
) -> Optional[Any]:
    if _is_circuit_open(circuit_key):
        return None
    delay = 1.0
    for attempt in range(retries):
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=10.0),
            ) as resp:
                if resp.status == 429:
                    await asyncio.sleep(delay * (attempt + 1))
                    continue
                if resp.status != 200:
                    logger.debug("SmartMoney: HTTP %s %s", resp.status, url[:80])
                    _record_fail(circuit_key)
                    return None
                _reset_circuit(circuit_key)
                return await resp.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.debug("SmartMoney fetch attempt %d/%d: %s", attempt + 1, retries, e)
            _record_fail(circuit_key)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 8.0)
    return None


# ── Scoring ───────────────────────────────────────────────────────────────────

@dataclass
class SmartMoneyReport:
    token_address: str
    chain_hint: str
    symbol: str = "?"
    name: str = "?"
    price_usd: float = 0.0
    liquidity_usd: float = 0.0
    volume_24h_usd: float = 0.0
    market_cap_usd: float = 0.0
    top10_concentration_pct: float = 0.0
    top_holders: List[Dict[str, Any]] = field(default_factory=list)
    risk_score: int = 0          # 0 = safest, 100 = most dangerous
    risk_label: str = "UNKNOWN"
    flags: List[str] = field(default_factory=list)
    degraded: bool = False       # True when some data sources failed


def _concentration_risk(top10_pct: float) -> tuple[int, Optional[str]]:
    """Map top-10 holder concentration % → (risk_points 0-40, flag|None)."""
    if top10_pct >= 80:
        return 40, "⚠️ 顶部10地址持仓>80%（高度集中，老鼠仓风险）"
    if top10_pct >= 60:
        return 28, "⚡ 顶部10地址持仓>60%（中度集中）"
    if top10_pct >= 40:
        return 16, None
    return 0, None


def _liquidity_risk(liq: float) -> int:
    """Higher liquidity → lower risk contribution (0 = safe, 30 = risky)."""
    if liq >= 1_000_000:
        return 0
    if liq >= 500_000:
        return 5
    if liq >= 100_000:
        return 12
    if liq >= 10_000:
        return 22
    return 30


def _vol_ratio_risk(vol: float, mcap: float) -> tuple[int, Optional[str]]:
    if mcap <= 0:
        return 10, None
    ratio = vol / mcap
    if ratio >= 0.5:
        return 0, None  # healthy
    if ratio >= 0.1:
        return 10, None
    return 20, "⚠️ 交易量/市值比率极低（流动性风险）"


def _compute_risk(report: SmartMoneyReport) -> None:
    score = 0
    flags: List[str] = []

    c_pts, c_flag = _concentration_risk(report.top10_concentration_pct)
    score += c_pts
    if c_flag:
        flags.append(c_flag)

    score += _liquidity_risk(report.liquidity_usd)

    v_pts, v_flag = _vol_ratio_risk(report.volume_24h_usd, report.market_cap_usd)
    score += v_pts
    if v_flag:
        flags.append(v_flag)

    report.risk_score = min(100, score)
    if score < 20:
        report.risk_label = "🟢 低风险"
    elif score < 45:
        report.risk_label = "🟡 中风险"
    else:
        report.risk_label = "🔴 高风险"
    report.flags = flags


# ── Main entry ────────────────────────────────────────────────────────────────

async def decode_smart_money_panel(
    token_address: str,
    chain_hint: str = "ethereum",
) -> str:
    """
    Skill entry point.

    Args:
        token_address: Contract address (0x… for EVM; base58 for Solana).
        chain_hint: 'ethereum', 'bsc', 'solana', etc.

    Returns:
        Telegram Markdown formatted report string.
    """
    cache_key = f"{chain_hint}:{token_address.lower()}"
    now = time.time()
    if cache_key in _cache:
        ts, cached = _cache[cache_key]
        if now - ts < CACHE_TTL:
            return cached

    report = SmartMoneyReport(
        token_address=token_address,
        chain_hint=chain_hint,
    )

    timeout = aiohttp.ClientTimeout(total=20.0)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            await asyncio.gather(
                _enrich_dexscreener(session, report),
                _enrich_ethplorer(session, report),
                return_exceptions=True,
            )
    except Exception as e:
        await trigger_alert("SmartMoneyDecoder", f"Session error: {e}", severity="warning")
        report.degraded = True

    _compute_risk(report)
    text = _format_report(report)
    _cache[cache_key] = (now, text)
    return text


async def _enrich_dexscreener(
    session: aiohttp.ClientSession,
    report: SmartMoneyReport,
) -> None:
    url = f"{_DEXSCREENER_BASE}/{report.token_address}"
    data = await _fetch_json(session, url, "dexscreener")
    if not data:
        report.degraded = True
        return
    pairs = data.get("pairs") or []
    if not pairs:
        return
    # Use the pool with highest liquidity
    best = max(pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
    base = best.get("baseToken", {})
    report.symbol = base.get("symbol", "?")
    report.name = base.get("name", "?")
    try:
        report.price_usd = float(best.get("priceUsd") or 0)
        report.liquidity_usd = float((best.get("liquidity") or {}).get("usd") or 0)
        report.volume_24h_usd = float((best.get("volume") or {}).get("h24") or 0)
        fdv = best.get("fdv")
        if fdv:
            report.market_cap_usd = float(fdv)
    except (TypeError, ValueError):
        report.degraded = True


async def _enrich_ethplorer(
    session: aiohttp.ClientSession,
    report: SmartMoneyReport,
) -> None:
    if report.chain_hint not in ("ethereum", "eth"):
        return
    ethplorer_token = os.getenv("ETHPLORER_API_KEY", "freekey")
    url = f"{_ETHPLORER_BASE}/getTopTokenHolders/{report.token_address}"
    data = await _fetch_json(
        session, url, "ethplorer", params={"apiKey": ethplorer_token, "limit": "20"}
    )
    if not data:
        report.degraded = True
        return
    holders = data.get("holders") or []
    if not holders:
        return
    report.top_holders = [
        {"address": h.get("address", "?"), "share": float(h.get("share", 0))}
        for h in holders[:10]
    ]
    report.top10_concentration_pct = sum(h["share"] for h in report.top_holders)


def _format_report(r: SmartMoneyReport) -> str:
    addr_short = r.token_address[:6] + "…" + r.token_address[-4:]
    degraded_note = "\n⚠️ _部分数据源失败，报告可能不完整_" if r.degraded else ""

    lines = [
        f"🔍 **聪明钱解码 — {r.symbol} ({r.name})**",
        f"合约: `{addr_short}` | 链: {r.chain_hint}",
        "",
        f"💰 价格: **${r.price_usd:,.6f}**",
        f"💧 流动性: **${r.liquidity_usd:,.0f}**",
        f"📊 24h 成交量: **${r.volume_24h_usd:,.0f}**",
    ]
    if r.market_cap_usd:
        lines.append(f"🏦 FDV/MCap: **${r.market_cap_usd:,.0f}**")

    lines += ["", f"👥 顶部10持仓集中度: **{r.top10_concentration_pct:.1f}%**"]
    for i, h in enumerate(r.top_holders[:5], 1):
        lines.append(f"  {i}. `{h['address'][:8]}…` — {h['share']:.1f}%")

    lines += [
        "",
        f"🎯 风险评分: **{r.risk_score}/100** — {r.risk_label}",
    ]
    for flag in r.flags:
        lines.append(flag)

    lines.append(degraded_note)
    return "\n".join(lines)


# ── Hot-cache alpha (on-chain only, no K-line / no technicals) ────────────────

async def analyze(
    token_address: str,
    chain_hint: str = "solana",
) -> Dict[str, Any]:
    """
    Read ``onchain_tracker.HotTokenCache`` only.

    If this mint was bought (≥10 SOL swap from hardcoded targets) by **more than two**
    distinct target wallets within the last 5 minutes, return a high-confidence buy dict.
    Otherwise returns ``action: none`` — no DexScreener, Ethplorer, or chart data.
    """
    addr = str(token_address or "").strip()
    if not addr:
        return {"action": "none", "confidence": 0.0, "target_asset": ""}

    hint = (chain_hint or "solana").lower()
    if hint not in ("sol", "solana"):
        return {"action": "none", "confidence": 0.0, "target_asset": addr}

    from onchain_tracker import PARASITE_CONSENSUS_WINDOW_SEC, hot_token_cache

    n = await hot_token_cache.distinct_wallets_in_window(addr, float(PARASITE_CONSENSUS_WINDOW_SEC))
    if n > 2:
        return {
            "action": "buy",
            "confidence": 0.99,
            "target_asset": addr,
        }
    return {"action": "none", "confidence": 0.0, "target_asset": addr}


# ── Skill interface contract ──────────────────────────────────────────────────

SKILL_METADATA = {
    "id": "sk_smart_money_decoder",
    "title": "聪明钱集中度解码",
    "description": "DexScreener + Ethplorer 多源聪明钱持仓集中度分析",
    "task_type": "analysis",
    "function": "decode_smart_money_panel",
    "input_schema": {
        "token_address": "str — EVM 0x… 或 Solana base58",
        "chain_hint": "str optional — 'ethereum'|'bsc'|'solana'",
    },
    "output_schema": "Telegram Markdown 报告字符串",
    "analyze_function": "analyze",
    "analyze_schema": '{"action": str, "confidence": float, "target_asset": str}',
}


async def run_skill(params: Dict[str, Any]) -> str:
    addr = str(params.get("token_address", "")).strip()
    if not addr:
        return "❌ 缺少 token_address 参数"
    chain = str(params.get("chain_hint", "ethereum"))
    return await decode_smart_money_panel(addr, chain)
