"""
非结构化文本 / 链接 → 极速情绪打分 + 实体（币）提取 → 可选事件驱动 DEX 小单。

- 文本：直接送 LLM。
- URL：优先经 r.jina.ai 读取正文（含 X/Twitter），失败则简单 HTML 去标签兜底。
- 情绪 ∈ [-1,1]；> SENTIMENT_EXTREME_LONG_THRESHOLD 且 Dex 流动性达标时，
  绕过技术因子，用 EVENT_SNIPER_SOL 在 paper /（未来）live 下打极小仓。
"""

from __future__ import annotations

import logging
import re
from typing import Any

import config
import httpx

from llm_http_client import complete_stateless, extract_json_object_from_llm_text, resolve_backend

logger = logging.getLogger(__name__)

_URL_RE = re.compile(r"https?://[^\s<>\")']+", re.I)

_SYS = """你是加密市场情绪分析器。根据给定正文（可能来自新闻或推文），输出严格 JSON（不要 markdown 围栏），键为:
- sentiment: 浮点数，-1=极端看跌，0=中性，1=极端看涨
- primary_symbol: 正文最核心看涨标的的 ticker，如 BONK、WIF；若无明确单一标的用 null
- primary_mint: 若正文中出现 Solana 合约地址（32-44 位 base58），填该字符串，否则 null
- coin_notes: 一句中文说明为何选该标的

规则：只输出一个 JSON 对象，无其它文字。"""


def _thresholds() -> tuple[float, float, float]:
    try:
        thr = float(getattr(config, "SENTIMENT_EXTREME_LONG_THRESHOLD", 0.8) or 0.8)
    except (TypeError, ValueError):
        thr = 0.8
    try:
        sol_amt = float(getattr(config, "EVENT_SNIPER_SOL", 0.05) or 0.05)
    except (TypeError, ValueError):
        sol_amt = 0.05
    try:
        liq_min = float(getattr(config, "EVENT_MIN_LIQUIDITY_USD", 50_000) or 50_000)
    except (TypeError, ValueError):
        liq_min = 50_000.0
    return max(-1.0, min(1.0, thr)), max(0.001, sol_amt), max(1000.0, liq_min)


def _fast_model_hint() -> str | None:
    b = resolve_backend()
    if b == "openai":
        return (getattr(config, "TASK_TIER_FAST_OPENAI", None) or "").strip() or None
    if b == "anthropic":
        return (getattr(config, "TASK_TIER_FAST_CLAUDE", None) or "").strip() or None
    return None


def extract_urls(text: str) -> list[str]:
    return list(dict.fromkeys(_URL_RE.findall(text or "")))


def _strip_html_loose(html: str, limit: int = 10_000) -> str:
    s = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", html)
    s = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", s)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:limit]


async def fetch_url_body(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    if not u.lower().startswith("http"):
        u = "https://" + u
    reader = f"https://r.jina.ai/{u}"
    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            r = await client.get(
                reader,
                headers={"User-Agent": "Mozilla/5.0 JarvisSentiment/1.0", "X-Return-Format": "text"},
            )
            if r.status_code == 200 and (r.text or "").strip():
                return (r.text or "").strip()[:14_000]
    except Exception as e:
        logger.debug("jina reader failed %s: %s", u[:80], e)
    try:
        async with httpx.AsyncClient(timeout=18.0, follow_redirects=True) as client:
            r2 = await client.get(u, headers={"User-Agent": "Mozilla/5.0 (compatible; Jarvis/1.0)"})
            if r2.status_code == 200:
                return _strip_html_loose(r2.text or "")
    except Exception as e:
        logger.debug("direct fetch failed %s: %s", u[:80], e)
    return ""


async def build_analysis_text(user_input: str) -> tuple[str, str]:
    """
    Returns (body_for_llm, provenance_note).
    """
    raw = (user_input or "").strip()
    if not raw:
        return "", "empty"
    urls = extract_urls(raw)
    if not urls:
        return raw[:14_000], "inline_text"
    chunks: list[str] = []
    for u in urls[:3]:
        body = await fetch_url_body(u)
        if body:
            chunks.append(f"=== URL {u} ===\n{body}")
    merged = "\n\n".join(chunks) if chunks else raw
    return merged[:14_000], "urls_fetched" if chunks else "urls_failed_use_raw"


async def analyze_sentiment_json(body: str) -> dict[str, Any] | None:
    text, err = await complete_stateless(
        system_prompt=_SYS,
        user_text=(body or "")[:14_000],
        model_hint=_fast_model_hint(),
        timeout_sec=min(45.0, float(getattr(config, "API_REQUEST_TIMEOUT_SEC", 60) or 60)),
        state_key=-71001,
    )
    if err:
        logger.warning("sentiment_feed LLM: %s", err[:300])
        return None
    obj = extract_json_object_from_llm_text(text)
    if not obj:
        logger.warning("sentiment_feed parse failed: %s", (text or "")[:200])
        return None
    return obj


async def resolve_solana_token(mint_or_symbol: str | None) -> dict[str, Any] | None:
    """DexScreener: mint 直查，否则按 symbol 在 solana 上搜流动性最高池。"""
    import dex_trader as dex

    q = (mint_or_symbol or "").strip()
    if not q:
        return None
    if re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", q):
        return await dex.lookup_token(q)
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                "https://api.dexscreener.com/latest/dex/search",
                params={"q": q},
            )
            if resp.status_code != 200:
                return None
            data = resp.json()
            pairs = data.get("pairs") or []
            sol = [p for p in pairs if str(p.get("chainId") or "").lower() == "solana"]
            if not sol:
                return None
            best = max(sol, key=lambda p: float((p.get("liquidity") or {}).get("usd", 0) or 0))
            base = (best.get("baseToken") or {}).get("address")
            if not base:
                return None
            return await dex.lookup_token(str(base))
    except Exception as e:
        logger.debug("resolve_solana_token: %s", e)
        return None


async def process_sentiment_feed(user_input: str, *, user_mode: str) -> str:
    thr, sol_amt, liq_min = _thresholds()
    body, provenance = await build_analysis_text(user_input)
    if not body:
        return "⚠️ 没有可分析的文本或链接正文。"

    parsed = await analyze_sentiment_json(body)
    if not parsed:
        return "⚠️ 情绪模型不可用或返回无法解析，请检查 LLM 密钥与网络。"

    try:
        sentiment = float(parsed.get("sentiment", 0) or 0)
    except (TypeError, ValueError):
        sentiment = 0.0
    sentiment = max(-1.0, min(1.0, sentiment))

    sym = parsed.get("primary_symbol")
    mint_hint = parsed.get("primary_mint")
    notes = str(parsed.get("coin_notes") or "").strip()

    sym_s = str(sym).strip().upper() if sym else ""
    mint_s = str(mint_hint).strip() if mint_hint else ""

    lines = [
        f"📊 情绪: {sentiment:+.3f}（事件做多阈值 > {thr}）",
        f"来源: {provenance}",
    ]
    if sym_s:
        lines.append(f"🪙 标的符号: {sym_s}")
    if notes:
        lines.append(f"📝 {notes[:500]}")

    target = mint_s if re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", mint_s) else sym_s
    if not target:
        lines.append("\n未识别到可交易的 Solana 标的（无 mint / symbol）。")
        return "\n".join(lines)

    info = await resolve_solana_token(target)
    if not info:
        lines.append(f"\nDexScreener 未找到 {target} 的有效 Solana 池。")
        return "\n".join(lines)

    liq = float(info.get("liquidity_usd", 0) or 0)
    lines.append(f"💧 流动性 USD: {liq:,.0f}（要求 ≥ {liq_min:,.0f}）")

    mode = "live" if (user_mode or "").lower() == "live" else "paper"

    if sentiment <= thr:
        lines.append(f"\n情绪未超过 `{thr}`，不触发事件单。")
        return "\n".join(lines)
    if liq < liq_min:
        lines.append("\n流动性不足，不触发事件单。")
        return "\n".join(lines)

    import dex_trader as dex

    pos = dex.execute_buy(info, sol_amt, mode=mode)
    if not pos:
        lines.append(
            f"\n⚡ 条件满足，但下单失败（实盘 DEX 执行尚未接入时请用 paper）。mode=`{mode}`"
        )
        return "\n".join(lines)

    pos["entry_reason"] = "sentiment_event"
    dex.save_positions(dex.get_positions())

    lines.append(
        f"\n⚡ 事件驱动：已用 {sol_amt} SOL 绕过技术因子下单（{mode}）。\n"
        f"{dex.format_buy_result(pos, sol_amt)}"
    )
    return "\n".join(lines)


def is_single_url_message(text: str) -> bool:
    t = (text or "").strip()
    if not t or "\n" in t:
        return False
    urls = extract_urls(t)
    return len(urls) == 1 and urls[0] == t
