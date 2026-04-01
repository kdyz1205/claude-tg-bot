"""
skills/sk_poly_oracle.py — Polymarket「神谕」：Gamma 市场发现 + arXiv RAG + LLM 贝叶斯概率 vs 隐含价。

- 与 sk_academic_researcher 共用 arXiv 拉取（自定义 search_query；48h 在客户端按 Atom `updated` 过滤）；
- 文件后部保留 Gamma vs CLOB 订单簿价差扫描（scan_probability_edges），供处决网关等复用。

⚠️ 非投资建议。

<step1_oracle_architecture>
**Condition ID / Token ID ↔ 自然语言**
- Gamma `question` + `description` 为人类可读事件；`conditionId` 为链上条件哈希。
- `outcomes` / `clobTokenIds` / `outcomePrices` 为同序 JSON 数组；二元市场通常 Yes 在 index 0，
  `outcomePrices[0]` 即 Yes 的参考价（隐含概率）。

**LLM 真实概率提示词要点**
- 角色：仅依据所给摘要做贝叶斯校准；摘要与解析规则不对齐则压低 confidence。
- 输出严格 JSON：true_probability_yes、confidence、rationale_brief。
</step1_oracle_architecture>
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import httpx

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from skills.sk_academic_researcher import ArxivEntry, fetch_arxiv_hft_transformer  # noqa: E402

try:
    from dotenv import load_dotenv

    load_dotenv(_ROOT / ".env")
except ImportError:
    pass

GAMMA_MARKETS = "https://gamma-api.polymarket.com/markets"
MIN_LIQUIDITY_USD = 50_000.0
EDGE_THRESHOLD = 0.15
RECENT_HOURS = 48

# 词边界匹配，避免 "definitely"→"defi"、"available"→"ai" 等误报。
_TECH_TERMS: Tuple[str, ...] = (
    "tech",
    "software",
    "semiconductor",
    "chip",
    "gpu",
    "quantum",
    "robot",
    "algorithm",
    "encryption",
    "cryptograph",
    "llm",
    "openai",
    "tensorflow",
    "pytorch",
    "spacex",
    "space",
    "nasa",
    "rocket",
    "orbit",
    "satellite",
    "starship",
    "moon",
    "mars",
    "bitcoin",
    "ethereum",
    "crypto",
    "blockchain",
    "defi",
    "solana",
    "fda",
    "clinical",
    "vaccine",
    "drug",
    "cancer",
    "biotech",
    "mrna",
    "gene",
    "trial",
    "genome",
)
_TECH_PHRASES: Tuple[str, ...] = (
    "machine learning",
    "artificial intelligence",
    "large language",
    "neural network",
    "clinical trial",
    "phase 3",
    "phase iii",
)
_TECH_REGEX = re.compile(
    r"\b(" + "|".join(re.escape(t) for t in _TECH_TERMS) + r")\b", re.IGNORECASE
)

STOPWORDS = frozenset(
    """
    the this that will does did from with have been were what when which into your more than
    before after about into over under only also such both each few most other some very
    just into onto than then them they their there these those though through while where
    were being been would could should might must shall can may its his her our out off per
    for and not but are was you all any are one two how who why way get got
    market resolve according defined refers official
    """.split()
)


@dataclass
class PolyMarket:
    question: str
    description: str
    condition_id: str
    yes_token_id: str
    no_token_id: str
    implied_yes: float
    liquidity_num: float
    slug: str


@dataclass
class OracleResult:
    market: PolyMarket
    papers: List[ArxivEntry]
    true_probability_yes: Optional[float]
    confidence: float
    implied_yes: float
    edge: Optional[float]
    rationale: str
    strong_signal: bool
    skipped_reason: str


def _lower_blob(q: str, d: str) -> str:
    return f"{q} {d}".lower()


def is_tech_related(question: str, description: str) -> bool:
    blob = _lower_blob(question, description)
    if _TECH_REGEX.search(blob):
        return True
    return any(p in blob for p in _TECH_PHRASES)


def _parse_json_list(raw: Optional[str]) -> List[Any]:
    if not raw:
        return []
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return []


def _liquidity(m: Dict[str, Any]) -> float:
    v = m.get("liquidityNum")
    if v is not None:
        try:
            return float(v)
        except (TypeError, ValueError):
            pass
    s = m.get("liquidity") or "0"
    try:
        return float(s)
    except (TypeError, ValueError):
        return 0.0


def parse_gamma_market(m: Dict[str, Any]) -> Optional[PolyMarket]:
    q = (m.get("question") or "").strip()
    if not q:
        return None
    desc = (m.get("description") or "").strip()
    cid = (m.get("conditionId") or "").strip()
    slug = (m.get("slug") or "").strip()
    outcomes = _parse_json_list(m.get("outcomes"))
    prices = _parse_json_list(m.get("outcomePrices"))
    tokens = _parse_json_list(m.get("clobTokenIds"))
    if len(outcomes) < 2 or len(prices) < 2 or len(tokens) < 2:
        return None
    try:
        yes_px = float(prices[0])
    except (TypeError, ValueError):
        return None
    return PolyMarket(
        question=q,
        description=desc,
        condition_id=cid,
        yes_token_id=str(tokens[0]),
        no_token_id=str(tokens[1]),
        implied_yes=yes_px,
        liquidity_num=_liquidity(m),
        slug=slug,
    )


async def fetch_tech_markets_top(
    min_liquidity: float = MIN_LIQUIDITY_USD,
    top_n: int = 3,
    page_limit: int = 400,
) -> List[PolyMarket]:
    params = {
        "active": "true",
        "closed": "false",
        "liquidity_num_min": str(int(min_liquidity)),
        "limit": str(min(page_limit, 500)),
        "offset": "0",
    }
    async with httpx.AsyncClient(timeout=45.0, follow_redirects=True) as client:
        r = await client.get(GAMMA_MARKETS, params=params)
        r.raise_for_status()
        data = r.json()
    if not isinstance(data, list):
        return []
    candidates: List[PolyMarket] = []
    for raw in data:
        if not isinstance(raw, dict):
            continue
        pm = parse_gamma_market(raw)
        if pm is None:
            continue
        if pm.liquidity_num < min_liquidity:
            continue
        if not is_tech_related(pm.question, pm.description):
            continue
        candidates.append(pm)
    candidates.sort(key=lambda x: x.liquidity_num, reverse=True)
    return candidates[:top_n]


def _extract_arxiv_terms(question: str, description: str, max_terms: int = 6) -> List[str]:
    # 优先标题，避免长 description 里的模板词污染 arXiv 查询
    blob = question.strip()
    words = re.findall(r"[A-Za-z][A-Za-z0-9+\-]{2,}", blob)
    seen: List[str] = []
    for w in words:
        lw = w.lower()
        if lw in STOPWORDS or len(lw) < 4:
            continue
        if lw not in seen:
            seen.append(lw)
        if len(seen) >= max_terms:
            break
    if len(seen) < 2:
        for w in re.findall(r"[A-Za-z][A-Za-z0-9+\-]{2,}", description):
            lw = w.lower()
            if lw in STOPWORDS or len(lw) < 4 or lw in seen:
                continue
            seen.append(lw)
            if len(seen) >= max_terms:
                break
    if not seen:
        for kw in ("technology", "science", "machine", "learning"):
            if kw not in seen:
                seen.append(kw)
            if len(seen) >= 3:
                break
    return seen[:max_terms]


def _arxiv_search_query(terms: Sequence[str]) -> str:
    or_parts = [f'all:"{t}"' for t in terms if t]
    if not or_parts:
        or_parts = ['all:"science"']
    return "(" + " OR ".join(or_parts) + ")"


def _parse_arxiv_time(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        if len(s) >= 19:
            dt = datetime.fromisoformat(s[:19])
        else:
            dt = datetime.fromisoformat(s[:10])
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def filter_papers_last_hours(papers: List[ArxivEntry], hours: int = RECENT_HOURS) -> List[ArxivEntry]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    out: List[ArxivEntry] = []
    for p in papers:
        dt = _parse_arxiv_time(p.updated)
        if dt is None:
            continue
        if dt >= cutoff:
            out.append(p)
    return out


async def fetch_recent_papers_for_market(pm: PolyMarket, max_results: int = 8) -> List[ArxivEntry]:
    """关键词检索 arXiv；48h 用 `updated` 客户端过滤（避免 submittedDate 组合触发 API 500）。"""
    terms = _extract_arxiv_terms(pm.question, pm.description)
    q = _arxiv_search_query(terms)
    papers: List[ArxivEntry] = []
    for attempt in range(3):
        try:
            papers = await fetch_arxiv_hft_transformer(q, max_results=max_results)
            break
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429 and attempt < 2:
                wait = 4.0 * (attempt + 1)
                logger.warning("arXiv 429, retry in %.1fs (%s)", wait, pm.slug)
                await asyncio.sleep(wait)
                continue
            logger.warning("arXiv fetch failed for market %s: %s", pm.slug, e)
            return []
        except Exception as e:
            logger.warning("arXiv fetch failed for market %s: %s", pm.slug, e)
            return []
    return filter_papers_last_hours(papers, RECENT_HOURS)


BAYESIAN_SYSTEM = """You are a calibrated Bayesian forecaster for prediction-market style binary questions.
You ONLY use the evidence in the user message (paper titles and abstracts). Do not invent citations or results.
If the abstracts are not clearly relevant to the exact resolution criteria of the market, set confidence to 0 or near 0.
Output a JSON object ONLY, no markdown, with keys:
  "true_probability_yes": number between 0 and 1 (your posterior P(Yes)),
  "confidence": number between 0 and 1 (epistemic confidence given evidence quality and relevance),
  "rationale_brief": string under 400 characters (Chinese or English)."""


def _papers_block(papers: List[ArxivEntry]) -> str:
    lines = []
    for i, p in enumerate(papers, 1):
        lines.append(f"[{i}] {p.title}\nupdated: {p.updated}\n{p.summary[:1200]}")
    return "\n\n".join(lines)


async def llm_estimate_yes_probability(
    pm: PolyMarket,
    papers: List[ArxivEntry],
) -> Tuple[float, float, str]:
    import os

    user_msg = f"""Market question: {pm.question}

Resolution context (excerpt): {pm.description[:2500]}

Recent arXiv evidence (last {RECENT_HOURS}h window after client filter):
{_papers_block(papers)}
"""
    text: Optional[str] = None

    ak = os.getenv("ANTHROPIC_API_KEY")
    if ak:
        try:
            import anthropic

            client = anthropic.AsyncAnthropic(api_key=ak, timeout=120.0)
            msg = await client.messages.create(
                model=os.getenv("POLY_ORACLE_ANTHROPIC_MODEL", "claude-3-5-haiku-20241022"),
                max_tokens=512,
                system=BAYESIAN_SYSTEM,
                messages=[{"role": "user", "content": user_msg}],
            )
            for b in msg.content:
                if b.type == "text":
                    text = b.text
                    break
        except Exception as e:
            logger.warning("Anthropic oracle call failed: %s", e)

    if text is None:
        ok = os.getenv("OPENAI_API_KEY")
        if ok:
            try:
                from openai import AsyncOpenAI

                oai = AsyncOpenAI(api_key=ok, timeout=120.0)
                resp = await oai.chat.completions.create(
                    model=os.getenv("POLY_ORACLE_OPENAI_MODEL", "gpt-4o-mini"),
                    max_tokens=512,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": BAYESIAN_SYSTEM},
                        {"role": "user", "content": user_msg},
                    ],
                )
                text = (resp.choices[0].message.content or "").strip()
            except Exception as e:
                logger.warning("OpenAI oracle call failed: %s", e)

    if not text:
        raise RuntimeError(
            "No LLM response (set ANTHROPIC_API_KEY or OPENAI_API_KEY, and install anthropic/openai)."
        )

    m = re.search(r"\{[\s\S]*\}\s*$", text)
    raw_json = m.group(0) if m else text
    data = json.loads(raw_json)
    ty = float(data["true_probability_yes"])
    conf = float(data["confidence"])
    rationale = str(data.get("rationale_brief", "")).strip()
    ty = max(0.0, min(1.0, ty))
    conf = max(0.0, min(1.0, conf))
    return ty, conf, rationale


async def run_oracle_for_market(pm: PolyMarket) -> OracleResult:
    papers = await fetch_recent_papers_for_market(pm)
    if not papers:
        return OracleResult(
            market=pm,
            papers=[],
            true_probability_yes=None,
            confidence=0.0,
            implied_yes=pm.implied_yes,
            edge=None,
            rationale="",
            strong_signal=False,
            skipped_reason=f"最近 {RECENT_HOURS} 小时内未检索到相关 arXiv 论文 — 置信度 0，放弃预测。",
        )

    try:
        ty, conf, rationale = await llm_estimate_yes_probability(pm, papers)
    except Exception as e:
        logger.warning("LLM oracle failed: %s", e)
        return OracleResult(
            market=pm,
            papers=papers,
            true_probability_yes=None,
            confidence=0.0,
            implied_yes=pm.implied_yes,
            edge=None,
            rationale="",
            strong_signal=False,
            skipped_reason=f"LLM 推断失败: {e}",
        )

    if conf <= 0.0:
        return OracleResult(
            market=pm,
            papers=papers,
            true_probability_yes=ty,
            confidence=0.0,
            implied_yes=pm.implied_yes,
            edge=None,
            rationale=rationale,
            strong_signal=False,
            skipped_reason="模型置信度为 0，不输出可用概率优势。",
        )

    edge = ty - pm.implied_yes
    strong = edge > EDGE_THRESHOLD
    return OracleResult(
        market=pm,
        papers=papers,
        true_probability_yes=ty,
        confidence=conf,
        implied_yes=pm.implied_yes,
        edge=edge,
        rationale=rationale,
        strong_signal=strong,
        skipped_reason="",
    )


async def run_skill(params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    params = params or {}
    top_n = int(params.get("top_n") or 3)
    min_liq = float(params.get("min_liquidity") or MIN_LIQUIDITY_USD)
    markets = await fetch_tech_markets_top(min_liquidity=min_liq, top_n=top_n)
    results: List[Dict[str, Any]] = []
    for pm in markets:
        r = await run_oracle_for_market(pm)
        results.append(
            {
                "question": r.market.question,
                "slug": r.market.slug,
                "condition_id": r.market.condition_id,
                "yes_token_id": r.market.yes_token_id,
                "liquidity_usd": r.market.liquidity_num,
                "implied_yes": r.implied_yes,
                "true_probability_yes": r.true_probability_yes,
                "confidence": r.confidence,
                "edge": r.edge,
                "strong_signal": r.strong_signal,
                "rationale": r.rationale,
                "skipped_reason": r.skipped_reason,
                "paper_count": len(r.papers),
            }
        )
    return {"ok": True, "skill": "sk_poly_oracle", "markets_scanned": len(markets), "results": results}


def _print_report(results: List[OracleResult]) -> None:
    print("=" * 72)
    print("Polymarket 神谕 — Top 科技/航天/加密/医研相关市场（流动性过滤后）")
    print("=" * 72)
    if not results:
        print("未找到同时满足流动性与技术主题过滤的市场；可调低 min_liquidity 或扩充 _TECH_TERMS。")
        return
    for i, r in enumerate(results, 1):
        m = r.market
        print(f"\n--- [{i}] {m.question}")
        print(f"slug: {m.slug}")
        print(f"conditionId: {m.condition_id}")
        print(f"yesTokenId: {m.yes_token_id}")
        print(f"liquidityNum: {m.liquidity_num:,.2f} USD")
        print(f"市场隐含 P(Yes): {r.implied_yes:.4f}")
        if r.skipped_reason:
            print(f"状态: {r.skipped_reason}")
            continue
        print(f"论文证据数 (48h 内): {len(r.papers)}")
        print(f"模型 P_true(Yes): {r.true_probability_yes:.4f}  |  置信度: {r.confidence:.4f}")
        print(f"边缘 Edge (True - Implied): {r.edge:+.4f}")
        if r.strong_signal:
            print(f"*** 强信号: Edge > {EDGE_THRESHOLD} — 信息差优势显著（仍非交易建议）***")
        print(f"简述: {r.rationale}")
    print("\n" + "=" * 72)


async def _cli_main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    markets = await fetch_tech_markets_top(top_n=3)
    out: List[OracleResult] = []
    for i, pm in enumerate(markets):
        if i:
            await asyncio.sleep(3.0)  # arXiv 公开 API 易 429，市场之间稍作间隔
        out.append(await run_oracle_for_market(pm))
    _print_report(out)


# ---------------------------------------------------------------------------
# Gamma 参考价 vs CLOB 订单簿（价差扫描，供其他模块调用）
# ---------------------------------------------------------------------------

CLOB_BOOK_URL = "https://clob.polymarket.com/book"
GAMMA_MARKETS_URL = GAMMA_MARKETS


def _best_bid_ask(book: Dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    bid_p = max((float(x["price"]) for x in bids if x.get("price") is not None), default=None)
    ask_p = min((float(x["price"]) for x in asks if x.get("price") is not None), default=None)
    return bid_p, ask_p


def _mid_from_book(book: Dict[str, Any]) -> Optional[float]:
    bid_p, ask_p = _best_bid_ask(book)
    if bid_p is not None and ask_p is not None and ask_p > bid_p:
        return (bid_p + ask_p) / 2.0
    if ask_p is not None:
        return ask_p
    if bid_p is not None:
        return bid_p
    lp = book.get("last_trade_price")
    try:
        return float(lp) if lp is not None else None
    except (TypeError, ValueError):
        return None


async def _fetch_book(client: httpx.AsyncClient, token_id: str) -> Optional[Dict[str, Any]]:
    try:
        r = await client.get(CLOB_BOOK_URL, params={"token_id": str(token_id)}, timeout=20.0)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception as e:
        logger.debug("CLOB book fetch failed token=%s err=%s", token_id[:16], e)
        return None


async def scan_probability_edges(
    min_edge_pct: float = 15.0,
    max_markets: int = 80,
    min_liquidity_usd: float = 5_000.0,
    require_unrestricted: bool = True,
    client: Optional[httpx.AsyncClient] = None,
) -> List[Dict[str, Any]]:
    own_client = client is None
    if own_client:
        client = httpx.AsyncClient()

    out: List[Dict[str, Any]] = []
    try:
        r = await client.get(
            GAMMA_MARKETS_URL,
            params={
                "limit": max(1, int(max_markets)),
                "active": "true",
                "closed": "false",
            },
            timeout=30.0,
        )
        if r.status_code != 200:
            logger.warning("Gamma markets HTTP %s", r.status_code)
            return []
        markets = r.json()
        if not isinstance(markets, list):
            return []

        for m in markets:
            if not isinstance(m, dict):
                continue
            if require_unrestricted and m.get("restricted"):
                continue
            if not m.get("acceptingOrders") and m.get("acceptingOrders") is not None:
                continue
            liq = float(m.get("liquidityNum") or m.get("liquidity") or 0)
            if liq < min_liquidity_usd:
                continue

            try:
                outcomes = json.loads(m.get("outcomes") or "[]")
                prices = json.loads(m.get("outcomePrices") or "[]")
                token_ids = json.loads(m.get("clobTokenIds") or "[]")
            except (json.JSONDecodeError, TypeError):
                continue

            if not outcomes or len(outcomes) != len(prices) or len(token_ids) != len(outcomes):
                continue

            tick = float(m.get("orderPriceMinTickSize") or 0.001)
            min_sz = float(m.get("orderMinSize") or 5)

            for i, name in enumerate(outcomes):
                tid = str(token_ids[i])
                try:
                    gamma_p = float(prices[i])
                except (TypeError, ValueError):
                    continue
                gamma_p = max(0.001, min(0.999, gamma_p))

                book = await _fetch_book(client, tid)
                if not book:
                    continue
                mid = _mid_from_book(book)
                if mid is None:
                    continue
                mid = max(0.001, min(0.999, mid))

                edge = abs(gamma_p - mid) * 100.0
                if edge < min_edge_pct:
                    continue

                _, ask_p = _best_bid_ask(book)
                entry_ask = float(ask_p) if ask_p is not None else mid
                entry_ask = max(tick, min(0.999, entry_ask))

                if gamma_p <= mid:
                    continue

                out.append(
                    {
                        "market_id": str(m.get("id", "")),
                        "condition_id": m.get("conditionId") or "",
                        "question": (m.get("question") or "")[:500],
                        "outcome_name": str(name),
                        "outcome_index": i,
                        "token_id": tid,
                        "oracle_prob": gamma_p,
                        "market_mid": mid,
                        "entry_ask": entry_ask,
                        "edge_pct": round(edge, 2),
                        "tick_size": tick,
                        "order_min_size": min_sz,
                        "liquidity_usd": liq,
                    }
                )

        out.sort(key=lambda x: float(x.get("edge_pct") or 0), reverse=True)
        return out
    finally:
        if own_client and client is not None:
            await client.aclose()


async def run_oracle_scan(
    min_edge_pct: float = 15.0,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    return await scan_probability_edges(min_edge_pct=min_edge_pct, **kwargs)


if __name__ == "__main__":
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except (AttributeError, OSError):
            pass
    asyncio.run(_cli_main())
