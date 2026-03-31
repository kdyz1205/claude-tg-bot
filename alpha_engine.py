"""
alpha_engine.py — Social Alpha Signal Mining Engine

Discovers early alpha from:
  - CoinGecko trending coins
  - DEXScreener top-boosted tokens
  - Pump.fun hot list (recent activity)

Scoring model (weights adjustable via _alpha_config.json):
  liquidity(30%) + holder_dispersion(30%) + price_momentum(20%) + community_heat(20%)

Every 30 min: pushes Top5 candidates to Telegram if score >= threshold (default 70).
Tracks pushed tokens for 7 days, calculates win rate, auto-adjusts weights.
"""

import asyncio
import json
import logging
import math
import os
import re
import time
from datetime import datetime
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "_alpha_config.json")
TRACKING_FILE = os.path.join(BASE_DIR, "_alpha_tracking.json")

# Tokens containing these words are likely scams/hype — filter out
RISK_KEYWORDS = frozenset({
    "safe", "rug", "moon", "100x", "1000x", "elon",
    "rugpull", "honeypot", "scam", "guaranteed",
})

DEFAULT_CONFIG = {
    "weights": {
        "liquidity": 0.35,           # higher weight = prefer real liquidity
        "holder_dispersion": 0.25,
        "price_momentum": 0.15,      # lower = avoid chasing pumps
        "community_heat": 0.25,
    },
    "score_threshold": 75,           # stricter threshold
    "scan_interval": 1800,           # 30 minutes
    "top_n": 5,
    "min_liquidity_usd": 50000,      # $50k minimum — no micro-cap rugs
    "min_market_cap_usd": 500000,    # $500k minimum market cap
    "min_age_hours": 24,             # must be at least 24h old (no launch sniping)
    "max_price_change_24h": 500,     # >500% in 24h = likely pump&dump
    "tracking_days": 7,
}


# ── Config / tracking persistence ────────────────────────────────────────────

def _load_config() -> dict:
    cfg = {**DEFAULT_CONFIG, "weights": dict(DEFAULT_CONFIG["weights"])}
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                overrides = json.load(f)
            cfg.update({k: v for k, v in overrides.items() if k != "weights"})
            if "weights" in overrides:
                cfg["weights"].update(overrides["weights"])
        except Exception as e:
            logger.warning("alpha_engine: config load error: %s", e)
    return cfg


def _save_config(cfg: dict) -> None:
    tmp = str(CONFIG_FILE) + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, str(CONFIG_FILE))
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


_MAX_TRACKING_RECORDS = 500  # max tracking records to keep in memory


def _load_tracking() -> list:
    if not os.path.exists(TRACKING_FILE):
        return []
    try:
        with open(TRACKING_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return []
        # Cap loaded records to prevent unbounded memory use
        if len(data) > _MAX_TRACKING_RECORDS:
            data = data[-_MAX_TRACKING_RECORDS:]
        return data
    except Exception:
        return []


def _save_tracking(records: list) -> None:
    tmp = str(TRACKING_FILE) + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(records[-300:], f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, str(TRACKING_FILE))
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ── Risk keyword filter ───────────────────────────────────────────────────────

def _is_risky(name: str, symbol: str) -> bool:
    text = f"{name} {symbol}".lower()
    tokens = set(re.findall(r"[a-z]+", text))
    return bool(tokens & RISK_KEYWORDS)


# ── Scoring component functions (each returns 0-100) ─────────────────────────

def _score_liquidity(usd: float) -> float:
    """Log scale: 0 at $0, ~50 at $50k, 100 at $10M+. Penalize micro-liquidity."""
    if usd <= 0:
        return 0.0
    # Stricter: need $50k+ for decent score
    return min(100.0, math.log10(max(1.0, usd)) / 7.0 * 100.0)


def _score_holder_dispersion(count: int) -> float:
    """More unique holders = better. Caps at 2000 for 100. Penalize <100 holders."""
    if count < 50:
        return 10.0  # very suspicious if < 50 holders
    return min(100.0, count / 20.0)


def _score_price_momentum(change_24h: float) -> float:
    """Bell curve: best at +10-50%, penalize extremes. >500% = likely pump&dump."""
    if change_24h < -30:
        return 10.0  # crashing
    if change_24h > 500:
        return 20.0  # pump & dump territory
    if change_24h > 200:
        return 40.0  # suspicious but possible
    # Sweet spot: +5% to +50%
    if 5 <= change_24h <= 50:
        return min(100.0, 60 + change_24h)
    # Moderate: 0-5% or 50-200%
    return min(100.0, max(0.0, (change_24h + 20.0) * 2.0))


def _score_community_heat(heat: float) -> float:
    """Direct 0-100 heat value from source-specific signal."""
    return min(100.0, max(0.0, heat))


def _composite(liq: float, hold: float, mom: float, heat: float, weights: dict) -> float:
    return (
        liq  * weights["liquidity"]
        + hold * weights["holder_dispersion"]
        + mom  * weights["price_momentum"]
        + heat * weights["community_heat"]
    )


# ── Data fetchers ─────────────────────────────────────────────────────────────

async def _fetch_coingecko_trending(client: httpx.AsyncClient) -> list[dict]:
    """CoinGecko /search/trending — free, no API key needed."""
    try:
        resp = await client.get(
            "https://api.coingecko.com/api/v3/search/trending",
            timeout=12.0,
        )
        if resp.status_code != 200:
            logger.debug("alpha_engine: CoinGecko HTTP %s", resp.status_code)
            return []
        coins = resp.json().get("coins", [])
        results = []
        for rank, item in enumerate(coins[:10]):
            coin = item.get("item", {})
            name = coin.get("name", "")
            symbol = coin.get("symbol", "")
            if _is_risky(name, symbol):
                continue
            data = coin.get("data", {})
            # price_change_percentage_24h can be a dict {"usd": x} or a float
            pct = data.get("price_change_percentage_24h", 0)
            change_24h = pct.get("usd", 0) if isinstance(pct, dict) else float(pct or 0)
            # market_cap_btc as rough liquidity proxy: 1 BTC ≈ $60k
            mc_btc = float(coin.get("market_cap_btc", 0) or 0)
            liquidity_proxy = mc_btc * 60_000 * 0.05  # 5% of mcap as liquidity proxy
            results.append({
                "name": name,
                "symbol": symbol,
                "source": "coingecko",
                "chain": "multi",
                "address": coin.get("id", ""),
                "pair_url": "",
                "liquidity_usd": liquidity_proxy,
                "holder_count": 200 + (9 - rank) * 30,   # rank proxy
                "price_change_24h": change_24h,
                "community_heat_raw": (10 - rank) * 10.0,  # rank 1 → 90, rank 10 → 0
            })
        return results
    except Exception as e:
        logger.debug("alpha_engine: CoinGecko error: %s", e)
        return []


async def _fetch_dexscreener_trending(client: httpx.AsyncClient) -> list[dict]:
    """DEXScreener top-boosted tokens. Enriches each with pair data."""
    try:
        resp = await client.get(
            "https://api.dexscreener.com/token-boosts/top/v1",
            timeout=12.0,
        )
        if resp.status_code != 200:
            logger.debug("alpha_engine: DEXScreener HTTP %s", resp.status_code)
            return []
        data = resp.json()
        items = data if isinstance(data, list) else []
    except Exception as e:
        logger.debug("alpha_engine: DEXScreener boost list error: %s", e)
        return []

    results = []
    seen_addr: set[str] = set()

    for _idx, item in enumerate(items[:15]):
        try:
            address = item.get("tokenAddress", "")
            chain = item.get("chainId", "")
            if not address or address in seen_addr:
                continue
            seen_addr.add(address)

            boost_amount = float(item.get("amount", 0) or 0)
            heat = min(100.0, math.log10(max(1.0, boost_amount)) / 4.0 * 100.0)

            # Rate limit between API calls
            if _idx > 0:
                await asyncio.sleep(0.2)
            # Fetch pair data for liquidity / price change
            try:
                pr = await client.get(
                    f"https://api.dexscreener.com/latest/dex/tokens/{address}",
                    timeout=8.0,
                )
                pairs = pr.json().get("pairs") or []
            except Exception:
                pairs = []

            if not pairs:
                continue

            best = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd", 0) or 0))
            liq = float((best.get("liquidity") or {}).get("usd", 0) or 0)
            txns_h24 = (best.get("txns") or {}).get("h24", {})
            buyer_count = int(txns_h24.get("buys", 0) or 0)
            change_24h = float((best.get("priceChange") or {}).get("h24", 0) or 0)
            token = best.get("baseToken", {})
            name = token.get("name", item.get("description", address[:8]))
            symbol = token.get("symbol", address[:6])

            if _is_risky(name, symbol):
                continue

            price_usd = float(best.get("priceUsd", 0) or 0)
            mcap = float((best.get("marketCap") or best.get("fdv") or 0) or 0)
            results.append({
                "name": name,
                "symbol": symbol,
                "source": "dexscreener",
                "chain": chain,
                "address": address,
                "pair_url": best.get("url", ""),
                "liquidity_usd": liq,
                "market_cap_usd": mcap,
                "price_usd": price_usd,
                "holder_count": buyer_count,
                "price_change_24h": change_24h,
                "community_heat_raw": heat,
            })
        except (ValueError, TypeError, KeyError):
            continue

    return results


async def _fetch_pumpfun_trending(client: httpx.AsyncClient) -> list[dict]:
    """Pump.fun hot list — tokens with most recent social activity."""
    try:
        resp = await client.get(
            "https://frontend-api.pump.fun/coins",
            params={
                "limit": 20,
                "sort": "last_reply",
                "order": "DESC",
                "includeNsfw": "false",
            },
            timeout=12.0,
            headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"},
        )
        if resp.status_code != 200:
            logger.debug("alpha_engine: Pump.fun HTTP %s", resp.status_code)
            return []
        data = resp.json()
        items = data if isinstance(data, list) else []
    except Exception as e:
        logger.debug("alpha_engine: Pump.fun error: %s", e)
        return []

    results = []
    for item in items[:20]:
        try:
            name = item.get("name", "")
            symbol = item.get("symbol", "")
            if _is_risky(name, symbol):
                continue
            market_cap = float(item.get("market_cap", 0) or 0)
            # Bonding curve: liquidity ≈ 10% of market cap (rough proxy)
            liq = market_cap * 0.10
            holder_count = int(item.get("holder_count", 0) or 0)
            reply_count = int(item.get("reply_count", 0) or 0)
            results.append({
                "name": name,
                "symbol": symbol,
                "source": "pumpfun",
                "chain": "solana",
                "address": item.get("mint", ""),
                "pair_url": "",
                "liquidity_usd": liq,
                "holder_count": holder_count or reply_count * 3,
                "price_change_24h": 0.0,     # Pump.fun doesn't expose 24h %
                "community_heat_raw": min(100.0, reply_count / 2.0),
            })
        except (ValueError, TypeError, KeyError):
            continue
    return results


# ── Onchain Filter (DexScreener advanced scan) ──────────────────────────────

ONCHAIN_FILTER_SET_1 = {
    "min_liquidity": 12597,
    "min_mcap": 12597,
    "max_mcap": 8000000,      # 800万 max
    "max_age_hours": 2400,
    "min_vol_3m": 8888,       # 3-minute volume > $8,888
    "min_vol_5m": 16666,      # 5-minute volume > $16,666
}

# Track already-alerted tokens to avoid duplicate pushes
_ONCHAIN_ALERTED_FILE = os.path.join(BASE_DIR, "_onchain_alerted.json")

def _load_onchain_alerted() -> dict:
    try:
        with open(_ONCHAIN_ALERTED_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_onchain_alerted(data: dict) -> None:
    try:
        tmp_path = _ONCHAIN_ALERTED_FILE + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, _ONCHAIN_ALERTED_FILE)
    except Exception:
        pass

def onchain_filter_new_only(tokens: list[dict]) -> list[dict]:
    """Return only tokens not yet alerted. Mark them as alerted."""
    alerted = _load_onchain_alerted()
    now = time.time()
    # Clean entries older than 24h
    alerted = {k: v for k, v in alerted.items() if now - v < 86400}
    # Hard cap: if still >5000 entries after time cleanup, keep only newest 2000
    if len(alerted) > 5000:
        sorted_items = sorted(alerted.items(), key=lambda x: x[1], reverse=True)
        alerted = dict(sorted_items[:2000])
    new_tokens = []
    for t in tokens:
        key = t.get("address", "")
        if not key or key in alerted:
            continue
        new_tokens.append(t)
        alerted[key] = now
    if new_tokens:
        _save_onchain_alerted(alerted)
    return new_tokens


async def scan_onchain_filter(filter_set: dict = None, client: httpx.AsyncClient = None) -> list[dict]:
    """
    Onchain filter: scan DexScreener for tokens matching strict criteria.
    Uses token-profiles for discovery + pair data for filtering.
    """
    if filter_set is None:
        filter_set = ONCHAIN_FILTER_SET_1
    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(follow_redirects=True, timeout=httpx.Timeout(20.0))

    results = []
    try:
        # 1. Fetch latest token profiles (recently boosted / active)
        resp = await client.get(
            "https://api.dexscreener.com/token-boosts/top/v1",
            timeout=12.0,
        )
        _boosts_data = resp.json()
        boosts = _boosts_data if isinstance(_boosts_data, list) else []

        # Also search for new pairs across chains
        try:
            resp2 = await client.get(
                "https://api.dexscreener.com/token-profiles/latest/v1",
                timeout=12.0,
            )
            _profiles_data = resp2.json()
            profiles = _profiles_data if isinstance(_profiles_data, list) else []
        except Exception:
            profiles = []

        candidates = []
        seen_addr: set[str] = set()
        for item in boosts + profiles:
            addr = item.get("tokenAddress", "")
            chain = item.get("chainId", "")
            if not addr or addr in seen_addr:
                continue
            seen_addr.add(addr)
            candidates.append({"address": addr, "chain": chain})

        # 2. Fetch pair data and apply filters
        for _ci, cand in enumerate(candidates[:30]):  # limit API calls
            if _ci > 0:
                await asyncio.sleep(0.2)  # rate-limit: max 5 req/s for DexScreener
            try:
                pr = await client.get(
                    f"https://api.dexscreener.com/latest/dex/tokens/{cand['address']}",
                    timeout=8.0,
                )
                pairs = pr.json().get("pairs") or []
            except Exception:
                continue
            if not pairs:
                continue

            try:
                best = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd", 0) or 0))
                liq = float((best.get("liquidity") or {}).get("usd", 0) or 0)
                mcap = float((best.get("marketCap") or best.get("fdv") or 0) or 0)

                # Filter: liquidity
                if liq < filter_set.get("min_liquidity", 0):
                    continue
                # Filter: market cap range
                if mcap < filter_set.get("min_mcap", 0):
                    continue
                if filter_set.get("max_mcap") and mcap > filter_set["max_mcap"]:
                    continue

                # Filter: pair age
                age_hours = None
                pair_created = best.get("pairCreatedAt", 0)
                if pair_created:
                    age_hours = (time.time() * 1000 - pair_created) / 3600000
                    max_age = filter_set.get("max_age_hours", 99999)
                    if age_hours > max_age:
                        continue

                # Filter: short-term volume (5m, 1h from DexScreener)
                vol_obj = best.get("volume") or {}
                vol_5m = float(vol_obj.get("m5", 0) or 0)
                vol_1h = float(vol_obj.get("h1", 0) or 0)
                # Approximate 3m volume = 60% of 5m volume
                vol_3m_approx = vol_5m * 0.6

                if vol_3m_approx < filter_set.get("min_vol_3m", 0):
                    continue
                if vol_5m < filter_set.get("min_vol_5m", 0):
                    continue

                # Get transaction counts
                txns = best.get("txns") or {}
                txns_5m = txns.get("m5", {})
                txns_1h = txns.get("h1", {})
                buys_5m = int(txns_5m.get("buys", 0) or 0)
                sells_5m = int(txns_5m.get("sells", 0) or 0)
                buys_1h = int(txns_1h.get("buys", 0) or 0)
                sells_1h = int(txns_1h.get("sells", 0) or 0)

                token = best.get("baseToken", {})
                name = token.get("name", cand["address"][:8])
                symbol = token.get("symbol", cand["address"][:6])

                price_usd = float(best.get("priceUsd", 0) or 0)
                change_5m = float((best.get("priceChange") or {}).get("m5", 0) or 0)
                change_1h = float((best.get("priceChange") or {}).get("h1", 0) or 0)
                change_24h = float((best.get("priceChange") or {}).get("h24", 0) or 0)

                results.append({
                    "name": name,
                    "symbol": symbol,
                    "chain": cand["chain"],
                    "address": cand["address"],
                    "pair_url": best.get("url", ""),
                    "price_usd": price_usd,
                    "liquidity_usd": liq,
                    "market_cap_usd": mcap,
                    "vol_5m": vol_5m,
                    "vol_3m_approx": vol_3m_approx,
                    "vol_1h": vol_1h,
                    "buys_5m": buys_5m,
                    "sells_5m": sells_5m,
                    "buys_1h": buys_1h,
                    "sells_1h": sells_1h,
                    "change_5m": change_5m,
                    "change_1h": change_1h,
                    "change_24h": change_24h,
                    "age_hours": age_hours if pair_created else None,
                })
            except (ValueError, TypeError, KeyError):
                continue
    except Exception as e:
        logger.error("onchain_filter scan error: %s", e)
    finally:
        if own_client:
            await client.aclose()

    # Sort by 5-minute volume (hottest first)
    results.sort(key=lambda x: x.get("vol_5m", 0), reverse=True)
    return results


def format_onchain_filter_report(tokens: list[dict]) -> str:
    """Format onchain filter results for Telegram."""
    if not tokens:
        return "🔍 Onchain Filter: 当前无符合条件的代币"

    lines = ["🔗 Onchain Filter 扫描结果\n"]
    for i, t in enumerate(tokens[:10], 1):
        price = t.get("price_usd", 0)
        price_str = f"${price:.6f}" if price < 0.01 else f"${price:.4f}" if price < 1 else f"${price:.2f}"
        mcap = t.get("market_cap_usd", 0)
        mcap_str = f"${mcap/1e6:.1f}M" if mcap >= 1e6 else f"${mcap/1e3:.0f}K"
        liq_str = f"${t.get('liquidity_usd', 0)/1e3:.0f}K"
        vol_5m = t.get("vol_5m", 0)
        vol_3m = t.get("vol_3m_approx", 0)

        buy_sell_5m = f"{t.get('buys_5m',0)}B/{t.get('sells_5m',0)}S"
        age = t.get("age_hours")
        age_str = f"{age:.0f}h" if age and age < 100 else f"{age/24:.0f}d" if age else "?"

        lines.append(
            f"{i}. {t.get('name', '?')} (${t.get('symbol', '?')}) [{t.get('chain', '?')}]\n"
            f"   {price_str} | MCap: {mcap_str} | Liq: {liq_str}\n"
            f"   5m量: ${vol_5m:,.0f} | ~3m量: ${vol_3m:,.0f}\n"
            f"   5m交易: {buy_sell_5m} | 5m涨跌: {t.get('change_5m',0):+.1f}%\n"
            f"   1h涨跌: {t.get('change_1h',0):+.1f}% | 24h: {t.get('change_24h',0):+.1f}%\n"
            f"   Age: {age_str}"
        )
        if t.get("pair_url"):
            lines.append(f"   {t['pair_url']}")
        lines.append("")

    lines.append(f"筛选条件: Liq>=${ONCHAIN_FILTER_SET_1['min_liquidity']:,} | "
                 f"MCap {ONCHAIN_FILTER_SET_1['min_mcap']:,}-{ONCHAIN_FILTER_SET_1['max_mcap']:,} | "
                 f"3m量>{ONCHAIN_FILTER_SET_1['min_vol_3m']:,} | 5m量>{ONCHAIN_FILTER_SET_1['min_vol_5m']:,}")
    result = "\n".join(lines)
    if len(result) > 4000:
        result = result[:3950] + "\n\n... (已截断)"
    return result


# ── Main scan ─────────────────────────────────────────────────────────────────

async def scan_alpha(cfg: dict = None) -> list[dict]:
    """
    Fetch all sources in parallel, score each token, return Top-N with score >= threshold.
    """
    if cfg is None:
        cfg = _load_config()

    weights = cfg["weights"]
    min_liq = cfg.get("min_liquidity_usd", 1000)
    threshold = cfg.get("score_threshold", 70)
    top_n = cfg.get("top_n", 5)

    async with httpx.AsyncClient(follow_redirects=True) as client:
        cg, dex, pump = await asyncio.gather(
            _fetch_coingecko_trending(client),
            _fetch_dexscreener_trending(client),
            _fetch_pumpfun_trending(client),
            return_exceptions=True,
        )

    all_tokens: list[dict] = []
    for batch in (cg, dex, pump):
        if isinstance(batch, list):
            all_tokens.extend(batch)

    # Deduplicate by (source, chain, symbol) — avoid cross-chain false dedup
    seen: set[str] = set()
    unique: list[dict] = []
    for t in all_tokens:
        key = f"{t.get('source', '?')}:{t.get('chain', '?')}:{t.get('symbol', '?').upper()}"
        if key not in seen:
            seen.add(key)
            unique.append(t)

    # Score and filter with strict quality gates
    min_mcap = cfg.get("min_market_cap_usd", 500000)
    max_pump = cfg.get("max_price_change_24h", 500)
    scored: list[dict] = []
    for t in unique:
        if t.get("liquidity_usd", 0) < min_liq:
            continue
        # Market cap filter (if available)
        mcap = t.get("market_cap_usd", t.get("mcap", 0))
        if mcap and mcap < min_mcap:
            continue
        # Pump & dump filter
        if t.get("price_change_24h", 0) > max_pump:
            continue
        # Negative momentum filter (crashing coins)
        if t.get("price_change_24h", 0) < -50:
            continue

        s_liq  = _score_liquidity(t.get("liquidity_usd", 0))
        s_hold = _score_holder_dispersion(t.get("holder_count", 0))
        s_mom  = _score_price_momentum(t.get("price_change_24h", 0))
        s_heat = _score_community_heat(t.get("community_heat_raw", 0))
        total  = _composite(s_liq, s_hold, s_mom, s_heat, weights)

        if total >= threshold:
            scored.append({
                **t,
                "score": round(total, 1),
                "score_breakdown": {
                    "liquidity": round(s_liq, 1),
                    "holder_dispersion": round(s_hold, 1),
                    "price_momentum": round(s_mom, 1),
                    "community_heat": round(s_heat, 1),
                },
                "scanned_at": time.time(),
            })

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored[:top_n]


# ── Performance tracking ──────────────────────────────────────────────────────

def record_push(tokens: list[dict]) -> None:
    """Persist pushed tokens with entry price for real P&L tracking."""
    records = _load_tracking()
    now = time.time()
    # Dedup: skip tokens already pushed in last 6 hours
    recent_addrs = {r.get("address") for r in records
                    if r.get("address") and now - r.get("pushed_at", 0) < 21600}
    for t in tokens:
        if t.get("address") and t["address"] in recent_addrs:
            continue
        records.append({
            "name": t.get("name", "unknown"),
            "symbol": t.get("symbol", "???"),
            "source": t.get("source", "unknown"),
            "address": t.get("address", ""),
            "chain": t.get("chain", ""),
            "score": t.get("score", 0),
            "score_breakdown": t.get("score_breakdown", {}),
            "pushed_at": now,
            "entry_liq": t.get("liquidity_usd", 0),
            "entry_price": t.get("price_usd", 0),
            "entry_mcap": t.get("market_cap_usd", t.get("mcap", 0)),
            # Short-term tracking checkpoints
            "check_1h": None,   # price at +1h
            "check_4h": None,   # price at +4h
            "check_24h": None,  # price at +24h
            "pnl_1h": None,
            "pnl_4h": None,
            "pnl_24h": None,
            "resolved": False,
            "outcome": None,
        })
    _save_tracking(records)


async def check_short_term_performance() -> dict:
    """Check 1h/4h/24h price changes for tracked tokens. Returns summary stats."""
    records = _load_tracking()
    now = time.time()
    updated = 0
    results = {"checked": 0, "wins_1h": 0, "losses_1h": 0, "wins_24h": 0, "losses_24h": 0}

    async with httpx.AsyncClient(timeout=30) as client:
        for r in records:
            if r.get("resolved"):
                continue
            address = r.get("address", "")
            entry_price = r.get("entry_price", 0)
            if not address or not entry_price or entry_price <= 0:
                continue
            age_h = (now - r.get("pushed_at", now)) / 3600

            # Check at appropriate intervals
            needs_check = False
            if age_h >= 1 and r.get("check_1h") is None:
                needs_check = True
            elif age_h >= 4 and r.get("check_4h") is None:
                needs_check = True
            elif age_h >= 24 and r.get("check_24h") is None:
                needs_check = True

            if not needs_check:
                continue

            # Fetch current price
            try:
                resp = await client.get(
                    f"https://api.dexscreener.com/latest/dex/tokens/{address}",
                    timeout=8.0,
                )
                pairs = resp.json().get("pairs") or []
                if not pairs:
                    continue
                best = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd", 0) or 0))
                current_price = float(best.get("priceUsd", 0) or 0)
                if current_price <= 0:
                    continue

                pnl_pct = ((current_price - entry_price) / entry_price) * 100

                if age_h >= 1 and r.get("check_1h") is None:
                    r["check_1h"] = current_price
                    r["pnl_1h"] = round(pnl_pct, 2)
                    results["checked"] += 1
                    if pnl_pct > 0:
                        results["wins_1h"] += 1
                    else:
                        results["losses_1h"] += 1
                if age_h >= 4 and r.get("check_4h") is None:
                    r["check_4h"] = current_price
                    r["pnl_4h"] = round(pnl_pct, 2)
                if age_h >= 24 and r.get("check_24h") is None:
                    r["check_24h"] = current_price
                    r["pnl_24h"] = round(pnl_pct, 2)
                    if pnl_pct > 0:
                        results["wins_24h"] += 1
                    else:
                        results["losses_24h"] += 1

                updated += 1
            except Exception:
                continue

    if updated > 0:
        _save_tracking(records)
    return results


def get_performance_summary() -> str:
    """Real P&L summary of all tracked signals."""
    records = _load_tracking()
    if not records:
        return "📊 暂无追踪数据"

    total = len(records)
    has_1h = [r for r in records if r.get("pnl_1h") is not None]
    has_24h = [r for r in records if r.get("pnl_24h") is not None]

    lines = [f"📊 Alpha信号真实绩效 (共{total}个信号)\n"]

    if has_1h:
        avg_1h = sum(r["pnl_1h"] for r in has_1h) / len(has_1h)
        win_1h = sum(1 for r in has_1h if r["pnl_1h"] > 0)
        lines.append(f"1h: 胜率{win_1h}/{len(has_1h)} ({win_1h/len(has_1h)*100:.0f}%) 平均{avg_1h:+.1f}%")

    if has_24h:
        avg_24h = sum(r["pnl_24h"] for r in has_24h) / len(has_24h)
        win_24h = sum(1 for r in has_24h if r["pnl_24h"] > 0)
        lines.append(f"24h: 胜率{win_24h}/{len(has_24h)} ({win_24h/len(has_24h)*100:.0f}%) 平均{avg_24h:+.1f}%")

    # Top 3 best and worst
    if has_24h:
        sorted_by_pnl = sorted(has_24h, key=lambda r: r.get("pnl_24h", 0), reverse=True)
        lines.append("\n🏆 最佳:")
        for r in sorted_by_pnl[:3]:
            lines.append(f"  {r['symbol']}: {r['pnl_24h']:+.1f}%")
        lines.append("💀 最差:")
        for r in sorted_by_pnl[-3:]:
            lines.append(f"  {r['symbol']}: {r['pnl_24h']:+.1f}%")

    result = "\n".join(lines)
    if len(result) > 4000:
        result = result[:3950] + "\n\n... (已截断)"
    return result


async def resolve_old_records(cfg: dict = None) -> dict:
    """
    Check tracking records older than 7 days.
    Fetches current DEXScreener data to determine win/loss (liquidity grew >20%).
    Updates weights if sufficient data is available.
    Returns stats dict.
    """
    if cfg is None:
        cfg = _load_config()

    records = _load_tracking()
    now = time.time()
    cutoff = cfg.get("tracking_days", 7) * 24 * 3600

    pending = [r for r in records if not r.get("resolved") and (now - r.get("pushed_at", now)) >= cutoff]
    if not pending:
        resolved = [r for r in records if r.get("resolved") and r.get("outcome") in ("win", "loss")]
        total = len(resolved)
        wins = sum(1 for r in resolved if r["outcome"] == "win")
        return {"total": total, "wins": wins, "win_rate": wins / total if total else 0.0, "newly_resolved": 0}

    wins_by_factor: dict[str, list[int]] = {
        "liquidity": [], "holder_dispersion": [], "price_momentum": [], "community_heat": []
    }
    newly_resolved = 0

    async with httpx.AsyncClient(timeout=30) as client:
        for r in pending:
            address = r.get("address", "")
            outcome = "unknown"
            if address and r["source"] in ("dexscreener", "pumpfun"):
                try:
                    resp = await client.get(
                        f"https://api.dexscreener.com/latest/dex/tokens/{address}",
                        timeout=8.0,
                    )
                    pairs = resp.json().get("pairs") or []
                    if pairs:
                        best = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd", 0) or 0))
                        new_liq = float((best.get("liquidity") or {}).get("usd", 0) or 0)
                        entry_liq = r.get("entry_liq", 0)
                        won = new_liq > entry_liq * 1.20 if entry_liq > 0 else False
                        outcome = "win" if won else "loss"
                        bd = r.get("score_breakdown", {})
                        for factor in wins_by_factor:
                            if bd.get(factor, 0) > 50:
                                wins_by_factor[factor].append(1 if won else 0)
                except Exception:
                    pass
            r["resolved"] = True
            r["outcome"] = outcome
            newly_resolved += 1

    _save_tracking(records)

    resolved_all = [r for r in records if r.get("resolved") and r.get("outcome") in ("win", "loss")]
    total = len(resolved_all)
    wins = sum(1 for r in resolved_all if r["outcome"] == "win")

    weight_adjusted = False
    if total >= 20:
        _auto_adjust_weights(cfg, wins_by_factor)
        weight_adjusted = True

    return {
        "total": total,
        "wins": wins,
        "win_rate": wins / total if total else 0.0,
        "newly_resolved": newly_resolved,
        "weight_adjusted": weight_adjusted,
    }


def _auto_adjust_weights(cfg: dict, wins_by_factor: dict) -> None:
    """Nudge weights by up to 5% based on which factors correlated most with wins."""
    weights = cfg["weights"]
    factor_wr: dict[str, float] = {}
    for factor, outcomes in wins_by_factor.items():
        if len(outcomes) >= 5:
            factor_wr[factor] = sum(outcomes) / len(outcomes)

    if not factor_wr:
        return

    avg_wr = sum(factor_wr.values()) / len(factor_wr) if factor_wr else 0
    new_w = dict(weights)
    for factor in weights:
        if factor in factor_wr:
            relative = factor_wr[factor] / max(avg_wr, 0.01)
            nudge = (relative - 1.0) * 0.05
            new_w[factor] = max(0.10, min(0.50, weights[factor] + nudge))

    total_w = sum(new_w.values())
    if total_w > 0:
        for k in new_w:
            new_w[k] = round(new_w[k] / total_w, 3)

    cfg["weights"] = new_w
    _save_config(cfg)
    logger.info("alpha_engine: weights adjusted → %s", new_w)


# ── Formatting ────────────────────────────────────────────────────────────────

_SOURCE_EMOJI = {"coingecko": "🦎", "dexscreener": "📊", "pumpfun": "💊"}


def format_alpha_report(tokens: list[dict], header: str = "🚀 **Alpha 信号扫描 Top5**") -> str:
    if not tokens:
        return "🔍 当前无高评分 Alpha 信号（评分未达70分）\n\n数据来源: CoinGecko / DEXScreener / Pump.fun"

    lines = [header, ""]
    for i, t in enumerate(tokens, 1):
        emoji = _SOURCE_EMOJI.get(t["source"], "🔥")
        bd = t.get("score_breakdown", {})
        addr = t.get("address", "")
        addr_str = f"`{addr[:10]}...`" if addr else ""
        url = t.get("pair_url", "")

        price_str = ""
        p = t.get("price_usd", 0)
        if p and p > 0:
            price_str = f"   💰 入场价: ${p:.6f}" if p < 0.01 else f"   💰 入场价: ${p:.4f}"
            mcap = t.get("market_cap_usd", t.get("mcap", 0))
            if mcap and mcap > 0:
                price_str += f" | MCap: ${mcap/1e6:.1f}M" if mcap >= 1e6 else f" | MCap: ${mcap/1e3:.0f}K"
        line = (
            f"{i}. {emoji} {t['name']} (${t['symbol']})\n"
            f"   评分: {t['score']} | {t['source']} | {t.get('chain','?')}\n"
            f"   💧{bd.get('liquidity',0):.0f} "
            f"👥{bd.get('holder_dispersion',0):.0f} "
            f"📈{bd.get('price_momentum',0):.0f} "
            f"🔥{bd.get('community_heat',0):.0f}\n"
            f"   24h: {t.get('price_change_24h',0):+.1f}%"
        )
        if price_str:
            line += f"\n{price_str}"
        if addr_str:
            line += f" | {addr_str}"
        if url:
            line += f"\n   🔗 {url}"
        lines.append(line)

    lines.append(f"\n⏰ {datetime.now().strftime('%H:%M:%S')} | 分项: 流动性 持仓 动能 热度")
    result = "\n".join(lines)
    if len(result) > 4000:
        result = result[:3950] + "\n\n... (已截断)"
    return result


def format_alpha_stats() -> str:
    """Summary of tracking records for /alpha stats."""
    records = _load_tracking()
    total = len(records)
    resolved = [r for r in records if r.get("resolved") and r.get("outcome") in ("win", "loss")]
    wins = sum(1 for r in resolved if r["outcome"] == "win")
    pending = sum(1 for r in records if not r.get("resolved"))
    wr = wins / len(resolved) if resolved else 0.0

    cfg = _load_config()
    w = cfg["weights"]

    return (
        f"📊 **Alpha 信号追踪统计**\n\n"
        f"总推送: {total} | 已结算: {len(resolved)} | 待结算: {pending}\n"
        f"胜率: {wr:.1%} ({wins}/{len(resolved)})\n\n"
        f"当前权重:\n"
        f"  💧流动性: {w['liquidity']:.0%}\n"
        f"  👥持仓分散: {w['holder_dispersion']:.0%}\n"
        f"  📈价格动能: {w['price_momentum']:.0%}\n"
        f"  🔥社区热度: {w['community_heat']:.0%}"
    )


# ── Background engine ─────────────────────────────────────────────────────────

class AlphaEngine:
    """Background 30-min alpha scanner. Pushes Top5 to Telegram when score >= threshold."""

    def __init__(self, send_func=None):
        self._send = send_func
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._last_scan: list[dict] = []
        self._last_scan_time: float = 0.0

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="alpha_engine")
        self._task.add_done_callback(self._on_done)
        logger.info("AlphaEngine started (30-min social alpha scanner)")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def _on_done(self, task: asyncio.Task) -> None:
        if not task.cancelled():
            try:
                task.result()
            except Exception as e:
                logger.error("AlphaEngine loop crashed: %s", e, exc_info=True)

    async def _loop(self) -> None:
        await asyncio.sleep(180)   # 3-min startup delay
        while self._running:
            try:
                cfg = _load_config()
                tokens = await scan_alpha(cfg)
                self._last_scan = tokens[:50]  # cap stored results
                self._last_scan_time = time.time()

                if tokens and self._send:
                    report = format_alpha_report(tokens)
                    await self._send(report)
                    record_push(tokens)

                # Auto-open paper trades for alpha tokens
                if tokens:
                    try:
                        import paper_trader as _pt
                        if hasattr(_pt, 'on_signal_detected'):
                            await _pt.on_signal_detected(tokens)
                    except Exception:
                        pass

                # Onchain filter scan — only push NEW tokens (no duplicates)
                try:
                    oc_tokens = await scan_onchain_filter()
                    new_oc = onchain_filter_new_only(oc_tokens)
                    if new_oc and self._send:
                        oc_report = format_onchain_filter_report(new_oc)
                        await self._send(oc_report)
                    # Auto-open paper trades for onchain filter tokens
                    if new_oc:
                        try:
                            import paper_trader as _pt
                            if hasattr(_pt, 'on_signal_detected'):
                                await _pt.on_signal_detected(new_oc)
                        except Exception:
                            pass
                except Exception as e:
                    logger.debug("AlphaEngine: onchain filter error: %s", e)

                # Short-term P&L tracking (1h/4h/24h)
                try:
                    perf = await check_short_term_performance()
                    if perf.get("checked", 0) > 0:
                        logger.info(f"AlphaEngine: checked {perf['checked']} signals, "
                                    f"1h wins: {perf['wins_1h']}, losses: {perf['losses_1h']}")
                except Exception as e:
                    logger.debug("AlphaEngine: perf check error: %s", e)

                # Resolve 7-day old records and report win rate
                try:
                    stats = await resolve_old_records(cfg)
                    if stats["newly_resolved"] > 0 and self._send:
                        wa_note = " ⚖️ 权重已自动调整" if stats.get("weight_adjusted") else ""
                        # Include real P&L in the report
                        perf_text = get_performance_summary()
                        await self._send(
                            f"📊 7天Alpha绩效: 共{stats['total']}个 胜率{stats['win_rate']:.1%}"
                            f"{wa_note}\n\n{perf_text}"
                        )
                except Exception as e:
                    logger.debug("AlphaEngine: resolve error: %s", e)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("AlphaEngine scan error: %s", e)

            try:
                cfg = _load_config()
                await asyncio.sleep(cfg.get("scan_interval", 1800))
            except asyncio.CancelledError:
                break

    def get_last_scan(self) -> list[dict]:
        return list(self._last_scan)

    @property
    def running(self) -> bool:
        return self._running


# Module-level singleton
alpha_engine = AlphaEngine()
