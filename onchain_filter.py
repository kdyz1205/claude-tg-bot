"""
onchain_filter.py — 链上量能筛选器

Filter Set 1:
  - 信号评分 >= 60（不分多空）
  - 3分钟成交量 > 8,888（币本位）
  - 5分钟成交量 > 16,666（币本位）
  - 市值 < 800万 USDT

Continuous monitoring: scans every 3 min, pushes to TG on hit.
Uses OKX WebSocket (candles) + CoinGecko (mcap only). No API key needed.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# Shared HTTP policy: one client per scan avoids connection churn / RPC-style timeouts.
# Tight connect/read so slow nodes (e.g. meme-coin bursts) are abandoned — never block the loop.
_HTTP_TIMEOUT = httpx.Timeout(10.0, connect=3.0, read=8.0, write=8.0)
_HTTP_LIMITS = httpx.Limits(max_connections=24, max_keepalive_connections=12)
# CoinGecko: cap in-flight mcap requests
_GECKO_CONCURRENCY = asyncio.Semaphore(4)

# Hard ceiling per symbol: entire scan for one instId must finish or we drop it (no indefinite wait).
_DEFAULT_PER_SYMBOL_DEADLINE_SEC = 14.0


def _finite_non_negative(x: float) -> bool:
    return x == x and x >= 0 and abs(x) != float("inf")


def _safe_float(x, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if _finite_non_negative(v) else default
    except (TypeError, ValueError):
        return default

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ALERTED_FILE = os.path.join(BASE_DIR, ".vol_filter_alerted.json")

# ── Filter Set 1 defaults ───────────────────────────────────────────────────
DEFAULT_FILTER = {
    "min_score": 60,
    "vol_3m_min": 8888,
    "vol_5m_min": 16666,
    "max_mcap_usdt": 8_000_000,  # 800万 USDT
    "direction_filter": None,  # None = both long & short
}

SCAN_INTERVAL = 180  # 3 minutes between scans

# OKX top symbols to scan (expand as needed)
DEFAULT_SYMBOLS = [
    "BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT", "XRP-USDT",
    "DOGE-USDT", "ADA-USDT", "AVAX-USDT", "LINK-USDT", "DOT-USDT",
    "MATIC-USDT", "UNI-USDT", "LTC-USDT", "BCH-USDT", "ATOM-USDT",
    "FIL-USDT", "ARB-USDT", "OP-USDT", "APT-USDT", "SUI-USDT",
    "NEAR-USDT", "INJ-USDT", "TIA-USDT", "SEI-USDT", "PEPE-USDT",
    "WIF-USDT", "BONK-USDT", "RENDER-USDT", "FET-USDT", "ONDO-USDT",
]


# ── Data fetching ────────────────────────────────────────────────────────────

async def _fetch_okx_candles(
    client: httpx.AsyncClient, symbol: str, bar: str, limit: int = 10
) -> list:
    """OKX candles from public WSS hub (oldest-first). ``client`` unused (mcap still uses HTTP)."""
    _ = client
    try:
        from trading import okx_ws_hub

        await okx_ws_hub.ensure_started()
        rows = okx_ws_hub.get_candles_sync(symbol, bar, limit=limit + 5)
        return rows[-limit:] if len(rows) > limit else rows
    except Exception as e:
        logger.debug("onchain_filter: OKX WS candle %s %s: %s", symbol, bar, e)
    return []


async def _get_volume(
    client: httpx.AsyncClient, symbol: str, bar: str, num_bars: int = 1
) -> float:
    """Get total volume (coin-denominated) over last num_bars candles."""
    candles = await _fetch_okx_candles(client, symbol, bar, limit=num_bars + 1)
    if len(candles) < 2:
        return 0.0
    # Use completed candles only (skip the latest which may be incomplete)
    completed = candles[-(num_bars + 1):-1] if len(candles) > num_bars else candles[:-1]
    total_vol = sum(_safe_float(c[5]) for c in completed if len(c) > 5)
    return total_vol


async def _get_volume_usdt(
    client: httpx.AsyncClient, symbol: str, bar: str, num_bars: int = 1
) -> float:
    """Get total USDT volume over last num_bars candles."""
    candles = await _fetch_okx_candles(client, symbol, bar, limit=num_bars + 1)
    if len(candles) < 2:
        return 0.0
    completed = candles[-(num_bars + 1):-1] if len(candles) > num_bars else candles[:-1]
    # volCcyQuote = index 7 on OKX
    total_vol = sum(_safe_float(c[7]) if len(c) > 7 else 0.0 for c in completed)
    return total_vol


# ── Market cap via CoinGecko (free, no key) ─────────────────────────────────
_mcap_cache: dict = {}  # symbol -> (mcap, timestamp)
_MCAP_CACHE_TTL = 600   # 10 min cache
_MCAP_CACHE_MAX = 500

# OKX symbol -> CoinGecko ID mapping
_GECKO_IDS = {
    "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "BNB": "binancecoin",
    "XRP": "ripple", "DOGE": "dogecoin", "ADA": "cardano", "AVAX": "avalanche-2",
    "LINK": "chainlink", "DOT": "polkadot", "MATIC": "matic-network",
    "UNI": "uniswap", "LTC": "litecoin", "BCH": "bitcoin-cash", "ATOM": "cosmos",
    "FIL": "filecoin", "ARB": "arbitrum", "OP": "optimism", "APT": "aptos",
    "SUI": "sui", "NEAR": "near", "INJ": "injective-protocol", "TIA": "celestia",
    "SEI": "sei-network", "PEPE": "pepe", "WIF": "dogwifcoin", "BONK": "bonk",
    "RENDER": "render-token", "FET": "fetch-ai", "ONDO": "ondo-finance",
}


async def _fetch_mcap(client: httpx.AsyncClient, symbol: str) -> Optional[float]:
    """Get market cap in USDT via CoinGecko. Returns None if unavailable."""
    base = symbol.split("-")[0]
    gecko_id = _GECKO_IDS.get(base)
    if not gecko_id:
        return None

    # Check cache
    cached = _mcap_cache.get(base)
    if cached and time.time() - cached[1] < _MCAP_CACHE_TTL:
        return cached[0]

    url = f"https://api.coingecko.com/api/v3/simple/price?ids={gecko_id}&vs_currencies=usd&include_market_cap=true"
    try:
        async with _GECKO_CONCURRENCY:
            resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
        mcap = data.get(gecko_id, {}).get("usd_market_cap")
        if mcap is not None:
            mcap_f = float(mcap)
            if not _finite_non_negative(mcap_f):
                return None
            _mcap_cache[base] = (mcap_f, time.time())
            # Evict stale entries when cache is too large
            if len(_mcap_cache) > _MCAP_CACHE_MAX:
                now = time.time()
                stale = [k for k, (_, ts) in _mcap_cache.items() if now - ts > _MCAP_CACHE_TTL]
                for k in stale:
                    del _mcap_cache[k]
            return mcap_f
    except Exception as e:
        logger.debug("onchain_filter: mcap fetch %s: %s", symbol, e)
    return None


# ── Alert dedup ──────────────────────────────────────────────────────────────
_alerted: dict = {}  # symbol -> last_alert_ts

def _load_alerted():
    global _alerted
    try:
        if os.path.exists(ALERTED_FILE):
            with open(ALERTED_FILE, "r", encoding="utf-8") as f:
                _alerted = json.load(f)
    except Exception:
        _alerted = {}

def _save_alerted():
    try:
        # Prune entries older than 24h
        now = time.time()
        stale = [k for k, ts in _alerted.items() if now - ts > 86400]
        for k in stale:
            del _alerted[k]
        tmp = str(ALERTED_FILE) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_alerted, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, str(ALERTED_FILE))
    except Exception:
        try:
            os.unlink(str(ALERTED_FILE) + ".tmp")
        except OSError:
            pass

_alerted_loaded = False

def _is_new_alert(symbol: str, cooldown: int = 900) -> bool:
    """Only alert once per symbol per cooldown (default 15 min)."""
    global _alerted_loaded
    if not _alerted_loaded:
        _load_alerted()
        _alerted_loaded = True
    now = time.time()
    last = _alerted.get(symbol, 0)
    if now - last < cooldown:
        return False
    _alerted[symbol] = now
    _save_alerted()
    return True


# ── Quick scoring (simplified from signal_engine) ────────────────────────────

def _rsi(closes: list, period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss < 1e-10:
        return 100.0
    rs = avg_gain / (avg_loss + 1e-10)
    return 100 - 100 / (1 + rs)


def _macd(closes: list, fast=12, slow=26, signal=9):
    if len(closes) < slow + signal:
        return None, None
    def ema(data, period):
        if not data:
            return 0.0
        k = 2 / (period + 1)
        e = data[0]
        for v in data[1:]:
            e = v * k + e * (1 - k)
        return e
    def ema_series(data, period):
        if not data:
            return [0.0]
        k = 2 / (period + 1)
        result = [data[0]]
        for v in data[1:]:
            result.append(v * k + result[-1] * (1 - k))
        return result
    # Compute MACD line as series over recent window
    ema_fast_series = ema_series(closes[-(slow + signal):], fast)
    ema_slow_series = ema_series(closes[-(slow + signal):], slow)
    macd_series = [f - s for f, s in zip(ema_fast_series, ema_slow_series)]
    # Signal line = 9-period EMA of MACD series
    signal_series = ema_series(macd_series, signal)
    macd_val = macd_series[-1]
    signal_val = signal_series[-1]
    return macd_val, signal_val


def _quick_score(closes: list) -> tuple:
    """Returns (direction, score 0-100) using RSI + MA + MACD."""
    if len(closes) < 30:
        return "neutral", 0

    score = 50  # start neutral
    direction_votes = {"long": 0, "short": 0}

    # RSI
    rsi = _rsi(closes)
    if rsi is not None:
        if rsi < 30:
            direction_votes["long"] += 2
            score += 15
        elif rsi < 40:
            direction_votes["long"] += 1
            score += 8
        elif rsi > 70:
            direction_votes["short"] += 2
            score += 15
        elif rsi > 60:
            direction_votes["short"] += 1
            score += 8

    # MA crossover
    ma9 = sum(closes[-9:]) / 9
    ma21 = sum(closes[-21:]) / 21
    if ma9 > ma21 * 1.005:
        direction_votes["long"] += 1
        score += 10
    elif ma9 < ma21 * 0.995:
        direction_votes["short"] += 1
        score += 10

    # MACD
    macd_val, _ = _macd(closes)
    if macd_val is not None:
        if macd_val > 0:
            direction_votes["long"] += 1
            score += 8
        elif macd_val < 0:
            direction_votes["short"] += 1
            score += 8

    # Price momentum (last 5 vs last 20 average)
    avg5 = sum(closes[-5:]) / 5
    avg20 = sum(closes[-20:]) / 20
    mom = (avg5 - avg20) / avg20 * 100 if avg20 != 0 else 0
    if abs(mom) > 2:
        score += 10
        if mom > 0:
            direction_votes["long"] += 1
        else:
            direction_votes["short"] += 1

    # Volume trend boost handled externally

    if direction_votes["long"] > direction_votes["short"]:
        direction = "long"
    elif direction_votes["short"] > direction_votes["long"]:
        direction = "short"
    else:
        direction = "neutral"

    return direction, min(100, score)


# ── Main filter scanner ──────────────────────────────────────────────────────

async def _scan_one(client: httpx.AsyncClient, symbol: str, filters: dict) -> Optional[dict]:
    """Scan a single symbol against filter set."""
    try:
        # Fetch 3m and 5m candles + 1H for scoring in parallel
        candles_3m, candles_5m, candles_1h = await asyncio.gather(
            _fetch_okx_candles(client, symbol, "3m", limit=5),
            _fetch_okx_candles(client, symbol, "5m", limit=5),
            _fetch_okx_candles(client, symbol, "1H", limit=40),
        )

        # Volume check: latest completed 3m candle
        vol_3m = _safe_float(candles_3m[-2][5]) if len(candles_3m) >= 2 and len(candles_3m[-2]) > 5 else 0.0
        vol_5m = _safe_float(candles_5m[-2][5]) if len(candles_5m) >= 2 and len(candles_5m[-2]) > 5 else 0.0

        # USDT volume for display
        vol_3m_usdt = _safe_float(candles_3m[-2][7]) if len(candles_3m) >= 2 and len(candles_3m[-2]) > 7 else 0.0
        vol_5m_usdt = _safe_float(candles_5m[-2][7]) if len(candles_5m) >= 2 and len(candles_5m[-2]) > 7 else 0.0

        # Filter 1: 3m volume
        if vol_3m < filters.get("vol_3m_min", 0):
            return None

        # Filter 2: 5m volume
        if vol_5m < filters.get("vol_5m_min", 0):
            return None

        # Filter 3: market cap (skip if unavailable — CoinGecko rate limit)
        max_mcap = filters.get("max_mcap_usdt")
        mcap = await _fetch_mcap(client, symbol)
        if max_mcap and mcap is not None and mcap > max_mcap:
            return None

        # Score
        closes_1h = [_safe_float(c[4]) for c in candles_1h if len(c) > 4]
        closes_1h = [c for c in closes_1h if c > 0]
        direction, score = _quick_score(closes_1h)

        # Filter 4: minimum score
        if score < filters.get("min_score", 0):
            return None

        # Filter 5: direction (None = both)
        dir_filter = filters.get("direction_filter")
        if dir_filter and direction != dir_filter:
            return None

        # Current price
        price = closes_1h[-1] if closes_1h else 0

        # RSI for display
        rsi = _rsi(closes_1h)

        return {
            "symbol": symbol,
            "direction": direction,
            "score": score,
            "price": price,
            "vol_3m": vol_3m,
            "vol_5m": vol_5m,
            "vol_3m_usdt": vol_3m_usdt,
            "vol_5m_usdt": vol_5m_usdt,
            "mcap": mcap,
            "rsi": round(rsi, 1) if rsi else None,
            "timestamp": time.time(),
        }
    except Exception as e:
        logger.debug("onchain_filter: %s error: %s", symbol, e)
        return None


async def _scan_one_with_deadline(
    client: httpx.AsyncClient,
    symbol: str,
    filters: dict,
    deadline_sec: float,
) -> Optional[dict]:
    """Run _scan_one under asyncio.wait_for; timeout → drop symbol, no stall."""
    try:
        return await asyncio.wait_for(
            _scan_one(client, symbol, filters),
            timeout=deadline_sec,
        )
    except asyncio.TimeoutError:
        logger.warning(
            "onchain_filter: dropped %s (per-symbol deadline %.1fs exceeded)",
            symbol,
            deadline_sec,
        )
        return None
    except asyncio.CancelledError:
        raise


async def scan_filtered(
    symbols: list = None,
    filters: dict = None,
    *,
    per_symbol_deadline_sec: float | None = None,
) -> list:
    """Run filter set on all symbols. Returns passing signals sorted by score desc.

    per_symbol_deadline_sec:
        If set, each symbol scan is aborted after this many seconds (default from module constant).
        Prevents one dead endpoint from wedging the event loop when scanning long meme lists.
    """
    if symbols is None:
        symbols = DEFAULT_SYMBOLS
    if filters is None:
        filters = DEFAULT_FILTER.copy()

    deadline = (
        per_symbol_deadline_sec
        if per_symbol_deadline_sec is not None
        else _DEFAULT_PER_SYMBOL_DEADLINE_SEC
    )

    try:
        from trading import okx_ws_hub

        await okx_ws_hub.ensure_started(symbols)
    except Exception as e:
        logger.debug("onchain_filter: okx_ws_hub ensure_started: %s", e)

    # Batch to avoid rate limits (10 concurrent)
    results = []
    batch_size = 10
    headers = {"Accept": "application/json", "User-Agent": "claude-tg-bot/onchain_filter"}
    async with httpx.AsyncClient(
        timeout=_HTTP_TIMEOUT, limits=_HTTP_LIMITS, headers=headers
    ) as client:
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            tasks = [
                _scan_one_with_deadline(client, sym, filters, deadline)
                for sym in batch
            ]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in batch_results:
                if isinstance(r, dict):
                    results.append(r)
                elif isinstance(r, Exception):
                    logger.debug("onchain_filter batch item error: %s", r)
            if i + batch_size < len(symbols):
                await asyncio.sleep(0.5)  # rate limit buffer

    results.sort(key=lambda x: x["score"], reverse=True)
    return results


def format_filtered(signals: list) -> str:
    """Format filtered signals for Telegram."""
    if not signals:
        return "🔍 **量能筛选** — 当前无符合条件的币\n\n过滤条件: 评分≥60 | 3m量>8,888 | 5m量>16,666"

    lines = [
        "🔍 **链上量能筛选** (不分多空)",
        f"过滤: 评分≥60 | 3m量>{DEFAULT_FILTER['vol_3m_min']:,} | 5m量>{DEFAULT_FILTER['vol_5m_min']:,}",
        f"扫描 {len(DEFAULT_SYMBOLS)} 币 | {datetime.now().strftime('%H:%M:%S')}",
        "─" * 30,
    ]

    for sig in signals:
        direction = sig.get("direction", "neutral")
        emoji = "🟢" if direction == "long" else ("🔴" if direction == "short" else "⚪")
        vol3_k = sig.get("vol_3m_usdt", 0) / 1000
        vol5_k = sig.get("vol_5m_usdt", 0) / 1000

        lines.append(
            f"{emoji} **{sig.get('symbol', '?')}** {direction.upper()} "
            f"| 评分:{sig.get('score', 0)}"
        )
        lines.append(
            f"  💰 ${sig.get('price', 0):.4f}  RSI:{sig.get('rsi', '-')}"
        )
        lines.append(
            f"  📊 3m量:{sig.get('vol_3m', 0):,.0f} (${vol3_k:,.0f}K)"
            f"  5m量:{sig.get('vol_5m', 0):,.0f} (${vol5_k:,.0f}K)"
        )
        lines.append("")

    lines.append(f"共 **{len(signals)}** 币通过筛选")
    return "\n".join(lines)


# ── CLI test ─────────────────────────────────────────────────────────────────

async def _main():
    print("Scanning with Filter Set 1...")
    results = await scan_filtered()
    print(format_filtered(results))


if __name__ == "__main__":
    asyncio.run(_main())
