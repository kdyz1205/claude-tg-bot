"""
onchain_filter.py — 链上量能筛选器

Filter Set 1:
  - 信号评分 >= 60（不分多空）
  - 3分钟成交量 > 8,888（币本位）
  - 5分钟成交量 > 16,666（币本位）
  - 市值 < 800万 USDT

Continuous monitoring: scans every 3 min, pushes to TG on hit.
Uses OKX + CoinGecko public APIs. No API key needed.
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

async def _fetch_okx_candles(symbol: str, bar: str, limit: int = 10) -> list:
    """OKX candles: returns [[ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm], ...]"""
    url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar={bar}&limit={limit}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            data = resp.json()
            if data.get("code") == "0":
                return list(reversed(data.get("data", [])))
    except Exception as e:
        logger.debug("onchain_filter: OKX candle %s %s: %s", symbol, bar, e)
    return []


async def _get_volume(symbol: str, bar: str, num_bars: int = 1) -> float:
    """Get total volume (coin-denominated) over last num_bars candles."""
    candles = await _fetch_okx_candles(symbol, bar, limit=num_bars + 1)
    if len(candles) < 2:
        return 0.0
    # Use completed candles only (skip the latest which may be incomplete)
    completed = candles[-(num_bars + 1):-1] if len(candles) > num_bars else candles[:-1]
    total_vol = sum(float(c[5]) for c in completed)
    return total_vol


async def _get_volume_usdt(symbol: str, bar: str, num_bars: int = 1) -> float:
    """Get total USDT volume over last num_bars candles."""
    candles = await _fetch_okx_candles(symbol, bar, limit=num_bars + 1)
    if len(candles) < 2:
        return 0.0
    completed = candles[-(num_bars + 1):-1] if len(candles) > num_bars else candles[:-1]
    # volCcyQuote = index 7 on OKX
    total_vol = sum(float(c[7]) if len(c) > 7 else 0.0 for c in completed)
    return total_vol


# ── Market cap via CoinGecko (free, no key) ─────────────────────────────────
_mcap_cache: dict = {}  # symbol -> (mcap, timestamp)
_MCAP_CACHE_TTL = 600   # 10 min cache

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


async def _fetch_mcap(symbol: str) -> Optional[float]:
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
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            data = resp.json()
            mcap = data.get(gecko_id, {}).get("usd_market_cap")
            if mcap is not None:
                _mcap_cache[base] = (mcap, time.time())
                return mcap
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
        tmp = str(ALERTED_FILE) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_alerted, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, str(ALERTED_FILE))
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass

def _is_new_alert(symbol: str, cooldown: int = 900) -> bool:
    """Only alert once per symbol per cooldown (default 15 min)."""
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
        k = 2 / (period + 1)
        e = data[0]
        for v in data[1:]:
            e = v * k + e * (1 - k)
        return e
    def ema_series(data, period):
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
    mom = (avg5 - avg20) / avg20 * 100
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

async def _scan_one(symbol: str, filters: dict) -> Optional[dict]:
    """Scan a single symbol against filter set."""
    try:
        # Fetch 3m and 5m candles + 1H for scoring in parallel
        candles_3m, candles_5m, candles_1h = await asyncio.gather(
            _fetch_okx_candles(symbol, "3m", limit=5),
            _fetch_okx_candles(symbol, "5m", limit=5),
            _fetch_okx_candles(symbol, "1H", limit=40),
        )

        # Volume check: latest completed 3m candle
        vol_3m = float(candles_3m[-2][5]) if len(candles_3m) >= 2 else 0.0
        vol_5m = float(candles_5m[-2][5]) if len(candles_5m) >= 2 else 0.0

        # USDT volume for display
        vol_3m_usdt = float(candles_3m[-2][7]) if len(candles_3m) >= 2 and len(candles_3m[-2]) > 7 else 0.0
        vol_5m_usdt = float(candles_5m[-2][7]) if len(candles_5m) >= 2 and len(candles_5m[-2]) > 7 else 0.0

        # Filter 1: 3m volume
        if vol_3m < filters["vol_3m_min"]:
            return None

        # Filter 2: 5m volume
        if vol_5m < filters["vol_5m_min"]:
            return None

        # Filter 3: market cap (skip if unavailable — CoinGecko rate limit)
        max_mcap = filters.get("max_mcap_usdt")
        mcap = await _fetch_mcap(symbol)
        if max_mcap and mcap is not None and mcap > max_mcap:
            return None

        # Score
        closes_1h = [float(c[4]) for c in candles_1h]
        direction, score = _quick_score(closes_1h)

        # Filter 4: minimum score
        if score < filters["min_score"]:
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


async def scan_filtered(
    symbols: list = None,
    filters: dict = None,
) -> list:
    """Run filter set on all symbols. Returns passing signals sorted by score desc."""
    if symbols is None:
        symbols = DEFAULT_SYMBOLS
    if filters is None:
        filters = DEFAULT_FILTER.copy()

    # Batch to avoid rate limits (10 concurrent)
    results = []
    batch_size = 10
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        tasks = [_scan_one(sym, filters) for sym in batch]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in batch_results:
            if isinstance(r, dict):
                results.append(r)
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
