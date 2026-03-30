"""
signal_engine.py — Multi-exchange aggregated signal generator.

Sources: OKX + Binance + Bybit price/orderbook data.
Signals include confidence score 0-100; only >70 are emitted.
Detects volume-price divergence and order book pressure.
Performance tracked in .signal_performance.json.
"""

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE           = os.path.join(BASE_DIR, "_signal_engine_config.json")
PERF_FILE             = os.path.join(BASE_DIR, ".signal_performance.json")
OPTIMIZED_PARAMS_FILE = os.path.join(BASE_DIR, ".optimized_params.json")

CONFIDENCE_THRESHOLD = 70
OUTCOME_CHECK_DELAY    = 4 * 3600   # 4 hours in seconds
OUTCOME_CHECK_24H      = 24 * 3600  # 24 hours
OUTCOME_CHECK_72H      = 72 * 3600  # 72 hours

# ── Evolution: cooldown dedup ────────────────────────────────────────────────
_signal_cooldowns: dict = {}  # symbol → last_signal_timestamp
COOLDOWN_SECONDS = 3600  # same coin must wait 1h between signals

DEFAULT_CONFIG = {
    "rsi_period": 14,
    "rsi_overbought": 70,
    "rsi_oversold": 30,
    "ma_fast": 9,
    "ma_slow": 21,
    "signal_score_threshold": 2,
    "scan_interval": 900,
    "timeframe": "1H",
    "confidence_threshold": CONFIDENCE_THRESHOLD,
    "symbols": [
        "BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT", "XRP-USDT",
        "DOGE-USDT", "ADA-USDT", "AVAX-USDT", "LINK-USDT", "DOT-USDT",
        "MATIC-USDT", "UNI-USDT", "LTC-USDT", "BCH-USDT", "ATOM-USDT",
        "FIL-USDT", "TRX-USDT", "ETC-USDT", "XLM-USDT", "NEAR-USDT",
    ],
}


def load_config() -> dict:
    cfg = dict(DEFAULT_CONFIG)
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                overrides = json.load(f)
            cfg.update(overrides)
        except Exception as e:
            logger.warning("signal_engine: failed to load config: %s", e)
    # P3_24: overlay GA-optimized params if available
    if os.path.exists(OPTIMIZED_PARAMS_FILE):
        try:
            with open(OPTIMIZED_PARAMS_FILE, "r", encoding="utf-8") as f:
                optim = json.load(f)
            params = optim.get("params", {})
            # rsi_threshold → symmetric oversold/overbought
            if "rsi_threshold" in params:
                t = int(params["rsi_threshold"])
                cfg["rsi_oversold"]   = t
                cfg["rsi_overbought"] = 100 - t
            # ma_period → slow MA; fast MA = period // 3 (min 3)
            if "ma_period" in params:
                slow = int(params["ma_period"])
                cfg["ma_slow"] = slow
                cfg["ma_fast"] = max(3, slow // 3)
            # vol_multiplier stored for downstream consumers
            if "vol_multiplier" in params:
                cfg["vol_multiplier_p3_24"] = float(params["vol_multiplier"])
            # hold_hours stored for reference
            if "hold_hours" in params:
                cfg["hold_hours_p3_24"] = int(params["hold_hours"])
        except Exception as e:
            logger.debug("signal_engine: optimized_params load error: %s", e)
    return cfg


def save_config(cfg: dict) -> None:
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


# ── Symbol format conversion ──────────────────────────────────────────────────

def _to_binance(symbol: str) -> str:
    """BTC-USDT → BTCUSDT"""
    return symbol.replace("-", "")


def _to_bybit(symbol: str) -> str:
    """BTC-USDT → BTCUSDT"""
    return symbol.replace("-", "")


def _tf_to_binance(tf: str) -> str:
    """1H → 1h, 4H → 4h, 1D → 1d"""
    return tf.lower()


def _tf_to_bybit(tf: str) -> str:
    """1H → 60, 4H → 240, 1D → D"""
    mapping = {"1H": "60", "4H": "240", "1D": "D", "15M": "15", "5M": "5", "1M": "1"}
    return mapping.get(tf.upper(), "60")


# ── Data fetching: OKX ────────────────────────────────────────────────────────

async def _fetch_okx_candles(symbol: str, bar: str, limit: int = 60) -> list:
    url = (
        f"https://www.okx.com/api/v5/market/candles"
        f"?instId={symbol}&bar={bar}&limit={limit}"
    )
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            data = resp.json()
            if data.get("code") == "0":
                return list(reversed(data.get("data", [])))
    except Exception as e:
        logger.debug("okx_candles %s: %s", symbol, e)
    return []


async def _fetch_okx_orderbook(symbol: str, sz: int = 20) -> dict:
    url = f"https://www.okx.com/api/v5/market/books?instId={symbol}&sz={sz}"
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(url)
            data = resp.json()
            if data.get("code") == "0" and data.get("data"):
                book = data["data"][0]
                return {
                    "bids": [[float(b[0]), float(b[1])] for b in book.get("bids", [])],
                    "asks": [[float(a[0]), float(a[1])] for a in book.get("asks", [])],
                }
    except Exception as e:
        logger.debug("okx_orderbook %s: %s", symbol, e)
    return {}


# ── Data fetching: Binance ────────────────────────────────────────────────────

async def _fetch_binance_candles(symbol: str, interval: str, limit: int = 60) -> list:
    url = (
        f"https://api.binance.com/api/v3/klines"
        f"?symbol={symbol}&interval={interval}&limit={limit}"
    )
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                return resp.json()
    except Exception as e:
        logger.debug("binance_candles %s: %s", symbol, e)
    return []


async def _fetch_binance_orderbook(symbol: str, limit: int = 20) -> dict:
    url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit={limit}"
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                return {
                    "bids": [[float(b[0]), float(b[1])] for b in data.get("bids", [])],
                    "asks": [[float(a[0]), float(a[1])] for a in data.get("asks", [])],
                }
    except Exception as e:
        logger.debug("binance_orderbook %s: %s", symbol, e)
    return {}


# ── Data fetching: Bybit ──────────────────────────────────────────────────────

async def _fetch_bybit_candles(symbol: str, interval: str, limit: int = 60) -> list:
    """Returns [[ts, o, h, l, c, vol, turnover], ...] oldest-first."""
    url = (
        f"https://api.bybit.com/v5/market/kline"
        f"?category=spot&symbol={symbol}&interval={interval}&limit={limit}"
    )
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            data = resp.json()
            if data.get("retCode") == 0:
                rows = data.get("result", {}).get("list", [])
                return list(reversed(rows))
    except Exception as e:
        logger.debug("bybit_candles %s: %s", symbol, e)
    return []


async def _fetch_bybit_orderbook(symbol: str, limit: int = 20) -> dict:
    url = (
        f"https://api.bybit.com/v5/market/orderbook"
        f"?category=spot&symbol={symbol}&limit={limit}"
    )
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(url)
            data = resp.json()
            if data.get("retCode") == 0:
                result = data.get("result", {})
                return {
                    "bids": [[float(b[0]), float(b[1])] for b in result.get("b", [])],
                    "asks": [[float(a[0]), float(a[1])] for a in result.get("a", [])],
                }
    except Exception as e:
        logger.debug("bybit_orderbook %s: %s", symbol, e)
    return {}


# ── Candle data accessors ─────────────────────────────────────────────────────

def _closes_okx(candles: list) -> list:
    return [float(c[4]) for c in candles]


def _volumes_okx(candles: list) -> list:
    return [float(c[5]) for c in candles]


def _closes_binance(candles: list) -> list:
    return [float(c[4]) for c in candles]


def _closes_bybit(candles: list) -> list:
    # Bybit kline: [startTime, open, high, low, close, volume, turnover]
    return [float(c[4]) for c in candles]


# ── Indicator calculations ────────────────────────────────────────────────────

def _rsi(closes: list, period: int) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0))
        losses.append(max(-delta, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    return round(100 - 100 / (1 + avg_gain / avg_loss), 2)


def _ema(values: list, period: int) -> Optional[float]:
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = v * k + ema * (1 - k)
    return round(ema, 6)


def _macd(closes: list) -> Optional[float]:
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    if ema12 is None or ema26 is None:
        return None
    return round(ema12 - ema26, 6)


def _ma(closes: list, period: int) -> Optional[float]:
    if len(closes) < period:
        return None
    return round(sum(closes[-period:]) / period, 6)


# ── Evolution: ATR (Average True Range) ─────────────────────────────────────

def _atr(candles_okx: list, period: int = 14) -> Optional[float]:
    """ATR from OKX candles [ts,o,h,l,c,vol,volCcy,volCcyQuote,confirm]."""
    if len(candles_okx) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles_okx)):
        h = float(candles_okx[i][2])
        l = float(candles_okx[i][3])
        prev_c = float(candles_okx[i - 1][4])
        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        trs.append(tr)
    return sum(trs[-period:]) / period


def _atr_pct(candles_okx: list, period: int = 14) -> Optional[float]:
    """ATR as % of current price — measures volatility."""
    atr_val = _atr(candles_okx, period)
    if atr_val is None or len(candles_okx) < 2:
        return None
    price = float(candles_okx[-1][4])
    if price == 0:
        return None
    return round(atr_val / price * 100, 4)


# ── Evolution: ADX (Average Directional Index) ──────────────────────────────

def _adx(candles_okx: list, period: int = 14) -> Optional[float]:
    """ADX — trend strength 0-100. >25 = trending, <20 = ranging."""
    if len(candles_okx) < period * 2 + 1:
        return None
    plus_dms, minus_dms, trs = [], [], []
    for i in range(1, len(candles_okx)):
        h = float(candles_okx[i][2])
        l = float(candles_okx[i][3])
        prev_h = float(candles_okx[i - 1][2])
        prev_l = float(candles_okx[i - 1][3])
        prev_c = float(candles_okx[i - 1][4])
        plus_dm = max(h - prev_h, 0) if (h - prev_h) > (prev_l - l) else 0
        minus_dm = max(prev_l - l, 0) if (prev_l - l) > (h - prev_h) else 0
        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        plus_dms.append(plus_dm)
        minus_dms.append(minus_dm)
        trs.append(tr)
    # Smoothed sums
    atr_s = sum(trs[:period])
    plus_s = sum(plus_dms[:period])
    minus_s = sum(minus_dms[:period])
    dxs = []
    for i in range(period, len(trs)):
        atr_s = atr_s - atr_s / period + trs[i]
        plus_s = plus_s - plus_s / period + plus_dms[i]
        minus_s = minus_s - minus_s / period + minus_dms[i]
        if atr_s == 0:
            dxs.append(0)
            continue
        plus_di = plus_s / atr_s * 100
        minus_di = minus_s / atr_s * 100
        di_sum = plus_di + minus_di
        if di_sum == 0:
            dxs.append(0)
        else:
            dxs.append(abs(plus_di - minus_di) / di_sum * 100)
    if len(dxs) < period:
        return None
    adx_val = sum(dxs[-period:]) / period
    return round(adx_val, 2)


# ── Evolution: Volume Spike Detection ────────────────────────────────────────

def _volume_spike(volumes: list, lookback: int = 20, threshold: float = 2.0) -> bool:
    """True if latest volume > threshold * average of previous volumes."""
    if len(volumes) < lookback + 1:
        return False
    avg_vol = sum(volumes[-(lookback + 1):-1]) / lookback
    if avg_vol == 0:
        return False
    return volumes[-1] > avg_vol * threshold


def _quick_direction(closes: list, cfg: dict) -> str:
    """RSI + MA + MACD → long / short / neutral."""
    if len(closes) < cfg["ma_slow"] + 1:
        return "neutral"
    rsi = _rsi(closes, cfg["rsi_period"])
    ma_fast = _ma(closes, cfg["ma_fast"])
    ma_slow = _ma(closes, cfg["ma_slow"])
    macd_line = _macd(closes)

    bulls = 0
    bears = 0
    if rsi is not None:
        if rsi < cfg["rsi_oversold"]:
            bulls += 1
        elif rsi > cfg["rsi_overbought"]:
            bears += 1
    if ma_fast is not None and ma_slow is not None:
        if ma_fast > ma_slow:
            bulls += 1
        else:
            bears += 1
    if macd_line is not None:
        if macd_line > 0:
            bulls += 1
        else:
            bears += 1

    if bulls > bears:
        return "long"
    elif bears > bulls:
        return "short"
    return "neutral"


# ── Volume-Price Divergence ───────────────────────────────────────────────────

def _detect_vol_divergence(closes: list, volumes: list, lookback: int = 10) -> str:
    """
    bearish_div  — price making new high but volume declining
    bullish_div  — price making new low but volume declining (potential reversal)
    none         — no divergence detected
    """
    if len(closes) < lookback or len(volumes) < lookback:
        return "none"
    recent_closes = closes[-lookback:]
    recent_vols = volumes[-lookback:]

    price_up = recent_closes[-1] > max(recent_closes[:3])
    price_down = recent_closes[-1] < min(recent_closes[:3])

    avg_vol_early = sum(recent_vols[:3]) / 3
    avg_vol_late = sum(recent_vols[-3:]) / 3
    vol_declining = avg_vol_late < avg_vol_early * 0.8

    if price_up and vol_declining:
        return "bearish_div"
    if price_down and vol_declining:
        return "bullish_div"
    return "none"


# ── Order Book Pressure ───────────────────────────────────────────────────────

def _orderbook_pressure(book: dict, top_n: int = 10) -> str:
    """
    Returns "buy" (big bid wall), "sell" (big ask wall), or "neutral".
    """
    if not book:
        return "neutral"
    bids = book.get("bids", [])[:top_n]
    asks = book.get("asks", [])[:top_n]
    if not bids or not asks:
        return "neutral"

    bid_vol = sum(b[1] for b in bids)
    ask_vol = sum(a[1] for a in asks)
    if bid_vol + ask_vol == 0:
        return "neutral"

    ratio = bid_vol / (ask_vol + 1e-9)
    if ratio > 1.5:
        return "buy"
    if ratio < 0.67:
        return "sell"
    return "neutral"


# ── Confidence Score ──────────────────────────────────────────────────────────

def _compute_confidence(
    okx_dir: str,
    bin_dir: str,
    bybit_dir: str,
    primary_dir: str,
    indis: dict,
    vol_div: str,
    ob_pressure: str,
    cfg: dict,
) -> int:
    """Compute 0-100 confidence score."""
    score = 0

    # 1. Exchange direction consensus (0-40)
    all_dirs = [okx_dir, bin_dir, bybit_dir]
    non_neutral = [d for d in all_dirs if d != "neutral"]
    agrees = sum(1 for d in non_neutral if d == primary_dir)
    total = len(non_neutral)
    if total >= 3 and agrees == 3:
        score += 40
    elif total >= 2 and agrees >= 2:
        score += 25
    elif agrees == 1:
        score += 10

    # 2. Technical indicator agreement (0-30)
    rsi = indis.get("rsi")
    ma_fast = indis.get("ma_fast")
    ma_slow = indis.get("ma_slow")
    macd = indis.get("macd")

    indi_agree = 0
    indi_total = 0
    if rsi is not None:
        indi_total += 1
        if primary_dir == "long" and rsi < cfg["rsi_oversold"]:
            indi_agree += 1
        elif primary_dir == "short" and rsi > cfg["rsi_overbought"]:
            indi_agree += 1
    if ma_fast is not None and ma_slow is not None:
        indi_total += 1
        if primary_dir == "long" and ma_fast > ma_slow:
            indi_agree += 1
        elif primary_dir == "short" and ma_fast < ma_slow:
            indi_agree += 1
    if macd is not None:
        indi_total += 1
        if primary_dir == "long" and macd > 0:
            indi_agree += 1
        elif primary_dir == "short" and macd < 0:
            indi_agree += 1
    if indi_total > 0:
        score += round((indi_agree / indi_total) * 30)

    # 3. Volume-price divergence (0-15)
    if vol_div == "none":
        score += 15
    elif vol_div == "bullish_div" and primary_dir == "long":
        score += 10
    elif vol_div == "bearish_div" and primary_dir == "short":
        score += 10

    # 4. Order book pressure alignment (0-15)
    if ob_pressure == "buy" and primary_dir == "long":
        score += 15
    elif ob_pressure == "sell" and primary_dir == "short":
        score += 15
    elif ob_pressure == "neutral":
        score += 7

    return min(score, 100)


# ── Performance Tracking ──────────────────────────────────────────────────────

def _load_performance() -> dict:
    if os.path.exists(PERF_FILE):
        try:
            with open(PERF_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"signals": [], "stats_24h": {}}


def _save_performance(perf: dict) -> None:
    try:
        with open(PERF_FILE, "w", encoding="utf-8") as f:
            json.dump(perf, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning("signal_engine: save_performance: %s", e)


def record_signal_performance(sig: dict) -> None:
    """Record a new signal for performance tracking."""
    perf = _load_performance()
    cfg_snapshot = {
        k: sig.get(k)
        for k in ("rsi", "ma_fast", "ma_slow", "macd", "confidence", "timeframe")
    }
    # Also capture active config params for Bayesian optimization tracking
    try:
        active_cfg = load_config()
        cfg_snapshot["params"] = {
            k: active_cfg.get(k)
            for k in ("rsi_period", "rsi_overbought", "rsi_oversold",
                      "ma_fast", "ma_slow", "signal_score_threshold",
                      "confidence_threshold")
        }
    except Exception:
        pass
    entry = {
        "id": str(uuid.uuid4())[:8],
        "symbol": sig.get("symbol", "UNKNOWN"),
        "direction": sig.get("direction", "neutral"),
        "confidence": sig.get("confidence", 0),
        "entry_price": sig.get("entry_price", 0),
        "timestamp": sig.get("timestamp", time.time()),
        "cfg_snapshot": cfg_snapshot,
        # 4h outcome (original)
        "outcome": None,
        "outcome_price": None,
        "outcome_time": None,
        # 24h outcome
        "outcome_24h": None,
        "outcome_price_24h": None,
        # 72h outcome
        "outcome_72h": None,
        "outcome_price_72h": None,
    }
    perf["signals"].append(entry)
    if len(perf["signals"]) > 500:
        perf["signals"] = perf["signals"][-500:]
    _save_performance(perf)


async def _check_pending_outcomes() -> None:
    """Check signals that are >4h/24h/72h old and update their outcome."""
    perf = _load_performance()
    now = time.time()

    # Gather symbols needing any kind of outcome update
    needs_price = set()
    for s in perf.get("signals", []):
        age = now - s.get("timestamp", 0)
        if s.get("outcome") is None and age >= OUTCOME_CHECK_DELAY:
            needs_price.add(s.get("symbol", ""))
        if s.get("outcome_24h") is None and age >= OUTCOME_CHECK_24H:
            needs_price.add(s.get("symbol", ""))
        if s.get("outcome_72h") is None and age >= OUTCOME_CHECK_72H:
            needs_price.add(s.get("symbol", ""))
    needs_price.discard("")

    if not needs_price:
        return

    current_prices: dict = {}
    async with httpx.AsyncClient(timeout=10.0) as client:
        for sym in needs_price:
            try:
                url = f"https://www.okx.com/api/v5/market/ticker?instId={sym}"
                resp = await client.get(url)
                data = resp.json()
                if data.get("code") == "0" and data.get("data"):
                    current_prices[sym] = float(data["data"][0].get("last", 0))
            except Exception:
                pass

    changed = False
    for entry in perf.get("signals", []):
        age = now - entry.get("timestamp", 0)
        price = current_prices.get(entry.get("symbol", ""))
        if price is None:
            continue

        def _win_loss(direction: str, entry_price: float, cur_price: float) -> str:
            if direction == "long":
                return "win" if cur_price > entry_price else "loss"
            return "win" if cur_price < entry_price else "loss"

        # 4h outcome
        if entry.get("outcome") is None and age >= OUTCOME_CHECK_DELAY:
            entry["outcome_price"] = price
            entry["outcome_time"] = now
            entry["outcome"] = _win_loss(entry.get("direction", "long"), entry.get("entry_price", 0), price)
            changed = True

        # 24h outcome
        if entry.get("outcome_24h") is None and age >= OUTCOME_CHECK_24H:
            entry["outcome_price_24h"] = price
            entry["outcome_24h"] = _win_loss(entry.get("direction", "long"), entry.get("entry_price", 0), price)
            changed = True

        # 72h outcome
        if entry.get("outcome_72h") is None and age >= OUTCOME_CHECK_72H:
            entry["outcome_price_72h"] = price
            entry["outcome_72h"] = _win_loss(entry.get("direction", "long"), entry.get("entry_price", 0), price)
            changed = True

    if changed:
        _save_performance(perf)


def get_signal_stats_24h() -> dict:
    """Return accuracy stats for signals in the past 24h."""
    perf = _load_performance()
    now = time.time()
    cutoff = now - 86400
    recent = [s for s in perf["signals"] if s["timestamp"] >= cutoff]
    resolved = [s for s in recent if s["outcome"] is not None]
    wins = sum(1 for s in resolved if s["outcome"] == "win")
    total_resolved = len(resolved)
    accuracy = round(wins / total_resolved * 100, 1) if total_resolved > 0 else None

    hc = [s for s in resolved if s.get("confidence", 0) >= 80]
    hc_wins = sum(1 for s in hc if s["outcome"] == "win")
    hc_acc = round(hc_wins / len(hc) * 100, 1) if hc else None

    return {
        "period": "24h",
        "total_signals": len(recent),
        "resolved": total_resolved,
        "pending": len(recent) - total_resolved,
        "wins": wins,
        "losses": total_resolved - wins,
        "accuracy_pct": accuracy,
        "high_confidence_count": len(hc),
        "high_confidence_accuracy": hc_acc,
        "avg_confidence": (
            round(sum(s.get("confidence", 0) for s in recent) / len(recent), 1)
            if recent else None
        ),
    }


def format_signal_stats() -> str:
    stats = get_signal_stats_24h()
    lines = ["📊 **信号准确率统计 (过去24小时)**\n"]
    lines.append(f"  总信号数: {stats['total_signals']}")
    lines.append(f"  已结算: {stats['resolved']}  |  待结算: {stats['pending']}")
    if stats["accuracy_pct"] is not None:
        emoji = "🟢" if stats["accuracy_pct"] >= 60 else ("🟡" if stats["accuracy_pct"] >= 50 else "🔴")
        lines.append(
            f"  准确率: {emoji} {stats['accuracy_pct']}%"
            f"  ({stats['wins']}胜 / {stats['losses']}负)"
        )
    else:
        lines.append("  准确率: ⏳ 暂无已结算信号")
    if stats["avg_confidence"] is not None:
        lines.append(f"  平均置信度: {stats['avg_confidence']}")
    if stats["high_confidence_count"] > 0:
        hc_acc = stats["high_confidence_accuracy"]
        acc_str = f"{hc_acc}%" if hc_acc is not None else "N/A"
        lines.append(f"  高置信度(≥80)信号: {stats['high_confidence_count']}个, 准确率: {acc_str}")
    lines.append("\n  _结算时间: 信号发出4小时后验证_")
    return "\n".join(lines)


# ── Signal generation ─────────────────────────────────────────────────────────

def _score_signal_full(closes: list, cfg: dict) -> tuple:
    """Returns (direction, score, indicators)."""
    rsi = _rsi(closes, cfg["rsi_period"])
    ma_fast = _ma(closes, cfg["ma_fast"])
    ma_slow = _ma(closes, cfg["ma_slow"])
    macd_line = _macd(closes)
    price = closes[-1] if closes else None

    bull_votes = 0
    bear_votes = 0
    indis = {
        "rsi": rsi, "ma_fast": ma_fast,
        "ma_slow": ma_slow, "macd": macd_line, "price": price,
    }

    if rsi is not None:
        if rsi < cfg["rsi_oversold"]:
            bull_votes += 1
        elif rsi > cfg["rsi_overbought"]:
            bear_votes += 1
    if ma_fast is not None and ma_slow is not None:
        if ma_fast > ma_slow:
            bull_votes += 1
        else:
            bear_votes += 1
    if macd_line is not None:
        if macd_line > 0:
            bull_votes += 1
        else:
            bear_votes += 1

    if bull_votes > bear_votes:
        return "long", bull_votes, indis
    elif bear_votes > bull_votes:
        return "short", bear_votes, indis
    return "neutral", 0, indis


def _adaptive_threshold() -> int:
    """Dynamically adjust confidence threshold based on recent win rate.
    High win rate → can lower threshold to catch more. Low → raise to filter noise."""
    try:
        perf = _load_performance()
        recent = [s for s in perf.get("signals", [])
                  if s.get("outcome") is not None
                  and time.time() - s["timestamp"] < 7 * 86400]  # last 7 days
        if len(recent) < 10:
            return CONFIDENCE_THRESHOLD  # not enough data
        wins = sum(1 for s in recent if s["outcome"] == "win")
        wr = wins / len(recent) if recent else 0
        if wr >= 0.65:
            return max(60, CONFIDENCE_THRESHOLD - 5)   # performing well → slightly lower bar
        elif wr < 0.45:
            return min(85, CONFIDENCE_THRESHOLD + 10)   # performing badly → raise bar
        return CONFIDENCE_THRESHOLD
    except Exception:
        return CONFIDENCE_THRESHOLD


async def _mtf_confirms(symbol: str, direction: str, cfg: dict) -> tuple:
    """Multi-timeframe confirmation: check 4H and 1D agree with signal direction.
    Returns (4h_agrees: bool, 1d_agrees: bool, details: dict)."""
    results = await asyncio.gather(
        _fetch_okx_candles(symbol, "4H", limit=60),
        _fetch_okx_candles(symbol, "1D", limit=60),
        return_exceptions=True,
    )
    candles_4h = results[0] if not isinstance(results[0], Exception) else []
    candles_1d = results[1] if not isinstance(results[1], Exception) else []

    dir_4h = "neutral"
    dir_1d = "neutral"
    if len(candles_4h) >= cfg["ma_slow"] + 1:
        dir_4h = _quick_direction(_closes_okx(candles_4h), cfg)
    if len(candles_1d) >= cfg["ma_slow"] + 1:
        dir_1d = _quick_direction(_closes_okx(candles_1d), cfg)

    return (
        dir_4h == direction or dir_4h == "neutral",
        dir_1d == direction or dir_1d == "neutral",
        {"4h": dir_4h, "1d": dir_1d},
    )


async def scan_symbol(symbol: str, cfg: dict) -> Optional[dict]:
    """Scan one symbol across OKX + Binance + Bybit with 6-layer evolved filters."""
    tf = cfg["timeframe"]
    bin_sym = _to_binance(symbol)
    byb_sym = _to_bybit(symbol)
    bin_tf = _tf_to_binance(tf)
    byb_tf = _tf_to_bybit(tf)

    # ── FILTER 1: Cooldown dedup — same coin can't fire within 1h ────────
    now = time.time()
    last_fire = _signal_cooldowns.get(symbol, 0)
    if now - last_fire < COOLDOWN_SECONDS:
        return None

    # Fetch from all exchanges + orderbook in parallel
    results = await asyncio.gather(
        _fetch_okx_candles(symbol, tf),
        _fetch_binance_candles(bin_sym, bin_tf),
        _fetch_bybit_candles(byb_sym, byb_tf),
        _fetch_okx_orderbook(symbol),
        return_exceptions=True,
    )
    okx_candles = results[0] if not isinstance(results[0], Exception) else []
    bin_candles = results[1] if not isinstance(results[1], Exception) else []
    byb_candles = results[2] if not isinstance(results[2], Exception) else []
    okx_book = results[3] if not isinstance(results[3], Exception) else {}

    if len(okx_candles) < 30:
        return None

    # Primary analysis on OKX data
    okx_closes = _closes_okx(okx_candles)
    okx_vols = _volumes_okx(okx_candles)
    direction, score, indis = _score_signal_full(okx_closes, cfg)

    if direction == "neutral" or score < cfg["signal_score_threshold"]:
        return None

    # ── FILTER 2: ATR volatility — reject low-volatility noise ───────────
    atr_p = _atr_pct(okx_candles)
    min_atr = cfg.get("min_atr_pct", 0.3)  # at least 0.3% ATR
    if atr_p is not None and atr_p < min_atr:
        logger.debug("signal_engine: %s filtered by ATR %.4f%% < %.1f%%", symbol, atr_p, min_atr)
        return None

    # ── FILTER 3: ADX trend strength — only signal when trend is clear ───
    adx_val = _adx(okx_candles)
    min_adx = cfg.get("min_adx", 20)
    if adx_val is not None and adx_val < min_adx:
        logger.debug("signal_engine: %s filtered by ADX %.1f < %d", symbol, adx_val, min_adx)
        return None

    # ── FILTER 4: Volume spike — confirm momentum is real ────────────────
    has_vol_spike = _volume_spike(okx_vols, lookback=20, threshold=1.5)

    # Exchange direction comparison
    okx_dir = direction
    bin_dir = (
        _quick_direction(_closes_binance(bin_candles), cfg)
        if len(bin_candles) >= cfg["ma_slow"] + 1
        else "neutral"
    )
    byb_dir = (
        _quick_direction(_closes_bybit(byb_candles), cfg)
        if len(byb_candles) >= cfg["ma_slow"] + 1
        else "neutral"
    )

    # Volume-price divergence
    vol_div = _detect_vol_divergence(okx_closes, okx_vols)

    # Order book pressure
    ob_pressure = _orderbook_pressure(okx_book if isinstance(okx_book, dict) else {})

    # Confidence score (base)
    confidence = _compute_confidence(
        okx_dir, bin_dir, byb_dir,
        direction, indis,
        vol_div, ob_pressure,
        cfg,
    )

    # ── FILTER 5: Multi-timeframe confirmation — boost/penalize confidence
    mtf_4h_ok, mtf_1d_ok, mtf_details = await _mtf_confirms(symbol, direction, cfg)
    mtf_bonus = 0
    if mtf_4h_ok and mtf_1d_ok:
        mtf_bonus = 10   # all timeframes agree → strong
    elif not mtf_4h_ok and not mtf_1d_ok:
        mtf_bonus = -15  # higher TFs disagree → penalize hard
    elif not mtf_4h_ok or not mtf_1d_ok:
        mtf_bonus = -5   # partial disagreement

    # Volume spike bonus
    vol_spike_bonus = 5 if has_vol_spike else 0

    # ADX strength bonus
    adx_bonus = 0
    if adx_val is not None:
        if adx_val > 40:
            adx_bonus = 8   # very strong trend
        elif adx_val > 30:
            adx_bonus = 4   # decent trend

    confidence = min(100, max(0, confidence + mtf_bonus + vol_spike_bonus + adx_bonus))

    # ── FILTER 6: Adaptive threshold — dynamic based on recent win rate ──
    threshold = _adaptive_threshold()
    threshold = cfg.get("confidence_threshold", threshold)  # user override wins
    if confidence < threshold:
        return None

    # Signal passed all 6 filters — record cooldown
    _signal_cooldowns[symbol] = now

    return {
        "symbol": symbol,
        "direction": direction,
        "score": score,
        "confidence": confidence,
        "signal_type": f"{'bullish' if direction == 'long' else 'bearish'}_{tf.lower()}",
        "entry_price": indis["price"],
        "rsi": indis["rsi"],
        "ma_fast": indis["ma_fast"],
        "ma_slow": indis["ma_slow"],
        "macd": indis["macd"],
        "timeframe": tf,
        "timestamp": now,
        "exchange_dirs": {"okx": okx_dir, "binance": bin_dir, "bybit": byb_dir},
        "vol_divergence": vol_div,
        "orderbook_pressure": ob_pressure,
        # Evolution: new fields
        "atr_pct": atr_p,
        "adx": adx_val,
        "volume_spike": has_vol_spike,
        "mtf": mtf_details,
        "adaptive_threshold": threshold,
    }


async def scan_all(cfg: dict = None) -> list:
    """Scan all symbols and return list of high-confidence signals."""
    if cfg is None:
        cfg = load_config()
    tasks = [scan_symbol(sym, cfg) for sym in cfg["symbols"]]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    signals = []
    for r in results:
        if isinstance(r, dict):
            signals.append(r)
        elif isinstance(r, Exception):
            logger.debug("signal_engine: scan error: %s", r)
    return signals


def format_signal(sig: dict) -> str:
    direction_emoji = "🟢" if sig.get("direction") == "long" else "🔴"
    strength = "⚡" * sig.get("score", 0)
    conf = max(0, min(100, sig.get("confidence", 0)))
    conf_bar = "█" * (conf // 10) + "░" * (10 - conf // 10)

    ex_dirs = sig.get("exchange_dirs", {})
    ex_parts = [
        f"{ex.upper()[:3]}:{d[0].upper()}"
        for ex, d in ex_dirs.items()
        if d != "neutral"
    ]
    ex_str = "  ".join(ex_parts) if ex_parts else ""

    vol_div = sig.get("vol_divergence", "none")
    ob = sig.get("orderbook_pressure", "neutral")

    div_note = ""
    if vol_div == "bearish_div":
        div_note = "  ⚠️ 量价背离(看跌)"
    elif vol_div == "bullish_div":
        div_note = "  🔄 量价背离(潜在反转)"

    ob_note = ""
    if ob == "buy":
        ob_note = "  📗 大买单支撑"
    elif ob == "sell":
        ob_note = "  📕 大卖单压力"

    lines = [
        f"{direction_emoji} **{sig.get('symbol', '?')}** {sig.get('direction', '?').upper()}  {strength}",
        f"  置信度: {conf}/100  [{conf_bar}]",
        f"  类型: {sig.get('signal_type', '?')}  |  {sig.get('timeframe', '?')}",
        f"  价格: ${sig.get('entry_price', 0):.4f}",
        f"  RSI: {sig.get('rsi', 'N/A')}  |  MACD: {sig.get('macd', 'N/A')}",
        f"  MA快: {sig.get('ma_fast', 'N/A')}  MA慢: {sig.get('ma_slow', 'N/A')}",
    ]
    if ex_str:
        lines.append(f"  交易所: {ex_str}")
    if div_note:
        lines.append(div_note)
    if ob_note:
        lines.append(ob_note)

    # Evolution: new filter indicators
    adx_val = sig.get("adx")
    atr_p = sig.get("atr_pct")
    vol_spike = sig.get("volume_spike")
    mtf = sig.get("mtf", {})
    filters = []
    if adx_val is not None:
        trend_label = "强趋势" if adx_val > 30 else "趋势中"
        filters.append(f"ADX:{adx_val:.0f}({trend_label})")
    if atr_p is not None:
        filters.append(f"ATR:{atr_p:.2f}%")
    if vol_spike:
        filters.append("🔥量能突增")
    if mtf:
        mtf_str = f"4H:{mtf.get('4h','?')} 1D:{mtf.get('1d','?')}"
        filters.append(f"MTF[{mtf_str}]")
    if filters:
        lines.append(f"  进化过滤: {' | '.join(filters)}")

    return "\n".join(lines)


# ── Background scanner class ──────────────────────────────────────────────────

class SignalEngine:
    """Background task: scans market every N seconds, emits signals via callback."""

    def __init__(self, send_func=None, record_func=None):
        self._send = send_func
        self._record = record_func
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._last_signals: list = []

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="signal_engine")
        self._task.add_done_callback(self._on_done)
        logger.info("SignalEngine started (multi-exchange, confidence≥%d)", CONFIDENCE_THRESHOLD)

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
                logger.error("SignalEngine loop crashed: %s", e, exc_info=True)

    async def _loop(self) -> None:
        await asyncio.sleep(60)
        while self._running:
            try:
                cfg = load_config()
                try:
                    await _check_pending_outcomes()
                except Exception as e:
                    logger.debug("SignalEngine: outcome check: %s", e)

                signals = await scan_all(cfg)
                if signals:
                    self._last_signals = signals
                    for sig in signals:
                        try:
                            record_signal_performance(sig)
                        except Exception:
                            pass
                        if self._send:
                            await self._send(format_signal(sig))
                        if self._record:
                            try:
                                self._record(
                                    sig.get("symbol", "UNKNOWN"),
                                    sig.get("direction", "neutral"),
                                    sig.get("signal_type", "unknown"),
                                    sig.get("entry_price", 0),
                                )
                            except Exception as e:
                                logger.warning("SignalEngine: record failed: %s", e)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("SignalEngine scan error: %s", e)

            try:
                cfg = load_config()
                await asyncio.sleep(cfg.get("scan_interval", 900))
            except asyncio.CancelledError:
                break

    def get_last_signals(self) -> list:
        return list(self._last_signals)

    @property
    def running(self) -> bool:
        return self._running


# Module-level singleton
signal_engine = SignalEngine()


# ── Arbitrage signal source ───────────────────────────────────────────────────

def get_arb_signals(top_n: int = 5) -> list:
    try:
        from arbitrage_engine import arb_engine
        return arb_engine.get_top_spreads(top_n)
    except Exception as e:
        logger.debug("get_arb_signals: %s", e)
        return []


def get_combined_signals(technical_signals: list = None, top_arb: int = 5) -> list:
    """
    Merge technical signals (confidence-scored) with arb signals (spread%-scored).
    Returns unified list sorted by score descending.
    Each entry has a 'unified_score' and 'signal_category' field added.
    """
    combined = []

    # Technical signals: score = confidence (0-100)
    for sig in (technical_signals or signal_engine.get_last_signals()):
        entry = dict(sig)
        entry["unified_score"] = sig.get("confidence", 0)
        entry["signal_category"] = "technical"
        combined.append(entry)

    # Arb signals: convert spread_pct to a 0-100 score proxy (0.5%→50, 1%→100)
    for sig in get_arb_signals(top_arb):
        entry = dict(sig)
        entry["unified_score"] = min(100, int(sig.get("spread_pct", 0) * 100))
        entry["signal_category"] = "arbitrage"
        combined.append(entry)

    combined.sort(key=lambda x: x["unified_score"], reverse=True)
    return combined
