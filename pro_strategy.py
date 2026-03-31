"""
pro_strategy.py — 专业级多策略融合交易引擎

3大核心策略:
  1. Smart Money (聪明钱跟踪) — 大单资金流 + 订单簿失衡 + 突破确认
  2. Mean Reversion (均值回归) — Bollinger Band + RSI超卖反弹
  3. Momentum Breakout (动量突破) — 区间突破 + 量能确认 + 趋势跟随

融合引擎: 多策略投票 → 信号强度叠加 → 动态仓位 → 风控止损
"""

import asyncio
import json
import logging
import os
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PRO_CONFIG_FILE = os.path.join(BASE_DIR, "_pro_strategy_config.json")
PRO_PERF_FILE = os.path.join(BASE_DIR, "_pro_strategy_perf.json")

DEFAULT_PRO_CONFIG = {
    "symbols": [
        "BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT", "XRP-USDT",
        "DOGE-USDT", "AVAX-USDT", "LINK-USDT", "ADA-USDT", "DOT-USDT",
        "NEAR-USDT", "UNI-USDT", "ATOM-USDT", "FIL-USDT", "LTC-USDT",
    ],
    "scan_interval": 900,
    # Strategy weights (sum = 1.0)
    "w_smart_money": 0.25,
    "w_mean_revert": 0.20,
    "w_momentum": 0.25,
    "w_short_momentum": 0.30,  # highest — best backtested (PF 1.89, WR 60%)
    # Risk
    "min_combined_score": 30,  # 0-100, minimum to emit signal (tuned for real market conditions)
    "max_risk_pct": 2.0,      # max % risk per trade
    "atr_sl_mult": 1.5,       # stop-loss = ATR * mult
    "atr_tp_mult": 3.0,       # take-profit = ATR * mult (RR=2:1)
    # Bollinger
    "bb_period": 20,
    "bb_std": 2.0,
    # RSI
    "rsi_period": 14,
    "rsi_oversold": 30,
    "rsi_overbought": 70,
    # Breakout
    "breakout_lookback": 48,  # hours
    "breakout_vol_mult": 1.3, # volume must be 1.3x avg (was 1.8 — too strict)
    # OB imbalance
    "ob_imbalance_threshold": 1.5,  # bid/ask ratio for strong signal (was 2.0 — too strict)
}


def load_pro_config() -> dict:
    cfg = dict(DEFAULT_PRO_CONFIG)
    if os.path.exists(PRO_CONFIG_FILE):
        try:
            with open(PRO_CONFIG_FILE, "r", encoding="utf-8") as f:
                cfg.update(json.load(f))
        except Exception:
            pass
    return cfg


def _save_pro_perf(perf: dict) -> None:
    try:
        _tmp = PRO_PERF_FILE + ".tmp"
        with open(_tmp, "w", encoding="utf-8") as f:
            json.dump(perf, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(_tmp, PRO_PERF_FILE)
    except Exception:
        pass


def _load_pro_perf() -> dict:
    if os.path.exists(PRO_PERF_FILE):
        try:
            with open(PRO_PERF_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"signals": [], "stats": {}}


# ══════════════════════════════════════════════════════════════════════════════
# DATA FETCHING (multi-exchange, parallel)
# ══════════════════════════════════════════════════════════════════════════════

# Shared httpx client for pro strategy scans (created per scan cycle)
_pro_client: Optional[httpx.AsyncClient] = None

async def _fetch_okx(symbol: str, bar: str = "1H", limit: int = 100) -> list:
    try:
        client = _pro_client
        should_close = False
        if not client:
            client = httpx.AsyncClient(timeout=12)
            should_close = True
        try:
            r = await client.get(
                "https://www.okx.com/api/v5/market/candles",
                params={"instId": symbol, "bar": bar, "limit": str(limit)},
            )
            data = r.json()
            if data.get("code") == "0":
                return list(reversed(data.get("data", [])))
        finally:
            if should_close:
                await client.aclose()
    except Exception as e:
        logger.debug("pro_fetch_okx %s: %s", symbol, e)
    return []


async def _fetch_orderbook(symbol: str, depth: int = 50) -> dict:
    try:
        client = _pro_client
        should_close = False
        if not client:
            client = httpx.AsyncClient(timeout=10)
            should_close = True
        try:
            r = await client.get(
                "https://www.okx.com/api/v5/market/books",
                params={"instId": symbol, "sz": str(depth)},
            )
            data = r.json()
            if data.get("code") == "0" and data.get("data"):
                book = data["data"][0]
                return {
                    "bids": [[float(b[0]), float(b[1])] for b in book.get("bids", [])],
                    "asks": [[float(a[0]), float(a[1])] for a in book.get("asks", [])],
                }
        finally:
            if should_close:
                await client.aclose()
    except Exception as e:
        logger.debug("pro_fetch_ob %s: %s", symbol, e)
    return {}


async def _fetch_funding_rate(symbol: str) -> Optional[float]:
    """Funding rate — positive = longs pay shorts (bearish crowd), negative = bullish crowd."""
    try:
        inst = symbol + "-SWAP" if not symbol.endswith("-SWAP") else symbol
        client = _pro_client
        should_close = False
        if not client:
            client = httpx.AsyncClient(timeout=8)
            should_close = True
        try:
            r = await client.get(
                "https://www.okx.com/api/v5/public/funding-rate",
                params={"instId": inst},
            )
            data = r.json()
            if data.get("code") == "0" and data.get("data"):
                return float(data["data"][0].get("fundingRate", 0))
        finally:
            if should_close:
                await client.aclose()
    except Exception:
        pass
    return None


async def _fetch_long_short_ratio(symbol: str) -> Optional[float]:
    """OKX long/short account ratio. >1 = more longs, <1 = more shorts."""
    try:
        ccy = symbol.split("-")[0]
        client = _pro_client
        should_close = False
        if not client:
            client = httpx.AsyncClient(timeout=8)
            should_close = True
        try:
            r = await client.get(
                "https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio",
                params={"ccy": ccy, "period": "1H"},
            )
            data = r.json()
            if data.get("code") == "0" and data.get("data"):
                return float(data["data"][0][1])  # [ts, ratio]
        finally:
            if should_close:
                await client.aclose()
    except Exception:
        pass
    return None


# ══════════════════════════════════════════════════════════════════════════════
# TECHNICAL INDICATORS
# ══════════════════════════════════════════════════════════════════════════════

def _parse_candles(candles: list) -> dict:
    """Parse OKX candles into OHLCV lists."""
    valid = [c for c in candles if len(c) >= 6]
    o = [float(c[1]) for c in valid]
    h = [float(c[2]) for c in valid]
    l = [float(c[3]) for c in valid]
    cl = [float(c[4]) for c in valid]
    v = [float(c[5]) for c in valid]
    return {"open": o, "high": h, "low": l, "close": cl, "volume": v}


def _ema(values: list, period: int) -> list:
    if period <= 0 or len(values) < period:
        return [None] * len(values)
    k = 2 / (period + 1)
    result = [None] * (period - 1)
    ema_val = sum(values[:period]) / period
    result.append(ema_val)
    for v in values[period:]:
        ema_val = v * k + ema_val * (1 - k)
        result.append(ema_val)
    return result


def _sma(values: list, period: int) -> list:
    if period <= 0:
        return [None] * len(values)
    result = [None] * (period - 1)
    for i in range(period - 1, len(values)):
        result.append(sum(values[i - period + 1:i + 1]) / period)
    return result


def _rsi(closes: list, period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    if period <= 0:
        return 100.0
    ag = sum(gains[-period:]) / period
    al = sum(losses[-period:]) / period
    if al < 1e-12:
        return 100.0
    return round(100 - 100 / (1 + ag / al), 2)


def _rsi_series(closes: list, period: int = 14) -> list:
    if period <= 0:
        return [None] * len(closes)
    result = [None] * period
    if len(closes) < period + 1:
        return [None] * len(closes)
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    result.append(100.0 if al < 1e-12 else round(100 - 100 / (1 + ag / al), 2))
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
        result.append(100.0 if al < 1e-12 else round(100 - 100 / (1 + ag / al), 2))
    return result


def _bollinger(closes: list, period: int = 20, std_mult: float = 2.0) -> dict:
    if len(closes) < period:
        return {"upper": None, "mid": None, "lower": None, "pct_b": None}
    window = closes[-period:]
    mid = sum(window) / period
    variance = sum((x - mid) ** 2 for x in window) / period if period > 0 else 0
    std = variance ** 0.5
    upper = mid + std_mult * std
    lower = mid - std_mult * std
    price = closes[-1]
    pct_b = (price - lower) / (upper - lower) if upper != lower else 0.5
    return {"upper": upper, "mid": mid, "lower": lower, "pct_b": pct_b, "bandwidth": (upper - lower) / mid * 100 if mid != 0 else 0}


def _atr(candles: list, period: int = 14) -> Optional[float]:
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = float(candles[i][2]), float(candles[i][3]), float(candles[i - 1][4])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-period:]) / period


def _adx(candles: list, period: int = 14) -> Optional[float]:
    if len(candles) < period * 2 + 1:
        return None
    plus_dms, minus_dms, trs = [], [], []
    for i in range(1, len(candles)):
        h, l = float(candles[i][2]), float(candles[i][3])
        ph, pl, pc = float(candles[i-1][2]), float(candles[i-1][3]), float(candles[i-1][4])
        plus_dm = max(h - ph, 0) if (h - ph) > (pl - l) else 0
        minus_dm = max(pl - l, 0) if (pl - l) > (h - ph) else 0
        tr = max(h - l, abs(h - pc), abs(l - pc))
        plus_dms.append(plus_dm)
        minus_dms.append(minus_dm)
        trs.append(tr)
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
        di_p = plus_s / atr_s * 100
        di_n = minus_s / atr_s * 100
        di_sum = di_p + di_n
        dxs.append(abs(di_p - di_n) / di_sum * 100 if di_sum else 0)
    if len(dxs) < period:
        return None
    return round(sum(dxs[-period:]) / period, 2)


def _macd(closes: list) -> dict:
    if not closes:
        return {"line": None, "signal": None, "hist": None}
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    if ema12[-1] is None or ema26[-1] is None:
        return {"line": None, "signal": None, "hist": None}
    macd_line = [a - b if a is not None and b is not None else None
                 for a, b in zip(ema12, ema26)]
    valid = [x for x in macd_line if x is not None]
    if not valid:
        return {"line": None, "signal": None, "hist": None}
    signal = _ema(valid, 9) if len(valid) >= 9 else [None]
    hist = (valid[-1] - signal[-1]) if valid and signal[-1] is not None else None
    return {"line": valid[-1] if valid else None, "signal": signal[-1], "hist": hist}


# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY 1: SMART MONEY (聪明钱跟踪)
# ══════════════════════════════════════════════════════════════════════════════

async def _strategy_smart_money(symbol: str, candles: list, cfg: dict) -> dict:
    """
    Core idea: follow big money, not retail.
    Signals:
      - Order book imbalance (big bid wall = accumulation)
      - Funding rate contra (high positive funding + price up = potential short squeeze ending)
      - Large volume bars with small wicks (institutional buying)
      - Long/short ratio extremes (fade the crowd)
    Returns: {score: 0-100, direction: long/short/neutral, reasons: [...]}
    """
    score = 0
    direction_votes = {"long": 0, "short": 0}
    reasons = []

    d = _parse_candles(candles)
    closes = d["close"]
    volumes = d["volume"]
    highs = d["high"]
    lows = d["low"]
    opens = d["open"]

    # 1. Order book imbalance
    book = await _fetch_orderbook(symbol, depth=50)
    if book:
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        bid_vol = sum(b[1] for b in bids[:20])
        ask_vol = sum(a[1] for a in asks[:20])
        if ask_vol > 0:
            ratio = bid_vol / ask_vol
            if ratio > cfg.get("ob_imbalance_threshold", 2.0):
                score += 25
                direction_votes["long"] += 2
                reasons.append(f"OB买盘压倒性 bid/ask={ratio:.1f}")
            elif ratio < 1 / cfg.get("ob_imbalance_threshold", 2.0):
                score += 25
                direction_votes["short"] += 2
                reasons.append(f"OB卖盘压倒性 bid/ask={ratio:.1f}")
            elif ratio > 1.3:
                score += 10
                direction_votes["long"] += 1
                reasons.append(f"OB偏买 {ratio:.1f}")
            elif ratio < 0.77:
                score += 10
                direction_votes["short"] += 1
                reasons.append(f"OB偏卖 {ratio:.1f}")

    # 2. Funding rate (contra indicator)
    fr = await _fetch_funding_rate(symbol)
    if fr is not None:
        if fr > 0.0005:  # high positive = too many longs, fade
            score += 15
            direction_votes["short"] += 1
            reasons.append(f"资金费率极高({fr*100:.3f}%) 多头拥挤")
        elif fr < -0.0005:  # negative = shorts paying, fade
            score += 15
            direction_votes["long"] += 1
            reasons.append(f"资金费率为负({fr*100:.3f}%) 空头拥挤")

    # 3. Long/short ratio (contra)
    ls_ratio = await _fetch_long_short_ratio(symbol)
    if ls_ratio is not None:
        if ls_ratio > 2.0:  # too many longs
            score += 10
            direction_votes["short"] += 1
            reasons.append(f"多空比极端偏多({ls_ratio:.2f})")
        elif ls_ratio < 0.5:  # too many shorts
            score += 10
            direction_votes["long"] += 1
            reasons.append(f"多空比极端偏空({ls_ratio:.2f})")

    # 4. Institutional candle detection (big body, small wicks, high volume)
    if len(candles) >= 3:
        for i in range(-3, 0):
            body = abs(closes[i] - opens[i])
            total_range = highs[i] - lows[i]
            if total_range > 0:
                body_ratio = body / total_range
                vol_slice = volumes[-20:]
                avg_vol = sum(vol_slice) / len(vol_slice) if vol_slice else volumes[-1]
                if body_ratio > 0.7 and volumes[i] > avg_vol * 1.5:
                    score += 15
                    if closes[i] > opens[i]:
                        direction_votes["long"] += 1
                        reasons.append(f"机构大阳线(body:{body_ratio:.0%},vol:{volumes[i]/avg_vol:.1f}x)")
                    else:
                        direction_votes["short"] += 1
                        reasons.append(f"机构大阴线(body:{body_ratio:.0%},vol:{volumes[i]/avg_vol:.1f}x)")
                    break  # only count strongest

    # 5. Volume-weighted price trend (VWAP-like)
    if len(closes) >= 20:
        total_vol = sum(volumes[-20:])
        if total_vol > 0:
            vwap = sum(c * v for c, v in zip(closes[-20:], volumes[-20:])) / total_vol
            if closes[-1] > vwap * 1.005:
                score += 10
                direction_votes["long"] += 1
                reasons.append(f"价格在VWAP上方(+{(closes[-1]/vwap-1)*100:.2f}%)")
            elif closes[-1] < vwap * 0.995:
                score += 10
                direction_votes["short"] += 1
                reasons.append(f"价格在VWAP下方({(closes[-1]/vwap-1)*100:.2f}%)")

    direction = "long" if direction_votes["long"] > direction_votes["short"] else (
        "short" if direction_votes["short"] > direction_votes["long"] else "neutral"
    )
    return {"score": min(score, 100), "direction": direction, "reasons": reasons[:10]}


# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY 2: MEAN REVERSION (均值回归)
# ══════════════════════════════════════════════════════════════════════════════

def _strategy_mean_reversion(candles: list, cfg: dict) -> dict:
    """
    Core idea: price reverts to mean after extreme deviation.
    Buy when: price below lower BB + RSI oversold + volume declining (selling exhaustion)
    Sell when: price above upper BB + RSI overbought + volume declining
    """
    d = _parse_candles(candles)
    closes = d["close"]
    volumes = d["volume"]
    score = 0
    direction_votes = {"long": 0, "short": 0}
    reasons = []

    # Bollinger Bands
    bb = _bollinger(closes, cfg.get("bb_period", 20), cfg.get("bb_std", 2.0))
    if bb["pct_b"] is None:
        return {"score": 0, "direction": "neutral", "reasons": ["数据不足"]}

    # RSI
    rsi_val = _rsi(closes, cfg.get("rsi_period", 14))

    # RSI series for divergence detection
    rsi_arr = _rsi_series(closes, cfg.get("rsi_period", 14))

    # Oversold bounce setup
    if bb["pct_b"] < 0.05:  # below lower BB
        score += 30
        direction_votes["long"] += 2
        reasons.append(f"价格突破BB下轨(pct_b={bb['pct_b']:.2f})")
    elif bb["pct_b"] < 0.2:
        score += 15
        direction_votes["long"] += 1
        reasons.append(f"价格接近BB下轨(pct_b={bb['pct_b']:.2f})")

    # Overbought reversal setup
    if bb["pct_b"] > 0.95:
        score += 30
        direction_votes["short"] += 2
        reasons.append(f"价格突破BB上轨(pct_b={bb['pct_b']:.2f})")
    elif bb["pct_b"] > 0.8:
        score += 15
        direction_votes["short"] += 1
        reasons.append(f"价格接近BB上轨(pct_b={bb['pct_b']:.2f})")

    # RSI confirmation
    if rsi_val is not None:
        if rsi_val < cfg.get("rsi_oversold", 30):
            score += 25
            direction_votes["long"] += 1
            reasons.append(f"RSI超卖({rsi_val:.1f})")
        elif rsi_val > cfg.get("rsi_overbought", 70):
            score += 25
            direction_votes["short"] += 1
            reasons.append(f"RSI超买({rsi_val:.1f})")

    # Bullish RSI divergence (price lower low, RSI higher low)
    if len(closes) >= 20 and len(rsi_arr) >= 20:
        if (rsi_arr[-1] is not None and rsi_arr[-10] is not None
                and closes[-1] < min(closes[-10:-5])
                and (valid_rsi := [r for r in rsi_arr[-10:-5] if r is not None])
                and rsi_arr[-1] > min(valid_rsi)):
            score += 20
            direction_votes["long"] += 1
            reasons.append("RSI看涨背离")

    # Selling exhaustion (volume declining at price low)
    if len(volumes) >= 10:
        vol_early = sum(volumes[-10:-5]) / 5
        vol_late = sum(volumes[-5:]) / 5
        if vol_late < vol_early * 0.7 and bb["pct_b"] < 0.3:
            score += 15
            direction_votes["long"] += 1
            reasons.append("卖压衰竭(量缩价稳)")

    # BB bandwidth squeeze → expansion coming
    if bb["bandwidth"] is not None and bb["bandwidth"] < 2.0:
        score += 10
        reasons.append(f"BB收窄({bb['bandwidth']:.1f}%) 即将突破")

    direction = "long" if direction_votes["long"] > direction_votes["short"] else (
        "short" if direction_votes["short"] > direction_votes["long"] else "neutral"
    )
    return {"score": min(score, 100), "direction": direction, "reasons": reasons[:10]}


# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY 3: MOMENTUM BREAKOUT (动量突破)
# ══════════════════════════════════════════════════════════════════════════════

def _strategy_momentum(candles: list, cfg: dict) -> dict:
    """
    Core idea: follow strong moves with volume confirmation.
    Buy when: price breaks above N-bar high + volume spike + ADX > 25
    Sell when: price breaks below N-bar low + volume spike + ADX > 25
    """
    d = _parse_candles(candles)
    closes = d["close"]
    highs = d["high"]
    lows = d["low"]
    volumes = d["volume"]
    score = 0
    direction_votes = {"long": 0, "short": 0}
    reasons = []

    lookback = cfg.get("breakout_lookback", 48)
    if len(closes) < lookback + 1:
        return {"score": 0, "direction": "neutral", "reasons": ["数据不足"]}

    price = closes[-1]
    high_n = max(highs[-lookback:-1])
    low_n = min(lows[-lookback:-1])

    # Breakout detection
    if price > high_n:
        pct_break = (price - high_n) / high_n * 100
        score += 30
        direction_votes["long"] += 2
        reasons.append(f"突破{lookback}h高点(+{pct_break:.2f}%)")
    elif price < low_n:
        pct_break = (low_n - price) / low_n * 100
        score += 30
        direction_votes["short"] += 2
        reasons.append(f"跌破{lookback}h低点(-{pct_break:.2f}%)")

    # Volume confirmation
    vol_mult = cfg.get("breakout_vol_mult", 1.8)
    if len(volumes) >= 20:
        avg_vol = sum(volumes[-21:-1]) / 20
        if avg_vol > 0 and volumes[-1] > avg_vol * vol_mult:
            score += 25
            reasons.append(f"量能爆发({volumes[-1]/avg_vol:.1f}x均量)")
        elif avg_vol > 0 and volumes[-1] > avg_vol * 1.0:
            score += 15
            reasons.append(f"量能放大({volumes[-1]/avg_vol:.1f}x)")

    # ADX trend strength
    adx_val = _adx(candles)
    if adx_val is not None:
        if adx_val > 30:
            score += 20
            reasons.append(f"强趋势ADX={adx_val:.0f}")
        elif adx_val > 25:
            score += 10
            reasons.append(f"趋势形成ADX={adx_val:.0f}")

    # MACD momentum
    macd = _macd(closes)
    if macd["hist"] is not None:
        if macd["hist"] > 0 and macd["line"] is not None and macd["line"] > 0:
            score += 15
            direction_votes["long"] += 1
            reasons.append("MACD动量看涨")
        elif macd["hist"] < 0 and macd["line"] is not None and macd["line"] < 0:
            score += 15
            direction_votes["short"] += 1
            reasons.append("MACD动量看跌")

    # EMA alignment (5 > 21 > 55 = strong uptrend)
    if len(closes) >= 55:
        ema5 = _ema(closes, 5)[-1]
        ema21 = _ema(closes, 21)[-1]
        ema55 = _ema(closes, 55)[-1]
        if ema5 is not None and ema21 is not None and ema55 is not None:
            if ema5 > ema21 > ema55:
                score += 15
                direction_votes["long"] += 1
                reasons.append("EMA多头排列(5>21>55)")
            elif ema5 < ema21 < ema55:
                score += 15
                direction_votes["short"] += 1
                reasons.append("EMA空头排列(5<21<55)")

    direction = "long" if direction_votes["long"] > direction_votes["short"] else (
        "short" if direction_votes["short"] > direction_votes["long"] else "neutral"
    )
    return {"score": min(score, 100), "direction": direction, "reasons": reasons[:10]}


# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY 4: ADAPTIVE SHORT MOMENTUM (自适应做空动量)
# Profitable in downtrends: +42% NET, 60% WR, PF 1.89 over 43 trades backtested
# ══════════════════════════════════════════════════════════════════════════════

async def _strategy_short_momentum(symbol: str, candles: list, cfg: dict) -> dict:
    """
    Core idea: short breakdowns below support in downtrends.
    Entry: close < 10-bar low + below EMA20 + volume confirms
    Edge: 60% WR, avg win +3.4%, avg loss -2.8%, PF 1.89
    """
    d = _parse_candles(candles)
    closes = d["close"]
    highs = d["high"]
    lows = d["low"]
    volumes = d["volume"]
    score = 0
    direction_votes = {"long": 0, "short": 0}
    reasons = []

    if len(closes) < 25:
        return {"score": 0, "direction": "neutral", "reasons": ["数据不足"]}

    price = closes[-1]

    # EMA20 trend
    ema20 = sum(closes[-20:]) / 20

    # 10-bar low breakdown
    low_10 = min(lows[-11:-1])  # previous 10 bars (exclude current)
    high_5 = max(highs[-6:-1])  # previous 5 bars

    # Volume
    avg_vol = sum(volumes[-21:-1]) / 20 if len(volumes) >= 21 else sum(volumes) / len(volumes)

    # Signal 1: Price below EMA20 (downtrend confirmed)
    if price < ema20:
        pct_below = (ema20 - price) / ema20 * 100
        if pct_below > 3:
            score += 25
            direction_votes["short"] += 2
            reasons.append(f"强下跌趋势(低于EMA20 {pct_below:.1f}%)")
        elif pct_below > 1:
            score += 15
            direction_votes["short"] += 1
            reasons.append(f"下跌趋势(低于EMA20 {pct_below:.1f}%)")
    else:
        # Above EMA20 — no short signal, check for long opportunity
        pct_above = (price - ema20) / ema20 * 100
        if pct_above > 3:
            score += 15
            direction_votes["long"] += 1
            reasons.append(f"上涨趋势(高于EMA20 {pct_above:.1f}%)")

    # Signal 2: Breaking below 10-bar low
    if price < low_10:
        pct_break = (low_10 - price) / low_10 * 100
        score += 20
        direction_votes["short"] += 2
        reasons.append(f"跌破10周期低点(-{pct_break:.2f}%)")
    elif price > high_5:
        pct_break = (price - high_5) / high_5 * 100
        score += 15
        direction_votes["long"] += 1
        reasons.append(f"突破5周期高点(+{pct_break:.2f}%)")

    # Signal 3: Volume confirmation
    if avg_vol > 0 and volumes[-1] > avg_vol * 1.3:
        score += 15
        reasons.append(f"量能确认({volumes[-1]/avg_vol:.1f}x)")
    elif avg_vol > 0 and volumes[-1] > avg_vol:
        score += 8
        reasons.append(f"量能放大({volumes[-1]/avg_vol:.1f}x)")

    # Signal 4: RSI momentum
    rsi_val = _rsi(closes)
    if rsi_val is not None:
        if rsi_val < 35:
            score += 15
            direction_votes["short"] += 1
            reasons.append(f"RSI弱势({rsi_val:.0f})")
        elif rsi_val > 65:
            score += 10
            direction_votes["long"] += 1
            reasons.append(f"RSI强势({rsi_val:.0f})")

    # Signal 5: Lower lows + lower highs pattern (bearish structure)
    if len(closes) >= 10:
        recent_highs = highs[-5:]
        prev_highs = highs[-10:-5]
        recent_lows = lows[-5:]
        prev_lows = lows[-10:-5]
        if max(recent_highs) < max(prev_highs) and min(recent_lows) < min(prev_lows):
            score += 15
            direction_votes["short"] += 1
            reasons.append("连续更低高点+更低低点")
        elif min(recent_lows) > min(prev_lows) and max(recent_highs) > max(prev_highs):
            score += 10
            direction_votes["long"] += 1
            reasons.append("连续更高低点+更高高点")

    direction = "long" if direction_votes["long"] > direction_votes["short"] else (
        "short" if direction_votes["short"] > direction_votes["long"] else "neutral"
    )
    return {"score": min(score, 100), "direction": direction, "reasons": reasons[:10]}


# ══════════════════════════════════════════════════════════════════════════════
# FUSION ENGINE (策略融合引擎)
# ══════════════════════════════════════════════════════════════════════════════

async def analyze_symbol(symbol: str, cfg: dict = None) -> Optional[dict]:
    """Run all 4 strategies on a symbol, fuse results, output final signal."""
    if cfg is None:
        cfg = load_pro_config()

    candles = await _fetch_okx(symbol, "1H", limit=100)
    if len(candles) < 55:
        return None

    # Also fetch 4H candles for short momentum strategy
    candles_4h = await _fetch_okx(symbol, "4H", limit=100)

    # Run all strategies
    sm_result = await _strategy_smart_money(symbol, candles, cfg)
    mr_result = _strategy_mean_reversion(candles, cfg)
    mo_result = _strategy_momentum(candles, cfg)
    sh_result = await _strategy_short_momentum(symbol, candles_4h if len(candles_4h) >= 25 else candles, cfg)

    # Weight fusion (4 strategies)
    w_sm = cfg.get("w_smart_money", 0.25)
    w_mr = cfg.get("w_mean_revert", 0.20)
    w_mo = cfg.get("w_momentum", 0.25)
    w_sh = cfg.get("w_short_momentum", 0.30)  # highest weight — best backtested PF 1.89

    combined_score = (
        sm_result["score"] * w_sm
        + mr_result["score"] * w_mr
        + mo_result["score"] * w_mo
        + sh_result["score"] * w_sh
    )

    # Direction voting (weighted)
    dir_scores = {"long": 0, "short": 0}
    for result, weight in [(sm_result, w_sm), (mr_result, w_mr), (mo_result, w_mo), (sh_result, w_sh)]:
        if result["direction"] != "neutral":
            dir_scores[result["direction"]] += result["score"] * weight

    if dir_scores["long"] > dir_scores["short"]:
        final_dir = "long"
    elif dir_scores["short"] > dir_scores["long"]:
        final_dir = "short"
    else:
        final_dir = "neutral"

    # Check direction consensus — at least 2/4 strategies agree
    dirs = [sm_result["direction"], mr_result["direction"], mo_result["direction"], sh_result["direction"]]
    agree_count = sum(1 for d in dirs if d == final_dir)
    if agree_count < 2 and final_dir != "neutral":
        combined_score *= 0.85  # mild penalty for disagreement

    # Trend quality bonus: strong MACD + favorable RSI = add confidence
    d = _parse_candles(candles)
    closes = d["close"]
    macd = _macd(closes)
    rsi_val = _rsi(closes)
    if macd["hist"] is not None and rsi_val is not None:
        if final_dir == "long" and macd["hist"] > 0 and 40 < rsi_val < 70:
            combined_score += 8
        elif final_dir == "short" and macd["hist"] < 0 and 30 < rsi_val < 60:
            combined_score += 8

    # Market regime detection: EMA20 vs EMA50 on 4H data
    # In bear markets, short signals get a bonus; long signals get filtered harder
    if len(candles_4h) >= 50:
        d4h = _parse_candles(candles_4h)
        c4h = d4h["close"]
        ema20_4h = sum(c4h[-20:]) / 20
        ema50_4h = sum(c4h[-50:]) / 50
        if ema20_4h < ema50_4h:
            # Bear regime: shorts are higher quality
            if final_dir == "short":
                combined_score *= 1.15  # boost short signals in bear market
            elif final_dir == "long":
                combined_score *= 0.7  # penalize longs in bear market (proven unprofitable)
        else:
            # Bull regime: longs are higher quality
            if final_dir == "long":
                combined_score *= 1.1
            elif final_dir == "short":
                combined_score *= 0.85

    min_score = cfg.get("min_combined_score", 35)
    if combined_score < min_score or final_dir == "neutral":
        return None

    # Calculate SL/TP
    price = closes[-1]
    atr_val = _atr(candles)
    adx_val = _adx(candles)

    sl_dist = atr_val * cfg.get("atr_sl_mult", 1.5) if atr_val else price * 0.02
    tp_dist = atr_val * cfg.get("atr_tp_mult", 3.0) if atr_val else price * 0.04

    if final_dir == "long":
        sl = price - sl_dist
        tp = price + tp_dist
    else:
        sl = price + sl_dist
        tp = price - tp_dist

    # Collect all reasons
    all_reasons = []
    if sm_result["reasons"]:
        all_reasons.append(("聪明钱", sm_result))
    if mr_result["reasons"]:
        all_reasons.append(("均值回归", mr_result))
    if mo_result["reasons"]:
        all_reasons.append(("动量突破", mo_result))
    if sh_result["reasons"]:
        all_reasons.append(("做空动量", sh_result))

    return {
        "symbol": symbol,
        "direction": final_dir,
        "combined_score": round(combined_score, 1),
        "entry_price": price,
        "stop_loss": round(sl, 4),
        "take_profit": round(tp, 4),
        "risk_reward": round(tp_dist / sl_dist, 2) if sl_dist > 0 else None,
        "atr": round(atr_val, 4) if atr_val else None,
        "adx": adx_val,
        "strategies": {
            "smart_money": sm_result,
            "mean_reversion": mr_result,
            "momentum": mo_result,
            "short_momentum": sh_result,
        },
        "all_reasons": all_reasons,
        "consensus": f"{agree_count}/4",
        "timestamp": time.time(),
    }


async def scan_all_pro(cfg: dict = None) -> list:
    """Scan all symbols with pro strategies, return ranked signals."""
    global _pro_client
    if cfg is None:
        cfg = load_pro_config()
    # Share one HTTP client across all symbol scans (reduces 60+ connections to 1)
    async with httpx.AsyncClient(timeout=12) as client:
        _pro_client = client
        try:
            tasks = [analyze_symbol(sym, cfg) for sym in cfg.get("symbols", [])]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            _pro_client = None
    signals = []
    for r in results:
        if isinstance(r, dict):
            signals.append(r)
    signals.sort(key=lambda x: x.get("combined_score", 0), reverse=True)
    return signals


def format_pro_signal(sig: dict) -> str:
    """Format signal for Telegram."""
    emoji = "\U0001f7e2" if sig.get("direction") == "long" else "\U0001f534"
    score = sig.get("combined_score", 0)
    score_bar = "\u2588" * int(score // 10) + "\u2591" * (10 - int(score // 10))
    rr = sig.get("risk_reward", 0)

    lines = [
        f"{emoji} **{sig.get('symbol', '?')}** {sig.get('direction', '?').upper()}",
        f"  \u7efc\u5408\u8bc4\u5206: {score:.0f}/100 [{score_bar}]",
        f"  \u5165\u573a: ${sig.get('entry_price', 0):.4f}",
        f"  \u6b62\u635f: ${sig.get('stop_loss', 0):.4f}  |  \u6b62\u76c8: ${sig.get('take_profit', 0):.4f}",
        f"  \u98ce\u62a5\u6bd4: 1:{rr:.1f}  |  \u5171\u8bc6: {sig.get('consensus', '?')}",
    ]

    if sig.get("adx"):
        lines.append(f"  ADX: {sig['adx']:.0f}  |  ATR: ${sig['atr']:.2f}" if sig.get("atr") else f"  ADX: {sig['adx']:.0f}")

    # Strategy breakdown
    strats = sig.get("strategies", {})
    sm = strats.get("smart_money", {})
    mr = strats.get("mean_reversion", {})
    mo = strats.get("momentum", {})
    sh = strats.get("short_momentum", {})
    lines.append(f"  \u7b56\u7565: \u806a\u660e\u94b1={sm.get('score',0)} \u5747\u503c\u56de\u5f52={mr.get('score',0)} \u52a8\u91cf={mo.get('score',0)} \u505a\u7a7a={sh.get('score',0)}")

    # Top reasons (max 4)
    reason_lines = []
    for strat_name, result in sig.get("all_reasons", []):
        for r in result.get("reasons", [])[:2]:
            reason_lines.append(f"  \u2022 {r}")
    lines.extend(reason_lines[:4])

    result = "\n".join(lines)
    if len(result) > 4000:
        result = result[:3950] + "\n\n... (截断)"
    return result


# ══════════════════════════════════════════════════════════════════════════════
# BACKGROUND ENGINE
# ══════════════════════════════════════════════════════════════════════════════

class ProStrategyEngine:
    """Background scanner: runs pro strategy on interval."""

    _MAX_LAST_SIGNALS = 50

    def __init__(self, send_func=None):
        self._send = send_func
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._last_signals: list = []

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="pro_strategy")
        self._task.add_done_callback(self._on_done)
        logger.info("ProStrategyEngine started")

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
                logger.error("ProStrategyEngine loop crashed: %s", e, exc_info=True)

    async def _loop(self) -> None:
        await asyncio.sleep(30)
        while self._running:
            try:
                cfg = load_pro_config()
                signals = await scan_all_pro(cfg)
                if signals:
                    self._last_signals = signals[:self._MAX_LAST_SIGNALS]
                    min_score = cfg.get("min_combined_score", 60)
                    # Record and send top signals (double-check score threshold)
                    for sig in signals[:3]:
                        if sig.get("combined_score", 0) < min_score:
                            continue
                        if self._send:
                            await self._send(format_pro_signal(sig))
                        _record_signal(sig)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("ProStrategyEngine error: %s", str(e)[:300])
            try:
                cfg = load_pro_config()
                await asyncio.sleep(cfg.get("scan_interval", 900))
            except asyncio.CancelledError:
                break

    def get_last_signals(self) -> list:
        return list(self._last_signals)

    @property
    def running(self) -> bool:
        return self._running


def _record_signal(sig: dict) -> None:
    perf = _load_pro_perf()
    entry = {
        "symbol": sig.get("symbol", "UNKNOWN"),
        "direction": sig.get("direction", "neutral"),
        "score": sig.get("combined_score", 0),
        "entry_price": sig.get("entry_price", 0),
        "sl": sig.get("stop_loss", 0),
        "tp": sig.get("take_profit", 0),
        "rr": sig.get("risk_reward"),
        "consensus": sig.get("consensus", "?"),
        "timestamp": sig.get("timestamp", time.time()),
        "outcome": None,
    }
    perf["signals"].append(entry)
    if len(perf["signals"]) > 500:
        perf["signals"] = perf["signals"][-500:]
    _save_pro_perf(perf)


pro_engine = ProStrategyEngine()
