"""
strategy_optimizer.py — Self-optimizing signal strategy engine.

Phase 1 (existing): Win-rate based signal config tuning (weekly, auto-rollback).
Phase 2 (new):      Genetic algorithm parameter optimization for MA Ribbon, RSI, MACD.

GA cycle (daily):
  - Maintains a population of 10 parameter sets per strategy
  - Backtests each set on 7-day OHLCV data, ranks by Sharpe ratio
  - Eliminates bottom 30%, regenerates via crossover + mutation
  - Writes best params to _signal_engine_config.json
  - Logs each generation to evolution_log.jsonl
"""

import asyncio
import json
import logging
import math
import os
import random
import shutil
import time
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ─── File paths ───────────────────────────────────────────────────────────────
PERFORMANCE_STATS_FILE = os.path.join(BASE_DIR, "_performance_stats.json")
SIGNAL_HISTORY_FILE    = os.path.join(BASE_DIR, "_signal_history.json")
CONFIG_FILE            = os.path.join(BASE_DIR, "_signal_engine_config.json")
CONFIG_BACKUP_FILE     = os.path.join(BASE_DIR, "_signal_engine_config.backup.json")
OPTIMIZATION_LOG_FILE  = os.path.join(BASE_DIR, "_optimization_log.json")
GA_POPULATION_FILE     = os.path.join(BASE_DIR, "_ga_population.json")
EVOLUTION_LOG_FILE     = os.path.join(BASE_DIR, "evolution_log.jsonl")
RISK_PARAMS_FILE       = os.path.join(BASE_DIR, ".risk_params.json")
RISK_STATE_FILE        = os.path.join(BASE_DIR, ".risk_state.json")

# ─── GA Configuration ─────────────────────────────────────────────────────────
GA_POPULATION_SIZE = 10
GA_SURVIVE_COUNT   = 7     # keep top 70% (7 of 10)
GA_MUTATION_RATE   = 0.25
GA_EVAL_SYMBOLS    = ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"]
GA_BAR             = "1H"
GA_LOOKBACK_BARS   = 300   # fetch 300 bars (~12.5 days); last 168 bars = 7 days used
GA_INTERVAL        = 24 * 3600  # run daily

# ─── Parameter spaces ─────────────────────────────────────────────────────────
PARAM_SPACES = {
    "ma_ribbon": {
        "ma_fast":   {"type": "int",   "min": 3,    "max": 10},
        "ma_mid":    {"type": "int",   "min": 6,    "max": 15},
        "ema_slow":  {"type": "int",   "min": 15,   "max": 30},
        "ma_trend":  {"type": "int",   "min": 40,   "max": 80},
        "adx_min":   {"type": "float", "min": 15.0, "max": 30.0},
        "atr_mult":  {"type": "float", "min": 1.0,  "max": 3.5},
        "rr":        {"type": "float", "min": 1.5,  "max": 4.0},
    },
    "rsi": {
        "rsi_period":     {"type": "int", "min": 10, "max": 30},
        "rsi_overbought": {"type": "int", "min": 65, "max": 85},
        "rsi_oversold":   {"type": "int", "min": 15, "max": 35},
    },
    "macd": {
        "macd_fast":   {"type": "int", "min": 8,  "max": 20},
        "macd_slow":   {"type": "int", "min": 20, "max": 40},
        "macd_signal": {"type": "int", "min": 7,  "max": 15},
    },
}

GA_DEFAULT_PARAMS = {
    "ma_ribbon": {
        "ma_fast": 5, "ma_mid": 8, "ema_slow": 21, "ma_trend": 55,
        "adx_min": 20.0, "atr_mult": 2.0, "rr": 2.0,
    },
    "rsi": {
        "rsi_period": 14, "rsi_overbought": 70, "rsi_oversold": 30,
    },
    "macd": {
        "macd_fast": 12, "macd_slow": 26, "macd_signal": 9,
    },
}

# ─── Phase 1 settings ─────────────────────────────────────────────────────────
ROLLBACK_THRESHOLD   = 5.0
MIN_RESOLVED_SIGNALS = 10
TOP_SYMBOLS_KEEP     = 12
MAX_SYMBOLS_ADD      = 8


# ═══════════════════════════════════════════════════════════════════════════════
# SHARED FILE HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _load_json(path: str, default=None):
    if not os.path.exists(path):
        return default if default is not None else {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("strategy_optimizer: failed to load %s: %s", path, e)
        return default if default is not None else {}


def _save_json(path: str, data) -> None:
    _tmp = path + ".tmp"
    with open(_tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(_tmp, path)


def _load_signal_history() -> list:
    data = _load_json(SIGNAL_HISTORY_FILE, default=[])
    # Cap to last 5000 entries to prevent unbounded memory use
    if len(data) > 5000:
        data = data[-5000:]
    return data


def _load_performance_stats() -> dict:
    return _load_json(PERFORMANCE_STATS_FILE, default={})


def _load_config() -> dict:
    from signal_engine import load_config
    return load_config()


def _save_config(cfg: dict) -> None:
    _save_json(CONFIG_FILE, cfg)


def _load_optimization_log() -> list:
    return _load_json(OPTIMIZATION_LOG_FILE, default=[])


def _append_optimization_log(entry: dict) -> None:
    log = _load_optimization_log()
    log.append(entry)
    _save_json(OPTIMIZATION_LOG_FILE, log[-200:])


def _append_evolution_log(entry: dict) -> None:
    """Append a JSON line to evolution_log.jsonl."""
    line = json.dumps(entry, ensure_ascii=False)
    with open(EVOLUTION_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    # Truncate to last 5000 lines to prevent unbounded growth
    # Uses atomic write via temp file to avoid data loss on concurrent access
    try:
        with open(EVOLUTION_LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        if len(lines) > 5000:
            import tempfile
            dir_name = os.path.dirname(os.path.abspath(EVOLUTION_LOG_FILE))
            fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as tmp_f:
                    tmp_f.writelines(lines[-5000:])
                os.replace(tmp_path, EVOLUTION_LOG_FILE)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2: SELF-CONTAINED INDICATOR FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def _np():
    try:
        import numpy as np
        return np
    except ImportError:
        raise ImportError("numpy required for GA strategy optimization: pip install numpy")


def _sma(x, n):
    np = _np()
    out = np.full(len(x), float("nan"))
    for i in range(n - 1, len(x)):
        out[i] = float(np.mean(x[i - n + 1 : i + 1]))
    return out


def _ema(x, n):
    np = _np()
    a = 2.0 / (n + 1)
    out = np.full(len(x), float("nan"))
    if len(x) < n:
        return out
    out[n - 1] = float(np.mean(x[:n]))
    for i in range(n, len(x)):
        out[i] = a * x[i] + (1 - a) * out[i - 1]
    return out


def _rsi_arr(closes, period):
    np = _np()
    n = len(closes)
    out = np.full(n, float("nan"))
    if n < period + 1:
        return out
    deltas = np.diff(closes)
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = float(np.mean(gains[:period]))
    avg_loss = float(np.mean(losses[:period]))
    def _to_rsi(ag, al):
        return 100.0 if al < 1e-12 else 100.0 - 100.0 / (1.0 + ag / al)
    out[period] = _to_rsi(avg_gain, avg_loss)
    for i in range(period + 1, n):
        avg_gain = (avg_gain * (period - 1) + gains[i - 1]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i - 1]) / period
        out[i] = _to_rsi(avg_gain, avg_loss)
    return out


def _atr(h, l, c, n=14):
    np = _np()
    prev_c = np.empty_like(c)
    prev_c[0] = c[0]; prev_c[1:] = c[:-1]
    tr = np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(l - prev_c)))
    tr[0] = h[0] - l[0]
    return _sma(tr, n)


def _adx(h, l, c, n=14):
    np = _np()
    prev_c = np.empty_like(c); prev_c[0] = c[0]; prev_c[1:] = c[:-1]
    prev_h = np.empty_like(h); prev_h[0] = h[0]; prev_h[1:] = h[:-1]
    prev_l = np.empty_like(l); prev_l[0] = l[0]; prev_l[1:] = l[:-1]
    tr  = np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(l - prev_c)))
    tr[0] = h[0] - l[0]
    dmp = np.where((h - prev_h) > (prev_l - l), np.maximum(h - prev_h, 0.0), 0.0); dmp[0] = 0
    dmn = np.where((prev_l - l) > (h - prev_h), np.maximum(prev_l - l, 0.0), 0.0); dmn[0] = 0
    atr14 = _sma(tr, n)
    dip   = 100 * _sma(dmp, n) / (atr14 + 1e-12)
    din   = 100 * _sma(dmn, n) / (atr14 + 1e-12)
    dx    = 100 * np.abs(dip - din) / (dip + din + 1e-12)
    return _sma(dx, n)


def _sharpe(returns):
    np = _np()
    r = np.array(returns)
    if len(r) < 2:
        return 0.0
    return float(np.mean(r) / (np.std(r) + 1e-12) * math.sqrt(252 * 24))


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2: STRATEGY BACKTEST FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def _backtest_ma_ribbon(o, h, l, c, v, params: dict) -> dict:
    """MA Ribbon strategy backtest. Returns {sharpe, max_dd_pct, trades, win_rate}."""
    np = _np()
    n          = len(c)
    ma_fast_n  = int(params.get("ma_fast", 5))
    ma_mid_n   = int(params.get("ma_mid", 8))
    ema_slow_n = int(params.get("ema_slow", 21))
    ma_trend_n = int(params.get("ma_trend", 55))
    adx_min    = float(params.get("adx_min", 20))
    atr_mult   = float(params.get("atr_mult", 2.0))
    rr         = float(params.get("rr", 2.0))
    fee        = 0.0005

    # Constraint: fast < mid < slow < trend
    if not (ma_fast_n < ma_mid_n < ema_slow_n < ma_trend_n):
        return {"sharpe": -1.0, "max_dd_pct": 100.0, "trades": 0, "win_rate": 0.0}

    ma5   = _sma(c, ma_fast_n)
    ma8   = _sma(c, ma_mid_n)
    e21   = _ema(c, ema_slow_n)
    ma55  = _sma(c, ma_trend_n)
    atr14 = _atr(h, l, c, 14)
    adx14 = _adx(h, l, c, 14)
    vol_avg = _sma(v, 20)

    # Use last 168 bars (7 days @ 1H) for trade simulation
    start_idx = max(ma_trend_n + 20, n - 168)

    bull   = (c > ma5) & (ma5 > ma8) & (ma8 > e21) & (e21 > ma55)
    bear   = (c < ma5) & (ma5 < ma8) & (ma8 < e21) & (e21 < ma55)
    adx_ok = adx14 > adx_min
    vol_ok = v > 1.2 * vol_avg

    pos = 0; entry = 0.0; sl = 0.0; tp = 0.0
    equity = 1.0; peak_eq = 1.0; max_dd = 0.0
    returns = []; wins = 0; trades = 0

    for i in range(max(1, start_idx), n):
        if pos != 0:
            hit_sl = (pos == 1 and c[i] <= sl) or (pos == -1 and c[i] >= sl)
            hit_tp = (pos == 1 and c[i] >= tp) or (pos == -1 and c[i] <= tp)
            if hit_tp:
                net = abs(tp - entry) / entry - fee * 2
                equity *= (1 + net); returns.append(net); wins += 1; trades += 1; pos = 0
            elif hit_sl:
                net = -abs(sl - entry) / entry - fee * 2
                equity *= (1 + net); returns.append(net); trades += 1; pos = 0
            # Trail stop
            if pos == 1 and not np.isnan(atr14[i]):
                sl = max(sl, c[i] - atr_mult * atr14[i])
            elif pos == -1 and not np.isnan(atr14[i]):
                sl = min(sl, c[i] + atr_mult * atr14[i])
            peak_eq = max(peak_eq, equity)
            max_dd  = max(max_dd, (peak_eq - equity) / peak_eq if peak_eq > 0 else 0)
        if pos == 0 and not np.isnan(atr14[i]):
            sig = 0
            if bull[i] and not bull[i - 1] and adx_ok[i] and vol_ok[i]:
                sig = 1
            elif bear[i] and not bear[i - 1] and adx_ok[i] and vol_ok[i]:
                sig = -1
            if sig != 0:
                pos   = sig; entry = c[i]
                sl_d  = atr_mult * atr14[i]
                sl    = entry - sl_d if pos == 1 else entry + sl_d
                tp    = entry + rr * sl_d if pos == 1 else entry - rr * sl_d

    sharpe   = _sharpe(returns)
    win_rate = wins / trades * 100 if trades else 0.0
    return {
        "sharpe":     round(sharpe, 4),
        "max_dd_pct": round(max_dd * 100, 2),
        "trades":     trades,
        "win_rate":   round(win_rate, 1),
    }


def _backtest_rsi(c, params: dict) -> dict:
    """RSI mean-reversion strategy: enter on threshold cross, exit on reverse."""
    np        = _np()
    n         = len(c)
    period    = int(params.get("rsi_period", 14))
    overbought = int(params.get("rsi_overbought", 70))
    oversold  = int(params.get("rsi_oversold", 30))
    fee       = 0.0005

    rsi       = _rsi_arr(c, period)
    start_idx = max(period + 1, n - 168)

    pos = 0; entry = 0.0
    equity = 1.0; peak_eq = 1.0; max_dd = 0.0
    returns = []; wins = 0; trades = 0

    for i in range(max(1, start_idx), n):
        if np.isnan(rsi[i]) or np.isnan(rsi[i - 1]):
            continue
        # Exit
        if pos == 1 and rsi[i] >= overbought:
            net = (c[i] - entry) / entry - fee * 2
            equity *= (1 + net); returns.append(net)
            if net > 0: wins += 1
            trades += 1; pos = 0
        elif pos == -1 and rsi[i] <= oversold:
            net = (entry - c[i]) / entry - fee * 2
            equity *= (1 + net); returns.append(net)
            if net > 0: wins += 1
            trades += 1; pos = 0
        # Entry
        if pos == 0:
            if rsi[i - 1] >= oversold and rsi[i] < oversold:
                pos = 1; entry = c[i]
            elif rsi[i - 1] <= overbought and rsi[i] > overbought:
                pos = -1; entry = c[i]
        peak_eq = max(peak_eq, equity)
        max_dd  = max(max_dd, (peak_eq - equity) / peak_eq if peak_eq > 0 else 0)

    sharpe   = _sharpe(returns)
    win_rate = wins / trades * 100 if trades else 0.0
    return {
        "sharpe":     round(sharpe, 4),
        "max_dd_pct": round(max_dd * 100, 2),
        "trades":     trades,
        "win_rate":   round(win_rate, 1),
    }


def _backtest_macd(c, params: dict) -> dict:
    """MACD line/signal crossover strategy."""
    np       = _np()
    n        = len(c)
    fast_p   = int(params.get("macd_fast", 12))
    slow_p   = int(params.get("macd_slow", 26))
    signal_p = int(params.get("macd_signal", 9))
    fee      = 0.0005

    if fast_p >= slow_p:
        return {"sharpe": -1.0, "max_dd_pct": 100.0, "trades": 0, "win_rate": 0.0}

    ema_fast    = _ema(c, fast_p)
    ema_slow    = _ema(c, slow_p)
    # Guard: if either EMA is all-NaN (insufficient data), bail early
    if np.all(np.isnan(ema_fast)) or np.all(np.isnan(ema_slow)):
        return {"sharpe": -1.0, "max_dd_pct": 100.0, "trades": 0, "win_rate": 0.0}
    macd_line   = ema_fast - ema_slow
    signal_line = _ema(macd_line, signal_p)
    start_idx   = max(slow_p + signal_p, n - 168)

    pos = 0; entry = 0.0
    equity = 1.0; peak_eq = 1.0; max_dd = 0.0
    returns = []; wins = 0; trades = 0

    for i in range(max(1, start_idx), n):
        ml, sl_ = macd_line[i], signal_line[i]
        ml_p, sl_p = macd_line[i - 1], signal_line[i - 1]
        if any(np.isnan(x) for x in (ml, sl_, ml_p, sl_p)):
            continue
        bull_x = ml_p < sl_p and ml > sl_
        bear_x = ml_p > sl_p and ml < sl_
        # Exit
        if pos == 1 and bear_x:
            net = (c[i] - entry) / entry - fee * 2
            equity *= (1 + net); returns.append(net)
            if net > 0: wins += 1
            trades += 1; pos = 0
        elif pos == -1 and bull_x:
            net = (entry - c[i]) / entry - fee * 2
            equity *= (1 + net); returns.append(net)
            if net > 0: wins += 1
            trades += 1; pos = 0
        # Entry
        if pos == 0:
            if bull_x: pos = 1; entry = c[i]
            elif bear_x: pos = -1; entry = c[i]
        peak_eq = max(peak_eq, equity)
        max_dd  = max(max_dd, (peak_eq - equity) / peak_eq if peak_eq > 0 else 0)

    sharpe   = _sharpe(returns)
    win_rate = wins / trades * 100 if trades else 0.0
    return {
        "sharpe":     round(sharpe, 4),
        "max_dd_pct": round(max_dd * 100, 2),
        "trades":     trades,
        "win_rate":   round(win_rate, 1),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2: OKX DATA FETCH (synchronous, runs in executor)
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_ohlcv_sync(inst_id: str, bar: str = "1H", limit: int = 300) -> Optional[tuple]:
    """Fetch OHLCV from OKX. Returns (o, h, l, c, v) numpy arrays or None."""
    try:
        import httpx
        import numpy as np
    except ImportError as e:
        logger.error("Missing dependency for GA: %s", e)
        return None

    rows = []
    try:
        # history-candles supports longer lookback
        r = httpx.get(
            "https://www.okx.com/api/v5/market/history-candles",
            params={"instId": inst_id, "bar": bar, "limit": "100"},
            timeout=15,
        )
        chunk = r.json().get("data", [])
        rows.extend(chunk)
        fetched_pages = 1
        while len(rows) < limit and chunk and fetched_pages < 4:
            after = chunk[-1][0]
            r = httpx.get(
                "https://www.okx.com/api/v5/market/history-candles",
                params={"instId": inst_id, "bar": bar, "limit": "100", "after": after},
                timeout=15,
            )
            chunk = r.json().get("data", [])
            rows.extend(chunk)
            fetched_pages += 1
            time.sleep(0.05)
    except Exception as e:
        logger.debug("GA history-candles %s failed: %s; trying /candles", inst_id, e)
        try:
            r = httpx.get(
                "https://www.okx.com/api/v5/market/candles",
                params={"instId": inst_id, "bar": bar, "limit": str(min(limit, 300))},
                timeout=15,
            )
            rows = r.json().get("data", [])
        except Exception as e2:
            logger.warning("GA fetch failed for %s: %s", inst_id, e2)
            return None

    if not rows:
        return None
    rows.sort(key=lambda x: int(x[0]))
    rows = rows[-limit:]
    try:
        arr = np.array([[float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in rows])
        return arr[:, 0], arr[:, 1], arr[:, 2], arr[:, 3], arr[:, 4]
    except Exception as e:
        logger.warning("GA parse OHLCV %s failed: %s", inst_id, e)
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2: GENETIC ALGORITHM OPERATORS
# ═══════════════════════════════════════════════════════════════════════════════

def _sample_param(spec: dict):
    lo, hi = spec["min"], spec["max"]
    if spec["type"] == "int":
        return random.randint(int(lo), int(hi))
    return round(random.uniform(lo, hi), 2)


def _init_individual(param_space: dict) -> dict:
    return {k: _sample_param(v) for k, v in param_space.items()}


def _crossover(p1: dict, p2: dict, param_space: dict) -> dict:
    """Uniform crossover: each gene independently drawn from either parent."""
    return {k: (p1[k] if random.random() < 0.5 else p2[k]) for k in param_space}


def _mutate(params: dict, param_space: dict, rate: float = GA_MUTATION_RATE) -> dict:
    """Per-parameter mutation with Gaussian-like step."""
    mutated = dict(params)
    for k, spec in param_space.items():
        if random.random() < rate:
            lo, hi = spec["min"], spec["max"]
            if spec["type"] == "int":
                step = max(1, int((hi - lo) * 0.15))
                mutated[k] = max(int(lo), min(int(hi), int(params[k]) + random.randint(-step, step)))
            else:
                step = (hi - lo) * 0.15
                mutated[k] = round(max(lo, min(hi, float(params[k]) + random.uniform(-step, step))), 2)
    return mutated


def _load_population() -> dict:
    return _load_json(GA_POPULATION_FILE, default={})


def _save_population(pop: dict) -> None:
    _save_json(GA_POPULATION_FILE, pop)


def _ensure_population(strategy: str) -> list:
    """Load or initialize population for a strategy (always returns GA_POPULATION_SIZE entries)."""
    pop      = _load_population()
    existing = list(pop.get(strategy, []))
    space    = PARAM_SPACES[strategy]
    while len(existing) < GA_POPULATION_SIZE:
        existing.append(_init_individual(space))
    return existing[:GA_POPULATION_SIZE]


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2: FITNESS EVALUATION
# ═══════════════════════════════════════════════════════════════════════════════

def _evaluate_individual(strategy: str, params: dict, ohlcv_data: dict) -> dict:
    """Backtest params across all symbols, return aggregate metrics."""
    results = []
    for sym, data in ohlcv_data.items():
        if data is None:
            continue
        try:
            o, h, l, c, v = data
            if strategy == "ma_ribbon":
                r = _backtest_ma_ribbon(o, h, l, c, v, params)
            elif strategy == "rsi":
                r = _backtest_rsi(c, params)
            elif strategy == "macd":
                r = _backtest_macd(c, params)
            else:
                continue
            results.append(r)
        except Exception as e:
            logger.debug("GA eval %s %s failed: %s", strategy, sym, e)

    if not results:
        return {"avg_sharpe": -2.0, "avg_max_dd": 100.0, "total_trades": 0}

    import numpy as np
    avg_sharpe   = float(np.mean([r["sharpe"] for r in results]))
    avg_max_dd   = float(np.mean([r["max_dd_pct"] for r in results]))
    total_trades = sum(r["trades"] for r in results)
    return {
        "avg_sharpe":   round(avg_sharpe, 4),
        "avg_max_dd":   round(avg_max_dd, 2),
        "total_trades": total_trades,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2: GA EVOLUTION CYCLE
# ═══════════════════════════════════════════════════════════════════════════════

def _run_ga_strategy(strategy: str, ohlcv_data: dict) -> dict:
    """
    Run one GA generation for a single strategy.
    Returns {best_params, best_sharpe, max_dd, total_trades, worst_sharpe, population}.
    """
    param_space = PARAM_SPACES[strategy]
    population  = _ensure_population(strategy)

    # Score every individual
    scored = []
    for params in population:
        metrics = _evaluate_individual(strategy, params, ohlcv_data)
        scored.append((params, metrics))
    scored.sort(key=lambda x: x[1]["avg_sharpe"], reverse=True)

    # Select top survivors
    survivors = [p for p, _ in scored[:GA_SURVIVE_COUNT]]

    # Generate replacements via crossover + mutation
    needed = GA_POPULATION_SIZE - len(survivors)
    new_individuals = []
    for _ in range(needed):
        if len(survivors) >= 2:
            p1, p2 = random.sample(survivors, 2)
            child  = _crossover(p1, p2, param_space)
        else:
            child = _init_individual(param_space)
        new_individuals.append(_mutate(child, param_space))

    new_population = survivors + new_individuals

    # Persist updated population
    pop = _load_population()
    pop[strategy] = new_population
    _save_population(pop)

    best_p, best_m = scored[0]
    worst_sharpe   = scored[-1][1]["avg_sharpe"]
    return {
        "best_params":  best_p,
        "best_sharpe":  best_m["avg_sharpe"],
        "max_dd":       best_m["avg_max_dd"],
        "total_trades": best_m["total_trades"],
        "worst_sharpe": worst_sharpe,
        "population":   new_population,
    }


def _write_best_params_to_config(strategy: str, best_params: dict) -> None:
    """Write GA best params into _signal_engine_config.json."""
    try:
        cfg = _load_json(CONFIG_FILE, default={})
        if strategy == "ma_ribbon":
            cfg["ma_ribbon_best"] = best_params
            # Propagate overlapping signal engine keys
            for key in ("ma_fast", "ema_slow"):
                if key in best_params:
                    cfg[key] = best_params[key]
        elif strategy == "rsi":
            cfg["rsi_best"] = best_params
            cfg.update({
                "rsi_period":     best_params.get("rsi_period", 14),
                "rsi_overbought": best_params.get("rsi_overbought", 70),
                "rsi_oversold":   best_params.get("rsi_oversold", 30),
            })
        elif strategy == "macd":
            cfg["macd_best"] = best_params
        _save_json(CONFIG_FILE, cfg)
    except Exception as e:
        logger.error("GA write_best_params %s failed: %s", strategy, e)


def run_ga_evolution(trigger: str = "daily") -> dict:
    """
    Run one full GA evolution cycle for MA Ribbon, RSI, and MACD.
    Synchronous — call via run_in_executor from async context.
    Returns a summary dict.
    """
    logger.info("GA evolution cycle starting (trigger=%s)", trigger)

    # Fetch OHLCV once, shared across all strategies
    ohlcv_data = {}
    for sym in GA_EVAL_SYMBOLS:
        data = _fetch_ohlcv_sync(sym, bar=GA_BAR, limit=GA_LOOKBACK_BARS)
        if data is not None:
            ohlcv_data[sym] = data
            logger.info("GA: fetched %d bars for %s", len(data[0]), sym)
        else:
            logger.warning("GA: no data for %s", sym)

    if not ohlcv_data:
        return {
            "status":     "error",
            "message":    "❌ 无法获取 OKX 数据，跳过本次进化",
            "strategies": {},
        }

    ts_str  = datetime.now().isoformat()
    results = {}

    for strategy in ("ma_ribbon", "rsi", "macd"):
        try:
            result = _run_ga_strategy(strategy, ohlcv_data)
            results[strategy] = result

            _write_best_params_to_config(strategy, result["best_params"])

            _append_evolution_log({
                "ts":           ts_str,
                "type":         "ga_strategy_evolution",
                "trigger":      trigger,
                "strategy":     strategy,
                "best_params":  result["best_params"],
                "best_sharpe":  result["best_sharpe"],
                "max_dd_pct":   result["max_dd"],
                "total_trades": result["total_trades"],
                "worst_sharpe": result["worst_sharpe"],
            })

            logger.info("GA %s done: best_sharpe=%.3f max_dd=%.1f%%",
                        strategy, result["best_sharpe"], result["max_dd"])
        except Exception as e:
            logger.error("GA evolution failed for %s: %s", strategy, e, exc_info=True)
            results[strategy] = {"error": str(e)}

    return {
        "status":       "ok",
        "trigger":      trigger,
        "timestamp":    ts_str,
        "strategies":   results,
        "symbols_used": list(ohlcv_data.keys()),
    }


def format_ga_result(result: dict) -> str:
    """Format GA evolution result for Telegram display."""
    if result.get("status") == "error":
        return result.get("message", "❌ 进化失败")

    lines = [
        "🧬 **策略遗传算法进化完成**",
        f"数据来源: {', '.join(result.get('symbols_used', []))}",
    ]

    names = {"ma_ribbon": "MA Ribbon", "rsi": "RSI", "macd": "MACD"}

    for strategy, data in result.get("strategies", {}).items():
        name = names.get(strategy, strategy)
        if "error" in data:
            lines.append(f"\n❌ {name}: {data['error'][:80]}")
            continue
        sharpe = data.get("best_sharpe", 0)
        dd     = data.get("max_dd", 0)
        trades = data.get("total_trades", 0)
        params = data.get("best_params", {})
        lines.append(f"\n📊 **{name}**")
        lines.append(f"  夏普: {sharpe:.3f} | 最大回撤: {dd:.1f}% | 交易: {trades}笔")
        if strategy == "ma_ribbon":
            lines.append(
                f"  MA: {params.get('ma_fast')}/{params.get('ma_mid')}"
                f"/{params.get('ema_slow')}/{params.get('ma_trend')}"
            )
            lines.append(
                f"  ADX≥{params.get('adx_min')} | ATRx{params.get('atr_mult')} | RR={params.get('rr')}"
            )
        elif strategy == "rsi":
            lines.append(
                f"  周期={params.get('rsi_period')} | "
                f"超买={params.get('rsi_overbought')} | 超卖={params.get('rsi_oversold')}"
            )
        elif strategy == "macd":
            lines.append(
                f"  快线={params.get('macd_fast')} | "
                f"慢线={params.get('macd_slow')} | 信号={params.get('macd_signal')}"
            )

    lines.append("\n✅ 最优参数已写入 _signal_engine_config.json")
    result = "\n".join(lines)
    if len(result) > 4000:
        result = result[:3950] + "\n\n... (截断)"
    return result


def get_ga_summary() -> str:
    """Return last GA evolution results from evolution_log.jsonl."""
    entries = []
    try:
        with open(EVOLUTION_LOG_FILE, "r", encoding="utf-8") as f:
            # Read only last 500 lines to avoid loading huge file into memory
            all_lines = f.readlines()
            tail_lines = all_lines[-500:] if len(all_lines) > 500 else all_lines
            for line in tail_lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if obj.get("type") == "ga_strategy_evolution":
                        entries.append(obj)
                except Exception:
                    pass
    except FileNotFoundError:
        return "暂无GA进化记录。"

    if not entries:
        return "暂无GA进化记录。"

    # Most recent entry per strategy
    latest = {}
    for e in entries:
        latest[e.get("strategy")] = e

    lines = ["📋 **最近GA进化结果**", ""]
    names = {"ma_ribbon": "MA Ribbon", "rsi": "RSI", "macd": "MACD"}
    for strategy in ("ma_ribbon", "rsi", "macd"):
        e = latest.get(strategy)
        if not e:
            continue
        name   = names.get(strategy, strategy)
        sharpe = e.get("best_sharpe", 0)
        dd     = e.get("max_dd_pct", 0)
        ts     = e.get("ts", "?")[:19]
        lines.append(f"  {name}: 夏普={sharpe:.3f} 最大回撤={dd:.1f}% [{ts}]")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 1: EXISTING WIN-RATE OPTIMIZATION (unchanged)
# ═══════════════════════════════════════════════════════════════════════════════

def _analyze_by_symbol(signals: list) -> dict:
    by_sym: dict = {}
    for s in signals:
        if s.get("status") not in ("win", "loss"):
            continue
        sym = s.get("symbol", "UNKNOWN")
        by_sym.setdefault(sym, {"wins": 0, "total": 0, "pnls": []})
        by_sym[sym]["total"] += 1
        by_sym[sym]["pnls"].append(s.get("final_pnl_pct", 0))
        if s.get("status") == "win":
            by_sym[sym]["wins"] += 1
    result = {}
    for sym, d in by_sym.items():
        wr  = round(d["wins"] / d["total"] * 100, 1) if d["total"] else 0
        avg = round(sum(d["pnls"]) / len(d["pnls"]), 3) if d["pnls"] else 0
        result[sym] = {"wins": d["wins"], "total": d["total"], "win_rate": wr, "avg_pnl": avg}
    return result


def _analyze_by_rsi_range(signals: list, cfg: dict) -> dict:
    resolved = [s for s in signals if s.get("status") in ("win", "loss")]
    if not resolved:
        return {}
    win_rate   = (sum(1 for s in resolved if s["status"] == "win") / len(resolved) * 100) if resolved else 0
    current_ob = cfg.get("rsi_overbought", 70)
    current_os = cfg.get("rsi_oversold", 30)
    new_ob, new_os = current_ob, current_os
    if win_rate >= 60:
        new_ob = min(current_ob + 2, 80)
        new_os = max(current_os - 2, 20)
    elif win_rate < 45:
        new_ob = max(current_ob - 2, 60)
        new_os = min(current_os + 2, 40)
    return {"rsi_overbought": new_ob, "rsi_oversold": new_os}


def _select_best_symbols(by_symbol: dict, current_symbols: list, all_default_symbols: list) -> list:
    if not by_symbol:
        return current_symbols
    ranked   = sorted(
        [(sym, d) for sym, d in by_symbol.items() if d["total"] >= 3],
        key=lambda x: (x[1]["win_rate"], x[1]["avg_pnl"]),
        reverse=True,
    )
    top_syms = [sym for sym, _ in ranked[:TOP_SYMBOLS_KEEP]]
    extras   = [s for s in all_default_symbols if s not in top_syms]
    top_syms += extras[:MAX_SYMBOLS_ADD]
    return top_syms if top_syms else current_symbols


def compute_optimized_config(signals: list, stats: dict, cfg: dict) -> tuple:
    new_cfg = dict(cfg)
    changes = {}
    resolved = [s for s in signals if s.get("status") in ("win", "loss")]
    if len(resolved) < MIN_RESOLVED_SIGNALS:
        return cfg, {}

    rsi_changes = _analyze_by_rsi_range(signals, cfg)
    for key, new_val in rsi_changes.items():
        old_val = cfg.get(key)
        if old_val != new_val:
            changes[key]    = {"old": old_val, "new": new_val}
            new_cfg[key]    = new_val

    by_symbol = _analyze_by_symbol(signals)
    from signal_engine import DEFAULT_CONFIG
    default_symbols = DEFAULT_CONFIG["symbols"]
    new_symbols     = _select_best_symbols(by_symbol, cfg.get("symbols", []), default_symbols)
    if sorted(new_symbols) != sorted(cfg.get("symbols", [])):
        changes["symbols"] = {"old": cfg.get("symbols", []), "new": new_symbols}
        new_cfg["symbols"] = new_symbols

    by_type  = stats.get("by_type", {})
    tf_stats: dict = {}
    for st, d in by_type.items():
        parts = st.rsplit("_", 1)
        if len(parts) == 2 and d.get("total", 0) >= 3:
            tf = parts[1].upper()
            tf_stats.setdefault(tf, {"wins": 0, "total": 0})
            tf_stats[tf]["wins"]  += d.get("wins", 0)
            tf_stats[tf]["total"] += d.get("total", 0)
    tf_stats = {k: v for k, v in tf_stats.items() if v.get("total", 0) > 0}
    if tf_stats:
        best_tf = max(tf_stats, key=lambda t: tf_stats[t]["wins"] / max(tf_stats[t]["total"], 1))
        if best_tf != cfg.get("timeframe"):
            changes["timeframe"] = {"old": cfg.get("timeframe"), "new": best_tf}
            new_cfg["timeframe"] = best_tf

    return new_cfg, changes


def _current_win_rate(signals: list) -> Optional[float]:
    resolved = [s for s in signals if s.get("status") in ("win", "loss")]
    if not resolved:
        return None
    return round(sum(1 for s in resolved if s["status"] == "win") / len(resolved) * 100, 1)


def _recent_win_rate(signals: list, days: int = 14) -> Optional[float]:
    cutoff = time.time() - days * 86400
    recent = [s for s in signals if s.get("timestamp", 0) >= cutoff
              and s.get("status") in ("win", "loss")]
    if not recent:
        return None
    return round(sum(1 for s in recent if s["status"] == "win") / len(recent) * 100, 1)


def _check_rollback_needed(log: list, signals: list) -> tuple:
    if not log:
        return False, None
    last = log[-1]
    if last.get("status") != "applied":
        return False, None
    pre_wr = last.get("pre_win_rate")
    if pre_wr is None:
        return False, None
    opt_ts       = last.get("timestamp", 0)
    post_signals = [s for s in signals if s.get("timestamp", 0) > opt_ts
                    and s.get("status") in ("win", "loss")]
    if len(post_signals) < 5:
        return False, None
    post_wr = round(sum(1 for s in post_signals if s["status"] == "win") / len(post_signals) * 100, 1)
    if post_wr < pre_wr - ROLLBACK_THRESHOLD:
        return True, f"Win rate dropped from {pre_wr}% → {post_wr}% (threshold: -{ROLLBACK_THRESHOLD}%)"
    last["post_win_rate"] = post_wr
    return False, None


def _do_rollback(reason: str) -> bool:
    if not os.path.exists(CONFIG_BACKUP_FILE):
        logger.warning("strategy_optimizer: rollback requested but no backup found")
        return False
    try:
        shutil.copy2(CONFIG_BACKUP_FILE, CONFIG_FILE)
        logger.info("strategy_optimizer: rolled back config. Reason: %s", reason)
        return True
    except Exception as e:
        logger.error("strategy_optimizer: rollback failed: %s", e)
        return False


def run_optimization(trigger: str = "weekly") -> dict:
    """Phase 1: Win-rate based config optimization with auto-rollback."""
    signals = _load_signal_history()
    stats   = _load_performance_stats()
    cfg     = _load_config()
    log     = _load_optimization_log()
    pre_wr  = _recent_win_rate(signals)

    should_rollback, rollback_reason = _check_rollback_needed(log, signals)
    if should_rollback:
        _do_rollback(rollback_reason)
        entry = {
            "timestamp":  time.time(),
            "date":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "trigger":    trigger,
            "status":     "rolled_back",
            "reason":     rollback_reason,
            "pre_win_rate": pre_wr,
            "changes":    {},
        }
        if log:
            entry["rolled_back_entry"] = log[-1].get("timestamp", "?")
        _append_optimization_log(entry)
        return {
            "status":       "rolled_back",
            "message":      f"⏪ 已回滚上次优化\n原因: {rollback_reason}",
            "changes":      {},
            "pre_win_rate": pre_wr,
        }

    new_cfg, changes = compute_optimized_config(signals, stats, cfg)
    if not changes:
        resolved_count = len([s for s in signals if s.get("status") in ("win", "loss")])
        if resolved_count < MIN_RESOLVED_SIGNALS:
            msg = f"📊 数据不足（已完成信号: {resolved_count}/{MIN_RESOLVED_SIGNALS}），暂不优化"
        else:
            msg = "✅ 参数已是最优，无需调整"
        _append_optimization_log({
            "timestamp":    time.time(),
            "date":         datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "trigger":      trigger,
            "status":       "no_change",
            "pre_win_rate": pre_wr,
            "changes":      {},
        })
        return {"status": "no_change", "message": msg, "changes": {}, "pre_win_rate": pre_wr}

    shutil.copy2(CONFIG_FILE, CONFIG_BACKUP_FILE) if os.path.exists(CONFIG_FILE) else None
    _save_config(new_cfg)

    change_lines = []
    for param, v in changes.items():
        if param == "symbols":
            added   = set(v["new"]) - set(v["old"])
            removed = set(v["old"]) - set(v["new"])
            if added:   change_lines.append(f"  +symbols: {', '.join(sorted(added))}")
            if removed: change_lines.append(f"  -symbols: {', '.join(sorted(removed))}")
        else:
            change_lines.append(f"  {param}: {v['old']} → {v['new']}")

    _append_optimization_log({
        "timestamp":    time.time(),
        "date":         datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "trigger":      trigger,
        "status":       "applied",
        "pre_win_rate": pre_wr,
        "post_win_rate": None,
        "changes":      {k: {"old": str(v["old"]), "new": str(v["new"])} for k, v in changes.items()},
    })

    msg_lines = ["🔧 **策略优化完成**", f"触发方式: {trigger}"]
    if pre_wr is not None:
        msg_lines.append(f"当前胜率: {pre_wr}%")
    msg_lines.append("\n**参数变更:**")
    msg_lines += change_lines
    msg_lines.append("\n（下次优化时将评估效果，若胜率下降将自动回滚）")

    return {
        "status":       "applied",
        "message":      "\n".join(msg_lines),
        "changes":      changes,
        "pre_win_rate": pre_wr,
    }


def get_optimization_summary() -> str:
    log = _load_optimization_log()
    if not log:
        return "暂无优化记录。"
    lines = ["📋 **优化历史** (最近5次)", ""]
    for entry in reversed(log[-5:]):
        status_emoji = {"applied": "✅", "rolled_back": "⏪", "no_change": "—"}.get(
            entry.get("status", ""), "?"
        )
        date     = entry.get("date", "未知时间")
        trigger  = entry.get("trigger", "?")
        pre_wr   = entry.get("pre_win_rate")
        post_wr  = entry.get("post_win_rate")
        wr_str   = f" | 胜率: {pre_wr}%" if pre_wr else ""
        if post_wr:
            wr_str += f" → {post_wr}%"
        changes  = entry.get("changes", {})
        n_changes = len([k for k in changes if k != "symbols"]) + (1 if "symbols" in changes else 0)
        lines.append(f"{status_emoji} {date} [{trigger}]{wr_str}")
        if n_changes:
            lines.append(f"   调整了 {n_changes} 个参数")
    result = "\n".join(lines)
    if len(result) > 4000:
        result = result[:3950] + "\n\n... (截断)"
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 3: ADAPTIVE RISK MANAGEMENT SYSTEM
# ═══════════════════════════════════════════════════════════════════════════════

DEFAULT_RISK_PARAMS = {
    # ATR volatility adaptation
    "atr_high_mult":      2.0,   # ATR ratio above this → high volatility
    "atr_low_mult":       0.7,   # ATR ratio below this → low volatility
    "pos_high_vol":       0.5,   # position multiplier when high vol
    "pos_low_vol":        1.5,   # position multiplier when low vol
    "pos_normal":         1.0,   # position multiplier when normal vol
    # Consecutive loss protection
    "consec_loss_limit":  3,     # consecutive losses before auto-pause
    "consec_loss_pause_h": 1,    # pause duration in hours
    # Daily drawdown limit
    "daily_dd_limit_pct": 5.0,   # stop new signals if daily loss exceeds this %
    # Kelly compounding
    "kelly_win_streak":   3,     # consecutive wins needed to activate Kelly bonus
    "kelly_max_mult":     1.5,   # max position multiplier from Kelly
    "kelly_fraction":     0.25,  # fraction of Kelly formula to apply
}

DEFAULT_RISK_STATE = {
    "consecutive_losses":  0,
    "consecutive_wins":    0,
    "paused_until":        0.0,   # epoch seconds; 0 = not paused
    "today_date":          "",    # YYYY-MM-DD; resets daily counters on change
    "today_pnl_pct":       0.0,
    "today_peak_pnl_pct":  0.0,
    "today_max_dd_pct":    0.0,
    "trades_today":        0,
}


class RiskManager:
    """
    Phase 3: Adaptive Risk Management.

    Features:
    - ATR volatility adapter: high ATR → shrink position, low ATR → grow position
    - Consecutive loss guard: 3 losses → auto-pause 1 hour
    - Daily max-drawdown limit: account loss > 5% → halt new signals today
    - Kelly compounding: win streak → small position increase (simplified Kelly)
    - Risk dashboard: real-time exposure / P&L / drawdown summary
    - All params in .risk_params.json with hot-reload support
    """

    def __init__(self):
        self._params_mtime = 0.0
        self._params = self._load_params()

    # ── Params ────────────────────────────────────────────────────────────────

    def _load_params(self) -> dict:
        data = _load_json(RISK_PARAMS_FILE, default={})
        params = dict(DEFAULT_RISK_PARAMS)
        params.update(data)
        if not os.path.exists(RISK_PARAMS_FILE):
            _save_json(RISK_PARAMS_FILE, params)
        try:
            self._params_mtime = os.path.getmtime(RISK_PARAMS_FILE)
        except Exception:
            pass
        return params

    def _maybe_reload_params(self) -> None:
        """Hot-reload .risk_params.json if the file has changed."""
        try:
            mtime = os.path.getmtime(RISK_PARAMS_FILE)
            if mtime > self._params_mtime:
                self._params = self._load_params()
                logger.info("RiskManager: hot-reloaded params from %s", RISK_PARAMS_FILE)
        except Exception:
            pass

    def reload_params(self) -> str:
        """Force-reload params; returns confirmation string."""
        self._params = self._load_params()
        return f"✅ 风险参数已重载：{len(self._params)} 项"

    # ── State ─────────────────────────────────────────────────────────────────

    def _load_state(self) -> dict:
        state = dict(DEFAULT_RISK_STATE)
        state.update(_load_json(RISK_STATE_FILE, default={}))
        today = datetime.now().strftime("%Y-%m-%d")
        if state.get("today_date") != today:
            state["today_date"]         = today
            state["today_pnl_pct"]      = 0.0
            state["today_peak_pnl_pct"] = 0.0
            state["today_max_dd_pct"]   = 0.0
            state["trades_today"]       = 0
        return state

    def _save_state(self, state: dict) -> None:
        _save_json(RISK_STATE_FILE, state)

    # ── Core checks ───────────────────────────────────────────────────────────

    def check_trade_allowed(self) -> tuple:
        """Returns (allowed: bool, reason: str)."""
        self._maybe_reload_params()
        state = self._load_state()
        now   = time.time()

        paused_until = state.get("paused_until", 0.0)
        if paused_until > now:
            remaining = int((paused_until - now) / 60)
            return False, f"⏸ 连亏保护：剩余 {remaining} 分钟"

        daily_loss = abs(min(state.get("today_pnl_pct", 0.0), 0.0))
        dd_limit   = self._params.get("daily_dd_limit_pct", 5.0)
        if daily_loss >= dd_limit:
            return False, f"🛑 今日亏损 {daily_loss:.1f}% ≥ 限制 {dd_limit:.1f}%，已停止新信号"

        return True, "ok"

    def get_position_multiplier(self, atr_current: float = None, atr_avg: float = None) -> float:
        """
        Returns a position size multiplier combining:
        - ATR volatility factor (high vol → smaller, low vol → larger)
        - Simplified Kelly for consecutive wins
        """
        self._maybe_reload_params()
        p = self._params

        # Volatility factor
        vol_mult = p.get("pos_normal", 1.0)
        if atr_current is not None and atr_avg is not None and atr_avg > 0:
            ratio = atr_current / atr_avg
            if ratio >= p.get("atr_high_mult", 2.0):
                vol_mult = p.get("pos_high_vol", 0.5)
            elif ratio <= p.get("atr_low_mult", 0.7):
                vol_mult = p.get("pos_low_vol", 1.5)

        # Kelly win-streak bonus
        state        = self._load_state()
        consec_wins  = state.get("consecutive_wins", 0)
        kelly_streak = int(p.get("kelly_win_streak", 3))
        kelly_max    = float(p.get("kelly_max_mult", 1.5))
        kelly_frac   = float(p.get("kelly_fraction", 0.25))

        kelly_mult = 1.0
        if consec_wins >= kelly_streak:
            extra      = consec_wins - kelly_streak + 1
            kelly_mult = min(1.0 + kelly_frac * extra, kelly_max)

        return round(vol_mult * kelly_mult, 3)

    # ── Trade recording ───────────────────────────────────────────────────────

    def record_trade_result(self, win: bool, pnl_pct: float = 0.0) -> None:
        """
        Call after each trade resolves.
        pnl_pct: percent P&L (e.g. +2.5 or -1.3).
        Updates consecutive counters, pause timer, and daily drawdown tracking.
        """
        self._maybe_reload_params()
        state = self._load_state()
        p     = self._params

        if win:
            state["consecutive_wins"]   = state.get("consecutive_wins", 0) + 1
            state["consecutive_losses"] = 0
        else:
            state["consecutive_losses"] = state.get("consecutive_losses", 0) + 1
            state["consecutive_wins"]   = 0
            loss_limit = int(p.get("consec_loss_limit", 3))
            if state["consecutive_losses"] >= loss_limit:
                pause_h = float(p.get("consec_loss_pause_h", 1))
                state["paused_until"] = time.time() + pause_h * 3600
                logger.warning(
                    "RiskManager: %d consecutive losses → pausing %.1fh",
                    state["consecutive_losses"], pause_h,
                )

        state["today_pnl_pct"] = round(state.get("today_pnl_pct", 0.0) + pnl_pct, 4)
        state["trades_today"]  = state.get("trades_today", 0) + 1

        today_pnl = state["today_pnl_pct"]
        if today_pnl > state.get("today_peak_pnl_pct", 0.0):
            state["today_peak_pnl_pct"] = today_pnl
        drawdown = state["today_peak_pnl_pct"] - today_pnl
        if drawdown > state.get("today_max_dd_pct", 0.0):
            state["today_max_dd_pct"] = drawdown

        self._save_state(state)

    # ── Dashboard ─────────────────────────────────────────────────────────────

    def get_dashboard(self, atr_current: float = None, atr_avg: float = None) -> str:
        """Return a formatted Telegram-ready risk dashboard."""
        self._maybe_reload_params()
        state = self._load_state()
        p     = self._params
        now   = time.time()

        allowed, reason = self.check_trade_allowed()
        status_line = "✅ 交易状态: 正常" if allowed else f"⚠️ 交易状态: {reason}"

        today_pnl   = state.get("today_pnl_pct", 0.0)
        pnl_emoji   = "📈" if today_pnl >= 0 else "📉"
        max_dd      = state.get("today_max_dd_pct", 0.0)
        dd_limit    = float(p.get("daily_dd_limit_pct", 5.0))
        dd_bar_used = min(int(max_dd / dd_limit * 10), 10) if dd_limit > 0 else 0
        dd_bar      = "█" * dd_bar_used + "░" * (10 - dd_bar_used)

        pos_mult      = self.get_position_multiplier(atr_current, atr_avg)
        consec_losses = state.get("consecutive_losses", 0)
        consec_wins   = state.get("consecutive_wins", 0)
        loss_limit    = int(p.get("consec_loss_limit", 3))
        paused_until  = state.get("paused_until", 0.0)

        vol_regime = "—"
        if atr_current is not None and atr_avg is not None and atr_avg > 0:
            ratio = atr_current / atr_avg
            if ratio >= p.get("atr_high_mult", 2.0):
                vol_regime = f"🔴 高波动 ({ratio:.1f}x)"
            elif ratio <= p.get("atr_low_mult", 0.7):
                vol_regime = f"🟢 低波动 ({ratio:.1f}x)"
            else:
                vol_regime = f"🟡 正常 ({ratio:.1f}x)"

        lines = [
            "📊 **风险仪表盘**",
            "",
            status_line,
            "",
            "📅 **今日统计**",
            f"  {pnl_emoji} 盈亏: {today_pnl:+.2f}%",
            f"  📉 最大回撤: {max_dd:.2f}% / {dd_limit:.1f}%  [{dd_bar}]",
            f"  🔢 交易笔数: {state.get('trades_today', 0)}",
            "",
            "⚡ **波动率适应**",
            f"  状态: {vol_regime}",
            f"  仓位倍数: {pos_mult:.2f}x",
            "",
            f"🎯 **连胜 / 连亏**",
            f"  连续亏损: {consec_losses} / {loss_limit}",
            f"  连续盈利: {consec_wins}",
        ]
        if paused_until > now:
            remaining = int((paused_until - now) / 60)
            resume_t  = datetime.fromtimestamp(paused_until).strftime("%H:%M")
            lines.append(f"  ⏸ 暂停至 {resume_t}（剩余 {remaining} 分钟）")

        lines += [
            "",
            "⚙️ **参数**",
            f"  连亏限制: {loss_limit}次 / 暂停 {p.get('consec_loss_pause_h', 1)}h",
            f"  日亏限制: {dd_limit:.1f}%",
            f"  Kelly激活: 连赢 {int(p.get('kelly_win_streak', 3))} 次",
        ]
        return "\n".join(lines)

    # ── Manual controls ───────────────────────────────────────────────────────

    def reset_pause(self) -> str:
        """Manually clear the trading pause and reset consecutive-loss counter."""
        state = self._load_state()
        state["paused_until"]       = 0.0
        state["consecutive_losses"] = 0
        self._save_state(state)
        return "✅ 已手动解除交易暂停，连亏计数已清零"

    def reset_today(self) -> str:
        """Reset all today's counters (for testing / manual override)."""
        state = self._load_state()
        state["today_pnl_pct"]      = 0.0
        state["today_peak_pnl_pct"] = 0.0
        state["today_max_dd_pct"]   = 0.0
        state["trades_today"]       = 0
        self._save_state(state)
        return "✅ 今日风险计数器已重置"


# ═══════════════════════════════════════════════════════════════════════════════
# COMBINED StrategyOptimizer CLASS
# ═══════════════════════════════════════════════════════════════════════════════

OPTIMIZE_INTERVAL = 7 * 24 * 3600  # Phase 1: weekly


class StrategyOptimizer:
    """
    Background optimizer:
    - Phase 1: Weekly win-rate based signal config tuning (auto-rollback)
    - Phase 2: Daily GA parameter evolution for MA Ribbon, RSI, MACD
    """

    def __init__(self, notify_func=None):
        self._notify   = notify_func
        self._running  = False
        self._task_p1: Optional[asyncio.Task] = None
        self._task_p2: Optional[asyncio.Task] = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task_p1 = asyncio.create_task(self._loop_phase1(), name="strategy_optimizer_p1")
        self._task_p2 = asyncio.create_task(self._loop_phase2(), name="strategy_optimizer_p2")
        for t in (self._task_p1, self._task_p2):
            t.add_done_callback(self._on_done)
        logger.info("StrategyOptimizer started (phase1=7d, phase2=daily)")

    async def stop(self) -> None:
        self._running = False
        for t in (self._task_p1, self._task_p2):
            if t and not t.done():
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass

    def _on_done(self, task: asyncio.Task) -> None:
        if not task.cancelled():
            try:
                task.result()
            except Exception as e:
                logger.error("StrategyOptimizer task crashed: %s", e, exc_info=True)

    async def _loop_phase1(self) -> None:
        await asyncio.sleep(3600)  # initial delay: 1h
        while self._running:
            try:
                result = run_optimization(trigger="weekly")
                if self._notify and result["status"] != "no_change":
                    await self._notify(result["message"])
                logger.info("Phase1 weekly run: %s", result["status"])
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Phase1 optimizer error: %s", e)
            try:
                await asyncio.sleep(OPTIMIZE_INTERVAL)
            except asyncio.CancelledError:
                break

    async def _loop_phase2(self) -> None:
        await asyncio.sleep(1800)  # initial delay: 30 min
        while self._running:
            try:
                loop   = asyncio.get_running_loop()
                result = await loop.run_in_executor(None, run_ga_evolution, "daily")
                if self._notify and result.get("status") == "ok":
                    await self._notify(format_ga_result(result))
                logger.info("Phase2 GA daily run: %s", result.get("status"))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Phase2 GA optimizer error: %s", e)
            try:
                await asyncio.sleep(GA_INTERVAL)
            except asyncio.CancelledError:
                break

    async def optimize_now(self, trigger: str = "manual") -> dict:
        """Phase 1: run signal config optimization immediately."""
        try:
            return run_optimization(trigger=trigger)
        except Exception as e:
            return {"status": "error", "message": f"❌ 优化失败: {str(e)[:300]}", "changes": {}}

    async def evolve_now(self) -> dict:
        """Phase 2: run GA evolution immediately (for /strategy_evolve command)."""
        try:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, run_ga_evolution, "manual")
        except Exception as e:
            logger.error("GA evolve_now failed: %s", e)
            return {"status": "error", "message": f"❌ 进化失败: {str(e)[:300]}", "strategies": {}}

    @property
    def running(self) -> bool:
        return self._running


# Module-level singletons
strategy_optimizer = StrategyOptimizer()
risk_manager       = RiskManager()


# ═══════════════════════════════════════════════════════════════════════════════
# P3_20: BAYESIAN SIGNAL OPTIMIZATION LOOP
# ═══════════════════════════════════════════════════════════════════════════════

P3_AB_STATE_FILE    = os.path.join(BASE_DIR, "_p3_ab_state.json")
P3_OPTIM_LOG_FILE   = os.path.join(BASE_DIR, "_p3_optim_log.json")
PERF_FILE_PATH      = os.path.join(BASE_DIR, ".signal_performance.json")

# Parameter space for Bayesian optimization (actual signal_engine params)
P3_PARAM_SPACE = {
    "rsi_oversold":           {"type": "int",   "min": 20, "max": 38},
    "rsi_overbought":         {"type": "int",   "min": 62, "max": 80},
    "confidence_threshold":   {"type": "int",   "min": 65, "max": 85},
    "signal_score_threshold": {"type": "int",   "min": 1,  "max": 4},
    "ma_fast":                {"type": "int",   "min": 3,  "max": 12},
    "ma_slow":                {"type": "int",   "min": 15, "max": 30},
}

P3_AB_SIGNALS_EACH  = 50   # signals per A/B leg
P3_OPTIM_TRIGGER    = 100  # run optimization every N resolved signals


# ── P3 helpers ────────────────────────────────────────────────────────────────

def _p3_load_ab() -> dict:
    default = {
        "phase": "idle",          # idle | A | B | evaluating
        "a_params": None,
        "b_params": None,
        "a_signals": [],          # [{"id": ..., "outcome": ...}, ...]
        "b_signals": [],
        "last_eval_ts": 0.0,
        "round_number": 0,
    }
    data = _load_json(P3_AB_STATE_FILE, default={})
    default.update(data)
    return default


def _p3_save_ab(state: dict) -> None:
    _save_json(P3_AB_STATE_FILE, state)


def _p3_load_optim_log() -> list:
    return _load_json(P3_OPTIM_LOG_FILE, default=[])


def _p3_append_optim_log(entry: dict) -> None:
    log = _p3_load_optim_log()
    log.append(entry)
    _save_json(P3_OPTIM_LOG_FILE, log[-100:])


def _p3_load_perf_signals() -> list:
    """Load signals from signal_engine's .signal_performance.json."""
    try:
        with open(PERF_FILE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        signals = data.get("signals", [])
        # Cap to last 5000 to prevent unbounded memory use
        if len(signals) > 5000:
            signals = signals[-5000:]
        return signals
    except Exception:
        return []


# ── Bayesian Optimization (pure numpy) ───────────────────────────────────────

def _p3_normalize_params(params: dict, space: dict) -> list:
    """Normalize param dict → [0,1] feature vector."""
    result = []
    for key in sorted(space.keys()):
        lo = float(space[key]["min"])
        hi = float(space[key]["max"])
        val = float(params.get(key, (lo + hi) / 2))
        result.append((val - lo) / (hi - lo + 1e-9))
    return result


def _p3_sample_candidate(space: dict) -> dict:
    """Sample a random candidate from the parameter space."""
    candidate = {}
    for key, spec in space.items():
        lo, hi = spec["min"], spec["max"]
        if spec["type"] == "int":
            candidate[key] = random.randint(int(lo), int(hi))
        else:
            candidate[key] = round(random.uniform(lo, hi), 2)
    return candidate


def _p3_gp_ucb(
    X_obs,      # np.array shape (n, d)
    y_obs,      # np.array shape (n,)
    X_cand,     # np.array shape (m, d)
    beta: float = 2.0,
    length_scale: float = 0.3,
) -> "np.ndarray":
    """
    Gaussian Process UCB acquisition (pure numpy, RBF kernel).
    Returns acquisition values for each candidate (higher = better to explore).
    """
    np = _np()
    n = len(X_obs)
    if n == 0:
        return np.zeros(len(X_cand))

    # RBF kernel: k(x,x') = exp(-||x-x'||^2 / (2*l^2))
    def _rbf(A, B):
        dists = np.sum((A[:, None, :] - B[None, :, :]) ** 2, axis=-1)
        return np.exp(-dists / (2 * length_scale ** 2))

    K      = _rbf(X_obs, X_obs) + 1e-4 * np.eye(n)
    K_star = _rbf(X_cand, X_obs)  # (m, n)

    try:
        L     = np.linalg.cholesky(K)
        alpha = np.linalg.solve(L.T, np.linalg.solve(L, y_obs))
        mu    = K_star @ alpha

        v     = np.linalg.solve(L, K_star.T)
        var   = 1.0 - np.sum(v ** 2, axis=0)
        std   = np.sqrt(np.maximum(var, 0))
    except np.linalg.LinAlgError:
        # Fallback to simple mean if Cholesky fails
        mu  = np.full(len(X_cand), float(np.mean(y_obs)))
        std = np.ones(len(X_cand)) * 0.1

    return mu + beta * std


def run_bayesian_optimization_p3() -> dict:
    """
    Run one round of Bayesian parameter optimization using real signal outcomes.
    Returns: {"params": dict, "predicted_winrate": float, "n_obs": int, "round": int}
    """
    np = _np()
    signals = _p3_load_perf_signals()

    # Only use resolved 4h signals with params snapshot
    resolved = [
        s for s in signals
        if s.get("outcome") in ("win", "loss")
        and isinstance(s.get("cfg_snapshot", {}).get("params"), dict)
    ]

    if len(resolved) < 10:
        return {"status": "insufficient_data", "n_obs": len(resolved)}

    # Build observation matrix
    X_list, y_list = [], []
    for sig in resolved:
        params = sig["cfg_snapshot"]["params"]
        try:
            x = _p3_normalize_params(params, P3_PARAM_SPACE)
            y = 1.0 if sig["outcome"] == "win" else 0.0
            X_list.append(x)
            y_list.append(y)
        except Exception:
            continue

    if len(X_list) < 5:
        return {"status": "insufficient_data", "n_obs": len(X_list)}

    X_obs = np.array(X_list)
    y_obs = np.array(y_list)

    # Normalize y to mean ≈ 0 for GP stability
    y_mean = float(np.mean(y_obs))
    y_obs_centered = y_obs - y_mean

    # Generate 500 random candidates
    candidates = [_p3_sample_candidate(P3_PARAM_SPACE) for _ in range(500)]
    X_cand = np.array([_p3_normalize_params(c, P3_PARAM_SPACE) for c in candidates])

    # Compute UCB acquisition
    acq = _p3_gp_ucb(X_obs, y_obs_centered, X_cand)
    best_idx = int(np.argmax(acq))
    best_params = candidates[best_idx]
    predicted_wr = float(y_mean + (acq[best_idx] - float(np.mean(acq))) * 0.1)
    predicted_wr = round(min(max(predicted_wr, 0.0), 1.0), 3)

    # Determine round number
    optim_log = _p3_load_optim_log()
    round_num = len(optim_log) + 1

    entry = {
        "ts":           datetime.now().isoformat()[:19],
        "round":        round_num,
        "n_obs":        len(X_obs),
        "best_params":  best_params,
        "predicted_wr": predicted_wr,
        "baseline_wr":  round(y_mean, 3),
    }
    _p3_append_optim_log(entry)
    _append_evolution_log({**entry, "type": "p3_bayesian_optimization"})

    logger.info(
        "P3 Bayesian opt round %d: n_obs=%d predicted_wr=%.1f%% baseline=%.1f%%",
        round_num, len(X_obs), predicted_wr * 100, y_mean * 100,
    )
    return {
        "status":         "ok",
        "params":         best_params,
        "predicted_wr":   predicted_wr,
        "baseline_wr":    y_mean,
        "n_obs":          len(X_obs),
        "round":          round_num,
    }


# ── A/B Test management ───────────────────────────────────────────────────────

def start_ab_test_p3(old_params: dict, new_params: dict) -> None:
    """Begin a new A/B test. Phase A uses existing config; phase B uses new_params."""
    state = _p3_load_ab()
    state.update({
        "phase":      "A",
        "a_params":   old_params,
        "b_params":   new_params,
        "a_signals":  [],
        "b_signals":  [],
        "round_number": state.get("round_number", 0) + 1,
        "start_ts":   time.time(),
    })
    _p3_save_ab(state)
    logger.info("P3 A/B test started (round %d), A=old params, collecting %d signals",
                state["round_number"], P3_AB_SIGNALS_EACH)


def record_ab_signal_p3(signal_id: str, outcome: Optional[str]) -> None:
    """Record a signal outcome into the active A/B leg."""
    state = _p3_load_ab()
    phase = state.get("phase")
    if phase not in ("A", "B"):
        return

    rec = {"id": signal_id, "outcome": outcome, "ts": time.time()}
    _MAX_AB_SIGNALS = P3_AB_SIGNALS_EACH * 3  # hard cap for safety
    if phase == "A":
        state["a_signals"].append(rec)
        if len(state["a_signals"]) > _MAX_AB_SIGNALS:
            state["a_signals"] = state["a_signals"][-_MAX_AB_SIGNALS:]
        # Transition to B after collecting enough
        resolved_a = [s for s in state["a_signals"] if s["outcome"] in ("win", "loss")]
        if len(resolved_a) >= P3_AB_SIGNALS_EACH:
            state["phase"] = "B"
            # Apply B params to signal engine config
            try:
                b = state["b_params"]
                if b:
                    cfg = _load_config()
                    cfg.update(b)
                    _save_config(cfg)
                    logger.info("P3 A/B: transitioned to phase B, applied new params")
            except Exception as e:
                logger.error("P3 A/B: failed to apply B params: %s", e)
    else:  # phase == "B"
        state["b_signals"].append(rec)
        if len(state["b_signals"]) > _MAX_AB_SIGNALS:
            state["b_signals"] = state["b_signals"][-_MAX_AB_SIGNALS:]
        # Evaluate when B leg is complete
        resolved_b = [s for s in state["b_signals"] if s["outcome"] in ("win", "loss")]
        if len(resolved_b) >= P3_AB_SIGNALS_EACH:
            state["phase"] = "evaluating"

    _p3_save_ab(state)


def evaluate_ab_test_p3() -> Optional[dict]:
    """
    If A/B test has enough data, determine winner and return result.
    Returns None if not ready yet.
    """
    state = _p3_load_ab()
    if state.get("phase") != "evaluating":
        return None

    def _wr(sigs):
        resolved = [s for s in sigs if s["outcome"] in ("win", "loss")]
        if not resolved:
            return 0.0, 0
        wins = sum(1 for s in resolved if s["outcome"] == "win")
        return wins / len(resolved), len(resolved)

    a_wr, a_n = _wr(state["a_signals"])
    b_wr, b_n = _wr(state["b_signals"])

    winner = "B" if b_wr > a_wr else "A"
    winner_params = state["b_params"] if winner == "B" else state["a_params"]
    delta = round((b_wr - a_wr) * 100, 1)

    result = {
        "winner": winner,
        "a_winrate": round(a_wr * 100, 1),
        "b_winrate": round(b_wr * 100, 1),
        "a_n": a_n,
        "b_n": b_n,
        "delta_pct": delta,
        "winner_params": winner_params,
        "round": state.get("round_number", 0),
        "ts": datetime.now().isoformat()[:19],
    }

    # Apply winner params
    apply_ab_winner_p3(winner_params)

    state["phase"]         = "idle"
    state["last_eval_ts"]  = time.time()
    _p3_save_ab(state)

    _p3_append_optim_log({**result, "type": "p3_ab_result"})
    _append_evolution_log({**result, "type": "p3_ab_result"})

    logger.info(
        "P3 A/B test round %d complete: winner=%s A=%.1f%% B=%.1f%% delta=%+.1f%%",
        result["round"], winner, a_wr * 100, b_wr * 100, delta,
    )
    return result


def apply_ab_winner_p3(winner_params: dict) -> None:
    """Write winning params to signal engine config."""
    try:
        cfg = _load_config()
        cfg.update(winner_params)
        # backup before writing
        shutil.copy2(CONFIG_FILE, CONFIG_BACKUP_FILE) if os.path.exists(CONFIG_FILE) else None
        _save_config(cfg)
        logger.info("P3: applied winner params to _signal_engine_config.json")
    except Exception as e:
        logger.error("P3: apply_ab_winner_p3 failed: %s", e)


def get_ab_phase() -> str:
    """Return current A/B test phase."""
    return _p3_load_ab().get("phase", "idle")


# ── Win-rate trend helpers ────────────────────────────────────────────────────

def _p3_weekly_winrates(signals: list, n_weeks: int = 5) -> list:
    """
    Returns list of (week_label, win_rate_pct, wins, total) for past n_weeks.
    Most recent entry last.
    """
    now = time.time()
    weeks = []
    for i in range(n_weeks - 1, -1, -1):
        start_ts = now - (i + 1) * 7 * 86400
        end_ts   = now - i * 7 * 86400
        week_sigs = [
            s for s in signals
            if start_ts <= s.get("timestamp", 0) < end_ts
            and s.get("outcome") in ("win", "loss")
        ]
        wins  = sum(1 for s in week_sigs if s["outcome"] == "win")
        total = len(week_sigs)
        wr    = round(wins / total * 100, 1) if total else None
        label = "本周" if i == 0 else f"-{i}周"
        weeks.append((label, wr, wins, total))
    return weeks


def _p3_ascii_bar(pct: Optional[float], width: int = 20) -> str:
    if pct is None:
        return "░" * width + " (无数据)"
    filled = round(pct / 100 * width)
    return "█" * filled + "░" * (width - filled)


# ── Public reporting functions ────────────────────────────────────────────────

def format_performance_report() -> str:
    """
    Format /performance command output.
    Shows: ASCII win-rate trend, current param version, optimization rounds.
    """
    signals   = _p3_load_perf_signals()
    optim_log = _p3_load_optim_log()
    ab_state  = _p3_load_ab()
    cfg       = _load_config()

    lines = ["📊 **策略优化性能报告**\n"]

    # ── Section 1: Win-rate trend ──────────────────────────────────────────
    lines.append("━━━ 胜率趋势 (周) ━━━")
    weeks = _p3_weekly_winrates(signals, n_weeks=5)
    for label, wr, wins, total in weeks:
        bar = _p3_ascii_bar(wr)
        if wr is not None:
            wr_str = f"{wr:5.1f}%"
            suffix = f"  ({wins}/{total})"
        else:
            wr_str = " N/A  "
            suffix = "  (0 信号)"
        lines.append(f"{label:>4}  {wr_str}  {bar}{suffix}")

    # Overall stats
    resolved = [s for s in signals if s.get("outcome") in ("win", "loss")]
    if resolved:
        total_wins = sum(1 for s in resolved if s["outcome"] == "win")
        overall_wr = round(total_wins / len(resolved) * 100, 1)

        res_24h = [s for s in signals if s.get("outcome_24h") in ("win", "loss")]
        res_72h = [s for s in signals if s.get("outcome_72h") in ("win", "loss")]
        wr_24h = round(sum(1 for s in res_24h if s["outcome_24h"] == "win") /
                       len(res_24h) * 100, 1) if res_24h else None
        wr_72h = round(sum(1 for s in res_72h if s["outcome_72h"] == "win") /
                       len(res_72h) * 100, 1) if res_72h else None

        lines.append(f"\n  总计 4h准确率: {overall_wr}%  ({total_wins}/{len(resolved)})")
        if wr_24h is not None:
            lines.append(f"  24h准确率: {wr_24h}%  ({len(res_24h)}条)")
        if wr_72h is not None:
            lines.append(f"  72h准确率: {wr_72h}%  ({len(res_72h)}条)")
    else:
        lines.append("\n  暂无已结算信号")

    # ── Section 2: Current params ──────────────────────────────────────────
    lines.append("\n━━━ 当前参数版本 ━━━")
    ba_round = len([e for e in optim_log if e.get("type") == "p3_bayesian_optimization"])
    ab_round = len([e for e in optim_log if e.get("type") == "p3_ab_result"])
    lines.append(f"  贝叶斯优化版本: v{ba_round}")
    lines.append(f"  RSI: 超卖={cfg.get('rsi_oversold', 30)}"
                 f"  超买={cfg.get('rsi_overbought', 70)}"
                 f"  周期={cfg.get('rsi_period', 14)}")
    lines.append(f"  MA: 快={cfg.get('ma_fast', 9)}  慢={cfg.get('ma_slow', 21)}")
    lines.append(f"  置信度阈值: {cfg.get('confidence_threshold', 70)}")
    lines.append(f"  信号分数阈值: {cfg.get('signal_score_threshold', 2)}")

    # ── Section 3: Optimization stats ─────────────────────────────────────
    lines.append("\n━━━ 优化统计 ━━━")
    lines.append(f"  累计贝叶斯优化: {ba_round} 轮")
    lines.append(f"  A/B测试: {ab_round} 次完成")

    # Last A/B result
    ab_results = [e for e in optim_log if e.get("type") == "p3_ab_result"]
    if ab_results:
        last = ab_results[-1]
        w    = last.get("winner", "?")
        d    = last.get("delta_pct", 0)
        sign = "+" if d >= 0 else ""
        lines.append(f"  最近A/B: {w}胜  胜率差 {sign}{d}%")

    # Current A/B phase
    phase = ab_state.get("phase", "idle")
    if phase != "idle":
        a_n = len([s for s in ab_state.get("a_signals", [])
                   if s.get("outcome") in ("win", "loss")])
        b_n = len([s for s in ab_state.get("b_signals", [])
                   if s.get("outcome") in ("win", "loss")])
        if phase == "A":
            lines.append(f"  🔬 A/B进行中: A组 {a_n}/{P3_AB_SIGNALS_EACH} 信号结算中")
        elif phase == "B":
            lines.append(f"  🔬 A/B进行中: B组 {b_n}/{P3_AB_SIGNALS_EACH} 信号结算中")

    # Next optimization trigger
    total_resolved = len(resolved)
    next_trigger   = ((total_resolved // P3_OPTIM_TRIGGER) + 1) * P3_OPTIM_TRIGGER
    remaining      = next_trigger - total_resolved
    lines.append(f"\n  下次优化: 再结算 {remaining} 条信号 (当前已结算 {total_resolved})")

    result = "\n".join(lines)
    if len(result) > 4000:
        result = result[:3950] + "\n\n... (截断)"
    return result


def format_daily_performance_summary() -> str:
    """Format daily performance summary for Telegram push."""
    signals   = _p3_load_perf_signals()
    optim_log = _p3_load_optim_log()
    now       = time.time()

    # Today's stats
    today_cutoff = now - 86400
    today_sigs   = [s for s in signals if s.get("timestamp", 0) >= today_cutoff]
    today_res    = [s for s in today_sigs if s.get("outcome") in ("win", "loss")]
    today_wins   = sum(1 for s in today_res if s["outcome"] == "win")
    today_wr     = round(today_wins / len(today_res) * 100, 1) if today_res else None

    # 7-day stats
    week_cutoff = now - 7 * 86400
    week_res    = [s for s in signals
                   if s.get("timestamp", 0) >= week_cutoff and s.get("outcome") in ("win", "loss")]
    week_wins   = sum(1 for s in week_res if s["outcome"] == "win")
    week_wr     = round(week_wins / len(week_res) * 100, 1) if week_res else None

    date_str = datetime.now().strftime("%Y-%m-%d")
    lines = [f"📈 **每日信号报告 — {date_str}**\n"]

    emoji_wr = lambda wr: "🟢" if wr and wr >= 60 else ("🟡" if wr and wr >= 50 else "🔴")

    lines.append("**今日信号**")
    lines.append(f"  总信号: {len(today_sigs)}  已结算: {len(today_res)}")
    if today_wr is not None:
        lines.append(f"  准确率: {emoji_wr(today_wr)} {today_wr}%  ({today_wins}胜/{len(today_res)-today_wins}负)")
    else:
        lines.append("  准确率: ⏳ 暂无已结算")

    lines.append("\n**7日汇总**")
    lines.append(f"  已结算: {len(week_res)}")
    if week_wr is not None:
        lines.append(f"  准确率: {emoji_wr(week_wr)} {week_wr}%  ({week_wins}胜/{len(week_res)-week_wins}负)")

    # Last optimization round
    ba_rounds = [e for e in optim_log if e.get("type") == "p3_bayesian_optimization"]
    if ba_rounds:
        last_opt = ba_rounds[-1]
        lines.append(f"\n**最近优化**")
        lines.append(f"  第{last_opt.get('round', '?')}轮  [{last_opt.get('ts', '')[:10]}]")
        lines.append(f"  基线胜率: {round(last_opt.get('baseline_wr', 0) * 100, 1)}%")
        lines.append(f"  预测胜率: {round(last_opt.get('predicted_wr', 0) * 100, 1)}%")

    total_ba = len(ba_rounds)
    total_ab = len([e for e in optim_log if e.get("type") == "p3_ab_result"])
    lines.append(f"\n  贝叶斯优化 {total_ba} 轮  |  A/B测试 {total_ab} 次")

    result = "\n".join(lines)
    if len(result) > 4000:
        result = result[:3950] + "\n\n... (截断)"
    return result


# ── PerformanceOptimizer background task ─────────────────────────────────────

class PerformanceOptimizer:
    """
    P3_20: Background optimizer.
    - Every 4h: update signal outcomes + check A/B test completion
    - Every 100 resolved signals: run Bayesian opt + start A/B test
    - Daily (09:00): send performance summary
    """

    _DAILY_PUSH_FILE = os.path.join(BASE_DIR, ".p3_last_daily_push")

    def __init__(self, notify_func=None):
        self._notify  = notify_func
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._last_optim_count = 0
        self._last_daily_push  = self._load_last_daily_push()

    # ── persist daily push timestamp across restarts ──
    def _load_last_daily_push(self) -> float:
        try:
            with open(self._DAILY_PUSH_FILE, "r", encoding="utf-8") as f:
                return float(f.read().strip())
        except Exception:
            return 0.0

    def _save_last_daily_push(self, ts: float) -> None:
        try:
            with open(self._DAILY_PUSH_FILE, "w", encoding="utf-8") as f:
                f.write(str(ts))
        except Exception:
            pass

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task    = asyncio.create_task(self._loop(), name="performance_optimizer_p3")
        self._task.add_done_callback(self._on_done)
        logger.info("PerformanceOptimizer (P3_20) started")

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
                logger.error("PerformanceOptimizer crashed: %s", e, exc_info=True)

    async def _loop(self) -> None:
        await asyncio.sleep(120)  # startup delay
        while self._running:
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("PerformanceOptimizer tick error: %s", e)
            try:
                await asyncio.sleep(4 * 3600)  # check every 4h
            except asyncio.CancelledError:
                break

    async def _tick(self) -> None:
        now = time.time()

        # 1. Check A/B test completion
        ab_result = evaluate_ab_test_p3()
        if ab_result and self._notify:
            w     = ab_result["winner"]
            a_wr  = ab_result["a_winrate"]
            b_wr  = ab_result["b_winrate"]
            delta = ab_result["delta_pct"]
            sign  = "+" if delta >= 0 else ""
            msg = (
                f"🔬 **A/B测试完成 (第{ab_result['round']}轮)**\n"
                f"胜者: **{w}组**  胜率差: {sign}{delta}%\n"
                f"A组: {a_wr}% ({ab_result['a_n']}条)\n"
                f"B组: {b_wr}% ({ab_result['b_n']}条)\n"
                f"✅ 已应用{w}组参数"
            )
            await self._notify(msg)

        # 2. Check if optimization should trigger
        signals   = _p3_load_perf_signals()
        resolved  = [s for s in signals if s.get("outcome") in ("win", "loss")]
        n_res     = len(resolved)
        threshold = ((n_res // P3_OPTIM_TRIGGER)) * P3_OPTIM_TRIGGER

        if (n_res >= P3_OPTIM_TRIGGER
                and threshold > self._last_optim_count
                and get_ab_phase() == "idle"):
            self._last_optim_count = threshold

            logger.info("P3: triggering Bayesian optimization at %d resolved signals", n_res)
            loop   = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, run_bayesian_optimization_p3)

            if result.get("status") == "ok":
                # Start A/B test
                old_params = {
                    k: _load_config().get(k)
                    for k in P3_PARAM_SPACE
                }
                start_ab_test_p3(old_params, result["params"])

                if self._notify:
                    pred_wr = round(result.get("predicted_wr", 0) * 100, 1)
                    base_wr = round(result.get("baseline_wr", 0) * 100, 1)
                    msg = (
                        f"🧪 **贝叶斯优化 第{result['round']}轮完成**\n"
                        f"观测数据: {result['n_obs']} 条信号\n"
                        f"基线胜率: {base_wr}%  →  预测胜率: {pred_wr}%\n"
                        f"新参数: {json.dumps(result['params'], ensure_ascii=False)}\n"
                        f"📊 A/B测试已启动，各收集 {P3_AB_SIGNALS_EACH} 条信号"
                    )
                    await self._notify(msg)
            else:
                logger.info("P3: Bayesian opt skipped: %s", result.get("status"))

        # 3. Daily push at 09:00 (UTC+8 approximate: check once per day)
        # NOTE: Hardcoded UTC+8 assumption. To change timezone, adjust the
        # offset below: offset_hours = desired_hour - 8 (e.g., UTC+3 → -5*3600)
        today_9am = (now // 86400) * 86400 + 1 * 3600  # 01:00 UTC = 09:00 UTC+8
        if (now >= today_9am
                and self._last_daily_push < today_9am
                and self._notify):
            self._last_daily_push = now
            self._save_last_daily_push(now)
            try:
                # Skip sending if there's no data at all
                signals = _p3_load_perf_signals()
                if not signals:
                    logger.info("P3: daily push skipped — no signals yet")
                else:
                    summary = format_daily_performance_summary()
                    await self._notify(summary)
            except Exception as e:
                logger.error("P3: daily push failed: %s", e)

    async def run_now(self) -> dict:
        """Manually trigger a Bayesian optimization round."""
        loop   = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, run_bayesian_optimization_p3)
        return result

    @property
    def running(self) -> bool:
        return self._running


# Module-level P3 singleton
performance_optimizer = PerformanceOptimizer()


# ═══════════════════════════════════════════════════════════════════════════════
# P3_24: GENETIC ALGORITHM PARAMETER OPTIMIZER
# ═══════════════════════════════════════════════════════════════════════════════

OPTIMIZED_PARAMS_FILE = os.path.join(BASE_DIR, ".optimized_params.json")
GA24_SIGNAL_TRIGGER   = 100   # re-optimize every N resolved signals

# Chromosome: 4 genes
GA24_PARAM_SPACE = {
    "ma_period":     {"type": "int",   "min": 5,   "max": 200},
    "rsi_threshold": {"type": "int",   "min": 20,  "max": 80},
    "vol_multiplier": {"type": "float", "min": 1.5, "max": 5.0},
    "hold_hours":    {"type": "int",   "min": 1,   "max": 48},
}

GA24_POPULATION_SIZE = 50
GA24_GENERATIONS     = 20
GA24_CROSSOVER_RATE  = 0.7
GA24_MUTATION_RATE   = 0.1


def _ga24_sample() -> dict:
    return {k: _sample_param(v) for k, v in GA24_PARAM_SPACE.items()}


def _ga24_crossover(p1: dict, p2: dict) -> dict:
    """Uniform crossover with GA24_CROSSOVER_RATE gene-swap probability."""
    return {
        k: (p1[k] if random.random() < GA24_CROSSOVER_RATE else p2[k])
        for k in GA24_PARAM_SPACE
    }


def _ga24_mutate(params: dict) -> dict:
    mutated = dict(params)
    for k, spec in GA24_PARAM_SPACE.items():
        if random.random() < GA24_MUTATION_RATE:
            lo, hi = spec["min"], spec["max"]
            if spec["type"] == "int":
                step = max(1, int((hi - lo) * 0.10))
                mutated[k] = max(int(lo), min(int(hi),
                    int(params[k]) + random.randint(-step, step)))
            else:
                step = (hi - lo) * 0.10
                mutated[k] = round(max(lo, min(hi,
                    float(params[k]) + random.uniform(-step, step))), 2)
    return mutated


def _ga24_load_fitness_data() -> list:
    """
    Load resolved signals for fitness evaluation.
    Primary: .signal_performance.json outcomes.
    Secondary: .experiments.jsonl backtest entries.
    """
    signals = []
    try:
        with open(PERF_FILE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        signals = [s for s in data.get("signals", [])
                   if s.get("outcome") in ("win", "loss")]
    except Exception:
        pass
    # Supplement with any backtest experiments (cap to last 2000 lines)
    try:
        exp_path = os.path.join(BASE_DIR, ".experiments.jsonl")
        with open(exp_path, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
        tail_lines = all_lines[-2000:] if len(all_lines) > 2000 else all_lines
        for line in tail_lines:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if obj.get("type") == "backtest" and obj.get("outcome") in ("win", "loss"):
                    signals.append(obj)
            except Exception:
                continue
    except Exception:
        pass
    # Hard cap to prevent unbounded memory
    if len(signals) > 5000:
        signals = signals[-5000:]
    return signals


def _ga24_evaluate(chromosome: dict, signals: list) -> float:
    """
    Fitness = win_rate on signals filtered by chromosome params,
    with a small coverage bonus to prevent over-filtering.

    Filter logic:
    - RSI: long signals where rsi < rsi_threshold;
           short signals where rsi > (100 - rsi_threshold)
    - Vol:  confidence >= 50 + (vol_multiplier - 1.5) * 10
    - Hold: chooses 4h / 24h / 72h outcome based on hold_hours
    """
    if not signals:
        return 0.0

    rsi_thresh = chromosome["rsi_threshold"]
    vol_mult   = chromosome["vol_multiplier"]
    hold_h     = chromosome["hold_hours"]

    if hold_h <= 8:
        outcome_key = "outcome"
    elif hold_h <= 36:
        outcome_key = "outcome_24h"
    else:
        outcome_key = "outcome_72h"

    conf_threshold = min(50.0 + (vol_mult - 1.5) * 10.0, 90.0)

    filtered = []
    for sig in signals:
        rsi       = sig.get("rsi")
        conf      = sig.get("confidence", 0)
        direction = sig.get("direction", "")

        rsi_ok = True
        if rsi is not None:
            if direction == "long":
                rsi_ok = rsi < rsi_thresh
            elif direction == "short":
                rsi_ok = rsi > (100 - rsi_thresh)

        vol_ok  = conf >= conf_threshold
        outcome = sig.get(outcome_key)
        if rsi_ok and vol_ok and outcome in ("win", "loss"):
            filtered.append(sig)

    if len(filtered) < 3:
        return 0.05 * len(filtered) / max(len(signals), 1)

    wins       = sum(1 for s in filtered if s.get(outcome_key) == "win")
    win_rate   = wins / len(filtered) if filtered else 0
    coverage   = len(filtered) / max(len(signals), 1)
    bonus      = min(coverage * 0.2, 0.10)
    return round(win_rate + bonus, 4)


def run_ga24_evolution(trigger: str = "auto") -> dict:
    """
    Full GA cycle: GA24_GENERATIONS generations, GA24_POPULATION_SIZE individuals.
    Returns result dict; writes best params to .optimized_params.json.
    """
    signals = _ga24_load_fitness_data()

    # Load previous best for comparison
    old_params  = {}
    old_fitness = 0.0
    try:
        if os.path.exists(OPTIMIZED_PARAMS_FILE):
            with open(OPTIMIZED_PARAMS_FILE, "r", encoding="utf-8") as f:
                stored = json.load(f)
            old_params  = stored.get("params", {})
            old_fitness = stored.get("fitness", 0.0)
    except Exception:
        pass

    # Initialize population (seed with old best if available)
    population = [_ga24_sample() for _ in range(GA24_POPULATION_SIZE)]
    if old_params:
        seeded = {k: old_params.get(k, _sample_param(v))
                  for k, v in GA24_PARAM_SPACE.items()}
        population[0] = seeded

    best_fitness  = 0.0
    best_params   = population[0]
    gen_bests     = []
    ts_str        = datetime.now().isoformat()[:19]

    for _gen in range(GA24_GENERATIONS):
        scored = [(ch, _ga24_evaluate(ch, signals)) for ch in population]
        scored.sort(key=lambda x: x[1], reverse=True)

        top_fit = scored[0][1]
        gen_bests.append(top_fit)

        if top_fit > best_fitness:
            best_fitness = top_fit
            best_params  = scored[0][0]

        # Elitism: keep top 50%
        survivors = [ch for ch, _ in scored[:GA24_POPULATION_SIZE // 2]]

        # Breed next generation
        new_pop = list(survivors)
        while len(new_pop) < GA24_POPULATION_SIZE:
            if len(survivors) >= 2 and random.random() < GA24_CROSSOVER_RATE:
                p1, p2 = random.sample(survivors, 2)
                child  = _ga24_crossover(p1, p2)
            else:
                child = random.choice(survivors)
            new_pop.append(_ga24_mutate(child))
        population = new_pop

    # Persist best params
    _save_json(OPTIMIZED_PARAMS_FILE, {
        "params":     best_params,
        "fitness":    round(best_fitness, 4),
        "updated_at": ts_str,
    })

    fitness_improvement = round(best_fitness - old_fitness, 4)

    _append_evolution_log({
        "ts":                  ts_str,
        "type":                "p3_24_ga_optimization",
        "trigger":             trigger,
        "generations":         GA24_GENERATIONS,
        "population_size":     GA24_POPULATION_SIZE,
        "n_signals":           len(signals),
        "best_params":         best_params,
        "best_fitness":        round(best_fitness, 4),
        "old_fitness":         round(old_fitness, 4),
        "fitness_improvement": fitness_improvement,
        "old_params":          old_params,
        "gen_bests":           [round(x, 4) for x in gen_bests],
    })

    logger.info(
        "P3_24 GA: best_fitness=%.4f improvement=%+.4f n_signals=%d (trigger=%s)",
        best_fitness, fitness_improvement, len(signals), trigger,
    )
    return {
        "status":              "ok",
        "best_params":         best_params,
        "old_params":          old_params,
        "best_fitness":        round(best_fitness, 4),
        "old_fitness":         round(old_fitness, 4),
        "fitness_improvement": fitness_improvement,
        "n_signals":           len(signals),
        "timestamp":           ts_str,
    }


def format_ga24_result(result: dict) -> str:
    """Format GeneticOptimizer result for Telegram."""
    if result.get("status") != "ok":
        return f"❌ GA优化失败: {result.get('error', '未知错误')}"

    bp  = result["best_params"]
    op  = result.get("old_params", {})
    imp = result.get("fitness_improvement", 0)
    sign = "+" if imp >= 0 else ""

    lines = [
        "🧬 **ML参数自优化完成 (P3_24)**",
        f"信号样本: {result.get('n_signals', 0)} 条",
        f"适应度: {result.get('old_fitness', 0):.4f} → {result.get('best_fitness', 0):.4f}"
        f"  ({sign}{imp:.4f})",
        "",
        "**最优参数:**",
        f"  MA周期:    {bp.get('ma_period', '?')}",
        f"  RSI阈值:   {bp.get('rsi_threshold', '?')}",
        f"  成交量倍数: {bp.get('vol_multiplier', '?')}",
        f"  持仓时间:  {bp.get('hold_hours', '?')}h",
    ]

    if op:
        lines.append("")
        lines.append("**参数变化 (旧 → 新):**")
        labels = [
            ("ma_period",     "MA周期"),
            ("rsi_threshold", "RSI阈值"),
            ("vol_multiplier","成交量倍数"),
            ("hold_hours",    "持仓时间"),
        ]
        for k, label in labels:
            old_v = op.get(k, "N/A")
            new_v = bp.get(k, "N/A")
            arrow = "→" if old_v != new_v else "="
            lines.append(f"  {label}: {old_v} {arrow} {new_v}")

    lines.append("\n✅ 已写入 .optimized_params.json")
    result = "\n".join(lines)
    if len(result) > 4000:
        result = result[:3950] + "\n\n... (截断)"
    return result


class GeneticOptimizer:
    """
    P3_24: Background GA optimizer.
    - Auto-triggers run_ga24_evolution every GA24_SIGNAL_TRIGGER resolved signals.
    - Exposes optimize_now() for manual /optimize ga invocation.
    """

    def __init__(self, notify_func=None):
        self._notify      = notify_func
        self._running     = False
        self._task: Optional[asyncio.Task] = None
        self._last_count  = 0

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        # Seed counter from existing data so we don't fire immediately on restart
        try:
            sigs = _ga24_load_fitness_data()
            self._last_count = (len(sigs) // GA24_SIGNAL_TRIGGER) * GA24_SIGNAL_TRIGGER
        except Exception:
            pass
        self._task = asyncio.create_task(self._loop(), name="genetic_optimizer_p3_24")
        self._task.add_done_callback(self._on_done)
        logger.info("GeneticOptimizer (P3_24) started")

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
                logger.error("GeneticOptimizer crashed: %s", e, exc_info=True)

    async def _loop(self) -> None:
        await asyncio.sleep(600)   # 10 min startup delay
        while self._running:
            try:
                await self._check_auto_trigger()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("GeneticOptimizer check error: %s", e)
            try:
                await asyncio.sleep(3600)   # poll every hour
            except asyncio.CancelledError:
                break

    async def _check_auto_trigger(self) -> None:
        """Fire a GA run if GA24_SIGNAL_TRIGGER new signals have resolved."""
        sigs      = _ga24_load_fitness_data()
        n         = len(sigs)
        threshold = (n // GA24_SIGNAL_TRIGGER) * GA24_SIGNAL_TRIGGER
        if n >= GA24_SIGNAL_TRIGGER and threshold > self._last_count:
            self._last_count = threshold
            logger.info("P3_24: auto-trigger GA at %d resolved signals", n)
            result = await self.optimize_now(trigger="auto_100")
            if self._notify and result.get("status") == "ok":
                await self._notify(format_ga24_result(result))

    async def optimize_now(self, trigger: str = "manual") -> dict:
        """Run GA evolution in executor (non-blocking)."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, run_ga24_evolution, trigger)

    @property
    def running(self) -> bool:
        return self._running


# Module-level P3_24 singleton
genetic_optimizer = GeneticOptimizer()
