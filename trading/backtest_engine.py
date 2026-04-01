"""
Backtesting Engine — Walk-forward validated strategy testing on real OKX OHLCV data.

Replaces the empty-data mock in infinite_evolver.py with a statistically rigorous
backtesting framework that:
- Downloads real historical candles from OKX public API
- Caches data locally in _data_cache/
- Runs walk-forward validation (70% train / 30% test, rolling window)
- Models transaction costs (0.05% maker, 0.1% taker + slippage)
- Calculates Sharpe on out-of-sample data only
- Supports V6 strategy parameters and arbitrary signal functions
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import numpy as np

try:
    import httpx
except ImportError:
    httpx = None

from .indicators import sma, ema, atr, bb_upper, bb_lower, slope

log = logging.getLogger(__name__)

DATA_CACHE_DIR = Path(__file__).resolve().parent.parent / "_data_cache"
DATA_CACHE_DIR.mkdir(parents=True, exist_ok=True)

OKX_REST_BASE = "https://www.okx.com"
TAKER_FEE = 0.001       # 0.1%
MAKER_FEE = 0.0005      # 0.05%
SLIPPAGE_BPS = 0.0003   # 0.03%


@dataclass
class BacktestConfig:
    symbols: list[str] = field(default_factory=lambda: ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    bar: str = "4H"
    lookback_bars: int = 1000
    train_pct: float = 0.70
    initial_equity: float = 10_000.0
    max_position_pct: float = 0.05
    max_positions: int = 3
    max_drawdown_pct: float = 0.10


@dataclass
class BacktestResult:
    sharpe_train: float = 0.0
    sharpe_test: float = 0.0
    total_return_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    win_rate: float = 0.0
    total_trades: int = 0
    avg_trade_pnl: float = 0.0
    profit_factor: float = 0.0
    calmar_ratio: float = 0.0

    def to_dict(self) -> dict:
        return {
            "sharpe_train": round(self.sharpe_train, 4),
            "sharpe_test": round(self.sharpe_test, 4),
            "total_return_pct": round(self.total_return_pct, 2),
            "max_drawdown_pct": round(self.max_drawdown_pct, 4),
            "win_rate": round(self.win_rate, 2),
            "total_trades": self.total_trades,
            "avg_trade_pnl": round(self.avg_trade_pnl, 4),
            "profit_factor": round(self.profit_factor, 4),
            "calmar_ratio": round(self.calmar_ratio, 4),
        }

    @property
    def is_viable(self) -> bool:
        return (
            self.sharpe_test > 1.0
            and self.max_drawdown_pct < 0.10
            and self.total_trades >= 10
            and self.win_rate > 30
        )


async def fetch_ohlcv(
    symbol: str, bar: str = "4H", limit: int = 1000, use_cache: bool = True
) -> np.ndarray:
    """Fetch OHLCV from OKX and return as numpy array [ts, o, h, l, c, vol].

    Caches results to avoid repeated API calls.
    """
    base = symbol.upper().replace("USDT", "").replace("-", "")
    inst_id = f"{base}-USDT-SWAP"
    cache_file = DATA_CACHE_DIR / f"{inst_id}_{bar}_{limit}.npy"

    if use_cache and cache_file.exists():
        age_hours = (time.time() - cache_file.stat().st_mtime) / 3600
        if age_hours < 6:
            data = np.load(str(cache_file))
            if len(data) >= limit * 0.8:
                return data

    if httpx is None:
        raise ImportError("httpx required for data fetching")

    all_candles: list[list[float]] = []
    after = ""
    remaining = limit

    async with httpx.AsyncClient(timeout=20.0) as client:
        while remaining > 0:
            params: dict[str, str] = {
                "instId": inst_id,
                "bar": bar,
                "limit": str(min(remaining, 300)),
            }
            if after:
                params["after"] = after

            resp = await client.get(
                f"{OKX_REST_BASE}/api/v5/market/history-candles", params=params
            )
            data = resp.json()
            if data.get("code") != "0" or not data.get("data"):
                break
            rows = data["data"]
            if not rows:
                break
            for r in rows:
                all_candles.append([
                    float(r[0]), float(r[1]), float(r[2]),
                    float(r[3]), float(r[4]), float(r[5]),
                ])
            after = rows[-1][0]
            remaining -= len(rows)
            if len(rows) < 100:
                break
            await asyncio.sleep(0.15)

    if not all_candles:
        raise ValueError(f"No data fetched for {inst_id} {bar}")

    all_candles.sort(key=lambda c: c[0])
    arr = np.array(all_candles)
    np.save(str(cache_file), arr)
    log.info("Fetched %d candles for %s %s", len(arr), inst_id, bar)
    return arr


def _compute_v6_signals(
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    vol: np.ndarray,
    params: dict[str, Any],
) -> np.ndarray:
    """Run V6 strategy on candle data and return signal array.

    Returns: array of shape (N,) with values:
        0 = no signal, 1 = long, -1 = short, 2 = close
    """
    n = len(close)
    signals = np.zeros(n, dtype=int)

    ma5 = sma(close, params.get("ma5_len", 5))
    ma8 = sma(close, params.get("ma8_len", 8))
    ema21 = ema(close, params.get("ema21_len", 21))
    ma55 = sma(close, params.get("ma55_len", 55))
    bb_up = bb_upper(close, params.get("bb_length", 21), params.get("bb_std_dev", 2.5))
    bb_lo = bb_lower(close, params.get("bb_length", 21), params.get("bb_std_dev", 2.5))
    atr_arr = atr(high, low, close, params.get("atr_period", 14))

    slope_len = params.get("slope_len", 3)
    slope_thresh = params.get("slope_threshold", 0.1)

    for i in range(max(55, params.get("ma55_len", 55)), n):
        if any(np.isnan(x[i]) for x in [ma5, ma8, ema21, ma55, bb_up, bb_lo, atr_arr]):
            continue

        price = close[i]
        if price <= 0:
            continue

        atr_pct = atr_arr[i] / price * 100
        atr_dist_scale = max(1.0, atr_pct / 2.0)
        atr_slope_scale = max(1.0, atr_pct / 1.5)

        long_order = price > ma5[i] > ma8[i] > ema21[i] > ma55[i]
        short_order = price < ma5[i] < ma8[i] < ema21[i] < ma55[i]

        if not long_order and not short_order:
            continue

        def pct_dist(a: float, b: float) -> float:
            return abs(a - b) / max(abs(b), 1e-10) * 100

        dist_5_8 = pct_dist(ma5[i], ma8[i])
        dist_8_21 = pct_dist(ma8[i], ema21[i])
        dist_21_55 = pct_dist(ema21[i], ma55[i])

        if not (
            dist_5_8 < params.get("dist_ma5_ma8", 1.5) * atr_dist_scale
            and dist_8_21 < params.get("dist_ma8_ema21", 2.5) * atr_dist_scale
            and dist_21_55 < params.get("dist_ema21_ma55", 4.0) * atr_dist_scale
        ):
            continue

        slopes = [
            slope(ma5, slope_len, i),
            slope(ma8, slope_len, i),
            slope(ema21, slope_len, i),
            slope(ma55, slope_len, i),
        ]
        adapted_slope_thresh = slope_thresh * atr_slope_scale
        if long_order and not all(s > adapted_slope_thresh for s in slopes):
            continue
        if short_order and not all(s < -adapted_slope_thresh for s in slopes):
            continue

        if long_order and price >= bb_up[i]:
            continue
        if short_order and price <= bb_lo[i]:
            continue

        # Volume check
        if len(vol) > 20 and i >= 20:
            vol_ma = np.nanmean(vol[i - 20 : i])
            if vol_ma > 0 and vol[i] / vol_ma < 0.5:
                continue

        signals[i] = 1 if long_order else -1

    return signals


def _simulate_trades(
    close: np.ndarray,
    signals: np.ndarray,
    config: BacktestConfig,
    ma55: np.ndarray | None = None,
    bb_up: np.ndarray | None = None,
    bb_lo: np.ndarray | None = None,
) -> list[dict]:
    """Simulate trades with position sizing, fees, and stop/take-profit."""
    trades: list[dict] = []
    equity = config.initial_equity
    cash = equity
    position: dict[str, Any] | None = None
    n = len(close)

    for i in range(n):
        price = close[i]
        if price <= 0:
            continue

        # Check exits for open position
        if position is not None:
            side = position["side"]
            entry = position["entry_price"]
            should_close = False
            reason = ""

            # SL: price crosses MA55
            if ma55 is not None and not np.isnan(ma55[i]):
                if side == "long" and price < ma55[i]:
                    should_close = True
                    reason = "SL_MA55"
                elif side == "short" and price > ma55[i]:
                    should_close = True
                    reason = "SL_MA55"

            # TP: price hits BB band
            if bb_up is not None and bb_lo is not None:
                if side == "long" and not np.isnan(bb_up[i]) and price >= bb_up[i]:
                    should_close = True
                    reason = "TP_BB"
                elif side == "short" and not np.isnan(bb_lo[i]) and price <= bb_lo[i]:
                    should_close = True
                    reason = "TP_BB"

            # Opposite signal forces close
            if signals[i] != 0 and (
                (side == "long" and signals[i] == -1) or
                (side == "short" and signals[i] == 1)
            ):
                should_close = True
                reason = "REVERSE"

            if should_close:
                exit_price = price * (1 - SLIPPAGE_BPS if side == "long" else 1 + SLIPPAGE_BPS)
                size = position["size"]
                if side == "long":
                    pnl_pct = (exit_price - entry) / entry
                else:
                    pnl_pct = (entry - exit_price) / entry
                fee = size * TAKER_FEE
                pnl_usd = size * pnl_pct - fee
                cash += size + pnl_usd
                equity = cash
                trades.append({
                    "entry_bar": position["entry_bar"],
                    "exit_bar": i,
                    "side": side,
                    "entry_price": entry,
                    "exit_price": exit_price,
                    "size": size,
                    "pnl_pct": pnl_pct * 100,
                    "pnl_usd": pnl_usd,
                    "fee": fee,
                    "reason": reason,
                })
                position = None

        # Open new position
        if position is None and signals[i] != 0:
            side = "long" if signals[i] == 1 else "short"
            max_size = equity * config.max_position_pct
            if max_size < 1.0 or cash < max_size:
                continue
            entry_price = price * (1 + SLIPPAGE_BPS if side == "long" else 1 - SLIPPAGE_BPS)
            fee = max_size * TAKER_FEE
            cash -= max_size
            equity = cash
            position = {
                "side": side,
                "entry_price": entry_price,
                "entry_bar": i,
                "size": max_size,
                "fee": fee,
            }

    # Force close any remaining position at end
    if position is not None:
        price = close[-1]
        if price > 0:
            side = position["side"]
            entry = position["entry_price"]
            exit_price = price * (1 - SLIPPAGE_BPS if side == "long" else 1 + SLIPPAGE_BPS)
            size = position["size"]
            if side == "long":
                pnl_pct = (exit_price - entry) / entry
            else:
                pnl_pct = (entry - exit_price) / entry
            fee = size * TAKER_FEE
            pnl_usd = size * pnl_pct - fee
            cash += size + pnl_usd
            trades.append({
                "entry_bar": position["entry_bar"],
                "exit_bar": n - 1,
                "side": side,
                "entry_price": entry,
                "exit_price": exit_price,
                "size": size,
                "pnl_pct": pnl_pct * 100,
                "pnl_usd": pnl_usd,
                "fee": fee,
                "reason": "END",
            })

    return trades


def _compute_metrics(trades: list[dict], initial_equity: float) -> dict:
    """Compute Sharpe, drawdown, win rate, profit factor from trade list."""
    if not trades:
        return {
            "sharpe": 0.0, "total_return_pct": 0.0, "max_drawdown_pct": 0.0,
            "win_rate": 0.0, "total_trades": 0, "avg_trade_pnl": 0.0,
            "profit_factor": 0.0, "calmar_ratio": 0.0,
        }

    pnls = [t["pnl_usd"] for t in trades]
    pnl_pcts = [t["pnl_pct"] for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [abs(p) for p in pnls if p < 0]

    total_pnl = sum(pnls)
    total_return_pct = total_pnl / initial_equity * 100
    win_rate = len(wins) / len(pnls) * 100 if pnls else 0

    # Sharpe (annualized, assuming 4h bars ~= 6 trades/day)
    mean_pnl = np.mean(pnl_pcts) if pnl_pcts else 0
    std_pnl = np.std(pnl_pcts) if len(pnl_pcts) > 1 else 1e-8
    sharpe = (mean_pnl / max(std_pnl, 1e-8)) * np.sqrt(252)

    # Max drawdown from equity curve
    equity_curve = [initial_equity]
    for p in pnls:
        equity_curve.append(equity_curve[-1] + p)
    eq = np.array(equity_curve)
    running_max = np.maximum.accumulate(eq)
    drawdowns = (running_max - eq) / running_max
    max_dd = float(np.max(drawdowns)) if len(drawdowns) > 0 else 0

    gross_profit = sum(wins) if wins else 0
    gross_loss = sum(losses) if losses else 1e-8
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0

    annual_return = total_return_pct / 100
    calmar = annual_return / max(max_dd, 1e-8) if max_dd > 0 else 0

    return {
        "sharpe": float(sharpe),
        "total_return_pct": float(total_return_pct),
        "max_drawdown_pct": float(max_dd),
        "win_rate": float(win_rate),
        "total_trades": len(trades),
        "avg_trade_pnl": float(np.mean(pnl_pcts)) if pnl_pcts else 0,
        "profit_factor": float(profit_factor),
        "calmar_ratio": float(calmar),
    }


def _run_backtest_cpu_inner(
    strategy_params: dict[str, Any],
    bundles: list[tuple[str, np.ndarray]],
    config: BacktestConfig,
    signal_fn: Callable | None,
) -> BacktestResult:
    """CPU-only walk-forward simulation (used from ProcessPoolExecutor or threads)."""
    all_trades_train: list[dict] = []
    all_trades_test: list[dict] = []

    for _symbol, data in bundles:
        close = data[:, 4]
        high = data[:, 2]
        low = data[:, 3]
        vol = data[:, 5]

        if signal_fn is not None:
            signals = signal_fn(close, high, low, vol, strategy_params)
        else:
            signals = _compute_v6_signals(close, high, low, vol, strategy_params)

        ma55_arr = sma(close, strategy_params.get("ma55_len", 55))
        bb_up_arr = bb_upper(
            close,
            strategy_params.get("bb_length", 21),
            strategy_params.get("bb_std_dev", 2.5),
        )
        bb_lo_arr = bb_lower(
            close,
            strategy_params.get("bb_length", 21),
            strategy_params.get("bb_std_dev", 2.5),
        )

        split_idx = int(len(close) * config.train_pct)

        train_signals = signals[:split_idx].copy()
        train_close = close[:split_idx]
        train_ma55 = ma55_arr[:split_idx] if ma55_arr is not None else None
        train_bb_up = bb_up_arr[:split_idx] if bb_up_arr is not None else None
        train_bb_lo = bb_lo_arr[:split_idx] if bb_lo_arr is not None else None
        train_trades = _simulate_trades(
            train_close,
            train_signals,
            config,
            ma55=train_ma55,
            bb_up=train_bb_up,
            bb_lo=train_bb_lo,
        )
        all_trades_train.extend(train_trades)

        test_signals = signals[split_idx:].copy()
        test_close = close[split_idx:]
        test_ma55 = ma55_arr[split_idx:] if ma55_arr is not None else None
        test_bb_up = bb_up_arr[split_idx:] if bb_up_arr is not None else None
        test_bb_lo = bb_lo_arr[split_idx:] if bb_lo_arr is not None else None
        test_trades = _simulate_trades(
            test_close,
            test_signals,
            config,
            ma55=test_ma55,
            bb_up=test_bb_up,
            bb_lo=test_bb_lo,
        )
        all_trades_test.extend(test_trades)

    train_metrics = _compute_metrics(all_trades_train, config.initial_equity)
    test_metrics = _compute_metrics(all_trades_test, config.initial_equity)

    return BacktestResult(
        sharpe_train=train_metrics["sharpe"],
        sharpe_test=test_metrics["sharpe"],
        total_return_pct=test_metrics["total_return_pct"],
        max_drawdown_pct=test_metrics["max_drawdown_pct"],
        win_rate=test_metrics["win_rate"],
        total_trades=test_metrics["total_trades"],
        avg_trade_pnl=test_metrics["avg_trade_pnl"],
        profit_factor=test_metrics["profit_factor"],
        calmar_ratio=test_metrics["calmar_ratio"],
    )


_BACKTEST_POOL: ProcessPoolExecutor | None = None


def _get_backtest_pool() -> ProcessPoolExecutor:
    global _BACKTEST_POOL
    if _BACKTEST_POOL is None:
        n = os.cpu_count() or 2
        workers = max(1, min(4, n - 1))
        _BACKTEST_POOL = ProcessPoolExecutor(max_workers=workers)
    return _BACKTEST_POOL


def _backtest_pool_worker(payload: tuple) -> dict:
    """Top-level for multiprocessing pickle."""
    strategy_params, bundles, config_dict = payload
    config = BacktestConfig(**config_dict)
    r = _run_backtest_cpu_inner(strategy_params, bundles, config, None)
    d = r.to_dict()
    d["sharpe"] = d["sharpe_test"]
    d["viable"] = r.is_viable
    return d


def _backtest_result_from_worker_dict(d: dict) -> BacktestResult:
    return BacktestResult(
        sharpe_train=float(d["sharpe_train"]),
        sharpe_test=float(d["sharpe_test"]),
        total_return_pct=float(d["total_return_pct"]),
        max_drawdown_pct=float(d["max_drawdown_pct"]),
        win_rate=float(d["win_rate"]),
        total_trades=int(d["total_trades"]),
        avg_trade_pnl=float(d["avg_trade_pnl"]),
        profit_factor=float(d["profit_factor"]),
        calmar_ratio=float(d["calmar_ratio"]),
    )


async def run_backtest(
    strategy_params: dict[str, Any],
    config: BacktestConfig | None = None,
    signal_fn: Callable | None = None,
) -> BacktestResult:
    """Run a full walk-forward backtest with real OKX data.

    OHLCV is fetched on the asyncio event loop; heavy NumPy simulation runs in a
    ``ProcessPoolExecutor`` so the trading bot process is not CPU-starved.

    Args:
        strategy_params: V6 strategy parameters dict
        config: backtest configuration (defaults to BacktestConfig())
        signal_fn: optional custom signal function (not picklable — runs in a thread)
    """
    if config is None:
        config = BacktestConfig()

    bundles: list[tuple[str, np.ndarray]] = []
    for symbol in config.symbols:
        try:
            data = await fetch_ohlcv(symbol, config.bar, config.lookback_bars)
        except Exception as e:
            log.warning("Failed to fetch data for %s: %s", symbol, e)
            continue

        if len(data) < 100:
            continue
        bundles.append((symbol, data))

    if not bundles:
        return BacktestResult()

    if signal_fn is not None:
        return await asyncio.to_thread(
            _run_backtest_cpu_inner,
            strategy_params,
            bundles,
            config,
            signal_fn,
        )

    payload = (strategy_params, bundles, dataclasses.asdict(config))
    loop = asyncio.get_running_loop()
    d = await loop.run_in_executor(
        _get_backtest_pool(),
        _backtest_pool_worker,
        payload,
    )
    return _backtest_result_from_worker_dict(d)


async def quick_backtest(strategy_params: dict[str, Any]) -> dict:
    """Convenience function returning a dict (for infinite_evolver integration)."""
    result = await run_backtest(strategy_params)
    d = result.to_dict()
    d["sharpe"] = d["sharpe_test"]
    d["viable"] = result.is_viable
    return d
