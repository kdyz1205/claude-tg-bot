"""
Alpha Evolver — LLM-driven alpha feature discovery cycle.

Before evaluation, generated code is scanned with ``pipeline.security_ast``; feature
evaluation runs in a ``ProcessPoolExecutor`` so ``exec()`` of LLM output cannot block
the trading process.

Inspired by AlphaQuant (2026) and FinAgent patterns:
1. LLM receives market context (price action, volume, funding rates)
2. LLM proposes a NumPy feature function (momentum, mean-reversion, volatility)
3. Feature is validated via rolling cross-validation
4. Evaluation metrics (IC, Sharpe, turnover) returned to LLM
5. LLM iterates: adjusts feature based on feedback
6. Best features become new alpha signals in the strategy pool

Uses Claude CLI for feature code generation.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
import textwrap
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from .backtest_engine import fetch_ohlcv, BacktestConfig
from .indicators import sma, ema, atr

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ALPHA_LIBRARY = PROJECT_ROOT / "_alpha_library"
ALPHA_LIBRARY.mkdir(parents=True, exist_ok=True)


@dataclass
class AlphaFeature:
    """A discovered alpha feature with its performance metrics."""
    feature_id: str
    name: str
    code: str
    ic: float = 0.0             # Information Coefficient (correlation with future returns)
    sharpe: float = 0.0         # Standalone Sharpe ratio
    turnover: float = 0.0       # Average daily turnover
    win_rate: float = 0.0
    total_trades: int = 0
    generation: int = 0
    parent_id: str = ""
    created_at: float = field(default_factory=time.time)
    status: str = "candidate"   # candidate | validated | promoted | rejected

    def to_dict(self) -> dict:
        return {
            "feature_id": self.feature_id,
            "name": self.name,
            "code": self.code,
            "ic": round(self.ic, 4),
            "sharpe": round(self.sharpe, 4),
            "turnover": round(self.turnover, 4),
            "win_rate": round(self.win_rate, 2),
            "total_trades": self.total_trades,
            "generation": self.generation,
            "parent_id": self.parent_id,
            "created_at": self.created_at,
            "status": self.status,
        }


_FEATURE_TEMPLATE = textwrap.dedent("""\
    import numpy as np

    def alpha_signal(close, high, low, volume, params=None):
        \"\"\"
        Alpha signal function.

        Args:
            close: np.ndarray of close prices
            high: np.ndarray of high prices
            low: np.ndarray of low prices
            volume: np.ndarray of volumes
            params: optional dict of parameters

        Returns:
            np.ndarray of signal values in range [-1, 1]
            Positive = long signal, negative = short signal, 0 = no signal
        \"\"\"
        n = len(close)
        signal = np.zeros(n)

        {FEATURE_CODE}

        return np.clip(signal, -1, 1)
""")

_GENERATION_PROMPT = textwrap.dedent("""\
    You are a quantitative researcher. Generate a NumPy-based alpha signal function.

    CONTEXT:
    - Asset: crypto perpetual contracts (BTC, ETH, SOL)
    - Timeframe: 4-hour candles
    - Available data: close, high, low, volume arrays (numpy)
    - Available helpers: np.mean, np.std, np.roll, np.convolve, np.maximum, np.minimum
    - The function returns values in [-1, 1]: positive = long, negative = short, 0 = no signal

    {FEEDBACK}

    REQUIREMENTS:
    - Pure NumPy only (no pandas, no external libs)
    - Must handle edge cases (NaN, division by zero)
    - Must be vectorized (no Python for-loops over all bars if avoidable)
    - Signal should be non-trivial (not all zeros, not random)
    - Must be a novel idea, not just SMA crossover

    OUTPUT: Only the body of the function (indented 8 spaces), which will be inserted into:
    ```
    def alpha_signal(close, high, low, volume, params=None):
        signal = np.zeros(len(close))
        # YOUR CODE HERE
        return np.clip(signal, -1, 1)
    ```

    Output ONLY the code body. No explanations, no markdown fences, no function signature.
""")


def _evaluate_feature(
    code: str,
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    volume: np.ndarray,
) -> dict:
    """Evaluate an alpha feature on historical data.

    Returns metrics dict or error dict.
    """
    full_code = _FEATURE_TEMPLATE.replace("{FEATURE_CODE}", code)

    try:
        namespace: dict[str, Any] = {"np": np}
        exec(full_code, namespace)
        alpha_fn = namespace["alpha_signal"]
    except Exception as e:
        return {"error": f"Code compilation failed: {e}"}

    try:
        signals = alpha_fn(close, high, low, volume)
    except Exception as e:
        return {"error": f"Execution failed: {e}"}

    if not isinstance(signals, np.ndarray) or len(signals) != len(close):
        return {"error": "Signal must be np.ndarray of same length as close"}

    if np.all(signals == 0) or np.all(np.isnan(signals)):
        return {"error": "Signal is all zeros or NaN"}

    signals = np.nan_to_num(signals, 0.0)

    # Forward returns (4h bar = next bar return)
    returns = np.diff(close) / close[:-1]
    returns = np.append(returns, 0.0)

    # Information Coefficient: rank correlation of signal vs future return
    valid = ~(np.isnan(signals) | np.isnan(returns))
    if valid.sum() < 50:
        return {"error": "Insufficient valid data points"}

    try:
        from scipy.stats import spearmanr
        ic, _ = spearmanr(signals[valid], returns[valid])
    except ImportError:
        sig_v = signals[valid]
        ret_v = returns[valid]
        ic = float(np.corrcoef(sig_v, ret_v)[0, 1])
    ic = float(ic) if not np.isnan(ic) else 0.0

    # Simple long/short PnL simulation
    position = np.sign(signals)
    pnl_per_bar = position[:-1] * returns[:-1]
    pnl_per_bar -= 0.001 * np.abs(np.diff(position[:-1], prepend=0))  # transaction costs

    if len(pnl_per_bar) == 0:
        return {"error": "No PnL data"}

    mean_pnl = float(np.mean(pnl_per_bar))
    std_pnl = float(np.std(pnl_per_bar)) if len(pnl_per_bar) > 1 else 1e-8
    sharpe = (mean_pnl / max(std_pnl, 1e-8)) * np.sqrt(252 * 6)  # annualized for 4h bars

    trades = int(np.sum(np.abs(np.diff(position)) > 0))
    wins = int(np.sum(pnl_per_bar > 0))
    total_bars = len(pnl_per_bar)
    win_rate = wins / max(total_bars, 1) * 100

    turnover = float(np.mean(np.abs(np.diff(position)))) if len(position) > 1 else 0

    total_return = float(np.sum(pnl_per_bar)) * 100

    return {
        "ic": ic,
        "sharpe": float(sharpe),
        "turnover": turnover,
        "win_rate": win_rate,
        "total_trades": trades,
        "total_return_pct": total_return,
        "mean_pnl_per_bar": mean_pnl,
        "bars_evaluated": total_bars,
    }


def _evaluate_feature_worker(args: tuple) -> dict:
    """ProcessPool entry: exec LLM alpha code off the main trading process."""
    code, close_a, high_a, low_a, vol_a = args
    c = np.asarray(close_a)
    h = np.asarray(high_a)
    l = np.asarray(low_a)
    v = np.asarray(vol_a)
    return _evaluate_feature(code, c, h, l, v)


_ALPHA_POOL: ProcessPoolExecutor | None = None


def _get_alpha_pool() -> ProcessPoolExecutor:
    global _ALPHA_POOL
    if _ALPHA_POOL is None:
        n = os.cpu_count() or 2
        workers = max(1, min(3, n - 1))
        _ALPHA_POOL = ProcessPoolExecutor(max_workers=workers)
    return _ALPHA_POOL


def _find_claude() -> str:
    """Find claude CLI executable."""
    import shutil
    for c in [
        shutil.which("claude.cmd"),
        shutil.which("claude"),
        str(Path.home() / "AppData/Roaming/npm/claude.cmd"),
    ]:
        if c and Path(c).is_file():
            return c
    return "claude.cmd"


async def _generate_feature_code(
    feedback: str = "",
    timeout: int = 120,
) -> str | None:
    """Use Claude CLI to generate alpha feature code."""
    prompt = _GENERATION_PROMPT.replace("{FEEDBACK}", feedback)

    try:
        result = await asyncio.to_thread(
            subprocess.run,
            [_find_claude(), "-p", prompt, "--output-format", "text"],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(PROJECT_ROOT),
            encoding="utf-8",
            errors="replace",
        )
        if result.returncode != 0:
            log.warning("Claude CLI failed: %s", (result.stderr or "")[:200])
            return None

        output = result.stdout.strip()
        lines = []
        in_code = False
        for line in output.splitlines():
            stripped = line.strip()
            if stripped.startswith("```"):
                in_code = not in_code
                continue
            if in_code or not stripped.startswith("#"):
                lines.append(line)

        code = "\n".join(lines).strip()
        if not code or len(code) < 20:
            return None
        return code
    except Exception as e:
        log.warning("Feature generation failed: %s", e)
        return None


class AlphaEvolver:
    """LLM-driven alpha feature evolution engine."""

    MAX_ITERATIONS = 5
    MIN_SHARPE = 0.5
    MIN_IC = 0.02
    POPULATION_SIZE = 20

    def __init__(self):
        self._features: list[AlphaFeature] = []
        self._generation = 0
        self._load_library()

    def _load_library(self):
        index_file = ALPHA_LIBRARY / "index.json"
        if index_file.exists():
            try:
                data = json.loads(index_file.read_text(encoding="utf-8"))
                self._features = [
                    AlphaFeature(**f) for f in data.get("features", [])
                ]
                self._generation = data.get("generation", 0)
            except Exception as e:
                log.warning("Failed to load alpha library: %s", e)

    def _save_library(self):
        index_file = ALPHA_LIBRARY / "index.json"
        try:
            data = {
                "generation": self._generation,
                "features": [f.to_dict() for f in self._features[-self.POPULATION_SIZE:]],
                "last_updated": time.time(),
            }
            index_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
        except Exception as e:
            log.warning("Failed to save alpha library: %s", e)

    async def evolve_cycle(self, symbols: list[str] | None = None) -> AlphaFeature | None:
        """Run one evolution cycle: generate → evaluate → iterate → promote.

        Returns the best feature if it passes quality thresholds.
        """
        if symbols is None:
            symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

        # Fetch market data for evaluation
        all_close, all_high, all_low, all_vol = [], [], [], []
        for sym in symbols:
            try:
                data = await fetch_ohlcv(sym, "4H", 500)
                if len(data) >= 100:
                    all_close.append(data[:, 4])
                    all_high.append(data[:, 2])
                    all_low.append(data[:, 3])
                    all_vol.append(data[:, 5])
            except Exception as e:
                log.warning("Failed to fetch data for %s: %s", sym, e)

        if not all_close:
            log.warning("No market data available for alpha evolution")
            return None

        close = np.concatenate(all_close)
        high = np.concatenate(all_high)
        low = np.concatenate(all_low)
        vol = np.concatenate(all_vol)

        best_feature: AlphaFeature | None = None
        best_sharpe = -999.0
        feedback = ""
        self._generation += 1

        for iteration in range(self.MAX_ITERATIONS):
            code = await _generate_feature_code(feedback=feedback)
            if code is None:
                log.info("Iteration %d: no code generated", iteration)
                await asyncio.sleep(5)
                continue

            full_src = _FEATURE_TEMPLATE.replace("{FEATURE_CODE}", code)
            try:
                from pipeline.security_ast import scan_source

                viol = scan_source(full_src, rel_path="alpha_candidate.py")
            except Exception as e:
                log.warning("alpha_evolver: security_ast failed: %s", e)
                viol = []
            if viol:
                v0 = viol[0]
                feedback = (
                    f"security_ast blocked ({v0.rule} line {v0.line}): {v0.detail[:200]}. "
                    "Use only NumPy math on close/high/low/volume — no I/O, imports beyond numpy, or env access."
                )
                log.info("Iteration %d: security_ast rejected: %s", iteration, v0.rule)
                continue

            loop = asyncio.get_running_loop()
            try:
                metrics = await loop.run_in_executor(
                    _get_alpha_pool(),
                    _evaluate_feature_worker,
                    (code, close, high, low, vol),
                )
            except Exception as e:
                metrics = {"error": f"pool execution failed: {e}"}

            if "error" in metrics:
                feedback = f"Previous attempt failed: {metrics['error']}. Try a different approach."
                log.info("Iteration %d: %s", iteration, metrics["error"])
                continue

            feature = AlphaFeature(
                feature_id=f"alpha_{self._generation}_{iteration}",
                name=f"Gen{self._generation} Iter{iteration}",
                code=code,
                ic=metrics["ic"],
                sharpe=metrics["sharpe"],
                turnover=metrics["turnover"],
                win_rate=metrics["win_rate"],
                total_trades=metrics["total_trades"],
                generation=self._generation,
            )

            log.info(
                "Iteration %d: IC=%.4f Sharpe=%.3f WR=%.1f%% Trades=%d",
                iteration, feature.ic, feature.sharpe, feature.win_rate,
                feature.total_trades,
            )

            if feature.sharpe > best_sharpe:
                best_sharpe = feature.sharpe
                best_feature = feature

            # Build feedback for next iteration
            feedback = (
                f"Previous attempt metrics:\n"
                f"- IC (information coefficient): {metrics['ic']:.4f} (want > {self.MIN_IC})\n"
                f"- Sharpe ratio: {metrics['sharpe']:.3f} (want > {self.MIN_SHARPE})\n"
                f"- Win rate: {metrics['win_rate']:.1f}%\n"
                f"- Trades: {metrics['total_trades']}\n"
                f"- Return: {metrics.get('total_return_pct', 0):.2f}%\n"
                f"{'GOOD: Sharpe above threshold!' if metrics['sharpe'] > self.MIN_SHARPE else 'Needs improvement. Try a different signal logic.'}\n"
                f"{'GOOD: IC is meaningful!' if abs(metrics['ic']) > self.MIN_IC else 'IC too low — signal has no predictive power.'}"
            )

        if best_feature and best_feature.sharpe > self.MIN_SHARPE:
            best_feature.status = "validated"
            self._features.append(best_feature)

            # Save the feature code
            feature_file = ALPHA_LIBRARY / f"{best_feature.feature_id}.py"
            full_code = _FEATURE_TEMPLATE.replace("{FEATURE_CODE}", best_feature.code)
            feature_file.write_text(full_code, encoding="utf-8")

            self._save_library()
            log.info(
                "Feature promoted: %s (IC=%.4f, Sharpe=%.3f)",
                best_feature.feature_id, best_feature.ic, best_feature.sharpe,
            )
            return best_feature

        log.info("No feature met quality threshold in generation %d", self._generation)
        self._save_library()
        return None

    def get_top_features(self, n: int = 5) -> list[AlphaFeature]:
        validated = [f for f in self._features if f.status in ("validated", "promoted")]
        return sorted(validated, key=lambda f: f.sharpe, reverse=True)[:n]

    def get_status(self) -> dict:
        return {
            "generation": self._generation,
            "total_features": len(self._features),
            "validated": len([f for f in self._features if f.status == "validated"]),
            "top_features": [f.to_dict() for f in self.get_top_features(3)],
        }

    async def probe_onchain_liquidity_lane(self) -> dict[str, Any]:
        """
        链上轨道日课（代理）：用 SOL 永续 OHLCV 近似「流动性波动 / 池深冲击」，
        与 Jupiter/Raydium 实盘流数据互补；不发起链上 RPC。
        """
        try:
            data = await fetch_ohlcv("SOLUSDT", "4H", 320, use_cache=True)
        except Exception as e:
            log.warning("onchain lane: fetch_ohlcv failed: %s", e)
            return {"ok": False, "samples": 0, "error": str(e)[:120]}
        if len(data) < 40:
            return {"ok": False, "samples": 0, "error": "short_history"}
        closes = np.asarray(data[:, 4], dtype=float)
        highs = np.asarray(data[:, 2], dtype=float)
        lows = np.asarray(data[:, 3], dtype=float)
        vols = np.asarray(data[:, 5], dtype=float)
        rng = (highs - lows) / np.maximum(closes, 1e-12) * 100.0
        recent = float(np.mean(rng[-20:]))
        base_v = float(np.mean(vols[-100:])) if len(vols) >= 100 else float(np.mean(vols))
        liq_proxy = float(np.mean(vols[-20:]) / max(base_v, 1e-12))
        samples = int(len(data)) * 5
        return {
            "ok": True,
            "samples": samples,
            "range_pct_20bar": recent,
            "liquidity_stress_proxy": liq_proxy,
            "lane": "onchain",
        }


# Module singleton
alpha_evolver = AlphaEvolver()
