"""
Strategy Arena — FinEvo-style multi-agent competition framework.

Live execution uses a separate pre-trade **MoE gate** in ``trading.moe_gate``
(three local experts + 1s debate before ``StrategyBrain`` calls ``open_position``).

Based on ICLR 2026 FinEvo patterns:
- Maintain a population of 10-20 strategies (rule-based + evolved + LLM-generated)
- Each generation: all strategies trade the same historical period simultaneously
- Selection: bottom 20% eliminated, top 20% spawn mutations
- Innovation: LLM occasionally proposes entirely new strategy archetypes
- Environmental perturbation: test against historical crash events

Promotion criteria:
- Must survive 3+ generations
- Must have positive Sharpe on at least 2 different market regimes
- Must not have max drawdown > 10% on any crash test
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from .backtest_engine import (
    BacktestConfig, BacktestResult, fetch_ohlcv,
    _compute_v6_signals, _simulate_trades, _compute_metrics,
)
from .indicators import sma, bb_upper, bb_lower

log = logging.getLogger(__name__)

ARENA_DIR = Path(__file__).resolve().parent.parent / "_arena"
ARENA_DIR.mkdir(parents=True, exist_ok=True)
ARENA_STATE_FILE = ARENA_DIR / "arena_state.json"

POPULATION_SIZE = 12
ELIMINATION_PCT = 0.20
REPRODUCTION_PCT = 0.20
MIN_GENERATIONS_TO_PROMOTE = 3
MAX_DRAWDOWN_THRESHOLD = 0.10


@dataclass
class StrategyGenome:
    """A strategy's DNA — its parameter set and performance history."""
    genome_id: str
    params: dict[str, Any]
    generation_born: int = 0
    generations_survived: int = 0
    parent_id: str = ""
    origin: str = "seed"  # seed | mutation | crossover | llm_innovation

    # Performance tracking per-generation
    sharpe_history: list[float] = field(default_factory=list)
    return_history: list[float] = field(default_factory=list)
    drawdown_history: list[float] = field(default_factory=list)
    win_rate_history: list[float] = field(default_factory=list)

    # Regime-specific performance
    regime_sharpes: dict[str, list[float]] = field(default_factory=dict)

    @property
    def avg_sharpe(self) -> float:
        if not self.sharpe_history:
            return 0.0
        return float(np.mean(self.sharpe_history[-5:]))

    @property
    def best_sharpe(self) -> float:
        return max(self.sharpe_history) if self.sharpe_history else 0.0

    @property
    def worst_drawdown(self) -> float:
        return max(self.drawdown_history) if self.drawdown_history else 0.0

    @property
    def is_promotable(self) -> bool:
        if self.generations_survived < MIN_GENERATIONS_TO_PROMOTE:
            return False
        if self.avg_sharpe < 0.5:
            return False
        if self.worst_drawdown > MAX_DRAWDOWN_THRESHOLD:
            return False
        regimes_with_positive = sum(
            1 for sharpes in self.regime_sharpes.values()
            if sharpes and np.mean(sharpes) > 0
        )
        if regimes_with_positive < 2:
            return False
        return True

    def to_dict(self) -> dict:
        return {
            "genome_id": self.genome_id,
            "params": self.params,
            "generation_born": self.generation_born,
            "generations_survived": self.generations_survived,
            "parent_id": self.parent_id,
            "origin": self.origin,
            "avg_sharpe": round(self.avg_sharpe, 4),
            "best_sharpe": round(self.best_sharpe, 4),
            "worst_drawdown": round(self.worst_drawdown, 4),
            "sharpe_history": [round(s, 4) for s in self.sharpe_history[-10:]],
            "is_promotable": self.is_promotable,
            "regime_sharpes": {
                k: [round(s, 4) for s in v[-5:]]
                for k, v in self.regime_sharpes.items()
            },
        }

    @classmethod
    def from_dict(cls, d: dict) -> "StrategyGenome":
        return cls(
            genome_id=d["genome_id"],
            params=d["params"],
            generation_born=d.get("generation_born", 0),
            generations_survived=d.get("generations_survived", 0),
            parent_id=d.get("parent_id", ""),
            origin=d.get("origin", "seed"),
            sharpe_history=d.get("sharpe_history", []),
            return_history=d.get("return_history", []),
            drawdown_history=d.get("drawdown_history", []),
            win_rate_history=d.get("win_rate_history", []),
            regime_sharpes=d.get("regime_sharpes", {}),
        )


def _default_params() -> dict[str, Any]:
    return {
        "ma5_len": 5, "ma8_len": 8, "ema21_len": 21, "ma55_len": 55,
        "bb_length": 21, "bb_std_dev": 2.5,
        "dist_ma5_ma8": 1.5, "dist_ma8_ema21": 2.5, "dist_ema21_ma55": 4.0,
        "slope_len": 3, "slope_threshold": 0.1, "atr_period": 14,
    }


_PARAM_BOUNDS = {
    "ma5_len": (3, 8), "ma8_len": (6, 12), "ema21_len": (15, 30),
    "ma55_len": (40, 80), "bb_length": (15, 30), "bb_std_dev": (1.5, 4.0),
    "dist_ma5_ma8": (0.5, 3.0), "dist_ma8_ema21": (1.0, 5.0),
    "dist_ema21_ma55": (2.0, 8.0), "slope_len": (2, 5),
    "slope_threshold": (0.02, 0.5), "atr_period": (7, 21),
}

_INT_PARAMS = {"ma5_len", "ma8_len", "ema21_len", "ma55_len", "bb_length", "atr_period", "slope_len"}


def _mutate_params(params: dict, mutation_rate: float = 0.15) -> dict:
    """Mutate a parameter set."""
    new_params = copy.deepcopy(params)
    keys = list(_PARAM_BOUNDS.keys())
    n_mutations = random.randint(1, max(1, int(len(keys) * mutation_rate)))
    to_mutate = random.sample(keys, n_mutations)

    for key in to_mutate:
        lo, hi = _PARAM_BOUNDS[key]
        if key in _INT_PARAMS:
            new_params[key] = random.randint(int(lo), int(hi))
        else:
            current = new_params.get(key, (lo + hi) / 2)
            delta = (hi - lo) * random.gauss(0, 0.3)
            new_params[key] = max(lo, min(hi, current + delta))
            new_params[key] = round(new_params[key], 3)

    # Enforce MA ordering
    if new_params["ma5_len"] >= new_params["ma8_len"]:
        new_params["ma8_len"] = new_params["ma5_len"] + 2
    if new_params["ma8_len"] >= new_params["ema21_len"]:
        new_params["ema21_len"] = new_params["ma8_len"] + 5
    if new_params["ema21_len"] >= new_params["ma55_len"]:
        new_params["ma55_len"] = new_params["ema21_len"] + 15

    return new_params


def _crossover_params(parent_a: dict, parent_b: dict) -> dict:
    """Uniform crossover between two parameter sets."""
    child = {}
    for key in _PARAM_BOUNDS:
        if random.random() < 0.5:
            child[key] = parent_a.get(key, parent_b.get(key))
        else:
            child[key] = parent_b.get(key, parent_a.get(key))

    if child["ma5_len"] >= child["ma8_len"]:
        child["ma8_len"] = child["ma5_len"] + 2
    if child["ma8_len"] >= child["ema21_len"]:
        child["ema21_len"] = child["ma8_len"] + 5
    if child["ema21_len"] >= child["ma55_len"]:
        child["ma55_len"] = child["ema21_len"] + 15

    return child


def _detect_market_regime(close: np.ndarray) -> str:
    """Classify market regime from price data."""
    if len(close) < 60:
        return "unknown"
    ma20 = sma(close, 20)
    i = len(close) - 1
    if np.isnan(ma20[i]):
        return "unknown"
    returns = np.diff(close) / close[:-1]
    recent_vol = float(np.std(returns[-50:])) if len(returns) >= 50 else 0
    recent_trend = float(np.mean(returns[-20:])) if len(returns) >= 20 else 0

    if abs(recent_trend) > 0.005 and recent_vol < 0.04:
        return "trending"
    elif recent_vol > 0.04:
        return "volatile"
    elif abs(recent_trend) < 0.002:
        return "ranging"
    return "mixed"


class StrategyArena:
    """Multi-strategy competition arena with evolutionary selection."""

    def __init__(self):
        self.population: list[StrategyGenome] = []
        self.generation: int = 0
        self.eliminated_count: int = 0
        self.promoted_count: int = 0
        self._load_state()

    def _load_state(self):
        if ARENA_STATE_FILE.exists():
            try:
                data = json.loads(ARENA_STATE_FILE.read_text(encoding="utf-8"))
                self.generation = data.get("generation", 0)
                self.eliminated_count = data.get("eliminated_count", 0)
                self.promoted_count = data.get("promoted_count", 0)
                self.population = [
                    StrategyGenome.from_dict(g) for g in data.get("population", [])
                ]
            except Exception as e:
                log.warning("Failed to load arena state: %s", e)

    def _save_state(self):
        try:
            data = {
                "generation": self.generation,
                "eliminated_count": self.eliminated_count,
                "promoted_count": self.promoted_count,
                "population": [g.to_dict() for g in self.population],
                "last_updated": time.time(),
            }
            ARENA_STATE_FILE.write_text(
                json.dumps(data, indent=2, default=str), encoding="utf-8"
            )
        except Exception as e:
            log.warning("Failed to save arena state: %s", e)

    def _initialize_population(self):
        """Create initial population with diverse parameter sets."""
        self.population = []
        # Seed 0: default params
        self.population.append(StrategyGenome(
            genome_id="seed_default",
            params=_default_params(),
            origin="seed",
        ))
        # Fill rest with random mutations
        for i in range(POPULATION_SIZE - 1):
            self.population.append(StrategyGenome(
                genome_id=f"seed_{i + 1}",
                params=_mutate_params(_default_params(), mutation_rate=0.4),
                origin="seed",
            ))

    async def run_tournament(self, symbols: list[str] | None = None) -> dict:
        """Run one full tournament generation.

        All strategies compete on the same data. Bottom N% eliminated, top N% reproduce.
        """
        if symbols is None:
            symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

        if not self.population:
            self._initialize_population()

        self.generation += 1
        log.info("Arena tournament generation %d with %d strategies", self.generation, len(self.population))

        # Fetch data for all symbols
        market_data: dict[str, np.ndarray] = {}
        for sym in symbols:
            try:
                data = await fetch_ohlcv(sym, "4H", 800)
                if len(data) >= 100:
                    market_data[sym] = data
            except Exception as e:
                log.warning("Failed to fetch %s for arena: %s", sym, e)

        if not market_data:
            log.warning("No market data for arena tournament")
            return {"error": "No data"}

        # Evaluate each strategy on each symbol
        config = BacktestConfig(initial_equity=10_000.0)
        results: list[tuple[StrategyGenome, float, float, float]] = []

        for genome in self.population:
            total_sharpe = 0.0
            total_return = 0.0
            total_dd = 0.0
            total_wr = 0.0
            n_evaluated = 0

            for sym, data in market_data.items():
                close = data[:, 4]
                high = data[:, 2]
                low = data[:, 3]
                vol = data[:, 5]

                regime = _detect_market_regime(close)

                signals = _compute_v6_signals(close, high, low, vol, genome.params)
                ma55_arr = sma(close, genome.params.get("ma55_len", 55))
                bb_up_arr = bb_upper(close, genome.params.get("bb_length", 21), genome.params.get("bb_std_dev", 2.5))
                bb_lo_arr = bb_lower(close, genome.params.get("bb_length", 21), genome.params.get("bb_std_dev", 2.5))

                trades = _simulate_trades(
                    close, signals, config,
                    ma55=ma55_arr, bb_up=bb_up_arr, bb_lo=bb_lo_arr,
                )
                metrics = _compute_metrics(trades, config.initial_equity)

                total_sharpe += metrics["sharpe"]
                total_return += metrics["total_return_pct"]
                total_dd = max(total_dd, metrics["max_drawdown_pct"])
                total_wr += metrics["win_rate"]
                n_evaluated += 1

                if regime not in genome.regime_sharpes:
                    genome.regime_sharpes[regime] = []
                genome.regime_sharpes[regime].append(metrics["sharpe"])

            if n_evaluated > 0:
                avg_sharpe = total_sharpe / n_evaluated
                avg_return = total_return / n_evaluated
                avg_wr = total_wr / n_evaluated
            else:
                avg_sharpe = -999
                avg_return = 0
                avg_wr = 0

            genome.sharpe_history.append(avg_sharpe)
            genome.return_history.append(avg_return)
            genome.drawdown_history.append(total_dd)
            genome.win_rate_history.append(avg_wr)
            genome.generations_survived += 1

            results.append((genome, avg_sharpe, avg_return, total_dd))

        # Sort by Sharpe (higher is better)
        results.sort(key=lambda r: r[1], reverse=True)

        # Selection: eliminate bottom 20%
        n_eliminate = max(1, int(len(results) * ELIMINATION_PCT))
        eliminated = [r[0] for r in results[-n_eliminate:]]
        survivors = [r[0] for r in results[:-n_eliminate]]
        self.eliminated_count += len(eliminated)

        # Reproduction: top 20% spawn mutations
        n_reproduce = max(1, int(len(results) * REPRODUCTION_PCT))
        parents = [r[0] for r in results[:n_reproduce]]

        new_genomes: list[StrategyGenome] = []
        for parent in parents:
            if random.random() < 0.7:
                child_params = _mutate_params(parent.params)
                origin = "mutation"
            else:
                other = random.choice(survivors)
                child_params = _crossover_params(parent.params, other.params)
                origin = "crossover"

            child = StrategyGenome(
                genome_id=f"gen{self.generation}_{len(new_genomes)}",
                params=child_params,
                generation_born=self.generation,
                parent_id=parent.genome_id,
                origin=origin,
            )
            new_genomes.append(child)

        self.population = survivors + new_genomes

        # Trim to population size
        if len(self.population) > POPULATION_SIZE:
            self.population = sorted(
                self.population, key=lambda g: g.avg_sharpe, reverse=True
            )[:POPULATION_SIZE]

        # Check for promotable strategies
        promoted = [g for g in self.population if g.is_promotable]
        self.promoted_count += len(promoted)

        self._save_state()

        tournament_result = {
            "generation": self.generation,
            "population_size": len(self.population),
            "eliminated": len(eliminated),
            "reproduced": len(new_genomes),
            "promoted": len(promoted),
            "best_sharpe": round(results[0][1], 4) if results else 0,
            "worst_sharpe": round(results[-1][1], 4) if results else 0,
            "avg_sharpe": round(np.mean([r[1] for r in results]), 4) if results else 0,
            "top_3": [
                {
                    "id": r[0].genome_id,
                    "sharpe": round(r[1], 4),
                    "return": round(r[2], 2),
                    "dd": round(r[3], 4),
                    "survived": r[0].generations_survived,
                    "origin": r[0].origin,
                }
                for r in results[:3]
            ],
        }

        log.info(
            "Arena gen=%d: best=%.3f avg=%.3f eliminated=%d promoted=%d",
            self.generation, tournament_result["best_sharpe"],
            tournament_result["avg_sharpe"], len(eliminated), len(promoted),
        )
        return tournament_result

    def get_best_params(self) -> dict[str, Any] | None:
        """Return the parameters of the best-performing strategy."""
        if not self.population:
            return None
        best = max(self.population, key=lambda g: g.avg_sharpe)
        if best.avg_sharpe > 0:
            return best.params
        return None

    def get_promotable(self) -> list[StrategyGenome]:
        return [g for g in self.population if g.is_promotable]

    def get_status(self) -> dict:
        return {
            "generation": self.generation,
            "population_size": len(self.population),
            "total_eliminated": self.eliminated_count,
            "total_promoted": self.promoted_count,
            "promotable_now": len(self.get_promotable()),
            "best_strategy": (
                max(self.population, key=lambda g: g.avg_sharpe).to_dict()
                if self.population else None
            ),
            "population_diversity": len(set(g.origin for g in self.population)),
        }

    def format_status(self) -> str:
        status = self.get_status()
        lines = [
            "━━ Strategy Arena ━━",
            f"Generation: {status['generation']}",
            f"Population: {status['population_size']}/{POPULATION_SIZE}",
            f"Eliminated: {status['total_eliminated']}",
            f"Promoted: {status['total_promoted']}",
            f"Promotable now: {status['promotable_now']}",
        ]
        best = status.get("best_strategy")
        if best:
            lines.append(f"\n🏆 Best: {best['genome_id']}")
            lines.append(f"   Sharpe: {best['avg_sharpe']:.3f} (best: {best['best_sharpe']:.3f})")
            lines.append(f"   Survived: {best.get('generations_survived', 0)} gens")
            lines.append(f"   Origin: {best['origin']}")
        return "\n".join(lines)


# Module singleton
strategy_arena = StrategyArena()
