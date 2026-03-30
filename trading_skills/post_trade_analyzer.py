"""
post_trade_analyzer.py — Production-grade Post-Trade Analyzer

Comprehensive trade review engine providing single-trade post-mortems, batch
portfolio analytics, recurring pattern detection, and actionable improvement
suggestions.  Computes MAE/MFE, edge ratios, R-multiples, entry/exit quality
scores, and generates human-readable performance reports.

Designed for quantitative crypto trading on OKX via agent_brain / okx_trader.
Expects Polars DataFrames with OHLCV columns and the standard TradeRecord
dict format used throughout the pipeline.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import polars as pl

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_VALID_SIDES: frozenset[str] = frozenset({"long", "short", "LONG", "SHORT"})
_VALID_REASONS: frozenset[str] = frozenset({
    "TP", "SL", "EMA_BREAK", "TRAILING", "SIGNAL",
})

_ANNUALISATION_FACTOR: float = 365.25 * 24  # hourly returns -> annual

# Grade boundaries (lower inclusive)
_GRADE_MAP: list[tuple[float, str]] = [
    (90.0, "A"),
    (75.0, "B"),
    (55.0, "C"),
    (35.0, "D"),
]

_LOOKFORWARD_BARS: int = 20  # bars to look ahead for optimal-exit analysis

# Pattern thresholds
_REVENGE_WINDOW_SEC: float = 30 * 60          # 30 minutes
_OVERTRADING_THRESHOLD: int = 5               # trades per day
_CUTTING_WINNERS_RATIO: float = 1.8           # MFE / realised
_HOLDING_LOSERS_RATIO: float = 1.8            # MAE / initial risk
_SIZE_ESCALATION_WINDOW: int = 3              # consecutive trades to check


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _grade_from_score(score: float) -> str:
    """Map a 0-100 numeric score to a letter grade."""
    for threshold, grade in _GRADE_MAP:
        if score >= threshold:
            return grade
    return "F"


def _safe_div(a: float, b: float, default: float = 0.0) -> float:
    """Division safe against zero / NaN denominators."""
    if b == 0.0 or not math.isfinite(b):
        return default
    result = a / b
    return result if math.isfinite(result) else default


def _ts_to_dt(ts: float | int | str | datetime) -> datetime:
    """Coerce various timestamp representations to a UTC datetime."""
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    if isinstance(ts, str):
        dt = datetime.fromisoformat(ts)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return datetime.fromtimestamp(float(ts), tz=timezone.utc)


def _ts_to_epoch(ts: float | int | str | datetime) -> float:
    """Coerce to UNIX epoch seconds."""
    return _ts_to_dt(ts).timestamp()


def _normalise_side(side: str) -> str:
    return side.strip().lower()


@dataclass
class _TradeSlice:
    """Internal representation of a trade aligned to OHLCV bars."""
    entry_idx: int
    exit_idx: int
    side: str
    entry_price: float
    exit_price: float
    highs: np.ndarray
    lows: np.ndarray
    closes: np.ndarray


# ---------------------------------------------------------------------------
# PostTradeAnalyzer
# ---------------------------------------------------------------------------

class PostTradeAnalyzer:
    """Hedge-fund grade post-trade analysis engine.

    Accepts completed trades as dicts conforming to the TradeRecord schema and
    optional Polars OHLCV DataFrames per symbol for bar-level analysis.

    Parameters
    ----------
    trade_history : list[dict] | None
        Optional initial list of completed trade records.
    lookforward_bars : int
        Number of bars to examine after the exit for optimal-exit calculation.
    """

    def __init__(
        self,
        trade_history: list[dict] | None = None,
        lookforward_bars: int = _LOOKFORWARD_BARS,
    ) -> None:
        self._history: list[dict] = list(trade_history) if trade_history else []
        if len(self._history) > 5000:
            self._history = self._history[-5000:]
        self._lookforward: int = lookforward_bars

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def analyze_trade(
        self,
        trade: dict,
        df: pl.DataFrame | None = None,
    ) -> dict:
        """Perform a full post-mortem on a single completed trade.

        Parameters
        ----------
        trade : dict
            Must contain at minimum: symbol, side, entry_price, exit_price,
            size, pnl_pct, pnl_usd, entry_time, exit_time, reason.
        df : pl.DataFrame | None
            OHLCV DataFrame for the trade's symbol.  Columns expected:
            open_time, open, high, low, close, volume.  When provided,
            bar-level MAE/MFE and entry/exit quality are computed; otherwise
            only trade-level fields are returned.

        Returns
        -------
        dict
            Comprehensive single-trade analysis result.
        """
        self._validate_trade(trade)

        side = _normalise_side(trade["side"])
        entry_price = float(trade["entry_price"])
        exit_price = float(trade["exit_price"])
        pnl_pct = float(trade.get("pnl_pct", 0.0))

        result: dict[str, Any] = {
            "trade": trade,
            "entry_quality": 50.0,
            "exit_quality": 50.0,
            "mae_pct": 0.0,
            "mfe_pct": 0.0,
            "edge_ratio": 0.0,
            "r_multiple": 0.0,
            "holding_bars": 0,
            "optimal_exit_price": exit_price,
            "left_on_table_pct": 0.0,
            "grade": "C",
            "notes": [],
        }

        # ---- Bar-level analysis (requires OHLCV DataFrame) ----
        if df is not None and len(df) > 0:
            try:
                ts = self._align_trade_to_bars(trade, df)
                if ts is not None:
                    result["holding_bars"] = ts.exit_idx - ts.entry_idx

                    mae_pct, mfe_pct = self._compute_mae_mfe(ts)
                    result["mae_pct"] = round(mae_pct, 4)
                    result["mfe_pct"] = round(mfe_pct, 4)
                    result["edge_ratio"] = round(
                        _safe_div(mfe_pct, abs(mae_pct), default=0.0), 2
                    )

                    entry_q = self._score_entry_quality(ts, df)
                    result["entry_quality"] = round(entry_q, 1)

                    optimal, exit_q = self._score_exit_quality(
                        ts, df, pnl_pct,
                    )
                    result["exit_quality"] = round(exit_q, 1)
                    result["optimal_exit_price"] = round(optimal, 8)

                    if side == "long":
                        lot_pct = _safe_div(
                            optimal - exit_price, entry_price
                        ) * 100.0
                    else:
                        lot_pct = _safe_div(
                            exit_price - optimal, entry_price
                        ) * 100.0
                    result["left_on_table_pct"] = round(max(lot_pct, 0.0), 4)
            except Exception:
                logger.exception("Bar-level analysis failed for %s", trade.get("symbol"))

        # ---- R-multiple ----
        r_mult = self._compute_r_multiple(trade)
        result["r_multiple"] = round(r_mult, 2)

        # ---- Composite grade ----
        composite = self._composite_score(result)
        result["grade"] = _grade_from_score(composite)

        # ---- Qualitative notes ----
        result["notes"] = self._generate_notes(result)

        return result

    def analyze_batch(
        self,
        trades: list[dict] | None = None,
        dfs: dict[str, pl.DataFrame] | None = None,
    ) -> dict:
        """Run batch analytics over a list of trades.

        Parameters
        ----------
        trades : list[dict] | None
            Trade records.  Falls back to internal history if ``None``.
        dfs : dict[str, pl.DataFrame] | None
            Map of symbol -> OHLCV DataFrame.

        Returns
        -------
        dict
            Portfolio-level metrics, per-trade analyses, and breakdowns.
        """
        trades = trades if trades is not None else self._history
        if not trades:
            return self._empty_batch()

        per_trade: list[dict] = []
        for t in trades:
            sym_df = dfs.get(t.get("symbol", ""), None) if dfs else None
            per_trade.append(self.analyze_trade(t, df=sym_df))

        stats = self.compute_statistics(trades)
        breakdowns = self._compute_breakdowns(trades)

        return {
            "trade_count": len(trades),
            "per_trade": per_trade,
            "statistics": stats,
            "breakdowns": breakdowns,
        }

    def compute_statistics(self, trades: list[dict] | None = None) -> dict:
        """Compute portfolio-level statistics.

        Includes win rate, expectancy, profit factor, Sharpe ratio,
        max consecutive streaks, distribution metrics, and holding-period
        comparisons.

        Parameters
        ----------
        trades : list[dict] | None
            Falls back to internal history when ``None``.

        Returns
        -------
        dict
        """
        trades = trades if trades is not None else self._history
        if not trades:
            return self._empty_stats()

        pnls = np.array([float(t.get("pnl_pct", 0.0)) for t in trades])
        pnl_usd = np.array([float(t.get("pnl_usd", 0.0)) for t in trades])

        wins = pnls[pnls > 0]
        losses = pnls[pnls < 0]
        n = len(pnls)
        n_wins = len(wins)
        n_losses = len(losses)
        win_rate = _safe_div(n_wins, n)

        avg_win = float(np.mean(wins)) if n_wins else 0.0
        avg_loss = float(np.mean(losses)) if n_losses else 0.0
        loss_rate = 1.0 - win_rate

        expectancy = avg_win * win_rate + avg_loss * loss_rate
        gross_profit = float(np.sum(wins)) if n_wins else 0.0
        gross_loss = float(np.abs(np.sum(losses))) if n_losses else 0.0
        profit_factor = _safe_div(gross_profit, gross_loss)

        # Sharpe (annualised from hourly-equivalent returns)
        sharpe = self._compute_sharpe(trades) if n >= 5 else None

        # Max consecutive wins / losses
        max_consec_w, max_consec_l = self._max_consecutive(pnls)

        # Holding period comparison
        avg_hold_win, avg_hold_loss = self._avg_holding_periods(trades)

        # Distribution shape
        skew = float(self._skewness(pnls)) if n >= 3 else None
        kurt = float(self._kurtosis(pnls)) if n >= 4 else None
        tail_ratio = self._tail_ratio(pnls) if n >= 20 else None

        return {
            "total_trades": n,
            "wins": n_wins,
            "losses": n_losses,
            "win_rate": round(win_rate, 4),
            "avg_win_pct": round(avg_win, 4),
            "avg_loss_pct": round(avg_loss, 4),
            "expectancy_pct": round(expectancy, 4),
            "profit_factor": round(profit_factor, 2),
            "total_pnl_usd": round(float(np.sum(pnl_usd)), 2),
            "total_pnl_pct": round(float(np.sum(pnls)), 4),
            "sharpe_annualised": round(sharpe, 2) if sharpe is not None else None,
            "max_consecutive_wins": max_consec_w,
            "max_consecutive_losses": max_consec_l,
            "avg_holding_period_wins_h": round(avg_hold_win, 2),
            "avg_holding_period_losses_h": round(avg_hold_loss, 2),
            "skewness": round(skew, 3) if skew is not None else None,
            "kurtosis": round(kurt, 3) if kurt is not None else None,
            "tail_ratio": round(tail_ratio, 3) if tail_ratio is not None else None,
        }

    def identify_patterns(
        self,
        trades: list[dict] | None = None,
    ) -> list[dict]:
        """Detect recurring behavioural patterns in trade history.

        Checks for revenge trading, overtrading, cutting winners, holding
        losers, time bias, symbol bias, and size escalation (martingale).

        Returns
        -------
        list[dict]
            Each dict has ``pattern``, ``severity`` (1-10), ``evidence``,
            and ``description`` keys.
        """
        trades = trades if trades is not None else self._history
        if len(trades) < 2:
            return []

        patterns: list[dict] = []

        revenge = self._detect_revenge_trading(trades)
        if revenge:
            patterns.append(revenge)

        overtrading = self._detect_overtrading(trades)
        if overtrading:
            patterns.append(overtrading)

        cutting = self._detect_cutting_winners(trades)
        if cutting:
            patterns.append(cutting)

        holding = self._detect_holding_losers(trades)
        if holding:
            patterns.append(holding)

        time_bias = self._detect_time_bias(trades)
        if time_bias:
            patterns.append(time_bias)

        symbol_bias = self._detect_symbol_bias(trades)
        if symbol_bias:
            patterns.append(symbol_bias)

        escalation = self._detect_size_escalation(trades)
        if escalation:
            patterns.append(escalation)

        patterns.sort(key=lambda p: p.get("severity", 0), reverse=True)
        return patterns

    def generate_report(self, trades: list[dict] | None = None) -> str:
        """Generate a human-readable performance report.

        Parameters
        ----------
        trades : list[dict] | None
            Falls back to internal history when ``None``.

        Returns
        -------
        str
            Multi-line formatted report suitable for logging or Telegram.
        """
        trades = trades if trades is not None else self._history
        if not trades:
            return "No trades to report."

        stats = self.compute_statistics(trades)
        patterns = self.identify_patterns(trades)
        suggestions = self.suggest_improvements(trades)

        lines: list[str] = []
        lines.append("=" * 56)
        lines.append("         POST-TRADE PERFORMANCE REPORT")
        lines.append("=" * 56)
        lines.append("")

        # Summary
        lines.append("--- Summary ---")
        lines.append(f"  Total trades:      {stats['total_trades']}")
        lines.append(f"  Win rate:          {stats['win_rate']:.1%}")
        lines.append(f"  Profit factor:     {stats['profit_factor']:.2f}")
        lines.append(f"  Expectancy:        {stats['expectancy_pct']:+.2f}%")
        lines.append(f"  Total PnL (USD):   ${stats['total_pnl_usd']:+,.2f}")
        lines.append(f"  Total PnL (%):     {stats['total_pnl_pct']:+.2f}%")
        if stats["sharpe_annualised"] is not None:
            lines.append(f"  Sharpe (ann.):     {stats['sharpe_annualised']:.2f}")
        lines.append("")

        # Win/Loss detail
        lines.append("--- Win / Loss ---")
        lines.append(f"  Avg win:           {stats['avg_win_pct']:+.2f}%")
        lines.append(f"  Avg loss:          {stats['avg_loss_pct']:+.2f}%")
        lines.append(f"  Best streak:       {stats['max_consecutive_wins']} wins")
        lines.append(f"  Worst streak:      {stats['max_consecutive_losses']} losses")
        lines.append(f"  Avg hold (wins):   {stats['avg_holding_period_wins_h']:.1f}h")
        lines.append(f"  Avg hold (losses): {stats['avg_holding_period_losses_h']:.1f}h")
        lines.append("")

        # Distribution
        if stats["skewness"] is not None:
            lines.append("--- Distribution ---")
            lines.append(f"  Skewness:          {stats['skewness']:.3f}")
            if stats["kurtosis"] is not None:
                lines.append(f"  Kurtosis:          {stats['kurtosis']:.3f}")
            if stats["tail_ratio"] is not None:
                lines.append(f"  Tail ratio:        {stats['tail_ratio']:.3f}")
            lines.append("")

        # Patterns
        if patterns:
            lines.append("--- Detected Patterns ---")
            for p in patterns:
                sev = p["severity"]
                marker = "!!!" if sev >= 7 else "! " if sev >= 4 else "  "
                lines.append(f"  {marker} [{sev}/10] {p['pattern']}: {p['description']}")
            lines.append("")

        # Suggestions
        if suggestions:
            lines.append("--- Suggestions ---")
            for i, s in enumerate(suggestions, 1):
                lines.append(f"  {i}. [{s['priority']}] {s['suggestion']}")
            lines.append("")

        lines.append("=" * 56)
        return "\n".join(lines)

    def suggest_improvements(
        self,
        trades: list[dict] | None = None,
    ) -> list[dict]:
        """Generate actionable improvement suggestions based on patterns.

        Returns
        -------
        list[dict]
            Each dict has ``priority`` ("HIGH" | "MEDIUM" | "LOW"),
            ``suggestion``, and ``based_on`` keys.
        """
        trades = trades if trades is not None else self._history
        if not trades:
            return []

        patterns = self.identify_patterns(trades)
        stats = self.compute_statistics(trades)
        suggestions: list[dict] = []

        for p in patterns:
            pat = p["pattern"]
            ev = p.get("evidence", {})

            if pat == "revenge_trading":
                suggestions.append({
                    "priority": "HIGH",
                    "suggestion": (
                        f"Detected {ev.get('count', '?')} revenge entries within "
                        f"30 min of a loss. Enforce a mandatory cooldown of at "
                        f"least 30 minutes after every losing trade."
                    ),
                    "based_on": pat,
                })

            elif pat == "overtrading":
                suggestions.append({
                    "priority": "HIGH",
                    "suggestion": (
                        f"Averaging {ev.get('avg_daily', 0):.1f} trades/day on "
                        f"peak days. Cap daily trades at 5 and prioritise higher-"
                        f"confidence setups."
                    ),
                    "based_on": pat,
                })

            elif pat == "cutting_winners":
                avg_mfe = ev.get("avg_mfe_pct", 0)
                avg_win = ev.get("avg_win_pct", 0)
                suggestions.append({
                    "priority": "HIGH",
                    "suggestion": (
                        f"Average MFE is {avg_mfe:.2f}% but average realised win "
                        f"is only {avg_win:.2f}% -- consider widening take-profit "
                        f"by {((avg_mfe / max(avg_win, 0.01)) - 1) * 100:.0f}% "
                        f"or using a trailing stop."
                    ),
                    "based_on": pat,
                })

            elif pat == "holding_losers":
                suggestions.append({
                    "priority": "HIGH",
                    "suggestion": (
                        "Losses are running well beyond initial risk. Tighten "
                        "stop-losses or use time-based exits for losing positions."
                    ),
                    "based_on": pat,
                })

            elif pat == "time_bias":
                worst_hours = ev.get("worst_hours", [])
                if worst_hours:
                    hr_str = ", ".join(f"{h:02d}:00" for h in worst_hours[:3])
                    pnl = ev.get("worst_pnl_pct", 0.0)
                    suggestions.append({
                        "priority": "MEDIUM",
                        "suggestion": (
                            f"{abs(pnl):.1f}% of losses concentrated around "
                            f"{hr_str} UTC -- consider reducing activity or "
                            f"tightening risk during these hours."
                        ),
                        "based_on": pat,
                    })

            elif pat == "symbol_bias":
                sym = ev.get("worst_symbol", "?")
                sym_pnl = ev.get("worst_pnl_pct", 0.0)
                suggestions.append({
                    "priority": "MEDIUM",
                    "suggestion": (
                        f"Consistently losing on {sym} (cumulative "
                        f"{sym_pnl:+.2f}%). Consider removing it from the "
                        f"watchlist or reducing allocation."
                    ),
                    "based_on": pat,
                })

            elif pat == "size_escalation":
                suggestions.append({
                    "priority": "HIGH",
                    "suggestion": (
                        "Position sizes are increasing after losses (martingale "
                        "tendency). Switch to fixed fractional sizing or reduce "
                        "size after consecutive losses."
                    ),
                    "based_on": pat,
                })

        # Additional stats-based suggestions
        if stats["win_rate"] > 0 and stats["avg_loss_pct"] != 0:
            rr = abs(_safe_div(stats["avg_win_pct"], stats["avg_loss_pct"]))
            if rr < 1.0 and stats["win_rate"] < 0.6:
                suggestions.append({
                    "priority": "MEDIUM",
                    "suggestion": (
                        f"Reward-to-risk ratio is {rr:.2f} with {stats['win_rate']:.0%} "
                        f"win rate -- either improve R:R above 1.0 or boost win "
                        f"rate above 60%."
                    ),
                    "based_on": "statistics",
                })

        if stats.get("avg_holding_period_losses_h", 0) > 0:
            hold_ratio = _safe_div(
                stats["avg_holding_period_wins_h"],
                stats["avg_holding_period_losses_h"],
            )
            if hold_ratio < 0.5:
                suggestions.append({
                    "priority": "MEDIUM",
                    "suggestion": (
                        "Winners are being closed much faster than losers. "
                        "Let winners run longer or cut losers sooner."
                    ),
                    "based_on": "holding_period_asymmetry",
                })

        suggestions.sort(
            key=lambda s: {"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(s["priority"], 3)
        )
        return suggestions

    # ------------------------------------------------------------------
    # Internal: bar alignment and MAE / MFE
    # ------------------------------------------------------------------

    def _align_trade_to_bars(
        self,
        trade: dict,
        df: pl.DataFrame,
    ) -> _TradeSlice | None:
        """Map trade entry/exit times to bar indices in the DataFrame."""
        if df.is_empty() or "open_time" not in df.columns:
            return None

        required = {"high", "low", "close"}
        if required - set(df.columns):
            return None

        entry_ts = _ts_to_epoch(trade["entry_time"])
        exit_ts = _ts_to_epoch(trade["exit_time"])

        # open_time can be datetime, int (ms), or float (s)
        ot_col = df["open_time"]
        if ot_col.dtype in (pl.Datetime, pl.Date):
            epochs = ot_col.cast(pl.Int64) / 1_000_000  # us -> s
        elif ot_col.dtype in (pl.Int64, pl.Int32, pl.UInt64, pl.UInt32):
            vals = ot_col.to_numpy()
            # heuristic: if values > 1e12 they are ms, else seconds
            if len(vals) > 0 and vals[0] > 1e12:
                epochs = ot_col.cast(pl.Float64) / 1_000.0
            else:
                epochs = ot_col.cast(pl.Float64)
        else:
            epochs = ot_col.cast(pl.Float64)

        ep_np = epochs.to_numpy()

        entry_idx = int(np.searchsorted(ep_np, entry_ts, side="right")) - 1
        exit_idx = int(np.searchsorted(ep_np, exit_ts, side="right")) - 1

        entry_idx = max(0, min(entry_idx, len(df) - 1))
        exit_idx = max(entry_idx, min(exit_idx, len(df) - 1))

        if entry_idx == exit_idx:
            # Trade within a single bar -- still valid
            exit_idx = min(entry_idx + 1, len(df) - 1)

        highs = df["high"].to_numpy().astype(np.float64)
        lows = df["low"].to_numpy().astype(np.float64)
        closes = df["close"].to_numpy().astype(np.float64)

        return _TradeSlice(
            entry_idx=entry_idx,
            exit_idx=exit_idx,
            side=_normalise_side(trade["side"]),
            entry_price=float(trade["entry_price"]),
            exit_price=float(trade["exit_price"]),
            highs=highs[entry_idx : exit_idx + 1],
            lows=lows[entry_idx : exit_idx + 1],
            closes=closes[entry_idx : exit_idx + 1],
        )

    def _compute_mae_mfe(self, ts: _TradeSlice) -> tuple[float, float]:
        """Compute Maximum Adverse / Favourable Excursion (percentage)."""
        entry = ts.entry_price
        if entry == 0 or len(ts.highs) == 0 or len(ts.lows) == 0:
            return 0.0, 0.0

        if ts.side == "long":
            # Adverse = price drops; favourable = price rises
            worst = float(np.min(ts.lows))
            best = float(np.max(ts.highs))
            mae = (worst - entry) / entry * 100.0   # negative
            mfe = (best - entry) / entry * 100.0     # positive
        else:
            # Short: adverse = price rises; favourable = price drops
            worst = float(np.max(ts.highs))
            best = float(np.min(ts.lows))
            mae = (entry - worst) / entry * 100.0    # negative
            mfe = (entry - best) / entry * 100.0      # positive

        return mae, mfe

    # ------------------------------------------------------------------
    # Internal: entry / exit quality scoring
    # ------------------------------------------------------------------

    def _score_entry_quality(
        self,
        ts: _TradeSlice,
        df: pl.DataFrame,
    ) -> float:
        """Score 0-100 how good the entry price was relative to the bar range.

        100 = entered at the absolute best price in the surrounding window.
        0   = entered at the worst possible price.
        """
        # Use a 5-bar window centred on entry
        n = len(df)
        start = max(0, ts.entry_idx - 2)
        end = min(n, ts.entry_idx + 3)

        window_highs = df["high"].slice(start, end - start).to_numpy().astype(np.float64)
        window_lows = df["low"].slice(start, end - start).to_numpy().astype(np.float64)

        range_high = float(np.max(window_highs))
        range_low = float(np.min(window_lows))
        bar_range = range_high - range_low

        if bar_range <= 0:
            return 50.0  # no range information

        entry = ts.entry_price

        if ts.side == "long":
            # Best long entry = near range low
            score = (1.0 - (entry - range_low) / bar_range) * 100.0
        else:
            # Best short entry = near range high
            score = ((entry - range_low) / bar_range) * 100.0

        return float(np.clip(score, 0.0, 100.0))

    def _score_exit_quality(
        self,
        ts: _TradeSlice,
        df: pl.DataFrame,
        pnl_pct: float,
    ) -> tuple[float, float]:
        """Score exit quality and compute the optimal exit price.

        Returns (optimal_exit_price, exit_quality_score).
        """
        n = len(df)
        look_end = min(n, ts.exit_idx + self._lookforward + 1)

        slice_len = look_end - ts.exit_idx
        if slice_len <= 0:
            return ts.exit_price, 50.0
        post_highs = df["high"].slice(ts.exit_idx, slice_len).to_numpy().astype(np.float64)
        post_lows = df["low"].slice(ts.exit_idx, slice_len).to_numpy().astype(np.float64)

        # Also consider within-trade range
        trade_highs = ts.highs
        trade_lows = ts.lows

        # Concatenate only non-empty arrays
        all_highs = [a for a in (trade_highs, post_highs) if len(a) > 0]
        all_lows = [a for a in (trade_lows, post_lows) if len(a) > 0]
        if not all_highs or not all_lows:
            return ts.exit_price, 50.0

        if ts.side == "long":
            optimal = float(np.max(np.concatenate(all_highs)))
            worst_exit = float(np.min(np.concatenate(all_lows)))
        else:
            optimal = float(np.min(np.concatenate(all_lows)))
            worst_exit = float(np.max(np.concatenate(all_highs)))

        exit_range = abs(optimal - worst_exit)
        if exit_range <= 0:
            return ts.exit_price, 50.0

        if ts.side == "long":
            exit_quality = ((ts.exit_price - worst_exit) / exit_range) * 100.0
        else:
            exit_quality = ((worst_exit - ts.exit_price) / exit_range) * 100.0

        exit_quality = float(np.clip(exit_quality, 0.0, 100.0))
        return optimal, exit_quality

    # ------------------------------------------------------------------
    # Internal: R-multiple
    # ------------------------------------------------------------------

    def _compute_r_multiple(self, trade: dict) -> float:
        """Compute R-multiple: actual profit / initial risk.

        Initial risk is estimated from stop-loss distance when the exit reason
        is ``SL`` (the trade hit the stop), or inferred from MAE otherwise.
        """
        side = _normalise_side(trade["side"])
        entry = float(trade["entry_price"])
        exit_p = float(trade["exit_price"])

        if entry == 0:
            return 0.0

        if side == "long":
            actual_pnl = (exit_p - entry) / entry
        else:
            actual_pnl = (entry - exit_p) / entry

        # Try to infer initial risk from a stop_loss field if present
        sl = trade.get("stop_loss") or trade.get("sl_price")
        if sl is not None:
            sl = float(sl)
            if side == "long":
                initial_risk = abs(entry - sl) / entry
            else:
                initial_risk = abs(sl - entry) / entry
        else:
            # Fallback: use exit price as risk when reason is SL
            if trade.get("reason") == "SL":
                initial_risk = abs(actual_pnl)
            else:
                # Conservative: assume 1% risk if no info
                initial_risk = 0.01

        return _safe_div(actual_pnl, initial_risk)

    # ------------------------------------------------------------------
    # Internal: composite grading
    # ------------------------------------------------------------------

    def _composite_score(self, result: dict) -> float:
        """Combine sub-scores into a 0-100 composite for grading."""
        entry_q = result.get("entry_quality", 50.0)
        exit_q = result.get("exit_quality", 50.0)
        edge = result.get("edge_ratio", 0.0)
        r_mult = result.get("r_multiple", 0.0)

        # Normalise edge ratio: 2.0 -> 100, 0 -> 0
        edge_score = float(np.clip(edge / 2.0 * 100.0, 0.0, 100.0))

        # Normalise R-multiple: 3.0 -> 100, -1 -> 0
        r_score = float(np.clip((r_mult + 1.0) / 4.0 * 100.0, 0.0, 100.0))

        composite = (
            0.25 * entry_q
            + 0.30 * exit_q
            + 0.20 * edge_score
            + 0.25 * r_score
        )
        return float(np.clip(composite, 0.0, 100.0))

    # ------------------------------------------------------------------
    # Internal: qualitative notes
    # ------------------------------------------------------------------

    def _generate_notes(self, result: dict) -> list[str]:
        """Generate brief qualitative notes based on analysis results."""
        notes: list[str] = []
        trade = result.get("trade", {})

        if result["mae_pct"] < -3.0:
            notes.append(
                f"Significant adverse excursion of {result['mae_pct']:.1f}% "
                f"-- consider tighter stops."
            )

        if result["mfe_pct"] > 0 and result.get("left_on_table_pct", 0) > 1.0:
            notes.append(
                f"Left {result['left_on_table_pct']:.1f}% on the table -- "
                f"trailing stop or wider TP may help."
            )

        if result["edge_ratio"] < 0.5 and result["edge_ratio"] > 0:
            notes.append(
                "Low edge ratio indicates poor risk management during the trade."
            )
        elif result["edge_ratio"] >= 2.0:
            notes.append("Excellent edge ratio -- trade was well-managed.")

        if result["r_multiple"] < -1.0:
            notes.append(
                f"R-multiple of {result['r_multiple']:.1f}R -- "
                f"loss exceeded initial risk."
            )
        elif result["r_multiple"] >= 3.0:
            notes.append(
                f"Outstanding {result['r_multiple']:.1f}R trade."
            )

        reason = trade.get("reason", "")
        if reason == "SL" and result["entry_quality"] < 30:
            notes.append(
                "Poor entry quality combined with stop-loss hit -- "
                "review entry timing."
            )

        if result["holding_bars"] <= 1:
            notes.append("Very short hold -- possible noise trade.")

        if not notes:
            notes.append("Trade executed within normal parameters.")

        return notes

    # ------------------------------------------------------------------
    # Internal: batch statistics helpers
    # ------------------------------------------------------------------

    def _compute_sharpe(self, trades: list[dict]) -> float:
        """Annualised Sharpe ratio from per-trade PnL percentages.

        Uses the mean holding period to estimate the annualisation factor.
        """
        pnls = np.array([float(t.get("pnl_pct", 0.0)) for t in trades])
        if len(pnls) < 2:
            return 0.0

        mean_ret = float(np.mean(pnls))
        std_ret = float(np.std(pnls, ddof=1))
        if std_ret == 0:
            return 0.0

        # Estimate trades per year from average holding period
        holding_hours = []
        for t in trades:
            try:
                entry = _ts_to_epoch(t["entry_time"])
                exit_ = _ts_to_epoch(t["exit_time"])
                h = (exit_ - entry) / 3600.0
                if h > 0:
                    holding_hours.append(h)
            except Exception:
                continue

        if holding_hours:
            avg_hold_h = float(np.mean(holding_hours))
            trades_per_year = _ANNUALISATION_FACTOR / max(avg_hold_h, 0.01)
        else:
            trades_per_year = 252.0  # fallback: ~daily

        return (mean_ret / std_ret) * math.sqrt(trades_per_year)

    @staticmethod
    def _max_consecutive(pnls: np.ndarray) -> tuple[int, int]:
        """Return (max_consecutive_wins, max_consecutive_losses)."""
        max_w = max_l = cur_w = cur_l = 0
        for p in pnls:
            if p > 0:
                cur_w += 1
                cur_l = 0
            elif p < 0:
                cur_l += 1
                cur_w = 0
            else:
                cur_w = 0
                cur_l = 0
            max_w = max(max_w, cur_w)
            max_l = max(max_l, cur_l)
        return max_w, max_l

    @staticmethod
    def _avg_holding_periods(trades: list[dict]) -> tuple[float, float]:
        """Average holding period in hours for wins vs losses."""
        win_hours: list[float] = []
        loss_hours: list[float] = []
        for t in trades:
            try:
                entry = _ts_to_epoch(t["entry_time"])
                exit_ = _ts_to_epoch(t["exit_time"])
                h = (exit_ - entry) / 3600.0
            except Exception:
                continue
            if float(t.get("pnl_pct", 0)) > 0:
                win_hours.append(h)
            elif float(t.get("pnl_pct", 0)) < 0:
                loss_hours.append(h)

        avg_w = float(np.mean(win_hours)) if win_hours else 0.0
        avg_l = float(np.mean(loss_hours)) if loss_hours else 0.0
        return avg_w, avg_l

    @staticmethod
    def _skewness(arr: np.ndarray) -> float:
        """Compute skewness (adjusted Fisher-Pearson, matches scipy.stats.skew bias=False)."""
        n = len(arr)
        if n < 3:
            return 0.0
        mean = np.mean(arr)
        std = np.std(arr, ddof=1)
        if std == 0:
            return 0.0
        # Adjusted Fisher-Pearson: n / ((n-1)*(n-2)) * sum(((x-mean)/std)^3)
        m3 = np.sum(((arr - mean) / std) ** 3)
        return float((n / ((n - 1) * (n - 2))) * m3)

    @staticmethod
    def _kurtosis(arr: np.ndarray) -> float:
        """Excess kurtosis (adjusted, matches scipy.stats.kurtosis bias=False)."""
        n = len(arr)
        if n < 4:
            return 0.0
        mean = np.mean(arr)
        std = np.std(arr, ddof=1)
        if std == 0:
            return 0.0
        # Adjusted excess kurtosis formula
        m4_sum = np.sum(((arr - mean) / std) ** 4)
        k = (n * (n + 1) / ((n - 1) * (n - 2) * (n - 3))) * m4_sum
        correction = 3.0 * (n - 1) ** 2 / ((n - 2) * (n - 3))
        return float(k - correction)

    @staticmethod
    def _tail_ratio(arr: np.ndarray) -> float:
        """Ratio of right tail (95th pctl) to left tail (5th pctl)."""
        if len(arr) < 20:
            return 1.0
        p95 = float(np.percentile(arr, 95))
        p5 = float(np.percentile(arr, 5))
        return _safe_div(abs(p95), abs(p5), default=1.0)

    def _compute_breakdowns(self, trades: list[dict]) -> dict:
        """Performance breakdowns by symbol, side, day, hour, exit reason."""
        by_symbol: dict[str, list[float]] = {}
        by_side: dict[str, list[float]] = {}
        by_dow: dict[int, list[float]] = {}
        by_hour: dict[int, list[float]] = {}
        by_reason: dict[str, list[float]] = {}

        for t in trades:
            pnl = float(t.get("pnl_pct", 0.0))

            sym = t.get("symbol", "UNKNOWN")
            by_symbol.setdefault(sym, []).append(pnl)

            side = _normalise_side(t.get("side", "unknown"))
            by_side.setdefault(side, []).append(pnl)

            reason = t.get("reason", "UNKNOWN")
            by_reason.setdefault(reason, []).append(pnl)

            try:
                dt = _ts_to_dt(t["entry_time"])
                by_dow.setdefault(dt.weekday(), []).append(pnl)
                by_hour.setdefault(dt.hour, []).append(pnl)
            except Exception:
                pass

        def _agg(d: dict) -> dict:
            return {
                k: {
                    "count": len(v),
                    "total_pnl_pct": round(sum(v), 4),
                    "win_rate": round(_safe_div(sum(1 for x in v if x > 0), len(v)), 4),
                    "avg_pnl_pct": round(float(np.mean(v)), 4),
                }
                for k, v in d.items()
            }

        return {
            "by_symbol": _agg(by_symbol),
            "by_side": _agg(by_side),
            "by_day_of_week": _agg(by_dow),
            "by_hour": _agg(by_hour),
            "by_exit_reason": _agg(by_reason),
        }

    # ------------------------------------------------------------------
    # Internal: pattern detection
    # ------------------------------------------------------------------

    def _detect_revenge_trading(self, trades: list[dict]) -> dict | None:
        """Loss followed by a new entry within 30 minutes."""
        count = 0
        for i in range(len(trades) - 1):
            if float(trades[i].get("pnl_pct", 0)) >= 0:
                continue
            try:
                prev_exit = _ts_to_epoch(trades[i]["exit_time"])
                next_entry = _ts_to_epoch(trades[i + 1]["entry_time"])
            except Exception:
                continue
            if 0 < (next_entry - prev_exit) < _REVENGE_WINDOW_SEC:
                count += 1

        if count == 0:
            return None

        severity = min(10, 3 + count * 2)
        return {
            "pattern": "revenge_trading",
            "severity": severity,
            "evidence": {"count": count},
            "description": (
                f"{count} instance(s) of entering a new trade within 30 min "
                f"of a loss."
            ),
        }

    def _detect_overtrading(self, trades: list[dict]) -> dict | None:
        """More than threshold trades in a single day."""
        daily_counts: dict[str, int] = {}
        for t in trades:
            try:
                dt = _ts_to_dt(t["entry_time"])
                day = dt.strftime("%Y-%m-%d")
                daily_counts[day] = daily_counts.get(day, 0) + 1
            except Exception:
                continue

        over_days = {d: c for d, c in daily_counts.items() if c > _OVERTRADING_THRESHOLD}
        if not over_days:
            return None

        avg_daily = float(np.mean(list(over_days.values())))
        severity = min(10, 3 + len(over_days))
        return {
            "pattern": "overtrading",
            "severity": severity,
            "evidence": {
                "over_days": len(over_days),
                "avg_daily": avg_daily,
                "worst_day": max(over_days, key=over_days.get),
                "worst_count": max(over_days.values()),
            },
            "description": (
                f"{len(over_days)} day(s) with >{_OVERTRADING_THRESHOLD} trades "
                f"(peak: {max(over_days.values())})."
            ),
        }

    def _detect_cutting_winners(self, trades: list[dict]) -> dict | None:
        """Average MFE of wins greatly exceeds average realised win profit."""
        wins = [t for t in trades if float(t.get("pnl_pct", 0)) > 0]
        if len(wins) < 3:
            return None

        # We need MFE data -- stored from prior analyze_trade calls or estimate
        avg_win_pct = float(np.mean([float(t["pnl_pct"]) for t in wins]))
        if avg_win_pct <= 0:
            return None

        # Use mfe_pct if available in trade metadata, else skip
        mfes = []
        for t in wins:
            mfe = t.get("_mfe_pct")
            if mfe is not None:
                mfes.append(float(mfe))

        if not mfes:
            return None

        avg_mfe = float(np.mean(mfes))
        if avg_mfe <= 0:
            return None

        ratio = avg_mfe / avg_win_pct
        if ratio < _CUTTING_WINNERS_RATIO:
            return None

        severity = min(10, int(ratio * 2))
        return {
            "pattern": "cutting_winners",
            "severity": severity,
            "evidence": {
                "avg_mfe_pct": round(avg_mfe, 4),
                "avg_win_pct": round(avg_win_pct, 4),
                "ratio": round(ratio, 2),
            },
            "description": (
                f"Avg MFE ({avg_mfe:.2f}%) is {ratio:.1f}x the avg realised "
                f"win ({avg_win_pct:.2f}%) -- exiting winners too early."
            ),
        }

    def _detect_holding_losers(self, trades: list[dict]) -> dict | None:
        """Average MAE of losses greatly exceeds initial risk."""
        losses = [t for t in trades if float(t.get("pnl_pct", 0)) < 0]
        if len(losses) < 3:
            return None

        maes = []
        for t in losses:
            mae = t.get("_mae_pct")
            if mae is not None:
                maes.append(abs(float(mae)))

        if not maes:
            return None

        avg_mae = float(np.mean(maes))
        avg_loss = abs(float(np.mean([float(t["pnl_pct"]) for t in losses])))
        if avg_loss <= 0:
            return None

        ratio = avg_mae / avg_loss
        if ratio < _HOLDING_LOSERS_RATIO:
            return None

        severity = min(10, int(ratio * 2))
        return {
            "pattern": "holding_losers",
            "severity": severity,
            "evidence": {
                "avg_mae_pct": round(avg_mae, 4),
                "avg_loss_pct": round(avg_loss, 4),
                "ratio": round(ratio, 2),
            },
            "description": (
                f"Avg MAE ({avg_mae:.2f}%) is {ratio:.1f}x the avg realised "
                f"loss ({avg_loss:.2f}%) -- holding losers too long."
            ),
        }

    def _detect_time_bias(self, trades: list[dict]) -> dict | None:
        """Significantly worse performance at certain hours."""
        hour_pnl: dict[int, list[float]] = {}
        for t in trades:
            try:
                dt = _ts_to_dt(t["entry_time"])
                h = dt.hour
                hour_pnl.setdefault(h, []).append(float(t.get("pnl_pct", 0)))
            except Exception:
                continue

        if len(hour_pnl) < 4:
            return None

        hour_totals = {h: sum(v) for h, v in hour_pnl.items()}
        total_pnl = sum(hour_totals.values())

        # Identify hours that account for disproportionate losses
        worst_hours = sorted(
            [h for h, p in hour_totals.items() if p < 0],
            key=lambda h: hour_totals[h],
        )

        if not worst_hours:
            return None

        worst_pnl = sum(hour_totals[h] for h in worst_hours[:3])
        total_loss = sum(p for p in hour_totals.values() if p < 0)

        if total_loss == 0:
            return None

        concentration = abs(worst_pnl / total_loss)
        if concentration < 0.5:
            return None

        severity = min(10, int(concentration * 8))
        return {
            "pattern": "time_bias",
            "severity": severity,
            "evidence": {
                "worst_hours": worst_hours[:3],
                "worst_pnl_pct": round(worst_pnl, 4),
                "concentration": round(concentration, 4),
            },
            "description": (
                f"{concentration:.0%} of losses concentrated in "
                f"{len(worst_hours[:3])} hour(s): "
                f"{', '.join(f'{h:02d}:00' for h in worst_hours[:3])} UTC."
            ),
        }

    def _detect_symbol_bias(self, trades: list[dict]) -> dict | None:
        """Consistently losing on specific symbols."""
        sym_pnl: dict[str, list[float]] = {}
        for t in trades:
            sym = t.get("symbol", "UNKNOWN")
            sym_pnl.setdefault(sym, []).append(float(t.get("pnl_pct", 0)))

        if len(sym_pnl) < 2:
            return None

        sym_totals = {s: sum(v) for s, v in sym_pnl.items()}
        losing_syms = {s: p for s, p in sym_totals.items() if p < 0}

        if not losing_syms:
            return None

        worst_sym = min(losing_syms, key=losing_syms.get)
        worst_pnl = losing_syms[worst_sym]
        num_trades = len(sym_pnl[worst_sym])

        # Must have enough trades to be meaningful
        if num_trades < 3:
            return None

        win_rate = sum(1 for x in sym_pnl[worst_sym] if x > 0) / num_trades if num_trades else 0
        if win_rate > 0.35:
            return None  # not consistently losing

        severity = min(10, 3 + int(abs(worst_pnl)))
        return {
            "pattern": "symbol_bias",
            "severity": severity,
            "evidence": {
                "worst_symbol": worst_sym,
                "worst_pnl_pct": round(worst_pnl, 4),
                "num_trades": num_trades,
                "win_rate": round(win_rate, 4),
            },
            "description": (
                f"Consistently losing on {worst_sym}: {num_trades} trades, "
                f"{win_rate:.0%} win rate, {worst_pnl:+.2f}% cumulative PnL."
            ),
        }

    def _detect_size_escalation(self, trades: list[dict]) -> dict | None:
        """Increasing position size after losses (martingale behaviour)."""
        escalation_count = 0
        total_checked = 0

        for i in range(len(trades) - _SIZE_ESCALATION_WINDOW + 1):
            window = trades[i : i + _SIZE_ESCALATION_WINDOW]
            pnls = [float(t.get("pnl_pct", 0)) for t in window]
            sizes = [float(t.get("size", 0)) for t in window]

            if not all(s > 0 for s in sizes):
                continue

            # Check: consecutive losses with increasing size
            if all(p < 0 for p in pnls[:-1]):
                total_checked += 1
                if all(sizes[j + 1] > sizes[j] * 1.1 for j in range(len(sizes) - 1)):
                    escalation_count += 1

        if escalation_count == 0:
            return None

        severity = min(10, 5 + escalation_count * 2)
        return {
            "pattern": "size_escalation",
            "severity": severity,
            "evidence": {
                "escalation_count": escalation_count,
                "total_checked": total_checked,
            },
            "description": (
                f"{escalation_count} instance(s) of increasing position size "
                f"after consecutive losses (martingale tendency)."
            ),
        }

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_trade(trade: dict) -> None:
        """Validate that a trade dict has the minimum required fields."""
        required = ("symbol", "side", "entry_price", "exit_price")
        missing = [k for k in required if k not in trade]
        if missing:
            raise ValueError(
                f"Trade record missing required fields: {', '.join(missing)}"
            )
        side = trade["side"]
        if isinstance(side, str) and side.strip().upper() not in {"LONG", "SHORT"}:
            raise ValueError(
                f"Invalid trade side '{side}'. Expected 'long' or 'short'."
            )

    # ------------------------------------------------------------------
    # Empty return structures
    # ------------------------------------------------------------------

    @staticmethod
    def _empty_batch() -> dict:
        return {
            "trade_count": 0,
            "per_trade": [],
            "statistics": PostTradeAnalyzer._empty_stats(),
            "breakdowns": {},
        }

    @staticmethod
    def _empty_stats() -> dict:
        return {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "avg_win_pct": 0.0,
            "avg_loss_pct": 0.0,
            "expectancy_pct": 0.0,
            "profit_factor": 0.0,
            "total_pnl_usd": 0.0,
            "total_pnl_pct": 0.0,
            "sharpe_annualised": None,
            "max_consecutive_wins": 0,
            "max_consecutive_losses": 0,
            "avg_holding_period_wins_h": 0.0,
            "avg_holding_period_losses_h": 0.0,
            "skewness": None,
            "kurtosis": None,
            "tail_ratio": None,
        }
