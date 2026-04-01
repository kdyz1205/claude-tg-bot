"""
Live execution sizing: Kelly-based absolute SOL stake from skill / signal identity.

``PortfolioManager.get_kelly_position_size`` is the single entry point for spot leg sizing.
Root ``portfolio_manager`` (repo) remains the Telegram/dashboard formatter — import via
``from trading.portfolio_manager import PortfolioManager``.
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# Below this absolute SOL size, live spot legs are treated as uneconomical vs gas/slippage.
KELLY_MIN_TRADE_SOL = float(os.environ.get("LIVE_KELLY_MIN_TRADE_SOL", "0.05"))


def clamp_kelly_stake_to_balance(kelly_sol: float, equity: float, cfg: dict) -> float | None:
    """Apply ``min_sol_reserve`` cap; return None if stake is infeasible (warning logged)."""
    if kelly_sol <= 0 or kelly_sol < KELLY_MIN_TRADE_SOL:
        logger.warning("Kelly 仓位建议不足或已熔断，放弃开火。")
        return None
    min_reserve = float((cfg or {}).get("min_sol_reserve", 0.05))
    capped = min(float(kelly_sol), max(0.0, float(equity) - min_reserve))
    if capped <= 0 or capped < KELLY_MIN_TRADE_SOL:
        logger.warning("Kelly 仓位建议不足或已熔断，放弃开火。")
        return None
    return capped


# Symmetric binary payoff (b=1): f* = 2p - 1; multiply by fractional Kelly.
_DEFAULT_FRAC = float(os.environ.get("LIVE_FRACTIONAL_KELLY", "0.25"))
_DEFAULT_MIN_EDGE_P = float(os.environ.get("LIVE_KELLY_MIN_EDGE", "0.52"))
_DEFAULT_MAX_FRAC = float(os.environ.get("LIVE_KELLY_MAX_EQUITY_FRAC", "0.35"))

# Conservative priors when no library/evolver win rate is available (signal_type → edge p).
_SIGNAL_TYPE_PRIORS: dict[str, float] = {
    "pro_strategy": 0.54,
    "alpha_engine": 0.53,
    "onchain_filter": 0.52,
    "funding_delta_positive": 0.55,
    "strategy_brain_neural": 0.56,
    "god_orchestrator": 0.55,
    "live_scan": 0.52,
}


def _norm_probability(raw: Any) -> float | None:
    if raw is None:
        return None
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    if v > 1.0:
        v = v / 100.0
    if v < 0.0 or v > 1.0:
        return None
    return v


class PortfolioManager:
    """Kelly sizing for live SOL spot legs keyed by skill_id / signal_type."""

    @staticmethod
    def get_kelly_position_size(
        skill_id: str | None,
        current_equity: float,
        *,
        cfg: dict[str, Any] | None = None,
    ) -> float:
        """
        Absolute SOL to deploy before min-reserve / max_trade clamps (applied by caller).

        Returns 0 when equity ≤ 0, edge ≤ min edge, or Kelly fraction ≤ 0 (fuse).
        """
        eq = float(current_equity or 0.0)
        if eq <= 0:
            return 0.0

        c = cfg or {}
        frac = float(c.get("kelly_fractional_scale", _DEFAULT_FRAC) or _DEFAULT_FRAC)
        min_edge = float(c.get("kelly_min_edge_p", _DEFAULT_MIN_EDGE_P) or _DEFAULT_MIN_EDGE_P)
        max_frac = float(c.get("kelly_max_equity_fraction", _DEFAULT_MAX_FRAC) or _DEFAULT_MAX_FRAC)

        priors = dict(_SIGNAL_TYPE_PRIORS)
        extra = c.get("kelly_signal_priors")
        if isinstance(extra, dict):
            for k, v in extra.items():
                p = _norm_probability(v)
                if k and p is not None:
                    priors[str(k)] = p

        p = PortfolioManager._resolve_win_probability(skill_id, priors, c)
        if p is None or p < min_edge:
            return 0.0

        f_star = 2.0 * p - 1.0
        if f_star <= 0:
            return 0.0
        kelly_sol = frac * f_star * eq

        pct_cap = float(c.get("max_trade_pct", 15.0) or 15.0) / 100.0 * eq
        kelly_sol = min(kelly_sol, pct_cap, max_frac * eq)

        cap_sol = c.get("max_trade_sol")
        if cap_sol is not None:
            try:
                kelly_sol = min(kelly_sol, float(cap_sol))
            except (TypeError, ValueError):
                pass

        return max(0.0, float(kelly_sol))

    @staticmethod
    def _resolve_win_probability(
        skill_id: str | None,
        priors: dict[str, float],
        cfg: dict[str, Any],
    ) -> float | None:
        sid = (skill_id or "").strip() or None

        try:
            from pipeline.god_orchestrator import get_global_best_snapshot

            snap = get_global_best_snapshot()
        except Exception as e:
            logger.debug("Kelly: global best snapshot unavailable: %s", e)
            snap = {}

        wr = _norm_probability(snap.get("win_rate"))
        best_skill = snap.get("skill_id")
        if wr is not None and sid and best_skill and str(best_skill) == sid:
            return wr
        if wr is not None and sid in ("god_orchestrator", "strategy_brain_neural"):
            return wr

        if sid and sid in priors:
            return priors[sid]

        ovr = cfg.get("kelly_default_win_p")
        p_ovr = _norm_probability(ovr)
        if p_ovr is not None:
            return p_ovr

        if wr is not None:
            return wr

        if sid:
            return priors.get(sid)

        return None
