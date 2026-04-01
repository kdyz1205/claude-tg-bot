"""Offline TUNE_STRATEGY grid helpers (no pool execution)."""

from infinite_evolver import _build_v6_param_grid


def test_build_v6_param_grid_respects_max_combos():
    base = {
        "ma5_len": 5,
        "ma8_len": 8,
        "ema21_len": 21,
        "ma55_len": 55,
        "bb_length": 21,
        "bb_std_dev": 2.5,
        "dist_ma5_ma8": 1.5,
        "dist_ma8_ema21": 2.5,
        "dist_ema21_ma55": 4.0,
        "slope_len": 3,
        "slope_threshold": 0.1,
        "atr_period": 14,
    }
    g = _build_v6_param_grid(base, max_combos=40)
    assert len(g) == 40
    assert all(p["ma5_len"] < p["ma8_len"] < p["ema21_len"] < p["ma55_len"] for p in g)
    assert "fee_mult" in g[0] and "slippage_mult" in g[0]
