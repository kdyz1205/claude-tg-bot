from dataclasses import dataclass

from trading.learned_risk_guards import _ast_guard_source_ok, evaluate_all, reload_guards
from trading import loss_immunity as li


@dataclass
class _R:
    symbol: str
    side: str
    entry_price: float
    exit_price: float
    size: float
    pnl_pct: float
    pnl_usd: float
    entry_time: float
    exit_time: float
    reason: str


def test_consecutive_three_losses_window():
    t0 = 1_000_000.0
    trades = [
        _R("BTC", "long", 100, 99, 100, -1, -10, t0, t0 + 10, "SL"),
        _R("BTC", "long", 100, 98, 100, -2, -20, t0 + 20, t0 + 100, "SL"),
        _R("BTC", "long", 100, 97, 100, -3, -30, t0 + 200, t0 + 500, "SL"),
    ]
    trip = li._last_three_consecutive_losses_within_window(trades)
    assert trip is not None
    assert len(trip) == 3


def test_ast_guard_ok():
    src = """def guard(ctx):
    return float(ctx.get('confidence', 1.0)) < 0.1
"""
    ok, _ = _ast_guard_source_ok(src)
    assert ok


def test_evaluate_veto_after_reload(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "trading.learned_risk_guards._GUARDS_FILE",
        tmp_path / "g.json",
    )
    reload_guards()
    from trading import learned_risk_guards as lrg

    assert lrg.append_guard(
        source="def guard(ctx):\n    return float(ctx.get('confidence',1))<0.2\n",
        diagnosis="test",
    )
    allow, _ = evaluate_all({"confidence": 0.1})
    assert allow is False
    allow2, _ = evaluate_all({"confidence": 0.9})
    assert allow2 is True
