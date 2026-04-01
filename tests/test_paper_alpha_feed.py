import pytest

from skills.sk_academic_researcher import (
    extract_formula_snippets,
    parse_arxiv_id_from_url,
)
from skills import sk_paper_to_alpha as pta


def test_parse_arxiv_url():
    assert parse_arxiv_id_from_url("https://arxiv.org/abs/2401.01234") == "2401.01234"
    assert parse_arxiv_id_from_url("http://arxiv.org/pdf/2401.01234.pdf") == "2401.01234"


def test_extract_formula_snippets():
    s = r"The loss is $\mathcal{L} = \sum_i y_i$ and energy $$E = mc^2$$"
    out = extract_formula_snippets(s)
    assert any("mathcal" in x for x in out)
    assert any("E = mc" in x for x in out)


def test_write_paper_factor_file(tmp_path):
    p = pta.write_paper_factor_skill_file(
        [r"\Delta p"],
        {"hyperparams": {"fragment_len": 12}},
        "feedtest01",
        skills_dir=tmp_path,
    )
    text = p.read_text(encoding="utf-8")
    assert "paper_alpha_signals" in text
    assert "pandas" in text.lower()
    assert "numpy" in text.lower()
    assert p.name.startswith("sk_paper_alpha_")


@pytest.mark.asyncio
async def test_pipeline_mocked_backtest(monkeypatch, tmp_path):
    from trading.backtest_engine import BacktestResult

    async def _fake_backtest(strategy_params, factor_py_path, config=None):
        return BacktestResult(
            sharpe_train=0.4,
            sharpe_test=0.8,
            total_return_pct=1.0,
            max_drawdown_pct=0.05,
            win_rate=55.0,
            total_trades=12,
            avg_trade_pnl=0.1,
            profit_factor=1.2,
            calmar_ratio=0.3,
        )

    monkeypatch.setattr(
        "trading.backtest_engine.run_backtest_with_factor_file",
        _fake_backtest,
    )
    monkeypatch.setattr(
        "skill_library.register_or_update_factor_skill",
        lambda **kwargs: None,
    )
    # Write factors under tmp_path/skills
    skills_sub = tmp_path / "skills"
    skills_sub.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        "pipeline.paper_alpha_feed.REPO_ROOT",
        tmp_path,
    )

    from pipeline.paper_alpha_feed import run_academic_to_alpha_pipeline

    out = await run_academic_to_alpha_pipeline(
        {"force_mock": True, "promote_min_win_rate": 50.0, "promote_min_trades": 5}
    )
    assert out.get("ok") is True
    assert out.get("stage") == "complete"
    assert out.get("win_rate_report") == 55.0
    assert out.get("promoted_to_library") is True
    assert "factor_py" in out
