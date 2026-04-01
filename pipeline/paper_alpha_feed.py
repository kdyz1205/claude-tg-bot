"""
学术论文 → NumPy/Pandas 因子 → ProcessPoolExecutor 回测 → 条件入库。

编排 ``sk_academic_researcher``（URL / 理论名词 → 摘要 + 公式片段）与
``sk_paper_to_alpha``（向量化模块），调用 ``run_backtest_with_factor_file``。
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent


def _fingerprint_parts(formulas: list[str], architecture: dict[str, Any], extra: str) -> str:
    h = hashlib.sha256()
    h.update(json.dumps(architecture, sort_keys=True, default=str).encode("utf-8", errors="ignore"))
    h.update(extra.encode("utf-8", errors="ignore"))
    for f in formulas[:8]:
        h.update((f or "").encode("utf-8", errors="ignore"))
    return h.hexdigest()


async def run_academic_to_alpha_pipeline(params: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    End-to-end feed. ``params`` may include:

    - Same keys as ``AcademicResearcherSkill`` (``paper_url``, ``theory_term``, ``force_mock``, …).
    - ``bar`` (default ``1H``), ``lookback_bars`` (default 750 ≈ 31d hourly),
      ``symbols``, ``z_threshold``, ``promote_min_win_rate``, ``promote_min_trades``.

    Returns ``backtest`` metrics, ``factor_py`` path, ``promoted_to_library`` bool.
    """
    params = dict(params or {})
    from skills.sk_academic_researcher import AcademicResearcherSkill
    from skills import sk_paper_to_alpha as pta
    from trading.backtest_engine import BacktestConfig, run_backtest_with_factor_file

    academic = await AcademicResearcherSkill().run(params)
    if not academic.get("ok"):
        return {
            "ok": False,
            "stage": "academic",
            "detail": academic,
        }

    formulas = list(academic.get("formulas_extracted") or [])
    arch = academic.get("architecture") or {}
    fp = _fingerprint_parts(
        formulas,
        arch,
        str(params.get("paper_url") or params.get("theory_term") or ""),
    )

    try:
        out_path = pta.write_paper_factor_skill_file(
            formulas, arch, fp, skills_dir=REPO_ROOT / "skills"
        )
    except OSError as e:
        logger.exception("write factor file: %s", e)
        return {"ok": False, "stage": "emit_factor", "error": str(e), "academic": academic}

    hp = arch.get("hyperparams") if isinstance(arch.get("hyperparams"), dict) else {}
    frag = 16
    try:
        frag = int(hp.get("fragment_len", 16))
    except (TypeError, ValueError):
        frag = 16

    strategy_params = {
        "fragment_len": frag,
        "z_threshold": float(params.get("z_threshold", 0.4)),
    }

    cfg = BacktestConfig(
        bar=str(params.get("bar") or "1H"),
        lookback_bars=int(params.get("lookback_bars") or 750),
        symbols=list(params.get("symbols") or ["BTCUSDT", "ETHUSDT"]),
    )

    try:
        result = await run_backtest_with_factor_file(
            strategy_params,
            out_path,
            config=cfg,
        )
    except Exception as e:
        logger.exception("paper alpha backtest: %s", e)
        return {
            "ok": False,
            "stage": "backtest",
            "error": str(e),
            "academic": academic,
            "factor_py": str(out_path.relative_to(REPO_ROOT)).replace("\\", "/"),
        }

    metrics = result.to_dict()
    win_rate = float(metrics.get("win_rate") or 0.0)
    min_wr = float(params.get("promote_min_win_rate") or 48.0)
    min_trades = int(params.get("promote_min_trades") or 6)
    promoted = (
        result.total_trades >= min_trades
        and win_rate >= min_wr
        and result.sharpe_test > 0.0
    )

    rel = out_path.relative_to(REPO_ROOT).as_posix()
    if promoted:
        try:
            import skill_library as sl

            raw_kw = params.get("theory_term") or params.get("paper_url") or "paper_alpha"
            kws = re.findall(r"[\w\u4e00-\u9fff]+", str(raw_kw).lower())[:12]
            if not kws:
                kws = ["paper", "alpha", "arxiv"]
            title = str(arch.get("display_name") or arch.get("architecture_id") or out_path.stem)[
                :120
            ]
            sid = out_path.stem
            sl.register_or_update_factor_skill(
                skill_id=sid,
                title=title,
                keywords=kws + ["paper_to_alpha", "academic"],
                user_request=json.dumps(
                    {"formulas": formulas[:5], "architecture_id": arch.get("architecture_id")},
                    ensure_ascii=False,
                )[:500],
                py_relpath=rel,
            )
        except Exception as e:
            logger.warning("skill_library promote failed: %s", e)
            promoted = False

    return {
        "ok": True,
        "stage": "complete",
        "academic": {
            "source": academic.get("source"),
            "paper_count": academic.get("paper_count"),
            "formula_count": len(formulas),
        },
        "factor_py": rel,
        "backtest": metrics,
        "win_rate_report": win_rate,
        "promoted_to_library": promoted,
        "promote_thresholds": {"min_win_rate": min_wr, "min_trades": min_trades},
    }
