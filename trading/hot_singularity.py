"""
Pick best harness/singularity_isolated run by metrics.json and load model.pt + arch.json.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from .tfta_model import load_singularity_bundle

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ISOLATED_ROOT = PROJECT_ROOT / "harness" / "singularity_isolated"


def _score_metrics(m: dict) -> float:
    wr = float(m.get("val_win_rate", 0) or 0)
    vl = float(m.get("val_loss", 1.0) or 1.0)
    return wr - 0.12 * vl


def find_best_run(
    isolated_root: Optional[Path] = None,
    require_weights: bool = True,
) -> Optional[Path]:
    root = Path(isolated_root or DEFAULT_ISOLATED_ROOT)
    if not root.is_dir():
        return None

    best: Optional[Path] = None
    best_score = -1e18

    for d in sorted(root.iterdir()):
        if not d.is_dir() or not d.name.startswith("run_"):
            continue
        mp = d / "metrics.json"
        if not mp.exists():
            continue
        if require_weights and not (d / "model.pt").exists():
            continue
        try:
            metrics = json.loads(mp.read_text(encoding="utf-8"))
        except Exception:
            continue
        sc = _score_metrics(metrics)
        if sc > best_score:
            best_score = sc
            best = d

    if best:
        log.info("Best singularity run: %s (score=%.4f)", best.name, best_score)
    return best


def load_best_bundle(
    isolated_root: Optional[Path] = None,
    device: Optional[str] = None,
) -> Optional[dict]:
    run = find_best_run(isolated_root, require_weights=True)
    if not run:
        hint = find_best_run(isolated_root, require_weights=False)
        if hint:
            log.warning(
                "Found %s but no model.pt — re-run singularity training to export weights.",
                hint.name,
            )
        return None
    return load_singularity_bundle(run, device=device)
