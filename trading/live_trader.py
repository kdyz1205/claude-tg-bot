"""
Canonical live trader implementation lives at repository root: ``live_trader.py``.
This module re-exports it so ``from trading.live_trader import LiveTrader`` works.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_root_lt = Path(__file__).resolve().parent.parent / "live_trader.py"
_spec = importlib.util.spec_from_file_location("_repo_live_trader", _root_lt)
if _spec is None or _spec.loader is None:
    raise ImportError("Cannot load repository root live_trader.py")
_mod = importlib.util.module_from_spec(_spec)
sys.modules.setdefault("_repo_live_trader", _mod)
_spec.loader.exec_module(_mod)

globals().update(
    {k: v for k, v in _mod.__dict__.items() if not k.startswith("_") or k in ("__all__",)}
)

__all__ = getattr(_mod, "__all__", [k for k in _mod.__dict__ if not k.startswith("_")])
