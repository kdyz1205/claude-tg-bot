"""
Canonical live trader: re-exports the repository root ``live_trader.py``.

Delta-neutral: root ``live_trader`` uses ``asyncio.gather(buy, short, return_exceptions=True)``
and ``okx_executor.limping_fuse_flatten_short`` for immediate naked-short rescue after failed DEX leg.
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

_skip = frozenset(
    {"__name__", "__file__", "__package__", "__loader__", "__spec__", "__doc__", "__builtins__"}
)
for _k, _v in _mod.__dict__.items():
    if _k in _skip:
        continue
    globals()[_k] = _v

del _k, _v, _skip, _spec, _mod, _root_lt
