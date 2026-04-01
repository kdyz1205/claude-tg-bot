"""
Dynamic load / reload / unload for skills under ``skills/`` or arbitrary ``.py`` paths.

- Packages: ``importlib.import_module("skills.sk_academic_researcher")``
- Files: ``spec_from_file_location`` with stable synthetic module names
- ``run_skill_module_async`` prefers ``SKILL_CLASS`` (:class:`BaseSkill`) else ``run_skill``
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Type

from skills.base_skill import BaseSkill

logger = logging.getLogger(__name__)

# file_path -> module_name we injected into sys.modules (for unload)
_FILE_MODULE_REGISTRY: dict[str, str] = {}


def _is_base_skill_subclass(obj: Any) -> bool:
    return (
        isinstance(obj, type)
        and issubclass(obj, BaseSkill)
        and obj is not BaseSkill
    )


def load_skill_from_file(path: str | Path, module_name: str | None = None) -> Any:
    """
    Load or hot-reload a skill ``.py`` from disk. Second call for the same path reloads
    the existing synthetic module name.
    """
    file_path = Path(path).resolve()
    if not file_path.is_file():
        raise FileNotFoundError(file_path)
    key = str(file_path)
    if module_name is None:
        existing = _FILE_MODULE_REGISTRY.get(key)
        if existing is not None and existing in sys.modules:
            logger.info("Reloading skill module %s from %s", existing, file_path)
            return importlib.reload(sys.modules[existing])
        safe = "".join(c if c.isalnum() else "_" for c in file_path.stem)[:40]
        module_name = f"_skills_hot_{safe}_{abs(hash(key)) % 1_000_000}"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load skill from {file_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    _FILE_MODULE_REGISTRY[key] = module_name
    logger.info("Loaded skill module %s from %s", module_name, file_path)
    return mod


def reload_skill_module(module_name: str) -> Any:
    """Hot-reload a package module or a file-backed module already in ``sys.modules``."""
    if module_name not in sys.modules:
        return importlib.import_module(module_name)
    return importlib.reload(sys.modules[module_name])


def unload_skill_module(module_name: str) -> None:
    """
    Remove module from ``sys.modules`` (best-effort). May break if other code
    still holds references to the old module object.
    """
    keys = [
        k
        for k in list(sys.modules.keys())
        if k == module_name or k.startswith(module_name + ".")
    ]
    for k in keys:
        del sys.modules[k]
    for fp, name in list(_FILE_MODULE_REGISTRY.items()):
        if name == module_name or name.startswith(module_name + "."):
            _FILE_MODULE_REGISTRY.pop(fp, None)
    logger.info("Unloaded skill module(s): %s", module_name)


async def run_skill_module_async(
    module: str | Path,
    payload: Optional[Dict[str, Any]] = None,
    *,
    timeout_sec: float = 120.0,
    reload_first: bool = False,
) -> Any:
    """
    Run a skill from package name (``skills.sk_foo``) or ``Path`` to ``.py``.

    Uses ``SKILL_CLASS`` if present (instantiates once per call); otherwise ``run_skill``,
    always bounded by ``timeout_sec`` at the dispatch layer when using ``run_skill`` only
    (``BaseSkill.run`` applies class default for SKILL_CLASS path).
    """
    payload = payload or {}

    if isinstance(module, Path) or (isinstance(module, str) and str(module).endswith(".py")):
        path = Path(module)
        key = str(path.resolve())
        mod_name = _FILE_MODULE_REGISTRY.get(key)
        if mod_name is None:
            m = load_skill_from_file(path)
        elif reload_first:
            m = importlib.reload(sys.modules[mod_name])
        else:
            m = sys.modules[mod_name]
    else:
        name = str(module)
        if reload_first and name in sys.modules:
            m = importlib.reload(sys.modules[name])
        else:
            m = importlib.import_module(name)

    skill_cls = getattr(m, "SKILL_CLASS", None)
    if _is_base_skill_subclass(skill_cls):
        cls: Type[BaseSkill] = skill_cls
        inst = cls()
        return await inst.run(payload, timeout_sec=timeout_sec)

    run_fn = getattr(m, "run_skill", None)
    if run_fn is None:
        raise AttributeError(
            f"Module {getattr(m, '__name__', module)!r} has neither SKILL_CLASS (BaseSkill) nor run_skill"
        )
    return await asyncio.wait_for(run_fn(payload), timeout=timeout_sec)
