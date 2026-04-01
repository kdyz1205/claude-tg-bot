"""
Static AST scan for untrusted / AI-generated strategy code.

Flags environment exfiltration and private-material access patterns so the
inspector can quarantine files before they run in production.
"""
from __future__ import annotations

import ast
import json
import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

logger = logging.getLogger(__name__)

# Suspicious string snippets (hex keys, PEM, common secret env names)
_SECRET_STRING_PATTERNS = (
    re.compile(r"BEGIN\s+(RSA\s+)?PRIVATE\s+KEY", re.I),
    # Avoid self.api_key = … / cls.api_key = … (legitimate attribute assigns)
    re.compile(r"(?<![.\w])api[_-]?key\s*=", re.I),
    re.compile(r"secret[_-]?key\s*=", re.I),
    re.compile(r"\b0x[a-fA-F0-9]{64}\b"),  # raw hex private key-ish
    re.compile(r"\bmnemonic\b", re.I),
)


@dataclass
class PolicyViolation:
    path: str
    line: int
    rule: str
    detail: str


@dataclass
class ScanReport:
    scanned: list[str] = field(default_factory=list)
    violations: list[PolicyViolation] = field(default_factory=list)


def _is_os_name(node: ast.AST) -> bool:
    return isinstance(node, ast.Name) and node.id == "os"


def _is_path_open_to_env_file(call: ast.Call) -> bool:
    if not isinstance(call.func, ast.Name) or call.func.id != "open":
        return False
    if not call.args:
        return False
    arg0 = call.args[0]
    if isinstance(arg0, ast.Constant) and isinstance(arg0.value, str):
        s = arg0.value.replace("\\", "/").lower()
        return ".env" in s
    if isinstance(arg0, ast.JoinedStr):
        for p in arg0.values:
            if isinstance(p, ast.Constant) and isinstance(p.value, str) and ".env" in p.value.lower():
                return True
    return False


def _references_os_environ(tree: ast.AST) -> tuple[bool, int]:
    """True if code touches os.environ (attribute, subscript, or getattr)."""
    line = 0
    for n in ast.walk(tree):
        if isinstance(n, ast.Attribute) and n.attr == "environ" and _is_os_name(n.value):
            line = getattr(n, "lineno", 0) or 0
            return True, line
        if isinstance(n, ast.Subscript):
            v = n.value
            if isinstance(v, ast.Attribute) and v.attr == "environ" and _is_os_name(v.value):
                line = getattr(n, "lineno", 0) or 0
                return True, line
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "getattr":
            args = n.args
            if len(args) >= 2 and _is_os_name(args[0]):
                key = args[1]
                if isinstance(key, ast.Constant) and key.value == "environ":
                    line = getattr(n, "lineno", 0) or 0
                    return True, line
    return False, 0


def scan_source(source: str, *, rel_path: str = "<string>") -> list[PolicyViolation]:
    """Return violations for one file's source."""
    out: list[PolicyViolation] = []
    try:
        tree = ast.parse(source, filename=rel_path)
    except SyntaxError as se:
        out.append(PolicyViolation(rel_path, getattr(se, "lineno", 0) or 0, "syntax_error", str(se)[:200]))
        return out

    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _is_path_open_to_env_file(node):
            out.append(
                PolicyViolation(
                    rel_path,
                    getattr(node, "lineno", 0) or 0,
                    "open_dotenv",
                    "open() targeting .env or env file path",
                )
            )

    has_env, env_line = _references_os_environ(tree)
    if has_env:
        out.append(
            PolicyViolation(
                rel_path,
                env_line,
                "os_environ",
                "Access to os.environ (forbidden in AI strategy surface)",
            )
        )

    for i, line in enumerate(source.splitlines(), start=1):
        for rx in _SECRET_STRING_PATTERNS:
            if rx.search(line):
                out.append(
                    PolicyViolation(
                        rel_path,
                        i,
                        "secret_pattern",
                        f"Line matches sensitive pattern: {rx.pattern[:40]}...",
                    )
                )
                break

    return out


def iter_strategy_paths(
    root: Path,
    extra_globs: Iterable[str] | None = None,
) -> list[Path]:
    """Collect candidate strategy/skill Python files under the repo."""
    root = root.resolve()
    paths: list[Path] = []

    skills = root / "skills"
    if skills.is_dir():
        paths.extend(p for p in skills.glob("*.py") if p.is_file() and p.name != "__init__.py")
    trading = root / "trading"
    if trading.is_dir():
        paths.extend(p for p in trading.glob("*.py") if p.is_file() and p.name != "__init__.py")
    ps = root / "pro_strategy.py"
    if ps.is_file():
        paths.append(ps)
    alpha = root / "_alpha_library"
    if alpha.is_dir():
        paths.extend(p for p in alpha.rglob("*.py") if p.is_file() and p.name != "__init__.py")

    for g in extra_globs or []:
        for p in root.glob(g):
            if p.is_file() and p.suffix == ".py" and p.name != "__init__.py":
                paths.append(p)

    seen: set[Path] = set()
    unique: list[Path] = []
    for p in sorted(paths, key=lambda x: str(x).lower()):
        rp = p.resolve()
        if rp not in seen:
            seen.add(rp)
            unique.append(p)
    return unique


def scan_strategy_tree(root: Path, extra_globs: Iterable[str] | None = None) -> ScanReport:
    report = ScanReport()
    for path in iter_strategy_paths(root, extra_globs):
        try:
            src = path.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            logger.warning("security_ast: skip %s: %s", path, exc)
            continue
        try:
            rel = str(path.relative_to(root))
        except ValueError:
            rel = str(path)
        report.scanned.append(rel)
        report.violations.extend(scan_source(src, rel_path=rel))
    return report


def quarantine_file(path: Path, quarantine_dir: Path, reason: str) -> Path | None:
    """
    Move file into quarantine_dir with a timestamp prefix. Returns new path or None.
    """
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    dest = quarantine_dir / f"{int(time.time())}_{path.name}"
    try:
        path.replace(dest)
        meta = dest.with_suffix(dest.suffix + ".quarantine.json")
        meta.write_text(
            json.dumps({"reason": reason, "original": str(path)}, indent=2),
            encoding="utf-8",
        )
        logger.error("security_ast: quarantined %s -> %s (%s)", path, dest, reason[:80])
        return dest
    except OSError as exc:
        logger.error("security_ast: quarantine failed for %s: %s", path, exc)
        return None
