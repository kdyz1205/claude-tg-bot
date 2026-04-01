import pytest

from pipeline.auto_dev_orchestrator import _safe_rel_path, _syntax_ok, _extract_python


def test_safe_rel_rejects_parent():
    with pytest.raises(ValueError):
        _safe_rel_path("../etc/passwd")


def test_syntax_ok():
    assert _syntax_ok("def f():\n    return 1\n")[0]


def test_extract_python_fence():
    src = _extract_python("```python\nx = 1\n```")
    assert "x = 1" in src
