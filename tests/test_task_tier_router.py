"""Unit tests for providers_router task-tier classification."""

import pytest

from providers_router import TaskTier, classify_task_tier, resolve_models_for_tier


@pytest.mark.parametrize(
    "msg,has_image,expected",
    [
        ("把下面这段格式化成 JSON", False, TaskTier.FAST),
        ("extract emails from this text", False, TaskTier.FAST),
        ("一句话总结", False, TaskTier.FAST),
        ("设计一个多步骤交易策略并回测", False, TaskTier.HEAVY),
        ("analyze the architecture of this system", False, TaskTier.HEAVY),
        ("hi", False, TaskTier.FAST),
        ("x" * 950, False, TaskTier.HEAVY),
        ("short", True, TaskTier.HEAVY),
    ],
)
def test_classify_task_tier(msg, has_image, expected):
    assert classify_task_tier(msg, has_image=has_image) == expected


def test_resolve_models_fast_uses_config(monkeypatch):
    monkeypatch.setattr(
        "providers_router.config.TASK_TIER_FAST_CLAUDE", "claude-test-fast"
    )
    monkeypatch.setattr(
        "providers_router.config.TASK_TIER_FAST_OPENAI", "gpt-test-fast"
    )
    c, o = resolve_models_for_tier(TaskTier.FAST)
    assert c == "claude-test-fast"
    assert o == "gpt-test-fast"


def test_resolve_models_heavy_falls_back_to_main(monkeypatch):
    monkeypatch.setattr("providers_router.config.TASK_TIER_HEAVY_CLAUDE", None)
    monkeypatch.setattr("providers_router.config.TASK_TIER_HEAVY_OPENAI", None)
    monkeypatch.setattr("providers_router.config.CLAUDE_MODEL", "claude-main")
    monkeypatch.setattr("providers_router.config.OPENAI_MODEL", "gpt-main")
    c, o = resolve_models_for_tier(TaskTier.HEAVY)
    assert c == "claude-main"
    assert o == "gpt-main"
