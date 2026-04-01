import pytest

from gateway.jarvis_semantic import (
    SUB_INTENT_FACTOR_FORGE,
    build_factor_forge_prompt,
    classify_intent,
)


@pytest.mark.asyncio
async def test_classify_factor_forge_vwap_strategy():
    row = await classify_intent("帮我写个VWAP背离策略", uid=1)
    assert row["intent"] == "AUTO_DEV"
    assert row.get("sub_intent") == SUB_INTENT_FACTOR_FORGE
    assert "VWAP" in row["extracted_requirement"]


@pytest.mark.asyncio
async def test_classify_auto_dev_no_sub_intent_for_bugfix():
    row = await classify_intent("修复 bug 在登录流程", uid=1)
    assert row["intent"] == "AUTO_DEV"
    assert row.get("sub_intent") is None


@pytest.mark.asyncio
async def test_classify_factor_over_generic_auto_dev_when_both_match():
    """Factor hint is checked first; '编程' alone should not add sub_intent."""
    row = await classify_intent("编程实现一个RSI超买因子", uid=1)
    assert row["intent"] == "AUTO_DEV"
    assert row.get("sub_intent") == SUB_INTENT_FACTOR_FORGE


def test_build_factor_forge_prompt_contains_contract():
    p = build_factor_forge_prompt("测试需求")
    assert "BaseSkill" in p
    assert "skills/base_skill.py" in p
    assert "buy_confidence" in p
    assert "sell_confidence" in p
    assert "skills/" in p and "sk_" in p
    assert "测试需求" in p
