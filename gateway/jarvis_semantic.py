"""
Lightweight plain-text routing for the gateway (no nested asyncio.run).

Heuristic intent when no LLM keys are configured; optional LLM can be wired later.
"""

from __future__ import annotations

import asyncio
import re
from collections import defaultdict
from typing import Any

_AUTO_DEV_HINT = re.compile(
    r"(写代码|编程|实现|重构|修复\s*bug|AUTO_DEV|自动开发|代码库|加一个\s*\w+)",
    re.I,
)

# Quant factor / strategy phrasing → AUTO_DEV + sub_intent FACTOR_FORGE (checked before generic AUTO_DEV).
_FACTOR_FORGE_HINT = re.compile(
    r"(因子|策略|量化|alpha|ALPHA|择时|多空|回测|信号|指标|背离|动量|均线|"
    r"VWAP|vwap|MACD|macd|RSI|rsi|布林带|KDJ|kdj|夏普|波动率|"
    r"factor|strategy|backtest|signal|indicator)",
    re.I,
)

SUB_INTENT_FACTOR_FORGE = "FACTOR_FORGE"

# 对手盘行为克隆：自然语言 + 0x 地址（优先于因子/自动开发匹配）
_EVM_ADDRESS_IN_TEXT = re.compile(r"0x[a-fA-F0-9]{40}")
_WALLET_CLONE_HINT = re.compile(
    r"(追踪并破解|对手盘行为克隆|行为克隆|克隆高手|克隆.*地址|破解地址|"
    r"跟单狙击|狙击因子克隆|破解.*钱包)",
    re.I,
)

_CHAOS_IMMUNITY_HINT = re.compile(
    r"(启动混沌测试|混沌测试|混沌猴|抗压免疫|系统级抗压|灾难演练|"
    r"chaos\s*test|resilience\s*test)",
    re.I,
)


def extract_wallet_clone_address(text: str) -> str | None:
    m = _EVM_ADDRESS_IN_TEXT.search(text or "")
    return m.group(0).lower() if m else None


def llm_backend_configured() -> bool:
    import os

    return bool(
        (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
        or (os.environ.get("OPENAI_API_KEY") or "").strip()
    )


_locks: defaultdict[int, asyncio.Lock] = defaultdict(asyncio.Lock)


def user_semantic_lock(uid: int) -> asyncio.Lock:
    return _locks[int(uid)]


async def classify_intent(text: str, *, uid: int) -> dict[str, Any]:
    t = (text or "").strip()
    if _CHAOS_IMMUNITY_HINT.search(t):
        return {
            "intent": "CHAOS_IMMUNITY",
            "extracted_requirement": t,
        }
    if _WALLET_CLONE_HINT.search(t) and extract_wallet_clone_address(t):
        return {
            "intent": "WALLET_CLONE",
            "extracted_address": extract_wallet_clone_address(t),
            "extracted_requirement": t,
        }
    if _FACTOR_FORGE_HINT.search(t):
        return {
            "intent": "AUTO_DEV",
            "sub_intent": SUB_INTENT_FACTOR_FORGE,
            "extracted_requirement": t,
        }
    if _AUTO_DEV_HINT.search(t):
        return {"intent": "AUTO_DEV", "extracted_requirement": t}
    if any(k in t.upper() for k in ("买", "卖", "平仓", "BUY", "SELL", "CLOSE")):
        return {"intent": "TRADE", "extracted_requirement": t}
    return {"intent": "CHAT", "extracted_requirement": t}


def build_factor_forge_prompt(user_requirement: str) -> str:
    """
    System-style instructions prepended for FACTOR_FORGE: one new BaseSkill under skills/ only.
    """
    req = (user_requirement or "").strip()
    return f"""你现在的任务是编写一个量化因子（Python 技能模块）。

硬性要求：
1. 必须继承 skills.base_skill.BaseSkill；先阅读 `skills/base_skill.py` 的契约（async run / _execute / 超时）。
2. 实现 `_execute(self, payload)`，返回值必须是 dict，且至少包含：
   - `buy_confidence`: float，买入信号置信度，范围 [0.0, 1.0]
   - `sell_confidence`: float，卖出信号置信度，范围 [0.0, 1.0]
   可根据因子逻辑补充其它字段（如 `reason`, `metadata`）。
3. 在模块内定义 `SKILL_CLASS = YourSkillClass`，`skill_id` 与文件名一致，使用 `sk_` 前缀。
4. 只允许在仓库根目录的 `skills/` 下新增或修改单个 `skills/sk_*.py` 文件；禁止修改、删除或创建任何其它路径下的文件（包括配置、测试、网关等）。
5. 可参考现有 `skills/sk_*.py` 的模块结构与 docstring 风格。

用户需求（自然语言）：
{req}
"""


async def execute_trade_from_user_text(
    text: str, *, uid: int, user_mode: str
) -> tuple[bool, str]:
    return False, "请在主机器人或专用交易流程中下单；网关面板仅展示与引擎控制。"


async def chat_reply(text: str, *, uid: int) -> tuple[str, str]:
    if llm_backend_configured():
        return "", "未接入对话模型实现（仅意图路由）；请用 /dev 或看板按钮。"
    return (
        "💬 已收到。配置 ANTHROPIC_API_KEY 或 OPENAI_API_KEY 后可启用完整对话；"
        "开发类需求请用 `/dev …` 或直接描述要改的代码。",
        "",
    )
