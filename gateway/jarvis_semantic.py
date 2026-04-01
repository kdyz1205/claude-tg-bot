"""
Lightweight plain-text routing for the gateway (no nested asyncio.run).

Heuristic intent first; optional ``JARVIS_INTENT_LLM=1`` + OpenAI/Anthropic reclassifies
messages that would otherwise be CHAT (small JSON classification call).

v2: broader quant / 造物 routing, secondary spec detector, ``reasoning`` on every path.

CHAT 对话经 ``claude_cli_tunnel.PersistentClaudeCLI`` 的 **chat 队列** 调用
``claude_agent.jarvis_gateway_cli_chat``（内部仍为 ``claude -p`` + ``--resume``；与造物 dev 任务双轨，chat 优先）。
AUTO_DEV / 造物走 ``pipeline.cli_bridge.run_claude_dev_prompt`` 的 **dev 队列**（默认 ``CLAUDE_CLI_TUNNEL_DEV=1``）。
关闭隧道：``CLAUDE_CLI_TUNNEL_CHAT=0`` / ``CLAUDE_CLI_TUNNEL_DEV=0``。
流式 NDJSON（额度恢复后便于 TG 逐段编辑）：``JARVIS_CHAT_STREAM_JSON=1``（``--output-format stream-json`` + ``--verbose``）；
可选 ``CLAUDE_STREAM_INCLUDE_PARTIAL=1`` 打开 ``--include-partial-messages``。
可选 ``JARVIS_CHAT_TIMEOUT_SEC``（秒，默认参考 ``config.API_REQUEST_TIMEOUT_SEC``，上限 600）。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from collections import defaultdict
from typing import Any

logger = logging.getLogger(__name__)

_AUTO_DEV_HINT = re.compile(
    r"(写代码|编程|实现|重构|修复\s*bug|AUTO_DEV|自动开发|代码库|加一个\s*\w+|"
    r"脚手架|模块|接口|单测|单元测试|CI|Dockerfile|README|bugfix|BUG|报错|traceback|"
    r"deploy|refactor|implement|fix\s+the|add\s+a\s+feature)",
    re.I,
)

# Quant factor / strategy phrasing → AUTO_DEV + sub_intent FACTOR_FORGE
_FACTOR_FORGE_HINT = re.compile(
    r"(因子|策略|量化|alpha|ALPHA|择时|多空|回测|信号|指标|背离|动量|均线|"
    r"VWAP|vwap|MACD|macd|RSI|rsi|布林带|KDJ|kdj|夏普|波动率|"
    r"factor|strategy|backtest|signal|indicator|portfolio|optimization|"
    r"mean\s*reversion|pairs?\s*trading|cointegration|z-?score|bollinger|atr|obv|"
    r"协整|套利|对冲|基差|期现|止损|止盈|入场|出场|仓位|杠杆|阈值|触发|"
    r"金叉|死叉|超买|超卖|网格|凯利|Kelly|"
    r"特征工程|特征|label|标签|训练集|验证集|过拟合|walk\s*forward|样本外|"
    r"论文|arxiv|文献|摘要|复现|开源策略|开源代码|"
    r"机器学习|LSTM|lstm|transformer|XGBoost|xgboost|lightgbm)",
    re.I,
)

# 二次检测：像「规则/公式/代码」描述，避免复杂设想被误标成纯聊天
_CODE_OR_DATA_HINT = re.compile(
    r"(def\s+\w+|import\s+numpy|import\s+pandas|from\s+pandas|pd\.|np\.|"
    r"DataFrame|dataframe|rolling\(|\.pct_change|corr\(|cov\(|"
    r"if\s+.+[<>=]{1,2}.+\d|return\s+[\d.]+)",
    re.I,
)
_RULE_LIKE_HINT = re.compile(
    r"(阈值|周期|窗口|参数|公式|条件|当.+时|大于|小于|等于|"
    r">=\s*\d|<=\s*\d|=\s*\d+\.?\d*|%\s*时|\d+\s*%|\d+\s*bp)",
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

# 配置总线：改写 session_commander_config.json 白名单键 + 炼丹入队
_LAB_EVOLVER_HINT = re.compile(
    r"(启动炼丹|开始炼丹|启动进化|无限进化|跑\s*evolver|infinite\s*evolver|科研\s*挂机|"
    r"开\s*实验室)",
    re.I,
)

_TRADE_HINT = re.compile(
    r"(平仓|开仓|加仓|减仓|止损单|止盈|市价|限价|抄底|逃顶|"
    r"买入|卖出|买进|沽出|做多|做空|清仓|全平|"
    r"买\s*\d|卖\s*\d|"
    r"\bBUY\b|\bSELL\b|\bCLOSE\b|\bLONG\b|\bSHORT\b)",
    re.I,
)


def extract_wallet_clone_address(text: str) -> str | None:
    m = _EVM_ADDRESS_IN_TEXT.search(text or "")
    return m.group(0).lower() if m else None


def llm_backend_configured() -> bool:
    return bool(
        (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
        or (os.environ.get("OPENAI_API_KEY") or "").strip()
    )


def intent_llm_refinement_enabled() -> bool:
    return (os.environ.get("JARVIS_INTENT_LLM") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


_INTENT_LLM_SYSTEM = """You classify Telegram user messages for a quant/trading gateway bot.
Reply with ONLY a JSON object (no markdown) with keys:
- intent: one of CHAT, AUTO_DEV, TRADE
- sub_intent: null, or FACTOR_FORGE (only when intent is AUTO_DEV and the user is describing a trading signal, factor, indicator math, strategy rules, backtest, alpha, or paper/code reproduction — not generic software)
- reasoning: one short English phrase that MUST state whether you considered hidden trading/quant logic (e.g. "no quant content, small talk" or "describes RSI rule → factor")

Rules:
- Before choosing CHAT, explicitly rule out that the user might be describing strategy, indicators, risk rules, or code for markets.
- TRADE: user wants an immediate order action (buy/sell/close this position now), not describing research.
- AUTO_DEV + FACTOR_FORGE: quantitative / trading logic to implement as code.
- AUTO_DEV without FACTOR_FORGE: general coding or repo changes unrelated to factors.
- CHAT: greetings, off-topic, or too ambiguous to route to code or trade.
When unsure, prefer CHAT."""


def _normalize_llm_intent_payload(
    data: dict[str, Any], raw_text: str
) -> dict[str, Any] | None:
    intent = str(data.get("intent") or "CHAT").upper()
    if intent == "CHAT":
        return None
    reasoning = "llm_refine:" + str(data.get("reasoning") or "")[:220]
    if intent == "TRADE":
        return {
            "intent": "TRADE",
            "extracted_requirement": raw_text,
            "reasoning": reasoning,
        }
    if intent == "AUTO_DEV":
        out: dict[str, Any] = {
            "intent": "AUTO_DEV",
            "extracted_requirement": raw_text,
            "reasoning": reasoning,
        }
        sub = str(data.get("sub_intent") or "").upper()
        if sub == "FACTOR_FORGE":
            out["sub_intent"] = SUB_INTENT_FACTOR_FORGE
        return out
    return None


async def _llm_refine_intent_after_chat_heuristic(text: str, *, uid: int) -> dict[str, Any] | None:
    """
    When regex heuristics fell through to CHAT, optionally ask a small LLM once.
    Enable with JARVIS_INTENT_LLM=1 and OPENAI_API_KEY or ANTHROPIC_API_KEY.
    Model: JARVIS_INTENT_MODEL (default gpt-4o-mini or claude-3-5-haiku-20241022).
    """
    if not intent_llm_refinement_enabled() or not llm_backend_configured():
        return None
    t = (text or "").strip()
    if len(t) < 12:
        return None

    user_block = f"uid={uid}\n\n{t[:8000]}"

    try:
        if (os.environ.get("OPENAI_API_KEY") or "").strip():
            from openai import AsyncOpenAI

            model = (os.environ.get("JARVIS_INTENT_MODEL") or "gpt-4o-mini").strip()
            client = AsyncOpenAI()
            completion = await client.chat.completions.create(
                model=model,
                temperature=0,
                messages=[
                    {"role": "system", "content": _INTENT_LLM_SYSTEM},
                    {"role": "user", "content": user_block},
                ],
                response_format={"type": "json_object"},
            )
            raw = (completion.choices[0].message.content or "").strip() or "{}"
            data = json.loads(raw)
            if not isinstance(data, dict):
                return None
            return _normalize_llm_intent_payload(data, t)

        if (os.environ.get("ANTHROPIC_API_KEY") or "").strip():
            from anthropic import AsyncAnthropic

            model = (
                os.environ.get("JARVIS_INTENT_MODEL") or "claude-3-5-haiku-20241022"
            ).strip()
            client = AsyncAnthropic()
            msg = await client.messages.create(
                model=model,
                max_tokens=256,
                system=_INTENT_LLM_SYSTEM,
                messages=[{"role": "user", "content": user_block}],
            )
            parts: list[str] = []
            for block in msg.content:
                if getattr(block, "type", None) == "text":
                    parts.append(getattr(block, "text", "") or "")
            raw = "".join(parts).strip() or "{}"
            data = json.loads(raw)
            if not isinstance(data, dict):
                return None
            return _normalize_llm_intent_payload(data, t)
    except Exception:
        logger.debug("JARVIS_INTENT_LLM refine failed", exc_info=True)
    return None


_locks: defaultdict[int, asyncio.Lock] = defaultdict(asyncio.Lock)


def try_config_bus_intent(text: str) -> dict[str, Any] | None:
    """配置总线 + 炼丹入队；挂载策略时写 ``active_skills``。"""
    t = (text or "").strip()
    if not t:
        return None
    patch: dict[str, Any] = {}
    if re.search(
        r"(启用|打开|开启).{0,20}jarvis\s*自动消费|自动消费\s*(打开|启用|开|on)",
        t,
        re.I,
    ):
        patch["jarvis_auto_consume"] = True
    elif re.search(
        r"(禁用|关闭).{0,20}jarvis\s*自动消费|自动消费\s*(关|关闭|off)",
        t,
        re.I,
    ):
        patch["jarvis_auto_consume"] = False
    if re.search(
        r"(开启|打开)\s*dry\s*run|dry\s*run\s*on|演练模式|只记录不点|干跑",
        t,
        re.I,
    ):
        patch["dry_run"] = True
    elif re.search(
        r"(关闭|取消)\s*dry\s*run|dry\s*run\s*off|真实点击|实盘操作",
        t,
        re.I,
    ):
        patch["dry_run"] = False
    if re.search(r"(清空雷达技能|取消挂载|雷达\s*默认|不用固定技能)", t, re.I):
        patch["active_skills"] = []
    elif re.search(
        r"(挂载|切换|雷达|使用技能|固定技能|运行策略|执行策略|用上)", t, re.I
    ):
        m = re.search(r"\b(sk_[a-zA-Z0-9_]{4,})\b", t)
        if m:
            patch["active_skills"] = [m.group(1)]
    lab_prompt: str | None = None
    if _LAB_EVOLVER_HINT.search(t):
        lab_prompt = t[:2000]
    if patch or lab_prompt is not None:
        return {
            "intent": "CONFIG_BUS",
            "config_patch": patch,
            "lab_prompt": lab_prompt,
            "reasoning": "config_bus_keywords",
        }
    return None


def update_config_active_skill(skill_name: str | None) -> tuple[bool, str]:
    """覆写 ``active_skills`` 并触发 God ``reload_skills``（经 config_bus）。"""
    from gateway.config_bus import apply_safe_config_patch

    name = (skill_name or "").strip()
    if name:
        return apply_safe_config_patch({"active_skills": [name]})
    return apply_safe_config_patch({"active_skills": []})


def maybe_mount_skill_after_auto_dev(text: str, req: str) -> tuple[bool, str]:
    """AUTO_DEV 语义里若明确要求挂载某 ``sk_*``，实权写入配置。"""
    blob = f"{(text or '').strip()}\n{(req or '').strip()}"
    if not re.search(
        r"(部署|挂载|实盘运行|切换为|用此策略).{0,24}sk_", blob, re.I
    ):
        return False, "skip"
    m = re.search(r"\b(sk_[a-zA-Z0-9_]{4,})\b", blob)
    if not m:
        return False, "no_skill_id"
    return update_config_active_skill(m.group(1))


def user_semantic_lock(uid: int) -> asyncio.Lock:
    return _locks[int(uid)]


def _numeric_token_count(text: str) -> int:
    return len(re.findall(r"\d+\.?\d*", text or ""))


def _looks_like_quant_algorithm_spec(t: str) -> bool:
    """
    在首轮未命中显式关键词时，用「长度 + 数字密度 + 规则/代码形态」兜底进 FACTOR_FORGE。
    刻意要求偏严，减少日常闲聊误触发。
    """
    s = (t or "").strip()
    if len(s) < 28:
        return False
    nums = _numeric_token_count(s)
    has_quant_anchor = bool(_FACTOR_FORGE_HINT.search(s))
    has_code = bool(_CODE_OR_DATA_HINT.search(s))
    has_rule = bool(_RULE_LIKE_HINT.search(s))
    if has_code:
        return True
    if has_quant_anchor and nums >= 2 and (has_rule or len(s) >= 48):
        return True
    if nums >= 4 and has_rule and len(s) >= 40:
        return True
    return False


async def classify_intent(text: str, *, uid: int) -> dict[str, Any]:
    t = (text or "").strip()
    _cb = try_config_bus_intent(t)
    if _cb is not None:
        return _cb
    if _CHAOS_IMMUNITY_HINT.search(t):
        return {
            "intent": "CHAOS_IMMUNITY",
            "extracted_requirement": t,
            "reasoning": "regex_chaos_immunity",
        }
    if _WALLET_CLONE_HINT.search(t) and extract_wallet_clone_address(t):
        return {
            "intent": "WALLET_CLONE",
            "extracted_address": extract_wallet_clone_address(t),
            "extracted_requirement": t,
            "reasoning": "regex_wallet_clone_with_address",
        }
    if re.search(r"(运行|执行|切换为|用上).{0,12}", t, re.I) and re.search(
        r"\bsk_[a-zA-Z0-9_]{4,}\b", t, re.I
    ):
        m = re.search(r"\b(sk_[a-zA-Z0-9_]{4,})\b", t)
        if m:
            return {
                "intent": "RUN_SKILL",
                "skill_id": m.group(1),
                "extracted_requirement": t,
                "reasoning": "regex_run_skill_mount",
            }
    if _FACTOR_FORGE_HINT.search(t):
        return {
            "intent": "AUTO_DEV",
            "sub_intent": SUB_INTENT_FACTOR_FORGE,
            "extracted_requirement": t,
            "reasoning": "regex_factor_forge_primary",
        }
    if _AUTO_DEV_HINT.search(t):
        return {
            "intent": "AUTO_DEV",
            "extracted_requirement": t,
            "reasoning": "regex_auto_dev",
        }
    if _TRADE_HINT.search(t):
        return {
            "intent": "TRADE",
            "extracted_requirement": t,
            "reasoning": "regex_trade_keywords",
        }
    if _looks_like_quant_algorithm_spec(t):
        return {
            "intent": "AUTO_DEV",
            "sub_intent": SUB_INTENT_FACTOR_FORGE,
            "extracted_requirement": t,
            "reasoning": "secondary_quant_spec_heuristic",
        }
    refined = await _llm_refine_intent_after_chat_heuristic(t, uid=uid)
    if refined is not None:
        return refined
    return {
        "intent": "CHAT",
        "extracted_requirement": t,
        "reasoning": "no_match_default_chat",
    }


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


# 网关日常对话：经本地 Claude Code CLI（与 claude_agent 同栈，无计费 HTTP complete_turn）
_JARVIS_CHAT_SYSTEM = """你是 Jarvis，长官的专属高频量化作战副官。

风格（强制）：
- 极度冷酷、专业、极简；华尔街宽客 / systematic desk 口吻。
- 拒绝废话、拒绝寒暄、拒绝堆叠表情符号（非必要不用）。
- 长官用中文提问时，用中文作答；英文则英文。保持同一冷峻密度。
- 不编造实时行情或持仓；无依据时直说「无数据 / 未接入」。
- 涉及下单、实盘执行时提醒：以主控交易流与风控为准，网关对话不构成指令。

回答长度默认控制在必要最小；若长官明确要求深度推演再展开。"""

_JARVIS_CHAT_ERR_USER = "本地 CLI 未就绪、超时或无输出；请确认本机已登录 Claude Code 并可用 `claude` 命令。"
_JARVIS_CHAT_ERR_AUTH = (
    "本地 CLI 未登录或订阅会话失效。请在终端运行 `claude` 完成登录，并确认订阅有效。"
)
_JARVIS_CHAT_ERR_CLI = "未检测到本机 `claude` 可执行文件。请安装 Claude Code CLI 或将其加入 PATH。"
_JARVIS_CHAT_ERR_TIMEOUT = "本地 CLI 响应超时。请稍后重试或检查本机网络与订阅状态。"

# 与 agents.sessions 的 8_000_000+ 虚拟 id 错开，按 Telegram uid 稳定映射多轮记忆
_JARVIS_GW_CHAT_BASE = 71_000_000
_JARVIS_GW_CHAT_MOD = 89_000_000


def _jarvis_gateway_chat_id(uid: int) -> int:
    return _JARVIS_GW_CHAT_BASE + (abs(int(uid)) % _JARVIS_GW_CHAT_MOD)


def _jarvis_chat_timeout_sec() -> float:
    try:
        raw = (os.environ.get("JARVIS_CHAT_TIMEOUT_SEC") or "").strip()
        if raw:
            return max(15.0, min(600.0, float(raw)))
    except (TypeError, ValueError):
        pass
    try:
        import config

        return max(30.0, float(getattr(config, "API_REQUEST_TIMEOUT_SEC", 120.0)))
    except Exception:
        return 120.0


async def chat_reply(text: str, *, uid: int) -> tuple[str, str]:
    """
    网关 CHAT 意图：本地 ``claude -p`` 一轮（per-uid 合成 chat_id + CLI ``--resume`` 记忆）。

    成功: (reply_text, "")
    失败: ("", user_message) — 按场景给出订阅 / CLI 提示；细节写日志。
    """
    t = (text or "").strip()
    if not t:
        return "", ""

    import claude_agent

    chat_id = _jarvis_gateway_chat_id(uid)
    timeout_sec = _jarvis_chat_timeout_sec()

    try:
        reply, diag = await claude_agent.jarvis_gateway_cli_chat(
            _JARVIS_CHAT_SYSTEM,
            t[:120_000],
            chat_id=chat_id,
            timeout_sec=timeout_sec,
        )
    except asyncio.TimeoutError:
        logger.warning("jarvis chat_reply asyncio.TimeoutError uid=%s", uid)
        return "", _JARVIS_CHAT_ERR_TIMEOUT
    except Exception as e:
        logger.exception("jarvis chat_reply transport uid=%s: %s", uid, e)
        return "", _JARVIS_CHAT_ERR_USER

    if diag == "auth":
        logger.warning("jarvis chat_reply CLI auth uid=%s chat_id=%s", uid, chat_id)
        return "", _JARVIS_CHAT_ERR_AUTH
    if diag == "cli_missing":
        logger.warning("jarvis chat_reply CLI missing uid=%s chat_id=%s", uid, chat_id)
        return "", _JARVIS_CHAT_ERR_CLI
    if diag == "timeout":
        logger.warning("jarvis chat_reply CLI timeout uid=%s chat_id=%s", uid, chat_id)
        return "", _JARVIS_CHAT_ERR_TIMEOUT
    if diag in ("empty", "error"):
        logger.warning(
            "jarvis chat_reply CLI fail uid=%s chat_id=%s diag=%s",
            uid,
            chat_id,
            diag,
        )
        return "", _JARVIS_CHAT_ERR_USER
    if diag:
        logger.warning("jarvis chat_reply unknown diag uid=%s: %s", uid, diag)
        return "", _JARVIS_CHAT_ERR_USER

    out = (reply or "").strip()
    if not out:
        logger.warning("jarvis chat_reply empty assistant uid=%s chat_id=%s", uid, chat_id)
        return "", _JARVIS_CHAT_ERR_USER

    return out, ""
