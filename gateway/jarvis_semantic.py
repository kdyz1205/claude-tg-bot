"""
Lightweight plain-text routing for the gateway (no nested asyncio.run).

Heuristic intent first; optional ``JARVIS_INTENT_LLM=1`` + OpenAI/Anthropic reclassifies
messages that would otherwise be CHAT (small JSON classification call).

v2: broader quant / 造物 routing, secondary spec detector, ``reasoning`` on every path.

CHAT 经 ``PersistentClaudeCLI`` **chat 队列** → ``jarvis_gateway_cli_chat``。
默认 ``CLAUDE_CLI_PIPE_WORKER=1``：子进程 ``python -m claude_tunnel_worker`` 常驻，stdin/stdout JSONL 投递每轮
``claude -p``（仍为 Claude Code 官方非交互模型；子进程内串行）。``JARVIS_CHAT_STREAM_JSON=1`` 时走父进程流式，不经 pipe。
造物 dev 走 **dev 队列**；关闭隧道：``CLAUDE_CLI_TUNNEL_CHAT=0``。
``JARVIS_SHADOW_FIRST_BYTE_SEC``（默认 3）：主 CLI 未按时完成则与 Kimi/DeepSeek 影子 **竞速**，先出正文者回复。
CDP 未监听时直接提示运行 ``chrome --remote-debugging-port=9222``（见 ``_JARVIS_SHADOW_CDP_MSG``）。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import socket
from urllib.parse import urlparse
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

_JARVIS_CHAT_ERR_USER = (
    "本地 Claude CLI 无有效输出或未就绪；请确认本机已安装 Claude Code 且 `claude` 可执行并已登录。"
)
_JARVIS_CHAT_ERR_AUTH = (
    "本地 CLI 未登录或会话失效。请在终端运行 `claude` 完成登录，并确认订阅有效。"
)
_JARVIS_CHAT_ERR_CLI = "未检测到本机 `claude` 可执行文件。请安装 Claude Code CLI 或将其加入 PATH。"
_JARVIS_CHAT_ERR_TIMEOUT = "本地 CLI 响应超时。请稍后重试或检查本机负载与订阅状态。"
_JARVIS_QUOTA_SHADOW_OK = "长官，订阅额度已干爆，已为您切换至 Kimi/DeepSeek 临时大脑。"
_JARVIS_QUOTA_SHADOW_FAIL = (
    "长官，订阅额度已干爆，影子部队（Kimi/DeepSeek）未能接通；"
    "请在本机浏览器登录对应站点，并配置 JARVIS_BROWSER_CDP_URL 或 JARVIS_BROWSER_USER_DATA_DIR。"
)
_JARVIS_SHADOW_CDP_MSG = (
    "长官，影子部队缺少 CDP 权限，请运行 chrome --remote-debugging-port=9222"
)

# 与 agents.sessions 的 8_000_000+ 虚拟 id 错开，按 Telegram uid 稳定映射多轮记忆
_JARVIS_GW_CHAT_BASE = 71_000_000
_JARVIS_GW_CHAT_MOD = 89_000_000


def _jarvis_gateway_chat_id(uid: int) -> int:
    return _JARVIS_GW_CHAT_BASE + (abs(int(uid)) % _JARVIS_GW_CHAT_MOD)


def _parse_cdp_host_port(cdp_url: str) -> tuple[str, int] | None:
    raw = (cdp_url or "").strip()
    if not raw:
        return None
    if "://" not in raw:
        raw = "http://" + raw.lstrip("/")
    u = urlparse(raw)
    host = u.hostname
    port = u.port
    if not host:
        return None
    if port is None:
        port = 9222
    return host, int(port)


async def _cdp_tcp_open(host: str, port: int, *, timeout: float = 0.4) -> bool:
    def _go() -> bool:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        try:
            s.connect((host, port))
            return True
        except OSError:
            return False
        finally:
            try:
                s.close()
            except OSError:
                pass

    return await asyncio.to_thread(_go)


def _is_cdp_refused(exc: BaseException) -> bool:
    low = str(exc).lower()
    return "econnrefused" in low or "connection refused" in low or (
        "refused" in low and "connect" in low
    )


def _jarvis_shadow_browser_config():
    """Playwright 配置：优先 CDP 复用已登录 Chrome，其次 user_data_dir。"""
    from browser_agents.base import BrowserConfig

    cdp = (
        (os.environ.get("JARVIS_BROWSER_CDP_URL") or os.environ.get("PLAYWRIGHT_CDP") or "")
        .strip()
    )
    udata = (os.environ.get("JARVIS_BROWSER_USER_DATA_DIR") or "").strip()
    if not udata and os.name == "nt":
        udata = os.path.join(
            os.path.expanduser("~"), "AppData", "Local", "Google", "Chrome", "User Data"
        )
    elif not udata:
        udata = os.path.join(os.path.expanduser("~"), ".config", "google-chrome")
    headless_raw = (os.environ.get("JARVIS_BROWSER_HEADLESS") or "1").strip().lower()
    headless = headless_raw in ("1", "true", "yes", "on")
    try:
        timeout_ms = int((os.environ.get("JARVIS_BROWSER_TIMEOUT_MS") or "120000").strip())
    except ValueError:
        timeout_ms = 120_000
    timeout_ms = max(30_000, min(600_000, timeout_ms))
    return BrowserConfig(
        headless=headless,
        cdp_url=cdp,
        user_data_dir="" if cdp else udata,
        timeout_ms=timeout_ms,
    )


async def _browser_shadow_reply(system_prompt: str, user_text: str) -> tuple[str, str]:
    """
    浏览器影子通道：按 ``JARVIS_SHADOW_PLATFORMS`` 尝试 Kimi/DeepSeek（Playwright）。

    Returns:
        ``(reply, platform_key)`` 成功时 platform_key 为小写平台名；
        失败 ``("", "")`` 或 ``("", "cdp_denied")``（CDP 未开）。
    """
    from browser_agents import get_browser_agent

    raw = (os.environ.get("JARVIS_SHADOW_PLATFORMS") or "kimi,deepseek").strip()
    platforms = [p.strip().lower() for p in raw.split(",") if p.strip()]
    if not platforms:
        platforms = ["kimi", "deepseek"]

    cfg = _jarvis_shadow_browser_config()
    cdp = (cfg.cdp_url or "").strip()
    if cdp:
        hp = _parse_cdp_host_port(cdp)
        if hp:
            host, port = hp
            if not await _cdp_tcp_open(host, port):
                logger.warning("jarvis shadow: CDP port closed %s:%s", host, port)
                return "", "cdp_denied"

    combined = (
        f"{(system_prompt or '')[:12_000]}\n\n--- 长官指令 ---\n\n{(user_text or '')[:80_000]}"
    )

    for plat in platforms:
        try:
            agent = get_browser_agent(plat, cfg)
        except ValueError:
            logger.warning("jarvis shadow: unknown platform %r", plat)
            continue
        except Exception as e:
            logger.warning("jarvis shadow: get_browser_agent %r: %s", plat, e)
            continue
        try:
            result = await agent.execute(combined)
        except Exception as e:
            if cdp and _is_cdp_refused(e):
                logger.warning("jarvis shadow CDP refused platform=%s: %s", plat, e)
                return "", "cdp_denied"
            logger.warning("jarvis shadow execute %r: %s", plat, e)
            continue
        out = (result.output or "").strip()
        if result.success and out:
            logger.info("jarvis shadow ok platform=%s chars=%d", plat, len(out))
            return out, plat
        logger.warning(
            "jarvis shadow empty platform=%s success=%s err=%s",
            plat,
            result.success,
            (result.error or "")[:200],
        )
    return "", ""


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


async def _jarvis_apply_cli_outcome(
    reply: str,
    diag: str,
    *,
    uid: int,
    chat_id: int,
    t: str,
) -> tuple[str, str]:
    if diag == "quota_exhausted":
        logger.warning("jarvis chat_reply CLI quota exhausted uid=%s chat_id=%s", uid, chat_id)
        shadow, plat = await _browser_shadow_reply(_JARVIS_CHAT_SYSTEM, t)
        if shadow:
            if plat in ("kimi", "moonshot", "deepseek"):
                intro = _JARVIS_QUOTA_SHADOW_OK
            else:
                intro = f"长官，订阅额度已干爆，已为您切换至 {plat} 临时网页通道。"
            return f"{intro}\n\n{shadow}", ""
        if plat == "cdp_denied":
            return "", _JARVIS_SHADOW_CDP_MSG
        return "", _JARVIS_QUOTA_SHADOW_FAIL

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


async def chat_reply(text: str, *, uid: int) -> tuple[str, str]:
    """
    网关 CHAT：主 CLI 与影子浏览器竞速；CLI 超过 ``JARVIS_SHADOW_FIRST_BYTE_SEC``（默认 3s）
    仍未结束时并发启动影子，**先完成且非空**者胜出，降低「已读不回」。
    """
    t = (text or "").strip()
    if not t:
        return "", ""

    import claude_agent

    chat_id = _jarvis_gateway_chat_id(uid)
    timeout_sec = _jarvis_chat_timeout_sec()

    try:
        early = float((os.environ.get("JARVIS_SHADOW_FIRST_BYTE_SEC") or "3").strip() or "3")
    except (TypeError, ValueError):
        early = 3.0
    early = max(0.5, min(60.0, early))

    async def _cli() -> tuple[str, str]:
        return await claude_agent.jarvis_gateway_cli_chat(
            _JARVIS_CHAT_SYSTEM,
            t[:120_000],
            chat_id=chat_id,
            timeout_sec=timeout_sec,
        )

    t_cli = asyncio.create_task(_cli())
    await asyncio.wait({t_cli}, timeout=early)

    if t_cli.done():
        try:
            reply, diag = t_cli.result()
        except asyncio.CancelledError:
            return "", _JARVIS_CHAT_ERR_USER
        except Exception as e:
            logger.exception("jarvis chat_reply cli uid=%s: %s", uid, e)
            return "", _JARVIS_CHAT_ERR_USER
        return await _jarvis_apply_cli_outcome(reply, diag, uid=uid, chat_id=chat_id, t=t)

    t_shadow = asyncio.create_task(_browser_shadow_reply(_JARVIS_CHAT_SYSTEM, t))
    done, _pending = await asyncio.wait(
        {t_cli, t_shadow}, return_when=asyncio.FIRST_COMPLETED
    )
    winner = next(iter(done))

    if winner is t_shadow:
        try:
            shadow, plat_tag = t_shadow.result()
        except Exception as e:
            logger.exception("jarvis shadow race: %s", e)
            shadow, plat_tag = "", ""
        if plat_tag == "cdp_denied":
            t_cli.cancel()
            try:
                await t_cli
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            return "", _JARVIS_SHADOW_CDP_MSG
        if (shadow or "").strip():
            t_cli.cancel()
            try:
                await t_cli
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            if plat_tag in ("kimi", "moonshot", "deepseek"):
                intro = "长官，主通道 Pending，影子部队先行抢答（Kimi/DeepSeek）。"
            elif plat_tag:
                intro = f"长官，主通道 Pending，{plat_tag} 先行抢答。"
            else:
                intro = "长官，影子部队先行抢答。"
            return f"{intro}\n\n{shadow.strip()}", ""
        try:
            reply, diag = await t_cli
        except asyncio.CancelledError:
            return "", _JARVIS_CHAT_ERR_USER
        except Exception as e:
            logger.exception("jarvis chat_reply cli uid=%s: %s", uid, e)
            return "", _JARVIS_CHAT_ERR_USER
        return await _jarvis_apply_cli_outcome(reply, diag, uid=uid, chat_id=chat_id, t=t)

    if not t_shadow.done():
        t_shadow.cancel()
    try:
        await t_shadow
    except asyncio.CancelledError:
        pass
    try:
        reply, diag = t_cli.result()
    except asyncio.CancelledError:
        return "", _JARVIS_CHAT_ERR_USER
    except Exception as e:
        logger.exception("jarvis chat_reply cli uid=%s: %s", uid, e)
        return "", _JARVIS_CHAT_ERR_USER
    return await _jarvis_apply_cli_outcome(reply, diag, uid=uid, chat_id=chat_id, t=t)
