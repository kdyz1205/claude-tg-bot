"""
Telegram-facing helpers wrapping ``pipeline.cli_bridge`` (formatting + streaming).

``run_dev_prompt`` runs the local Claude CLI via ``asyncio.create_subprocess_exec`` by
default (see ``TG_DEV_USE_HTTP``). Pass ``on_stdout_line`` for live TG chunks.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, Optional

from pipeline.cli_bridge import (
    CliDevRunResult as TgDevRunResult,
    find_claude_executable,
    git_changed_files,
    run_claude_dev_prompt as run_dev_prompt,
)

_logger = logging.getLogger(__name__)

__all__ = [
    "TgDevRunResult",
    "find_claude_executable",
    "format_telegram_report",
    "git_changed_files",
    "process_chaos_immunity_task",
    "process_dev_task",
    "process_wallet_clone_task",
    "run_dev_prompt",
    "run_dev_prompt_with_stream",
]


async def run_dev_prompt_with_stream(
    prompt: str,
    send_text_chunk: Callable[[str], Awaitable[None]],
    *,
    cwd: Optional[Path] = None,
    timeout_sec: Optional[int] = None,
    min_interval_sec: float = 2.0,
) -> TgDevRunResult:
    """
    Run dev CLI and periodically forward buffered stdout lines to Telegram (rate-limited).
    """
    import time as _time

    last_send = 0.0
    buf: list[str] = []
    max_buf_lines = 40

    async def on_line(line: str) -> None:
        nonlocal last_send
        buf.append(line)
        if len(buf) > max_buf_lines:
            del buf[: len(buf) - max_buf_lines]
        now = _time.monotonic()
        if now - last_send < min_interval_sec:
            return
        last_send = now
        text = "\n".join(buf[-max_buf_lines:])
        if len(text) > 3500:
            text = text[-3500:]
        try:
            await send_text_chunk(f"📟 `{_time.strftime('%H:%M:%S')}`\n{text}")
        except Exception:
            pass

    return await run_dev_prompt(
        prompt,
        cwd=cwd,
        timeout_sec=timeout_sec,
        on_stdout_line=on_line,
    )


def format_telegram_report(result: TgDevRunResult) -> str:
    """Human-readable report for Telegram (keep under ~4000 chars)."""
    lines: list[str] = []
    if result.ok and result.modified_files:
        lines.append("✅ 开发任务完成，以下文件已被修改：")
    elif result.ok:
        lines.append("✅ 开发任务完成。未检测到工作区文件变更（或当前目录非 Git 仓库）。")
    elif result.timed_out:
        lines.append("⏱️ 开发任务超时，已终止子进程。当前 Git 可见变更：")
    elif result.returncode not in (0, None):
        lines.append("⚠️ CLI 已结束但退出码非零。Git 可见变更：")
    else:
        lines.append("ℹ️ 开发任务已结束。Git 可见变更：")

    if result.modified_files:
        for f in result.modified_files[:80]:
            lines.append(f"• `{f}`")
        if len(result.modified_files) > 80:
            lines.append(f"… 另有 {len(result.modified_files) - 80} 个路径未列出")
    elif not result.ok:
        lines.append("（工作区无变更记录，或未在 Git 仓库内运行）")

    if result.error_message:
        lines.append("")
        lines.append(result.error_message)

    if result.combined_output_tail:
        lines.append("")
        lines.append("—— 输出摘要 ——")
        lines.append(result.combined_output_tail[:2800])

    text = "\n".join(lines)
    if len(text) > 4090:
        text = text[:4087] + "..."
    return text


def _keywords_from_user_request(text: str) -> list[str]:
    return list(
        dict.fromkeys(re.findall(r"[\w\u4e00-\u9fff]+", (text or "").lower()))
    )[:25]


def _title_from_user_request(text: str) -> str:
    t = (text or "").strip().replace("\n", " ")
    return (t[:80] + "…") if len(t) > 80 else t if t else ""


def _register_factor_skills_from_git(
    modified_files: list[str], user_request: str
) -> None:
    import skill_library as sl

    for raw in modified_files:
        fn = raw.replace("\\", "/")
        if not fn.startswith("skills/sk_") or not fn.endswith(".py"):
            continue
        skill_id = Path(fn).name[:-3]
        kws = _keywords_from_user_request(user_request)
        title = _title_from_user_request(user_request)
        try:
            sl.register_or_update_factor_skill(
                skill_id=skill_id,
                title=title or None,
                keywords=kws,
                user_request=user_request,
                py_relpath=fn,
            )
        except Exception as ex:
            _logger.warning("factor skill register %s: %s", skill_id, ex)


async def process_dev_task(
    *,
    bot: Any,
    chat_id: int,
    prompt: str,
    cwd: Optional[Path] = None,
    timeout_sec: Optional[int] = 600,
    min_interval_sec: float = 3.0,
    sub_intent: Optional[str] = None,
) -> None:
    """Run dev CLI in background task; use PTB ``application.create_task`` / ``create_task``."""

    async def _stream_chunk(t: str) -> None:
        try:
            await bot.send_message(chat_id=chat_id, text=t[:4090])
        except Exception as ex:
            _logger.debug("process_dev_task stream chunk: %s", ex)

    if sub_intent == "FACTOR_FORGE":
        from gateway.jarvis_semantic import build_factor_forge_prompt

        run_prompt = build_factor_forge_prompt(prompt)
    else:
        run_prompt = prompt

    try:
        result = await run_dev_prompt_with_stream(
            run_prompt,
            _stream_chunk,
            cwd=cwd,
            timeout_sec=timeout_sec,
            min_interval_sec=min_interval_sec,
        )
        if result.ok and sub_intent == "FACTOR_FORGE" and result.modified_files:
            await asyncio.to_thread(
                _register_factor_skills_from_git, result.modified_files, prompt
            )
        if result.ok and result.modified_files:
            text = "✅ 自动编程完成。您的代码库已被修改。"
        else:
            text = format_telegram_report(result)
    except Exception as e:
        _logger.exception("process_dev_task: %s", e)
        text = f"❌ 桥接执行异常：{e!s}"
    text = text[:4096]
    try:
        await bot.send_message(chat_id=chat_id, text=text)
    except Exception as e:
        _logger.warning("process_dev_task report send failed: %s", e)


async def process_wallet_clone_task(
    *,
    bot: Any,
    chat_id: int,
    wallet_address: str,
    cwd: Optional[Path] = None,
    timeout_sec: Optional[int] = 600,
    min_interval_sec: float = 3.0,
) -> None:
    """
    后台任务：采集目标地址链上特征 → 构造克隆提示词 → 走与 /dev 相同的 CLI 流水线，
    生成 ``skills/sk_clone_0x*.py`` 并在成功时注册因子技能元数据。
    """
    from pipeline.wallet_clone_pipeline import (
        build_wallet_clone_dev_prompt,
        collect_wallet_clone_bundle,
    )

    async def _stream_chunk(t: str) -> None:
        try:
            await bot.send_message(chat_id=chat_id, text=t[:4090])
        except Exception as ex:
            _logger.debug("process_wallet_clone_task stream chunk: %s", ex)

    wa = (wallet_address or "").strip()
    try:
        bundle = await collect_wallet_clone_bundle(wa)
    except Exception as e:
        _logger.exception("collect_wallet_clone_bundle: %s", e)
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=f"❌ 链上采集异常：{e!s}"[:4090],
            )
        except Exception:
            pass
        return

    errs = list(bundle.get("errors") or [])
    if "missing_ETHERSCAN_API_KEY" in errs:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    "❌ 未配置 ETHERSCAN_API_KEY，无法拉取交易与区块数据。"
                    "请在运行网关的环境中设置该变量后重试。"
                ),
            )
        except Exception:
            pass
        return

    agg = bundle.get("aggregate") or {}
    fn = bundle.get("target_skill_file") or "skills/sk_clone_0x*.py"
    lines = [
        "📡 对手盘行为克隆 — 采集完成",
        f"• 普通交易样本数: {bundle.get('normal_tx_count', 0)}",
        f"• 地址收到的 ERC20 转入（近 100 条内）: {bundle.get('token_tx_sampled', 0)}",
        f"• 已展开特征的买入样本: {agg.get('buy_samples', 0)}",
        f"• 将生成文件: {fn}",
    ]
    if agg.get("median_gas_ratio") is not None:
        lines.append(f"• 中位 gas/窗口 baseFee 比: {agg.get('median_gas_ratio')}")
    if agg.get("median_pool_liquidity_usd") is not None:
        lines.append(
            f"• 样本中位池子流动性(USD 快照): {agg.get('median_pool_liquidity_usd')}"
        )
    vs = agg.get("verified_token_share")
    if vs is not None:
        lines.append(f"• 样本中合约已验证占比: {round(float(vs), 3)}")
    if errs:
        lines.append(f"• 告警: {', '.join(errs)}")
    lines.append("")
    lines.append("🧠 正在唤醒造物主引擎，根据统计规律编写狙击因子…")

    try:
        await bot.send_message(chat_id=chat_id, text="\n".join(lines)[:4090])
    except Exception as ex:
        _logger.debug("process_wallet_clone_task summary send: %s", ex)

    prompt = build_wallet_clone_dev_prompt(wa, bundle)
    user_request = f"wallet_clone {wa}"

    try:
        result = await run_dev_prompt_with_stream(
            prompt,
            _stream_chunk,
            cwd=cwd,
            timeout_sec=timeout_sec,
            min_interval_sec=min_interval_sec,
        )
        if result.ok and result.modified_files:
            await asyncio.to_thread(
                _register_factor_skills_from_git, result.modified_files, user_request
            )
        if result.ok and result.modified_files:
            text = f"✅ 克隆管线完成。已尝试写入 {fn} 并更新技能索引。"
        else:
            text = format_telegram_report(result)
    except Exception as e:
        _logger.exception("process_wallet_clone_task: %s", e)
        text = f"❌ 桥接执行异常：{e!s}"
    text = text[:4096]
    try:
        await bot.send_message(chat_id=chat_id, text=text)
    except Exception as e:
        _logger.warning("process_wallet_clone_task report send failed: %s", e)


async def process_chaos_immunity_task(
    *,
    bot: Any,
    chat_id: int,
    uid: int,
    cwd: Optional[Path] = None,
    dev_timeout_sec: Optional[int] = 900,
    min_interval_sec: float = 3.0,
) -> None:
    """
    混沌抗压免疫：强制 TG 会话为模拟盘 → 隔离状态跑电池 → 汇报；
    若有失败场景且未设置 ``CHAOS_SKIP_DEV_REPAIR=1``，触发 CLI 自动修补提示。
    """
    import os

    from harness.chaos_trading_immunity import (
        build_chaos_immunity_repair_prompt,
        format_chaos_report_telegram,
        run_chaos_immunity_battery,
    )
    from tracker.session_store import SessionStore

    async def _stream_chunk(t: str) -> None:
        try:
            await bot.send_message(chat_id=chat_id, text=t[:4090])
        except Exception as ex:
            _logger.debug("process_chaos_immunity_task stream chunk: %s", ex)

    try:
        await asyncio.to_thread(SessionStore().set_trade_mode, int(uid), "paper")
    except Exception as ex:
        _logger.warning("chaos immunity: set_trade_mode paper: %s", ex)

    try:
        report = await run_chaos_immunity_battery()
    except Exception as e:
        _logger.exception("run_chaos_immunity_battery: %s", e)
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=f"❌ 混沌电池异常中止：{e!s}"[:4090],
            )
        except Exception:
            pass
        return

    summary = "\n\n" + format_chaos_report_telegram(report)
    try:
        await bot.send_message(chat_id=chat_id, text=summary[:4090])
    except Exception as ex:
        _logger.warning("process_chaos_immunity_task report send: %s", ex)

    skip_repair = (os.getenv("CHAOS_SKIP_DEV_REPAIR") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    if report.get("summary", {}).get("all_ok") or skip_repair:
        if skip_repair and not report.get("summary", {}).get("all_ok"):
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text="（已跳过自动修补：CHAOS_SKIP_DEV_REPAIR）",
                )
            except Exception:
                pass
        return

    prompt = build_chaos_immunity_repair_prompt(report)
    try:
        await bot.send_message(
            chat_id=chat_id,
            text="🔧 检测到失败场景，正在根据报告唤醒造物主引擎修补韧性代码…",
        )
    except Exception:
        pass

    try:
        result = await run_dev_prompt_with_stream(
            prompt,
            _stream_chunk,
            cwd=cwd,
            timeout_sec=dev_timeout_sec,
            min_interval_sec=min_interval_sec,
        )
        text = format_telegram_report(result)
    except Exception as e:
        _logger.exception("process_chaos_immunity_task dev: %s", e)
        text = f"❌ 自动修补流程异常：{e!s}"
    text = text[:4096]
    try:
        await bot.send_message(chat_id=chat_id, text=text)
    except Exception as e:
        _logger.warning("process_chaos_immunity_task dev report send failed: %s", e)
