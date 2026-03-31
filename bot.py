"""
bot.py — Telegram bot entry point.

Handles: text, voice, photos, documents, commands, quick actions.
Routes all messages through claude_agent.py → Claude Code CLI.
"""
import logging
import asyncio
import os
import sys
import signal
import time
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
import config
import claude_agent
import bridge
from safety import handle_confirmation_callback
import psutil  # for health/system checks
import datetime
from self_monitor import self_monitor, action_memory, code_repair
from proactive_agent import proactive_agent
try:
    from self_repair import (
        proactive_repair, format_repair_status,
        code_evolution_engine, format_evostatus,
        analyze_code_quality, format_code_health,
        code_quality_scheduler, generate_quality_patches,
    )
    _self_repair_available = True
except ImportError:
    proactive_repair = None
    format_repair_status = None
    code_evolution_engine = None
    format_evostatus = None
    analyze_code_quality = None
    format_code_health = None
    code_quality_scheduler = None
    generate_quality_patches = None
    _self_repair_available = False
from proactive_monitor import market_monitor
import memory_engine
import profit_tracker as _profit_tracker

try:
    from onchain_tracker import whale_tracker as _whale_tracker
    from onchain_tracker import smart_tracker as _smart_tracker
    _whale_available = True
    _smart_tracker_available = True
except ImportError:
    _whale_tracker = None
    _smart_tracker = None
    _whale_available = False
    _smart_tracker_available = False

try:
    from arbitrage_engine import arb_engine as _arb_engine
    from arbitrage_engine import format_arb_top5 as _format_arb_top5
    from arbitrage_engine import format_arb_top10 as _format_arb_top10
    from arbitrage_engine import format_arb_today as _format_arb_today
    _arb_available = True
except ImportError:
    _arb_engine = None
    _format_arb_top5 = None
    _format_arb_top10 = None
    _format_arb_today = None
    _arb_available = False

try:
    import strategy_optimizer as _strategy_optimizer
    _optimizer_available = True
except ImportError:
    _strategy_optimizer = None
    _optimizer_available = False

try:
    import paper_trader as _paper_trader
    _paper_trader_available = True
except ImportError:
    _paper_trader = None
    _paper_trader_available = False

try:
    import dex_trader as _dex
    _dex_available = True
except ImportError:
    _dex = None
    _dex_available = False

try:
    import secure_wallet as _wallet
    import live_trader as _live_trader
    import trade_scheduler as _trade_scheduler
    _live_available = True
except ImportError:
    _wallet = None
    _live_trader = None
    _trade_scheduler = None
    _live_available = False

try:
    from alpha_engine import alpha_engine as _alpha_engine, scan_alpha as _scan_alpha
    from alpha_engine import format_alpha_report as _format_alpha_report
    from alpha_engine import format_alpha_stats as _format_alpha_stats
    from alpha_engine import record_push as _alpha_record_push
    _alpha_available = True
except ImportError:
    _alpha_engine = None
    _scan_alpha = None
    _format_alpha_report = None
    _format_alpha_stats = None
    _alpha_record_push = None
    _alpha_available = False

try:
    import dashboard as _dashboard
    _dashboard_available = True
except ImportError:
    _dashboard = None
    _dashboard_available = False

try:
    import codex_charger as _codex
    _codex_available = True
except ImportError:
    _codex = None
    _codex_available = False

try:
    import session_learner as _sl
    _session_learner_available = True
except ImportError:
    _sl = None
    _session_learner_available = False

try:
    from skills.intelligence import IntelligenceSkill
    _intel = IntelligenceSkill()
    _intel_available = True
except Exception:
    _intel = None
    _intel_available = False

try:
    from agents.autonomy import get_autonomy_engine
    from agents.consciousness import get_self_awareness
    from agents.reflexion import get_reflexion_engine
    from agents.rag import get_solution_store
    _autonomy_available = True
except Exception:
    _autonomy_available = False

try:
    from agents.sessions import SessionManager
    _session_mgr = SessionManager()
    _sessions_available = True
except Exception:
    _session_mgr = None
    _sessions_available = False

# Message counter for periodic session learning
_message_counter = 0

# Bot start time — used for uptime tracking
_BOT_START_TIME = time.time()

# providers.py removed — inline display names
PROVIDER_DISPLAY = {"claude": "Claude (CLI)", "openai": "OpenAI", "gemini": "Gemini"}

# ─── Logging (console + rotating file) ────────────────────────────────────────
from logging.handlers import RotatingFileHandler

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler(
            config.LOG_FILE,
            maxBytes=20 * 1024 * 1024,  # 20 MB per file
            backupCount=3,              # Keep 3 rotated backups (60 MB total max)
            encoding="utf-8",
            errors="replace",
        ),
    ],
)
logger = logging.getLogger(__name__)

# Suppress httpx INFO noise (getUpdates polling every 10s fills the log)
logging.getLogger("httpx").setLevel(logging.WARNING)

# Auth filter — use a deny-all filter when AUTHORIZED_USER_ID is unset
auth_filter = (
    filters.User(user_id=config.AUTHORIZED_USER_ID)
    if config.AUTHORIZED_USER_ID is not None
    else filters.User(user_id=[])  # deny all until the ID is configured
)


def _is_authorized(user_id: int) -> bool:
    """Check if a user ID is authorized. Safe when AUTHORIZED_USER_ID is None."""
    return config.AUTHORIZED_USER_ID is not None and user_id == config.AUTHORIZED_USER_ID


def _track_task(bot_data: dict, task: asyncio.Task) -> None:
    """Register a background task with proper exception logging on completion."""
    tasks = bot_data.setdefault("_background_tasks", set())
    # Cap background tasks - prune completed ones if too many
    if len(tasks) > 100:
        done = {t for t in tasks if t.done()}
        tasks -= done
    tasks.add(task)
    def _on_done(t):
        try:
            if not t.cancelled():
                t.result()
        except Exception as e:
            logger.error(f"Background task {t.get_name()} failed: {e}", exc_info=True)
        bot_data.get("_background_tasks", set()).discard(t)
    task.add_done_callback(_on_done)

AVAILABLE_MODELS = {
    "sonnet": "claude-sonnet-4-6",
    "opus": "claude-opus-4-6",
    "haiku": "claude-haiku-4-5-20251001",
}


# ─── Error Handler ────────────────────────────────────────────────────────────

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Global error handler — logs errors and notifies user if possible.

    Special handling for Conflict errors (two bot instances polling):
    exits with code 42 so run.py can kill duplicates and retry cleanly.
    """
    err_str = str(context.error)[:500] if context.error else "Unknown error"

    # Detect Conflict error: "terminated by other getUpdates request"
    # This means another bot instance is running. Exit cleanly with special code.
    if context.error and "Conflict" in type(context.error).__name__:
        logger.error(f"CONFLICT ERROR: Another bot instance is polling. Exiting with code 42.")
        logger.error(f"Details: {err_str}")
        _release_pid_lock()
        os._exit(42)
    if context.error and "terminated by other getUpdates" in err_str.lower():
        logger.error(f"CONFLICT ERROR (string match): Another bot instance is polling. Exiting with code 42.")
        _release_pid_lock()
        os._exit(42)

    logger.error(f"Exception while handling update: {context.error}", exc_info=context.error)

    # Feed errors into monitoring systems
    try:
        self_monitor.record_error(err_str)
    except Exception:
        pass
    try:
        await proactive_agent.push_error("unhandled", err_str, source="error_handler")
    except Exception:
        pass

    # Try to notify the user
    if isinstance(update, Update) and update.effective_chat:
        try:
            err_msg = str(context.error)[:200] if context.error else "Unknown error"
            # Strip chars that may cause Telegram to reject the message
            import re as _re
            err_msg = _re.sub(r'[<>&\x00-\x08\x0b\x0c\x0e-\x1f]', '', err_msg)
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"⚠️ 内部错误: {err_msg}\n\n发消息继续使用。",
            )
        except Exception:
            # Last resort: send without the error details
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="⚠️ 内部错误。发消息继续使用。",
                )
            except Exception:
                pass


# --- Safe send helpers -----------------------------------------------------------

async def _safe_reply(message, text, **kwargs):
    """Send with Markdown, fallback to plain text on parse error."""
    if not message:
        return
    try:
        return await message.reply_text(text[:4096], **kwargs)
    except Exception:
        kwargs.pop("parse_mode", None)
        try:
            return await message.reply_text(text[:4096], **kwargs)
        except Exception:
            pass

async def _safe_send(bot, chat_id, text, **kwargs):
    """Send with Markdown, fallback to plain text on parse error."""
    try:
        return await bot.send_message(chat_id, text[:4096], **kwargs)
    except Exception:
        kwargs.pop("parse_mode", None)
        try:
            return await bot.send_message(chat_id, text[:4096], **kwargs)
        except Exception:
            pass


# ─── Commands ─────────────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    try:
        await update.message.reply_text(
            "🤖 Remote Controller v3.0\n\n"
            "我是你的远程电脑控制器。\n"
            "直接告诉我要做什么，我来操作。\n\n"
            "📋 /panel - Command panel (all commands)\n\n"
            "Quick commands:\n"
            "/status - 状态 + 系统健康\n"
            "/health - 详细健康检查\n"
            "/screenshot - 截图\n"
            "/model - 切换模型\n"
            "/score - Agent表现评分\n"
            "/portfolio - 持仓\n"
            "/signal - 信号\n"
            "/risk - 风险指标\n"
            "/kill - 终止卡住的任务\n"
            "/tasks - 查看任务队列\n"
            "/cancel - 取消排队任务\n"
            "/q - 快捷操作面板\n"
            "/help - 帮助"
        )
    except Exception as e:
        logger.error(f"Start command error: {e}", exc_info=True)


async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Quick health check — responds instantly."""
    if not update.message:
        return
    try:
        import time
        start = time.time()
        msg = await update.message.reply_text("🏓")
        latency = (time.time() - start) * 1000
        await msg.edit_text(f"🏓 Pong! ({latency:.0f}ms)")
    except Exception as e:
        logger.error(f"Ping command error: {e}", exc_info=True)


async def clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if not update.effective_chat:
        return
    try:
        claude_agent.clear_history(update.effective_chat.id)
        await update.message.reply_text("✅ 对话已清空，新会话已开始。")
    except Exception as e:
        logger.error(f"Clear command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ 清空失败: {str(e)[:300]}")
        except Exception:
            pass


async def kill_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Kill any stuck Claude CLI process and clear the queue."""
    if not update.message:
        return
    if not update.effective_chat:
        return
    try:
        chat_id = update.effective_chat.id
        claude_agent._pending_messages.pop(chat_id, None)
        logger.info(f"Kill requested for chat {chat_id}")
        claude_agent._claude_sessions.pop(chat_id, None)
        claude_agent._save_sessions()
        # Kill any claude.cmd child processes (non-blocking)
        try:
            import subprocess
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: subprocess.run(
                ["powershell", "-NoProfile", "-Command",
                 "Get-Process claude -ErrorAction SilentlyContinue | Stop-Process -Force"],
                capture_output=True, text=True, timeout=10,
            ))
            await update.message.reply_text(
                "🔪 已终止 Claude 进程并清空队列。\n发新消息重新开始。"
            )
        except Exception as e:
            await update.message.reply_text(f"🔪 队列已清空。进程终止: {str(e)[:300]}")
    except Exception as e:
        logger.error(f"Kill command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Kill 错误: {str(e)[:300]}")
        except Exception:
            pass


async def tasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/tasks — show running and queued tasks."""
    if not update.message:
        return
    if not update.effective_chat:
        return
    try:
        chat_id = update.effective_chat.id
        status = claude_agent.get_task_status()
        lines = ["📋 **任务状态**\n"]

        conc = status.get("concurrent", {})
        in_use = conc.get("in_use", 0)
        max_slots = conc.get("max", 3)
        lines.append(f"⚡ **并发槽**: {in_use}/{max_slots} 占用\n")

        running = status.get("running", {})
        if running:
            lines.append("🔄 **执行中:**")
            for cid, info in running.items():
                elapsed = int(time.time() - info.get("start_time", time.time()))
                preview = info.get("text", "")[:60]
                tid = info.get("task_id", "?")
                if cid == chat_id:
                    lines.append(f"  #`{tid}` ({elapsed}s): {preview}")
                else:
                    lines.append(f"  #`{tid}` [其他会话] ({elapsed}s)")
        else:
            lines.append("🔄 执行中: 无")

        queued = status.get("queued", {})
        workers = status.get("workers", {})
        my_queue = queued.get(chat_id, [])
        has_worker = workers.get(chat_id, False)
        if my_queue:
            worker_tag = " 🔧worker" if has_worker else " ⚠️no-worker"
            lines.append(f"\n⏳ **排队中 ({len(my_queue)}){worker_tag}:**")
            for i, m in enumerate(my_queue, 1):
                tid = m.get("task_id", "?")
                preview = m.get("text", "")[:60]
                age = int(time.time() - m.get("time", time.time()))
                lines.append(f"  {i}. #`{tid}` ({age}s前): {preview}")
            lines.append("\n/cancel 取消全部 · /cancel <id> 取消指定")
        else:
            lines.append("\n⏳ 排队中: 无")

        await _safe_reply(update.message, "\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Tasks command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ 错误: {str(e)[:200]}")
        except Exception:
            pass


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/cancel [task_id] — cancel queued tasks."""
    if not update.message:
        return
    if not update.effective_chat:
        return
    try:
        chat_id = update.effective_chat.id
        args = context.args or []
        if args:
            try:
                task_id = int(args[0].lstrip("#"))
            except ValueError:
                await update.message.reply_text("用法: /cancel 或 /cancel <任务ID>")
                return
            removed = claude_agent.cancel_queued_task(chat_id, task_id)
            if removed:
                await update.message.reply_text(f"✅ 已取消任务 #{task_id}")
            else:
                await update.message.reply_text(f"❌ 未找到排队任务 #{task_id}")
        else:
            removed = claude_agent.cancel_queued_task(chat_id, None)
            if removed:
                await update.message.reply_text(f"✅ 已取消 {removed} 个排队任务")
            else:
                await update.message.reply_text("队列为空，无需取消")
    except Exception as e:
        logger.error(f"Cancel command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ 错误: {str(e)[:200]}")
        except Exception:
            pass


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if not update.effective_chat:
        return
    try:
        await _send_status(context, update.effective_chat.id)
    except Exception as e:
        logger.error(f"Status command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Status error: {str(e)[:300]}")
        except Exception:
            pass


async def model_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if not update.effective_chat:
        return
    try:
        if not context.args:
            await _safe_reply(
                update.message,
                f"当前模型: `{config.CLAUDE_MODEL}`\n"
                "用法: `/model sonnet|opus|haiku`",
                parse_mode="Markdown",
            )
            return

        name = context.args[0].lower()
        if name not in AVAILABLE_MODELS:
            await update.message.reply_text("可选: sonnet, opus, haiku")
            return

        config.CLAUDE_MODEL = AVAILABLE_MODELS[name]
        claude_agent.clear_history(update.effective_chat.id)
        await _safe_reply(update.message, f"✅ 已切换为 `{config.CLAUDE_MODEL}`", parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Model command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Model error: {str(e)[:300]}")
        except Exception:
            pass


async def provider_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    try:
        valid = list(PROVIDER_DISPLAY.keys())
        if not context.args:
            lines = ["AI Provider 状态:\n"]
            key_map = {"claude": config.ANTHROPIC_API_KEY, "openai": config.OPENAI_API_KEY, "gemini": config.GEMINI_API_KEY}
            for p, name in PROVIDER_DISPLAY.items():
                if p == config.CURRENT_PROVIDER:
                    status = "✅ 当前"
                elif key_map.get(p):
                    status = "🔑 可用"
                else:
                    status = "❌ 无key"
                lines.append(f"{status} — {name}")
            lines.append(f"\n用法: `/provider claude|openai|gemini`")
            await _safe_reply(update.message, "\n".join(lines), parse_mode="Markdown")
            return

        choice = context.args[0].lower()
        if choice not in valid:
            await update.message.reply_text(f"可选: {', '.join(valid)}")
            return

        config.CURRENT_PROVIDER = choice
        claude_agent.clear_history(update.effective_chat.id)
        await update.message.reply_text(f"✅ 已切换到 {PROVIDER_DISPLAY[choice]}")
    except Exception as e:
        logger.error(f"Provider command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Provider error: {str(e)[:300]}")
        except Exception:
            pass


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    try:
        await update.message.reply_text(
            "🤖 我能做什么:\n\n"
            "💻 执行命令、打开应用\n"
            "📁 文件操作、代码编辑\n"
            "🌐 网页浏览、下载文件\n"
            "🖱 鼠标键盘控制\n"
            "📸 截图、GUI操作\n"
            "🔧 安装软件、Git操作\n\n"
            "直接说就行，不用客气。"
        )
    except Exception as e:
        logger.error(f"Help command error: {e}", exc_info=True)


async def quota_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show quota status for all AI platforms."""
    if not update.message:
        return
    try:
        from tracker.quota import QuotaTracker
        qt = QuotaTracker()
        report = qt.status_report()
        rate_info = ""
        if claude_agent.is_rate_limited():
            remaining = max(0, int(claude_agent._rate_limited_until - time.time()))
            rate_info = f"\n⚠️ Claude CLI 限速中 (还剩 {remaining}s)\n"
        await update.message.reply_text(f"📊 AI 平台用量\n{rate_info}\n{report}"[:4096])
    except Exception as e:
        rate_info = ""
        if claude_agent.is_rate_limited():
            remaining = max(0, int(claude_agent._rate_limited_until - time.time()))
            rate_info = f"⚠️ Claude CLI 限速中 (还剩 {remaining}s)\n"
        await update.message.reply_text(f"📊 Quota\n{rate_info}\nHarness 未初始化 (首次限速时自动启动)")


async def sessions_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List Claude Code sessions and interact with them.

    /sessions              - list all detected Claude Code sessions
    /sessions ask <name> <question>  - ask a specific session a question
    /sessions delegate <name> <task> - delegate task to a session
    """
    if not update.message:
        return
    try:
        return await _sessions_command_impl(update, context)
    except Exception as e:
        logger.error(f"Sessions command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Sessions error: {str(e)[:300]}")
        except Exception:
            pass

async def _sessions_command_impl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if not _session_learner_available:
        await update.message.reply_text("session_learner module not available.")
        return

    args = context.args or []
    learner = _sl.get_learner()

    if not args:
        # List all detected sessions
        active = learner.get_active_sessions()
        scanned = learner.scan_session_logs()

        lines = ["📋 Claude Code Sessions\n"]
        if active:
            lines.append(f"Active ({len(active)}):")
            for s in active[:15]:
                pid = s.get("pid", "?")
                sid = s.get("session_id", "?")[:12]
                cwd = s.get("cwd", "")
                cwd_short = os.path.basename(cwd) if cwd else "?"
                lines.append(f"  PID {pid} | {sid}... | {cwd_short}")
        else:
            lines.append("No active sessions found.")

        lines.append(f"\nSession logs found: {len(scanned)}")
        if scanned:
            lines.append("Recent:")
            for s in scanned[:5]:
                sid = s.get("session_id", "?")[:12]
                proj = s.get("project", "?")
                msgs = s.get("message_count", 0)
                task = s.get("task_summary", "")[:60]
                lines.append(f"  {sid}... | {proj} | {msgs} msgs | {task}")

        await update.message.reply_text("\n".join(lines)[:4096])
        return

    action = args[0].lower()

    if action == "ask" and len(args) >= 3:
        session_name = args[1][:100]
        question = " ".join(args[2:])[:2000]  # cap user input length
        msg = await update.message.reply_text(f"Asking session '{session_name}'...")
        try:
            response = await learner.ask_session(session_name, question)
            if response:
                await msg.edit_text(f"Response from '{session_name}':\n\n{response[:3500]}")
            else:
                await msg.edit_text(f"No response from session '{session_name}'. It may not be reachable.")
        except Exception as e:
            await msg.edit_text(f"Error asking session: {str(e)[:300]}")

    elif action == "delegate" and len(args) >= 3:
        session_name = args[1][:100]
        task = " ".join(args[2:])[:2000]  # cap user input length
        msg = await update.message.reply_text(f"Delegating to session '{session_name}'...")
        try:
            result = await learner.delegate_task(session_name, task)
            status = result.get("status", "unknown")
            resp = result.get("response", "")
            duration = result.get("duration_seconds", 0)
            text = (
                f"Delegation result: {status}\n"
                f"Duration: {duration}s\n"
            )
            if resp:
                text += f"\nResponse:\n{resp[:3000]}"
            if result.get("error"):
                text += f"\nError: {str(result.get('error', ''))[:300]}"
            await msg.edit_text(text[:4000])
        except Exception as e:
            await msg.edit_text(f"Error delegating: {str(e)[:300]}")

    else:
        await update.message.reply_text(
            "Usage:\n"
            "/sessions - list sessions\n"
            "/sessions ask <name> <question>\n"
            "/sessions delegate <name> <task>"
        )


async def learn_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Session learning system.

    /learn               - scan all session logs, learn patterns, show summary
    /learn sessions      - list active sessions found
    /learn from <id>     - learn from specific session
    /learn report        - show intelligence report
    /learn gaps          - show skill gaps and training curriculum
    """
    if not update.message:
        return
    try:
        return await _learn_command_impl(update, context)
    except Exception as e:
        logger.error(f"Learn command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Learn error: {str(e)[:300]}")
        except Exception:
            pass

async def _learn_command_impl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if not _session_learner_available:
        await update.message.reply_text("session_learner module not available.")
        return

    if not config.SESSION_LEARNING_ENABLED:
        await update.message.reply_text("Session learning is disabled. Set SESSION_LEARNING_ENABLED=true in .env")
        return

    args = context.args or []
    learner = _sl.get_learner()

    if not args:
        # Full scan + learn + summary
        msg = await update.message.reply_text("Scanning session logs...")
        try:
            result = learner.learn_from_all_recent(max_sessions=20)
            summary_text = (
                f"Session Learning Complete\n\n"
                f"Sessions scanned: {result.get('sessions_scanned', 0)}\n"
                f"Sessions learned: {result.get('sessions_learned', 0)}\n"
                f"Errors: {result.get('errors', 0)}\n"
                f"Curriculum items: {result.get('curriculum_items', 0)}\n"
            )
            knowledge_summary = result.get("knowledge_summary", "")
            if knowledge_summary:
                # Trim to fit Telegram message limit
                remaining = 4000 - len(summary_text) - 10
                if len(knowledge_summary) > remaining:
                    knowledge_summary = knowledge_summary[:remaining] + "..."
                summary_text += f"\n{knowledge_summary}"
            await msg.edit_text(summary_text[:4096])
        except Exception as e:
            logger.error(f"Learn command failed: {e}", exc_info=True)
            await msg.edit_text(f"Learning failed: {str(e)[:300]}")
        return

    action = args[0].lower()

    if action == "sessions":
        active = learner.get_active_sessions()
        if not active:
            await update.message.reply_text("No active Claude Code sessions found.")
            return
        lines = [f"Active Claude Code Sessions ({len(active)}):\n"]
        for s in active:
            pid = s.get("pid", "?")
            sid = s.get("session_id", "?")[:16]
            cwd = s.get("cwd", "")
            cwd_short = os.path.basename(cwd) if cwd else "?"
            kind = s.get("kind", "")
            lines.append(f"  PID {pid} | {sid}... | {cwd_short} | {kind}")
        await update.message.reply_text("\n".join(lines)[:4096])

    elif action == "from" and len(args) >= 2:
        session_id = args[1]
        msg = await update.message.reply_text(f"Learning from session {session_id[:16]}...")
        try:
            result = learner.learn_from_session(session_id)
            if "error" in result:
                await msg.edit_text(f"Error: {result['error']}")
                return
            text = (
                f"Learned from session {session_id[:16]}...\n\n"
                f"Task: {result.get('task', '?')[:200]}\n"
                f"Approaches tried: {result.get('approaches_tried', 0)}\n"
                f"Successful: {len(result.get('successful_approaches', []))}\n"
                f"Failed: {len(result.get('failed_approaches', []))}\n"
                f"Strategies extracted: {len(result.get('reusable_strategies', []))}\n"
                f"Patterns found: {result.get('patterns_found', 0)}"
            )
            await msg.edit_text(text[:4096])
        except Exception as e:
            await msg.edit_text(f"Error: {str(e)[:300]}")

    elif action == "report":
        try:
            summary = learner.get_session_summary()
            # Split if too long for one message
            summary = summary[:12000]  # cap at 3 messages
            if len(summary) <= 4096:
                await update.message.reply_text(summary)
            else:
                for i in range(0, len(summary), 4000):
                    await update.message.reply_text(summary[i:i+4000])
        except Exception as e:
            await update.message.reply_text(f"Report error: {str(e)[:300]}")

    elif action == "gaps":
        try:
            curriculum = learner.generate_training_curriculum()
            if not curriculum:
                await update.message.reply_text("No skill gaps identified. Run /learn first to analyze sessions.")
                return
            lines = ["Skill Gaps & Training Curriculum\n"]
            for item in curriculum[:20]:  # cap items to prevent oversized message
                priority = item.get("priority", "?").upper()
                skill = item.get("skill", "?")
                gap = item.get("gap_description", "")
                task = item.get("training_task", "")
                evidence = item.get("evidence", "")
                lines.append(f"[{priority}] {skill}")
                lines.append(f"  Gap: {gap}")
                lines.append(f"  Training: {task}")
                lines.append(f"  Evidence: {evidence}")
                lines.append("")
            await update.message.reply_text("\n".join(lines)[:4096])
        except Exception as e:
            await update.message.reply_text(f"Gaps error: {str(e)[:300]}")

    else:
        await update.message.reply_text(
            "Usage:\n"
            "/learn - scan & learn from all sessions\n"
            "/learn sessions - list active sessions\n"
            "/learn from <session_id> - learn from one session\n"
            "/learn report - show intelligence report\n"
            "/learn gaps - show skill gaps & curriculum"
        )


async def score_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show agent performance scores and insights."""
    if not update.message:
        return
    try:
        import harness_learn
        scores = harness_learn.get_recent_scores(10)
        if not scores:
            await update.message.reply_text("📊 暂无评分数据。发几条消息后再看。")
            return
        insights = harness_learn.detect_patterns(scores)
        avg = sum(s.get("overall", 0) for s in scores) / len(scores)
        flags_all = []
        for s in scores:
            flags_all.extend(s.get("flags", [])[:20])  # cap flags per score
        flags_all = flags_all[:200]  # cap total flags
        top_flags = {}
        for f in flags_all:
            top_flags[f] = top_flags.get(f, 0) + 1

        text = f"📊 Agent 表现 (最近{len(scores)}次)\n\n"
        text += f"平均分: {avg:.2f}/1.00\n"
        if top_flags:
            text += f"问题标签: {', '.join(f'{k}({v})' for k,v in sorted(top_flags.items(), key=lambda x:-x[1]))}\n"
        text += "\n"
        for i in insights:
            text += f"• {i}\n"
        text += f"\n最近3次:\n"
        for s in scores[-3:]:
            text += f"  {s.get('overall', 0):.2f} | {s.get('model', '?')[:10]} | {s.get('user_message', '')[:30]}\n"

        # Evolution system stats
        text += "\n" + harness_learn.get_evolution_stats()

        # Skill library stats
        try:
            import skill_library
            text += "\n" + skill_library.get_skill_stats()
        except Exception:
            pass

        # Auto-research / meta-learning stats
        try:
            import auto_research
            text += "\n" + auto_research.get_meta_stats()
            h_stats = auto_research.get_hypothesis_stats()
            if h_stats:
                text += "\n" + h_stats
        except Exception:
            pass
        # Telegram message limit: split if >4096 chars, cap at 3 messages max
        text = text[:12000]
        if len(text) <= 4096:
            await update.message.reply_text(text)
        else:
            for i in range(0, min(len(text), 12000), 4000):
                await update.message.reply_text(text[i:i+4000])
    except Exception as e:
        await update.message.reply_text(f"📊 评分错误: {str(e)[:300]}")


async def train_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Self-training curriculum system."""
    if not update.message or not update.effective_chat:
        return
    try:
        import auto_train
    except ImportError:
        await update.message.reply_text("auto_train module not available.")
        return

    # Support /train_xxx commands (e.g., /train_obedience → domain=obedience)
    _raw = (update.message.text or "").strip().split()
    cmd_text = _raw[0] if _raw else "/train"
    domain_from_cmd = None
    if cmd_text.startswith("/train_"):
        domain_from_cmd = cmd_text[7:]  # strip "/train_"

    if not context.args and not domain_from_cmd:
        # Show progress
        report = auto_train.get_progress_report()
        await update.message.reply_text(report)
        return

    action = domain_from_cmd or context.args[0].lower()

    if action == "stop":
        auto_train.stop_training()
        await update.message.reply_text("⏹ 停止训练。")
        return

    if action == "reset":
        domain = context.args[1] if len(context.args) > 1 else None
        auto_train.reset_progress(domain)
        await update.message.reply_text(f"🔄 已重置{'全部' if not domain else domain}训练进度。")
        return

    # Start training for a domain
    domain_id = action
    chat_id = update.effective_chat.id

    async def send_status(text):
        try:
            await context.bot.send_message(chat_id=chat_id, text=text[:4000])
        except Exception as e:
            # Retry without special chars if parse fails
            try:
                clean = text.replace("*", "").replace("_", "").replace("`", "")[:4000]
                await context.bot.send_message(chat_id=chat_id, text=clean)
            except Exception:
                logger.warning(f"send_status failed: {e}")

    async def send_photo(photo_buffer):
        try:
            await context.bot.send_photo(chat_id=chat_id, photo=photo_buffer)
        except Exception:
            pass

    async def _training_wrapper():
        try:
            await auto_train.run_training(
                domain_id=domain_id,
                send_status=send_status,
                send_photo=send_photo,
                max_tasks=5,
            )
        except Exception as exc:
            logger.error(f"Training task failed: {exc}", exc_info=True)

    _train_task = asyncio.create_task(_training_wrapper())
    _track_task(context.bot_data, _train_task)


async def bridge_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    try:
        if not context.args:
            harness = "✅ 开启" if config.HARNESS_MODE else "❌ 关闭"
            bridge_status = "✅ 开启" if config.BRIDGE_MODE else "❌ 关闭"
            await update.message.reply_text(
                f"🧠 Harness Mode: {harness}\n"
                f"🔗 Bridge Mode: {bridge_status}\n\n"
                "Harness = 免费AI优先 (浏览器自动化)\n"
                "  纯问答→ChatGPT/Grok, 代码→Claude.ai\n"
                "  电脑操控→Claude CLI (唯一有工具的)\n\n"
                "Bridge = Claude CLI 直连 (消耗token)\n\n"
                "用法: /bridge harness|on|off"
            )
            return
        action = context.args[0].lower()
        if action == "harness":
            config.HARNESS_MODE = True
            config.BRIDGE_MODE = True
            await update.message.reply_text(
                "✅ Harness Mode 开启\n"
                "免费AI优先，Claude CLI仅用于电脑操控"
            )
        elif action == "on":
            config.HARNESS_MODE = False
            config.BRIDGE_MODE = True
            bridge.clear_bridge()
            await update.message.reply_text("✅ Bridge Mode 开启 (Claude CLI 直连)")
        elif action == "off":
            config.HARNESS_MODE = False
            config.BRIDGE_MODE = False
            await update.message.reply_text("✅ API Mode (直接调API，最贵)")
        else:
            await update.message.reply_text("用法: /bridge harness|on|off")
    except Exception as e:
        logger.error(f"Bridge command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Bridge error: {str(e)[:300]}")
        except Exception:
            pass


# ─── Command Panel System ────────────────────────────────────────────────────

# Panel category definitions: (callback_prefix, label, commands)
PANEL_CATEGORIES = [
    ("panel_ai",      "🤖 AI Controls",  [("ask", "提问"), ("model", "模型"), ("reset", "重置"), ("status", "状态")]),
    ("panel_trading", "📊 Trading",       [("trade", "交易"), ("signal", "信号"), ("volfilter", "量能筛选"), ("report", "收益报告"), ("portfolio", "持仓"), ("risk", "风险"), ("funding", "资金"), ("okx_top30", "OKX Top30"), ("token_analyze", "Token分析"), ("ma_ribbon_bt", "MA Ribbon回测"), ("ma_ribbon_scr", "MA扫描"), ("okx_backtest", "OKX回测"), ("session_ctrl", "会话控制")]),
    ("panel_pc",      "🖥️ PC Control",   [("screenshot", "截图"), ("click", "点击"), ("type", "输入"), ("window", "窗口")]),
    ("panel_web",     "🌐 Web",           [("browse", "浏览"), ("search", "搜索"), ("scrape", "抓取")]),
    ("panel_system",  "🔧 System",        [("health", "健康"), ("memory", "内存"), ("evolve", "进化"), ("scan", "扫描")]),
    ("panel_analysis","📈 Analysis",      [("score", "评分"), ("regime", "行情"), ("confluence", "共振"), ("backtest", "回测")]),
    ("panel_learn",   "🧠 Learning",      [("learn", "学习"), ("learn_report", "报告"), ("learn_gaps", "差距"), ("cc_sessions", "会话")]),
]


async def panel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show the main command panel with category buttons."""
    if not update.message:
        return
    try:
        rows = []
        for i in range(0, len(PANEL_CATEGORIES), 2):
            row = []
            for cat_key, cat_label, _ in PANEL_CATEGORIES[i:i+2]:
                row.append(InlineKeyboardButton(cat_label, callback_data=cat_key))
            rows.append(row)
        rows.append([InlineKeyboardButton("⚡ Quick Actions", callback_data="qa_panel")])
        keyboard = InlineKeyboardMarkup(rows)
        await update.message.reply_text("📋 Command Panel — choose a category:", reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Panel command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Panel error: {str(e)[:300]}")
        except Exception:
            pass


async def handle_panel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle panel category and command button presses."""
    query = update.callback_query
    if not query or not query.from_user:
        return
    if not _is_authorized(query.from_user.id):
        await query.answer("⛔ Unauthorized", show_alert=True)
        return
    data = query.data

    try:
        # ── Category button: show sub-panel ──────────────────────────────────
        for cat_key, cat_label, commands in PANEL_CATEGORIES:
            if data == cat_key:
                await query.answer()
                rows = []
                for i in range(0, len(commands), 3):
                    row = []
                    for cmd, label in commands[i:i+3]:
                        row.append(InlineKeyboardButton(
                            f"{label}", callback_data=f"pcmd_{cmd}"
                        ))
                    rows.append(row)
                rows.append([InlineKeyboardButton("⬅️ Back", callback_data="panel_back")])
                await query.edit_message_text(
                    f"{cat_label} — pick a command:",
                    reply_markup=InlineKeyboardMarkup(rows),
                )
                return

        # ── Back to main panel ───────────────────────────────────────────────
        if data == "panel_back":
            await query.answer()
            rows = []
            for i in range(0, len(PANEL_CATEGORIES), 2):
                row = []
                for cat_key, cat_label, _ in PANEL_CATEGORIES[i:i+2]:
                    row.append(InlineKeyboardButton(cat_label, callback_data=cat_key))
                rows.append(row)
            rows.append([InlineKeyboardButton("⚡ Quick Actions", callback_data="qa_panel")])
            await query.edit_message_text(
                "📋 Command Panel — choose a category:",
                reply_markup=InlineKeyboardMarkup(rows),
            )
            return

        # ── Quick Actions redirect ───────────────────────────────────────────
        if data == "qa_panel":
            await query.answer()
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("📸 截图", callback_data="qa_screenshot"),
                    InlineKeyboardButton("🖥 窗口", callback_data="qa_windows"),
                ],
                [
                    InlineKeyboardButton("🌐 Chrome", callback_data="qa_open_chrome"),
                    InlineKeyboardButton("📁 Explorer", callback_data="qa_open_explorer"),
                ],
                [
                    InlineKeyboardButton("💻 Terminal", callback_data="qa_open_terminal"),
                    InlineKeyboardButton("📝 VS Code", callback_data="qa_open_vscode"),
                ],
                [
                    InlineKeyboardButton("🔒 锁屏", callback_data="qa_lock"),
                    InlineKeyboardButton("📊 系统状态", callback_data="qa_sysinfo"),
                ],
                [InlineKeyboardButton("⬅️ Back", callback_data="panel_back")],
            ])
            await query.edit_message_text("⚡ Quick Actions", reply_markup=keyboard)
            return

        # ── Individual command buttons (pcmd_xxx) ────────────────────────────
        if not data.startswith("pcmd_"):
            # Unknown callback — answer to prevent timeout spinner
            try:
                await query.answer()
            except Exception:
                pass
            return
    except Exception as e:
        logger.error(f"Panel callback error for '{data}': {e}", exc_info=True)
        try:
            await query.answer("Error - try /panel again")
        except Exception:
            pass
        return

    cmd = data[5:]  # strip "pcmd_"
    # Answer callback FIRST to prevent Telegram timeout spinner
    await query.answer(f"/{cmd}...")
    if not query.message:
        return
    chat_id = query.message.chat_id

    # Dispatch table for simple text responses (no async work needed)
    _simple_responses = {
        "ask":       "💬 Send me your question directly — I'll answer it.",
        "click":     "🖱 Tell me where to click, e.g. \"click the start button\"",
        "type":      "⌨️ Tell me what to type, e.g. \"type hello world\"",
        "browse":    "🌐 Send me a URL or say \"open google.com\"",
        "search":    "🔍 发送 search <关键词> 或 搜索 <关键词> 即可搜索\n例如: search BTC price today",
        "scrape":    "🕷 Send me a URL to scrape, e.g. \"scrape https://example.com\"",
        "evolve":    "🧬 Use /train to start the evolution/training system.",
        "backtest":  "📈 Send a backtest request, e.g. \"backtest BTC MA crossover last 30 days\"",
        "trade":     "💹 Send a trade instruction, e.g. \"buy 0.1 BTC at market\"",
        "funding":   "💰 Send funding request, e.g. \"check funding rates\"",
    }

    try:
        # Simple text responses
        if cmd in _simple_responses:
            await context.bot.send_message(chat_id=chat_id, text=_simple_responses[cmd])

        # Commands that need special handling
        elif cmd == "model":
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"Current model: `{config.CLAUDE_MODEL}`\nUse /model sonnet|opus|haiku to switch.",
                parse_mode="Markdown",
            )
        elif cmd == "reset":
            claude_agent.clear_history(chat_id)
            await context.bot.send_message(chat_id=chat_id, text="✅ Conversation cleared. Fresh session.")
        elif cmd == "status":
            await _send_status(context, chat_id)
        elif cmd == "screenshot":
            from screenshots import capture_screenshot
            await context.bot.send_chat_action(chat_id=chat_id, action="upload_photo")
            _loop = asyncio.get_running_loop()
            buf = await _loop.run_in_executor(None, capture_screenshot)
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("🔄 Refresh", callback_data="qa_screenshot"),
                InlineKeyboardButton("⬆️ Up", callback_data="qa_scroll_up"),
                InlineKeyboardButton("⬇️ Down", callback_data="qa_scroll_down"),
            ]])
            await context.bot.send_photo(chat_id=chat_id, photo=buf, reply_markup=kb)
        elif cmd == "window":
            import tools
            result = await tools.execute_list_windows()
            await context.bot.send_message(chat_id=chat_id, text=f"🪟 Windows:\n{result[:3000]}")
        elif cmd == "health":
            await _send_health(context, chat_id)
        elif cmd == "memory":
            await _send_memory_info(context, chat_id)
        elif cmd == "scan":
            await _send_scan(context, chat_id)
        elif cmd == "score":
            await _send_score_brief(context, chat_id)
        elif cmd == "regime":
            await _send_regime(context, chat_id)
        elif cmd == "confluence":
            await _send_confluence(context, chat_id)
        elif cmd == "signal":
            await _send_signals(context, chat_id)
        elif cmd == "volfilter":
            await _send_vol_filter(context, chat_id)
        elif cmd == "report":
            await _send_profit_report(context, chat_id)
        elif cmd == "portfolio":
            await _send_portfolio(context, chat_id)
        elif cmd == "risk":
            await _send_risk(context, chat_id)
        elif cmd == "learn":
            await _send_learn_brief(context, chat_id)
        elif cmd == "learn_report":
            await _send_learn_report(context, chat_id)
        elif cmd == "learn_gaps":
            await _send_learn_gaps(context, chat_id)
        elif cmd == "cc_sessions":
            await _send_cc_sessions(context, chat_id)
        elif cmd == "okx_top30":
            await _send_okx_top30(context, chat_id)
        elif cmd == "token_analyze":
            await context.bot.send_message(
                chat_id=chat_id,
                text="Use /token_analyze <address> [network]\nExample: /token_analyze EPjFW... solana"
            )
        elif cmd == "ma_ribbon_bt":
            await context.bot.send_message(
                chat_id=chat_id,
                text="Use /ma_ribbon_backtest <symbol> [tf]\nExample: /ma_ribbon_backtest BTC 1d"
            )
        elif cmd == "ma_ribbon_scr":
            await context.bot.send_message(
                chat_id=chat_id,
                text="Starting MA Ribbon Screener in background...\nUse /ma_ribbon_screener for full run."
            )
        elif cmd == "okx_backtest":
            await context.bot.send_message(
                chat_id=chat_id,
                text="Use /okx_backtest [timeframe]\nExample: /okx_backtest 1H"
            )
        elif cmd == "session_ctrl":
            await _send_session_control(context, chat_id)
        else:
            await context.bot.send_message(chat_id=chat_id, text=f"⚠️ Unknown command: {cmd}")
    except Exception as e:
        logger.error(f"Panel command '{cmd}' failed: {e}", exc_info=True)
        try:
            await context.bot.send_message(chat_id=chat_id, text=f"❌ Error in /{cmd}: {str(e)[:300]}")
        except Exception:
            pass


# ─── Shared helpers for panel + standalone commands ──────────────────────────

async def _send_status(context, chat_id):
    """Send bot status. Used by both /status and panel button."""
    try:
        return await _send_status_impl(context, chat_id)
    except Exception as e:
        logger.error(f"Status error: {e}", exc_info=True)
        try:
            await context.bot.send_message(chat_id=chat_id, text=f"❌ Status error: {str(e)[:300]}")
        except Exception:
            pass

async def _send_status_impl(context, chat_id):
    """Inner status implementation."""
    session_id = claude_agent._claude_sessions.get(chat_id, "无")
    if session_id != "无":
        session_id = session_id[:12] + "..."
    queue_size = len(claude_agent._pending_messages.get(chat_id, []))
    is_busy = claude_agent._get_lock(chat_id).locked()

    if config.HARNESS_MODE:
        mode_str = "🧠 Harness (免费AI优先)"
    elif config.BRIDGE_MODE:
        mode_str = "🔗 Claude CLI (直连)"
    else:
        mode_str = f"💰 API ({config.CURRENT_PROVIDER})"

    rate_info = ""
    if claude_agent.is_rate_limited():
        remaining = max(0, int(claude_agent._rate_limited_until - time.time()))
        rate_info = f"\n⚠️ CLI限速: 还剩 {remaining}s"

    # Bot uptime
    uptime_secs = int(time.time() - _BOT_START_TIME)
    hours, remainder = divmod(uptime_secs, 3600)
    minutes, secs = divmod(remainder, 60)
    uptime_str = f"{hours}h {minutes}m {secs}s"

    # System health summary
    try:
        _loop = asyncio.get_running_loop()
        cpu = await _loop.run_in_executor(None, lambda: psutil.cpu_percent(interval=0.3))
        mem = psutil.virtual_memory()
        disk_path = "C:\\" if sys.platform == "win32" else "/"
        disk = psutil.disk_usage(disk_path)
        sys_info = (
            f"\n\n💻 System: CPU {cpu}% | RAM {mem.percent}% "
            f"({mem.used // (1024**3)}/{mem.total // (1024**3)} GB) | "
            f"Disk {disk.percent}%"
        )
    except Exception:
        sys_info = ""

    # Service health from self-monitor
    health_str = ""
    try:
        health = self_monitor.get_health_summary()
        state_emoji = {"healthy": "🟢", "degraded": "🟡", "broken": "🔴", "critical": "💀"}
        health_str = f"\n\nHealth: {state_emoji.get(health['overall'], '?')} {health['overall'].upper()}"
        if health.get("consecutive_failures"):
            health_str += f" ({health['consecutive_failures']} consecutive failures)"
        if health.get("last_success_ago") is not None:
            ago = health["last_success_ago"]
            if ago > 3600:
                health_str += f"\nLast success: {ago // 3600}h {(ago % 3600) // 60}m ago"
            elif ago > 60:
                health_str += f"\nLast success: {ago // 60}m ago"
        for svc, info in list(health.get("services", {}).items())[:20]:
            if info.get("failures", 0) > 0:
                health_str += f"\n  {svc[:30]}: {state_emoji.get(info.get('state', '?'), '?')} {info.get('failures', 0)} failures"
    except Exception:
        pass

    status_text = (
        f"📊 Status\n\n"
        f"Mode: {mode_str}\n"
        f"Model: {config.CLAUDE_MODEL}\n"
        f"Session: {session_id}\n"
        f"Queue: {queue_size} pending\n"
        f"Busy: {'Yes' if is_busy else 'No'}\n"
        f"Uptime: {uptime_str}"
        f"{rate_info}{health_str}{sys_info}"
    )
    await context.bot.send_message(chat_id=chat_id, text=status_text[:4096])


async def _send_health(context, chat_id):
    """Detailed health check."""
    lines = ["🏥 Health Check\n"]
    try:
        _loop = asyncio.get_running_loop()
        cpu = await _loop.run_in_executor(None, lambda: psutil.cpu_percent(interval=0.5))
        cpu_freq = psutil.cpu_freq()
        mem = psutil.virtual_memory()
        swap = psutil.swap_memory()
        disk_path = "C:\\" if sys.platform == "win32" else "/"
        disk = psutil.disk_usage(disk_path)
        boot = psutil.boot_time()
        uptime_seconds = time.time() - boot
        os_uptime = datetime.timedelta(seconds=int(uptime_seconds))
        bot_uptime_secs = int(time.time() - _BOT_START_TIME)
        bot_h, bot_rem = divmod(bot_uptime_secs, 3600)
        bot_m, bot_s = divmod(bot_rem, 60)

        lines.append(f"CPU: {cpu}% ({psutil.cpu_count()} cores)")
        if cpu_freq:
            lines.append(f"  Freq: {cpu_freq.current:.0f} MHz")
        lines.append(f"RAM: {mem.percent}% ({mem.used // (1024**2)} / {mem.total // (1024**2)} MB)")
        lines.append(f"Swap: {swap.percent}% ({swap.used // (1024**2)} / {swap.total // (1024**2)} MB)")
        lines.append(f"Disk: {disk.percent}% ({disk.used // (1024**3)} / {disk.total // (1024**3)} GB)")
        lines.append(f"  Free: {disk.free // (1024**3)} GB")
        lines.append(f"OS Uptime: {str(os_uptime).split('.')[0]}")
        lines.append(f"Bot Uptime: {bot_h}h {bot_m}m {bot_s}s")
        lines.append(f"Processes: {len(psutil.pids())}")
    except Exception as e:
        lines.append(f"System info error: {str(e)[:200]}")

    # Network info
    lines.append("")
    lines.append("🌐 Network:")
    try:
        net_io = psutil.net_io_counters()
        lines.append(f"  Sent: {net_io.bytes_sent // (1024**2)} MB")
        lines.append(f"  Recv: {net_io.bytes_recv // (1024**2)} MB")
        try:
            conns = psutil.net_connections(kind='inet')
            established = sum(1 for c in conns if c.status == 'ESTABLISHED')
            lines.append(f"  Connections: {established} established / {len(conns)} total")
        except (psutil.AccessDenied, PermissionError):
            lines.append("  Connections: access denied (run as admin)")
    except Exception as e:
        lines.append(f"  Network error: {e}")

    # Bot-specific health
    lines.append("")
    lines.append("🤖 Bot Health:")
    lines.append(f"  Mode: {'Harness' if config.HARNESS_MODE else 'Bridge' if config.BRIDGE_MODE else 'API'}")
    lines.append(f"  Model: {config.CLAUDE_MODEL}")
    lines.append(f"  Rate limited: {'Yes' if claude_agent.is_rate_limited() else 'No'}")
    lines.append(f"  Active sessions: {len(claude_agent._claude_sessions)}")
    lines.append(f"  Pending queues: {sum(len(v) for v in claude_agent._pending_messages.values())}")
    lines.append(f"  PID: {os.getpid()}")

    # Bot process memory
    try:
        proc = psutil.Process(os.getpid())
        rss = proc.memory_info().rss // (1024 * 1024)
        lines.append(f"  Bot RSS: {rss} MB")
    except Exception:
        pass

    # Check Claude CLI availability
    try:
        import shutil
        claude_path = shutil.which("claude") or shutil.which("claude.cmd")
        lines.append(f"  Claude CLI: {'✅ ' + claude_path if claude_path else '❌ Not found'}")
    except Exception:
        lines.append("  Claude CLI: ❓ Unknown")

    text = "\n".join(lines)[:12000]  # cap at 3 messages max
    if len(text) <= 4096:
        await context.bot.send_message(chat_id=chat_id, text=text)
    else:
        for i in range(0, len(text), 4000):
            await context.bot.send_message(chat_id=chat_id, text=text[i:i+4000])


async def _send_memory_info(context, chat_id):
    """Show structured JSON memory overview."""
    try:
        text = memory_engine.format_display()
        # Also append process stats
        try:
            proc = psutil.Process(os.getpid())
            rss = proc.memory_info().rss // (1024 * 1024)
            text += f"\n\n💻 Process RAM: {rss} MB"
        except Exception:
            pass
        text += f"\nSessions: {len(claude_agent._claude_sessions)} active"
        await _safe_send(context.bot, chat_id, text, parse_mode="Markdown")
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"🧠 Memory error: {str(e)[:300]}")


async def _send_scan(context, chat_id):
    """Quick system scan — processes, network."""
    lines = ["🔍 System Scan\n"]
    try:
        # Top 5 CPU-consuming processes
        procs = []
        for p in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
            try:
                info = p.info
                procs.append(info)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        procs.sort(key=lambda x: x.get('cpu_percent', 0) or 0, reverse=True)
        lines.append("Top CPU processes:")
        for p in procs[:5]:
            lines.append(f"  {p['name'][:20]:20s} CPU {p.get('cpu_percent', 0):5.1f}%  RAM {p.get('memory_percent', 0):5.1f}%")

        # Network connections count
        try:
            conns = psutil.net_connections(kind='inet')
            established = sum(1 for c in conns if c.status == 'ESTABLISHED')
            lines.append(f"\nNetwork: {established} established connections / {len(conns)} total")
        except (psutil.AccessDenied, PermissionError):
            lines.append("\nNetwork: access denied (run as admin)")
    except Exception as e:
        lines.append(f"Scan error: {str(e)[:200]}")
    await context.bot.send_message(chat_id=chat_id, text="\n".join(lines)[:4096])


async def _send_score_brief(context, chat_id):
    """Brief score summary for panel."""
    try:
        import harness_learn
        scores = harness_learn.get_recent_scores(10)
        if not scores:
            await context.bot.send_message(chat_id=chat_id, text="📊 No scores yet. Send some messages first.")
            return
        avg = sum(s.get("overall", 0) for s in scores) / len(scores)
        last = scores[-1]
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"📊 Agent Score (last {len(scores)})\n\n"
                f"Average: {avg:.2f}/1.00\n"
                f"Latest: {last.get('overall', 0):.2f} | {last.get('model', '?')[:15]}\n\n"
                f"Use /score for full details."
            ),
        )
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"📊 Score error: {str(e)[:300]}")


async def _send_signals(context, chat_id):
    """Show latest trading signals."""
    lines = ["📡 Latest Signals\n"]
    try:
        # Try loading from a signals file/module if it exists
        signals_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".bot_signals.json")
        if os.path.exists(signals_file) and os.path.getsize(signals_file) < 5 * 1024 * 1024:
            import json
            with open(signals_file, "r", encoding="utf-8") as f:
                signals = json.load(f)
            if not isinstance(signals, list):
                signals = []
            for s in signals[-5:]:
                lines.append(f"  {s.get('symbol', '?')} {s.get('direction', '?')} @ {s.get('price', '?')} ({s.get('time', '?')})")
        else:
            lines.append("No signal data available.")
            lines.append("Send a request like \"analyze BTC signals\" to generate.")
    except Exception as e:
        lines.append(f"Error: {str(e)[:200]}")
    await context.bot.send_message(chat_id=chat_id, text="\n".join(lines)[:4096])


async def _send_vol_filter(context, chat_id):
    """Run onchain volume filter: score>=60, 3m vol>8888, 5m vol>16666."""
    try:
        await context.bot.send_message(chat_id=chat_id, text="🔍 量能筛选中... (扫描30币)")
        from onchain_filter import scan_filtered, format_filtered
        results = await scan_filtered()
        text = format_filtered(results)
        await _safe_send(context.bot, chat_id, text, parse_mode="Markdown")
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"筛选失败: {str(e)[:300]}")


async def _send_profit_report(context, chat_id):
    """Generate and send profit tracker report + chart."""
    try:
        await context.bot.send_message(chat_id=chat_id, text="📊 生成收益报告中...")
        text, chart_path = await _profit_tracker.profit_tracker.get_report_and_chart()
        text = text[:12000]  # cap at 3 messages max
        if len(text) <= 4096:
            await context.bot.send_message(chat_id=chat_id, text=text)
        else:
            for i in range(0, len(text), 4000):
                await context.bot.send_message(chat_id=chat_id, text=text[i:i+4000])
        if chart_path and os.path.exists(chart_path):
            with open(chart_path, "rb") as f:
                await context.bot.send_photo(chat_id=chat_id, photo=f, caption="📈 胜率趋势 & 累计收益")
    except Exception as e:
        logger.error(f"Profit report error: {e}", exc_info=True)
        await context.bot.send_message(chat_id=chat_id, text=f"❌ 报告错误: {str(e)[:300]}")


async def _send_portfolio(context, chat_id):
    """Show trading portfolio / positions."""
    lines = ["💼 Portfolio\n"]
    try:
        portfolio_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".bot_portfolio.json")
        if os.path.exists(portfolio_file) and os.path.getsize(portfolio_file) < 5 * 1024 * 1024:
            import json
            with open(portfolio_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            for pos in data.get("positions", [])[:50]:
                pnl = float(pos.get("pnl", 0) or 0)
                emoji = "🟢" if pnl >= 0 else "🔴"
                lines.append(
                    f"  {emoji} {pos.get('symbol', '?')}: {pos.get('size', '?')} "
                    f"@ {pos.get('entry', '?')} | PnL: {pnl:+.2f}"
                )
            if "total_value" in data:
                lines.append(f"\nTotal value: ${float(data.get('total_value', 0)):,.2f}")
        else:
            lines.append("No portfolio data.")
            lines.append("Ask me to check your exchange positions.")
    except Exception as e:
        lines.append(f"Error: {str(e)[:200]}")
    await context.bot.send_message(chat_id=chat_id, text="\n".join(lines)[:4096])


async def _send_risk(context, chat_id):
    """Show risk metrics."""
    lines = ["⚠️ Risk Metrics\n"]
    try:
        risk_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".bot_risk.json")
        if os.path.exists(risk_file) and os.path.getsize(risk_file) < 5 * 1024 * 1024:
            import json
            with open(risk_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            lines.append(f"Max Drawdown: {data.get('max_drawdown', 'N/A')}")
            lines.append(f"Current Drawdown: {data.get('current_drawdown', 'N/A')}")
            lines.append(f"Exposure: {data.get('exposure', 'N/A')}")
            lines.append(f"Leverage: {data.get('leverage', 'N/A')}")
            lines.append(f"Win Rate: {data.get('win_rate', 'N/A')}")
            lines.append(f"Sharpe: {data.get('sharpe', 'N/A')}")
        else:
            lines.append("No risk data available.")
            lines.append("Ask me to calculate risk metrics for your portfolio.")
    except Exception as e:
        lines.append(f"Error: {str(e)[:200]}")
    await context.bot.send_message(chat_id=chat_id, text="\n".join(lines)[:4096])


async def _send_regime(context, chat_id):
    """Show market regime analysis."""
    await context.bot.send_message(
        chat_id=chat_id,
        text="📈 Market Regime\n\nSend a request like \"analyze market regime for BTC\" to get regime detection.",
    )


async def _send_confluence(context, chat_id):
    """Show confluence analysis."""
    await context.bot.send_message(
        chat_id=chat_id,
        text="🔀 Confluence Analysis\n\nSend a request like \"confluence analysis BTC\" to check multi-indicator alignment.",
    )


async def _send_learn_brief(context, chat_id):
    """Brief learning status for panel."""
    if not _session_learner_available:
        await context.bot.send_message(chat_id=chat_id, text="session_learner module not available.")
        return
    try:
        learner = _sl.get_learner()
        kb = learner.get_knowledge_base()
        stats = kb.get("stats", {})
        text = (
            f"🧠 Session Learning\n\n"
            f"Patterns: {kb.get('total_patterns', 0)}\n"
            f"Sessions analyzed: {kb.get('total_sessions', 0)}\n"
            f"Strategies: {len(kb.get('strategies_by_type', {}))}\n"
            f"Last scan: {stats.get('last_scan', 'never')}\n\n"
            f"Use /learn for full scan, /learn report for details."
        )
        await context.bot.send_message(chat_id=chat_id, text=text)
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"🧠 Learning error: {str(e)[:300]}")


async def _send_learn_report(context, chat_id):
    """Intelligence report for panel."""
    if not _session_learner_available:
        await context.bot.send_message(chat_id=chat_id, text="session_learner module not available.")
        return
    try:
        learner = _sl.get_learner()
        summary = learner.get_session_summary()[:12000]  # cap at 3 messages
        if len(summary) <= 4096:
            await context.bot.send_message(chat_id=chat_id, text=summary)
        else:
            for i in range(0, len(summary), 4000):
                await context.bot.send_message(chat_id=chat_id, text=summary[i:i+4000])
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"Report error: {str(e)[:300]}")


async def _send_learn_gaps(context, chat_id):
    """Skill gaps for panel."""
    if not _session_learner_available:
        await context.bot.send_message(chat_id=chat_id, text="session_learner module not available.")
        return
    try:
        learner = _sl.get_learner()
        curriculum = learner.generate_training_curriculum()
        if not curriculum:
            await context.bot.send_message(chat_id=chat_id, text="No skill gaps identified. Run /learn first.")
            return
        lines = ["Skill Gaps & Curriculum\n"]
        for item in curriculum[:8]:
            priority = item.get("priority", "?").upper()
            skill = item.get("skill", "?")
            gap = item.get("gap_description", "")
            lines.append(f"[{priority}] {skill}: {gap}")
        await context.bot.send_message(chat_id=chat_id, text="\n".join(lines)[:4096])
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"Gaps error: {str(e)[:300]}")


async def _send_cc_sessions(context, chat_id):
    """Claude Code sessions list for panel."""
    if not _session_learner_available:
        await context.bot.send_message(chat_id=chat_id, text="session_learner module not available.")
        return
    try:
        learner = _sl.get_learner()
        active = learner.get_active_sessions()
        if not active:
            await context.bot.send_message(chat_id=chat_id, text="No active Claude Code sessions found.\nUse /sessions for more options.")
            return
        lines = [f"📋 Active Sessions ({len(active)})\n"]
        for s in active[:10]:
            pid = s.get("pid", "?")
            sid = s.get("session_id", "?")[:12]
            cwd_short = os.path.basename(s.get("cwd", "")) or "?"
            lines.append(f"  PID {pid} | {sid}... | {cwd_short}")
        lines.append(f"\nUse /sessions ask/delegate for interaction.")
        await context.bot.send_message(chat_id=chat_id, text="\n".join(lines)[:4096])
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"Sessions error: {str(e)[:300]}")


async def _send_okx_top30(context, chat_id):
    """OKX Top 30 for panel button."""
    try:
        import httpx
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "https://www.okx.com/api/v5/market/tickers?instType=SWAP"
            )
            resp.raise_for_status()
            data = resp.json().get("data") or []

        usdt = [d for d in data if d.get("instId", "").endswith("-USDT-SWAP")]
        usdt.sort(key=lambda x: float(x.get("volCcy24h", 0) or 0), reverse=True)
        top30 = usdt[:30]

        lines = ["OKX Top 30 (24h Vol)\n"]
        for i, t in enumerate(top30, 1):
            sym = t.get("instId", "?").replace("-USDT-SWAP", "")
            try:
                price = float(t.get("last", 0) or 0)
                open24 = float(t.get("open24h", 0) or 0)
            except (ValueError, TypeError):
                continue
            chg = ((price - open24) / open24 * 100) if abs(open24) > 0.0001 else 0
            vol = float(t.get("volCcy24h", 0) or 0)
            vol_str = f"{vol/1e6:.0f}M" if vol >= 1e6 else f"{vol:,.0f}"
            emoji = "+" if chg >= 0 else ""
            lines.append(f"{i:>2}. {sym:<10} {price:.4f}  {emoji}{chg:.1f}%  {vol_str}")

        await context.bot.send_message(chat_id=chat_id, text="\n".join(lines)[:4096])
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"OKX Top 30 error: {str(e)[:300]}")


async def _send_session_control(context, chat_id):
    """Session control panel for panel button."""
    if config.HARNESS_MODE:
        current = "Harness"
    elif config.BRIDGE_MODE:
        current = "Bridge"
    else:
        current = f"API ({config.CURRENT_PROVIDER})"

    sessions = len(claude_agent._claude_sessions)
    pending = sum(len(v) for v in claude_agent._pending_messages.values())
    rate = "Yes" if claude_agent.is_rate_limited() else "No"

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Harness", callback_data="sc_harness"),
            InlineKeyboardButton("Bridge", callback_data="sc_bridge"),
            InlineKeyboardButton("API", callback_data="sc_api"),
        ],
        [
            InlineKeyboardButton("Clear Sessions", callback_data="sc_clear"),
            InlineKeyboardButton("Kill All", callback_data="sc_kill"),
        ],
    ])

    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            f"Session Control\n\n"
            f"Mode: {current}\n"
            f"Model: {config.CLAUDE_MODEL}\n"
            f"Sessions: {sessions}\n"
            f"Pending: {pending}\n"
            f"Rate limited: {rate}"
        ),
        reply_markup=keyboard,
    )


# ─── Standalone command handlers for new commands ────────────────────────────

async def dashboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send current performance stats. Dashboard at http://localhost:8080"""
    if not update.message:
        return
    try:
        if _dashboard_available:
            text = _dashboard.get_stats_text()
            text += "\n\n🌐 Web dashboard: http://localhost:8080"
        else:
            text = "❌ Dashboard module not available. Run: pip install flask"
        await _safe_reply(update.message, text, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Dashboard command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Dashboard error: {str(e)[:300]}")
        except Exception:
            pass


async def health_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Detailed health check command."""
    if not update.message or not update.effective_chat:
        return
    try:
        await _send_health(context, update.effective_chat.id)
    except Exception as e:
        logger.error(f"Health command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Health error: {str(e)[:300]}")
        except Exception:
            pass


async def vital_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show vital signs — the 5 invariants of 'being alive'."""
    if not update.message:
        return
    try:
        import vital_signs
        text = vital_signs.get_status_text()
        await update.message.reply_text(text[:4096])
    except Exception as e:
        logger.error(f"Vital command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Vital signs error: {str(e)[:300]}")
        except Exception:
            pass


async def portfolio_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show trading positions."""
    if not update.message or not update.effective_chat:
        return
    try:
        await _send_portfolio(context, update.effective_chat.id)
    except Exception as e:
        logger.error(f"Portfolio command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Portfolio error: {str(e)[:300]}")
        except Exception:
            pass


async def signal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show latest trading signals."""
    if not update.message or not update.effective_chat:
        return
    try:
        await _send_signals(context, update.effective_chat.id)
    except Exception as e:
        logger.error(f"Signal command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Signal error: {str(e)[:300]}")
        except Exception:
            pass


async def signal_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show 24h signal accuracy statistics."""
    if not update.message:
        return
    try:
        from signal_engine import format_signal_stats
        text = format_signal_stats()
        try:
            await update.message.reply_text(text[:4096], parse_mode="Markdown")
        except Exception:
            await update.message.reply_text(text[:4096])
    except Exception as e:
        logger.error(f"signal_stats command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ 统计错误: {str(e)[:300]}")
        except Exception:
            pass


async def alpha_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Immediately scan for current top Alpha opportunities."""
    if not update.message:
        return
    try:
        if not _alpha_available or _scan_alpha is None:
            await update.message.reply_text("❌ Alpha引擎模块不可用。")
            return

        # /alpha stats — show performance stats
        args = context.args or []
        if args and args[0].lower() in ("stats", "统计", "stat"):
            try:
                await update.message.reply_text(_format_alpha_stats()[:4096], parse_mode="Markdown")
            except Exception:
                await update.message.reply_text(_format_alpha_stats()[:4096])
            return

        # Live scan
        await update.message.reply_text("🔍 正在扫描 CoinGecko / DEXScreener / Pump.fun ...", parse_mode="Markdown")
        tokens = await _scan_alpha()

        # Cache result into engine's last_scan
        if _alpha_engine is not None:
            _alpha_engine._last_scan = tokens
            import time as _t
            _alpha_engine._last_scan_time = _t.time()

        report = _format_alpha_report(tokens, header="🚀 **Alpha 信号 — 立即扫描**")
        try:
            await update.message.reply_text(report[:4000], parse_mode="Markdown")
        except Exception:
            await update.message.reply_text(report[:4000])

        if tokens and _alpha_record_push is not None:
            _alpha_record_push(tokens)

    except Exception as e:
        logger.error(f"Alpha command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Alpha扫描失败: {str(e)[:300]}")
        except Exception:
            pass


async def onchain_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Onchain filter — scan DEXScreener with strict volume/liquidity/mcap criteria."""
    if not update.message:
        return
    try:
        from alpha_engine import scan_onchain_filter, format_onchain_filter_report
        await update.message.reply_text("🔗 Onchain Filter 扫描中...(Liq/MCap/量能筛选)")
        tokens = await scan_onchain_filter()
        report = format_onchain_filter_report(tokens)
        try:
            await update.message.reply_text(report[:4000])
        except Exception:
            await update.message.reply_text(report[:4000])
        # Auto-open paper trades for qualifying tokens
        if tokens and _paper_trader_available and _paper_trader is not None and hasattr(_paper_trader, 'on_signal_detected'):
            try:
                opened = await _paper_trader.on_signal_detected(tokens)
                if opened:
                    await _safe_reply(update.message, f"📝 自动开启 {len(opened)} 笔 Paper Trade")
            except Exception as e:
                logger.debug(f"Paper trade auto-open error: {e}")
    except Exception as e:
        logger.error(f"Onchain filter error: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Onchain扫描失败: {str(e)[:300]}")


async def paper_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Paper trading status and control. /paper [on|off|stats|close]"""
    if not update.message:
        return
    if not _paper_trader_available or _paper_trader is None:
        await _safe_reply(update.message, "❌ Paper Trading 模块不可用")
        return
    args = context.args or []
    subcmd = args[0].lower() if args else "stats"

    if subcmd == "on":
        cfg = _paper_trader._load_config()
        cfg["enabled"] = True
        _paper_trader._save_config(cfg)
        await _safe_reply(update.message, "✅ Paper Trading 已开启")
    elif subcmd == "off":
        cfg = _paper_trader._load_config()
        cfg["enabled"] = False
        _paper_trader._save_config(cfg)
        await _safe_reply(update.message, "⏸ Paper Trading 已暂停")
    elif subcmd in ("close", "closeall"):
        # Close all open positions at current price
        trades = _paper_trader._load_trades()
        open_trades = [t for t in trades[-500:] if t.get("status") == "open"]
        if not open_trades:
            await _safe_reply(update.message, "没有持仓需要平仓")
            return
        closed = 0
        for t in open_trades[:100]:  # cap to avoid excessive API calls
            price = await _paper_trader._fetch_current_price(t.get("address", ""))
            if price and price > 0:
                _paper_trader.close_paper_trade(t["id"], price, "manual")
                closed += 1
        await _safe_reply(update.message, f"✅ 已手动平仓 {closed}/{len(open_trades)} 笔")
    else:
        report = _paper_trader.format_stats_full()
        await _safe_reply(update.message, report[:4096])


# ── Live Trading Commands ────────────────────────────────────────────

_live_trader_instance = None
_trade_scheduler_instance = None

async def wallet_setup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set up wallet for live trading. /wallet_setup <private_key_or_seed>"""
    if not update.message:
        return
    if not _live_available:
        await _safe_reply(update.message, "\u274c Live Trading \u6a21\u5757\u4e0d\u53ef\u7528")
        return

    args = context.args or []
    if not args:
        has_wallet = _wallet.wallet_exists()
        if has_wallet:
            pubkey = _wallet.get_public_key()
            bal = await _wallet.get_sol_balance()
            await _safe_reply(update.message,
                f"\U0001f512 Wallet configured\n"
                f"\u5730\u5740: {pubkey[:8]}...{pubkey[-6:] if pubkey else '?'}\n"
                f"\u4f59\u989d: {bal:.4f} SOL" if bal else f"\u4f59\u989d: fetch failed\n"
                f"\n\u2139\ufe0f /wallet_setup <key_or_seed> \u66f4\u6362\u94b1\u5305\n"
                f"\u2139\ufe0f /wallet_delete \u5220\u9664\u94b1\u5305"
            )
        else:
            await _safe_reply(update.message,
                "\U0001f512 \u672a\u914d\u7f6e\u94b1\u5305\n\n"
                "\u7528\u6cd5: /wallet_setup <private_key_or_seed_phrase>\n\n"
                "\u26a0\ufe0f \u53d1\u9001\u540e\u7acb\u5373\u5220\u9664\u6d88\u606f\uff01\n"
                "\u26a0\ufe0f \u8bf7\u786e\u4fdd\u8bbe\u7f6e WALLET_PASSWORD \u73af\u5883\u53d8\u91cf"
            )
        return

    # SECURITY: Delete the user's message containing the key
    try:
        await update.message.delete()
    except Exception:
        pass  # May not have delete permissions in groups

    key_input = " ".join(args)
    success = _wallet.store_wallet(key_input)

    if success:
        pubkey = _wallet.get_public_key()
        bal = await _wallet.get_sol_balance()
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=(
                f"\u2705 \u94b1\u5305\u5df2\u52a0\u5bc6\u5b58\u50a8\n"
                f"\u5730\u5740: {pubkey[:8]}...{pubkey[-6:] if pubkey else '?'}\n"
                f"\u4f59\u989d: {bal:.4f} SOL\n" if bal else ""
                f"\n\U0001f512 \u79c1\u94a5\u5df2AES\u52a0\u5bc6\uff0c\u539f\u6d88\u606f\u5df2\u5220\u9664\n"
                f"\u26a0\ufe0f Bot\u4ec5\u7b7e\u540d\u4ea4\u6362\u4ea4\u6613\uff0c\u65e0\u8f6c\u8d26\u6743\u9650"
            )
        )
    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="\u274c \u94b1\u5305\u5b58\u50a8\u5931\u8d25\uff0c\u8bf7\u68c0\u67e5\u5bc6\u94a5\u683c\u5f0f\u548c WALLET_PASSWORD \u73af\u5883\u53d8\u91cf"
        )


async def wallet_delete_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete wallet securely. /wallet_delete"""
    if not update.message or not _live_available:
        return
    if _wallet.delete_wallet():
        await _safe_reply(update.message, "\u2705 \u94b1\u5305\u5df2\u5b89\u5168\u5220\u9664")
    else:
        await _safe_reply(update.message, "\u274c \u6ca1\u6709\u53ef\u5220\u9664\u7684\u94b1\u5305")


async def live_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Live trading control. /live [start|stop|status|positions]"""
    if not update.message:
        return
    if not _live_available:
        await _safe_reply(update.message, "\u274c Live Trading \u6a21\u5757\u4e0d\u53ef\u7528")
        return

    global _live_trader_instance, _trade_scheduler_instance
    args = context.args or []
    subcmd = args[0].lower() if args else "status"

    if subcmd == "start":
        if not _wallet.wallet_exists():
            await _safe_reply(update.message, "\u274c \u8bf7\u5148\u914d\u7f6e\u94b1\u5305: /wallet_setup")
            return

        chat_id = update.effective_chat.id
        async def _live_send(msg):
            try:
                await context.bot.send_message(chat_id=chat_id, text=msg)
            except Exception:
                pass

        _trade_scheduler_instance = _trade_scheduler.TradeScheduler(send_func=_live_send)
        result = await _trade_scheduler_instance.start(mode="live")
        await _safe_reply(update.message, f"\U0001f680 {result}")

    elif subcmd == "stop":
        if _trade_scheduler_instance:
            result = await _trade_scheduler_instance.stop()
            await _safe_reply(update.message, f"\u23f8 {result}")
        else:
            await _safe_reply(update.message, "\u274c No active trading session")

    elif subcmd == "positions":
        positions = _live_trader._load_positions()
        open_pos = [p for p in positions if p.get("status") == "open"]
        if not open_pos:
            await _safe_reply(update.message, "\U0001f4ad \u65e0\u6301\u4ed3")
            return
        lines = ["\U0001f4b0 Live Positions:\n"]
        for p in open_pos:
            age_h = (time.time() - p.get("entry_time", 0)) / 3600
            lines.append(f"  {p.get('symbol','?')} | {p.get('amount_sol',0):.4f} SOL | {age_h:.1f}h")
        await _safe_reply(update.message, "\n".join(lines))

    else:  # status
        status = _live_trader.format_live_status()
        if _trade_scheduler_instance:
            status += f"\n\n{_trade_scheduler_instance.status()}"
        await _safe_reply(update.message, status)


# ── DEX Trading Commands ─────────────────────────────────────────────

# Address cache for callback lookups (callback_data limited to 64 bytes)
_recent_addresses: dict = {}

def _cache_address(address: str):
    """Cache full address keyed by prefix."""
    _recent_addresses[address[:20]] = address
    # Keep cache bounded
    if len(_recent_addresses) > 200:
        keys = list(_recent_addresses.keys())
        for k in keys[:100]:
            _recent_addresses.pop(k, None)

def _find_full_address(prefix: str):
    """Find full address from prefix."""
    if prefix in _recent_addresses:
        return _recent_addresses[prefix]
    # Try positions
    for p in _dex.get_positions() if _dex else []:
        if p.get("address", "").startswith(prefix):
            return p["address"]
    return None


async def handle_token_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Detect pasted Solana token address and show token card with buy buttons."""
    if not update.message or not update.message.text:
        return
    if not _dex_available:
        return

    text = update.message.text.strip()

    # Detect Solana address (base58, 32-44 chars)
    import re
    if not re.match(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$', text):
        return

    await update.message.reply_text("🔍 Looking up token...")

    info = await _dex.lookup_token(text)
    if not info:
        await _safe_reply(update.message, "❌ Token not found on DexScreener")
        return

    _cache_address(text)
    card = _dex.format_token_card(info)

    # Check if we already have a position
    existing = _dex.get_position_by_address(text)

    settings = _dex.get_settings()
    buy_amounts = settings.get("buy_buttons", [0.1, 0.3, 0.5, 1.0])

    if existing:
        # Show sell buttons for existing position
        pnl = existing.get("pnl_pct", 0)
        card += f"\n📍 持仓中: {existing.get('amount_sol', 0):.2f} SOL | PnL: {pnl:+.1f}%"

        keyboard = [
            [
                InlineKeyboardButton("Sell 25%", callback_data=f"dex_sell_{text[:20]}_25"),
                InlineKeyboardButton("Sell 50%", callback_data=f"dex_sell_{text[:20]}_50"),
                InlineKeyboardButton("Sell 100%", callback_data=f"dex_sell_{text[:20]}_100"),
            ],
            [
                InlineKeyboardButton(f"Buy {buy_amounts[0]} SOL", callback_data=f"dex_buy_{text[:20]}_{buy_amounts[0]}"),
                InlineKeyboardButton(f"Buy {buy_amounts[-1]} SOL", callback_data=f"dex_buy_{text[:20]}_{buy_amounts[-1]}"),
            ],
            [
                InlineKeyboardButton("📊 Detail", callback_data=f"dex_detail_{text[:20]}"),
                InlineKeyboardButton("🔄 Refresh", callback_data=f"dex_refresh_{text[:20]}"),
            ],
        ]
    else:
        # Show buy buttons for new token
        keyboard = [
            [InlineKeyboardButton(f"{amt} SOL", callback_data=f"dex_buy_{text[:20]}_{amt}") for amt in buy_amounts],
            [
                InlineKeyboardButton("🔄 Refresh", callback_data=f"dex_refresh_{text[:20]}"),
                InlineKeyboardButton("📊 Chart", url=info.get("pair_url", "")),
            ],
        ]

    await _safe_reply(update.message, card, reply_markup=InlineKeyboardMarkup(keyboard))


async def handle_dex_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline button presses for DEX trading."""
    query = update.callback_query
    if not query or not query.from_user:
        return
    if not _is_authorized(query.from_user.id):
        await query.answer("\u26d4 Unauthorized", show_alert=True)
        return
    await query.answer()

    if not _dex_available:
        return

    data = query.data or ""

    if data.startswith("dex_buy_"):
        # Format: dex_buy_<addr_prefix>_<amount>
        parts = data.split("_")
        if len(parts) < 4:
            return
        addr_prefix = parts[2]
        try:
            amount = float(parts[3])
        except (ValueError, IndexError):
            await query.edit_message_text("❌ Invalid amount")
            return

        full_addr = _find_full_address(addr_prefix)
        if not full_addr:
            await query.edit_message_text("❌ Token address expired. Paste the CA again.")
            return

        info = await _dex.lookup_token(full_addr)
        if not info:
            await query.edit_message_text("❌ Token not found")
            return

        pos = _dex.execute_buy(info, amount)
        if pos:
            msg = _dex.format_buy_result(pos, amount)
            try:
                await query.edit_message_text(msg[:4096])
            except Exception:
                await _safe_send(context.bot, query.from_user.id, msg[:4096])
        else:
            await query.edit_message_text("❌ Buy failed")

    elif data.startswith("dex_sell_"):
        parts = data.split("_")
        if len(parts) < 4:
            return
        addr_prefix = parts[2]
        try:
            pct = int(parts[3])
        except (ValueError, IndexError):
            await query.edit_message_text("❌ Invalid percentage")
            return

        full_addr = _find_full_address(addr_prefix)
        if not full_addr:
            await query.edit_message_text("❌ Token address expired. Paste the CA again.")
            return

        await _dex.refresh_positions()

        result = _dex.execute_sell(full_addr, pct)
        if result:
            msg = _dex.format_sell_result(result)
            try:
                await query.edit_message_text(msg[:4096])
            except Exception:
                await _safe_send(context.bot, query.from_user.id, msg[:4096])
        else:
            await query.edit_message_text("❌ No open position found")

    elif data.startswith("dex_refresh_"):
        parts = data.split("_")
        if len(parts) < 3:
            return
        addr_prefix = parts[2]
        full_addr = _find_full_address(addr_prefix)
        if not full_addr:
            return

        info = await _dex.lookup_token(full_addr)
        if info:
            card = _dex.format_token_card(info)
            try:
                await query.edit_message_text((card + "\n🔄 Updated")[:4096])
            except Exception:
                pass

    elif data.startswith("dex_detail_"):
        parts = data.split("_")
        if len(parts) < 3:
            return
        addr_prefix = parts[2]
        full_addr = _find_full_address(addr_prefix)
        if not full_addr:
            return
        pos = _dex.get_position_by_address(full_addr)
        if pos:
            msg = _dex.format_position_detail(pos)
            try:
                await query.edit_message_text(msg[:4096])
            except Exception:
                pass


async def positions_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show all open positions."""
    if not update.message:
        return
    if not _dex_available:
        await _safe_reply(update.message, "DEX module not available")
        return
    await _dex.refresh_positions()
    msg = _dex.format_positions()
    await _safe_reply(update.message, msg[:4096])


async def buy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Buy token. /buy <CA> [amount_sol]"""
    if not update.message or not _dex_available:
        return
    args = context.args or []
    if not args:
        await _safe_reply(update.message, "Usage: /buy <token_address> [amount_sol]\nOr just paste a CA directly!")
        return
    address = args[0]
    try:
        amount = float(args[1]) if len(args) > 1 else _dex.get_settings().get("auto_buy_sol", 0.5)
    except (ValueError, TypeError):
        await _safe_reply(update.message, "❌ Invalid amount. Usage: /buy <CA> [amount_sol]")
        return

    info = await _dex.lookup_token(address)
    if not info:
        await _safe_reply(update.message, "❌ Token not found")
        return

    _cache_address(address)
    pos = _dex.execute_buy(info, amount)
    if pos:
        await _safe_reply(update.message, _dex.format_buy_result(pos, amount))
    else:
        await _safe_reply(update.message, "❌ Buy failed")


async def sell_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sell token. /sell <CA> [pct]"""
    if not update.message or not _dex_available:
        return
    args = context.args or []
    if not args:
        await _safe_reply(update.message, "Usage: /sell <token_address> [25|50|75|100]")
        return
    address = args[0]
    try:
        pct = int(args[1]) if len(args) > 1 else 100
    except (ValueError, TypeError):
        await _safe_reply(update.message, "❌ Invalid percentage. Usage: /sell <CA> [25|50|75|100]")
        return

    await _dex.refresh_positions()
    result = _dex.execute_sell(address, pct)
    if result:
        await _safe_reply(update.message, _dex.format_sell_result(result))
    else:
        await _safe_reply(update.message, "❌ No open position for this token")


async def trade_settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Trading settings. /settings [key] [value]"""
    if not update.message or not _dex_available:
        return
    args = context.args or []

    if len(args) >= 2:
        key = args[0]
        value = args[1]
        # Parse value
        try:
            if value.lower() in ("true", "on", "yes"): value = True
            elif value.lower() in ("false", "off", "no"): value = False
            elif "." in value: value = float(value)
            else: value = int(value)
        except ValueError:
            pass

        valid_keys = list(_dex.DEFAULT_SETTINGS.keys())
        if key in valid_keys:
            _dex.update_settings(**{key: value})
            await _safe_reply(update.message, f"✅ {key} = {value}")
        else:
            await _safe_reply(update.message, f"Unknown setting. Valid: {', '.join(valid_keys)}")
        return

    await _safe_reply(update.message, _dex.format_settings())


async def pnl_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show PnL stats."""
    if not update.message or not _dex_available:
        return
    await _safe_reply(update.message, _dex.format_trade_stats())


# ── Trading Dashboard Panel ──────────────────────────────────────────

async def _build_trading_dashboard() -> str:
    """Build the rich trading dashboard text."""
    lines = []
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("      💹 TRADING DASHBOARD")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")

    # ── Portfolio Summary ──
    if _dex_available:
        try:
            await _dex.refresh_positions()
        except Exception:
            pass
        positions = _dex.get_open_positions() if _dex else []
        total_invested = sum(p.get("amount_sol", 0) for p in positions)
        total_value = sum(p.get("current_value_sol", p.get("amount_sol", 0)) for p in positions)
        total_pnl = total_value - total_invested
        pnl_pct = (total_pnl / total_invested * 100) if total_invested > 0 else 0
        pnl_emoji = "\U0001f7e2" if total_pnl >= 0 else "\U0001f534"

        lines.append("\U0001f4bc PORTFOLIO")
        lines.append(f"  Positions: {len(positions)}")
        lines.append(f"  Invested:  {total_invested:.3f} SOL")
        lines.append(f"  Value:     {total_value:.3f} SOL")
        lines.append(f"  {pnl_emoji} PnL:      {total_pnl:+.4f} SOL ({pnl_pct:+.1f}%)")
        lines.append("")

        # Top positions (up to 3)
        if positions:
            sorted_pos = sorted(positions, key=lambda p: abs(p.get("pnl_pct", 0)), reverse=True)[:3]
            for p in sorted_pos:
                pnl = p.get("pnl_pct", 0)
                em = "\U0001f7e2" if pnl > 0 else "\U0001f534" if pnl < 0 else "\u26aa"
                lines.append(f"  {em} {p.get('symbol', '?'):8s} {pnl:+6.1f}%  {p.get('amount_sol', 0):.2f} SOL")
            lines.append("")
    else:
        lines.append("\U0001f4bc PORTFOLIO: DEX module not loaded")
        lines.append("")

    # ── Trading Stats ──
    if _dex_available:
        stats = _dex.get_trade_stats()
        lines.append("\U0001f4c8 STATS")
        lines.append(f"  Trades: {stats.get('total', 0)}  |  Open: {stats.get('open', 0)}")
        wr = stats.get("win_rate", 0)
        wr_bar = "\U0001f7e9" * int(wr // 10) + "\u2b1c" * (10 - int(wr // 10))
        lines.append(f"  WinRate: {wr:.0f}% {wr_bar}")
        lines.append(f"  Total PnL: {stats.get('total_pnl_sol', 0):+.4f} SOL")
        lines.append(f"  Best: {stats.get('best_pct', 0):+.1f}%  Worst: {stats.get('worst_pct', 0):+.1f}%")
        lines.append("")

    # ── Paper Trading Status ──
    if _paper_trader_available and _paper_trader:
        try:
            cfg = _paper_trader._load_config()
            enabled = cfg.get("enabled", False)
            trades = _paper_trader._load_trades()[-1000:]  # cap trades loaded
            open_trades = [t for t in trades if t.get("status") == "open"]
            closed_trades = [t for t in trades if t.get("status") == "closed"]
            paper_status = "\U0001f7e2 ON" if enabled else "\U0001f534 OFF"
            lines.append(f"\U0001f4dd PAPER MODE: {paper_status}")
            lines.append(f"  Open: {len(open_trades)}  |  Closed: {len(closed_trades)}")
            if closed_trades:
                wins = sum(1 for t in closed_trades if (t.get("pnl_pct") or 0) > 0)
                wr = wins / len(closed_trades) * 100 if closed_trades else 0
                lines.append(f"  Win Rate: {wr:.0f}%  ({wins}W/{len(closed_trades)-wins}L)")
        except Exception:
            lines.append("\U0001f4dd PAPER MODE: Error loading")
    else:
        lines.append("\U0001f4dd PAPER MODE: Not available")
    lines.append("")

    # ── Trading Settings Quick View ──
    if _dex_available:
        s = _dex.get_settings()
        mev = "\U0001f6e1" if s.get("mev_protection") else "\u274c"
        lines.append("\u2699\ufe0f SETTINGS")
        lines.append(f"  Buy Slip: {s.get('buy_slippage_pct', 15)}%  |  Sell Slip: {s.get('sell_slippage_pct', 20)}%")
        lines.append(f"  Default Buy: {s.get('auto_buy_sol', 0.5)} SOL  |  MEV: {mev}")
        lines.append(f"  TP: +{s.get('default_tp_pct', 100)}%  |  SL: {s.get('default_sl_pct', -30)}%")
        lines.append("")

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("  Paste a Solana CA to trade")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    return "\n".join(lines)


def _build_dashboard_keyboard() -> InlineKeyboardMarkup:
    """Build the inline keyboard for the trading dashboard."""
    return InlineKeyboardMarkup([
        # Row 1: Core trading
        [
            InlineKeyboardButton("\U0001f4ca Positions", callback_data="td_positions"),
            InlineKeyboardButton("\U0001f4b0 Buy", callback_data="td_buy_menu"),
            InlineKeyboardButton("\U0001f4b8 Sell", callback_data="td_sell_menu"),
        ],
        # Row 2: Analysis
        [
            InlineKeyboardButton("\U0001f4c8 PnL", callback_data="td_pnl"),
            InlineKeyboardButton("\U0001f4dd Paper", callback_data="td_paper_menu"),
            InlineKeyboardButton("\u2699\ufe0f Settings", callback_data="td_settings"),
        ],
        # Row 3: Signals
        [
            InlineKeyboardButton("\U0001f50d Alpha", callback_data="td_alpha"),
            InlineKeyboardButton("\U0001f517 Onchain", callback_data="td_onchain"),
            InlineKeyboardButton("\U0001f40b Whales", callback_data="td_whales"),
        ],
        # Row 4: Refresh
        [
            InlineKeyboardButton("\U0001f504 Refresh Dashboard", callback_data="td_refresh"),
        ],
    ])


async def trade_dashboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show the main trading dashboard with visual panel and inline commands."""
    if not update.message:
        return
    text = await _build_trading_dashboard()
    kb = _build_dashboard_keyboard()
    await _safe_reply(update.message, text, reply_markup=kb)


async def handle_trade_dashboard_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all trading dashboard inline button presses."""
    query = update.callback_query
    if not query or not query.from_user:
        return
    if not _is_authorized(query.from_user.id):
        await query.answer("\u26d4 Unauthorized", show_alert=True)
        return
    await query.answer()
    data = query.data or ""
    chat_id = query.message.chat_id if query.message else query.from_user.id

    # ── Refresh Dashboard ──
    if data == "td_refresh":
        text = await _build_trading_dashboard()
        kb = _build_dashboard_keyboard()
        try:
            await query.edit_message_text(text, reply_markup=kb)
        except Exception:
            pass
        return

    # ── Positions ──
    if data == "td_positions":
        if not _dex_available:
            try:
                await query.edit_message_text("DEX module not available")
            except Exception:
                pass
            return
        await _dex.refresh_positions()
        positions = _dex.get_open_positions()
        msg = _dex.format_positions()

        rows = []
        # Add per-position buttons
        for p in positions[:5]:
            addr = p.get("address", "")
            sym = p.get("symbol", "?")[:6]
            pnl = p.get("pnl_pct", 0)
            _cache_address(addr)
            rows.append([
                InlineKeyboardButton(f"{sym} {pnl:+.1f}%", callback_data=f"dex_detail_{addr[:20]}"),
                InlineKeyboardButton("Sell 50%", callback_data=f"dex_sell_{addr[:20]}_50"),
                InlineKeyboardButton("Sell 100%", callback_data=f"dex_sell_{addr[:20]}_100"),
            ])
        rows.append([InlineKeyboardButton("\u2b05\ufe0f Dashboard", callback_data="td_back")])

        try:
            await query.edit_message_text(msg[:4096], reply_markup=InlineKeyboardMarkup(rows))
        except Exception:
            await _safe_send(context.bot, chat_id, msg[:4096])
        return

    # ── Buy Menu ──
    if data == "td_buy_menu":
        settings = _dex.get_settings() if _dex_available else {}
        buy_amounts = settings.get("buy_buttons", [0.1, 0.3, 0.5, 1.0])

        msg = (
            "\U0001f4b0 BUY TOKEN\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Paste a Solana token address (CA)\n"
            "to get a token card with buy buttons.\n\n"
            f"Quick buy presets: {buy_amounts}\n"
            f"Default amount: {settings.get('auto_buy_sol', 0.5)} SOL\n\n"
            "Or use command:\n"
            "/buy <CA> [amount_sol]"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("\u2b05\ufe0f Dashboard", callback_data="td_back")],
        ])
        try:
            await query.edit_message_text(msg, reply_markup=kb)
        except Exception:
            pass
        return

    # ── Sell Menu ──
    if data == "td_sell_menu":
        if not _dex_available:
            return
        positions = _dex.get_open_positions()
        if not positions:
            msg = "\U0001f4b8 No open positions to sell\n\nBuy a token first!"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("\u2b05\ufe0f Dashboard", callback_data="td_back")]])
            try:
                await query.edit_message_text(msg, reply_markup=kb)
            except Exception:
                pass
            return

        msg = "\U0001f4b8 SELL — Choose position:\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        rows = []
        for p in positions[:8]:
            addr = p.get("address", "")
            sym = p.get("symbol", "?")[:6]
            pnl = p.get("pnl_pct", 0)
            em = "\U0001f7e2" if pnl > 0 else "\U0001f534"
            _cache_address(addr)
            msg += f"\n{em} {sym}: {pnl:+.1f}% | {p.get('amount_sol', 0):.2f} SOL"
            rows.append([
                InlineKeyboardButton(f"{sym} 25%", callback_data=f"dex_sell_{addr[:20]}_25"),
                InlineKeyboardButton("50%", callback_data=f"dex_sell_{addr[:20]}_50"),
                InlineKeyboardButton("100%", callback_data=f"dex_sell_{addr[:20]}_100"),
            ])
        rows.append([InlineKeyboardButton("\u2b05\ufe0f Dashboard", callback_data="td_back")])

        try:
            await query.edit_message_text(msg[:4096], reply_markup=InlineKeyboardMarkup(rows))
        except Exception:
            pass
        return

    # ── PnL Stats ──
    if data == "td_pnl":
        msg = _dex.format_trade_stats() if _dex_available else "DEX not available"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("\U0001f504 Refresh", callback_data="td_pnl"),
             InlineKeyboardButton("\u2b05\ufe0f Dashboard", callback_data="td_back")],
        ])
        try:
            await query.edit_message_text(msg[:4096], reply_markup=kb)
        except Exception:
            pass
        return

    # ── Paper Trading Menu ──
    if data == "td_paper_menu":
        if not _paper_trader_available or not _paper_trader:
            try:
                await query.edit_message_text(
                    "Paper Trading not available",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("\u2b05\ufe0f Dashboard", callback_data="td_back")]]),
                )
            except Exception:
                pass
            return

        try:
            cfg = _paper_trader._load_config()
            enabled = cfg.get("enabled", False)
            trades = _paper_trader._load_trades()[-1000:]  # cap trades loaded
            open_t = [t for t in trades if t.get("status") == "open"]
            closed_t = [t for t in trades if t.get("status") == "closed"]

            status_em = "\U0001f7e2" if enabled else "\U0001f534"
            msg = (
                f"\U0001f4dd PAPER TRADING\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Status: {status_em} {'ACTIVE' if enabled else 'PAUSED'}\n"
                f"Open trades: {len(open_t)}\n"
                f"Closed trades: {len(closed_t)}\n"
            )

            if closed_t:
                wins = sum(1 for t in closed_t if (t.get("pnl_pct") or 0) > 0)
                total_pnl = sum(t.get("pnl_sol", 0) or 0 for t in closed_t)
                wr = wins / len(closed_t) * 100
                msg += (
                    f"\nWin Rate: {wr:.0f}% ({wins}W/{len(closed_t)-wins}L)\n"
                    f"Total PnL: {total_pnl:+.4f} SOL\n"
                )

            # Graduation check
            if len(closed_t) >= 100 and (sum(1 for t in closed_t if (t.get("pnl_pct") or 0) > 0) / len(closed_t)) >= 0.55:
                msg += "\n\U0001f393 READY FOR LIVE TRADING!"
            elif closed_t:
                needed = max(0, 100 - len(closed_t))
                msg += f"\n\U0001f393 Graduation: {len(closed_t)}/100 trades"
        except Exception:
            msg = "Paper Trading: Error loading data"
            enabled = False

        toggle_text = "\u23f8 Pause" if enabled else "\u25b6\ufe0f Start"
        toggle_data = "td_paper_off" if enabled else "td_paper_on"

        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(toggle_text, callback_data=toggle_data),
                InlineKeyboardButton("\U0001f4ca Stats", callback_data="td_paper_stats"),
            ],
            [
                InlineKeyboardButton("\u274c Close All", callback_data="td_paper_closeall"),
                InlineKeyboardButton("\u2b05\ufe0f Dashboard", callback_data="td_back"),
            ],
        ])
        try:
            await query.edit_message_text(msg[:4096], reply_markup=kb)
        except Exception:
            pass
        return

    # ── Paper on/off/closeall/stats ──
    if data == "td_paper_on" and _paper_trader:
        try:
            cfg = _paper_trader._load_config()
            cfg["enabled"] = True
            _paper_trader._save_config(cfg)
            await query.edit_message_text(
                "\u25b6\ufe0f Paper Trading STARTED\n\nBot will auto-trade on signals.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("\u2b05\ufe0f Dashboard", callback_data="td_back")]]),
            )
        except Exception:
            pass
        return

    if data == "td_paper_off" and _paper_trader:
        try:
            cfg = _paper_trader._load_config()
            cfg["enabled"] = False
            _paper_trader._save_config(cfg)
            await query.edit_message_text(
                "\u23f8 Paper Trading PAUSED",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("\u2b05\ufe0f Dashboard", callback_data="td_back")]]),
            )
        except Exception:
            pass
        return

    if data == "td_paper_closeall" and _paper_trader:
        try:
            trades = _paper_trader._load_trades()
            open_trades = [t for t in trades if t.get("status") == "open"]
            closed = 0
            for t in open_trades:
                price = await _paper_trader._fetch_current_price(t.get("address", ""))
                if price and price > 0:
                    _paper_trader.close_paper_trade(t["id"], price, "manual")
                    closed += 1
            await query.edit_message_text(
                f"\u274c Closed {closed}/{len(open_trades)} paper positions",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("\u2b05\ufe0f Dashboard", callback_data="td_back")]]),
            )
        except Exception:
            pass
        return

    if data == "td_paper_stats" and _paper_trader:
        try:
            report = _paper_trader.format_stats_full()
            await query.edit_message_text(
                report[:4096],
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("\U0001f504 Refresh", callback_data="td_paper_stats"),
                     InlineKeyboardButton("\u2b05\ufe0f Dashboard", callback_data="td_back")],
                ]),
            )
        except Exception:
            pass
        return

    # ── Settings ──
    if data == "td_settings":
        msg = _dex.format_settings() if _dex_available else "DEX not available"
        msg += "\n\nUse /settings <key> <value> to change"

        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("MEV: Toggle", callback_data="td_set_mev"),
                InlineKeyboardButton("Auto: Toggle", callback_data="td_set_auto"),
            ],
            [
                InlineKeyboardButton("Slip +5", callback_data="td_set_slip_up"),
                InlineKeyboardButton("Slip -5", callback_data="td_set_slip_down"),
            ],
            [InlineKeyboardButton("\u2b05\ufe0f Dashboard", callback_data="td_back")],
        ])
        try:
            await query.edit_message_text(msg[:4096], reply_markup=kb)
        except Exception:
            pass
        return

    # ── Settings toggles ──
    if data == "td_set_mev" and _dex_available:
        s = _dex.get_settings()
        new_val = not s.get("mev_protection", True)
        _dex.update_settings(mev_protection=new_val)
        em = "\U0001f6e1 ON" if new_val else "\u274c OFF"
        try:
            await query.edit_message_text(
                f"MEV Protection: {em}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("\u2b05\ufe0f Settings", callback_data="td_settings")]]),
            )
        except Exception:
            pass
        return

    if data == "td_set_auto" and _dex_available:
        s = _dex.get_settings()
        new_val = not s.get("auto_approve", False)
        _dex.update_settings(auto_approve=new_val)
        em = "\u2705 ON" if new_val else "\u274c OFF"
        try:
            await query.edit_message_text(
                f"Auto Confirm: {em}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("\u2b05\ufe0f Settings", callback_data="td_settings")]]),
            )
        except Exception:
            pass
        return

    if data == "td_set_slip_up" and _dex_available:
        s = _dex.get_settings()
        new_buy = min(50, s.get("buy_slippage_pct", 15) + 5)
        new_sell = min(50, s.get("sell_slippage_pct", 20) + 5)
        _dex.update_settings(buy_slippage_pct=new_buy, sell_slippage_pct=new_sell)
        try:
            await query.edit_message_text(
                f"Slippage: Buy {new_buy}% | Sell {new_sell}%",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("\u2b05\ufe0f Settings", callback_data="td_settings")]]),
            )
        except Exception:
            pass
        return

    if data == "td_set_slip_down" and _dex_available:
        s = _dex.get_settings()
        new_buy = max(1, s.get("buy_slippage_pct", 15) - 5)
        new_sell = max(1, s.get("sell_slippage_pct", 20) - 5)
        _dex.update_settings(buy_slippage_pct=new_buy, sell_slippage_pct=new_sell)
        try:
            await query.edit_message_text(
                f"Slippage: Buy {new_buy}% | Sell {new_sell}%",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("\u2b05\ufe0f Settings", callback_data="td_settings")]]),
            )
        except Exception:
            pass
        return

    # ── Alpha Signals ──
    if data == "td_alpha":
        try:
            if _alpha_engine and hasattr(_alpha_engine, 'get_latest_picks'):
                picks = _alpha_engine.get_latest_picks(5)
                if picks:
                    msg = "\U0001f50d ALPHA SIGNALS (Latest 5)\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    for p in picks:
                        sym = p.get("symbol", "?")
                        score = p.get("score", 0)
                        msg += f"\n\u2b50 {sym} — score: {score:.1f}"
                        addr = p.get("address", "")
                        if addr:
                            _cache_address(addr)
                            msg += f"\nCA: {addr[:20]}..."
                else:
                    msg = "\U0001f50d No recent alpha signals"
            else:
                msg = "\U0001f50d Alpha Engine: use /alpha for full scan"
        except Exception:
            msg = "\U0001f50d Alpha: use /alpha for full scan"

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("\U0001f504 Scan Now", callback_data="td_alpha_scan"),
             InlineKeyboardButton("\u2b05\ufe0f Dashboard", callback_data="td_back")],
        ])
        try:
            await query.edit_message_text(msg[:4096], reply_markup=kb)
        except Exception:
            pass
        return

    if data == "td_alpha_scan":
        try:
            await query.edit_message_text("\U0001f50d Scanning alpha signals...")
            if _scan_alpha:
                result = await _scan_alpha()
                report = _format_alpha_report(result) if _format_alpha_report and result else "No signals found"
                await _safe_send(context.bot, chat_id, report[:4096])
            else:
                await _safe_send(context.bot, chat_id, "Alpha engine not available. Use /alpha")
        except Exception as e:
            await _safe_send(context.bot, chat_id, f"Scan error: {str(e)[:200]}")
        return

    # ── Onchain ──
    if data == "td_onchain":
        msg = (
            "\U0001f517 ONCHAIN SCANNER\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Use /onchain for latest on-chain signals\n"
            "New tokens, trending, top boosts"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("\U0001f504 Scan Now", callback_data="td_onchain_scan"),
             InlineKeyboardButton("\u2b05\ufe0f Dashboard", callback_data="td_back")],
        ])
        try:
            await query.edit_message_text(msg[:4096], reply_markup=kb)
        except Exception:
            pass
        return

    if data == "td_onchain_scan":
        try:
            await query.edit_message_text("\U0001f517 Scanning on-chain...")
            # Trigger onchain scan via the existing function
            if _alpha_engine and hasattr(_alpha_engine, 'onchain_filter_new_only'):
                result = await _alpha_engine.onchain_filter_new_only()
                if result:
                    lines = ["\U0001f517 On-chain Results\n"]
                    for item in (result if isinstance(result, list) else [result])[:5]:
                        if isinstance(item, dict):
                            lines.append(f"\u2022 {item.get('symbol', '?')} — ${item.get('mcap', 0):,.0f} mcap")
                    await _safe_send(context.bot, chat_id, "\n".join(lines))
                else:
                    await _safe_send(context.bot, chat_id, "No new tokens found")
            else:
                await _safe_send(context.bot, chat_id, "Use /onchain for full scan")
        except Exception as e:
            await _safe_send(context.bot, chat_id, f"Scan error: {str(e)[:200]}")
        return

    # ── Whales ──
    if data == "td_whales":
        try:
            if _whale_available and _whale_tracker:
                report = _whale_tracker.format_24h_report()
                msg = report[:3800] if report else "No whale activity"
            else:
                msg = "\U0001f40b Whale tracker not available"
        except Exception:
            msg = "\U0001f40b Whale data unavailable"

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("\U0001f504 Refresh", callback_data="td_whales"),
             InlineKeyboardButton("\u2b05\ufe0f Dashboard", callback_data="td_back")],
        ])
        try:
            await query.edit_message_text(msg[:4096], reply_markup=kb)
        except Exception:
            pass
        return

    # ── Back to Dashboard ──
    if data == "td_back":
        text = await _build_trading_dashboard()
        kb = _build_dashboard_keyboard()
        try:
            await query.edit_message_text(text, reply_markup=kb)
        except Exception:
            pass
        return


async def arb_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show today's arbitrage summary + live top-5 spreads."""
    if not update.message:
        return
    try:
        if not _arb_available or _arb_engine is None:
            await update.message.reply_text("❌ 套利引擎模块不可用。")
            return

        if not _arb_engine.running:
            await update.message.reply_text(
                "⚠️ 套利引擎未运行。\n"
                "重启 bot 后将自动启动 OKX/Bybit/Binance 行情流。"
            )
            return

        # Today's history summary
        summary = _arb_engine.get_today_summary()
        today_text = _format_arb_today(summary)

        # Live signals — TOP 5
        top5 = _arb_engine.get_top_spreads(5)
        live_text = _format_arb_top5(top5)

        # Connection status
        counts = _arb_engine.exchange_count()
        status_parts = [f"{ex}({n})" for ex, n in counts.items()]
        status_line = "已连接: " + " / ".join(status_parts) if status_parts else "连接中..."

        full = f"{today_text}\n\n─────────────────\n{live_text}\n\n📶 {status_line}"
        await _safe_reply(update.message, full[:4096], parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Arb command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ 套利数据获取失败: {str(e)[:200]}")
        except Exception:
            pass


async def whales_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show 24h on-chain smart money activity."""
    if not update.message:
        return
    try:
        if not _whale_available or _whale_tracker is None:
            await update.message.reply_text("⚠️ 链上追踪器未启动")
            return
        report = _whale_tracker.format_24h_report()
        addr_list = _whale_tracker.format_address_list()
        await update.message.reply_text(f"{report}\n\n{addr_list}"[:4000])
    except Exception as e:
        logger.error(f"Whales command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ 链上数据错误: {str(e)[:200]}")
        except Exception:
            pass


async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Web search. Usage: /search <query>"""
    if not update.message or not update.effective_chat:
        return
    query = " ".join(context.args) if context.args else ""
    if not query:
        await update.message.reply_text("🔍 用法: /search <关键词>\n例如: /search python async tutorial")
        return
    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
        from tools import execute_web_search
        result = await execute_web_search(query, max_results=5)
        await update.message.reply_text(f"🔍 {result[:4000]}")
    except Exception as e:
        logger.error(f"Search command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ 搜索失败: {str(e)[:200]}")
        except Exception:
            pass


async def track_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a custom whale address to monitor. Usage: /track <address> [label]"""
    if not update.message:
        return
    try:
        if not _whale_available or _whale_tracker is None:
            await update.message.reply_text("⚠️ 链上追踪器未启动")
            return
        args = context.args
        if not args:
            await update.message.reply_text(
                "用法: /track <地址> [标签]\n"
                "例: /track 0x28C6...1d60 MyWhale\n"
                "例: /track 9WzDX...AWM SolWhale"
            )
            return
        address = args[0].strip()
        label = " ".join(args[1:]) if len(args) > 1 else ""
        added = _whale_tracker.add_address(address, label)
        if added:
            addr_data = _whale_tracker._addresses.get(address, {})
            net = addr_data.get("network", "eth").upper()
            lbl = addr_data.get("label", "")
            await update.message.reply_text(
                f"✅ 已添加监控地址\n"
                f"  网络: {net}\n"
                f"  标签: {lbl}\n"
                f"  地址: {address[:16]}..."
            )
        else:
            await update.message.reply_text(f"ℹ️ 地址已在监控列表中: {address[:16]}...")
    except Exception as e:
        logger.error(f"Track command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ 添加地址失败: {str(e)[:200]}")
        except Exception:
            pass


async def wallets_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show smart money wallet list and recent buy activity. /wallets"""
    if not update.message:
        return
    try:
        if not _smart_tracker_available or _smart_tracker is None:
            await update.message.reply_text("⚠️ 聪明钱追踪器未启动")
            return
        wallet_list = _smart_tracker.format_wallet_list()
        activity = _smart_tracker.format_recent_activity(24)
        await update.message.reply_text(f"{wallet_list}\n\n{activity}"[:4000])
    except Exception as e:
        logger.error(f"Wallets command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ 错误: {str(e)[:200]}")
        except Exception:
            pass


async def addwallet_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a smart money wallet to track. Usage: /addwallet <address> [label]"""
    if not update.message:
        return
    try:
        if not _smart_tracker_available or _smart_tracker is None:
            await update.message.reply_text("⚠️ 聪明钱追踪器未启动")
            return
        args = context.args or []
        if not args:
            await update.message.reply_text(
                "用法: /addwallet <地址> [标签]\n"
                "例: /addwallet 0x1234...abcd MyWhale\n"
                "例: /addwallet 9WzDX...AWM SolTrader"
            )
            return
        address = args[0].strip()
        label = " ".join(args[1:]) if len(args) > 1 else ""
        added = _smart_tracker.add_wallet(address, label)
        if added:
            wallets = _smart_tracker.get_wallets()
            meta = wallets.get(address, {})
            await update.message.reply_text(
                f"✅ 已添加聪明钱地址\n"
                f"标签: {meta.get('label', '')}\n"
                f"网络: {meta.get('network', 'eth').upper()}\n"
                f"地址: {address[:8]}...{address[-4:]}\n"
                f"当前跟踪: {len(wallets)}个"
            )
        else:
            await update.message.reply_text(
                f"ℹ️ 该地址已在跟踪列表中: {address[:8]}...{address[-4:]}"
            )
    except Exception as e:
        logger.error(f"Addwallet command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ 添加失败: {str(e)[:200]}")
        except Exception:
            pass


async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generate and send signal performance report."""
    if not update.message or not update.effective_chat:
        return
    try:
        await _send_profit_report(context, update.effective_chat.id)
    except Exception as e:
        logger.error(f"Report command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Report error: {str(e)[:300]}")
        except Exception:
            pass


async def performance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show signal win-rate trend, current param version, and optimization rounds."""
    if not update.message:
        return
    try:
        if not _optimizer_available:
            await update.message.reply_text("❌ strategy_optimizer 模块不可用")
            return
        from strategy_optimizer import format_performance_report
        text = format_performance_report()
        try:
            await update.message.reply_text(text[:4000], parse_mode="Markdown")
        except Exception:
            await update.message.reply_text(text[:4000])
    except Exception as e:
        logger.error(f"Performance command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ 性能报告错误: {str(e)[:300]}")
        except Exception:
            pass


async def risk_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show risk metrics."""
    if not update.message or not update.effective_chat:
        return
    try:
        await _send_risk(context, update.effective_chat.id)
    except Exception as e:
        logger.error(f"Risk command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Risk error: {str(e)[:300]}")
        except Exception:
            pass


async def codex_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Codex自充能 — 显示状态或手动触发Codex进化任务。"""
    if not update.message:
        return
    try:
        if not _codex_available:
            await update.message.reply_text("❌ codex_charger 模块不可用")
            return

        args = context.args or []
        sub = args[0].lower() if args else "status"

        if sub == "status":
            status = _codex.get_status()
            await update.message.reply_text(
                f"🔋 **Codex自充能状态**\n\n{status}\n\n"
                f"命令:\n"
                f"`/codex` — 查看状态\n"
                f"`/codex test` — 测试Codex连接\n"
                f"`/codex cli` — 强制切换到CLI模式\n"
                f"`/codex web` — 强制切换到Codex模式",
                parse_mode="Markdown",
            )

        elif sub == "test":
            await update.message.reply_text("🌐 正在测试 Codex (claude.ai/code) 连接...")
            charger = _codex.CodexCharger()
            result = await asyncio.wait_for(
                charger.run_task("Say exactly: ✅任务完成 — this is a connection test"),
                timeout=120,
            )
            if result.get("success"):
                await update.message.reply_text(
                    f"✅ Codex连接成功！\n"
                    f"耗时: {result.get('duration', 0):.1f}s\n"
                    f"响应预览: {str(result.get('output', ''))[:200]}"
                )
            else:
                await update.message.reply_text(
                    f"❌ Codex连接失败\n错误: {str(result.get('error', ''))[:300]}"
                )

        elif sub == "cli":
            _codex.mark_cli_recovered()
            await update.message.reply_text("💻 已切换到 CLI 模式")

        elif sub == "web":
            _codex.mark_cli_exhausted()
            await update.message.reply_text("🌐 已切换到 Codex (Web) 模式")

        else:
            await update.message.reply_text(f"❓ 未知子命令: {sub}\n使用 `/codex` 查看帮助")

    except asyncio.TimeoutError:
        await update.message.reply_text("⏱ Codex测试超时 (120s)")
    except Exception as e:
        logger.error(f"Codex command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Codex命令错误: {str(e)[:300]}")
        except Exception:
            pass


async def optimize_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manually trigger strategy optimization.
    /optimize          — Phase 1 win-rate tuning
    /optimize ga       — P3_24 Genetic Algorithm optimization
    /optimize history  — show Phase 1 optimization history
    """
    if not update.message:
        return
    try:
        if not _optimizer_available:
            await update.message.reply_text("❌ strategy_optimizer 模块不可用")
            return

        args = context.args or []
        sub = args[0].lower() if args else "run"

        if sub == "history":
            summary = _strategy_optimizer.get_optimization_summary()
            await _safe_reply(update.message, summary, parse_mode="Markdown")
            return

        if sub == "ga":
            # P3_24: Genetic Algorithm parameter optimization
            await _safe_reply(
                update.message,
                "🧬 **P3_24 遗传算法优化启动**\n"
                f"种群大小: {_strategy_optimizer.GA24_POPULATION_SIZE}  "
                f"进化代数: {_strategy_optimizer.GA24_GENERATIONS}\n"
                "请稍候，正在优化中...",
                parse_mode="Markdown",
            )
            result = await _strategy_optimizer.genetic_optimizer.optimize_now(trigger="manual")
            msg = _strategy_optimizer.format_ga24_result(result)
            await _safe_reply(update.message, msg, parse_mode="Markdown")
            return

        # Default: Phase 1 win-rate optimization
        await update.message.reply_text("⚙️ 正在分析信号数据并优化参数...")
        result = await _strategy_optimizer.strategy_optimizer.optimize_now(trigger="manual")
        await _safe_reply(update.message, result.get("message", "No result"), parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Optimize command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ 优化失败: {str(e)[:300]}")
        except Exception:
            pass


async def selfcheck_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check critical files exist and have valid Python syntax."""
    if not update.message:
        return
    try:
        import ast
        base = os.path.dirname(os.path.abspath(__file__))
        critical_files = [
            "bot.py", "run.py", "claude_agent.py", "config.py",
            "tools.py", "skill_library.py", "self_monitor.py",
            "proactive_agent.py", "harness_learn.py", "auto_research.py",
        ]
        lines = ["🔍 **Self-Check Report**\n"]
        all_ok = True
        for fname in critical_files:
            fpath = os.path.join(base, fname)
            if not os.path.exists(fpath):
                lines.append(f"❌ `{fname}` — NOT FOUND")
                all_ok = False
                continue
            size = os.path.getsize(fpath)
            try:
                with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                    src = f.read()
                ast.parse(src)
                lines.append(f"✅ `{fname}` ({size//1024}KB)")
            except SyntaxError as se:
                lines.append(f"⚠️ `{fname}` — SYNTAX ERROR line {se.lineno}: {se.msg}")
                all_ok = False
            except Exception as e2:
                lines.append(f"⚠️ `{fname}` — READ ERROR: {e2}")
                all_ok = False

        # Check error log
        error_log = os.path.join(base, "_error_log.txt")
        if os.path.exists(error_log):
            size = os.path.getsize(error_log)
            with open(error_log, "r", encoding="utf-8", errors="replace") as f:
                # Read only last 8KB to avoid loading huge error logs into memory
                f.seek(max(0, size - 8192))
                last_lines = f.readlines()[-3:]
            last = "".join(last_lines).strip()[:200]
            lines.append(f"\n📋 `_error_log.txt` ({size//1024}KB)\nLast entry: `{last}`")
        else:
            lines.append("\n📋 `_error_log.txt` — no crashes recorded ✅")

        lines.append(f"\n{'✅ All checks passed' if all_ok else '⚠️ Issues found — see above'}")
        await _safe_reply(update.message, "\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Selfcheck error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ selfcheck error: {str(e)[:300]}")
        except Exception:
            pass


async def repairs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show recent auto-repair history from CodeSelfRepair."""
    if not update.message:
        return
    try:
        n = 10
        if context.args:
            try:
                n = max(1, min(50, int(context.args[0])))
            except ValueError:
                pass
        records = code_repair.get_recent_repairs(n)
        if not records:
            await update.message.reply_text("🔧 No auto-repairs recorded yet.")
            return
        lines = [f"🔧 **Auto-Repair History** (last {len(records)})\n"]
        for r in records:
            ts = r.get("ts", "?")[:19].replace("T", " ")
            ok = "✅" if r.get("success") else "❌"
            bak = " 💾bak" if r.get("backed_up") else ""
            conf = float(r.get("confidence", 0) or 0)
            etype = r.get("error_type", "?")
            fname = r.get("file", "?")
            line = r.get("line", "?")
            emsg = r.get("error_msg", "")[:60]
            lines.append(
                f"{ok} `{ts}` **{etype}**{bak}\n"
                f"   📄 `{fname}:{line}` conf={conf:.0%}\n"
                f"   _{emsg}_"
            )
        await _safe_reply(update.message, "\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"repairs_command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ repairs error: {str(e)[:300]}")
        except Exception:
            pass


async def repair_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show code health status and repair history (/repair_status)."""
    if not update.message:
        return
    try:
        if not _self_repair_available:
            await update.message.reply_text("⚠️ self_repair module not available.")
            return
        # Optionally trigger an immediate scan
        run_scan = context.args and context.args[0].lower() in ("scan", "now")
        if run_scan:
            await update.message.reply_text("🔍 正在扫描代码健康状态...")
            await proactive_repair.run_scan_now()
        report = format_repair_status(n_recent=10)
        await _safe_reply(update.message, report, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"repair_status_command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ repair_status error: {str(e)[:300]}")
        except Exception:
            pass


async def evostatus_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show this week's self-evolution stats (/evostatus). Pass 'run' to trigger immediately."""
    if not update.message:
        return
    try:
        if not _self_repair_available:
            await update.message.reply_text("⚠️ self_repair module not available.")
            return
        run_now = context.args and context.args[0].lower() in ("run", "now", "go")
        if run_now:
            await update.message.reply_text("🧬 正在运行自进化周期...")
            result = (await code_evolution_engine.run_now()) or {}
            status = result.get("status", "unknown")
            target = result.get("target", "N/A")
            applied = result.get("applied", False)
            rolled = result.get("rolled_back", False)
            await _safe_reply(
                update.message,
                f"进化结果: `{status}`\n"
                f"目标: `{target}`\n"
                f"已应用: {'✅' if applied else '❌'}\n"
                f"已回滚: {'⏮️' if rolled else '—'}",
                parse_mode="Markdown",
            )
        report = format_evostatus()
        await _safe_reply(update.message, report, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"evostatus_command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ evostatus error: {str(e)[:300]}")
        except Exception:
            pass


async def code_health_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show code quality scores and pending patches. Pass 'scan' to re-run analysis."""
    if not update.message:
        return
    try:
        if not _self_repair_available:
            await update.message.reply_text("⚠️ self_repair module not available.")
            return
        run_scan = context.args and context.args[0].lower() in ("scan", "run", "now")
        if run_scan:
            await update.message.reply_text("🔬 正在运行代码质量分析...")
            report = analyze_code_quality()
            low_count = len(report.get("low_quality_files", []))
            avg = report.get("avg_score", 0)
            await update.message.reply_text(
                f"✅ 扫描完成: {report.get('file_count', 0)}个文件, 平均分{avg}/100, "
                f"低质量文件{low_count}个",
            )
        health = format_code_health()
        await _safe_reply(update.message, health, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"code_health_command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ code_health error: {str(e)[:300]}")
        except Exception:
            pass


async def selfrepair_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Full syntax scan + auto-repair of all .py files (/selfrepair)."""
    if not update.message:
        return
    try:
        if not _self_repair_available:
            await update.message.reply_text("⚠️ self_repair module not available.")
            return
        await update.message.reply_text("🔍 正在全量扫描所有 Python 文件的语法错误...")
        results = await proactive_repair.run_scan_now()
        syntax_errs = results.get("syntax_errors", [])
        import_errs = results.get("import_errors", [])
        fixed = results.get("fixed", [])
        installed = results.get("installed", [])
        lines = ["🔧 *全量自检修复结果*\n"]
        if not syntax_errs and not import_errs:
            lines.append("✅ 所有文件语法正常，无错误")
        else:
            if syntax_errs:
                lines.append(f"⚠️ *语法错误* ({len(syntax_errs)} 个):")
                for e in syntax_errs[:5]:
                    lines.append(f"  • `{e['file']}` 行{e.get('line',0)}: {e['error_msg'][:60]}")
            if import_errs:
                lines.append(f"\n⚠️ *导入错误* ({len(import_errs)} 个):")
                for e in import_errs[:5]:
                    lines.append(f"  • `{e['file']}`: {e['error_msg'][:60]}")
        if fixed:
            lines.append(f"\n🔧 *已自动修复* ({len(fixed)} 个): {', '.join(f'`{f}`' for f in fixed)}")
        if installed:
            lines.append(f"\n📦 *已安装依赖*: {', '.join(f'`{p}`' for p in installed)}")
        await _safe_reply(update.message, "\n".join(lines), parse_mode="Markdown")
        report = format_repair_status(n_recent=5)
        await _safe_reply(update.message, report, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"selfrepair_command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ selfrepair error: {str(e)[:300]}")
        except Exception:
            pass


# ─── Trading Skill Commands ──────────────────────────────────────────────────

CRYPTO_SERVER = "http://127.0.0.1:8001"
CRYPTO_ANALYSIS_DIR = os.path.join(os.path.expanduser("~"), "Desktop", "crypto-analysis-")


async def token_analyze_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Analyze a token on-chain (Solana/EVM).

    Usage: /token_analyze <address> [network] [pool]
    Example: /token_analyze So11...  solana
    """
    if not update.message:
        return
    if not context.args:
        await update.message.reply_text(
            "Usage: /token_analyze <token_address> [network] [pool]\n\n"
            "Example:\n"
            "  /token_analyze EPjFW...  solana\n"
            "  /token_analyze 0xdAC1...  ethereum"
        )
        return

    address = context.args[0]
    network = context.args[1] if len(context.args) > 1 else "solana"
    pool = context.args[2] if len(context.args) > 2 else None

    msg = await update.message.reply_text(f"Analyzing token {address[:12]}... on {network}...")
    try:
        import httpx
        url = f"{CRYPTO_SERVER}/api/onchain/token/analyze/{address}?network={network}"
        if pool:
            url += f"&pool={pool}"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()

        if "error" in data:
            await msg.edit_text(f"Token Analysis Error:\n{str(data.get('error', 'Unknown'))[:300]}")
            return

        # Format results for Telegram
        lines = [f"Token Analysis: {address[:16]}...\n"]
        if isinstance(data, dict):
            for key, val in data.items():
                if isinstance(val, dict):
                    lines.append(f"\n{key}:")
                    for k2, v2 in list(val.items())[:10]:
                        lines.append(f"  {k2}: {v2}")
                elif isinstance(val, list):
                    lines.append(f"{key}: [{len(val)} items]")
                else:
                    lines.append(f"{key}: {val}")
        else:
            lines.append(str(data)[:3000])

        text = "\n".join(lines)[:4096]
        await msg.edit_text(text)
    except Exception as e:
        err = str(e)[:300]
        if "ConnectError" in err or "Connection refused" in err:
            await msg.edit_text(
                "Crypto analysis server not running.\n"
                f"Start it: cd {CRYPTO_ANALYSIS_DIR} && python run.py"
            )
        else:
            await msg.edit_text(f"Token analysis failed: {err}")


async def okx_backtest_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Run OKX Top-30 x 100 Strategies backtest (background).

    Usage: /okx_backtest [timeframe]
    Example: /okx_backtest 1H
    """
    if not update.message or not update.effective_chat:
        return
    tf = context.args[0] if context.args else "1H"
    chat_id = update.effective_chat.id

    msg = await update.message.reply_text(
        f"Starting OKX Top-30 backtest (TF={tf})...\n"
        "This takes several minutes. Results will be sent when ready."
    )

    async def _run_backtest():
        try:
            import subprocess
            script = os.path.join(CRYPTO_ANALYSIS_DIR, "strategy_backtest_100.py")
            if not os.path.exists(script):
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"Script not found: {script}"
                )
                return

            result = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: subprocess.run(
                    [sys.executable, script, "--tf", tf],
                    capture_output=True, text=True, timeout=600,
                    cwd=CRYPTO_ANALYSIS_DIR,
                )
            )

            output = result.stdout or ""
            stderr = result.stderr or ""

            if result.returncode != 0:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"OKX Backtest failed (rc={result.returncode}):\n{stderr[:3500]}"[:4096]
                )
                return

            # Send last ~3500 chars (the summary section)
            summary = output[-3500:] if len(output) > 3500 else output
            if not summary.strip():
                summary = "Backtest completed but no output captured."

            # Split into chunks if needed
            for i in range(0, len(summary), 4000):
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"OKX Backtest Results:\n\n{summary[i:i+4000]}"
                )
        except (asyncio.TimeoutError, subprocess.TimeoutExpired):
            await context.bot.send_message(
                chat_id=chat_id, text="OKX Backtest timed out (10 min limit)."
            )
        except Exception as e:
            await context.bot.send_message(
                chat_id=chat_id, text=f"OKX Backtest error: {str(e)[:500]}"
            )

    task = asyncio.create_task(_run_backtest())
    _track_task(context.bot_data, task)


async def ma_ribbon_backtest_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """MA Ribbon multi-TF backtest via crypto-analysis server.

    Usage: /ma_ribbon_backtest <symbol> [anchor_tf] [forward_bars] [success_pct]
    Example: /ma_ribbon_backtest BTC 1d 5 2.0
    """
    if not update.message:
        return
    if not context.args:
        await update.message.reply_text(
            "Usage: /ma_ribbon_backtest <symbol> [anchor_tf] [forward_bars] [success_pct]\n\n"
            "Example:\n"
            "  /ma_ribbon_backtest BTC\n"
            "  /ma_ribbon_backtest ETH 4h 10 3.0"
        )
        return

    symbol = context.args[0].upper()
    anchor_tf = context.args[1] if len(context.args) > 1 else "1d"
    forward_bars = context.args[2] if len(context.args) > 2 else "5"
    success_pct = context.args[3] if len(context.args) > 3 else "2.0"

    msg = await update.message.reply_text(f"Running MA Ribbon backtest for {symbol} ({anchor_tf})...")
    try:
        import httpx
        url = (
            f"{CRYPTO_SERVER}/api/ma-ribbon/backtest"
            f"?symbol={symbol}&anchor_tf={anchor_tf}"
            f"&forward_bars={forward_bars}&success_pct={success_pct}"
        )
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()

        if "error" in data:
            await msg.edit_text(f"Backtest Error:\n{str(data.get('error', 'Unknown'))[:300]}")
            return

        lines = [f"MA Ribbon Backtest: {symbol} ({anchor_tf})\n"]

        # Format score-band results
        if "score_bands" in data:
            lines.append("Score Band Results:")
            for band in data["score_bands"]:
                score = band.get("score_range", "?")
                total = band.get("total", 0)
                success = band.get("success_rate", 0)
                avg_ret = band.get("avg_return", 0)
                lines.append(
                    f"  Score {score}: {total} signals | "
                    f"Win {success:.1f}% | Avg {avg_ret:+.2f}%"
                )

        # Overall stats
        for key in ["total_signals", "overall_success_rate", "tier", "score"]:
            if key in data:
                lines.append(f"{key}: {data[key]}")

        # Dump remaining top-level keys
        shown = {"score_bands", "total_signals", "overall_success_rate", "tier", "score"}
        for key, val in data.items():
            if key not in shown:
                if isinstance(val, (int, float, str, bool)):
                    lines.append(f"{key}: {val}")

        text = "\n".join(lines)[:4096]
        await msg.edit_text(text)
    except Exception as e:
        err = str(e)[:300]
        if "ConnectError" in err or "Connection refused" in err:
            await msg.edit_text(
                "Crypto analysis server not running.\n"
                f"Start it: cd {CRYPTO_ANALYSIS_DIR} && python run.py"
            )
        else:
            await msg.edit_text(f"MA Ribbon backtest failed: {err}")


async def ma_ribbon_screener_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Run MA Ribbon full-market screener (background, takes minutes).

    Usage: /ma_ribbon_screener
    """
    if not update.message or not update.effective_chat:
        return
    chat_id = update.effective_chat.id
    msg = await update.message.reply_text(
        "Starting MA Ribbon full-market screener...\n"
        "Scanning 120 OKX pairs x 2 timeframes. This takes 5-10 minutes.\n"
        "Results will be sent when ready."
    )

    async def _run_screener():
        try:
            import subprocess
            script = os.path.join(CRYPTO_ANALYSIS_DIR, "ma_ribbon_screener.py")
            if not os.path.exists(script):
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"Script not found: {script}"
                )
                return

            result = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: subprocess.run(
                    [sys.executable, script],
                    capture_output=True, text=True, timeout=900,
                    cwd=CRYPTO_ANALYSIS_DIR,
                )
            )

            output = result.stdout or ""
            stderr = result.stderr or ""

            if result.returncode != 0:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"MA Ribbon Screener failed (rc={result.returncode}):\n{stderr[:3500]}"[:4096]
                )
                return

            # Extract the summary section (after the scanning lines)
            summary = output[-3500:] if len(output) > 3500 else output
            if not summary.strip():
                summary = "Screener completed but no output captured."

            for i in range(0, len(summary), 4000):
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"MA Ribbon Screener Results:\n\n{summary[i:i+4000]}"
                )
        except (asyncio.TimeoutError, subprocess.TimeoutExpired):
            await context.bot.send_message(
                chat_id=chat_id, text="MA Ribbon Screener timed out (15 min limit)."
            )
        except Exception as e:
            await context.bot.send_message(
                chat_id=chat_id, text=f"Screener error: {str(e)[:300]}"
            )

    task = asyncio.create_task(_run_screener())
    _track_task(context.bot_data, task)


async def okx_top30_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show OKX Top 30 USDT-SWAP by 24h volume.

    Usage: /okx_top30
    """
    if not update.message:
        return
    msg = await update.message.reply_text("Fetching OKX Top 30 by volume...")
    try:
        import httpx
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "https://www.okx.com/api/v5/market/tickers?instType=SWAP"
            )
            resp.raise_for_status()
            data = resp.json().get("data") or []

        usdt = [d for d in data if d.get("instId", "").endswith("-USDT-SWAP")]
        usdt.sort(key=lambda x: float(x.get("volCcy24h", 0) or 0), reverse=True)
        top30 = usdt[:30]

        lines = ["OKX Top 30 USDT-SWAP (24h Volume)\n"]
        lines.append(f"{'#':>2} {'Symbol':<14} {'Price':>12} {'24h%':>8} {'Vol24h':>14}")
        lines.append("-" * 54)

        for i, t in enumerate(top30, 1):
            sym = t.get("instId", "?").replace("-USDT-SWAP", "")
            price = float(t.get("last", 0) or 0)
            open24 = float(t.get("open24h", 0) or 0)
            chg = ((price - open24) / open24 * 100) if abs(open24) > 0.0001 else 0
            vol = float(t.get("volCcy24h", 0) or 0)

            if vol >= 1_000_000_000:
                vol_str = f"{vol/1e9:.1f}B"
            elif vol >= 1_000_000:
                vol_str = f"{vol/1e6:.1f}M"
            else:
                vol_str = f"{vol:,.0f}"

            emoji = "+" if chg >= 0 else ""
            lines.append(f"{i:>2} {sym:<14} {price:>12.4f} {emoji}{chg:>6.2f}% {vol_str:>14}")

        text = "\n".join(lines)[:4080]
        try:
            await msg.edit_text(f"```\n{text}\n```", parse_mode="Markdown")
        except Exception:
            await msg.edit_text(text[:4096])
    except Exception as e:
        await msg.edit_text(f"OKX Top 30 failed: {str(e)[:300]}")


async def session_control_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Control bot session mode and manage active sessions.

    Usage: /session_control [harness|bridge|api|status]
    """
    if not update.message:
        return
    try:
        return await _session_control_impl(update, context)
    except Exception as e:
        logger.error(f"Session control command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Session control error: {str(e)[:300]}")
        except Exception:
            pass

async def _session_control_impl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if not context.args:
        # Show interactive panel
        if config.HARNESS_MODE:
            current = "Harness (free AI primary)"
        elif config.BRIDGE_MODE:
            current = "Bridge (Claude CLI direct)"
        else:
            current = f"API ({config.CURRENT_PROVIDER})"

        rate_info = ""
        if claude_agent.is_rate_limited():
            remaining = max(0, int(claude_agent._rate_limited_until - time.time()))
            rate_info = f"\nRate limited: {remaining}s remaining"

        sessions_count = len(claude_agent._claude_sessions)
        pending = sum(len(v) for v in claude_agent._pending_messages.values())

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("Harness", callback_data="sc_harness"),
                InlineKeyboardButton("Bridge", callback_data="sc_bridge"),
                InlineKeyboardButton("API", callback_data="sc_api"),
            ],
            [
                InlineKeyboardButton("Clear Sessions", callback_data="sc_clear"),
                InlineKeyboardButton("Kill All", callback_data="sc_kill"),
            ],
        ])

        await update.message.reply_text(
            f"Session Control\n\n"
            f"Current mode: {current}\n"
            f"Model: {config.CLAUDE_MODEL}\n"
            f"Active sessions: {sessions_count}\n"
            f"Pending messages: {pending}"
            f"{rate_info}\n\n"
            "Choose a mode or action:",
            reply_markup=keyboard,
        )
        return

    action = context.args[0].lower()
    if action == "harness":
        config.HARNESS_MODE = True
        config.BRIDGE_MODE = True
        await update.message.reply_text("Harness Mode ON (free AI primary, CLI for tools)")
    elif action == "bridge":
        config.HARNESS_MODE = False
        config.BRIDGE_MODE = True
        bridge.clear_bridge()
        await update.message.reply_text("Bridge Mode ON (Claude CLI direct)")
    elif action == "api":
        config.HARNESS_MODE = False
        config.BRIDGE_MODE = False
        await update.message.reply_text(f"API Mode ON (provider: {config.CURRENT_PROVIDER})")
    elif action == "status":
        if config.HARNESS_MODE:
            mode = "Harness"
        elif config.BRIDGE_MODE:
            mode = "Bridge"
        else:
            mode = f"API ({config.CURRENT_PROVIDER})"
        sessions = len(claude_agent._claude_sessions)
        pending = sum(len(v) for v in claude_agent._pending_messages.values())
        rate = "Yes" if claude_agent.is_rate_limited() else "No"
        await update.message.reply_text(
            f"Mode: {mode}\nModel: {config.CLAUDE_MODEL}\n"
            f"Sessions: {sessions}\nPending: {pending}\nRate limited: {rate}"
        )
    elif action == "clear":
        claude_agent._claude_sessions.clear()
        claude_agent._save_sessions()
        await update.message.reply_text("All sessions cleared.")
    else:
        await update.message.reply_text(
            "Usage: /session_control [harness|bridge|api|status|clear]"
        )


async def monitor_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Control the self-monitoring system. Usage: /monitor [on|off|status]"""
    if not update.message:
        return
    try:
        action = (context.args[0].lower() if context.args else "status")

        if action == "on":
            if not self_monitor._running:
                await self_monitor.start()
            await update.message.reply_text("Self-Monitor started.")
        elif action == "off":
            if self_monitor._running:
                await self_monitor.stop()
            await update.message.reply_text("Self-Monitor stopped.")
        elif action == "status":
            report = self_monitor.get_status_report()
            # Include action memory stats
            patterns = action_memory.get_failure_patterns()
            if patterns:
                report += "\n\n=== Failure Patterns ===\n"
                for p in patterns[:5]:
                    report += f"  {p['action_type']}: {p['error_signature'][:80]} (x{p['count']})\n"
            await update.message.reply_text(report[:4096])
        else:
            await update.message.reply_text("Usage: /monitor [on|off|status]")
    except Exception as e:
        logger.error(f"Monitor command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Monitor error: {str(e)[:300]}")
        except Exception:
            pass


async def proactive_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Control the proactive agent. Usage: /proactive [on|off|status]"""
    if not update.message:
        return
    try:
        from proactive_agent import PROACTIVE_CONFIG

        action = (context.args[0].lower() if context.args else "status")

        if action == "on":
            if not proactive_agent._running:
                await proactive_agent.start()
            await update.message.reply_text("Proactive Agent started.")
        elif action == "off":
            if proactive_agent._running:
                await proactive_agent.stop()
            await update.message.reply_text("Proactive Agent stopped.")
        elif action == "status":
            running = "running" if proactive_agent._running else "stopped"
            tasks = len(proactive_agent._tasks)
            enabled = [k for k, v in PROACTIVE_CONFIG.items() if v]
            disabled = [k for k, v in PROACTIVE_CONFIG.items() if not v]
            text = (
                f"Proactive Agent: {running}\n"
                f"Active tasks: {tasks}\n"
                f"Enabled loops: {', '.join(enabled) if enabled else 'none'}\n"
                f"Disabled loops: {', '.join(disabled) if disabled else 'none'}"
            )
            await update.message.reply_text(text)
        else:
            await update.message.reply_text("Usage: /proactive [on|off|status]")
    except Exception as e:
        logger.error(f"Proactive command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Proactive error: {str(e)[:300]}")
        except Exception:
            pass


async def market_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Control the market monitor. Usage: /market [on|off|status]"""
    if not update.message or not update.effective_chat:
        return
    try:
        action = (context.args[0].lower() if context.args else "status")

        if action == "on":
            if not market_monitor._running:
                _bot = context.bot
                _chat_id = update.effective_chat.id
                async def _send(text):
                    try:
                        await _bot.send_message(
                            chat_id=_chat_id, text=text[:4096]
                        )
                    except Exception:
                        pass
                market_monitor._send = _send
                await market_monitor.start()
            try:
                _status = str(market_monitor.status())
                _interval = _status.split('interval:')[1].split(',')[0].strip() if 'interval:' in _status else '300s'
            except Exception:
                _interval = '300s'
            await update.message.reply_text(
                "Market Monitor started.\n"
                "Watching: BTC/ETH/SOL\n"
                "Alerts: 24h breakout + 1h change >3%\n"
                f"Interval: every {_interval}"
            )
        elif action == "off":
            if market_monitor._running:
                await market_monitor.stop()
            await update.message.reply_text("Market Monitor stopped.")
        elif action == "status":
            await update.message.reply_text(market_monitor.status())
        else:
            await update.message.reply_text("Usage: /market [on|off|status]")
    except Exception as e:
        logger.error(f"Market command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Market error: {str(e)[:300]}")
        except Exception:
            pass


# ─── Autonomy & Consciousness Commands ────────────────────────────────────────

async def autonomy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Control autonomous agent. Usage: /autonomy [start|stop|status|goal <text>]"""
    if not update.message or not update.effective_chat:
        return
    if not _autonomy_available:
        await update.message.reply_text("Autonomy module not available.")
        return
    try:
        engine = get_autonomy_engine()
        action = (context.args[0].lower() if context.args else "status")

        if action == "start":
            chat_id = update.effective_chat.id
            async def _send_status(msg):
                try:
                    await context.bot.send_message(chat_id=chat_id, text=msg[:4096])
                except Exception:
                    pass
            engine.start(send_fn=_send_status, interval=15.0)
            await update.message.reply_text("🤖 Autonomy engine started.")
        elif action == "stop":
            engine.stop()
            await update.message.reply_text("🤖 Autonomy engine stopped.")
        elif action == "goal":
            goal_text = " ".join(context.args[1:]) if len(context.args) > 1 else ""
            if not goal_text:
                await update.message.reply_text("Usage: /autonomy goal <description>")
                return
            goal = engine.add_goal(goal_text)
            await update.message.reply_text(f"🎯 Goal added: {goal.description[:200]}")
        elif action == "status":
            summary = engine.get_status_summary()
            stats = await engine.self_evaluate()
            text = (
                f"🤖 Autonomy Engine\n\n"
                f"{summary}\n"
                f"Success rate: {stats.get('success_rate', 0):.0%}\n"
            )
            active = engine.get_active_goals()
            if active:
                text += "\nActive goals:\n"
                for g in active[:5]:
                    text += f"  • {g.description[:80]} ({g.attempts}/{g.max_attempts})\n"
            await update.message.reply_text(text[:4096])
        else:
            await update.message.reply_text("Usage: /autonomy [start|stop|status|goal <text>]")
    except Exception as e:
        logger.error(f"Autonomy command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ {str(e)[:300]}")
        except Exception:
            pass


async def consciousness_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Self-awareness report. Usage: /consciousness"""
    if not update.message:
        return
    if not _autonomy_available:
        await update.message.reply_text("Consciousness module not available.")
        return
    try:
        awareness = get_self_awareness()
        report = awareness.self_reflect()
        desc = awareness.get_self_description()

        text = f"🧠 Self-Awareness Report\n\n{desc}\n\n"
        text += f"Performance trend: {report.get('performance_trend', 'unknown')}\n"

        gaps = report.get("top_capability_gaps", [])
        if gaps:
            text += "\nCapability gaps:\n"
            for g in gaps[:3]:
                text += f"  • {g.get('reason', '?')[:60]} (×{g.get('count', 0)})\n"

        evolutions = report.get("recent_evolutions", [])
        if evolutions:
            text += f"\nRecent evolutions: {len(evolutions)}\n"
            for e in evolutions[-3:]:
                outcome = e.get("outcome", "?")
                text += f"  • [{outcome}] {e.get('description', '?')[:60]}\n"

        await update.message.reply_text(text[:4096])
    except Exception as e:
        logger.error(f"Consciousness command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ {str(e)[:300]}")
        except Exception:
            pass


async def evolve_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Self-evolution: bot analyzes and improves its own code. Usage: /evolve [focus]"""
    if not update.message or not update.effective_chat:
        return
    try:
        from agents.loop import self_evolve
        focus = " ".join(context.args) if context.args else ""
        chat_id = update.effective_chat.id

        await update.message.reply_text(f"🧬 Self-evolution starting... Focus: {focus or 'general'}")

        async def _send(msg):
            try:
                await context.bot.send_message(chat_id=chat_id, text=msg[:4000])
            except Exception:
                pass

        result = await self_evolve(send_status=_send, focus=focus, max_rounds=3)
        await context.bot.send_message(chat_id=chat_id, text=f"🧬 Evolution result:\n{result[:4000]}")
    except Exception as e:
        logger.error(f"Evolve command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Evolution error: {str(e)[:300]}")
        except Exception:
            pass


async def strategy_evolve_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """GA strategy parameter evolution. Usage: /strategy_evolve"""
    if not update.message or not update.effective_chat:
        return
    try:
        from strategy_optimizer import strategy_optimizer, format_ga_result
        chat_id = update.effective_chat.id

        await update.message.reply_text(
            "🧬 开始策略遗传算法优化...\n"
            "评估 MA Ribbon / RSI / MACD 参数种群（10组×3策略）\n"
            "预计需要 30-60 秒，请稍候..."
        )

        result = await strategy_optimizer.evolve_now()
        msg    = format_ga_result(result)
        await _safe_send(context.bot, chat_id, msg[:4096], parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Strategy evolve command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ 策略进化失败: {str(e)[:300]}")
        except Exception:
            pass


async def memory_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View or edit bot memory. Usage: /memory [show|shortcuts|patterns|summary <text>|set <key> <value>]"""
    if not update.message or not update.effective_chat:
        return
    chat_id = update.effective_chat.id
    args = context.args or []
    subcmd = args[0].lower() if args else "show"

    try:
        if subcmd == "show" or subcmd == "":
            text = memory_engine.format_display()
            await _safe_reply(update.message, text[:4096], parse_mode="Markdown")

        elif subcmd == "shortcuts":
            shortcuts = memory_engine.get_shortcuts()
            if not shortcuts:
                await update.message.reply_text("No shortcuts learned yet.")
                return
            lines = ["⚡ **Shortcuts** (by frequency)\n"]
            for s in shortcuts[:20]:
                lines.append(f"[{s.get('frequency',0)}x] {s['trigger'][:80]}")
            await _safe_reply(update.message, "\n".join(lines)[:4096], parse_mode="Markdown")

        elif subcmd == "patterns":
            patterns = memory_engine.get_patterns(20)
            if not patterns:
                await update.message.reply_text("No patterns recorded yet.")
                return
            lines = ["📊 **Patterns** (success/total)\n"]
            for p in patterns:
                tot = p["success_count"] + p.get("fail_count", 0)
                score = f"{p.get('score',0):.2f}"
                lines.append(f"{p['success_count']}/{tot} [{score}] {p['text'][:70]}")
            await _safe_reply(update.message, "\n".join(lines)[:4096], parse_mode="Markdown")

        elif subcmd == "summary" and len(args) >= 2:
            text = " ".join(args[1:])
            memory_engine.add_summary(text, source="manual")
            await update.message.reply_text("✅ Summary saved.")

        elif subcmd == "set" and len(args) >= 3:
            key = args[1]
            value = " ".join(args[2:])
            memory_engine.update_profile(key, value)
            await update.message.reply_text(f"✅ Profile updated: {key} = {value}")

        elif subcmd == "stats":
            mem = memory_engine.get_memory()
            from memory_engine import _total_entries
            total = _total_entries(mem)
            await update.message.reply_text(
                f"🧠 Memory Stats\n"
                f"Total entries: {total}/500\n"
                f"Shortcuts: {len(mem['shortcuts'])}\n"
                f"Patterns: {len(mem['patterns'])}\n"
                f"Summaries: {len(mem['summaries'])}\n"
                f"Updated: {mem.get('last_updated','?')[:16]}"
            )

        else:
            await update.message.reply_text(
                "Usage: /memory [show|shortcuts|patterns|summary <text>|set <key> <value>|stats]"
            )
    except Exception as e:
        logger.error(f"memory_command error: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Memory error: {str(e)[:300]}")


async def skills_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View synthesized skill library. Usage: /skills [list|stats|top]"""
    if not update.message:
        return
    try:
        import skill_library
        args = context.args or []
        subcmd = args[0].lower() if args else "list"

        if subcmd == "seed":
            seeded = skill_library.seed_evolution_skills()
            await update.message.reply_text(f"✅ 已种入 {seeded} 个技能 (跳过已存在的)")
            return
        elif subcmd == "synth":
            n = skill_library.synthesize_all_to_md()
            await update.message.reply_text(f"✅ 合成了 {n} 个MD技能文件")
            return
        elif subcmd == "stats":
            index = skill_library._load_index()
            entries = index.get("entries", [])
            total = len(entries)
            used = sum(1 for e in entries if e.get("use_count", 0) > 0)
            auto = len(skill_library.list_synthesized_skills())
            msg = (f"📚 技能库统计\n"
                   f"总技能数: {total}\n"
                   f"已使用: {used}\n"
                   f"自动合成MD: {auto}\n"
                   f"最大容量: {skill_library.MAX_SKILLS}")
        elif subcmd == "top":
            index = skill_library._load_index()
            entries = sorted(index.get("entries", []),
                             key=lambda e: e.get("use_count", 0), reverse=True)[:5]
            if not entries:
                msg = "📚 暂无技能记录"
            else:
                lines = ["📚 最常用技能 Top 5:"]
                for e in entries:
                    lines.append(f"• {e.get('title', e['id'])} (用了{e.get('use_count',0)}次)")
                msg = "\n".join(lines)
        else:  # list
            index = skill_library._load_index()
            entries = index.get("entries", [])
            if not entries:
                msg = "📚 技能库为空，完成几个任务后会自动提取技能"
            else:
                lines = [f"📚 技能库 ({len(entries)}个):"]
                for e in entries[:15]:
                    score_str = f" ★{e['avg_score']:.1f}" if e.get("avg_score") else ""
                    lines.append(f"• {e.get('title', e['id'])}{score_str}")
                if len(entries) > 15:
                    lines.append(f"...还有{len(entries)-15}个")
                msg = "\n".join(lines)

        await update.message.reply_text(msg[:4096])
    except Exception as e:
        logger.error(f"skills_command error: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Skills error: {str(e)[:300]}")


async def multi_session_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Multi-session control. Usage: /ms [list|create <name> <dir>|send <name> <msg>]"""
    if not update.message:
        return
    if not _sessions_available:
        await update.message.reply_text("Sessions module not available.")
        return
    try:
        action = (context.args[0].lower() if context.args else "list")

        if action == "list":
            sessions = _session_mgr.list_sessions()
            if not sessions:
                await update.message.reply_text("No sessions. Use /ms create <name> <project_dir>")
                return
            text = "📋 Active Sessions:\n"
            for s in sessions:
                status = "🔴 busy" if s["busy"] else "🟢 idle"
                ctx = "✅" if s["has_context"] else "❌"
                text += f"\n• {s['name']} [{status}] ctx:{ctx}\n  {s['project_dir']}\n"
            await update.message.reply_text(text)

        elif action == "create":
            if len(context.args) < 3:
                await update.message.reply_text("Usage: /ms create <name> <project_dir>")
                return
            name = context.args[1]
            proj_dir = " ".join(context.args[2:])
            _session_mgr.create(name, proj_dir)
            await update.message.reply_text(f"✅ Session '{name}' created → {proj_dir}")

        elif action == "send":
            if len(context.args) < 3:
                await update.message.reply_text("Usage: /ms send <name> <message>")
                return
            name = context.args[1]
            msg = " ".join(context.args[2:])
            await update.message.reply_text(f"⏳ Sending to {name}...")
            result = await _session_mgr.send(name, msg)
            # Truncate for Telegram
            await update.message.reply_text(result[:4000])

        elif action == "broadcast":
            if len(context.args) < 2:
                await update.message.reply_text("Usage: /ms broadcast <message>")
                return
            msg = " ".join(context.args[1:])
            await update.message.reply_text(f"⏳ Broadcasting to {len(getattr(_session_mgr, 'sessions', {}))} sessions...")
            results = await _session_mgr.broadcast(msg)
            text = "Broadcast results:\n"
            for name, result in results.items():
                text += f"\n• {name}: {result[:200]}\n"
            await update.message.reply_text(text[:4000])
        else:
            await update.message.reply_text("Usage: /ms [list|create|send|broadcast]")
    except Exception as e:
        logger.error(f"Multi-session command error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ {str(e)[:300]}")
        except Exception:
            pass


# ─── Screenshot & Quick Actions ───────────────────────────────────────────────

async def quick_screenshot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    try:
        from screenshots import capture_screenshot
    except ImportError:
        await update.message.reply_text("screenshots module not available.")
        return

    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="upload_photo")
    except Exception:
        pass
    try:
        _loop = asyncio.get_running_loop()
        buffer = await _loop.run_in_executor(None, capture_screenshot)
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔄 刷新", callback_data="qa_screenshot"),
                InlineKeyboardButton("⬆️ 上", callback_data="qa_scroll_up"),
                InlineKeyboardButton("⬇️ 下", callback_data="qa_scroll_down"),
            ],
        ])
        await update.message.reply_photo(photo=buffer, reply_markup=keyboard)
    except Exception as e:
        await update.message.reply_text(f"Screenshot failed: {str(e)[:300]}")


async def quick_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    try:
        return await _quick_action_impl(update, context)
    except Exception as e:
        logger.error(f"Quick action error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Quick action error: {str(e)[:300]}")
        except Exception:
            pass

async def _quick_action_impl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📸 截图", callback_data="qa_screenshot"),
            InlineKeyboardButton("🖥 窗口", callback_data="qa_windows"),
        ],
        [
            InlineKeyboardButton("🌐 Chrome", callback_data="qa_open_chrome"),
            InlineKeyboardButton("📁 Explorer", callback_data="qa_open_explorer"),
        ],
        [
            InlineKeyboardButton("💻 Terminal", callback_data="qa_open_terminal"),
            InlineKeyboardButton("📝 VS Code", callback_data="qa_open_vscode"),
        ],
        [
            InlineKeyboardButton("🔒 锁屏", callback_data="qa_lock"),
            InlineKeyboardButton("📊 系统状态", callback_data="qa_sysinfo"),
        ],
        [
            InlineKeyboardButton("📊 评分", callback_data="qa_score"),
            InlineKeyboardButton("🎓 训练", callback_data="qa_train"),
            InlineKeyboardButton("🔪 终止", callback_data="qa_kill"),
        ],
    ])
    await update.message.reply_text("⚡ 快捷操作", reply_markup=keyboard)


async def handle_session_control_callback(update, context):
    """Handle session_control inline button presses (sc_ prefix)."""
    query = update.callback_query
    if not query or not query.from_user:
        return
    if not _is_authorized(query.from_user.id):
        await query.answer("⛔ Unauthorized", show_alert=True)
        return
    if not query.data.startswith("sc_"):
        return
    await query.answer()
    if not query.message:
        return
    chat_id = query.message.chat_id
    action = query.data[3:]  # strip "sc_"

    try:
        if action == "harness":
            config.HARNESS_MODE = True
            config.BRIDGE_MODE = True
            await query.edit_message_text("Harness Mode ON (free AI primary, CLI for tools)")
        elif action == "bridge":
            config.HARNESS_MODE = False
            config.BRIDGE_MODE = True
            bridge.clear_bridge()
            await query.edit_message_text("Bridge Mode ON (Claude CLI direct)")
        elif action == "api":
            config.HARNESS_MODE = False
            config.BRIDGE_MODE = False
            await query.edit_message_text(f"API Mode ON (provider: {config.CURRENT_PROVIDER})")
        elif action == "clear":
            claude_agent._claude_sessions.clear()
            claude_agent._save_sessions()
            await query.edit_message_text("All sessions cleared.")
        elif action == "kill":
            claude_agent._pending_messages.clear()
            claude_agent._claude_sessions.clear()
            claude_agent._save_sessions()
            try:
                import subprocess
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, lambda: subprocess.run(
                    ["powershell", "-NoProfile", "-Command",
                     "Get-Process claude -ErrorAction SilentlyContinue | Stop-Process -Force"],
                    capture_output=True, text=True, timeout=10,
                ))
            except Exception:
                pass
            await query.edit_message_text("All sessions killed, queues cleared.")
    except Exception as e:
        try:
            await context.bot.send_message(chat_id=chat_id, text=f"Session control error: {str(e)[:300]}")
        except Exception:
            pass


async def handle_quick_action_callback(update, context):
    query = update.callback_query
    if not query or not query.from_user:
        return
    if not _is_authorized(query.from_user.id):
        await query.answer("⛔ Unauthorized", show_alert=True)
        return
    if query.data.startswith(("allow_", "deny_")):
        return
    if not query.data.startswith("qa_"):
        return

    await query.answer()
    if not query.message:
        return
    action = query.data
    chat_id = query.message.chat_id

    try:
        if action == "qa_screenshot":
            from screenshots import capture_screenshot
            await context.bot.send_chat_action(chat_id=chat_id, action="upload_photo")
            _loop = asyncio.get_running_loop()
            buffer = await _loop.run_in_executor(None, capture_screenshot)
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🔄 刷新", callback_data="qa_screenshot"),
                    InlineKeyboardButton("⬆️ 上", callback_data="qa_scroll_up"),
                    InlineKeyboardButton("⬇️ 下", callback_data="qa_scroll_down"),
                ],
            ])
            await context.bot.send_photo(chat_id=chat_id, photo=buffer, reply_markup=keyboard)

        elif action == "qa_windows":
            import tools
            result = await tools.execute_list_windows()
            await context.bot.send_message(chat_id=chat_id, text=f"🪟 窗口:\n{result[:3000]}")

        elif action.startswith("qa_open_"):
            import tools
            app_name = action.replace("qa_open_", "")
            result = await tools.execute_open_application(app_name)
            await context.bot.send_message(chat_id=chat_id, text=f"✅ {result}")

        elif action == "qa_lock":
            import tools
            result = await tools.execute_run_command("rundll32.exe user32.dll,LockWorkStation")
            await context.bot.send_message(chat_id=chat_id, text="🔒 已锁屏")

        elif action == "qa_sysinfo":
            import tools
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, tools.execute_get_system_info)
            await context.bot.send_message(chat_id=chat_id, text=f"📊\n{result[:3000]}")

        elif action in ("qa_scroll_up", "qa_scroll_down"):
            import pyautogui
            from screenshots import capture_screenshot
            _loop = asyncio.get_running_loop()
            await _loop.run_in_executor(None, lambda: pyautogui.scroll(5 if action == "qa_scroll_up" else -5))
            await asyncio.sleep(0.3)
            _loop = asyncio.get_running_loop()
            buffer = await _loop.run_in_executor(None, capture_screenshot)
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🔄 刷新", callback_data="qa_screenshot"),
                    InlineKeyboardButton("⬆️ 上", callback_data="qa_scroll_up"),
                    InlineKeyboardButton("⬇️ 下", callback_data="qa_scroll_down"),
                ],
            ])
            await context.bot.send_photo(chat_id=chat_id, photo=buffer, reply_markup=keyboard)

        elif action == "qa_score":
            import harness_learn
            scores = harness_learn.get_recent_scores(10)
            if not scores:
                await context.bot.send_message(chat_id=chat_id, text="📊 暂无评分数据。")
            else:
                avg = sum(s.get("overall", 0) for s in scores) / len(scores)
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"📊 最近{len(scores)}次平均: {avg:.2f}/1.00\n发 /score 查看详情",
                )

        elif action == "qa_train":
            import auto_train
            report = auto_train.get_progress_report()
            await context.bot.send_message(chat_id=chat_id, text=report[:3000])

        elif action == "qa_kill":
            claude_agent._pending_messages.pop(chat_id, None)
            claude_agent._claude_sessions.pop(chat_id, None)
            claude_agent._save_sessions()
            await context.bot.send_message(
                chat_id=chat_id, text="🔪 已终止并清空队列。发新消息重新开始。",
            )

    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"❌ {str(e)[:300]}")


# ─── Session Learning Background ─────────────────────────────────────────────

async def _background_session_scan(learner):
    """Run session log scanning in the background (non-blocking)."""
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, learner.learn_from_all_recent, 10)
        logger.debug("Background session scan completed")
    except Exception as exc:
        logger.debug("Background session scan failed: %s", exc)


# ─── Message Handlers ─────────────────────────────────────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text messages."""
    if not update.message or not update.effective_chat:
        return
    chat_id = update.effective_chat.id
    text = (update.message.text or "").strip()
    if not text:
        return

    # Guard: truncate extremely long messages to prevent CLI arg overflow on Windows
    if len(text) > 30000:
        text = text[:30000] + "\n\n...(消息过长，已截断到30000字符)"

    # ── Fast search shortcut: "search XXX" / "搜索 XXX" → direct DDG search ──
    _lower = text.lower().strip()
    _search_match = None
    for _prefix in ("search ", "搜索 ", "搜索:", "search:"):
        if _lower.startswith(_prefix):
            _search_match = text[len(_prefix):].strip()
            break
    if _search_match:
        try:
            await context.bot.send_chat_action(chat_id=chat_id, action="typing")
            from tools import execute_web_search
            result = await execute_web_search(_search_match, max_results=5)
            await context.bot.send_message(chat_id=chat_id, text=f"🔍 {result[:4000]}")
            self_monitor.record_message_success()
            return
        except Exception as e:
            logger.error(f"Fast search error: {e}")
            # Fall through to normal processing

    try:
        await context.bot.send_chat_action(chat_id=chat_id, action="typing")
    except Exception:
        pass

    _msg_start = time.time()
    try:
        actually_processed = await claude_agent.process_message(text, chat_id, context)
        if not actually_processed:
            return  # Message was queued, don't count as success
        # Record success for self-monitor
        self_monitor.record_message_success()
        duration_ms = (time.time() - _msg_start) * 1000
        self_monitor.record_response_time(duration_ms)
        action_memory.record_action("handle_message", {"text": text[:100]}, success=True, duration_ms=duration_ms)
        if _dashboard_available:
            try:
                _dashboard.record_message(text, True, duration_ms)
            except Exception:
                pass

        # Memory engine: auto-learn successful patterns
        try:
            memory_engine.learn_pattern(text, success=True, duration_ms=duration_ms)
        except Exception:
            pass

        # Intelligence + Reflexion + RAG: learn from success
        if _intel_available:
            try:
                _intel.extract_pattern({
                    "request": text[:300],
                    "response": "(success)",
                    "duration_ms": duration_ms,
                    "tools_used": [],
                })
            except Exception:
                pass
        if _autonomy_available:
            try:
                get_reflexion_engine().reflect_on_action(
                    action=text[:200], result="success", success=True, duration_ms=duration_ms,
                )
                # Store as retrievable solution if response was substantive
                if duration_ms > 2000:  # Non-trivial task
                    get_solution_store().store_solution(
                        task=text[:300], solution="(completed successfully)",
                        category="general", score=1.0,
                    )
            except Exception:
                pass

        # Consciousness: periodic performance snapshot (every 50 messages)
        global _message_counter
        _message_counter += 1
        if _autonomy_available and _message_counter % 50 == 0:
            try:
                awareness = get_self_awareness()
                awareness.record_performance_snapshot()
            except Exception:
                pass
        # Memory: auto-summary every 30 messages
        if _message_counter % 30 == 0:
            try:
                patterns = memory_engine.get_patterns(5)
                if patterns:
                    top = ", ".join(p["text"][:40] for p in patterns[:3])
                    memory_engine.add_summary(
                        f"Session summary (msg #{_message_counter}): top commands: {top}",
                        source="auto"
                    )
            except Exception:
                pass

        # Session learning: periodic background scan
        if _session_learner_available and config.SESSION_LEARNING_ENABLED:
            if _message_counter % config.SESSION_LEARNING_INTERVAL == 0:
                try:
                    learner = _sl.get_learner()
                    _scan_task = asyncio.create_task(_background_session_scan(learner))
                    _track_task(context.bot_data, _scan_task)
                except Exception:
                    pass

    except Exception as e:
        logger.error(f"Error processing message: {e}", exc_info=True)
        # Record error for self-monitor and proactive agent
        duration_ms = (time.time() - _msg_start) * 1000
        try:
            self_monitor.record_error(str(e))
            self_monitor.record_message_failure()
        except Exception:
            pass
        try:
            action_memory.record_action("handle_message", {"text": text[:100]}, success=False, error=str(e), duration_ms=duration_ms)
            memory_engine.learn_pattern(text, success=False, duration_ms=duration_ms)
        except Exception:
            pass
        if _dashboard_available:
            try:
                _dashboard.record_message(text, False, duration_ms)
            except Exception:
                pass

        # Intelligence + Reflexion: analyze failure for learning
        if _intel_available:
            try:
                analysis = _intel.analyze_failure(
                    action="handle_message",
                    error=str(e)[:500],
                    context={"user_message": text[:300], "duration_ms": duration_ms},
                )
                if analysis.get("suggestions"):
                    logger.info(f"Intelligence suggestions: {analysis['suggestions'][:2]}")
            except Exception:
                pass
        if _autonomy_available:
            try:
                get_reflexion_engine().reflect_on_action(
                    action=text[:200], result=str(e)[:300], success=False, duration_ms=duration_ms,
                )
            except Exception:
                pass

        # Session learning: analyze failure
        if _session_learner_available and config.SESSION_LEARNING_ENABLED:
            try:
                learner = _sl.get_learner()
                kb = learner._knowledge
                failure_log = kb.setdefault("failure_log", [])
                failure_log.append({
                    "timestamp": datetime.datetime.now().isoformat(),
                    "user_message": text[:200],
                    "error": str(e)[:500],
                })
                # Cap failure log in-place
                if len(failure_log) > 200:
                    failure_log[:] = failure_log[-200:]
                learner._save_knowledge()
            except Exception:
                pass

        try:
            await proactive_agent.push_error("message_processing", str(e)[:500], source="handle_message")
        except Exception:
            pass
        try:
            await update.message.reply_text(f"❌ Error: {str(e)[:500]}")
        except Exception:
            pass


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle photos — save to disk and pass to Claude with vision support in API mode."""
    if not update.message or not update.effective_chat:
        return
    chat_id = update.effective_chat.id
    caption = update.message.caption or ""

    try:
        await context.bot.send_chat_action(chat_id=chat_id, action="typing")

        # Save photo to disk — handle stickers/GIFs sent as photos
        if not update.message.photo:
            if update.message.sticker:
                sticker = update.message.sticker
                file = await context.bot.get_file(sticker.file_id)
                save_dir = config.TELEGRAM_FILES_DIR
                os.makedirs(save_dir, exist_ok=True)
                ext = ".webp" if not sticker.is_animated else ".tgs"
                save_path = os.path.join(save_dir, f"sticker_{sticker.file_id}{ext}")
                await file.download_to_drive(save_path)
                msg = f"用户发送了一个贴纸，已保存到: {save_path}"
                if caption:
                    msg += f"\n用户说: {caption}"
                await update.message.reply_text(f"🎭 贴纸已保存: {save_path}")
                _sr = await claude_agent.process_message(msg, chat_id, context)
                if _sr:
                    self_monitor.record_message_success()
                return
            await update.message.reply_text("📸 无法获取图片。请重新发送。")
            return
        photo = update.message.photo[-1]  # Largest resolution
        file = await context.bot.get_file(photo.file_id)
        save_dir = config.TELEGRAM_FILES_DIR
        os.makedirs(save_dir, exist_ok=True)
        save_path = os.path.join(save_dir, f"photo_{photo.file_id}.jpg")
        await file.download_to_drive(save_path)

        # In API mode with vision enabled, read as base64
        # Skip this in bridge/harness mode where Claude CLI reads from disk directly
        image_data = None
        if config.ENABLE_VISION and not config.BRIDGE_MODE and not config.HARNESS_MODE:
            try:
                import base64
                def _read_b64(path):
                    with open(path, "rb") as fh:
                        return base64.b64encode(fh.read()).decode("utf-8")
                loop = asyncio.get_running_loop()
                image_data = await loop.run_in_executor(None, _read_b64, save_path)
            except Exception as e:
                logger.warning(f"Failed to read image as base64: {e}")

        # --- Multimodal analysis pipeline (p3_19) ---
        vision_ctx = ""
        needs_confirm = False
        try:
            from vision_engine import analyze_telegram_image
            _loop_ve = asyncio.get_running_loop()
            ve_result = await _loop_ve.run_in_executor(
                None, analyze_telegram_image, save_path, caption
            )
            vision_ctx = ve_result.get("claude_context", "")
            needs_confirm = ve_result.get("needs_confirmation", False)
        except Exception as _ve_err:
            logger.warning(f"Vision analysis skipped: {_ve_err}")

        if vision_ctx:
            msg = f"{vision_ctx}\n\n图片已保存到: {save_path}"
        else:
            msg = f"用户发送了一张图片，已保存到: {save_path}"
            if caption:
                msg += f"\n用户说: {caption}"
            else:
                msg += "\n(无附加说明)"

        status_emoji = "⚠️" if needs_confirm else "📸"
        status_line = f"{status_emoji} 图片已分析: {save_path}"
        await update.message.reply_text(status_line)

        actually_processed = await claude_agent.process_message(msg, chat_id, context, image_data=image_data)
        if actually_processed:
            self_monitor.record_message_success()

    except Exception as e:
        logger.error(f"Photo handling error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ 图片处理失败: {str(e)[:300]}")
        except Exception:
            pass


async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle voice messages — transcribe with Gemini, process as text."""
    if not update.message or not update.effective_chat:
        return
    chat_id = update.effective_chat.id
    try:
        await context.bot.send_chat_action(chat_id=chat_id, action="typing")
    except Exception:
        pass

    tmp = None
    try:
        voice = update.message.voice or update.message.audio
        if not voice:
            await update.message.reply_text("🎙 无法获取音频。")
            return
        # Reject audio over 20 MB
        if voice.file_size and voice.file_size > 20 * 1024 * 1024:
            await update.message.reply_text("🎙 音频太大，最大支持 20 MB。")
            return
        file = await context.bot.get_file(voice.file_id)
        import tempfile
        tmp = os.path.join(tempfile.gettempdir(), f"voice_{voice.file_id}.ogg")
        await file.download_to_drive(tmp)

        transcription = None

        # Try Gemini transcription (free)
        if config.GEMINI_API_KEY:
            try:
                from google import genai as google_genai
                from google.genai import types as gtypes
                client = google_genai.Client(api_key=config.GEMINI_API_KEY)
                with open(tmp, "rb") as f:
                    audio_data = f.read()
                # Gemini SDK is sync — run in executor to avoid blocking event loop
                _loop = asyncio.get_running_loop()
                _contents = [
                    gtypes.Content(role="user", parts=[
                        gtypes.Part(inline_data=gtypes.Blob(mime_type="audio/ogg", data=audio_data)),
                        gtypes.Part(text="Transcribe this voice message exactly. Output only the text. If Chinese, keep Chinese."),
                    ])
                ]
                resp = await asyncio.wait_for(
                    _loop.run_in_executor(
                        None,
                        lambda: client.models.generate_content(
                            model="gemini-2.5-flash",
                            contents=_contents,
                        )
                    ),
                    timeout=60.0,
                )
                text = resp.text.strip() if resp.text else ""
                if text and len(text) > 1:  # Ignore single-char noise
                    transcription = text
            except (ValueError, AttributeError) as e:
                logger.warning(f"Gemini transcription response error: {e}")
            except Exception as e:
                logger.warning(f"Gemini transcription failed: {e}")

        # Fallback: save voice file and tell Claude about it
        if not transcription:
            save_dir = config.TELEGRAM_FILES_DIR
            os.makedirs(save_dir, exist_ok=True)
            import shutil
            save_path = os.path.join(save_dir, f"voice_{voice.file_id}.ogg")
            shutil.copy2(tmp, save_path)
            await update.message.reply_text(f"🎙 语音已保存: {save_path}")
            # Still notify Claude so it can try to process the audio file
            _vr = await claude_agent.process_message(
                f"用户发送了一条语音消息，已保存到: {save_path}\n(无法自动转录，请尝试用其他方式处理)",
                chat_id, context
            )
            if _vr:
                self_monitor.record_message_success()
            return

        await update.message.reply_text(f"🎙 「{transcription}」")
        _vr = await claude_agent.process_message(transcription, chat_id, context)
        if _vr:
            self_monitor.record_message_success()

    except Exception as e:
        logger.error(f"Voice handling error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ 语音处理失败: {str(e)[:300]}")
        except Exception:
            pass
    finally:
        # Always cleanup temp file
        try:
            if tmp and os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle files — save to desktop and notify AI."""
    if not update.message or not update.effective_chat:
        return
    chat_id = update.effective_chat.id
    try:
        await context.bot.send_chat_action(chat_id=chat_id, action="typing")
    except Exception:
        pass

    try:
        doc = update.message.document
        if not doc:
            await update.message.reply_text("📁 无法获取文件。")
            return
        # Reject files over 50 MB to prevent disk exhaustion
        if doc.file_size and doc.file_size > 50 * 1024 * 1024:
            await update.message.reply_text(f"📁 文件太大 ({doc.file_size // (1024*1024)} MB)，最大支持 50 MB。")
            return
        file = await context.bot.get_file(doc.file_id)
        save_dir = config.TELEGRAM_FILES_DIR
        os.makedirs(save_dir, exist_ok=True)
        # Sanitize filename to prevent path traversal and invalid chars
        safe_name = os.path.basename(doc.file_name or "")
        if not safe_name or safe_name in (".", ".."):
            safe_name = f"file_{doc.file_id}"
        # Remove control characters and Windows reserved chars, but keep Unicode (Chinese filenames etc.)
        _WINDOWS_RESERVED = set('<>:"/\\|?*')
        safe_name = "".join(c for c in safe_name if c.isprintable() and c not in _WINDOWS_RESERVED)
        if not safe_name or safe_name in (".", ".."):
            safe_name = f"file_{doc.file_id}"
        save_path = os.path.join(save_dir, safe_name)
        await file.download_to_drive(save_path)

        caption = update.message.caption or ""
        msg = f"用户发送了文件，已保存到: {save_path}"
        if caption:
            msg += f"\n用户说: {caption}"

        await update.message.reply_text(f"📁 文件已保存: {save_path}")
        actually_processed = await claude_agent.process_message(msg, chat_id, context)
        if actually_processed:
            self_monitor.record_message_success()

    except Exception as e:
        logger.error(f"Document handling error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ 文件处理失败: {str(e)[:300]}")
        except Exception:
            pass


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle video/animation — save to disk and tell Claude."""
    if not update.message or not update.effective_chat:
        return
    chat_id = update.effective_chat.id
    try:
        await context.bot.send_chat_action(chat_id=chat_id, action="typing")
        video = update.message.video or update.message.animation or update.message.video_note
        if not video:
            await update.message.reply_text("🎬 无法获取视频。请重新发送。")
            return
        # Reject video over 50 MB
        if hasattr(video, 'file_size') and video.file_size and video.file_size > 50 * 1024 * 1024:
            await update.message.reply_text(f"🎬 视频太大，最大支持 50 MB。")
            return
        file = await context.bot.get_file(video.file_id)
        save_dir = config.TELEGRAM_FILES_DIR
        os.makedirs(save_dir, exist_ok=True)
        ext = ".mp4" if (update.message.video or update.message.video_note) else ".gif"
        save_path = os.path.join(save_dir, f"video_{video.file_id}{ext}")
        await file.download_to_drive(save_path)
        caption = update.message.caption or ""
        msg = f"用户发送了视频/动图，已保存到: {save_path}"
        if caption:
            msg += f"\n用户说: {caption}"
        await update.message.reply_text(f"🎬 已保存: {save_path}")
        actually_processed = await claude_agent.process_message(msg, chat_id, context)
        if actually_processed:
            self_monitor.record_message_success()
    except Exception as e:
        logger.error(f"Video handling error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ 视频处理失败: {str(e)[:300]}")
        except Exception:
            pass


async def handle_unauthorized(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    try:
        user = update.effective_user
        if not user:
            return
        logger.warning(f"Unauthorized: user {user.id} ({user.username})")
        if update.message:
            await update.message.reply_text(
                f"⛔ 未授权。你的 ID: `{user.id}`\n"
                f"将此 ID 加到 .env 的 AUTHORIZED_USER_ID。",
                parse_mode="Markdown",
            )
    except Exception as e:
        logger.error(f"Unauthorized handler error: {e}")


# ─── PID Lock (prevent dual instances) ────────────────────────────────────────

_PID_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".bot.pid")

def _is_python_process(pid):
    """Check if PID is alive and is a Python process.
    On Windows, avoids os.kill(pid, 0) which sends CTRL_C_EVENT and can kill processes.
    """
    if sys.platform == "win32":
        try:
            import subprocess as _sp
            # First check if PID exists at all (fast, no PowerShell)
            result = _sp.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                capture_output=True, text=True, timeout=5,
            )
            if str(pid) not in result.stdout:
                return False
            # Then verify it's a python process
            _cmd_out = _sp.run(
                ["powershell", "-NoProfile", "-Command",
                 f"(Get-Process -Id {pid} -ErrorAction SilentlyContinue).ProcessName"],
                capture_output=True, text=True, timeout=5,
            )
            return "python" in (_cmd_out.stdout or "").strip().lower()
        except Exception:
            return False
    else:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

def _acquire_pid_lock():
    """Check for existing bot instance and kill it before starting.

    Steps:
    1. Read PID file, kill old process if still alive
    2. Scan for any other python processes running bot.py (catch orphans)
    3. Write our PID
    4. Wait for Telegram polling to expire if we killed something
    """
    killed = False
    my_pid = os.getpid()

    # 1. Check PID file
    if os.path.exists(_PID_FILE):
        try:
            with open(_PID_FILE, "r", encoding="utf-8") as _pf:
                old_pid = int(_pf.read().strip())
            if old_pid != my_pid and _is_python_process(old_pid):
                logger.warning(f"Killing previous bot instance (PID {old_pid})")
                print(f"Killing previous bot instance (PID {old_pid})")
                try:
                    if sys.platform == "win32":
                        import subprocess as _sp2
                        _sp2.run(["taskkill", "/PID", str(old_pid), "/F"],
                                 capture_output=True, timeout=5)
                    else:
                        os.kill(old_pid, signal.SIGTERM)
                    time.sleep(3)
                    killed = True
                except Exception:
                    pass
        except (ValueError, IOError):
            pass

    # 2. Scan for orphan bot.py processes (belt and suspenders)
    if sys.platform == "win32":
        try:
            import subprocess as _sp3
            result = _sp3.run(
                ["powershell", "-NoProfile", "-Command",
                 "Get-WmiObject Win32_Process | Where-Object { $_.Name -like 'python*' -and $_.CommandLine -like '*bot.py*' -and $_.ProcessId -ne " + str(my_pid) + " } | Select-Object -ExpandProperty ProcessId"],
                capture_output=True, text=True, timeout=10,
            )
            for line in result.stdout.strip().split("\n"):
                line = line.strip()
                if line and line.isdigit():
                    orphan_pid = int(line)
                    if orphan_pid != my_pid:
                        logger.warning(f"Killing orphan bot.py process (PID {orphan_pid})")
                        print(f"Killing orphan bot.py process (PID {orphan_pid})")
                        try:
                            _sp3.run(["taskkill", "/PID", str(orphan_pid), "/F"],
                                     capture_output=True, timeout=5)
                            killed = True
                        except Exception:
                            pass
        except Exception as e:
            logger.debug(f"Orphan scan failed: {e}")

    # 3. Write our PID
    with open(_PID_FILE, "w", encoding="utf-8") as f:
        f.write(str(os.getpid()))

    # 4. If we killed something, wait for Telegram's long-poll to expire
    if killed:
        wait = 12
        print(f"Killed old instance(s). Waiting {wait}s for Telegram polling to expire...")
        time.sleep(wait)

def _release_pid_lock():
    """Remove PID file on clean exit."""
    try:
        if os.path.exists(_PID_FILE):
            with open(_PID_FILE, "r", encoding="utf-8") as _pf:
                pid_in_file = int(_pf.read().strip())
            if pid_in_file == os.getpid():
                os.remove(_PID_FILE)
    except Exception:
        pass


# ─── Main ─────────────────────────────────────────────────────────────────────

def _startup_health_check():
    """Self-healing startup check: detect and report config issues."""
    issues = []

    # Check Claude CLI availability
    import shutil
    claude_cmd = os.path.join(os.path.expanduser("~"), "AppData", "Roaming", "npm", "claude.cmd")
    if not os.path.exists(claude_cmd) and not shutil.which("claude"):
        issues.append("[WARN] Claude CLI not found - fallback providers will be used")

    # Check if Claude CLI is logged in (quick test)
    try:
        import subprocess
        result = subprocess.run(
            [claude_cmd, "--version"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            issues.append(f"[WARN] Claude CLI returned error: {result.stderr[:100]}")
    except Exception:
        pass  # CLI check is optional

    # API keys not needed — API fallback is disabled (user preference: CLI only, no paid API)

    if issues:
        logger.warning("Startup Health Check: %d issues found", len(issues))
        for issue in issues:
            logger.warning(issue)

    return issues


def main():
    if not config.TELEGRAM_BOT_TOKEN or config.TELEGRAM_BOT_TOKEN == "your_token_here":
        print("ERROR: Set TELEGRAM_BOT_TOKEN in .env")
        return
    if config.AUTHORIZED_USER_ID is None:
        print("NOTE: AUTHORIZED_USER_ID not set. Send any message to get your ID.")

    _startup_health_check()
    _acquire_pid_lock()

    # Seed skill library with evolution knowledge on first run
    try:
        import skill_library
        seeded = skill_library.seed_evolution_skills()
        if seeded:
            logger.info(f"Skill library seeded with {seeded} evolution skills")
    except Exception as _se:
        logger.warning(f"Skill seed failed: {_se}")

    # Start web dashboard on port 8080
    if _dashboard_available:
        try:
            _dashboard.start_dashboard(port=8080)
            print("Dashboard started at http://localhost:8080")
        except Exception as _de:
            logger.warning(f"Dashboard start failed: {_de}")

    app = ApplicationBuilder().token(config.TELEGRAM_BOT_TOKEN).build()

    # Global error handler
    app.add_error_handler(error_handler)

    # Commands
    app.add_handler(CommandHandler("start", start, filters=auth_filter))
    app.add_handler(CommandHandler("help", help_command, filters=auth_filter))
    app.add_handler(CommandHandler("ping", ping, filters=auth_filter))
    app.add_handler(CommandHandler("clear", clear, filters=auth_filter))
    app.add_handler(CommandHandler("screenshot", quick_screenshot, filters=auth_filter))
    app.add_handler(CommandHandler("model", model_command, filters=auth_filter))
    app.add_handler(CommandHandler("provider", provider_command, filters=auth_filter))
    app.add_handler(CommandHandler("status", status_command, filters=auth_filter))
    app.add_handler(CommandHandler("bridge", bridge_command, filters=auth_filter))
    app.add_handler(CommandHandler("kill", kill_command, filters=auth_filter))
    app.add_handler(CommandHandler("tasks", tasks_command, filters=auth_filter))
    app.add_handler(CommandHandler("cancel", cancel_command, filters=auth_filter))
    app.add_handler(CommandHandler("quota", quota_command, filters=auth_filter))
    app.add_handler(CommandHandler("sessions", sessions_command, filters=auth_filter))
    app.add_handler(CommandHandler("learn", learn_command, filters=auth_filter))
    app.add_handler(CommandHandler("score", score_command, filters=auth_filter))
    app.add_handler(CommandHandler("train", train_command, filters=auth_filter))
    # /train_xxx shortcuts so TG clickable commands work
    for _domain in ["file_ops", "code_edit", "computer_control", "browser", "obedience", "all", "stop", "reset"]:
        app.add_handler(CommandHandler(f"train_{_domain}", train_command, filters=auth_filter))
    app.add_handler(CommandHandler("q", quick_action, filters=auth_filter))
    app.add_handler(CommandHandler("quick", quick_action, filters=auth_filter))
    app.add_handler(CommandHandler("panel", panel_command, filters=auth_filter))
    app.add_handler(CommandHandler("health", health_command, filters=auth_filter))
    app.add_handler(CommandHandler("vital", vital_command, filters=auth_filter))
    app.add_handler(CommandHandler("portfolio", portfolio_command, filters=auth_filter))
    app.add_handler(CommandHandler("signal", signal_command, filters=auth_filter))
    app.add_handler(CommandHandler("signal_stats", signal_stats_command, filters=auth_filter))
    app.add_handler(CommandHandler("alpha", alpha_command, filters=auth_filter))
    app.add_handler(CommandHandler("onchain", onchain_command, filters=auth_filter))
    app.add_handler(CommandHandler("paper", paper_command, filters=auth_filter))
    # DEX trading commands
    app.add_handler(CommandHandler("positions", positions_command, filters=auth_filter))
    app.add_handler(CommandHandler("pos", positions_command, filters=auth_filter))
    app.add_handler(CommandHandler("buy", buy_command, filters=auth_filter))
    app.add_handler(CommandHandler("sell", sell_command, filters=auth_filter))
    app.add_handler(CommandHandler("settings", trade_settings_command, filters=auth_filter))
    app.add_handler(CommandHandler("pnl", pnl_command, filters=auth_filter))
    app.add_handler(CommandHandler("trade", trade_dashboard_command, filters=auth_filter))
    app.add_handler(CommandHandler("t", trade_dashboard_command, filters=auth_filter))
    app.add_handler(CommandHandler("arb", arb_command, filters=auth_filter))
    app.add_handler(CommandHandler("search", search_command, filters=auth_filter))
    app.add_handler(CommandHandler("whales", whales_command, filters=auth_filter))
    app.add_handler(CommandHandler("track", track_command, filters=auth_filter))
    app.add_handler(CommandHandler("wallets", wallets_command, filters=auth_filter))
    app.add_handler(CommandHandler("addwallet", addwallet_command, filters=auth_filter))
    app.add_handler(CommandHandler("report", report_command, filters=auth_filter))
    app.add_handler(CommandHandler("risk", risk_command, filters=auth_filter))
    app.add_handler(CommandHandler("monitor", monitor_command, filters=auth_filter))
    app.add_handler(CommandHandler("proactive", proactive_command, filters=auth_filter))
    app.add_handler(CommandHandler("market", market_command, filters=auth_filter))
    # Trading skill commands
    app.add_handler(CommandHandler("token_analyze", token_analyze_command, filters=auth_filter))
    app.add_handler(CommandHandler("okx_backtest", okx_backtest_command, filters=auth_filter))
    app.add_handler(CommandHandler("ma_ribbon_backtest", ma_ribbon_backtest_command, filters=auth_filter))
    app.add_handler(CommandHandler("ma_ribbon_screener", ma_ribbon_screener_command, filters=auth_filter))
    app.add_handler(CommandHandler("okx_top30", okx_top30_command, filters=auth_filter))
    app.add_handler(CommandHandler("session_control", session_control_command, filters=auth_filter))
    app.add_handler(CommandHandler("autonomy", autonomy_command, filters=auth_filter))
    app.add_handler(CommandHandler("consciousness", consciousness_command, filters=auth_filter))
    app.add_handler(CommandHandler("ms", multi_session_command, filters=auth_filter))
    app.add_handler(CommandHandler("evolve", evolve_command, filters=auth_filter))
    app.add_handler(CommandHandler("strategy_evolve", strategy_evolve_command, filters=auth_filter))
    app.add_handler(CommandHandler("skills", skills_command, filters=auth_filter))
    app.add_handler(CommandHandler("selfcheck", selfcheck_command, filters=auth_filter))
    app.add_handler(CommandHandler("repairs", repairs_command, filters=auth_filter))
    app.add_handler(CommandHandler("repair_status", repair_status_command, filters=auth_filter))
    app.add_handler(CommandHandler("performance", performance_command, filters=auth_filter))
    app.add_handler(CommandHandler("evostatus", evostatus_command, filters=auth_filter))
    app.add_handler(CommandHandler("code_health", code_health_command, filters=auth_filter))
    app.add_handler(CommandHandler("selfrepair", selfrepair_command, filters=auth_filter))
    app.add_handler(CommandHandler("memory", memory_command, filters=auth_filter))
    app.add_handler(CommandHandler("dashboard", dashboard_command, filters=auth_filter))
    app.add_handler(CommandHandler("codex", codex_command, filters=auth_filter))
    app.add_handler(CommandHandler("optimize", optimize_command, filters=auth_filter))
    # Live trading commands
    app.add_handler(CommandHandler("wallet_setup", wallet_setup_command, filters=auth_filter))
    app.add_handler(CommandHandler("wallet_delete", wallet_delete_command, filters=auth_filter))
    app.add_handler(CommandHandler("live", live_command, filters=auth_filter))

    # Callbacks — panel first, then DEX trading, then session control, then quick actions, then safety confirmations
    app.add_handler(CallbackQueryHandler(handle_panel_callback, pattern="^(panel_|pcmd_|qa_panel)"))
    app.add_handler(CallbackQueryHandler(handle_trade_dashboard_callback, pattern="^td_"))
    app.add_handler(CallbackQueryHandler(handle_dex_callback, pattern="^dex_"))
    app.add_handler(CallbackQueryHandler(handle_session_control_callback, pattern="^sc_"))
    app.add_handler(CallbackQueryHandler(handle_quick_action_callback, pattern="^qa_"))
    app.add_handler(CallbackQueryHandler(handle_confirmation_callback))

    # Paste-to-buy: detect Solana CA pasted as plain text (group=1 so it doesn't block main handler)
    app.add_handler(MessageHandler(
        auth_filter & filters.TEXT & ~filters.COMMAND & filters.Regex(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$'),
        handle_token_address,
    ), group=1)

    # Messages
    app.add_handler(MessageHandler(auth_filter & filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(auth_filter & filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(auth_filter & (filters.VOICE | filters.AUDIO), handle_voice))
    app.add_handler(MessageHandler(auth_filter & filters.Document.ALL, handle_document))

    # Stickers — acknowledge but don't process
    async def _handle_sticker(u, c):
        if u.message:
            await u.message.reply_text("😄👍")
    app.add_handler(MessageHandler(auth_filter & filters.Sticker.ALL, _handle_sticker))

    # Video/animation — save and notify Claude
    app.add_handler(MessageHandler(
        auth_filter & (filters.VIDEO | filters.ANIMATION | filters.VIDEO_NOTE),
        handle_video
    ))

    # Location — forward to Claude as text description
    async def _handle_location(u, c):
        if not u.message or not u.message.location:
            return
        loc = u.message.location
        msg = f"用户分享了位置: 纬度 {loc.latitude}, 经度 {loc.longitude}"
        try:
            await u.message.reply_text(f"📍 位置已收到: ({loc.latitude}, {loc.longitude})")
            await claude_agent.process_message(msg, u.effective_chat.id, c)
        except Exception as e:
            logger.error(f"Location handling error: {e}", exc_info=True)
            try:
                await u.message.reply_text(f"❌ 位置处理失败: {str(e)[:300]}")
            except Exception:
                pass
    app.add_handler(MessageHandler(auth_filter & filters.LOCATION, _handle_location))

    # Contact — forward to Claude as text
    async def _handle_contact(u, c):
        if not u.message or not u.message.contact:
            return
        ct = u.message.contact
        msg = f"用户分享了联系人: {ct.first_name or ''} {ct.last_name or ''}, 电话: {ct.phone_number or 'N/A'}"
        try:
            await u.message.reply_text(f"👤 联系人已收到: {ct.first_name or ''} {ct.phone_number or ''}")
            await claude_agent.process_message(msg, u.effective_chat.id, c)
        except Exception as e:
            logger.error(f"Contact handling error: {e}", exc_info=True)
            try:
                await u.message.reply_text(f"❌ 联系人处理失败: {str(e)[:300]}")
            except Exception:
                pass
    app.add_handler(MessageHandler(auth_filter & filters.CONTACT, _handle_contact))

    # Poll — acknowledge
    async def _handle_poll(u, c):
        if u.message:
            await u.message.reply_text("📊 收到投票/问卷，暂不支持处理。")
    app.add_handler(MessageHandler(auth_filter & filters.POLL, _handle_poll))

    # Unauthorized
    app.add_handler(MessageHandler(~auth_filter, handle_unauthorized))

    # Register commands in Telegram menu + start background loops
    async def post_init(application):
        # Register commands FIRST (before any background tasks that might fail)
        try:
            await application.bot.set_my_commands([
                ("trade", "💹 Trading Dashboard"),
                ("positions", "📊 View Positions"),
                ("buy", "💰 Buy Token"),
                ("sell", "💸 Sell Token"),
                ("pnl", "📈 PnL & Stats"),
                ("paper", "📝 Paper Trading"),
                ("settings", "⚙️ Trading Settings"),
                ("alpha", "🔍 Alpha / Onchain"),
                ("status", "🤖 Bot Status"),
                ("help", "❓ All Commands"),
            ])
            logger.info("Bot commands registered with Telegram")
        except Exception as e:
            logger.error(f"Failed to set bot commands: {e}")

        # Boot Vital Signs lifecycle tracking
        try:
            import vital_signs
            vital_signs.boot()
            logger.info(f"VitalSigns booted: lifecycle={vital_signs.get_vital_signs().get('lifecycle', 'unknown')}")
        except Exception as e:
            logger.warning(f"VitalSigns failed to boot: {e}")

        # Start auto-research background loop (non-essential, don't crash bot on failure)
        try:
            import auto_research

            async def _send_to_user(text):
                if config.AUTHORIZED_USER_ID is None:
                    return
                try:
                    await application.bot.send_message(
                        chat_id=config.AUTHORIZED_USER_ID, text=text[:4096],
                    )
                except Exception:
                    pass

            _research_task = asyncio.create_task(auto_research.run_experiment_loop(send_status=_send_to_user))
            _track_task(application.bot_data, _research_task)
            logger.info("Auto-research background loop started")
        except Exception as e:
            logger.warning(f"Auto-research background loop failed to start: {e}")

        # Start Self-Monitor background loop
        try:
            if config.SELF_MONITOR_ENABLED:
                self_monitor._interval = config.SELF_MONITOR_INTERVAL

                # Register alert handler that sends anomaly alerts to the user via Telegram
                async def _monitor_alert_handler(anomalies):
                    if not anomalies or config.AUTHORIZED_USER_ID is None:
                        return
                    lines = ["Self-Monitor Alert\n"]
                    for a in anomalies[:20]:
                        lines.append(f"  [{a.get('severity', '?')}] {a.get('type', '?')}: {a.get('message', '')[:200]}")
                    try:
                        await application.bot.send_message(
                            chat_id=config.AUTHORIZED_USER_ID,
                            text="\n".join(lines)[:4096],
                        )
                    except Exception:
                        pass

                self_monitor.register_alert_handler(_monitor_alert_handler)
                await self_monitor.start()
                logger.info("SelfMonitor background loop started")
        except Exception as e:
            logger.warning(f"SelfMonitor failed to start: {e}")

        # Start Proactive Agent background loops
        try:
            if config.PROACTIVE_AGENT_ENABLED:
                # Wire the send_func to deliver messages via Telegram
                async def _proactive_send(text):
                    if config.AUTHORIZED_USER_ID is None:
                        return
                    try:
                        await application.bot.send_message(
                            chat_id=config.AUTHORIZED_USER_ID,
                            text=text[:4000],
                        )
                    except Exception:
                        pass

                proactive_agent._send = _proactive_send
                await proactive_agent.start()
                logger.info("ProactiveAgent background loops started")
        except Exception as e:
            logger.warning(f"ProactiveAgent failed to start: {e}")

        # Start Market Monitor background loop
        try:
            if config.MARKET_MONITOR_ENABLED:
                async def _market_send(text):
                    if config.AUTHORIZED_USER_ID is None:
                        return
                    try:
                        await application.bot.send_message(
                            chat_id=config.AUTHORIZED_USER_ID,
                            text=text[:4000],
                        )
                    except Exception:
                        pass

                market_monitor._send = _market_send
                await market_monitor.start()
                logger.info("MarketMonitor background loop started")
        except Exception as e:
            logger.warning(f"MarketMonitor failed to start: {e}")

        # Start ProactiveSelfRepair background scanner
        try:
            if _self_repair_available:
                async def _repair_notify(text):
                    if config.AUTHORIZED_USER_ID is None:
                        return
                    try:
                        await application.bot.send_message(
                            chat_id=config.AUTHORIZED_USER_ID,
                            text=text[:4096],
                            parse_mode="Markdown",
                        )
                    except Exception:
                        try:
                            await application.bot.send_message(
                                chat_id=config.AUTHORIZED_USER_ID,
                                text=text[:4096],
                            )
                        except Exception:
                            pass
                proactive_repair.set_notify_fn(_repair_notify)
                await proactive_repair.start()
                logger.info("ProactiveSelfRepair background scanner started")
                # Start code evolution engine alongside repair scanner
                if code_evolution_engine is not None:
                    code_evolution_engine.set_notify_fn(_repair_notify)
                    await code_evolution_engine.start()
                    logger.info("CodeEvolutionEngine started")
                if code_quality_scheduler is not None:
                    code_quality_scheduler.set_notify_fn(_repair_notify)
                    await code_quality_scheduler.start()
                    logger.info("CodeQualityScheduler started (daily UTC 02:00)")
        except Exception as e:
            logger.warning(f"ProactiveSelfRepair failed to start: {e}")

        # Start Arbitrage Engine background WebSocket streams + REST scanner
        try:
            if _arb_available and _arb_engine is not None:
                async def _arb_send(text):
                    if config.AUTHORIZED_USER_ID is None:
                        return
                    try:
                        await application.bot.send_message(
                            chat_id=config.AUTHORIZED_USER_ID,
                            text=text[:4096],
                            parse_mode="Markdown",
                        )
                    except Exception:
                        try:
                            await application.bot.send_message(
                                chat_id=config.AUTHORIZED_USER_ID,
                                text=text[:4096],
                            )
                        except Exception:
                            pass
                _arb_engine._send = _arb_send
                await _arb_engine.start()
                logger.info("ArbEngine started (OKX/Bybit/Binance WS + REST scanner)")
        except Exception as e:
            logger.warning(f"ArbEngine failed to start: {e}")

        # Start OnchainTracker (whale monitor)
        try:
            if _whale_available and _whale_tracker is not None:
                async def _whale_send(text):
                    if config.AUTHORIZED_USER_ID is None:
                        return
                    try:
                        await application.bot.send_message(
                            chat_id=config.AUTHORIZED_USER_ID,
                            text=text[:4000],
                        )
                    except Exception:
                        pass
                _whale_tracker._send = _whale_send
                await _whale_tracker.start()
                logger.info("OnchainTracker started")
        except Exception as e:
            logger.warning(f"OnchainTracker failed to start: {e}")

        # Start SmartMoneyTracker (smart money buy signals every 2 min)
        try:
            if _smart_tracker_available and _smart_tracker is not None:
                async def _smart_send(text):
                    if config.AUTHORIZED_USER_ID is None:
                        return
                    try:
                        await application.bot.send_message(
                            chat_id=config.AUTHORIZED_USER_ID,
                            text=text[:4000],
                        )
                    except Exception:
                        pass
                _smart_tracker._send = _smart_send
                await _smart_tracker.start()
                logger.info("SmartMoneyTracker started")
        except Exception as e:
            logger.warning(f"SmartMoneyTracker failed to start: {e}")

        # Start Profit Tracker background loop
        try:
            async def _profit_send(text):
                if config.AUTHORIZED_USER_ID is None:
                    return
                try:
                    await application.bot.send_message(
                        chat_id=config.AUTHORIZED_USER_ID,
                        text=text[:4000],
                    )
                except Exception:
                    pass

            async def _profit_send_photo(path):
                if config.AUTHORIZED_USER_ID is None:
                    return
                try:
                    with open(path, "rb") as f:
                        await application.bot.send_photo(
                            chat_id=config.AUTHORIZED_USER_ID,
                            photo=f,
                            caption="📈 胜率趋势 & 累计收益 (每日报告)",
                        )
                except Exception:
                    pass

            _profit_tracker.profit_tracker._send = _profit_send
            _profit_tracker.profit_tracker._send_photo = _profit_send_photo
            await _profit_tracker.profit_tracker.start()
            logger.info("ProfitTracker background loop started")
        except Exception as e:
            logger.warning(f"ProfitTracker failed to start: {e}")

        # ─── Strategy Optimizer: weekly auto-optimization ─────────────────────
        try:
            if _optimizer_available:
                async def _optimizer_notify(text):
                    if config.AUTHORIZED_USER_ID is None:
                        return
                    try:
                        await application.bot.send_message(
                            chat_id=config.AUTHORIZED_USER_ID,
                            text=text[:4096],
                            parse_mode="Markdown",
                        )
                    except Exception:
                        try:
                            await application.bot.send_message(
                                chat_id=config.AUTHORIZED_USER_ID,
                                text=text[:4096],
                            )
                        except Exception:
                            pass

                _strategy_optimizer.strategy_optimizer._notify = _optimizer_notify
                await _strategy_optimizer.strategy_optimizer.start()
                logger.info("StrategyOptimizer background loop started")

                # P3_20: PerformanceOptimizer (Bayesian opt + A/B test + daily push)
                try:
                    _strategy_optimizer.performance_optimizer._notify = _optimizer_notify
                    await _strategy_optimizer.performance_optimizer.start()
                    logger.info("PerformanceOptimizer (P3_20) started")
                except Exception as e_p3:
                    logger.warning(f"PerformanceOptimizer (P3_20) failed to start: {e_p3}")

                # P3_24: GeneticOptimizer (GA parameter optimization)
                try:
                    _strategy_optimizer.genetic_optimizer._notify = _optimizer_notify
                    await _strategy_optimizer.genetic_optimizer.start()
                    logger.info("GeneticOptimizer (P3_24) started")
                except Exception as e_p3_24:
                    logger.warning(f"GeneticOptimizer (P3_24) failed to start: {e_p3_24}")
        except Exception as e:
            logger.warning(f"StrategyOptimizer failed to start: {e}")

        # ─── Alpha Engine: 30-min social alpha signal scanner ────────────────
        try:
            if _alpha_available and _alpha_engine is not None:
                async def _alpha_send(text):
                    if config.AUTHORIZED_USER_ID is None:
                        return
                    try:
                        await application.bot.send_message(
                            chat_id=config.AUTHORIZED_USER_ID,
                            text=text[:4096],
                            parse_mode="Markdown",
                        )
                    except Exception:
                        try:
                            await application.bot.send_message(
                                chat_id=config.AUTHORIZED_USER_ID,
                                text=text[:4096],
                            )
                        except Exception:
                            pass

                _alpha_engine._send = _alpha_send
                await _alpha_engine.start()
                logger.info("AlphaEngine started (CoinGecko/DEXScreener/PumpFun 30-min)")
        except Exception as e:
            logger.warning(f"AlphaEngine failed to start: {e}")

        # ─── PaperTrader: background paper trading monitor ──────────────────
        try:
            if _paper_trader_available and _paper_trader is not None:
                async def _paper_send(text):
                    if config.AUTHORIZED_USER_ID is None:
                        return
                    try:
                        await application.bot.send_message(
                            chat_id=config.AUTHORIZED_USER_ID,
                            text=text[:4096],
                        )
                    except Exception:
                        pass
                _paper_trader.paper_trader._send = _paper_send
                await _paper_trader.paper_trader.start(_paper_send)
                logger.info("PaperTrader background monitor started")
        except Exception as e:
            logger.warning(f"PaperTrader failed to start: {e}")

        # ─── Pro Strategy Engine: multi-strategy fusion scanner ─────────────
        try:
            from pro_strategy import pro_engine as _pro_engine

            async def _pro_send(text):
                if config.AUTHORIZED_USER_ID is None:
                    return
                try:
                    await application.bot.send_message(
                        chat_id=config.AUTHORIZED_USER_ID,
                        text=text[:4000],
                    )
                except Exception:
                    pass

            _pro_engine._send = _pro_send
            await _pro_engine.start()
            logger.info("ProStrategyEngine started (3-strategy fusion, 15min scan)")
        except Exception as e:
            logger.warning(f"ProStrategyEngine failed to start: {e}")

        # ─── Autonomy Engine: auto-start if there are pending goals ──────────
        try:
            if _autonomy_available:
                engine = get_autonomy_engine()
                active = engine.get_active_goals()
                if active:
                    async def _auto_send(msg):
                        try:
                            if config.AUTHORIZED_USER_ID:
                                await application.bot.send_message(
                                    chat_id=config.AUTHORIZED_USER_ID,
                                    text=msg[:4000],
                                )
                        except Exception:
                            pass
                    engine.start(send_fn=_auto_send, interval=30.0)
                    logger.info(f"Autonomy engine auto-started with {len(active)} pending goals")
                else:
                    logger.info("Autonomy engine: no pending goals, idle")
        except Exception as e:
            logger.warning(f"Autonomy engine failed to start: {e}")

        # ─── Heartbeat: self-test every HEARTBEAT_INTERVAL seconds ────────────
        try:
            if getattr(config, "HEARTBEAT_ENABLED", True):
                async def _heartbeat_loop():
                    """Periodic self-test. If the bot can't respond to itself, auto-restart."""
                    # os already imported at module level
                    interval = getattr(config, "HEARTBEAT_INTERVAL", 1800)
                    timeout = getattr(config, "HEARTBEAT_TIMEOUT", 60)
                    _consecutive_failures = 0
                    _MAX_FAILURES = 3  # restart after 3 consecutive heartbeat failures

                    while True:
                        await asyncio.sleep(interval)
                        try:
                            # Test 1: Can we call the Telegram API?
                            me = await asyncio.wait_for(
                                application.bot.get_me(), timeout=timeout
                            )
                            if me and me.id:
                                _consecutive_failures = 0
                                logger.debug(f"Heartbeat OK: bot @{me.username} alive")
                                continue

                            _consecutive_failures += 1
                            logger.warning(f"Heartbeat: get_me returned empty ({_consecutive_failures}/{_MAX_FAILURES})")

                        except asyncio.TimeoutError:
                            _consecutive_failures += 1
                            logger.warning(f"Heartbeat: timeout ({_consecutive_failures}/{_MAX_FAILURES})")
                        except Exception as e:
                            _consecutive_failures += 1
                            logger.warning(f"Heartbeat: error {e} ({_consecutive_failures}/{_MAX_FAILURES})")

                        if _consecutive_failures >= _MAX_FAILURES:
                            logger.error(f"Heartbeat: {_MAX_FAILURES} consecutive failures, triggering auto-restart")
                            try:
                                if config.AUTHORIZED_USER_ID is not None:
                                    await application.bot.send_message(
                                        chat_id=config.AUTHORIZED_USER_ID,
                                        text=f"Heartbeat failed {_MAX_FAILURES}x. Auto-restarting...",
                                    )
                            except Exception:
                                pass
                            # Exit with non-zero code so run.py restarts us
                            os._exit(1)

                _hb_task = asyncio.create_task(_heartbeat_loop())
                _track_task(application.bot_data, _hb_task)
                logger.info(f"Heartbeat started (interval={getattr(config, 'HEARTBEAT_INTERVAL', 1800)}s)")
        except Exception as e:
            logger.warning(f"Heartbeat failed to start: {e}")

        # ─── MetaLearner: daily pattern analysis + weekly evolution report ───
        try:
            import meta_learner as _meta_learner

            async def _meta_send(text):
                if config.AUTHORIZED_USER_ID is None:
                    return
                try:
                    await application.bot.send_message(
                        chat_id=config.AUTHORIZED_USER_ID,
                        text=text[:4096],
                        parse_mode="Markdown",
                    )
                except Exception:
                    try:
                        await application.bot.send_message(
                            chat_id=config.AUTHORIZED_USER_ID,
                            text=text[:4096],
                        )
                    except Exception:
                        pass

            _meta_learner.meta_learner._send = _meta_send
            await _meta_learner.meta_learner.start()
            logger.info("MetaLearner background loop started (daily analysis + weekly report)")
        except Exception as e:
            logger.warning(f"MetaLearner failed to start: {e}")

        # ─── Evolve Watcher: background subprocess that keeps evolution queue running ──
        try:
            import subprocess, sys
            _evolver_script = os.path.join(os.path.dirname(__file__), "evolve_watcher.py")
            if os.path.exists(_evolver_script):
                _evolver_proc = subprocess.Popen(
                    [sys.executable, _evolver_script],
                    cwd=os.path.dirname(__file__),
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                application.bot_data["_evolver_pid"] = _evolver_proc.pid
                logger.info(f"EvolveWatcher started as subprocess (PID {_evolver_proc.pid})")
            else:
                logger.info("EvolveWatcher: evolve_watcher.py not found, skipped")
        except Exception as e:
            logger.warning(f"EvolveWatcher failed to start: {e}")

    app.post_init = post_init

    async def post_shutdown(application):
        """Clean up resources on bot shutdown."""
        # Close Playwright browsers to prevent orphan processes
        try:
            from web_ai import close_web_ai
            await close_web_ai()
        except Exception:
            pass
        try:
            from browser_agent import browser_close_all
            await browser_close_all()
        except Exception:
            pass
        # Stop proactive agent loops
        try:
            if proactive_agent:
                await proactive_agent.stop()
        except Exception:
            pass
        # Stop ProactiveSelfRepair scanner + CodeEvolutionEngine
        try:
            if _self_repair_available and proactive_repair:
                await proactive_repair.stop()
            if _self_repair_available and code_evolution_engine:
                await code_evolution_engine.stop()
            if _self_repair_available and code_quality_scheduler:
                await code_quality_scheduler.stop()
        except Exception:
            pass
        # Stop arbitrage engine
        try:
            if _arb_available and _arb_engine is not None and _arb_engine.running:
                await _arb_engine.stop()
        except Exception:
            pass
        # Stop OnchainTracker
        try:
            if _whale_available and _whale_tracker is not None and _whale_tracker.running:
                await _whale_tracker.stop()
        except Exception:
            pass
        # Stop SmartMoneyTracker
        try:
            if _smart_tracker_available and _smart_tracker is not None and _smart_tracker.running:
                await _smart_tracker.stop()
        except Exception:
            pass
        # Stop self_monitor
        try:
            if self_monitor._running:
                await self_monitor.stop()
        except Exception:
            pass
        # Stop market_monitor
        try:
            if market_monitor._running:
                await market_monitor.stop()
        except Exception:
            pass
        # Stop MetaLearner
        try:
            import meta_learner as _meta_learner
            if _meta_learner.meta_learner._running:
                await _meta_learner.meta_learner.stop()
        except Exception:
            pass
        # Stop ProfitTracker
        try:
            if _profit_tracker.profit_tracker.running:
                await _profit_tracker.profit_tracker.stop()
        except Exception:
            pass
        # Stop PaperTrader
        try:
            if _paper_trader_available and _paper_trader is not None and _paper_trader.paper_trader.running:
                await _paper_trader.paper_trader.stop()
        except Exception:
            pass
        # Stop EvolveWatcher subprocess
        try:
            _evolver_pid = application.bot_data.get("_evolver_pid")
            if _evolver_pid:
                import signal
                os.kill(_evolver_pid, signal.SIGTERM)
                logger.info(f"EvolveWatcher subprocess (PID {_evolver_pid}) terminated")
        except (ProcessLookupError, OSError):
            pass
        except Exception:
            pass
        logger.info("Post-shutdown cleanup complete")

    app.post_shutdown = post_shutdown

    print(f"Bot started! Mode: {'CLI (Plan tokens)' if config.BRIDGE_MODE else 'API'}")
    print("Press Ctrl+C to stop.")
    try:
        app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
    finally:
        _release_pid_lock()


if __name__ == "__main__":
    main()
