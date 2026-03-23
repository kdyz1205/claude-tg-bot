"""
bot.py — Telegram bot entry point.

Handles: text, voice, photos, documents, commands, quick actions.
Routes all messages through claude_agent.py → Claude Code CLI.
"""
import logging
import asyncio
import os
import traceback
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
from providers import PROVIDER_DISPLAY

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Auth filter
auth_filter = filters.User(user_id=config.AUTHORIZED_USER_ID)

AVAILABLE_MODELS = {
    "sonnet": "claude-sonnet-4-6",
    "opus": "claude-opus-4-6",
    "haiku": "claude-haiku-4-5-20251001",
}


# ─── Error Handler ────────────────────────────────────────────────────────────

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Global error handler — logs errors and notifies user if possible."""
    logger.error(f"Exception while handling update: {context.error}", exc_info=context.error)

    # Try to notify the user
    if isinstance(update, Update) and update.effective_chat:
        try:
            err_msg = str(context.error)[:200] if context.error else "Unknown error"
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"⚠️ 内部错误: {err_msg}\n\n发消息继续使用。",
            )
        except Exception:
            pass


# ─── Commands ─────────────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 Remote Controller v3.0\n\n"
        "我是你的远程电脑控制器。\n"
        "直接告诉我要做什么，我来操作。\n\n"
        "Commands:\n"
        "/clear - 清除对话(开始新会话)\n"
        "/screenshot - 截图\n"
        "/status - 状态\n"
        "/q - 快捷操作面板"
    )


async def clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    claude_agent.clear_history(update.effective_chat.id)
    await update.message.reply_text("✅ 对话已清空，新会话已开始。")


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    session_id = claude_agent._claude_sessions.get(chat_id, "无")
    if session_id != "无":
        session_id = session_id[:12] + "..."
    queue_size = len(claude_agent._pending_messages.get(chat_id, []))
    is_busy = claude_agent._get_lock(chat_id).locked()

    await update.message.reply_text(
        f"📊 状态\n\n"
        f"模式: Claude Code CLI (Plan tokens)\n"
        f"Session: {session_id}\n"
        f"队列: {queue_size} 条待处理\n"
        f"处理中: {'是' if is_busy else '否'}\n"
        f"Bridge: {'✅' if config.BRIDGE_MODE else '❌'}",
    )


async def model_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
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
    await update.message.reply_text(f"✅ 已切换为 `{config.CLAUDE_MODEL}`", parse_mode="Markdown")


async def provider_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    valid = list(PROVIDER_DISPLAY.keys())
    if not context.args:
        lines = ["AI Provider 状态:\n"]
        for p, name in PROVIDER_DISPLAY.items():
            has_key = bool({"claude": config.ANTHROPIC_API_KEY, "openai": config.OPENAI_API_KEY, "gemini": config.GEMINI_API_KEY}.get(p))
            active = "✅ 当前" if p == config.CURRENT_PROVIDER else ("🔑 可用" if has_key else "❌ 无key")
            lines.append(f"{active} — {name}")
        lines.append(f"\n用法: `/provider claude|openai|gemini`")
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
        return

    choice = context.args[0].lower()
    if choice not in valid:
        await update.message.reply_text(f"可选: {', '.join(valid)}")
        return

    config.CURRENT_PROVIDER = choice
    claude_agent.clear_history(update.effective_chat.id)
    await update.message.reply_text(f"✅ 已切换到 {PROVIDER_DISPLAY[choice]}")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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


async def bridge_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        status = "✅ 开启" if config.BRIDGE_MODE else "❌ 关闭"
        await update.message.reply_text(
            f"Bridge Mode: {status}\n"
            "on = 用 Claude Code CLI (Plan tokens)\n"
            "off = 用 API (消耗token)\n"
            "用法: /bridge on|off"
        )
        return
    action = context.args[0].lower()
    if action == "on":
        config.BRIDGE_MODE = True
        bridge.clear_bridge()
        await update.message.reply_text("✅ Bridge Mode 开启 (Plan tokens)")
    elif action == "off":
        config.BRIDGE_MODE = False
        await update.message.reply_text("✅ Bridge Mode 关闭 (API mode)")
    else:
        await update.message.reply_text("用法: /bridge on|off")


# ─── Screenshot & Quick Actions ───────────────────────────────────────────────

async def quick_screenshot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from screenshots import capture_screenshot

    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="upload_photo")
    try:
        buffer = capture_screenshot()
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔄 刷新", callback_data="qa_screenshot"),
                InlineKeyboardButton("⬆️ 上", callback_data="qa_scroll_up"),
                InlineKeyboardButton("⬇️ 下", callback_data="qa_scroll_down"),
            ],
        ])
        await update.message.reply_photo(photo=buffer, reply_markup=keyboard)
    except Exception as e:
        await update.message.reply_text(f"Screenshot failed: {e}")


async def quick_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    ])
    await update.message.reply_text("⚡ 快捷操作", reply_markup=keyboard)


async def handle_quick_action_callback(update, context):
    query = update.callback_query
    if query.data.startswith(("allow_", "deny_")):
        return
    if not query.data.startswith("qa_"):
        return

    await query.answer()
    action = query.data
    chat_id = query.message.chat_id

    try:
        if action == "qa_screenshot":
            from screenshots import capture_screenshot
            await context.bot.send_chat_action(chat_id=chat_id, action="upload_photo")
            buffer = capture_screenshot()
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
            await tools.execute_run_command("rundll32.exe user32.dll,LockWorkStation")
            await context.bot.send_message(chat_id=chat_id, text="🔒 已锁屏")

        elif action == "qa_sysinfo":
            import tools
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, tools.execute_get_system_info)
            await context.bot.send_message(chat_id=chat_id, text=f"📊\n{result[:3000]}")

        elif action in ("qa_scroll_up", "qa_scroll_down"):
            import pyautogui
            from screenshots import capture_screenshot
            pyautogui.scroll(5 if action == "qa_scroll_up" else -5)
            await asyncio.sleep(0.3)
            buffer = capture_screenshot()
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🔄 刷新", callback_data="qa_screenshot"),
                    InlineKeyboardButton("⬆️ 上", callback_data="qa_scroll_up"),
                    InlineKeyboardButton("⬇️ 下", callback_data="qa_scroll_down"),
                ],
            ])
            await context.bot.send_photo(chat_id=chat_id, photo=buffer, reply_markup=keyboard)

    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"❌ {e}")


# ─── Message Handlers ─────────────────────────────────────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text messages."""
    chat_id = update.effective_chat.id
    text = update.message.text
    if not text:
        return

    try:
        await context.bot.send_chat_action(chat_id=chat_id, action="typing")
    except Exception:
        pass

    try:
        await claude_agent.process_message(text, chat_id, context)
    except Exception as e:
        logger.error(f"Error processing message: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ Error: {str(e)[:500]}")
        except Exception:
            pass


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle photos — save to disk and tell Claude about it."""
    chat_id = update.effective_chat.id
    caption = update.message.caption or ""

    try:
        await context.bot.send_chat_action(chat_id=chat_id, action="typing")

        # Save photo to disk so Claude can reference it
        photo = update.message.photo[-1]  # Largest resolution
        file = await context.bot.get_file(photo.file_id)
        save_dir = os.path.join(os.path.expanduser("~"), "Desktop", "telegram_files")
        os.makedirs(save_dir, exist_ok=True)
        save_path = os.path.join(save_dir, f"photo_{photo.file_id}.jpg")
        await file.download_to_drive(save_path)

        msg = f"用户发送了一张图片，已保存到: {save_path}"
        if caption:
            msg += f"\n用户说: {caption}"
        else:
            msg += "\n(无附加说明)"

        await update.message.reply_text(f"📸 图片已保存: {save_path}")
        await claude_agent.process_message(msg, chat_id, context)

    except Exception as e:
        logger.error(f"Photo handling error: {e}", exc_info=True)
        try:
            await update.message.reply_text(f"❌ 图片处理失败: {e}")
        except Exception:
            pass


async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle voice messages — transcribe with Gemini, process as text."""
    chat_id = update.effective_chat.id
    await context.bot.send_chat_action(chat_id=chat_id, action="typing")

    try:
        voice = update.message.voice or update.message.audio
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
                resp = client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=[
                        gtypes.Content(role="user", parts=[
                            gtypes.Part(inline_data=gtypes.Blob(mime_type="audio/ogg", data=audio_data)),
                            gtypes.Part(text="Transcribe this voice message exactly. Output only the text. If Chinese, keep Chinese."),
                        ])
                    ],
                )
                transcription = resp.text.strip()
            except Exception as e:
                logger.warning(f"Gemini transcription failed: {e}")

        # Fallback: tell Claude about the voice file
        if not transcription:
            save_dir = os.path.join(os.path.expanduser("~"), "Desktop", "telegram_files")
            os.makedirs(save_dir, exist_ok=True)
            import shutil
            save_path = os.path.join(save_dir, f"voice_{voice.file_id}.ogg")
            shutil.copy2(tmp, save_path)
            await update.message.reply_text(f"🎙 语音已保存: {save_path}\n(无法转录，请发文字)")
            return

        await update.message.reply_text(f"🎙 「{transcription}」")
        await claude_agent.process_message(transcription, chat_id, context)

        # Cleanup temp file
        try:
            os.remove(tmp)
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Voice handling error: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 语音处理失败: {e}")


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle files — save to desktop and notify AI."""
    chat_id = update.effective_chat.id
    await context.bot.send_chat_action(chat_id=chat_id, action="typing")

    try:
        doc = update.message.document
        file = await context.bot.get_file(doc.file_id)
        save_dir = os.path.join(os.path.expanduser("~"), "Desktop", "telegram_files")
        os.makedirs(save_dir, exist_ok=True)
        save_path = os.path.join(save_dir, doc.file_name or f"file_{doc.file_id}")
        await file.download_to_drive(save_path)

        caption = update.message.caption or ""
        msg = f"用户发送了文件，已保存到: {save_path}"
        if caption:
            msg += f"\n用户说: {caption}"

        await update.message.reply_text(f"📁 文件已保存: {save_path}")
        await claude_agent.process_message(msg, chat_id, context)

    except Exception as e:
        logger.error(f"Document handling error: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 文件处理失败: {e}")


async def handle_unauthorized(update: Update, context: ContextTypes.DEFAULT_TYPE):
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


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    if not config.TELEGRAM_BOT_TOKEN or config.TELEGRAM_BOT_TOKEN == "your_token_here":
        print("ERROR: Set TELEGRAM_BOT_TOKEN in .env")
        return
    if config.AUTHORIZED_USER_ID == 0:
        print("NOTE: AUTHORIZED_USER_ID not set. Send any message to get your ID.")

    app = ApplicationBuilder().token(config.TELEGRAM_BOT_TOKEN).build()

    # Global error handler
    app.add_error_handler(error_handler)

    # Commands
    app.add_handler(CommandHandler("start", start, filters=auth_filter))
    app.add_handler(CommandHandler("help", help_command, filters=auth_filter))
    app.add_handler(CommandHandler("clear", clear, filters=auth_filter))
    app.add_handler(CommandHandler("screenshot", quick_screenshot, filters=auth_filter))
    app.add_handler(CommandHandler("model", model_command, filters=auth_filter))
    app.add_handler(CommandHandler("provider", provider_command, filters=auth_filter))
    app.add_handler(CommandHandler("status", status_command, filters=auth_filter))
    app.add_handler(CommandHandler("bridge", bridge_command, filters=auth_filter))
    app.add_handler(CommandHandler("q", quick_action, filters=auth_filter))
    app.add_handler(CommandHandler("quick", quick_action, filters=auth_filter))

    # Callbacks
    app.add_handler(CallbackQueryHandler(handle_quick_action_callback, pattern="^qa_"))
    app.add_handler(CallbackQueryHandler(handle_confirmation_callback))

    # Messages
    app.add_handler(MessageHandler(auth_filter & filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(auth_filter & filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(auth_filter & (filters.VOICE | filters.AUDIO), handle_voice))
    app.add_handler(MessageHandler(auth_filter & filters.Document.ALL, handle_document))

    # Unauthorized
    app.add_handler(MessageHandler(~auth_filter, handle_unauthorized))

    print(f"Bot started! Mode: {'CLI (Plan tokens)' if config.BRIDGE_MODE else 'API'}")
    print("Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
