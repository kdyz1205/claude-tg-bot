"""
safety.py — Permission system for dangerous commands.

In CLI mode (Bridge), Claude Code handles its own permissions.
This module is only used for API fallback mode where tools are called directly.

Trading hard kill (daily loss / drawdown) is handled by trading.hard_risk_kill and
DrawdownGuardian — it does not pass through this permission flow.
"""
import re
import asyncio
import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import config

logger = logging.getLogger(__name__)

# Pending confirmations: chat_id -> asyncio.Future
pending_confirmations: dict[int, asyncio.Future] = {}


def is_dangerous(command: str) -> bool:
    """Check if a command matches any dangerous pattern."""
    if not command:
        return False
    for pattern in config.DANGEROUS_PATTERNS:
        try:
            if re.search(pattern, command, re.IGNORECASE):
                return True
        except re.error:
            # Invalid regex pattern in config - skip it
            logger.warning(f"Invalid dangerous pattern: {pattern}")
    return False


async def request_permission(action_description: str, chat_id: int, context) -> bool:
    """Send an inline keyboard asking user to allow or deny an action.
    Returns True if allowed, False if denied or timed out.
    """
    # Cancel any previous pending confirmation for this chat
    if chat_id in pending_confirmations:
        old_future = pending_confirmations[chat_id]
        if not old_future.done():
            old_future.set_result(False)

    if len(pending_confirmations) > 50:
        # Snapshot keys to avoid mutation during iteration
        snapshot = list(pending_confirmations.items())
        for k, f in snapshot:
            if f.done():
                pending_confirmations.pop(k, None)
        # If still too many, cancel oldest (except current chat_id)
        if len(pending_confirmations) > 50:
            for k in list(pending_confirmations.keys())[:25]:
                f = pending_confirmations.pop(k, None)
                if f and not f.done():
                    f.set_result(False)

    future = asyncio.get_running_loop().create_future()
    pending_confirmations[chat_id] = future

    # Truncate very long command descriptions
    display_cmd = action_description[:500]
    if len(action_description) > 500:
        display_cmd += "..."

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Allow", callback_data=f"allow_{chat_id}"),
            InlineKeyboardButton("❌ Deny", callback_data=f"deny_{chat_id}"),
        ]
    ])

    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ Permission Required\n\n{display_cmd}\n\nThis looks potentially dangerous. Allow?",
            reply_markup=keyboard,
        )
    except Exception as e:
        logger.error(f"Failed to send permission request: {e}")
        # If we can't ask, deny by default
        pending_confirmations.pop(chat_id, None)
        return False

    try:
        result = await asyncio.wait_for(future, timeout=60)
        return result
    except asyncio.TimeoutError:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text="⏰ Permission request timed out. Action denied.",
            )
        except Exception:
            pass
        return False
    finally:
        pending_confirmations.pop(chat_id, None)


async def handle_confirmation_callback(update, context):
    """Handle inline keyboard button presses for permission requests."""
    query = update.callback_query
    if not query or not query.data:
        return
    data = query.data

    # Only handle allow/deny callbacks — ignore everything else
    if not data.startswith(("allow_", "deny_")):
        return

    # Verify the callback is from an authorized user
    if not query.from_user or config.AUTHORIZED_USER_ID is None or query.from_user.id != config.AUTHORIZED_USER_ID:
        await query.answer("⛔ Unauthorized", show_alert=True)
        return

    await query.answer()

    if data.startswith("allow_"):
        try:
            chat_id = int(data.split("_", 1)[1])
        except (ValueError, IndexError):
            return
        # Verify the callback chat matches the confirmation context
        if str(query.message.chat_id) != str(chat_id):
            await query.answer("⛔ Invalid confirmation context", show_alert=True)
            return
        if chat_id in pending_confirmations:
            future = pending_confirmations[chat_id]
            if not future.done():
                future.set_result(True)
        try:
            await query.edit_message_text("✅ Action allowed.")
        except Exception:
            pass
    elif data.startswith("deny_"):
        try:
            chat_id = int(data.split("_", 1)[1])
        except (ValueError, IndexError):
            return
        # Verify the callback chat matches the confirmation context
        if str(query.message.chat_id) != str(chat_id):
            await query.answer("⛔ Invalid confirmation context", show_alert=True)
            return
        if chat_id in pending_confirmations:
            future = pending_confirmations[chat_id]
            if not future.done():
                future.set_result(False)
        try:
            await query.edit_message_text("❌ Action denied.")
        except Exception:
            pass
