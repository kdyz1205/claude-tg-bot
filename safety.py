import re
import asyncio
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import config

# Pending confirmations: chat_id -> asyncio.Future
pending_confirmations: dict[int, asyncio.Future] = {}


def is_dangerous(command: str) -> bool:
    """Check if a command matches any dangerous pattern."""
    for pattern in config.DANGEROUS_PATTERNS:
        if re.search(pattern, command, re.IGNORECASE):
            return True
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

    future = asyncio.get_running_loop().create_future()
    pending_confirmations[chat_id] = future

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Allow", callback_data=f"allow_{chat_id}"),
            InlineKeyboardButton("❌ Deny", callback_data=f"deny_{chat_id}"),
        ]
    ])

    await context.bot.send_message(
        chat_id=chat_id,
        text=f"⚠️ **Permission Required**\n\n`{action_description}`\n\nThis action looks potentially dangerous. Allow?",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )

    try:
        result = await asyncio.wait_for(future, timeout=60)
        return result
    except asyncio.TimeoutError:
        await context.bot.send_message(
            chat_id=chat_id,
            text="⏰ Permission request timed out. Action denied.",
        )
        return False
    finally:
        pending_confirmations.pop(chat_id, None)


async def handle_confirmation_callback(update, context):
    """Handle inline keyboard button presses for permission requests."""
    query = update.callback_query
    await query.answer()

    data = query.data
    if data.startswith("allow_"):
        chat_id = int(data.split("_", 1)[1])
        if chat_id in pending_confirmations:
            future = pending_confirmations[chat_id]
            if not future.done():
                future.set_result(True)
        await query.edit_message_text("✅ Action allowed.")
    elif data.startswith("deny_"):
        chat_id = int(data.split("_", 1)[1])
        if chat_id in pending_confirmations:
            future = pending_confirmations[chat_id]
            if not future.done():
                future.set_result(False)
        await query.edit_message_text("❌ Action denied.")
