"""
Bridge system: Routes Telegram messages to Claude Code (this conversation).

How it works:
1. User sends message to Telegram bot
2. Bot writes it to bridge_inbox.json
3. Claude Code reads and executes the task
4. Claude Code writes response to bridge_outbox.json
5. Bot picks up response and sends it back to Telegram

In the bot: /bridge on → enable bridge mode
In Claude Code: I read the inbox and respond
"""

import json
import logging
import os
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

BRIDGE_DIR = Path(__file__).parent / "bridge_data"
INBOX = BRIDGE_DIR / "inbox.json"
OUTBOX = BRIDGE_DIR / "outbox.json"

BRIDGE_DIR.mkdir(exist_ok=True)

_bridge_lock = threading.Lock()


def _read_json(path: Path):
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        logger.warning(f"Corrupted JSON in {path.name}, resetting: {e}")
        # Back up corrupt file for diagnosis, then reset (keep max 5 backups)
        try:
            backup = path.with_suffix(f".corrupt.{int(time.time())}.json")
            shutil.copy2(str(path), str(backup))
            # Clean up old corrupt backups to prevent unbounded accumulation
            import glob as _glob
            corrupt_files = sorted(_glob.glob(str(path.with_suffix(".corrupt.*.json"))))
            for old in corrupt_files[:-5]:
                try:
                    os.unlink(old)
                except OSError:
                    pass
        except Exception:
            pass
        return []
    except Exception as e:
        logger.warning(f"Failed to read {path.name}: {e}")
        return []


def _write_json(path: Path, data):
    tmp = None
    tmp_name = None
    try:
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".tmp", dir=str(path.parent), delete=False, encoding="utf-8",
        )
        tmp_name = tmp.name
        tmp.write(json.dumps(data, ensure_ascii=False, indent=2))
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp.close()
        tmp = None  # Mark as closed so cleanup doesn't double-close
        # On Windows, shutil.move may fail if target is locked — use replace
        os.replace(tmp_name, str(path))
    except Exception as e:
        logger.error(f"Failed to write {path.name}: {e}")
        if tmp is not None:
            try:
                tmp.close()
            except Exception:
                pass
        if tmp_name is not None:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
        raise


# ─── Called by bot.py ──────────────────────────────────────────────────────────

def write_to_inbox(chat_id: int, user_message: str) -> int:
    """Bot calls this to send a message to Claude Code."""
    with _bridge_lock:
        return _write_to_inbox_locked(chat_id, user_message)


_MAX_INBOX_MESSAGES = 200

def _write_to_inbox_locked(chat_id: int, user_message: str) -> int:
    messages = _read_json(INBOX)
    # Use max existing ID + 1 to avoid collisions after clear()
    msg_id = max((m.get("id", -1) for m in messages), default=-1) + 1
    messages.append({
        "id": msg_id,
        "chat_id": chat_id,
        "message": user_message[:50000],  # Cap message size
        "timestamp": datetime.now().isoformat(),
        "read": False,
    })
    # Trim old messages to prevent unbounded growth
    if len(messages) > _MAX_INBOX_MESSAGES:
        messages = messages[-_MAX_INBOX_MESSAGES:]
    _write_json(INBOX, messages)
    return msg_id


async def read_response(message_id: int, timeout: int = 120) -> str | None:
    """Bot waits for Claude Code's response.

    This is async to avoid blocking the event loop with time.sleep().
    Uses run_in_executor to avoid blocking the event loop with the threading lock.
    """
    import asyncio
    loop = asyncio.get_running_loop()
    start = time.time()
    while time.time() - start < timeout:
        result = await loop.run_in_executor(None, _check_outbox_for_response, message_id)
        if result is not None:
            return result
        await asyncio.sleep(1)
    return None


def _check_outbox_for_response(message_id: int) -> str | None:
    """Check outbox for a response (called from executor to avoid blocking event loop)."""
    with _bridge_lock:
        responses = _read_json(OUTBOX)
        for r in responses:
            if r.get("reply_to_id") == message_id and not r.get("sent"):
                r["sent"] = True
                _write_json(OUTBOX, responses)
                return r["response"]
    return None


# ─── Called by Claude Code ─────────────────────────────────────────────────────

def check_inbox() -> list[dict]:
    """Read all unread messages from Telegram."""
    with _bridge_lock:
        messages = _read_json(INBOX)
        unread = [m for m in messages if not m.get("read")]
        if not unread:
            return []
        for m in unread:
            m["read"] = True
        try:
            _write_json(INBOX, messages)
        except Exception as e:
            logger.error(f"Failed to mark messages as read: {e}")
            # Revert read marks so messages aren't lost
            for m in unread:
                m["read"] = False
            return []
        return unread


_MAX_OUTBOX_MESSAGES = 200

def send_response(message_id: int, chat_id: int, response: str):
    """Write a response for the bot to send back to Telegram."""
    with _bridge_lock:
        responses = _read_json(OUTBOX)
        responses.append({
            "reply_to_id": message_id,
            "chat_id": chat_id,
            "response": response[:100000],  # Cap response size
            "timestamp": datetime.now().isoformat(),
            "sent": False,
        })
        # Trim old sent responses to prevent unbounded growth
        if len(responses) > _MAX_OUTBOX_MESSAGES:
            # Keep unsent messages + most recent sent ones
            unsent = [r for r in responses if not r.get("sent")]
            sent = [r for r in responses if r.get("sent")]
            responses = sent[-(max(0, _MAX_OUTBOX_MESSAGES - len(unsent))):] + unsent
        _write_json(OUTBOX, responses)


def clear_bridge():
    """Clear all bridge data."""
    with _bridge_lock:
        _write_json(INBOX, [])
        _write_json(OUTBOX, [])


# ─── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "watch":
        print("🔗 Bridge Watcher - watching for Telegram messages...")
        seen = set()
        while True:
            with _bridge_lock:
                pending = [m for m in _read_json(INBOX) if not m.get("read")]
            for msg in pending:
                msg_id = msg.get("id")
                if msg_id is not None and msg_id not in seen:
                    seen.add(msg_id)
                    print(f"\n📩 [{msg_id}] {msg.get('timestamp', '?')}")
                    print(f"   {msg.get('message', '')[:500]}")
            # Prevent seen set from growing unbounded
            if len(seen) > 10000:
                seen = set(sorted(seen)[-5000:])
            time.sleep(2)
    elif len(sys.argv) > 1 and sys.argv[1] == "clear":
        clear_bridge()
        print("Bridge cleared.")
    else:
        print("python bridge.py watch  — Watch for messages")
        print("python bridge.py clear  — Clear bridge data")
