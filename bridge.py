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
import os
import sys
import time
from pathlib import Path
from datetime import datetime

BRIDGE_DIR = Path(__file__).parent / "bridge_data"
INBOX = BRIDGE_DIR / "inbox.json"
OUTBOX = BRIDGE_DIR / "outbox.json"

BRIDGE_DIR.mkdir(exist_ok=True)


def _read_json(path: Path):
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []


def _write_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ─── Called by bot.py ──────────────────────────────────────────────────────────

def write_to_inbox(chat_id: int, user_message: str) -> int:
    """Bot calls this to send a message to Claude Code."""
    messages = _read_json(INBOX)
    # Use max existing ID + 1 to avoid collisions after clear()
    msg_id = max((m.get("id", -1) for m in messages), default=-1) + 1
    messages.append({
        "id": msg_id,
        "chat_id": chat_id,
        "message": user_message,
        "timestamp": datetime.now().isoformat(),
        "read": False,
    })
    _write_json(INBOX, messages)
    return msg_id


def read_response(message_id: int, timeout: int = 120) -> str | None:
    """Bot waits for Claude Code's response."""
    start = time.time()
    while time.time() - start < timeout:
        responses = _read_json(OUTBOX)
        for r in responses:
            if r.get("reply_to_id") == message_id and not r.get("sent"):
                r["sent"] = True
                _write_json(OUTBOX, responses)
                return r["response"]
        time.sleep(1)
    return None


# ─── Called by Claude Code ─────────────────────────────────────────────────────

def check_inbox() -> list[dict]:
    """Read all unread messages from Telegram."""
    messages = _read_json(INBOX)
    unread = [m for m in messages if not m.get("read")]
    for m in messages:
        m["read"] = True
    _write_json(INBOX, messages)
    return unread


def send_response(message_id: int, chat_id: int, response: str):
    """Write a response for the bot to send back to Telegram."""
    responses = _read_json(OUTBOX)
    responses.append({
        "reply_to_id": message_id,
        "chat_id": chat_id,
        "response": response,
        "timestamp": datetime.now().isoformat(),
        "sent": False,
    })
    _write_json(OUTBOX, responses)


def clear_bridge():
    """Clear all bridge data."""
    _write_json(INBOX, [])
    _write_json(OUTBOX, [])


# ─── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "watch":
        print("🔗 Bridge Watcher - watching for Telegram messages...")
        seen = set()
        while True:
            pending = [m for m in _read_json(INBOX) if not m.get("read")]
            for msg in pending:
                if msg["id"] not in seen:
                    seen.add(msg["id"])
                    print(f"\n📩 [{msg['id']}] {msg['timestamp']}")
                    print(f"   {msg['message']}")
            time.sleep(2)
    elif len(sys.argv) > 1 and sys.argv[1] == "clear":
        clear_bridge()
        print("Bridge cleared.")
    else:
        print("python bridge.py watch  — Watch for messages")
        print("python bridge.py clear  — Clear bridge data")
