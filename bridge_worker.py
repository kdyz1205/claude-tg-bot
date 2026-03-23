"""
Bridge Worker: Watches for Telegram messages and prints them.
Run this alongside Claude Code so it can see incoming messages.

Usage: python bridge_worker.py
"""
import json
import time
import sys
import os
import requests
from pathlib import Path
from datetime import datetime

BRIDGE_DIR = Path(__file__).parent / "bridge_data"
INBOX = BRIDGE_DIR / "inbox.json"
OUTBOX = BRIDGE_DIR / "outbox.json"

# Load env
from dotenv import load_dotenv
load_dotenv(dotenv_path=Path(__file__).parent / ".env")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = int(os.getenv("AUTHORIZED_USER_ID", "0"))

BRIDGE_DIR.mkdir(exist_ok=True)


def _read_json(path):
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except:
        return []


def _write_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def send_telegram(text):
    """Send a message back to Telegram."""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={"chat_id": CHAT_ID, "text": text})
        return resp.status_code == 200
    except:
        return False


def send_telegram_photo(photo_path, caption=None):
    """Send a photo to Telegram."""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    try:
        with open(photo_path, "rb") as f:
            files = {"photo": f}
            data = {"chat_id": CHAT_ID}
            if caption:
                data["caption"] = caption
            resp = requests.post(url, data=data, files=files)
        return resp.status_code == 200
    except:
        return False


def respond(msg_id, response_text):
    """Write response to outbox for the bot to pick up, AND send directly."""
    outbox = _read_json(OUTBOX)
    outbox.append({
        "reply_to_id": msg_id,
        "response": response_text,
        "timestamp": datetime.now().isoformat(),
        "sent": False,
    })
    _write_json(OUTBOX, outbox)

    # Mark as responded in inbox
    inbox = _read_json(INBOX)
    for m in inbox:
        if m["id"] == msg_id:
            m["read"] = True
    _write_json(INBOX, inbox)

    # Also send directly via Telegram API
    send_telegram(response_text)


def get_pending():
    """Get unread messages."""
    inbox = _read_json(INBOX)
    return [m for m in inbox if not m.get("read")]


if __name__ == "__main__":
    print("=" * 60)
    print("  BRIDGE WORKER - Telegram <-> Claude Code")
    print("=" * 60)
    print(f"Bot Token: ...{BOT_TOKEN[-10:] if BOT_TOKEN else 'NOT SET'}")
    print(f"Chat ID: {CHAT_ID}")
    print(f"Inbox: {INBOX}")
    print(f"Outbox: {OUTBOX}")
    print()
    print("Watching for messages... (Ctrl+C to stop)")
    print("=" * 60)

    seen = set()
    while True:
        pending = get_pending()
        for msg in pending:
            if msg["id"] not in seen:
                seen.add(msg["id"])
                print(f"\n{'='*60}")
                print(f"NEW MESSAGE [id={msg['id']}]")
                print(f"Time: {msg['timestamp']}")
                print(f"Text: {msg['message']}")
                print(f"{'='*60}")
                sys.stdout.flush()
        time.sleep(2)
