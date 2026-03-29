"""
Bridge Worker: Watches for Telegram messages and prints them.
Run this alongside Claude Code so it can see incoming messages.

Requires: requests (pip install requests)

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
try:
    CHAT_ID = int(os.getenv("AUTHORIZED_USER_ID", "0"))
except ValueError:
    CHAT_ID = 0

BRIDGE_DIR.mkdir(exist_ok=True)


def _read_json(path):
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError, ValueError):
        return []


def _write_json(path, data):
    """Atomic write: write to temp file then rename to prevent corruption."""
    import tempfile
    tmp = None
    tmp_name = None
    try:
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".tmp", encoding="utf-8",
            dir=str(path.parent), delete=False,
        )
        tmp_name = tmp.name
        tmp.write(json.dumps(data, ensure_ascii=False, indent=2))
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp.close()
        tmp = None  # Mark as closed so cleanup doesn't double-close
        os.replace(tmp_name, str(path))  # os.replace is atomic on Windows
    except Exception as e:
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
        # Fallback: direct write (log warning so failures aren't fully silent)
        print(f"Warning: atomic write failed for {path}: {e}, trying direct write")
        try:
            path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e2:
            print(f"Warning: _write_json fallback also failed for {path}: {e2}")


def send_telegram(text):
    """Send a message back to Telegram. Splits long messages."""
    if not text:
        return False
    if not BOT_TOKEN:
        print("Error: TELEGRAM_BOT_TOKEN not set")
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    # Telegram message limit is 4096 chars
    MAX_LEN = 4096
    chunks = []
    remaining = text
    while remaining:
        if len(remaining) <= MAX_LEN:
            chunks.append(remaining)
            break
        split_pos = remaining.rfind("\n", 0, MAX_LEN)
        if split_pos == -1 or split_pos < MAX_LEN // 2:
            split_pos = MAX_LEN
        chunks.append(remaining[:split_pos])
        remaining = remaining[split_pos:].lstrip("\n")

    success = True
    for chunk in chunks:
        try:
            resp = requests.post(url, json={"chat_id": CHAT_ID, "text": chunk}, timeout=30)
            if resp.status_code != 200:
                success = False
        except requests.exceptions.RequestException:
            success = False
    return success


def send_telegram_photo(photo_path, caption=None):
    """Send a photo to Telegram."""
    if not BOT_TOKEN:
        print("Error: TELEGRAM_BOT_TOKEN not set")
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    try:
        with open(photo_path, "rb") as f:
            files = {"photo": f}
            data = {"chat_id": CHAT_ID}
            if caption:
                data["caption"] = caption[:1024]  # Telegram caption limit
            resp = requests.post(url, data=data, files=files, timeout=60)
        return resp.status_code == 200
    except (requests.exceptions.RequestException, OSError) as e:
        print(f"send_telegram_photo error: {e}")
        return False


import threading
_bridge_lock = threading.Lock()

def respond(msg_id, response_text):
    """Write response to outbox for the bot to pick up, AND send directly."""
    with _bridge_lock:
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
            if m.get("id") == msg_id:
                m["read"] = True
        _write_json(INBOX, inbox)

    # Send directly via Telegram API (outside lock - network call)
    if not send_telegram(response_text):
        print(f"Warning: failed to send Telegram message for msg_id={msg_id}")


def get_pending():
    """Get unread messages."""
    with _bridge_lock:
        inbox = _read_json(INBOX)
        return [m for m in inbox if not m.get("read")]


if __name__ == "__main__":
    print("=" * 60)
    print("  BRIDGE WORKER - Telegram <-> Claude Code")
    print("=" * 60)
    print(f"Bot Token: {'[SET]' if BOT_TOKEN else '[NOT SET]'}")
    print(f"Chat ID: {CHAT_ID}")
    print(f"Inbox: {INBOX}")
    print(f"Outbox: {OUTBOX}")
    print()
    print("Watching for messages... (Ctrl+C to stop)")
    print("=" * 60)

    from collections import OrderedDict
    seen = OrderedDict()
    MAX_SEEN = 5000
    try:
        while True:
            try:
                pending = get_pending()
                for msg in pending:
                    msg_id = msg.get("id")
                    if msg_id is not None and msg_id not in seen:
                        seen[msg_id] = True
                        print(f"\n{'='*60}")
                        print(f"NEW MESSAGE [id={msg_id}]")
                        print(f"Time: {msg.get('timestamp', '?')}")
                        print(f"Text: {msg.get('message', '')}")
                        print(f"{'='*60}")
                        sys.stdout.flush()
                # Evict oldest entries when too large
                while len(seen) > MAX_SEEN:
                    seen.popitem(last=False)
            except Exception as e:
                print(f"Error in poll loop: {e}")
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nStopped.")
