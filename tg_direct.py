"""
Direct Telegram interface for Claude Code.
Polls for new messages and provides simple functions to respond.

Usage from Claude Code:
  1. Run: python tg_direct.py poll    -- see new messages
  2. Run: python tg_direct.py send "your message"  -- send text
  3. Run: python tg_direct.py photo path/to/image.jpg  -- send photo
"""
import json
import sys
import os
import tempfile
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent / ".env")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
try:
    CHAT_ID = int(os.getenv("AUTHORIZED_USER_ID", "0"))
except ValueError:
    CHAT_ID = 0
OFFSET_FILE = Path(__file__).parent / "bridge_data" / "last_offset.txt"
Path(__file__).parent.joinpath("bridge_data").mkdir(exist_ok=True)

API = f"https://api.telegram.org/bot{BOT_TOKEN}"


def get_last_offset():
    if OFFSET_FILE.exists():
        try:
            return int(OFFSET_FILE.read_text(encoding="utf-8").strip())
        except Exception:
            pass
    return 0


def save_offset(offset):
    """Save offset atomically to prevent corruption on crash."""
    tmp_name = None
    try:
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".tmp", dir=str(OFFSET_FILE.parent),
            delete=False, encoding="utf-8",
        )
        tmp_name = tmp.name
        tmp.write(str(offset))
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp.close()
        os.replace(tmp_name, str(OFFSET_FILE))
    except Exception as e:
        print(f"Warning: failed to save offset: {e}")
        if tmp_name is not None:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass


def poll():
    """Get new messages since last check."""
    if not BOT_TOKEN:
        print("Error: TELEGRAM_BOT_TOKEN not set")
        return []
    offset = get_last_offset()
    try:
        resp = requests.get(f"{API}/getUpdates", params={
            "offset": offset,
            "timeout": 1,
            "allowed_updates": json.dumps(["message"]),
        }, timeout=10)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 30))
            print(f"Rate limited by Telegram, waiting {retry_after}s...")
            import time as _time
            _time.sleep(min(retry_after, 120))
            return []
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.RequestException as e:
        print(f"Network error: {e}")
        return []
    except (json.JSONDecodeError, ValueError) as e:
        print(f"Invalid JSON response: {e}")
        return []

    if not data.get("ok"):
        print(f"Error: {data}")
        return []

    messages = []
    max_uid = offset
    for update in data.get("result", []):
        uid = update.get("update_id")
        if uid is None:
            continue
        max_uid = max(max_uid, uid + 1)
        msg = update.get("message", {})
        if not msg:
            continue
        text = msg.get("text", "")
        user_id = msg.get("from", {}).get("id", 0)
        chat_id = msg.get("chat", {}).get("id", 0)

        if user_id == CHAT_ID and text:
            messages.append({
                "update_id": uid,
                "text": text,
                "chat_id": chat_id,
                "date": msg.get("date", 0),
            })

    # Save offset after processing all updates (not per-update to avoid partial saves)
    if max_uid > offset:
        save_offset(max_uid)

    return messages


def send(text):
    """Send a text message to the user. Splits long messages to respect Telegram's 4096 char limit."""
    MAX_LEN = 4096
    if not text:
        return False
    if not BOT_TOKEN:
        print("Error: TELEGRAM_BOT_TOKEN not set")
        return False
    # Split long messages
    chunks = []
    remaining = text
    while remaining:
        if len(remaining) <= MAX_LEN:
            chunks.append(remaining)
            break
        # Try to split at a newline
        split_pos = remaining.rfind("\n", 0, MAX_LEN)
        if split_pos == -1 or split_pos < MAX_LEN // 2:
            split_pos = MAX_LEN
        chunks.append(remaining[:split_pos])
        remaining = remaining[split_pos:].lstrip("\n")

    success = True
    for chunk in chunks:
        try:
            resp = requests.post(f"{API}/sendMessage", json={
                "chat_id": CHAT_ID,
                "text": chunk,
            }, timeout=30)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 30))
                print(f"Rate limited by Telegram, waiting {retry_after}s...")
                import time as _time
                _time.sleep(min(retry_after, 120))
                # Retry this chunk once
                resp = requests.post(f"{API}/sendMessage", json={
                    "chat_id": CHAT_ID,
                    "text": chunk,
                }, timeout=30)
            if resp.status_code != 200:
                success = False
        except requests.exceptions.RequestException:
            success = False
    return success


def send_photo(photo_path, caption=None):
    """Send a photo to the user."""
    if not BOT_TOKEN:
        print("Error: TELEGRAM_BOT_TOKEN not set")
        return False
    try:
        with open(photo_path, "rb") as f:
            data = {"chat_id": CHAT_ID}
            if caption:
                data["caption"] = caption[:1024]  # Telegram caption limit
            resp = requests.post(f"{API}/sendPhoto", data=data, files={"photo": f}, timeout=30)
        return resp.status_code == 200
    except (requests.exceptions.RequestException, OSError) as e:
        print(f"send_photo error: {e}")
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python tg_direct.py poll          -- Check for new messages")
        print("  python tg_direct.py send 'text'   -- Send a message")
        print("  python tg_direct.py photo file.jpg -- Send a photo")
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "poll":
        msgs = poll()
        if msgs:
            for m in msgs:
                print(f"[{m['update_id']}] {m['text']}")
        else:
            print("(no new messages)")

    elif cmd == "send":
        text = " ".join(sys.argv[2:])
        if not text:
            print("Usage: python tg_direct.py send 'your message'")
            sys.exit(1)
        if send(text):
            print("Sent OK")
        else:
            print("Send FAILED")

    elif cmd == "photo":
        if len(sys.argv) < 3:
            print("Usage: python tg_direct.py photo <file> [caption]")
            sys.exit(1)
        path = sys.argv[2]
        caption = " ".join(sys.argv[3:]) if len(sys.argv) > 3 else None
        if send_photo(path, caption):
            print("Photo sent OK")
        else:
            print("Photo send FAILED")
