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
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent / ".env")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = int(os.getenv("AUTHORIZED_USER_ID", "0"))
OFFSET_FILE = Path(__file__).parent / "bridge_data" / "last_offset.txt"
Path(__file__).parent.joinpath("bridge_data").mkdir(exist_ok=True)

API = f"https://api.telegram.org/bot{BOT_TOKEN}"


def get_last_offset():
    if OFFSET_FILE.exists():
        try:
            return int(OFFSET_FILE.read_text().strip())
        except Exception:
            pass
    return 0


def save_offset(offset):
    OFFSET_FILE.write_text(str(offset))


def poll():
    """Get new messages since last check."""
    offset = get_last_offset()
    resp = requests.get(f"{API}/getUpdates", params={
        "offset": offset,
        "timeout": 1,
        "allowed_updates": json.dumps(["message"]),
    })
    data = resp.json()
    if not data.get("ok"):
        print(f"Error: {data}")
        return []

    messages = []
    for update in data.get("result", []):
        uid = update["update_id"]
        msg = update.get("message", {})
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

        save_offset(uid + 1)

    return messages


def send(text):
    """Send a text message to the user."""
    resp = requests.post(f"{API}/sendMessage", json={
        "chat_id": CHAT_ID,
        "text": text,
    })
    return resp.status_code == 200


def send_photo(photo_path, caption=None):
    """Send a photo to the user."""
    with open(photo_path, "rb") as f:
        data = {"chat_id": CHAT_ID}
        if caption:
            data["caption"] = caption
        resp = requests.post(f"{API}/sendPhoto", data=data, files={"photo": f})
    return resp.status_code == 200


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
        if send(text):
            print("Sent OK")
        else:
            print("Send FAILED")

    elif cmd == "photo":
        path = sys.argv[2]
        caption = " ".join(sys.argv[3:]) if len(sys.argv) > 3 else None
        if send_photo(path, caption):
            print("Photo sent OK")
        else:
            print("Photo send FAILED")
