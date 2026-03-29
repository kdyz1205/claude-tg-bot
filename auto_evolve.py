"""
Auto Evolve Monitor v1.0
自动检测 Claude Code session 状态：
- 运行中 → 继续等待
- 空闲/停止 → 催促继续
- Context limit → 自动发送续命指令
- 连续多次空闲 → 认定为 context limit，重新激活
"""
import pyautogui
import pyperclip
import time
import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
USER_ID = os.getenv('AUTHORIZED_USER_ID')

_BOT_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(_BOT_DIR, "_evolve_state.json")

EVOLUTION_PROMPT = """You are a SUPERINTELLIGENT AGENT. Your mission: EVOLVE AGGRESSIVELY.

LOOP FOREVER - fix bugs, add capabilities, evolve until you surpass OpenClaw:
1. SCAN all code for bugs, limitations, inefficiencies
2. FIX immediately - computer vision, click targeting, self-healing, adaptive control
3. EVOLVE - smarter screenshot analysis, universal element detection, multi-window, web scraping
4. TEST - verify each fix works
5. REPEAT - never stop

You have FULL Windows access: any window, any webpage, any app.
Do NOT ask questions. Do NOT stop. BUILD IT NOW."""

CONTEXT_LIMIT_HINT = """CONTEXT LIMIT DETECTED - continuing evolution from previous session.

Previous progress: massive bug fixing campaign, evolving PC control capabilities.
Goal: surpass OpenClaw in intelligence and computer control.

Continue from where you left off:
1. Check current state of bot code
2. Find next set of bugs/limitations
3. Fix and evolve
4. Keep going - do NOT stop"""

CLAUDE_REPLY_BOX = (730, 878)
SESSION_SIDEBAR_X = 90
TG_BOT_SESSION_Y = 445  # verified 2026-03-28 (below Dispatch/Customize menus)

def screenshot(path=None):
    if path is None:
        path = os.path.join(_BOT_DIR, "current_screen.png")
    img = pyautogui.screenshot()
    img.save(path)
    return img

def notify(text):
    if TOKEN and USER_ID:
        try:
            requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                json={"chat_id": USER_ID, "text": text}, timeout=10)
        except Exception as e:
            print(f"[TG] notify failed: {e}")
    print(f"[TG] {text.encode('ascii', errors='replace').decode()}")

def send_photo(path, caption=""):
    if TOKEN and USER_ID:
        try:
            with open(path, "rb") as f:
                requests.post(f"https://api.telegram.org/bot{TOKEN}/sendPhoto",
                    data={"chat_id": USER_ID, "caption": caption},
                    files={"photo": f}, timeout=15)
        except Exception as e:
            print(f"[TG] photo failed: {e}")

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, ValueError, OSError) as e:
            print(f"[WARN] Corrupt state file, resetting: {e}")
    return {"idle_count": 0, "last_status": "unknown", "last_check": "", "total_prods": 0}

def save_state(state):
    tmp = STATE_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, STATE_FILE)
    except OSError as e:
        print(f"[WARN] Failed to save state: {e}")
        try:
            os.remove(tmp)
        except OSError:
            pass

def check_session_status(img):
    """
    分析截图判断 session 状态
    返回: 'running' | 'idle' | 'context_limit'
    """
    img_w, img_h = img.size

    def safe_crop(l, t, r, b):
        """Clamp crop rectangle to image bounds."""
        return img.crop((max(0, l), max(0, t), min(r, img_w), min(b, img_h)))

    # 1. 检查状态栏区域（y=355-410）是否有橙/黄色动画文字（运行中）
    status_area = safe_crop(300, 350, 900, 415)
    pixels = status_area.load()
    w, h = status_area.size

    orange_count = 0
    for y in range(0, h, 2):
        for x in range(0, w, 2):
            r, g, b = pixels[x, y][:3]
            # 橙色/黄色 = 运行中的 spinner
            if r > 180 and g > 100 and b < 100:
                orange_count += 1

    if orange_count > 20:
        return 'running'

    # 2. 检查 Reply 框区域（空闲时背景深色，有占位文字）
    reply_area = safe_crop(340, 860, 760, 900)
    pixels2 = reply_area.load()
    w2, h2 = reply_area.size
    dark_count = sum(1 for y in range(h2) for x in range(w2)
                    if sum(pixels2[x, y][:3]) < 150)
    reply_available = dark_count > (w2 * h2 * 0.6)

    # 3. 检查顶部是否有 context limit 提示框（灰色大块 summary box）
    top_area = safe_crop(300, 60, 900, 200)
    pixels3 = top_area.load()
    w3, h3 = top_area.size
    gray_count = sum(1 for y in range(0, h3, 3) for x in range(0, w3, 3)
                    if 30 < sum(pixels3[x, y][:3]) // 3 < 80)
    has_summary_box = gray_count > 200

    if reply_available and has_summary_box:
        return 'context_limit'
    elif reply_available:
        return 'idle'
    else:
        return 'running'

def send_to_session(message):
    """发送消息到当前 Claude Code session"""
    pyautogui.click(CLAUDE_REPLY_BOX[0], CLAUDE_REPLY_BOX[1])
    time.sleep(0.8)
    pyperclip.copy(message)
    pyautogui.hotkey('ctrl', 'v')
    time.sleep(0.3)
    pyautogui.press('enter')
    print(f"[SENT] {message[:60]}...")

def ensure_tg_bot_session_active():
    """确保当前显示的是 TG Bot session"""
    pyautogui.click(SESSION_SIDEBAR_X, TG_BOT_SESSION_Y)
    time.sleep(1.5)

def run_check():
    """执行一次监控检查（由 cron 每5分钟调用）"""
    state = load_state()
    now = datetime.now().strftime("%H:%M:%S")

    # 确保在正确 session
    ensure_tg_bot_session_active()
    time.sleep(1)

    img = screenshot()
    status = check_session_status(img)
    state["last_check"] = now
    state["last_status"] = status

    print(f"[{now}] Status: {status} | idle_count: {state['idle_count']} | prods: {state['total_prods']}")

    if status == 'running':
        # 正在运行 - 好，等待
        state["idle_count"] = 0
        send_photo(os.path.join(_BOT_DIR, "current_screen.png"), f"✅ [{now}] 进化中... 共催促{state['total_prods']}次")

    elif status == 'context_limit':
        # Context limit - 发送续命指令
        state["idle_count"] = 0
        state["total_prods"] += 1
        send_to_session(CONTEXT_LIMIT_HINT)
        time.sleep(5)
        screenshot()
        send_photo(os.path.join(_BOT_DIR, "current_screen.png"),
            f"🔄 [{now}] Context limit! 已自动续命 (第{state['total_prods']}次)")
        notify(f"🔄 Context limit 检测到，已自动续命进化！")

    elif status == 'idle':
        state["idle_count"] += 1

        if state["idle_count"] >= 2:
            # 连续2次空闲 = 可能 context limit 或真的停了
            state["idle_count"] = 0
            state["total_prods"] += 1
            msg = CONTEXT_LIMIT_HINT if state["total_prods"] % 3 == 0 else EVOLUTION_PROMPT
            send_to_session(msg)
            time.sleep(5)
            screenshot()
            send_photo(os.path.join(_BOT_DIR, "current_screen.png"),
                f"⚡ [{now}] 连续空闲，已强制续命 (第{state['total_prods']}次)")
        else:
            # 第一次空闲，先轻推一下
            state["total_prods"] += 1
            send_to_session("Continue evolving. Do not stop.")
            time.sleep(5)
            screenshot()
            send_photo(os.path.join(_BOT_DIR, "current_screen.png"),
                f"💬 [{now}] Session空闲，已催促 (第{state['total_prods']}次)")

    save_state(state)
    return status

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "check":
        status = run_check()
        print(f"Final status: {status}")
    elif len(sys.argv) > 1 and sys.argv[1] == "state":
        print(json.dumps(load_state(), indent=2))
    elif len(sys.argv) > 1 and sys.argv[1] == "reset":
        save_state({"idle_count": 0, "last_status": "reset", "last_check": "", "total_prods": 0})
        print("State reset")
    else:
        # 直接运行一次检查
        run_check()
