"""
精准 UI 定位器 - 窗口感知版
找到 Claude Code 窗口，计算相对坐标，返回物理像素点击位置。
"""
import ctypes, ctypes.wintypes, pyautogui, sys, time

user32 = ctypes.windll.user32

def get_chrome_window():
    """找到含 Claude Code 的 Chrome 窗口，返回 (hwnd, left, top, right, bottom) 物理像素"""
    results = []
    def cb(hwnd, _):
        if user32.IsWindowVisible(hwnd):
            buf = ctypes.create_unicode_buffer(512)
            user32.GetWindowTextW(hwnd, buf, 512)
            t = buf.value
            if 'Chrome' in t or t in ('Claude', ''):
                r = ctypes.wintypes.RECT()
                user32.GetWindowRect(hwnd, ctypes.byref(r))
                w, h = r.right - r.left, r.bottom - r.top
                if w > 800 and h > 500 and 'Chrome' in t:
                    results.append((hwnd, r.left, r.top, r.right, r.bottom, t))
        return True
    PROC = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_int, ctypes.c_int)
    user32.EnumWindows(PROC(cb), 0)
    # Return largest Chrome window (most likely fullscreen Claude Code)
    results.sort(key=lambda x: (x[3]-x[1])*(x[4]-x[2]), reverse=True)
    return results[0] if results else None

def focus_claude_code():
    """把 Chrome/Claude Code 窗口带到前台"""
    win = get_chrome_window()
    if win:
        hwnd = win[0]
        user32.ShowWindow(hwnd, 9)
        user32.SetForegroundWindow(hwnd)
        time.sleep(0.5)
        return True
    return False

def find_sidebar_sessions(screenshot=None):
    """
    在截图中扫描侧边栏，找到所有 session 行的 Y 坐标。
    返回 list of y (物理像素)，按从上到下排序。
    """
    if screenshot is None:
        screenshot = pyautogui.screenshot()
    
    px = screenshot.load()
    w, h = screenshot.size
    
    # 找 Claude Code 窗口左边界 (侧边栏在窗口左侧)
    # 扫描 y=100 处找到从暗到亮的跳变点 = Chrome 窗口左边
    chrome_left = 0
    prev = 0
    for x in range(0, min(500, w), 2):
        r,g,b = px[x, 100]
        s = r+g+b
        if s > 150 and prev < 100 and x > 10:
            chrome_left = x
            break
        prev = s
    
    # 侧边栏在 chrome_left 到 chrome_left+200 之间
    sidebar_cx = chrome_left + 90  # 侧边栏中心 x
    
    # 扫描 y=50-700 找到明亮的文字行
    sessions_y = []
    last_bright_y = -20
    for y in range(50, min(700, h), 1):
        vals = [sum(px[x,y][:3]) for x in range(chrome_left+20, min(chrome_left+180, w), 8)]
        avg = sum(vals)/len(vals) if vals else 0
        if avg > 350 and (y - last_bright_y) > 8:
            sessions_y.append(y)
            last_bright_y = y
    
    return chrome_left, sidebar_cx, sessions_y

def find_reply_box(screenshot=None):
    """
    找到 Claude Code 回复输入框的位置。
    返回 (x, y) 物理像素。
    """
    if screenshot is None:
        screenshot = pyautogui.screenshot()
    
    px = screenshot.load()
    w, h = screenshot.size
    
    # 找 Chrome 窗口左边界
    chrome_left = 0
    for x in range(0, min(500, w), 2):
        r,g,b = px[x, h//2]
        if r+g+b > 100 and chrome_left == 0:
            chrome_left = x
            break
    
    # 输入框 x 范围: chrome_left+200 到 chrome_left+900
    input_x = min(chrome_left + 500, w - 1)  # 中间位置, clamped

    # 从底部往上扫，找到输入框 (通常比周围亮一些的矩形区域)
    chrome_bottom = h - 50  # 去掉任务栏
    for y in range(chrome_bottom, chrome_bottom-200, -1):
        if y < 0 or y >= h:
            continue
        vals = [sum(px[x,y][:3]) for x in range(chrome_left+200, min(chrome_left+800, w), 20)]
        if not vals:
            continue
        avg = sum(vals)/len(vals)
        if avg > 120:  # 找到有内容的行
            return (input_x, y - 10)
    
    return (input_x, chrome_bottom - 80)

if __name__ == '__main__':
    import sys
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
    
    print('=== locate.py diagnostic ===')
    img = pyautogui.screenshot()
    chrome_left, sidebar_cx, sessions_y = find_sidebar_sessions(img)
    reply_pos = find_reply_box(img)
    
    print(f'Chrome left edge: x={chrome_left}')
    print(f'Sidebar center x: {sidebar_cx}')
    print(f'Session rows found: {len(sessions_y)}')
    for i, y in enumerate(sessions_y[:8]):
        r,g,b = img.load()[sidebar_cx, y]
        print(f'  Session {i+1}: y={y}  rgb=({r},{g},{b})')
    print(f'Reply box position: {reply_pos}')
