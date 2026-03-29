"""
takeover.py — Screen takeover countdown overlay.

Shows a 3-second countdown in the center of the screen before bot takes control.
Right-click anywhere to CANCEL the takeover.

Usage:
  python takeover.py                    → 3s countdown, default message
  python takeover.py 5                  → 5s countdown
  python takeover.py 3 "正在打开浏览器"  → custom message

Exit codes:
  0 = proceed (countdown finished)
  1 = cancelled (user right-clicked)
"""
import sys
import tkinter as tk


def main():
    try:
        duration = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    except ValueError:
        duration = 3
    if duration < 1:
        duration = 1
    message = sys.argv[2] if len(sys.argv) > 2 else "Bot 即将接管鼠标键盘"
    remaining = [duration]
    cancelled = [False]

    root = tk.Tk()
    root.title("Bot Takeover")
    root.attributes("-topmost", True)
    root.overrideredirect(True)  # No window border

    # Semi-transparent dark overlay — centered box
    screen_w = root.winfo_screenwidth()
    screen_h = root.winfo_screenheight()
    box_w, box_h = 500, 200
    x = (screen_w - box_w) // 2
    y = (screen_h - box_h) // 2
    root.geometry(f"{box_w}x{box_h}+{x}+{y}")
    root.configure(bg="#1a1a2e")

    # Try to make it semi-transparent (Windows)
    try:
        root.attributes("-alpha", 0.92)
    except Exception:
        pass

    # Title
    tk.Label(
        root, text="🤖 " + message,
        font=("Segoe UI", 14, "bold"), fg="#e0e0e0", bg="#1a1a2e",
        wraplength=460,
    ).pack(pady=(20, 5))

    # Countdown number
    countdown_label = tk.Label(
        root, text=str(duration),
        font=("Segoe UI", 48, "bold"), fg="#00d4ff", bg="#1a1a2e",
    )
    countdown_label.pack()

    # Hint
    hint_label = tk.Label(
        root, text="Click / Esc / Space to cancel",
        font=("Segoe UI", 10), fg="#888888", bg="#1a1a2e",
    )
    hint_label.pack(pady=(0, 10))

    # Progress bar frame
    bar_frame = tk.Frame(root, bg="#333333", height=6, width=460)
    bar_frame.pack(pady=(0, 10))
    bar_frame.pack_propagate(False)
    bar_fill = tk.Frame(bar_frame, bg="#00d4ff", height=6)
    bar_fill.place(x=0, y=0, relwidth=1.0, relheight=1.0)

    destroyed = [False]

    def safe_destroy():
        if not destroyed[0]:
            destroyed[0] = True
            try:
                root.destroy()
            except Exception:
                pass

    def on_cancel(event=None):
        if cancelled[0]:
            return  # Prevent double-cancel
        cancelled[0] = True
        try:
            countdown_label.config(text="X", fg="#ff4444")
            hint_label.config(text="Cancelled", fg="#ff4444")
        except Exception:
            pass
        root.after(400, safe_destroy)

    # Bind right-click, left-click, and Escape to cancel
    root.bind("<Button-1>", on_cancel)
    root.bind("<Button-3>", on_cancel)
    root.bind("<Escape>", on_cancel)
    root.bind("<space>", on_cancel)

    # Grab keyboard focus so Escape works even if overlay isn't clicked
    root.focus_force()

    def tick():
        if cancelled[0] or destroyed[0]:
            return
        remaining[0] -= 1
        if remaining[0] <= 0:
            safe_destroy()
            return
        countdown_label.config(text=str(remaining[0]))
        # Update progress bar
        progress = remaining[0] / duration
        bar_fill.place(x=0, y=0, relwidth=progress, relheight=1.0)
        root.after(1000, tick)

    root.after(1000, tick)

    try:
        root.mainloop()
    except Exception:
        pass

    if cancelled[0]:
        print("CANCELLED")
        sys.exit(1)
    else:
        print("PROCEED")
        sys.exit(0)


if __name__ == "__main__":
    main()
