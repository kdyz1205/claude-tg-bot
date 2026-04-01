"""
Gateway Telegram front: MarkdownV2 + inline keyboards (PTB).

Presentation only: callers pass already-fetched snapshot dicts / stats (no network here).
"""

from __future__ import annotations

try:
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.helpers import escape_markdown as _tg_escape_markdown
except ImportError:  # pragma: no cover
    InlineKeyboardButton = None  # type: ignore[misc, assignment]
    InlineKeyboardMarkup = None  # type: ignore[misc, assignment]

    def _tg_escape_markdown(text: str, version: int = 2) -> str:
        if version != 2:
            return text
        s = str(text)
        for ch in r"_*[]()~`>#+-=|{}.!":
            s = s.replace(ch, "\\" + ch)
        return s


GW_CB = "gw"


def escape_v2(text: str) -> str:
    if not text:
        return text
    return _tg_escape_markdown(str(text), version=2)


def _okx_sym_from_inst(inst: str) -> str:
    s = (inst or "").strip()
    if not s:
        return "?"
    return s.split("-")[0] or "?"


def _num_v2(x: float, spec: str) -> str:
    return escape_v2(format(float(x), spec))


def _hedge_section_v2(snap: dict, open_live: list[dict]) -> str:
    e = escape_v2
    lines: list[str] = ["*⚔️ 自动对冲持仓:*", ""]
    ox = snap.get("okx") or {}
    positions = ox.get("positions") or []
    wallet = snap.get("wallet") or {}
    sol_bal = float(wallet.get("sol_bal") or 0)
    sol_p = float(snap.get("sol_price") or 0)

    n = 1
    for p in positions:
        pos = float(p.get("pos", 0) or 0)
        if pos >= 0:
            continue
        inst = str(p.get("instId", ""))
        sym = _okx_sym_from_inst(inst)
        upl = float(p.get("upl", 0) or 0)
        if sym == "SOL" and sol_bal > 0 and sol_p > 0:
            leg = e(f"${sym} (现货多) + (OKX空)")
        else:
            leg = e(f"${sym} (OKX空)")
        sign = "+" if upl >= 0 else ""
        lines.append(f"{n}\\. {leg} \\| 浮盈: {sign}{upl:.2f}")
        n += 1

    if n == 1 and open_live:
        for p in open_live[:3]:
            sym = e(str(p.get("symbol") or "?")[:12])
            pnl = float(p.get("pnl_sol", 0) or 0)
            sign = "+" if pnl >= 0 else ""
            lines.append(f"{n}\\. {sym} \\| PnL: {sign}{_num_v2(pnl, '.4f')} SOL")
            n += 1

    if n == 1:
        lines.append("_" + e("暂无对冲腿快照（等待后台同步）") + "_")

    return "\n".join(lines)


def render_dashboard_text(
    mode: str,
    snap: dict,
    sched_state: dict,
    live_stats: dict,
) -> str:
    e = escape_v2
    m = (mode or "paper").lower()
    active = bool(sched_state.get("active"))
    err = (snap.get("last_error") or "").strip()
    health = "🟢 运行正常" if active and not err else ("🔴 异常" if err else "⚪ 引擎未启动")
    health_e = e(health)

    ox = snap.get("okx") or {}
    eq = float(ox.get("total_equity_usd", 0) or 0)
    if eq <= 0:
        sol_p = float(snap.get("sol_price") or 0)
        w = snap.get("wallet") or {}
        sol_b = float(w.get("sol_bal", 0) or 0)
        eq = sol_b * sol_p if sol_p > 0 else 0.0

    daily_sol = float(live_stats.get("daily_pnl_sol", 0) or 0)
    sol_p = float(snap.get("sol_price") or 0)
    daily_usd = daily_sol * sol_p if sol_p > 0 else daily_sol
    start_bal = float(live_stats.get("starting_balance", 0) or 0)
    denom_usd = start_bal * sol_p if (start_bal > 0 and sol_p > 0) else (eq if eq > 0 else 1.0)
    pct = (daily_usd / denom_usd * 100.0) if denom_usd else 0.0
    d_sign = "+" if daily_usd >= 0 else ""
    p_sign = "+" if pct >= 0 else ""
    emoji_pnl = "🟢" if daily_usd >= 0 else "🔴"

    try:
        import live_trader

        open_live = live_trader.get_open_live_positions()
    except Exception:
        open_live = []

    mode_note = e("实盘" if m == "live" else "模拟")
    body = (
        f"🤖 *奇点量化终端* \\| \\[{health_e}\\]\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 *总资产净值:* \\${eq_v}\n"
        f"📈 *今日盈亏:* {d_sign}\\${du_v} \\({emoji_pnl} {p_sign}{pct_v}%\\)\n"
        f"_{e('模式')}: {mode_note}_\n\n"
        f"{_hedge_section_v2(snap, open_live)}\n"
        "━━━━━━━━━━━━━━━━━━━━━━"
    )
    if len(body) > 4000:
        body = body[:3990] + "\n…"
    return body


def build_dashboard_keyboard(sched_active: bool) -> InlineKeyboardMarkup | None:
    if InlineKeyboardButton is None or InlineKeyboardMarkup is None:
        return None
    row_engine = [
        InlineKeyboardButton(
            "▶️ 启动全自动引擎",
            callback_data=f"{GW_CB}:engine_start",
        ),
        InlineKeyboardButton(
            "⏹ 紧急停止并平仓",
            callback_data=f"{GW_CB}:engine_stop",
        ),
    ]
    row_ops = [
        InlineKeyboardButton("🔄 刷新资产", callback_data=f"{GW_CB}:refresh"),
        InlineKeyboardButton("⚙️ 风控设置", callback_data=f"{GW_CB}:risk"),
    ]
    return InlineKeyboardMarkup([row_engine, row_ops])


def build_risk_keyboard(mode: str) -> InlineKeyboardMarkup | None:
    if InlineKeyboardButton is None or InlineKeyboardMarkup is None:
        return None
    m = (mode or "paper").lower()
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"{'✓ ' if m != 'live' else ''}🔵 模拟盘",
                    callback_data=f"{GW_CB}:mode:paper",
                ),
                InlineKeyboardButton(
                    f"{'✓ ' if m == 'live' else ''}🔴 实盘",
                    callback_data=f"{GW_CB}:mode:live",
                ),
            ],
            [InlineKeyboardButton("⬅️ 返回看板", callback_data=f"{GW_CB}:dash")],
        ]
    )


def build_back_keyboard() -> InlineKeyboardMarkup | None:
    return build_risk_keyboard("paper")


def render_risk_settings_text(mode: str, cfg: dict) -> str:
    e = escape_v2
    m = (mode or "paper").lower()
    lines = [
        f"⚙️ *风控设置* \\(_{e('模式')}: {e('实盘' if m == 'live' else '模拟')}_\\)",
        "━━━━━━━━━━━━━━━━━━━━━━",
        "",
        f"· max\\_trade\\_pct: `{float(cfg.get('max_trade_pct', 0) or 0):.1f}`",
        f"· max\\_positions: `{int(cfg.get('max_positions', 0) or 0)}`",
        f"· daily\\_loss\\_limit\\_pct: `{float(cfg.get('daily_loss_limit_pct', 0) or 0):.1f}`",
        f"· stop\\_loss\\_pct: `{float(cfg.get('stop_loss_pct', 0) or 0):.1f}`",
        f"· take\\_profit\\_pct: `{float(cfg.get('take_profit_pct', 0) or 0):.1f}`",
        f"· neural\\_execution: `{'on' if cfg.get('neural_execution_enabled') else 'off'}`",
        "",
        "_" + e("修改 _live_config.json 或通过策略模块调整；此处仅展示当前文件值。") + "_",
    ]
    return "\n".join(lines)


# ─── Legacy names (imports / harness) ─────────────────────────────────────────
tg_gw_escape_v2 = escape_v2


def render_home_text(mode: str) -> str:
    """Backward-compatible: builds dashboard from in-process cache + local files only."""
    from trading import portfolio_snapshot

    import trade_scheduler

    try:
        import live_trader
    except ImportError:
        live_trader = None  # type: ignore[misc, assignment]

    snap = portfolio_snapshot.get_snapshot_for_gateway()
    st = trade_scheduler.read_scheduler_state()
    stats = live_trader.get_live_stats() if live_trader else {}
    return render_dashboard_text(mode, snap, st, stats)


tg_gw_render_home_text = render_home_text


def build_main_keyboard(mode: str, *, god_engine_active: bool = False):
    if InlineKeyboardButton is None or InlineKeyboardMarkup is None:
        return None
    import trade_scheduler

    active = bool(trade_scheduler.read_scheduler_state().get("active"))
    return build_dashboard_keyboard(active)


def build_positions_keyboard():
    return build_main_keyboard("paper")


def render_positions_text(mode: str, snap: dict | None, *, refreshing: bool = False) -> str:
    import trade_scheduler

    try:
        import live_trader
    except ImportError:
        live_trader = None  # type: ignore[misc, assignment]

    if not snap:
        snap = {}
    st = trade_scheduler.read_scheduler_state()
    stats = live_trader.get_live_stats() if live_trader else {}
    t = render_dashboard_text(mode, snap, st, stats)
    if refreshing:
        t = t + "\n\n_" + escape_v2("后台刷新已排队…") + "_"
    return t


def render_strategy_text(mode: str) -> str:
    try:
        import live_trader

        cfg = live_trader._load_config()
    except Exception:
        cfg = {}
    return render_risk_settings_text(mode, cfg)


def mode_label(mode: str) -> str:
    return "🔴 真金实盘 (Live)" if (mode or "paper").lower() == "live" else "🔵 模拟盘 (Paper)"


tg_gw_mode_label = mode_label
tg_gw_build_main_keyboard = build_main_keyboard
tg_gw_build_back_keyboard = build_back_keyboard
tg_gw_build_positions_keyboard = build_positions_keyboard
tg_gw_render_positions_text = render_positions_text
tg_gw_render_strategy_text = render_strategy_text


def tg_gw_render_callback_pending_text() -> str:
    return escape_v2("…")
