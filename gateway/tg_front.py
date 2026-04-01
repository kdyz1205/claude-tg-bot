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
        sign = "\\+" if upl >= 0 else ""
        lines.append(f"{n}\\. {leg} \\| 浮盈: {sign}{_num_v2(upl, '.2f')}")
        n += 1

    if n == 1 and open_live:
        for p in open_live[:3]:
            sym = e(str(p.get("symbol") or "?")[:12])
            pnl = float(p.get("pnl_sol", 0) or 0)
            sign = "\\+" if pnl >= 0 else ""
            lines.append(f"{n}\\. {sym} \\| PnL: {sign}{_num_v2(pnl, '.4f')} SOL")
            n += 1

    if n == 1:
        lines.append("_" + e("暂无对冲腿快照（等待后台同步）") + "_")

    return "\n".join(lines)


def _hedge_section_plain(snap: dict, open_live: list[dict]) -> str:
    lines: list[str] = ["⚔️ 自动对冲持仓:", ""]
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
            leg = f"${sym} (现货多) + (OKX空)"
        else:
            leg = f"${sym} (OKX空)"
        sign = "+" if upl >= 0 else ""
        lines.append(f"{n}. {leg} | 浮盈: {sign}{upl:.2f}")
        n += 1

    if n == 1 and open_live:
        for p in open_live[:3]:
            sym = str(p.get("symbol") or "?")[:12]
            pnl = float(p.get("pnl_sol", 0) or 0)
            sign = "+" if pnl >= 0 else ""
            lines.append(f"{n}. {sym} | PnL: {sign}{pnl:.4f} SOL")
            n += 1

    if n == 1:
        lines.append("（暂无对冲腿快照，等待后台同步）")

    return "\n".join(lines)


def _okx_net_usd_plain(snap: dict) -> tuple[str, float]:
    """Label line + numeric USD (0 if unknown). OKX row only — no wallet fallback."""
    ox = snap.get("okx") or {}
    if not ox.get("ok"):
        return "— （未接入 API 或未返回）", 0.0
    v = float(ox.get("total_equity_usd", 0) or 0)
    if v <= 0:
        return "$0.00（权益为 0 或未同步）", 0.0
    return f"${v:,.2f}", v


def _chain_net_plain(snap: dict) -> tuple[str, float]:
    w = snap.get("wallet") or {}
    sp = float(snap.get("sol_price") or 0)
    if not w.get("ok"):
        return "— （链上钱包未配置或未读）", 0.0
    sb = float(w.get("sol_bal", 0) or 0)
    if sp > 0:
        usd = sb * sp
        return f"${usd:,.2f}（≈ {sb:.4f} SOL）", usd
    return f"{sb:.4f} SOL（暂无 SOL/USD 价）", 0.0


def render_dashboard_plain_text(
    mode: str,
    snap: dict,
    sched_state: dict,
    live_stats: dict,
) -> str:
    """Same facts as ``render_dashboard_text`` but no Markdown — safe for ``parse_mode=None``."""
    m = (mode or "paper").lower()
    active = bool(sched_state.get("active"))
    err = (snap.get("last_error") or "").strip()
    health = "🟢 运行正常" if active and not err else ("🔴 异常" if err else "⚪ 引擎未启动")

    okx_line, _ = _okx_net_usd_plain(snap)
    chain_line, _ = _chain_net_plain(snap)

    daily_sol = float(live_stats.get("daily_pnl_sol", 0) or 0)
    sol_p = float(snap.get("sol_price") or 0)
    daily_usd = daily_sol * sol_p if sol_p > 0 else daily_sol
    _, ox_v = _okx_net_usd_plain(snap)
    _, ch_v = _chain_net_plain(snap)
    ref = ox_v + ch_v
    start_bal = float(live_stats.get("starting_balance", 0) or 0)
    denom_usd = start_bal * sol_p if (start_bal > 0 and sol_p > 0) else (ref if ref > 0 else 1.0)
    pct = (daily_usd / denom_usd * 100.0) if denom_usd else 0.0
    d_sign = "+" if daily_usd >= 0 else ""
    p_sign = "+" if pct >= 0 else ""
    emoji_pnl = "🟢" if daily_usd >= 0 else "🔴"

    try:
        import live_trader

        open_live = live_trader.get_open_live_positions()
    except Exception:
        open_live = []

    mode_note = "实盘" if m == "live" else "模拟"
    body = (
        f"🤖 奇点量化终端 | [{health}]\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"OKX 净资产: {okx_line}\n"
        f"链上净资产: {chain_line}\n"
        f"📈 今日盈亏: {d_sign}${abs(daily_usd):.2f} ({emoji_pnl} {p_sign}{abs(pct):.1f}%)\n"
        f"模式: {mode_note}\n\n"
        f"{_hedge_section_plain(snap, open_live)}\n"
        "━━━━━━━━━━━━━━━━━━━━━━"
    )
    if err:
        body += "\n引擎: " + str(err)[:280]
    if len(body) > 4000:
        body = body[:3990] + "\n…"
    return body


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
    okx_plain, ox_usd = _okx_net_usd_plain(snap)
    chain_plain, ch_usd = _chain_net_plain(snap)
    okx_line_e = e(okx_plain)
    chain_line_e = e(chain_plain)

    daily_sol = float(live_stats.get("daily_pnl_sol", 0) or 0)
    sol_p = float(snap.get("sol_price") or 0)
    daily_usd = daily_sol * sol_p if sol_p > 0 else daily_sol
    ref_usd = float(ox_usd) + float(ch_usd)
    start_bal = float(live_stats.get("starting_balance", 0) or 0)
    denom_usd = start_bal * sol_p if (start_bal > 0 and sol_p > 0) else (ref_usd if ref_usd > 0 else 1.0)
    pct = (daily_usd / denom_usd * 100.0) if denom_usd else 0.0
    d_sign = "\\+" if daily_usd >= 0 else ""
    p_sign = "\\+" if pct >= 0 else ""
    emoji_pnl = "🟢" if daily_usd >= 0 else "🔴"
    du_v = _num_v2(abs(daily_usd), ".2f")
    pct_v = _num_v2(abs(pct), ".1f")

    try:
        import live_trader

        open_live = live_trader.get_open_live_positions()
    except Exception:
        open_live = []

    wallet_hint = ""
    if m == "live" and not (snap.get("wallet") or {}).get("ok"):
        wallet_hint = f"\n_{e('链上未就绪时可先 /wallet_setup；快照来自内存缓存。')}_\n"

    mode_note = e("实盘" if m == "live" else "模拟")
    ox_age = bool(ox.get("ok"))
    cache_note = e(
        f"快照: OKX={'OK' if ox_age else '—'} · 内存缓存 {_num_v2(float(snap.get('age_sec') or 0), '.0f')}s 前"
    )
    body = (
        f"🤖 *奇点量化终端* \\| \\[{health_e}\\]\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏦 *{e('OKX 净资产')}:* {okx_line_e}\n"
        f"🔗 *{e('链上净资产')}:* {chain_line_e}\n"
        f"📈 *{e('今日盈亏')}:* {d_sign}\\${du_v} \\({emoji_pnl} {p_sign}{pct_v}%\\)\n"
        f"_{e('模式')}: {mode_note}_ · _{cache_note}_{wallet_hint}\n"
        f"{_hedge_section_v2(snap, open_live)}\n"
        "━━━━━━━━━━━━━━━━━━━━━━"
    )
    if len(body) > 4000:
        body = body[:3990] + "\n…"
    return body


def render_status_brief_text(
    mode: str,
    snap: dict,
    sched_state: dict,
    live_stats: dict,
) -> str:
    """Compact status strip for spinal fast path (MarkdownV2, no network)."""
    e = escape_v2
    m = (mode or "paper").lower()
    active = bool(sched_state.get("active"))
    err = (snap.get("last_error") or "").strip()
    health = "🟢 运行正常" if active and not err else ("🔴 异常" if err else "⚪ 引擎未启动")
    age = float(snap.get("age_sec") or 0)
    scans = int(sched_state.get("total_scans") or 0)
    err_n = int(sched_state.get("errors") or 0)
    ox = snap.get("okx") or {}
    w = snap.get("wallet") or {}
    ox_ok = bool(ox.get("ok"))
    w_ok = bool(w.get("ok"))
    open_n = int(live_stats.get("open_positions") or 0)
    closed_n = int(live_stats.get("closed_trades") or 0)
    daily = float(live_stats.get("daily_pnl_sol") or 0)
    d_sign = "\\+" if daily >= 0 else ""
    mode_note = e("实盘" if m == "live" else "模拟")
    lines = [
        f"*📡 {e('状态速报')}*",
        "━━━━━━━━━━━━━━━━━━━━━━",
        f"· {e('模式')}: {mode_note}",
        f"· {e('健康')}: {e(health)}",
        f"· {e('资金快照缓存')}: {_num_v2(age, '.0f')}{e(' 秒前')}",
        f"· {e('调度')}: `{scans}` {e('扫')} · {e('异常计数')} `{err_n}`",
        f"· OKX: `{'1' if ox_ok else '0'}` · {e('链上钱包')}: `{'1' if w_ok else '0'}`",
        f"· {e('引擎')}: `{open_n}` {e('开')} / `{closed_n}` {e('平')}",
        f"· {e('今日引擎 PnL')}: `{d_sign}{_num_v2(daily, '.4f')}` SOL",
    ]
    if err:
        lines.append(f"· _{e(err[:200])}_")
    out = "\n".join(lines)
    if len(out) > 4000:
        out = out[:3990] + "\n…"
    return out


def build_dashboard_keyboard(sched_active: bool) -> InlineKeyboardMarkup | None:
    if InlineKeyboardButton is None or InlineKeyboardMarkup is None:
        return None
    _ = sched_active  # 保留签名；引擎状态在文案中已体现
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🚀 启动全引擎",
                    callback_data=f"{GW_CB}:engine_start",
                ),
                InlineKeyboardButton(
                    "🛑 停止全引擎",
                    callback_data=f"{GW_CB}:engine_stop",
                ),
            ],
            [
                InlineKeyboardButton(
                    "⚡ 极速手动交易",
                    callback_data=f"{GW_CB}:manual_hub",
                ),
                InlineKeyboardButton(
                    "🛡️ 风险控制设置",
                    callback_data=f"{GW_CB}:risk",
                ),
            ],
            [
                InlineKeyboardButton(
                    "🔍 遍历全平台同步",
                    callback_data=f"{GW_CB}:full_sync",
                ),
            ],
        ]
    )


def build_manual_hub_keyboard() -> InlineKeyboardMarkup | None:
    if InlineKeyboardButton is None or InlineKeyboardMarkup is None:
        return None
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🏦 OKX 研发中枢",
                    callback_data=f"{GW_CB}:manual_okx",
                ),
                InlineKeyboardButton(
                    "🔗 链上狙击中心",
                    callback_data=f"{GW_CB}:manual_onchain",
                ),
            ],
            [InlineKeyboardButton("↩️ 返回主看板", callback_data=f"{GW_CB}:dash")],
        ]
    )


def render_manual_hub_text() -> str:
    e = escape_v2
    return (
        f"⚡ *{e('极速手动交易')}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"_{e('二选一进入子面板；OKX 侧重策略研发，链上侧重 DEX 狙击。')}_"
    )


def render_okx_lab_text() -> str:
    e = escape_v2
    return (
        f"🏦 *{e('OKX 策略研发中枢')}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"_{e('此处不提供手动买卖；用于算法进化、训练与因子工程。')}_\n\n"
        f"· *{e('自动进化')}:* {e('InfiniteEvolver 周期 sweep')}\n"
        f"· *{e('模型训练')}:* {e('auto_train 提示词/评测闭环')}\n"
        f"· *{e('因子挖掘')}:* {e('FACTOR_FORGE 造物主写 sk_ 技能')}\n"
        f"· *{e('Alpha 状态')}:* {e('本机 skills 与任务一瞥')}_"
    )


def build_okx_lab_keyboard() -> InlineKeyboardMarkup | None:
    if InlineKeyboardButton is None or InlineKeyboardMarkup is None:
        return None
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🧬 开启自动进化",
                    callback_data=f"{GW_CB}:lab:evolve",
                ),
            ],
            [
                InlineKeyboardButton(
                    "🧠 启动模型训练",
                    callback_data=f"{GW_CB}:lab:train",
                ),
            ],
            [
                InlineKeyboardButton(
                    "🧪 因子挖掘 (Factor Forge)",
                    callback_data=f"{GW_CB}:lab:factor",
                ),
            ],
            [
                InlineKeyboardButton(
                    "📊 Alpha 状态看板",
                    callback_data=f"{GW_CB}:lab:alpha",
                ),
            ],
            [InlineKeyboardButton("↩️ 上一层", callback_data=f"{GW_CB}:manual_hub")],
            [InlineKeyboardButton("↩️ 返回主看板", callback_data=f"{GW_CB}:dash")],
        ]
    )


def render_onchain_hub_text() -> str:
    e = escape_v2
    return (
        f"🔗 *{e('链上狙击中心')}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"_{e('DEX 手动买卖、持仓与 CA 粘贴在本机器人请使用命令 /trade（与主交易面板同源）。')}_\n\n"
        f"*{e('操作')}:* {e('在会话中发送')} `/trade`"
    )


def build_onchain_hub_keyboard() -> InlineKeyboardMarkup | None:
    if InlineKeyboardButton is None or InlineKeyboardMarkup is None:
        return None
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("↩️ 返回主看板", callback_data=f"{GW_CB}:dash")],
            [
                InlineKeyboardButton(
                    "↩️ 上一层",
                    callback_data=f"{GW_CB}:manual_hub",
                ),
            ],
        ]
    )


def build_risk_keyboard(mode: str) -> InlineKeyboardMarkup | None:
    return build_risk_settings_keyboard(mode)


def build_back_keyboard() -> InlineKeyboardMarkup | None:
    return build_risk_keyboard("paper")


def render_risk_settings_text(mode: str, cfg: dict) -> str:
    e = escape_v2
    m = (mode or "paper").lower()
    mts = cfg.get("max_trade_sol")
    mts_s = "null" if mts is None else f"{float(mts):.4f}"
    lines = [
        f"🛡️ *{e('风险控制')}* \\(_{e('模式')}: {e('实盘' if m == 'live' else '模拟')}_\\)",
        "━━━━━━━━━━━━━━━━━━━━━━",
        "",
        f"1\\. *MaxTradeSize* \\(`max\\_trade\\_pct`\\): `{float(cfg.get('max_trade_pct', 0) or 0):.1f}`% {e('单笔相对权益上限')}",
        f"2\\. *MaxTradeSOL* \\(`max\\_trade\\_sol`\\): `{mts_s}` {e('SOL 硬顶，null=仅按百分比')}",
        f"3\\. *MaxPosition* \\(`max\\_positions`\\): `{int(cfg.get('max_positions', 0) or 0)}` {e('最大并发仓位')}",
        f"4\\. *DailyLossLimit* \\(`daily\\_loss\\_limit\\_pct`\\): `{float(cfg.get('daily_loss_limit_pct', 0) or 0):.1f}`%",
        f"5\\. *StopLoss* \\(`stop\\_loss\\_pct`\\): `{float(cfg.get('stop_loss_pct', 0) or 0):.1f}`%",
        f"6\\. *TakeProfit* \\(`take\\_profit\\_pct`\\): `{float(cfg.get('take_profit_pct', 0) or 0):.1f}`%",
        f"7\\. *PairVolatilityThreshold* \\(`max\\_slippage\\_bps`\\): `{int(cfg.get('max_slippage_bps', 0) or 0)}` bps",
        f"8\\. *MinLiquidity* \\(`min\\_liquidity\\_usd`\\): `{float(cfg.get('min_liquidity_usd', 0) or 0):,.0f}` USD",
        f"9\\. *neural\\_execution:* `{'on' if cfg.get('neural_execution_enabled') else 'off'}`",
        "",
        "_" + e("点下方 ✏️ 回复数字即可热更新；无需改代码。") + "_",
    ]
    return "\n".join(lines)


def build_risk_settings_keyboard(mode: str) -> InlineKeyboardMarkup | None:
    """风控页：每键 ✏️ 调整 + 模式切换 + 返回看板。"""
    if InlineKeyboardButton is None or InlineKeyboardMarkup is None:
        return None
    m = (mode or "paper").lower()
    re = f"{GW_CB}:risk_edit"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✏️ MaxTrade%", callback_data=f"{re}:max_trade_pct"),
                InlineKeyboardButton("✏️ MaxTradeSOL", callback_data=f"{re}:max_trade_sol"),
            ],
            [
                InlineKeyboardButton("✏️ MaxPos", callback_data=f"{re}:max_positions"),
                InlineKeyboardButton("✏️ DailyLoss", callback_data=f"{re}:daily_loss_limit_pct"),
            ],
            [
                InlineKeyboardButton("✏️ SL%", callback_data=f"{re}:stop_loss_pct"),
                InlineKeyboardButton("✏️ TP%", callback_data=f"{re}:take_profit_pct"),
            ],
            [
                InlineKeyboardButton("✏️ Slip bps", callback_data=f"{re}:max_slippage_bps"),
                InlineKeyboardButton("✏️ MinLiq", callback_data=f"{re}:min_liquidity_usd"),
            ],
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
            [
                InlineKeyboardButton(
                    "❌ 取消数值编辑",
                    callback_data=f"{GW_CB}:risk_cancel",
                ),
            ],
            [InlineKeyboardButton("↩️ 返回主看板", callback_data=f"{GW_CB}:dash")],
        ]
    )


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

    snap = portfolio_snapshot.get_local_cache()
    st = trade_scheduler.read_scheduler_state()
    stats = live_trader.get_live_stats() if live_trader else {}
    return render_dashboard_text(mode, snap, st, stats)


tg_gw_render_home_text = render_home_text


def build_main_keyboard(mode: str):
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
