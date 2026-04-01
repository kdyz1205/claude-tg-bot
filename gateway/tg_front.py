"""
Gateway Telegram front: MarkdownV2 copy + inline keyboards (PTB).

Pure presentation — no HTTP, no portfolio refresh. Used only by ``gateway.telegram_bot``.
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


def mode_label(mode: str) -> str:
    m = (mode or "paper").lower()
    if m == "live":
        return "🔴 真金实盘 (Live)"
    return "🔵 模拟盘 (Paper)"


def _mode_banner_v2(mode: str) -> str:
    label = escape_v2(mode_label(mode))
    return f"*当前交易模式：* {label}"


def build_main_keyboard(mode: str):
    if InlineKeyboardButton is None or InlineKeyboardMarkup is None:
        return None
    m = (mode or "paper").lower()
    paper_mark = "✓ " if m == "paper" else ""
    live_mark = "✓ " if m == "live" else ""
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"{paper_mark}🔵 模拟盘",
                    callback_data=f"{GW_CB}:mode:paper",
                ),
                InlineKeyboardButton(
                    f"{live_mark}🔴 真金实盘",
                    callback_data=f"{GW_CB}:mode:live",
                ),
            ],
            [
                InlineKeyboardButton("📊 持仓", callback_data=f"{GW_CB}:pos"),
                InlineKeyboardButton("📈 策略", callback_data=f"{GW_CB}:strat"),
            ],
        ]
    )


def build_back_keyboard():
    if InlineKeyboardButton is None or InlineKeyboardMarkup is None:
        return None
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⬅️ 返回主页", callback_data=f"{GW_CB}:home")]]
    )


def build_positions_keyboard():
    if InlineKeyboardButton is None or InlineKeyboardMarkup is None:
        return None
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔄 刷新数据", callback_data=f"{GW_CB}:pos")],
            [InlineKeyboardButton("⬅️ 返回主页", callback_data=f"{GW_CB}:home")],
        ]
    )


def render_home_text(mode: str) -> str:
    banner = _mode_banner_v2(mode)
    m = (mode or "paper").lower()
    hint = (
        "模拟盘：展示与演练环境，链上/交易所只读或按你的本地配置；下单前请再确认。"
        if m == "paper"
        else "⚠️ 实盘：真实资金与真实成交。请谨慎操作。"
    )
    hint_e = escape_v2(hint)
    return (
        f"{banner}\n\n"
        "*🏠 交易面板 · 主页*\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{hint_e}\n\n"
        "请选择上方模式，或进入 *持仓* / *策略*。"
    )


def _format_snapshot_v2(mode: str, snap: dict) -> str:
    e = escape_v2
    lines: list[str] = [
        _mode_banner_v2(mode),
        "",
        "*📊 持仓概览*",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    age = snap.get("age_sec")
    if age is not None:
        lines.append(f"_{e(f'数据延迟：约 {int(age)}s（后台刷新后更新）')}_")
    lines.append("")

    w = snap.get("wallet") or {}
    if w.get("ok"):
        lines.append("*链上钱包*")
        pk = e(str(w.get("pubkey_short", "?")))
        lines.append(f"· 地址：`{pk}`")
        lines.append(f"· SOL：`{w.get('sol_bal', 0):.4f}`")
        lines.append(f"· Token 数：{w.get('token_count', 0)}")
        for t in (w.get("tokens") or [])[:8]:
            lab = e(str(t.get("label", "?")))
            amt_s = e(f"{t.get('amount', 0):.6g}")
            lines.append(f"  \\- {lab}: {amt_s}")
        lines.append("")
    elif w.get("error"):
        lines.append(f"*链上钱包：* 读取失败（{e(str(w.get('error', ''))[:120])}）")
        lines.append("")
    else:
        lines.append("*链上钱包：* 未配置或不可用")
        lines.append("")

    ox = snap.get("okx") or {}
    if ox.get("ok"):
        lines.append("*OKX*")
        lines.append(f"· 权益 USD：`{ox.get('total_equity_usd', 0):.2f}`")
        lines.append(f"· 可用 USDT：`{ox.get('usdt_available', 0):.2f}`")
        pos = ox.get("positions") or []
        if pos:
            lines.append("· 合约持仓：")
            for p in pos[:10]:
                iid = e(str(p.get("instId", "")))
                pos_upl = e(f"pos={p.get('pos', 0)} upl={p.get('upl', 0):.4f}")
                lines.append(f"  \\- `{iid}` {pos_upl}")
        lines.append("")
    elif ox.get("has_keys") and ox.get("error"):
        lines.append(f"*OKX：* {e(str(ox.get('error', ''))[:200])}")
        lines.append("")
    elif not ox.get("has_keys"):
        lines.append("*OKX：* 未配置 API 密钥")
        lines.append("")

    dex = snap.get("dex") or {}
    dpos = dex.get("positions") or []
    if dpos:
        lines.append("*DEX 持仓*")
        lines.append(
            f"· 合计投入 SOL：`{dex.get('total_invested_sol', 0):.4f}` \\| "
            f"估值 SOL：`{dex.get('total_value_sol', 0):.4f}`"
        )
        for p in dpos[:8]:
            sym = e(str(p.get("symbol") or "?")[:10])
            pnl_s = e(f"{float(p.get('pnl_pct', 0) or 0):+.1f}%")
            amt_sol = e(f"{float(p.get('amount_sol', 0) or 0):.4f} SOL")
            lines.append(f"  \\- {sym} \\| PnL {pnl_s} \\| {amt_sol}")
        lines.append("")
    elif dex.get("error"):
        lines.append(f"*DEX：* {e(str(dex.get('error', ''))[:200])}")
        lines.append("")

    sol_p = snap.get("sol_price") or 0
    if sol_p:
        chg = snap.get("sol_chg_pct") or 0
        chg_s = e(f"{float(chg):+.2f}% 24h")
        lines.append(f"*SOL 参考价：* \\${sol_p:.4f} \\({chg_s}\\)")

    err = snap.get("last_error") or ""
    if err:
        lines.append("")
        lines.append(f"⚠️ _{e(str(err)[:300])}_")

    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3990] + "\n…"
    return text


def render_positions_text(mode: str, snap: dict | None, *, refreshing: bool = False) -> str:
    if not snap:
        return (
            f"{_mode_banner_v2(mode)}\n\n"
            "*📊 持仓*\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "暂无快照数据。请点击 *刷新数据*。"
        )
    body = _format_snapshot_v2(mode, snap)
    if refreshing:
        body = body + "\n\n_" + escape_v2("后台同步中…") + "_"
    return body


def render_strategy_text(mode: str) -> str:
    m = (mode or "paper").lower()
    banner = _mode_banner_v2(mode)
    if m == "paper":
        body = (
            "*📈 策略 · 模拟盘*\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "· 建议：先在模拟盘完成信号验证与仓位规则演练。\n"
            "· 与主机器人 `/panel` 中的 Paper 模块配合使用。\n"
            "· 切换到实盘前请确认 API / 钱包权限与风控上限。"
        )
    else:
        body = (
            "*📈 策略 · 实盘*\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "*⚠️ 真实资金* — 任何自动或手动下单均可能产生盈亏。\n\n"
            "· 确认 API Key 权限（只读 vs 交易）。\n"
            "· 建议启用单笔上限、日亏损熔断。\n"
            "· 详细执行逻辑见项目内 `trading/` 与 `live_trader` 配置。"
        )
    return f"{banner}\n\n{body}"
