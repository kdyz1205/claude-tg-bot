"""
dashboard.py — Flask web dashboard for bot performance monitoring.
Serves at http://localhost:8080

Tracks: message stats, system resources, evolution progress, recent log.
"""

import json
import os
import threading
import time
from collections import deque
from datetime import datetime

BOT_DIR = os.path.dirname(os.path.abspath(__file__))
EVOLUTION_QUEUE_FILE = os.path.join(BOT_DIR, "_evolution_queue.json")

# ── In-memory stats ──────────────────────────────────────────────────────────

_recent_messages: deque = deque(maxlen=10)

_stats = {
    "total": 0,
    "success": 0,
    "failed": 0,
    "total_ms": 0.0,
}
_stats_lock = threading.Lock()

_server_started = False
_server_thread = None


def record_message(text: str, success: bool, duration_ms: float, response: str = "") -> None:
    """Record a processed message. Call from bot.py after each message."""
    with _stats_lock:
        _stats["total"] += 1
        if success:
            _stats["success"] += 1
        else:
            _stats["failed"] += 1
        _stats["total_ms"] += duration_ms
        _recent_messages.append({
            "ts": datetime.now().strftime("%H:%M:%S"),
            "text": text[:80],
            "success": success,
            "duration_ms": round(duration_ms),
            "response": response[:100],
        })


# ── Flask app ─────────────────────────────────────────────────────────────────

try:
    from flask import Flask, jsonify, render_template_string
    _flask_available = True
except ImportError:
    _flask_available = False

if _flask_available:
    app = Flask(__name__)

    def _get_system_stats() -> dict:
        try:
            from self_monitor import self_monitor
            h = self_monitor._last_health or {}
            checks = h.get("checks", {})
            bot_info = checks.get("bot", {})
            return {
                "cpu_pct": checks.get("cpu", {}).get("usage_pct", -1),
                "mem_pct": checks.get("memory", {}).get("usage_pct", -1),
                "mem_used_gb": checks.get("memory", {}).get("used_gb", -1),
                "mem_total_gb": checks.get("memory", {}).get("total_gb", -1),
                "disk_pct": checks.get("disk", {}).get("usage_pct", -1),
                "disk_free_gb": checks.get("disk", {}).get("free_gb", -1),
                "uptime": bot_info.get("uptime_human", "?"),
                "process_rss_mb": bot_info.get("process_rss_mb", -1),
                "overall_state": self_monitor.get_overall_state(),
                "error_rate_1h": self_monitor._error_rate_last_hour(),
                "consecutive_failures": self_monitor._consecutive_msg_failures,
                "telegram_ok": checks.get("network", {}).get("reachable", None),
            }
        except Exception:
            return {}

    def _get_evolution_progress() -> dict:
        try:
            with open(EVOLUTION_QUEUE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            return {
                "tasks": [
                    {"id": t.get("id", "?"), "name": t.get("name", "?"), "status": t.get("status", "unknown")}
                    for t in data.get("tasks", [])
                ],
                "completed_count": len(data.get("completed_tasks", [])),
                "total": len(data.get("tasks", [])),
                "current_index": data.get("current_task_index", 0),
            }
        except Exception:
            return {"tasks": [], "completed_count": 0, "total": 7, "current_index": 0}

    def _get_recent_messages_safe() -> list:
        with _stats_lock:
            return list(_recent_messages)

    @app.route("/api/stats")
    def api_stats():
        with _stats_lock:
            stats_snap = dict(_stats)

        total = stats_snap.get("total", 0)
        success_rate = round(stats_snap.get("success", 0) / total * 100, 1) if total > 0 else 0.0
        avg_ms = round(stats_snap.get("total_ms", 0) / total) if total > 0 else 0

        sys_stats = _get_system_stats()
        evolution = _get_evolution_progress()

        portfolio = {}
        try:
            from trading.portfolio_snapshot import get_snapshot as _pf_snap

            portfolio = _pf_snap()
        except Exception:
            portfolio = {}

        return jsonify({
            "timestamp": datetime.now().isoformat(),
            "messages": {
                "total": total,
                "success": stats_snap.get("success", 0),
                "failed": stats_snap.get("failed", 0),
                "success_rate": success_rate,
                "avg_ms": avg_ms,
            },
            "system": sys_stats,
            "recent_messages": _get_recent_messages_safe(),
            "evolution": evolution,
            "portfolio": portfolio,
        })

    _HTML = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bot Dashboard</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #0d1117; color: #c9d1d9; font-family: 'Segoe UI', monospace; font-size: 14px; }
  header { background: #161b22; border-bottom: 1px solid #30363d; padding: 12px 20px; display: flex; align-items: center; justify-content: space-between; }
  header h1 { font-size: 18px; color: #58a6ff; }
  #ts { font-size: 12px; color: #8b949e; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; padding: 16px; }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 14px; }
  .card h2 { font-size: 12px; color: #8b949e; text-transform: uppercase; letter-spacing: .6px; margin-bottom: 10px; }
  .big { font-size: 28px; font-weight: 700; color: #e6edf3; }
  .sub { font-size: 12px; color: #8b949e; margin-top: 2px; }
  .ok { color: #3fb950; } .warn { color: #d29922; } .bad { color: #f85149; }
  .bar-wrap { background: #21262d; border-radius: 4px; height: 8px; margin-top: 8px; }
  .bar { height: 8px; border-radius: 4px; transition: width .5s; }
  .section { padding: 0 16px 16px; }
  .section h2 { font-size: 13px; color: #8b949e; text-transform: uppercase; letter-spacing: .6px; margin-bottom: 8px; }
  table { width: 100%; border-collapse: collapse; }
  td, th { padding: 6px 8px; text-align: left; border-bottom: 1px solid #21262d; font-size: 13px; }
  th { color: #8b949e; font-weight: 500; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; }
  .badge.done { background: #1f6feb; color: #fff; }
  .badge.pending { background: #21262d; color: #8b949e; }
  .badge.ip { background: #388bfd22; color: #58a6ff; border: 1px solid #388bfd; }
  .msg-ok { color: #3fb950; } .msg-fail { color: #f85149; }
</style>
</head>
<body>
<header>
  <h1>🤖 Bot Performance Dashboard</h1>
  <span id="ts">Loading…</span>
</header>

<div class="grid" id="cards">
  <div class="card">
    <h2>Messages Processed</h2>
    <div class="big" id="msg-total">—</div>
    <div class="sub">Total since startup</div>
  </div>
  <div class="card">
    <h2>Success Rate</h2>
    <div class="big" id="msg-rate">—</div>
    <div class="bar-wrap"><div class="bar ok" id="rate-bar" style="width:0%"></div></div>
    <div class="sub" id="msg-ok-fail">— ok / — failed</div>
  </div>
  <div class="card">
    <h2>Avg Response Time</h2>
    <div class="big" id="avg-ms">—</div>
    <div class="sub">milliseconds</div>
  </div>
  <div class="card">
    <h2>Errors (1h)</h2>
    <div class="big" id="err-rate">—</div>
    <div class="sub">error events</div>
  </div>
  <div class="card">
    <h2>Memory Usage</h2>
    <div class="big" id="mem-pct">—</div>
    <div class="bar-wrap"><div class="bar" id="mem-bar" style="width:0%"></div></div>
    <div class="sub" id="mem-detail">—</div>
  </div>
  <div class="card">
    <h2>CPU</h2>
    <div class="big" id="cpu-pct">—</div>
    <div class="bar-wrap"><div class="bar" id="cpu-bar" style="width:0%"></div></div>
    <div class="sub" id="bot-uptime">Uptime: —</div>
  </div>
  <div class="card">
    <h2>Process Memory</h2>
    <div class="big" id="proc-mem">—</div>
    <div class="sub">MB RSS</div>
  </div>
  <div class="card">
    <h2>Bot State</h2>
    <div class="big" id="bot-state">—</div>
    <div class="sub" id="bot-sub">—</div>
  </div>
  <div class="card">
    <h2>Portfolio (cached)</h2>
    <div class="big" id="pf-age">—</div>
    <div class="sub" id="pf-total">OKX+DEX 参考 —</div>
  </div>
  <div class="card">
    <h2>SOL / Wallet</h2>
    <div class="big" id="pf-sol">—</div>
    <div class="sub" id="pf-wallet">—</div>
  </div>
</div>

<div class="section">
  <h2>Trading snapshot (后台轮询)</h2>
  <table id="pf-table">
    <thead><tr><th>来源</th><th>明细</th></tr></thead>
    <tbody id="pf-body"></tbody>
  </table>
</div>

<div class="section">
  <h2>Evolution Progress (7 Tasks)</h2>
  <table id="evo-table">
    <thead><tr><th>#</th><th>Task</th><th>Status</th></tr></thead>
    <tbody id="evo-body"></tbody>
  </table>
</div>

<div class="section">
  <h2>Recent Messages (last 10)</h2>
  <table>
    <thead><tr><th>Time</th><th>Message</th><th>Result</th><th>Duration</th></tr></thead>
    <tbody id="msg-body"></tbody>
  </table>
</div>

<script>
function colorPct(pct) {
  if (pct < 0) return 'ok';
  if (pct < 70) return 'ok';
  if (pct < 90) return 'warn';
  return 'bad';
}
function barColor(pct) {
  if (pct < 70) return '#3fb950';
  if (pct < 90) return '#d29922';
  return '#f85149';
}
function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
async function refresh() {
  try {
    const r = await fetch('/api/stats');
    const d = await r.json();

    document.getElementById('ts').textContent = 'Updated: ' + new Date(d.timestamp).toLocaleTimeString();

    // Messages
    const m = d.messages;
    document.getElementById('msg-total').textContent = m.total;
    document.getElementById('msg-rate').textContent = m.success_rate + '%';
    document.getElementById('msg-rate').className = 'big ' + (m.success_rate >= 90 ? 'ok' : m.success_rate >= 70 ? 'warn' : 'bad');
    document.getElementById('rate-bar').style.width = m.success_rate + '%';
    document.getElementById('rate-bar').style.background = barColor(m.success_rate);
    document.getElementById('msg-ok-fail').textContent = m.success + ' ok / ' + m.failed + ' failed';
    document.getElementById('avg-ms').textContent = m.avg_ms > 0 ? m.avg_ms + 'ms' : '—';

    // System
    const s = d.system;
    document.getElementById('err-rate').textContent = s.error_rate_1h !== undefined ? s.error_rate_1h : '—';
    document.getElementById('err-rate').className = 'big ' + (s.error_rate_1h > 20 ? 'bad' : s.error_rate_1h > 5 ? 'warn' : 'ok');

    if (s.mem_pct >= 0) {
      document.getElementById('mem-pct').textContent = s.mem_pct + '%';
      document.getElementById('mem-pct').className = 'big ' + colorPct(s.mem_pct);
      document.getElementById('mem-bar').style.width = s.mem_pct + '%';
      document.getElementById('mem-bar').style.background = barColor(s.mem_pct);
      document.getElementById('mem-detail').textContent = s.mem_used_gb > 0 ? s.mem_used_gb + ' / ' + s.mem_total_gb + ' GB' : '';
    }
    if (s.cpu_pct >= 0) {
      document.getElementById('cpu-pct').textContent = s.cpu_pct + '%';
      document.getElementById('cpu-pct').className = 'big ' + colorPct(s.cpu_pct);
      document.getElementById('cpu-bar').style.width = s.cpu_pct + '%';
      document.getElementById('cpu-bar').style.background = barColor(s.cpu_pct);
    }
    document.getElementById('bot-uptime').textContent = 'Uptime: ' + (s.uptime || '—');
    document.getElementById('proc-mem').textContent = s.process_rss_mb > 0 ? s.process_rss_mb : '—';

    const state = s.overall_state || 'healthy';
    const stateEl = document.getElementById('bot-state');
    stateEl.textContent = state.toUpperCase();
    stateEl.className = 'big ' + (state === 'healthy' ? 'ok' : state === 'degraded' ? 'warn' : 'bad');
    document.getElementById('bot-sub').textContent = s.telegram_ok === false ? '❌ Telegram unreachable' : s.consecutive_failures > 0 ? s.consecutive_failures + ' consecutive failures' : 'All systems normal';

    // Evolution
    const evo = d.evolution;
    const tbody = document.getElementById('evo-body');
    tbody.innerHTML = '';
    (evo.tasks || []).forEach(t => {
      const cls = t.status === 'completed' ? 'done' : t.status === 'in_progress' ? 'ip' : 'pending';
      const label = t.status === 'completed' ? '✅ Done' : t.status === 'in_progress' ? '🔄 Active' : '⏳ Pending';
      tbody.innerHTML += `<tr><td>${t.id}</td><td>${esc(t.name)}</td><td><span class="badge ${cls}">${label}</span></td></tr>`;
    });

    // Recent messages
    const mbody = document.getElementById('msg-body');
    mbody.innerHTML = '';
    const msgs = (d.recent_messages || []).slice().reverse();
    msgs.forEach(msg => {
      const cls = msg.success ? 'msg-ok' : 'msg-fail';
      const icon = msg.success ? '✓' : '✗';
      mbody.innerHTML += `<tr>
        <td>${esc(msg.ts)}</td>
        <td style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(msg.text)}</td>
        <td class="${cls}">${icon}</td>
        <td>${msg.duration_ms}ms</td>
      </tr>`;
    });

    const pf = d.portfolio || {};
    const age = pf.age_sec != null ? Math.round(pf.age_sec) + 's' : '—';
    document.getElementById('pf-age').textContent = age;
    const sp = pf.sol_price || 0;
    const okxEq = (pf.okx && pf.okx.total_equity_usd) || 0;
    const dexV = (pf.dex && pf.dex.total_value_sol) || 0;
    const dexUsd = sp > 0 ? dexV * sp : 0;
    document.getElementById('pf-total').textContent = '≈ $' + (okxEq + dexUsd).toLocaleString(undefined, {maximumFractionDigits: 0});
    document.getElementById('pf-sol').textContent = sp > 0 ? ('$' + sp.toFixed(2)) : '—';
    const w = pf.wallet || {};
    document.getElementById('pf-wallet').textContent = w.sol_bal != null ? (w.sol_bal + ' SOL · ' + (w.token_count || 0) + ' tok') : '—';

    const pbody = document.getElementById('pf-body');
    pbody.innerHTML = '';
    const rows = [];
    (pf.okx && pf.okx.positions || []).slice(0, 6).forEach(p => {
      rows.push(['OKX', esc(p.instId || '') + '  $' + (p.notionalUsd || 0)]);
    });
    (pf.dex && pf.dex.positions || []).slice(0, 6).forEach(p => {
      rows.push(['DEX', esc(p.symbol || '') + '  ' + (p.amount_sol || 0) + ' SOL']);
    });
    (pf.wallet && pf.wallet.tokens || []).slice(0, 6).forEach(t => {
      rows.push(['链上', esc(t.label || '') + '  ' + t.amount]);
    });
    if (!rows.length) {
      rows.push(['—', pf.last_error ? esc(String(pf.last_error).slice(0, 120)) : '等待后台同步…']);
    }
    rows.forEach(([a, b]) => { pbody.innerHTML += `<tr><td>${a}</td><td>${b}</td></tr>`; });
  } catch(e) {
    document.getElementById('ts').textContent = 'Error: ' + e.message;
  }
}
refresh();
setInterval(refresh, 2000);
</script>
</body>
</html>"""

    @app.route("/")
    def index():
        return render_template_string(_HTML)


def start_dashboard(host: str = "127.0.0.1", port: int = 8080) -> bool:
    """Start Flask dashboard in a background thread. Returns True if started."""
    global _server_started, _server_thread

    if not _flask_available:
        import logging
        logging.getLogger(__name__).warning(
            "dashboard: Flask not installed. Run: pip install flask"
        )
        return False

    if _server_started:
        return True

    # Suppress Flask/Werkzeug startup noise
    import logging
    logging.getLogger("werkzeug").setLevel(logging.WARNING)

    def _run():
        try:
            app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)
        except Exception as e:
            global _server_started
            _server_started = False
            logging.getLogger(__name__).error("Dashboard failed to start: %s", e)

    _server_thread = threading.Thread(target=_run, name="dashboard-server", daemon=True)
    _server_thread.start()
    _server_started = True  # Set after thread.start() so the thread is actually running

    logging.getLogger(__name__).info("Dashboard started at http://localhost:%d", port)
    return True


def get_stats_text() -> str:
    """Return formatted text stats for Telegram /dashboard command."""
    with _stats_lock:
        total = _stats.get("total", 0)
        success = _stats.get("success", 0)
        failed = _stats.get("failed", 0)
        total_ms = _stats.get("total_ms", 0)

    success_rate = round(success / total * 100, 1) if total > 0 else 0.0
    avg_ms = round(total_ms / total) if total > 0 else 0

    try:
        from self_monitor import self_monitor
        sys_info = self_monitor.get_status_report()
        state = self_monitor.get_overall_state()
        err_1h = self_monitor._error_rate_last_hour()
    except Exception:
        sys_info = ""
        state = "unknown"
        err_1h = 0

    evo = _get_evolution_progress() if _flask_available else {"completed_count": 0, "total": 7, "current_index": 0}

    lines = [
        "📊 **Performance Dashboard**\n",
        f"**Messages:** {total} total | {success_rate}% success",
        f"**Avg Response:** {avg_ms}ms",
        f"**Errors (1h):** {err_1h}",
        f"**Bot State:** {state.upper()}\n",
        f"**Evolution:** {evo.get('completed_count', 0)}/{evo.get('total', 7)} tasks done",
        f"**Dashboard URL:** http://localhost:8080\n",
    ]

    with _stats_lock:
        recent_snap = list(_recent_messages)
    if recent_snap:
        lines.append("**Recent Messages:**")
        for msg in recent_snap[-5:]:
            icon = "✅" if msg.get("success") else "❌"
            lines.append(f"  {icon} [{msg.get('ts', '?')}] {str(msg.get('text', ''))[:50]} ({msg.get('duration_ms', 0)}ms)")

    result = "\n".join(lines)
    # Truncate for Telegram's 4096 char limit
    if len(result) > 4000:
        result = result[:4000] + "\n... (truncated)"
    return result


# ── Telegram Gateway panel (text + keyboards for gateway/telegram_bot.py) ───

try:
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
except ImportError:  # pragma: no cover
    InlineKeyboardButton = None  # type: ignore[misc, assignment]
    InlineKeyboardMarkup = None  # type: ignore[misc, assignment]

# Callback prefix — keep under 64 bytes per Telegram rule
GW_CB = "gw"


def tg_gw_mode_label(mode: str) -> str:
    m = (mode or "paper").lower()
    if m == "live":
        return "🔴 真金实盘 (Live)"
    return "🔵 模拟盘 (Paper)"


def tg_gw_mode_banner(mode: str) -> str:
    return f"**当前交易模式：** {tg_gw_mode_label(mode)}"


def tg_gw_build_main_keyboard(mode: str):
    """Main menu: mode row (always visible) + home navigation."""
    if InlineKeyboardButton is None or InlineKeyboardMarkup is None:
        return None
    m = (mode or "paper").lower()
    paper_mark = "✓ " if m == "paper" else ""
    live_mark = "✓ " if m == "live" else ""
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"{paper_mark}🔵 模拟盘 (Paper)",
                    callback_data=f"{GW_CB}:mode:paper",
                ),
                InlineKeyboardButton(
                    f"{live_mark}🔴 真金实盘 (Live)",
                    callback_data=f"{GW_CB}:mode:live",
                ),
            ],
            [
                InlineKeyboardButton("📊 持仓", callback_data=f"{GW_CB}:pos"),
                InlineKeyboardButton("📈 策略", callback_data=f"{GW_CB}:strat"),
            ],
        ]
    )


def tg_gw_build_back_keyboard():
    if InlineKeyboardButton is None or InlineKeyboardMarkup is None:
        return None
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⬅️ 返回主页", callback_data=f"{GW_CB}:home")]]
    )


def tg_gw_build_positions_keyboard():
    """Positions screen: refresh + home."""
    if InlineKeyboardButton is None or InlineKeyboardMarkup is None:
        return None
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔄 刷新数据", callback_data=f"{GW_CB}:pos"),
            ],
            [InlineKeyboardButton("⬅️ 返回主页", callback_data=f"{GW_CB}:home")],
        ]
    )


def tg_gw_render_home_text(mode: str) -> str:
    banner = tg_gw_mode_banner(mode)
    m = (mode or "paper").lower()
    hint = (
        "模拟盘：展示与演练环境，链上/交易所只读或按你的本地配置；下单前请再确认。"
        if m == "paper"
        else "⚠️ 实盘：真实资金与真实成交。请谨慎操作。"
    )
    return (
        f"{banner}\n\n"
        "🏠 **交易面板 · 主页**\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{hint}\n\n"
        "请选择上方模式，或进入 **持仓** / **策略**。"
    )


def tg_gw_render_positions_loading_text(mode: str) -> str:
    return (
        f"{tg_gw_mode_banner(mode)}\n\n"
        "📊 **持仓**\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "⏳ 正在获取链上与交易所数据，请稍候…"
    )


def _tg_gw_format_snapshot(mode: str, snap: dict) -> str:
    lines = [
        tg_gw_mode_banner(mode),
        "",
        "📊 **持仓概览**",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    age = snap.get("age_sec")
    if age is not None:
        lines.append(f"_数据延迟：约 {int(age)}s（后台刷新后更新）_")
    lines.append("")

    w = snap.get("wallet") or {}
    if w.get("ok"):
        lines.append("**链上钱包**")
        lines.append(f"· 地址：`{w.get('pubkey_short', '?')}`")
        lines.append(f"· SOL：`{w.get('sol_bal', 0):.4f}`")
        lines.append(f"· Token 数：{w.get('token_count', 0)}")
        for t in (w.get("tokens") or [])[:8]:
            lines.append(f"  - {t.get('label', '?')}: {t.get('amount', 0):.6g}")
        lines.append("")
    elif w.get("error"):
        lines.append(f"**链上钱包：** 读取失败（{w.get('error', '')[:120]}）")
        lines.append("")
    else:
        lines.append("**链上钱包：** 未配置或不可用")
        lines.append("")

    ox = snap.get("okx") or {}
    if ox.get("ok"):
        lines.append("**OKX**")
        lines.append(f"· 权益 USD：`{ox.get('total_equity_usd', 0):.2f}`")
        lines.append(f"· 可用 USDT：`{ox.get('usdt_available', 0):.2f}`")
        pos = ox.get("positions") or []
        if pos:
            lines.append("· 合约持仓：")
            for p in pos[:10]:
                lines.append(
                    f"  - `{p.get('instId', '')}` pos={p.get('pos', 0)} "
                    f"upl={p.get('upl', 0):.4f}"
                )
        lines.append("")
    elif ox.get("has_keys") and ox.get("error"):
        lines.append(f"**OKX：** {ox.get('error', '')[:200]}")
        lines.append("")
    elif not ox.get("has_keys"):
        lines.append("**OKX：** 未配置 API 密钥")
        lines.append("")

    dex = snap.get("dex") or {}
    dpos = dex.get("positions") or []
    if dpos:
        lines.append("**DEX 持仓**")
        lines.append(
            f"· 合计投入 SOL：`{dex.get('total_invested_sol', 0):.4f}` | "
            f"估值 SOL：`{dex.get('total_value_sol', 0):.4f}`"
        )
        for p in dpos[:8]:
            sym = (p.get("symbol") or "?")[:10]
            lines.append(
                f"  - {sym} | PnL {float(p.get('pnl_pct', 0) or 0):+.1f}% | "
                f"{float(p.get('amount_sol', 0) or 0):.4f} SOL"
            )
        lines.append("")
    elif dex.get("error"):
        lines.append(f"**DEX：** {dex.get('error', '')[:200]}")
        lines.append("")

    sol_p = snap.get("sol_price") or 0
    if sol_p:
        chg = snap.get("sol_chg_pct") or 0
        lines.append(f"**SOL 参考价：** ${sol_p:.4f} ({chg:+.2f}% 24h)")

    err = snap.get("last_error") or ""
    if err:
        lines.append("")
        lines.append(f"⚠️ _部分数据源：{err[:300]}_")

    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3990] + "\n…"
    return text


def tg_gw_render_positions_text(mode: str, snap: dict | None) -> str:
    if not snap:
        return (
            f"{tg_gw_mode_banner(mode)}\n\n"
            "📊 **持仓**\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "暂无快照数据。请点击 **刷新数据**。"
        )
    return _tg_gw_format_snapshot(mode, snap)


def tg_gw_render_strategy_text(mode: str) -> str:
    m = (mode or "paper").lower()
    banner = tg_gw_mode_banner(mode)
    if m == "paper":
        body = (
            "📈 **策略 · 模拟盘**\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "· 建议：先在模拟盘完成信号验证与仓位规则演练。\n"
            "· 与主机器人 `/panel` 中的 Paper 模块配合使用。\n"
            "· 切换到实盘前请确认 API / 钱包权限与风控上限。"
        )
    else:
        body = (
            "📈 **策略 · 实盘**\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "⚠️ **真实资金** — 任何自动或手动下单均可能产生盈亏。\n\n"
            "· 确认 API Key 权限（只读 vs 交易）。\n"
            "· 建议启用单笔上限、日亏损熔断。\n"
            "· 详细执行逻辑见项目内 `trading/` 与 `live_trader` 配置。"
        )
    return f"{banner}\n\n{body}"
