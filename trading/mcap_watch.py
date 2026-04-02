"""
User-defined market-cap watches: natural-language registration, DexScreener polling,
Telegram alerts on threshold cross (with hysteresis, one-shot fire per watch).

Storage: trading/_mcap_watches.json
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import secrets
import time
from dataclasses import asdict, dataclass
from typing import Any, Literal

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STORE_PATH = os.path.join(BASE_DIR, "_mcap_watches.json")

Direction = Literal["above", "below"]

# Crossing hysteresis: "above" fires when mcap >= threshold and was < threshold * factor
_CROSS_HYSTERESIS = float(os.environ.get("MCAP_WATCH_HYSTERESIS", "0.985") or 0.985)
POLL_INTERVAL_SEC = float(os.environ.get("MCAP_WATCH_POLL_SEC", "90") or 90)
PENDING_TTL_SEC = 600.0

_REMINDER_RE = re.compile(
    r"(提醒|通知|告警|提醒我|告诉我|发消息|推送|"
    r"watch|alert|notify|ping\s*me|"
    r"监控|盯着|留意|关注)",
    re.I,
)
_MCAP_CTX_RE = re.compile(
    r"(市值|mcap|market\s*cap|fdv|流通市值)",
    re.I,
)
_ABOVE_RE = re.compile(
    r"(?:突破|超过|达到|涨到|升至|上破|高于|"
    r"above|cross(?:es)?|reach(?:es)?|hits?|>\s*|≥\s*)"
    r"\s*([\d][\d,._]*)\s*"
    r"(mil|million|m\b|万|百万|千万|亿|k\b|K\b|bn|b\b)?",
    re.I,
)
_BELOW_RE = re.compile(
    r"(?:跌破|跌穿|低于|下破|掉到|降至|"
    r"below|under|drop(?:s)?\s*to|falls?\s*to|<\s*|≤\s*)"
    r"\s*([\d][\d,._]*)\s*"
    r"(mil|million|m\b|万|百万|千万|亿|k\b|K\b|bn|b\b)?",
    re.I,
)
# Standalone: "12m mcap" / "mcap 12 mil"
_STANDALONE_MCAP_NUM_RE = re.compile(
    r"([\d][\d,._]*)\s*(mil|million|m\b|万|百万|千万|亿|k\b|K\b|bn|b\b)\s*(?:市值|mcap|fdv)?",
    re.I,
)
_TOKEN_QUOTED_RE = re.compile(r'["\']([^"\']{1,48})["\']')
_TOKEN_THIS_RE = re.compile(
    r"当\s*([^\s当，。,.]{1,32}?)\s*(?:这个|那個|那个)?\s*(?:token|币|幣)",
    re.I,
)
_TOKEN_AFTER_RE = re.compile(
    r"(?:token|币|幣)\s*[：:是为]?\s*([A-Za-z0-9._-]{2,32})",
    re.I,
)
_MINT_SOL_RE = re.compile(r"\b([1-9A-HJ-NP-Za-km-z]{32,44})\b")
_MINT_EVM_RE = re.compile(r"\b(0x[a-fA-F0-9]{40})\b")


def _atomic_save(path: str, data: Any) -> None:
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=1)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except OSError as e:
        logger.warning("mcap_watch save failed: %s", e)


def _load_store() -> dict[str, Any]:
    try:
        with open(STORE_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
            return raw if isinstance(raw, dict) else {"watches": []}
    except (FileNotFoundError, json.JSONDecodeError):
        return {"watches": []}


def _normalize_num(s: str) -> float:
    return float(s.replace(",", "").replace("_", ""))


def _unit_to_mult(unit: str | None) -> float:
    if not unit:
        return 1.0
    u = unit.strip().lower()
    if u in ("mil", "million", "m"):
        return 1e6
    if u in ("bn", "b"):
        return 1e9
    if u in ("k",):
        return 1e3
    if u == "万":
        return 1e4
    if u == "百万":
        return 1e6
    if u == "千万":
        return 1e7
    if u == "亿":
        return 1e8
    return 1.0


def parse_usd_amount(num_s: str, unit: str | None) -> float | None:
    try:
        n = _normalize_num(num_s)
    except ValueError:
        return None
    return max(0.0, n * _unit_to_mult(unit))


@dataclass
class ParsedWatchIntent:
    token_query: str
    threshold_usd: float
    direction: Direction
    anchor_usd: float | None
    source_text: str


_MONITOR_SYM_RE = re.compile(
    r"(?:监控|盯住|盯着|watch|track)\s+([A-Za-z][A-Za-z0-9]{1,15})\b",
    re.I,
)


def _extract_mint_or_symbol(text: str) -> str | None:
    t = (text or "").strip()
    m = _MINT_SOL_RE.search(t) or _MINT_EVM_RE.search(t)
    if m:
        return m.group(1)
    mq = _TOKEN_QUOTED_RE.search(t)
    if mq:
        return mq.group(1).strip()
    mt = _TOKEN_THIS_RE.search(t)
    if mt:
        q = mt.group(1).strip()
        if q and not re.fullmatch(r"[\d,.]+", q):
            return q
    ta = _TOKEN_AFTER_RE.search(t)
    if ta:
        return ta.group(1).strip()
    # "punch token" / "watch PUNCH"
    m2 = re.search(
        r"\b([A-Za-z][A-Za-z0-9]{1,15})\s+(?:token|币|幣)\b",
        t,
        re.I,
    )
    if m2:
        return m2.group(1)
    mm = _MONITOR_SYM_RE.search(t)
    if mm:
        return mm.group(1).strip()
    return None


def _pick_threshold_and_direction(text: str) -> tuple[float, Direction] | None:
    t = text or ""
    below_m = _BELOW_RE.search(t)
    above_m = _ABOVE_RE.search(t)
    if below_m and (not above_m or below_m.start() < above_m.start()):
        amt = parse_usd_amount(below_m.group(1), below_m.group(2))
        return (amt, "below") if amt else None
    if above_m:
        amt = parse_usd_amount(above_m.group(1), above_m.group(2))
        return (amt, "above") if amt else None
    sm = _STANDALONE_MCAP_NUM_RE.search(t)
    if sm and _MCAP_CTX_RE.search(t):
        amt = parse_usd_amount(sm.group(1), sm.group(2))
        if amt:
            # Default to "above" when user says "12m mcap alert"
            if re.search(r"跌破|低于|below|under", t, re.I):
                return amt, "below"
            return amt, "above"
    return None


def parse_mcap_watch_intent(text: str) -> ParsedWatchIntent | None:
    """
    Heuristic NL → structured watch. Returns None if this does not look like a mcap alert request.
    """
    raw = (text or "").strip()
    if len(raw) < 6:
        return None

    has_ctx = bool(_REMINDER_RE.search(raw) or _MCAP_CTX_RE.search(raw))
    if not has_ctx:
        return None

    td = _pick_threshold_and_direction(raw)
    if not td:
        return None
    threshold_usd, direction = td
    if threshold_usd <= 0:
        return None

    token_query = _extract_mint_or_symbol(raw)
    if not token_query:
        # e.g. "市值突破12m时提醒我" with no symbol — reject
        return None

    anchor_usd: float | None = None
    # Optional "10mil 市值" reference (first amount before 市值 that's not the threshold span)
    for m in re.finditer(
        r"([\d][\d,._]*)\s*(mil|million|m\b|万|百万|千万|亿|k\b|K\b)\s*市值",
        raw,
        re.I,
    ):
        a = parse_usd_amount(m.group(1), m.group(2))
        if a and abs(a - threshold_usd) > max(1000.0, threshold_usd * 0.01):
            anchor_usd = a
            break

    return ParsedWatchIntent(
        token_query=token_query.strip(),
        threshold_usd=threshold_usd,
        direction=direction,
        anchor_usd=anchor_usd,
        source_text=raw[:500],
    )


@dataclass
class WatchRecord:
    id: str
    user_id: int
    chat_id: int
    address: str
    symbol: str
    chain: str
    pair_url: str
    direction: Direction
    threshold_usd: float
    fired: bool
    created_ts: float
    last_mcap: float
    last_check_ts: float
    label: str
    prev_mcap: float | None  # for hysteresis

    def to_json(self) -> dict[str, Any]:
        d = asdict(self)
        return d

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> WatchRecord:
        return cls(
            id=str(d.get("id") or ""),
            user_id=int(d.get("user_id") or 0),
            chat_id=int(d.get("chat_id") or 0),
            address=str(d.get("address") or ""),
            symbol=str(d.get("symbol") or "?"),
            chain=str(d.get("chain") or ""),
            pair_url=str(d.get("pair_url") or ""),
            direction=(
                "below"
                if str(d.get("direction") or "").lower() == "below"
                else "above"
            ),
            threshold_usd=float(d.get("threshold_usd") or 0),
            fired=bool(d.get("fired")),
            created_ts=float(d.get("created_ts") or 0),
            last_mcap=float(d.get("last_mcap") or 0),
            last_check_ts=float(d.get("last_check_ts") or 0),
            label=str(d.get("label") or "")[:500],
            prev_mcap=(
                float(d["prev_mcap"])
                if d.get("prev_mcap") is not None
                else None
            ),
        )


def load_watches() -> list[WatchRecord]:
    data = _load_store()
    watches = data.get("watches") or []
    out: list[WatchRecord] = []
    for w in watches:
        if isinstance(w, dict):
            try:
                out.append(WatchRecord.from_json(w))
            except (TypeError, ValueError):
                continue
    return out


def save_watches(records: list[WatchRecord]) -> None:
    _atomic_save(
        STORE_PATH,
        {"watches": [w.to_json() for w in records], "updated_ts": time.time()},
    )


def add_watch(rec: WatchRecord) -> None:
    all_w = load_watches()
    all_w.append(rec)
    save_watches(all_w)


def delete_watch(watch_id: str, user_id: int) -> bool:
    all_w = load_watches()
    new = [w for w in all_w if not (w.id == watch_id and w.user_id == user_id)]
    if len(new) == len(all_w):
        return False
    save_watches(new)
    return True


def delete_watch_by_user_index(user_id: int, one_based_index: int) -> bool:
    if one_based_index < 1:
        return False
    mine = sorted(
        [w for w in load_watches() if w.user_id == user_id],
        key=lambda x: x.created_ts,
    )
    if one_based_index > len(mine):
        return False
    target = mine[one_based_index - 1]
    return delete_watch(target.id, user_id)


def update_watch(rec: WatchRecord) -> None:
    all_w = load_watches()
    for i, w in enumerate(all_w):
        if w.id == rec.id:
            all_w[i] = rec
            save_watches(all_w)
            return
    all_w.append(rec)
    save_watches(all_w)


def list_watches_for_user(user_id: int) -> list[WatchRecord]:
    return sorted(
        [w for w in load_watches() if w.user_id == user_id],
        key=lambda x: x.created_ts,
    )


def has_active_duplicate(
    user_id: int, address: str, threshold_usd: float, direction: Direction
) -> bool:
    addr_l = (address or "").lower()
    for w in load_watches():
        if w.user_id != user_id or w.fired:
            continue
        if (w.address or "").lower() != addr_l:
            continue
        if w.direction != direction:
            continue
        if abs(w.threshold_usd - threshold_usd) < 1.0:
            return True
    return False


def parse_cancel_intent(text: str) -> int | None:
    raw = (text or "").strip()
    if not raw:
        return None
    m3 = re.search(r"/mcap_unwatch(?:@\w+)?\s+(\d+)", raw, re.I)
    if m3:
        return int(m3.group(1))
    if not re.search(
        r"(取消|删除|移除|删掉|停止).{0,16}(提醒|监控|watch|任务|市值)",
        raw,
        re.I,
    ):
        return None
    m = re.search(
        r"(?:提醒|监控|watch|任务|市值提醒)\s*#?\s*(\d+)\s*$",
        raw,
        re.I,
    )
    if m:
        return int(m.group(1))
    m2 = re.search(
        r"(?:取消|删除|移除|删掉)\s*(?:市值提醒|提醒|监控|watch)?\s*#?\s*(\d+)\s*$",
        raw,
        re.I,
    )
    if m2:
        return int(m2.group(1))
    m4 = re.search(r"(?:市值提醒|提醒)\s*(\d+)\s*$", raw, re.I)
    if m4:
        return int(m4.group(1))
    return None


def parse_list_intent(text: str) -> bool:
    raw = (text or "").strip()
    low = raw.lower()
    parts = low.split()
    head = parts[0] if parts else low
    if head in ("/mcap_watches", "/watch_mcap") or head.startswith(
        ("/mcap_watches@", "/watch_mcap@")
    ):
        return True
    if low in ("/市值提醒",):
        return True
    if re.search(
        r"(市值提醒|mcap\s*watch|市值监控)\s*(列表|清单|有哪些)?",
        text or "",
        re.I,
    ):
        return True
    if re.search(r"有哪些\s*市值提醒|列出\s*市值提醒|list\s+mcap\s+watches", text or "", re.I):
        return True
    return False


async def dexscreener_search_candidates(query: str, limit: int = 5) -> list[dict[str, Any]]:
    """Return deduped token candidates (by base address), best liquidity first."""
    q = (query or "").strip()
    if not q:
        return []
    if re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", q) or re.fullmatch(
        r"0x[a-fA-F0-9]{40}", q, re.I
    ):
        import dex_trader as dex

        info = await dex.lookup_token(q)
        if not info:
            return []
        return [
            {
                "address": info["address"],
                "symbol": info.get("symbol", "?"),
                "name": info.get("name", ""),
                "chain": info.get("chain", ""),
                "pair_url": info.get("pair_url", ""),
                "liquidity_usd": float(info.get("liquidity_usd") or 0),
                "mcap": float(info.get("mcap") or 0),
            }
        ]

    try:
        import httpx

        async with httpx.AsyncClient(timeout=18.0) as client:
            resp = await client.get(
                "https://api.dexscreener.com/latest/dex/search",
                params={"q": q},
            )
            if resp.status_code != 200:
                return []
            data = resp.json()
            pairs = data.get("pairs") or []
    except Exception as e:
        logger.debug("dexscreener search failed: %s", e)
        return []

    by_addr: dict[str, dict[str, Any]] = {}
    for p in pairs:
        base = p.get("baseToken") or {}
        addr = base.get("address")
        if not addr:
            continue
        liq = float((p.get("liquidity") or {}).get("usd") or 0)
        mcap = float(p.get("marketCap") or p.get("fdv") or 0)
        prev = by_addr.get(addr)
        if not prev or liq > float(prev.get("liquidity_usd") or 0):
            by_addr[addr] = {
                "address": str(addr),
                "symbol": str(base.get("symbol") or "?"),
                "name": str(base.get("name") or ""),
                "chain": str(p.get("chainId") or ""),
                "pair_url": str(p.get("url") or ""),
                "liquidity_usd": liq,
                "mcap": mcap,
            }
    ranked = sorted(
        by_addr.values(),
        key=lambda x: float(x.get("liquidity_usd") or 0),
        reverse=True,
    )
    return ranked[:limit]


async def fetch_mcap_for_address(address: str) -> tuple[float, dict[str, Any]]:
    """Returns (mcap_usd, meta) via dex_trader.lookup_token."""
    import dex_trader as dex

    info = await dex.lookup_token(address)
    if not info:
        return 0.0, {}
    mcap = float(info.get("mcap") or 0)
    return mcap, info


_stop_event: asyncio.Event | None = None


def configure_stop_event(ev: asyncio.Event | None) -> None:
    global _stop_event
    _stop_event = ev


async def run_watch_loop(bot: Any, *, interval_sec: float | None = None) -> None:
    """
    Poll all unfired watches; send Telegram message via bot.send_message on trigger.
    ``bot`` is telegram.Bot instance.
    """
    global _stop_event
    if _stop_event is None:
        _stop_event = asyncio.Event()
    iv = interval_sec if interval_sec is not None else POLL_INTERVAL_SEC
    logger.info("mcap_watch: loop started (interval=%.0fs)", iv)
    while not _stop_event.is_set():
        try:
            await _poll_once(bot)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("mcap_watch: poll pass failed")
        try:
            await asyncio.wait_for(_stop_event.wait(), timeout=max(15.0, iv))
            break
        except asyncio.TimeoutError:
            pass
    logger.info("mcap_watch: loop stopped")


def stop_watch_loop() -> None:
    global _stop_event
    if _stop_event is not None:
        _stop_event.set()


async def _poll_once(bot: Any) -> None:
    watches = [w for w in load_watches() if not w.fired]
    if not watches:
        return
    # Stagger DexScreener calls lightly
    for w in watches:
        if _stop_event and _stop_event.is_set():
            return
        try:
            mcap, info = await fetch_mcap_for_address(w.address)
        except Exception:
            logger.debug("mcap fetch failed id=%s", w.id, exc_info=True)
            continue
        w.last_check_ts = time.time()
        meta_mcap = mcap
        if meta_mcap <= 0 and info:
            meta_mcap = float(info.get("mcap") or 0)

        fire = False
        if meta_mcap <= 0:
            w.prev_mcap = meta_mcap
            w.last_mcap = meta_mcap
            update_watch(w)
            continue

        if w.direction == "above":
            under = w.prev_mcap is None or w.prev_mcap < w.threshold_usd * _CROSS_HYSTERESIS
            if meta_mcap >= w.threshold_usd and under:
                fire = True
        else:
            over = w.prev_mcap is None or w.prev_mcap > w.threshold_usd * (2.0 - _CROSS_HYSTERESIS)
            if meta_mcap <= w.threshold_usd and over:
                fire = True

        w.prev_mcap = meta_mcap
        w.last_mcap = meta_mcap
        update_watch(w)

        if not fire:
            continue

        w.fired = True
        update_watch(w)
        sym = w.symbol
        url = w.pair_url or f"https://dexscreener.com/{w.chain}/{w.address}"
        dir_zh = "突破" if w.direction == "above" else "跌破"
        body = (
            f"📈 市值提醒\n\n"
            f"代币 {sym} ({w.chain})\n"
            f"{dir_zh}阈值 ${format_usd_compact(w.threshold_usd)} USD\n"
            f"当前 DexScreener 参考市值 ${format_usd_compact(meta_mcap)} USD\n"
            f"{url}"
        )
        try:
            await bot.send_message(
                chat_id=w.chat_id,
                text=body[:4096],
                disable_web_page_preview=False,
            )
        except Exception as e:
            logger.warning("mcap_watch notify failed: %s", e)
            w.fired = False
            update_watch(w)


def format_usd_compact(n: float) -> str:
    if n >= 1e9:
        return f"{n/1e9:.2f}B"
    if n >= 1e6:
        return f"{n/1e6:.2f}M"
    if n >= 1e3:
        return f"{n/1e3:.1f}K"
    return f"{n:,.0f}"


def make_watch_record(
    *,
    user_id: int,
    chat_id: int,
    candidate: dict[str, Any],
    parsed: ParsedWatchIntent,
) -> WatchRecord:
    return WatchRecord(
        id=secrets.token_hex(6),
        user_id=user_id,
        chat_id=chat_id,
        address=str(candidate.get("address") or ""),
        symbol=str(candidate.get("symbol") or "?"),
        chain=str(candidate.get("chain") or ""),
        pair_url=str(candidate.get("pair_url") or ""),
        direction=parsed.direction,
        threshold_usd=parsed.threshold_usd,
        fired=False,
        created_ts=time.time(),
        last_mcap=float(candidate.get("mcap") or 0),
        last_check_ts=0.0,
        label=parsed.source_text[:300],
        prev_mcap=None,
    )


def format_watch_list(user_id: int) -> str:
    rows = list_watches_for_user(user_id)
    if not rows:
        return (
            "📭 暂无市值提醒。\n\n"
            "示例：当 punch 这个 token 约 10M 市值，突破 12M 时提醒我。\n"
            "列表：/mcap_watches\n"
            "取消：「取消市值提醒 1」或 /mcap_unwatch 1"
        )
    lines = ["📋 你的市值提醒（DexScreener 参考市值）\n"]
    for i, w in enumerate(rows, start=1):
        st = "✅已触发" if w.fired else "⏳监控中"
        dir_zh = "≥" if w.direction == "above" else "≤"
        lines.append(
            f"{i}. {w.symbol} {st} {dir_zh}${format_usd_compact(w.threshold_usd)} · {w.chain}\n"
            f"   id:{w.id}"
        )
    lines.append("\n取消第 N 条：「取消市值提醒 N」或 /mcap_unwatch N")
    return "\n".join(lines)


def parsed_watch_from_dict(d: dict[str, Any]) -> ParsedWatchIntent:
    d2 = d or {}
    dr = str(d2.get("direction") or "above").lower()
    direction: Direction = "below" if dr == "below" else "above"
    au = d2.get("anchor_usd")
    return ParsedWatchIntent(
        token_query=str(d2.get("token_query") or ""),
        threshold_usd=float(d2.get("threshold_usd") or 0),
        direction=direction,
        anchor_usd=float(au) if au is not None else None,
        source_text=str(d2.get("source_text") or ""),
    )
