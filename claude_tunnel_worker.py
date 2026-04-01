"""
Persistent child process: stdin/stdout JSONL protocol for Claude ``-p`` turns.

Parent (``claude_cli_tunnel.PersistentClaudePipe``) spawns this module with
``python -u -m claude_tunnel_worker``. Each line is one JSON object.

Request (single line):
  {\"id\": <int>, \"prompt_path\": <utf-8 path>, \"resume\": \"\", \"timeout_sec\": 120, \"wall_cap_sec\": 120}

Ping:
  {\"op\": \"ping\", \"id\": 0}

Response (single line):
  {\"id\": <int>, \"ok\": true, \"text\": \"\", \"err\": \"\", \"sid\": null|string}
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path


def _emit(obj: dict) -> None:
    sys.stdout.write(json.dumps(obj, ensure_ascii=False, default=str) + "\n")
    sys.stdout.flush()


async def _one_turn(msg: dict) -> None:
    req_id = int(msg.get("id", 0))
    path = str(msg.get("prompt_path") or "").strip()
    if not path or not Path(path).is_file():
        _emit({"id": req_id, "ok": False, "error": "missing_prompt_path"})
        return
    try:
        combined = Path(path).read_text(encoding="utf-8")
    except OSError as e:
        _emit({"id": req_id, "ok": False, "error": str(e)[:500]})
        return
    resume = (msg.get("resume") or "").strip() or None
    try:
        timeout_sec = float(msg.get("timeout_sec", 120))
        wall_cap_sec = float(msg.get("wall_cap_sec", timeout_sec))
    except (TypeError, ValueError):
        timeout_sec, wall_cap_sec = 120.0, 120.0

    try:
        import claude_agent

        cwd = claude_agent.BOT_PROJECT_DIR
        text, err, sid = await claude_agent.async_claude_code_prompt(
            combined,
            cwd=cwd,
            resume=resume,
            timeout_sec=timeout_sec,
            wall_cap_sec=wall_cap_sec,
        )
        _emit(
            {
                "id": req_id,
                "ok": True,
                "text": text or "",
                "err": err or "",
                "sid": sid,
            }
        )
    except Exception as e:
        _emit({"id": req_id, "ok": False, "error": str(e)[:800]})


def main() -> None:
    os.environ.setdefault("PYTHONUTF8", "1")
    if hasattr(sys.stdin, "reconfigure"):
        try:
            sys.stdin.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            _emit({"id": -1, "ok": False, "error": "bad_json"})
            continue
        if msg.get("op") == "ping":
            _emit({"id": int(msg.get("id", 0)), "ok": True, "op": "pong"})
            continue
        asyncio.run(_one_turn(msg))


if __name__ == "__main__":
    main()
