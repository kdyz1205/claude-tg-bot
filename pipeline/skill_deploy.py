"""
科研 → 战场（兼容层）：委托 ``auto_research.deploy_to_battlefield``。
"""

from __future__ import annotations


def finalize_promoted_skill(script_path: str, skill_id: str) -> str:
    from auto_research import deploy_to_battlefield

    ok, msg = deploy_to_battlefield(
        script_path,
        skill_id=skill_id,
        sharpe=0.0,
        send_telegram=False,
    )
    return msg if ok else f"fail:{msg}"


def build_promote_telegram_message(skill_id: str, sharpe: float, extra: str = "") -> str:
    return (
        f"⚔️ *AI 自主研发成功：新武器* `{skill_id}.py` *已通过回测并强制部署实盘！*\n"
        f"Sharpe≈*{sharpe:.2f}*\n"
        f"{extra}"[:3900]
    )
