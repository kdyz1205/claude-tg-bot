"""Python skills package: ``BaseSkill``, dynamic loader, and ``sk_*`` implementations."""

from skills.base_skill import BaseSkill, SkillTimeoutError

__all__ = ["BaseSkill", "SkillTimeoutError", "skill_runtime"]

# Lazy: importing skill_runtime pulls importlib helpers only (not every sk_*).
def __getattr__(name: str):
    if name == "skill_runtime":
        import skills.skill_runtime as skill_runtime

        return skill_runtime
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
