"""
run.py — single-process entry: one asyncio event loop, Telegram polling, light background GC.

Usage: python run.py
"""
from __future__ import annotations

import asyncio
import gc
import logging
import sys

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def _setup_logging() -> None:
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT)
    else:
        root.setLevel(logging.INFO)


def _install_exception_hooks() -> None:
    def _excepthook(exc_type, exc, tb):
        logging.critical("Uncaught exception", exc_info=(exc_type, exc, tb))
        sys.__excepthook__(exc_type, exc, tb)

    sys.excepthook = _excepthook


def _async_exception_handler(loop, context):
    msg = context.get("message", context)
    exc = context.get("exception")
    if exc:
        logging.error("asyncio: %s", msg, exc_info=exc)
    else:
        logging.error("asyncio: %s", msg)


async def _idle_maintenance() -> None:
    while True:
        await asyncio.sleep(600.0)
        gc.collect()


def _green_banner() -> None:
    print("\033[32mSystem Started Cleanly\033[0m", flush=True)


async def _amain() -> None:
    _setup_logging()
    _install_exception_hooks()
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(_async_exception_handler)
    asyncio.create_task(_idle_maintenance(), name="idle_gc")

    import bot

    await bot.async_main(on_system_ready=_green_banner)


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
