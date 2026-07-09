"""Timeout policy: bound how long a single call may take."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from typing import Any

from ._core import Policy, positive_number

__all__ = ["timeout"]


class timeout(Policy):
    """Fail with :class:`TimeoutError` if the call takes longer than *seconds*.

    Async functions are cancelled cleanly via the event loop. Sync functions
    are executed on a daemon worker thread and **abandoned** on timeout —
    Python cannot kill a running thread, so the underlying work may continue
    in the background even after the caller has received ``TimeoutError``.
    Prefer the async path (or the callee's own timeout parameter, e.g. an
    HTTP client's) when abandonment is not acceptable.
    """

    __slots__ = ("seconds",)

    def __init__(self, seconds: float) -> None:
        self.seconds = positive_number("seconds", seconds)

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        result: list[Any] = []
        error: list[BaseException] = []
        done = threading.Event()

        def target() -> None:
            try:
                result.append(fn(*args, **kwargs))
            except BaseException as exc:
                error.append(exc)
            finally:
                done.set()

        worker = threading.Thread(
            target=target, name=f"nopanic-timeout-{fn.__name__}", daemon=True
        )
        worker.start()
        if not done.wait(self.seconds):
            raise TimeoutError(
                f"{fn.__qualname__} did not finish within {self.seconds}s "
                "(worker thread abandoned, not killed)"
            )
        if error:
            raise error[0]
        return result[0]

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        try:
            return await asyncio.wait_for(fn(*args, **kwargs), timeout=self.seconds)
        except asyncio.TimeoutError as exc:  # distinct from TimeoutError on 3.10
            raise TimeoutError(
                f"{fn.__qualname__} did not finish within {self.seconds}s"
            ) from exc
