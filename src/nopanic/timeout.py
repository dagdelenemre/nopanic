"""Timeout policy: bound how long a single call may take."""

from __future__ import annotations

import asyncio
import contextvars
import sys
import threading
from collections.abc import Callable
from typing import Any

from ._core import Policy, positive_number
from .events import emit

__all__ = ["timeout"]


class timeout(Policy):
    """Fail with :class:`TimeoutError` if the call takes longer than *seconds*.

    Async functions are cancelled cleanly via the event loop. Sync functions
    are executed on a daemon worker thread and **abandoned** on timeout —
    Python cannot kill a running thread, so the underlying work may continue
    in the background even after the caller has received ``TimeoutError``.
    Prefer the async path (or the callee's own timeout parameter, e.g. an
    HTTP client's) when abandonment is not acceptable. The worker runs in a
    copy of the caller's context, so ``contextvars`` (tracing spans, request
    IDs, structured-logging fields) are visible inside the call.

    Only an expiry of *this* timeout raises a new ``TimeoutError``. A
    ``TimeoutError`` raised by the wrapped function itself (its own I/O
    timeout, say) propagates unchanged, like any other user exception.
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
        context = contextvars.copy_context()

        def target() -> None:
            try:
                result.append(context.run(fn, *args, **kwargs))
            except BaseException as exc:
                error.append(exc)
            finally:
                done.set()

        worker = threading.Thread(
            target=target, name=f"nopanic-timeout-{fn.__name__}", daemon=True
        )
        worker.start()
        if not done.wait(self.seconds):
            emit("timeout.expired", "timeout", seconds=self.seconds, function=fn.__qualname__)
            raise TimeoutError(
                f"{fn.__qualname__} did not finish within {self.seconds}s "
                "(worker thread abandoned, not killed)"
            )
        if error:
            raise error[0]
        return result[0]

    def _expired(self, fn: Callable[..., Any]) -> TimeoutError:
        emit("timeout.expired", "timeout", seconds=self.seconds, function=fn.__qualname__)
        return TimeoutError(f"{fn.__qualname__} did not finish within {self.seconds}s")

    if sys.version_info >= (3, 11):

        async def _run_async(
            self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
        ) -> Any:
            # asyncio.timeout runs the call in the caller's own task (no extra
            # task, context shared) and tells an expiry of ours apart from a
            # TimeoutError the function raised itself: since 3.11 both are the
            # same class, and only the former may be replaced.
            scope = asyncio.timeout(self.seconds)
            try:
                async with scope:
                    return await fn(*args, **kwargs)
            except TimeoutError as exc:
                if not scope.expired():
                    raise
                raise self._expired(fn) from exc

    else:

        async def _run_async(
            self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
        ) -> Any:
            try:
                return await asyncio.wait_for(fn(*args, **kwargs), timeout=self.seconds)
            except asyncio.TimeoutError as exc:  # not the builtin TimeoutError on 3.10
                raise self._expired(fn) from exc
