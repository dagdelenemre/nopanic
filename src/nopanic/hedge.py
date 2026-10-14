"""Hedged requests: trade a little extra load for much better tail latency.

If a call hasn't finished after *delay* seconds, fire a second identical call
and take whichever finishes first ("The Tail at Scale", Dean & Barroso, 2013).
Ideal for high-variance backends — LLM APIs, cross-region reads, flaky CDNs.

Only meaningful for idempotent operations, and async-only by nature.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from ._core import ExcFilter, Policy, exc_matches, positive_int, positive_number
from .events import emit

__all__ = ["hedge"]


class hedge(Policy):
    """Race duplicate calls against a slow first attempt.

    Args:
        delay: Seconds to wait before launching each additional attempt.
        max_hedges: Maximum number of *extra* attempts (so total concurrent
            attempts is at most ``1 + max_hedges``).
        on: Which failures are worth hedging. An attempt failing with a
            matching exception lets the others continue (and launches the
            next hedge at once); any other exception is permanent: it
            cancels the remaining attempts and propagates immediately, so a
            400-style error is not repeated ``max_hedges`` more times.
        name: Diagnostic name carried by this policy's events.

    The first successful attempt wins and all others are cancelled. If every
    attempt fails, the last failure is re-raised.
    """

    __slots__ = ("delay", "max_hedges", "name", "on")

    def __init__(
        self,
        *,
        delay: float,
        max_hedges: int = 1,
        on: ExcFilter = Exception,
        name: str | None = None,
    ) -> None:
        self.max_hedges = positive_int("max_hedges", max_hedges)
        self.delay = positive_number("delay", delay, allow_zero=True)
        self.on = on
        self.name = name

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        raise TypeError("hedge() only supports async functions")

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        tasks: set[asyncio.Task[Any]] = {asyncio.ensure_future(fn(*args, **kwargs))}
        hedges_left = self.max_hedges
        last_exc: BaseException | None = None
        try:
            while True:
                wait_timeout = self.delay if hedges_left > 0 else None
                done, _ = await asyncio.wait(
                    tasks, timeout=wait_timeout, return_when=asyncio.FIRST_COMPLETED
                )
                if not done:
                    # Nothing finished within the delay: launch a hedge.
                    emit("hedge.launched", "hedge", self.name, fn, delay=self.delay)
                    tasks.add(asyncio.ensure_future(fn(*args, **kwargs)))
                    hedges_left -= 1
                    continue
                for task in done:
                    tasks.discard(task)
                    if task.cancelled():  # cancelled from elsewhere: no outcome
                        continue
                    exc = task.exception()
                    if exc is None:
                        return task.result()
                    if not exc_matches(exc, self.on):
                        raise exc  # permanent: no point racing more copies
                    last_exc = exc
                if not tasks:
                    if hedges_left > 0:
                        # Everything so far failed fast; hedge immediately.
                        emit("hedge.launched", "hedge", self.name, fn, delay=0.0)
                        tasks.add(asyncio.ensure_future(fn(*args, **kwargs)))
                        hedges_left -= 1
                    elif last_exc is not None:
                        raise last_exc
                    else:
                        raise asyncio.CancelledError("every hedged attempt was cancelled")
        finally:
            for task in tasks:
                task.cancel()
            if tasks:
                # Reap the losers: nothing keeps running behind the caller's
                # back and no "exception was never retrieved" warnings leak.
                await asyncio.gather(*tasks, return_exceptions=True)
