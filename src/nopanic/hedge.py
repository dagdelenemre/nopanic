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

from ._core import Policy, positive_number

__all__ = ["hedge"]


class hedge(Policy):
    """Race duplicate calls against a slow first attempt.

    Args:
        delay: Seconds to wait before launching each additional attempt.
        max_hedges: Maximum number of *extra* attempts (so total concurrent
            attempts is at most ``1 + max_hedges``).

    The first successful attempt wins and all others are cancelled. If every
    attempt fails, the last failure is re-raised. If an attempt fails while
    hedges remain, the next hedge is launched immediately rather than waiting
    out the delay.
    """

    __slots__ = ("delay", "max_hedges")

    def __init__(self, *, delay: float, max_hedges: int = 1) -> None:
        if max_hedges < 1:
            raise ValueError("max_hedges must be >= 1")
        self.delay = positive_number("delay", delay, allow_zero=True)
        self.max_hedges = max_hedges

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
                    tasks.add(asyncio.ensure_future(fn(*args, **kwargs)))
                    hedges_left -= 1
                    continue
                for task in done:
                    tasks.discard(task)
                    exc = task.exception()
                    if exc is None:
                        return task.result()
                    last_exc = exc
                if not tasks:
                    if hedges_left > 0:
                        # Everything so far failed fast; hedge immediately.
                        tasks.add(asyncio.ensure_future(fn(*args, **kwargs)))
                        hedges_left -= 1
                    elif last_exc is not None:
                        raise last_exc
                    else:  # defensive: cannot happen, but never `raise None`
                        raise RuntimeError("hedge finished with no result and no exception")
        finally:
            for task in tasks:
                task.cancel()
            if tasks:
                # Reap the losers: nothing keeps running behind the caller's
                # back and no "exception was never retrieved" warnings leak.
                await asyncio.gather(*tasks, return_exceptions=True)
