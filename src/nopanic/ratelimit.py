"""Client-side rate limiting via a token bucket."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from typing import Any

from ._core import Policy, positive_number
from .exceptions import RateLimited

__all__ = ["rate_limit"]


class rate_limit(Policy):
    """Throttle calls to at most *rate* per *per* seconds (token bucket).

    Args:
        rate: Number of calls allowed per period.
        per: Period length in seconds. ``rate_limit(10, 1.0)`` is 10 calls/s.
        burst: Bucket capacity — how many calls may go through back-to-back
            after an idle stretch. Defaults to ``rate``.
        max_wait: By default a throttled call *waits* for its token. If set,
            calls that would wait longer than this raise :class:`RateLimited`
            instead (``0`` means never wait).
        clock: Monotonic time source; injectable for tests.

    Waiting callers reserve their token up front (the bucket may go
    negative), which keeps ordering roughly FIFO under contention.
    """

    __slots__ = (
        "_capacity",
        "_clock",
        "_lock",
        "_refill_per_sec",
        "_tokens",
        "_updated",
        "max_wait",
    )

    def __init__(
        self,
        rate: float,
        per: float = 1.0,
        *,
        burst: float | None = None,
        max_wait: float | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        rate = positive_number("rate", rate)
        per = positive_number("per", per)
        if burst is not None:
            burst = positive_number("burst", burst)
            if burst < 1:
                raise ValueError("burst must be >= 1")
        self.max_wait = (
            None if max_wait is None else positive_number("max_wait", max_wait, allow_zero=True)
        )
        self._refill_per_sec = rate / per
        self._capacity = burst if burst is not None else rate
        self._tokens = self._capacity
        self._clock = clock
        self._updated = clock()
        self._lock = threading.Lock()

    def _reserve(self) -> float:
        """Consume one token; return seconds to wait before proceeding."""
        with self._lock:
            now = self._clock()
            self._tokens = min(
                self._capacity, self._tokens + (now - self._updated) * self._refill_per_sec
            )
            self._updated = now
            self._tokens -= 1.0
            if self._tokens >= 0.0:
                return 0.0
            wait = -self._tokens / self._refill_per_sec
            if self.max_wait is not None and wait > self.max_wait:
                self._tokens += 1.0  # refund the reservation
                raise RateLimited(
                    f"rate limit would require waiting {wait:.3f}s (max_wait={self.max_wait})"
                )
            return wait

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        wait = self._reserve()
        if wait > 0:
            time.sleep(wait)
        return fn(*args, **kwargs)

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        wait = self._reserve()
        if wait > 0:
            await asyncio.sleep(wait)
        return await fn(*args, **kwargs)
