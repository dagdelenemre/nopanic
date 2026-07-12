"""Adaptive (AIMD) client-side rate limiting.

A token bucket whose rate reacts to the server: when a call raises a
"slow down" error (HTTP 429/503, or anything carrying ``retry_after``),
the rate is cut multiplicatively; every success earns a small additive
recovery. This is the classic AIMD scheme TCP uses for congestion control,
applied to API quotas: you converge on whatever rate the server actually
sustains, without hardcoding it.
"""

from __future__ import annotations

import asyncio
import math
import threading
import time
from collections.abc import Callable
from typing import Any

from ._core import ExcFilter, Policy, exc_matches, positive_number
from .events import emit
from .exceptions import RateLimited

__all__ = ["adaptive_rate_limit", "looks_throttled"]


def looks_throttled(exc: BaseException) -> bool:
    """Best-effort default for "the server told us to slow down".

    True when the exception carries a ``retry_after`` attribute, an HTTP
    status of 429 or 503 (checked on the exception itself and on an attached
    ``response`` object, which covers requests and httpx errors). Pass your
    own predicate for anything smarter.
    """
    if getattr(exc, "retry_after", None) is not None:
        return True
    status = getattr(exc, "status_code", None) or getattr(exc, "status", None)
    if status in (429, 503):
        return True
    response = getattr(exc, "response", None)
    return response is not None and getattr(response, "status_code", None) in (429, 503)


class adaptive_rate_limit(Policy):
    """Token bucket whose rate adapts to server pushback (AIMD).

    Args:
        rate: Starting (and by default maximum) calls per ``per`` seconds.
        per: Period length in seconds.
        min_rate: Floor the rate never drops below (same unit as ``rate``).
            Defaults to ``rate / 10``.
        max_rate: Ceiling the rate never climbs above. Defaults to ``rate``,
            i.e. the configured rate is treated as the allowed maximum.
        increase: Additive recovery per successful call, in calls-per-``per``
            units. Small values recover cautiously.
        decrease: Multiplicative factor in (0, 1) applied on throttle errors.
        throttle_on: Which exceptions mean "slow down". Defaults to
            :func:`looks_throttled`. Matching exceptions still propagate to
            the caller (pair with ``retry`` to also re-attempt them).
        max_wait: As in ``rate_limit``: None waits, otherwise calls that
            would wait longer raise ``RateLimited``.
        max_block: Upper bound on how long a server-sent ``retry_after``
            hint may block the bucket. The hint comes from the remote side;
            without a cap a hostile or buggy server could freeze the client
            indefinitely.
        clock: Monotonic time source; injectable for tests.

    A ``retry_after`` attribute on a throttle exception additionally blocks
    the bucket for that many seconds (capped at ``max_block``), so the next
    calls respect the server's explicit wish. Inspect the live rate via
    ``current_rate``.
    """

    __slots__ = (
        "_blocked_until",
        "_capacity_per_sec",
        "_clock",
        "_decrease",
        "_increase_per_sec",
        "_lock",
        "_max_block",
        "_max_per_sec",
        "_min_per_sec",
        "_rate_per_sec",
        "_tokens",
        "_updated",
        "max_wait",
        "throttle_on",
    )

    def __init__(
        self,
        rate: float,
        per: float = 1.0,
        *,
        min_rate: float | None = None,
        max_rate: float | None = None,
        increase: float = 0.1,
        decrease: float = 0.5,
        throttle_on: ExcFilter = looks_throttled,
        max_wait: float | None = None,
        max_block: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        rate = positive_number("rate", rate)
        per = positive_number("per", per)
        min_rate = rate / 10.0 if min_rate is None else positive_number("min_rate", min_rate)
        max_rate = rate if max_rate is None else positive_number("max_rate", max_rate)
        if not min_rate <= rate <= max_rate:
            raise ValueError("expected min_rate <= rate <= max_rate")
        positive_number("increase", increase)
        positive_number("decrease", decrease)
        if not 0.0 < decrease < 1.0:
            raise ValueError("decrease must be in (0, 1)")
        self.max_wait = (
            None if max_wait is None else positive_number("max_wait", max_wait, allow_zero=True)
        )
        self._max_block = positive_number("max_block", max_block)
        self.throttle_on = throttle_on
        self._rate_per_sec = rate / per
        self._min_per_sec = min_rate / per
        self._max_per_sec = max_rate / per
        self._increase_per_sec = increase / per
        self._decrease = decrease
        self._capacity_per_sec = self._rate_per_sec
        self._tokens = max(1.0, self._rate_per_sec)
        self._blocked_until = 0.0
        self._clock = clock
        self._updated = clock()
        self._lock = threading.Lock()

    @property
    def current_rate(self) -> float:
        """Current adapted rate, in calls per second."""
        with self._lock:
            return self._rate_per_sec

    def _reserve(self) -> float:
        """Consume one token; return seconds to wait before proceeding."""
        with self._lock:
            now = self._clock()
            capacity = max(1.0, self._rate_per_sec)
            self._tokens = min(
                capacity, self._tokens + (now - self._updated) * self._rate_per_sec
            )
            self._updated = now
            self._tokens -= 1.0
            wait = 0.0 if self._tokens >= 0.0 else -self._tokens / self._rate_per_sec
            if self._blocked_until > now:
                wait = max(wait, self._blocked_until - now)
            if self.max_wait is None or wait <= self.max_wait:
                return wait
            self._tokens += 1.0  # refund the reservation
        emit("ratelimit.rejected", "adaptive_rate_limit", would_wait=wait, max_wait=self.max_wait)
        raise RateLimited(
            f"adaptive rate limit would require waiting {wait:.3f}s (max_wait={self.max_wait})"
        )

    def _on_success(self) -> None:
        with self._lock:
            self._rate_per_sec = min(self._max_per_sec, self._rate_per_sec + self._increase_per_sec)

    def _on_throttle(self, exc: BaseException) -> None:
        with self._lock:
            old = self._rate_per_sec
            self._rate_per_sec = max(self._min_per_sec, self._rate_per_sec * self._decrease)
            new = self._rate_per_sec
            hint = getattr(exc, "retry_after", None)
            retry_after = None
            if (
                isinstance(hint, (int, float))
                and not isinstance(hint, bool)
                and math.isfinite(hint)
                and hint > 0
            ):
                retry_after = min(float(hint), self._max_block)
                self._blocked_until = max(self._blocked_until, self._clock() + retry_after)
        emit(
            "ratelimit.throttled",
            "adaptive_rate_limit",
            old_rate=old,
            new_rate=new,
            retry_after=retry_after,
        )

    def _record(self, exc: BaseException | None) -> None:
        if exc is None:
            self._on_success()
        elif exc_matches(exc, self.throttle_on):
            self._on_throttle(exc)

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        wait = self._reserve()
        if wait > 0:
            emit("ratelimit.waited", "adaptive_rate_limit", seconds=wait)
            time.sleep(wait)
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:
            self._record(exc)
            raise
        self._record(None)
        return result

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        wait = self._reserve()
        if wait > 0:
            emit("ratelimit.waited", "adaptive_rate_limit", seconds=wait)
            await asyncio.sleep(wait)
        try:
            result = await fn(*args, **kwargs)
        except Exception as exc:
            self._record(exc)
            raise
        self._record(None)
        return result
