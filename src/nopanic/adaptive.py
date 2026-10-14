"""Adaptive (AIMD) client-side rate limiting.

A token bucket whose rate reacts to the server: when a call raises a
"slow down" error (HTTP 429/503, or anything carrying ``retry_after``),
the rate is cut multiplicatively; every success earns a small additive
recovery. This is the classic AIMD scheme TCP uses for congestion control,
applied to API quotas: you converge on whatever rate the server actually
sustains, without hardcoding it.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any, ClassVar

from ._core import ExcFilter, exc_matches, finite_hint, positive_number
from .events import emit
from .exceptions import ResilienceError
from .ratelimit import _TokenBucket

__all__ = ["adaptive_rate_limit", "looks_throttled"]


def looks_throttled(exc: BaseException) -> bool:
    """Best-effort default for "the server told us to slow down".

    True when the exception carries a ``retry_after`` attribute, an HTTP
    status of 429 or 503 (checked on the exception itself and on an attached
    ``response`` object, which covers requests and httpx errors). Pass your
    own predicate for anything smarter.

    nopanic's own signals (``CircuitOpen``, ``RateLimited``, ...) are never
    pushback: they carry ``retry_after`` too, but they come from the local
    process, and reading them as the server's voice would let a tripped
    breaker collapse the adaptive rate.
    """
    if isinstance(exc, ResilienceError):
        return False
    if getattr(exc, "retry_after", None) is not None:
        return True
    status = getattr(exc, "status_code", None) or getattr(exc, "status", None)
    if status in (429, 503):
        return True
    response = getattr(exc, "response", None)
    return response is not None and getattr(response, "status_code", None) in (429, 503)


class adaptive_rate_limit(_TokenBucket):
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
            would wait longer raise ``RateLimited``. An ambient
            :class:`~nopanic.deadline` is respected the same way.
        max_block: Upper bound on how long a server-sent ``retry_after``
            hint may block the bucket. The hint comes from the remote side;
            without a cap a hostile or buggy server could freeze the client
            indefinitely.
        name: Diagnostic name carried by this policy's events.
        clock: Monotonic time source; injectable for tests.

    A ``retry_after`` attribute on a throttle exception additionally blocks
    the bucket for that many seconds (capped at ``max_block``), so the next
    calls respect the server's explicit wish. Inspect the live rate via
    ``current_rate`` and the bucket via ``available_tokens``. Streams take
    one token when they open and report their outcome when they end;
    ``guard()`` does the same for a block.
    """

    __slots__ = (
        "_blocked_until",
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
        "name",
        "throttle_on",
    )

    _POLICY: ClassVar[str] = "adaptive_rate_limit"

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
        name: str | None = None,
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
        self.name = name
        self._rate_per_sec = rate / per
        self._min_per_sec = min_rate / per
        self._max_per_sec = max_rate / per
        self._increase_per_sec = increase / per
        self._decrease = decrease
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

    def _refill(self) -> float:
        # Caller holds the lock. Returns the current time.
        now = self._clock()
        capacity = max(1.0, self._rate_per_sec)
        self._tokens = min(capacity, self._tokens + (now - self._updated) * self._rate_per_sec)
        self._updated = now
        return now

    @property
    def available_tokens(self) -> float:
        """Tokens in the bucket right now; negative while callers are queued."""
        with self._lock:
            self._refill()
            return self._tokens

    def _reserve(self, fn: Callable[..., Any] | None = None) -> float:
        """Consume one token; return seconds to wait before proceeding."""
        with self._lock:
            now = self._refill()
            self._tokens -= 1.0
            wait = 0.0 if self._tokens >= 0.0 else -self._tokens / self._rate_per_sec
            if self._blocked_until > now:
                wait = max(wait, self._blocked_until - now)
            if wait == 0.0:
                return 0.0
            limit = self._wait_limit()
            if limit is None or wait <= limit:
                return wait
            self._tokens += 1.0  # refund the reservation
        raise self._rejected(fn, wait)

    def _refund(self) -> None:
        with self._lock:
            self._tokens = min(max(1.0, self._rate_per_sec), self._tokens + 1.0)

    def _on_success(self) -> None:
        with self._lock:
            self._rate_per_sec = min(self._max_per_sec, self._rate_per_sec + self._increase_per_sec)

    def _on_throttle(self, exc: BaseException, fn: Callable[..., Any] | None) -> None:
        with self._lock:
            old = self._rate_per_sec
            self._rate_per_sec = max(self._min_per_sec, self._rate_per_sec * self._decrease)
            new = self._rate_per_sec
            hint = finite_hint(getattr(exc, "retry_after", None))
            retry_after = None
            if hint is not None:
                retry_after = min(hint, self._max_block)
                self._blocked_until = max(self._blocked_until, self._clock() + retry_after)
        emit(
            "ratelimit.throttled",
            "adaptive_rate_limit",
            self.name,
            fn,
            old_rate=old,
            new_rate=new,
            retry_after=retry_after,
        )

    def _record(self, exc: BaseException | None, fn: Callable[..., Any] | None) -> None:
        if exc is None:
            self._on_success()
        elif exc_matches(exc, self.throttle_on):
            self._on_throttle(exc, fn)
