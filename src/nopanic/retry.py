"""Retry policy: re-run a failing call with configurable backoff."""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

from ._core import ExcFilter, Policy, exc_matches, positive_number
from .backoff import Backoff, as_backoff, full_jitter
from .events import emit

__all__ = ["RetryAttempt", "retry"]

_log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RetryAttempt:
    """Passed to ``before_sleep`` after a failed attempt, before the wait.

    Attributes:
        attempt: 1-based index of the attempt that just failed.
        exception: The exception that attempt raised.
        delay: Seconds that will be slept before the next attempt.
    """

    attempt: int
    exception: BaseException
    delay: float


class retry(Policy):
    """Retry a call when it raises a matching exception.

    Args:
        attempts: Total number of attempts, including the first call.
        on: Exception type, tuple of types, or predicate selecting which
            exceptions trigger a retry. Anything else propagates immediately.
        backoff: A backoff strategy (see :mod:`nopanic.backoff`) or a bare
            number of seconds for a fixed delay. Defaults to capped
            exponential growth with full jitter.
        giveup: Optional predicate; returning True for a raised exception
            stops retrying even if it matches ``on`` (e.g. an HTTP 401 among
            retriable transport errors).
        before_sleep: Optional callback invoked with a :class:`RetryAttempt`
            after each failure, before the wait — the natural place to log.
        honor_retry_after: When True (default), an exception carrying a
            numeric ``retry_after`` attribute (seconds) raises the wait to at
            least that long. Works out of the box with ``CircuitOpen`` and
            with any exception you build from an HTTP 429's Retry-After
            header.
        retry_after_cap: Upper bound on an honored ``retry_after`` hint. The
            hint comes from the remote side; without a cap a hostile or
            buggy server could park the client for arbitrary time.

    When attempts are exhausted the *original last exception* is re-raised
    unchanged; there is no wrapper exception to unwrap. If the backoff
    strategy stops yielding delays, retrying stops early the same way. An
    exception raised *inside* ``before_sleep`` is logged and suppressed —
    observability hooks are never allowed to break the call they observe.
    """

    __slots__ = (
        "attempts",
        "backoff",
        "before_sleep",
        "giveup",
        "honor_retry_after",
        "on",
        "retry_after_cap",
    )

    def __init__(
        self,
        attempts: int = 3,
        on: ExcFilter = Exception,
        backoff: float | Backoff | None = None,
        *,
        giveup: Callable[[BaseException], bool] | None = None,
        before_sleep: Callable[[RetryAttempt], None] | None = None,
        honor_retry_after: bool = True,
        retry_after_cap: float = 60.0,
    ) -> None:
        if attempts < 1:
            raise ValueError("attempts must be >= 1")
        self.attempts = attempts
        self.on = on
        self.backoff = full_jitter() if backoff is None else as_backoff(backoff)
        self.giveup = giveup
        self.before_sleep = before_sleep
        self.honor_retry_after = honor_retry_after
        self.retry_after_cap = positive_number("retry_after_cap", retry_after_cap)

    def _next_delay(self, exc: Exception, attempt: int, delays: Iterator[float]) -> float | None:
        """Delay before the next attempt, or None if we must re-raise."""
        if not exc_matches(exc, self.on):
            return None
        if attempt >= self.attempts or (self.giveup is not None and self.giveup(exc)):
            emit("retry.gave_up", "retry", attempt=attempt, exception=exc)
            return None
        try:
            delay = max(0.0, next(delays))
        except StopIteration:
            emit("retry.gave_up", "retry", attempt=attempt, exception=exc)
            return None  # strategy exhausted: re-raise the original error
        if self.honor_retry_after:
            hint = getattr(exc, "retry_after", None)
            if (
                isinstance(hint, (int, float))
                and not isinstance(hint, bool)
                and math.isfinite(hint)
            ):
                delay = max(delay, min(float(hint), self.retry_after_cap))
        emit("retry.attempt_failed", "retry", attempt=attempt, delay=delay, exception=exc)
        if self.before_sleep is not None:
            try:
                self.before_sleep(RetryAttempt(attempt, exc, delay))
            except Exception:
                _log.exception("before_sleep hook raised; ignoring so the retry proceeds")
        return delay

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        # The iterator is created lazily: the success path pays nothing.
        delays: Iterator[float] | None = None
        attempt = 0
        while True:
            attempt += 1
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                if delays is None:
                    delays = iter(self.backoff)
                delay = self._next_delay(exc, attempt, delays)
                if delay is None:
                    raise
                time.sleep(delay)

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        delays: Iterator[float] | None = None
        attempt = 0
        while True:
            attempt += 1
            try:
                return await fn(*args, **kwargs)
            except Exception as exc:
                if delays is None:
                    delays = iter(self.backoff)
                delay = self._next_delay(exc, attempt, delays)
                if delay is None:
                    raise
                await asyncio.sleep(delay)
