"""Retry policy: re-run a failing call with configurable backoff."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

from ._core import ExcFilter, Policy, exc_matches
from .backoff import Backoff, as_backoff, full_jitter

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

    When attempts are exhausted the *original last exception* is re-raised
    unchanged; there is no wrapper exception to unwrap. If the backoff
    strategy stops yielding delays, retrying stops early the same way. An
    exception raised *inside* ``before_sleep`` is logged and suppressed —
    observability hooks are never allowed to break the call they observe.
    """

    __slots__ = ("attempts", "backoff", "before_sleep", "giveup", "on")

    def __init__(
        self,
        attempts: int = 3,
        on: ExcFilter = Exception,
        backoff: float | Backoff | None = None,
        *,
        giveup: Callable[[BaseException], bool] | None = None,
        before_sleep: Callable[[RetryAttempt], None] | None = None,
    ) -> None:
        if attempts < 1:
            raise ValueError("attempts must be >= 1")
        self.attempts = attempts
        self.on = on
        self.backoff = full_jitter() if backoff is None else as_backoff(backoff)
        self.giveup = giveup
        self.before_sleep = before_sleep

    def _next_delay(self, exc: Exception, attempt: int, delays: Iterator[float]) -> float | None:
        """Delay before the next attempt, or None if we must re-raise."""
        if attempt >= self.attempts:
            return None
        if not exc_matches(exc, self.on):
            return None
        if self.giveup is not None and self.giveup(exc):
            return None
        try:
            delay = max(0.0, next(delays))
        except StopIteration:
            return None  # strategy exhausted: re-raise the original error
        if self.before_sleep is not None:
            try:
                self.before_sleep(RetryAttempt(attempt, exc, delay))
            except Exception:
                _log.exception("before_sleep hook raised; ignoring so the retry proceeds")
        return delay

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        delays = iter(self.backoff)
        attempt = 0
        while True:
            attempt += 1
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                delay = self._next_delay(exc, attempt, delays)
                if delay is None:
                    raise
                time.sleep(delay)

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        delays = iter(self.backoff)
        attempt = 0
        while True:
            attempt += 1
            try:
                return await fn(*args, **kwargs)
            except Exception as exc:
                delay = self._next_delay(exc, attempt, delays)
                if delay is None:
                    raise
                await asyncio.sleep(delay)
