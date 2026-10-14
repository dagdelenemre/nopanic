"""Retry policy: re-run a failing call with configurable backoff."""

from __future__ import annotations

import contextlib
import logging
import time
from collections.abc import AsyncGenerator, Callable, Generator, Iterator
from dataclasses import dataclass
from typing import Any, ClassVar

from . import _core
from ._core import ExcFilter, Policy, exc_matches, finite_hint, positive_int, positive_number
from .backoff import Backoff, as_backoff, full_jitter
from .budget import RetryBudget
from .events import emit

__all__ = ["RetryAttempt", "retry"]

_log = logging.getLogger(__name__)

# Safety margin added on top of an honored retry_after hint. "Retry after X"
# means "not before X": sleeping a little longer is always safe, while racing
# the deadline exactly is not. On Windows, time.sleep can wake measurably
# before the monotonic deadline (and sub-quantum sleeps can be no-ops), which
# would otherwise burn retry attempts against a still-closed window.
_RETRY_AFTER_MARGIN = 0.05
_RETRY_AFTER_FACTOR = 1.05


@dataclass(frozen=True, slots=True)
class RetryAttempt:
    """Passed to ``before_sleep`` after a failed attempt, before the wait.

    Attributes:
        attempt: 1-based index of the attempt that just failed.
        exception: The exception that attempt raised, or None when the
            attempt returned a value rejected by ``on_result``.
        delay: Seconds that will be slept before the next attempt.
        result: The rejected return value when ``on_result`` triggered the
            retry, otherwise None.
    """

    attempt: int
    exception: BaseException | None
    delay: float
    result: Any = None


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
            least that long, plus a small safety margin (5% + 50ms) so the
            deadline is strictly passed despite OS timer granularity. Works
            out of the box with ``CircuitOpen``, ``RateLimited`` and with any
            exception you build from an HTTP 429's Retry-After header.
        retry_after_cap: Upper bound on an honored ``retry_after`` hint. The
            hint comes from the remote side; without a cap a hostile or
            buggy server could park the client for arbitrary time.
        max_time: Total time budget in seconds, measured from the start of
            the first attempt. Retrying stops as soon as the next wait would
            end past the budget, so no attempt ever starts after the
            deadline. ``None`` (default) means attempts alone bound retrying.
            An ambient :class:`~nopanic.deadline` is respected the same way.
        on_result: Optional predicate over the *return value*; returning
            True treats the result as a failure and retries (an HTTP client
            that returns a 503 response instead of raising). When retrying
            stops, the last result is returned as-is. Not applied to streams.
        budget: Optional :class:`~nopanic.RetryBudget` shared with other
            retry policies; when it is spent, retrying stops early.
        name: Diagnostic name carried by this policy's events.
        clock: Monotonic time source for ``max_time``; injectable for tests.

    When attempts are exhausted the *original last exception* is re-raised
    unchanged; there is no wrapper exception to unwrap. If the backoff
    strategy stops yielding delays, or the ``max_time`` budget, deadline or
    retry budget would be exceeded, retrying stops early the same way. An
    exception raised *inside* ``before_sleep`` is logged and suppressed —
    observability hooks are never allowed to break the call they observe.

    On a generator or async generator function (an LLM token stream, say),
    the *opening* of the stream is retried: failures before the first item
    are retried like any call, while a failure after items were delivered
    propagates, since those items cannot be taken back.
    """

    __slots__ = (
        "_clock",
        "attempts",
        "backoff",
        "before_sleep",
        "budget",
        "giveup",
        "honor_retry_after",
        "max_time",
        "name",
        "on",
        "on_result",
        "retry_after_cap",
    )

    _STREAMS: ClassVar[frozenset[str]] = frozenset({"sync", "async"})

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
        max_time: float | None = None,
        on_result: Callable[[Any], bool] | None = None,
        budget: RetryBudget | None = None,
        name: str | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.attempts = positive_int("attempts", attempts)
        self.on = on
        self.backoff = full_jitter() if backoff is None else as_backoff(backoff)
        self.giveup = giveup
        self.before_sleep = before_sleep
        self.honor_retry_after = honor_retry_after
        self.retry_after_cap = positive_number("retry_after_cap", retry_after_cap)
        self.max_time = None if max_time is None else positive_number("max_time", max_time)
        self.on_result = on_result
        self.budget = budget
        self.name = name
        self._clock = clock

    def _stop(
        self, fn: Callable[..., Any], attempt: int, exc: Exception | None, result: Any, reason: str
    ) -> None:
        extra = {} if exc is not None else {"result": result}
        emit(
            "retry.gave_up",
            "retry",
            self.name,
            fn,
            attempt=attempt,
            exception=exc,
            reason=reason,
            **extra,
        )

    def _next_delay(
        self,
        fn: Callable[..., Any],
        exc: Exception | None,
        result: Any,
        attempt: int,
        delays: Iterator[float],
        start: float,
    ) -> float | None:
        """Delay before the next attempt, or None to stop (re-raise or return).

        *exc* is the raised exception, or None when *result* was rejected by
        ``on_result``.
        """
        if exc is not None and not exc_matches(exc, self.on):
            return None
        if attempt >= self.attempts:
            self._stop(fn, attempt, exc, result, "attempts")
            return None
        if exc is not None and self.giveup is not None and self.giveup(exc):
            self._stop(fn, attempt, exc, result, "giveup")
            return None
        try:
            delay = max(0.0, next(delays))
        except StopIteration:
            self._stop(fn, attempt, exc, result, "backoff")
            return None  # strategy exhausted: re-raise the original error
        if exc is not None and self.honor_retry_after:
            hint = finite_hint(getattr(exc, "retry_after", None))
            if hint is not None:
                padded = hint * _RETRY_AFTER_FACTOR + _RETRY_AFTER_MARGIN
                delay = max(delay, min(padded, self.retry_after_cap))
        if self.max_time is not None and self._clock() - start + delay > self.max_time:
            # The next attempt could only start past the deadline.
            self._stop(fn, attempt, exc, result, "max_time")
            return None
        remaining = _core.time_remaining()
        if remaining is not None and delay >= remaining:
            self._stop(fn, attempt, exc, result, "deadline")
            return None
        if self.budget is not None and not self.budget.try_withdraw():
            self._stop(fn, attempt, exc, result, "budget")
            return None
        extra = {} if exc is not None else {"result": result}
        emit(
            "retry.attempt_failed",
            "retry",
            self.name,
            fn,
            attempt=attempt,
            delay=delay,
            exception=exc,
            **extra,
        )
        if self.before_sleep is not None:
            try:
                self.before_sleep(RetryAttempt(attempt, exc, delay, result))
            except Exception:
                _log.exception("before_sleep hook raised; ignoring so the retry proceeds")
        return delay

    def _begin(self) -> float:
        if self.budget is not None:
            self.budget.deposit()
        return 0.0 if self.max_time is None else self._clock()

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        # The iterator is created lazily: the success path pays nothing.
        delays: Iterator[float] | None = None
        # Hot path: without a budget or max_time there is nothing to set up.
        no_setup = self.budget is None and self.max_time is None
        start = 0.0 if no_setup else self._begin()
        attempt = 0
        while True:
            attempt += 1
            try:
                result = fn(*args, **kwargs)
            except Exception as exc:
                if delays is None:
                    delays = iter(self.backoff)
                delay = self._next_delay(fn, exc, None, attempt, delays, start)
                if delay is None:
                    raise
            else:
                if self.on_result is None or not self.on_result(result):
                    return result
                if delays is None:
                    delays = iter(self.backoff)
                delay = self._next_delay(fn, None, result, attempt, delays, start)
                if delay is None:
                    return result
            _core.sleep(delay)

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        delays: Iterator[float] | None = None
        # Hot path: without a budget or max_time there is nothing to set up.
        no_setup = self.budget is None and self.max_time is None
        start = 0.0 if no_setup else self._begin()
        attempt = 0
        while True:
            attempt += 1
            try:
                result = await fn(*args, **kwargs)
            except Exception as exc:
                if delays is None:
                    delays = iter(self.backoff)
                delay = self._next_delay(fn, exc, None, attempt, delays, start)
                if delay is None:
                    raise
            else:
                if self.on_result is None or not self.on_result(result):
                    return result
                if delays is None:
                    delays = iter(self.backoff)
                delay = self._next_delay(fn, None, result, attempt, delays, start)
                if delay is None:
                    return result
            await _core.asleep(delay)

    def _run_sync_gen(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Generator[Any, None, None]:
        delays: Iterator[float] | None = None
        start = self._begin()
        attempt = 0
        while True:
            attempt += 1
            with contextlib.closing(fn(*args, **kwargs)) as items:
                try:
                    first = next(items)
                except StopIteration:
                    return
                except Exception as exc:
                    if delays is None:
                        delays = iter(self.backoff)
                    delay = self._next_delay(fn, exc, None, attempt, delays, start)
                    if delay is None:
                        raise
                else:
                    # Delivered items cannot be retracted: from here on the
                    # stream is the caller's, failures included.
                    yield first
                    yield from items
                    return
            _core.sleep(delay)

    async def _run_async_gen(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> AsyncGenerator[Any, None]:
        delays: Iterator[float] | None = None
        start = self._begin()
        attempt = 0
        while True:
            attempt += 1
            async with contextlib.aclosing(fn(*args, **kwargs)) as items:
                try:
                    first = await items.__anext__()
                except StopAsyncIteration:
                    return
                except Exception as exc:
                    if delays is None:
                        delays = iter(self.backoff)
                    delay = self._next_delay(fn, exc, None, attempt, delays, start)
                    if delay is None:
                        raise
                else:
                    yield first
                    async for item in items:
                        yield item
                    return
            await _core.asleep(delay)
