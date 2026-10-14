"""Client-side rate limiting via a token bucket."""

from __future__ import annotations

import contextlib
import threading
import time
from collections.abc import AsyncGenerator, Callable, Generator
from types import TracebackType
from typing import Any, ClassVar

from . import _core
from ._core import Policy, positive_number
from .events import emit
from .exceptions import RateLimited

__all__ = ["rate_limit"]


class _TokenBucket(Policy):
    """Shared machinery of the plain and the adaptive rate limiter.

    Subclasses implement ``_reserve`` (take a token, return the wait) and
    ``_refund`` (give back a token whose wait was abandoned), and may record
    call outcomes in ``_record``.
    """

    __slots__ = ()

    _POLICY: ClassVar[str] = "rate_limit"
    _STREAMS: ClassVar[frozenset[str]] = frozenset({"sync", "async"})

    name: str | None
    max_wait: float | None

    def _reserve(self, fn: Callable[..., Any] | None = None) -> float:
        raise NotImplementedError

    def _refund(self) -> None:
        raise NotImplementedError

    def _record(self, exc: BaseException | None, fn: Callable[..., Any] | None) -> None:
        """Outcome hook; only the adaptive limiter learns from outcomes."""

    def _wait_limit(self) -> float | None:
        """max_wait, tightened by an ambient deadline. Caller holds the lock."""
        remaining = _core.time_remaining()
        if remaining is None:
            return self.max_wait
        remaining = max(0.0, remaining)
        return remaining if self.max_wait is None else min(self.max_wait, remaining)

    def _rejected(self, fn: Callable[..., Any] | None, wait: float) -> RateLimited:
        emit(
            "ratelimit.rejected",
            self._POLICY,
            self.name,
            fn,
            would_wait=wait,
            max_wait=self.max_wait,
        )
        what = "adaptive rate limit" if self._POLICY != "rate_limit" else "rate limit"
        return RateLimited(
            f"{what} would require waiting {wait:.3f}s (max_wait={self.max_wait})",
            retry_after=wait,
        )

    def _wait_sync(self, fn: Callable[..., Any] | None) -> None:
        wait = self._reserve(fn)
        if wait > 0:
            emit("ratelimit.waited", self._POLICY, self.name, fn, seconds=wait)
            try:
                _core.sleep(wait)
            except BaseException:
                self._refund()  # the reserved token was never used
                raise

    async def _wait_async(self, fn: Callable[..., Any] | None) -> None:
        wait = self._reserve(fn)
        if wait > 0:
            emit("ratelimit.waited", self._POLICY, self.name, fn, seconds=wait)
            try:
                await _core.asleep(wait)
            except BaseException:
                # Cancelled while queued: without the refund every cancelled
                # waiter would push all later callers back by one token.
                self._refund()
                raise

    def guard(self) -> _BucketGuard:
        """Take a token for a block: ``with limiter.guard():`` / ``async with``."""
        return _BucketGuard(self)

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        self._wait_sync(fn)
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:
            self._record(exc, fn)
            raise
        self._record(None, fn)
        return result

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        await self._wait_async(fn)
        try:
            result = await fn(*args, **kwargs)
        except Exception as exc:
            self._record(exc, fn)
            raise
        self._record(None, fn)
        return result

    # A stream costs one token, taken when it opens.

    def _run_sync_gen(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Generator[Any, None, None]:
        self._wait_sync(fn)
        try:
            with contextlib.closing(fn(*args, **kwargs)) as items:
                yield from items
        except Exception as exc:
            self._record(exc, fn)
            raise
        self._record(None, fn)

    async def _run_async_gen(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> AsyncGenerator[Any, None]:
        await self._wait_async(fn)
        try:
            async with contextlib.aclosing(fn(*args, **kwargs)) as items:
                async for item in items:
                    yield item
        except Exception as exc:
            self._record(exc, fn)
            raise
        self._record(None, fn)


class _BucketGuard:
    """One token taken for a block; outcomes feed the adaptive limiter."""

    __slots__ = ("_bucket",)

    def __init__(self, bucket: _TokenBucket) -> None:
        self._bucket = bucket

    def __enter__(self) -> _BucketGuard:
        self._bucket._wait_sync(None)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc is None or isinstance(exc, Exception):
            self._bucket._record(exc, None)

    async def __aenter__(self) -> _BucketGuard:
        await self._bucket._wait_async(None)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.__exit__(exc_type, exc, tb)


class rate_limit(_TokenBucket):
    """Throttle calls to at most *rate* per *per* seconds (token bucket).

    Args:
        rate: Number of calls allowed per period.
        per: Period length in seconds. ``rate_limit(10, 1.0)`` is 10 calls/s.
        burst: Bucket capacity — how many calls may go through back-to-back
            after an idle stretch. Defaults to ``rate``.
        max_wait: By default a throttled call *waits* for its token. If set,
            calls that would wait longer than this raise :class:`RateLimited`
            instead (``0`` means never wait). Inside an active
            :class:`~nopanic.deadline` a call never waits past the deadline.
        name: Diagnostic name carried by this policy's events.
        clock: Monotonic time source; injectable for tests.

    Waiting callers reserve their token up front (the bucket may go
    negative), which keeps ordering roughly FIFO under contention; a waiter
    that is cancelled gives its token back. A stream takes one token when it
    opens; ``with limiter.guard():`` takes one for a block.
    """

    __slots__ = (
        "_capacity",
        "_clock",
        "_lock",
        "_refill_per_sec",
        "_tokens",
        "_updated",
        "max_wait",
        "name",
    )

    def __init__(
        self,
        rate: float,
        per: float = 1.0,
        *,
        burst: float | None = None,
        max_wait: float | None = None,
        name: str | None = None,
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
        self.name = name
        self._refill_per_sec = rate / per
        self._capacity = burst if burst is not None else rate
        self._tokens = self._capacity
        self._clock = clock
        self._updated = clock()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        # Caller holds the lock.
        now = self._clock()
        self._tokens = min(
            self._capacity, self._tokens + (now - self._updated) * self._refill_per_sec
        )
        self._updated = now

    @property
    def available_tokens(self) -> float:
        """Tokens in the bucket right now; negative while callers are queued."""
        with self._lock:
            self._refill()
            return self._tokens

    def _reserve(self, fn: Callable[..., Any] | None = None) -> float:
        """Consume one token; return seconds to wait before proceeding."""
        with self._lock:
            self._refill()
            self._tokens -= 1.0
            if self._tokens >= 0.0:
                return 0.0
            wait = -self._tokens / self._refill_per_sec
            limit = self._wait_limit()
            if limit is None or wait <= limit:
                return wait
            self._tokens += 1.0  # refund the reservation
        raise self._rejected(fn, wait)

    def _refund(self) -> None:
        with self._lock:
            self._tokens = min(self._capacity, self._tokens + 1.0)

    # The plain limiter does not learn from outcomes: skip the bookkeeping.

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        self._wait_sync(fn)
        return fn(*args, **kwargs)

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        await self._wait_async(fn)
        return await fn(*args, **kwargs)
