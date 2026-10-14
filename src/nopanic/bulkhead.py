"""Bulkhead policy: cap concurrent calls so one dependency can't sink the ship."""

from __future__ import annotations

import asyncio
import contextlib
import threading
import weakref
from collections.abc import AsyncGenerator, Callable, Generator
from types import TracebackType
from typing import Any, ClassVar

from . import _core
from ._core import Policy, positive_int, positive_number
from .events import emit
from .exceptions import BulkheadFull

__all__ = ["bulkhead"]


def _release_if_acquired(sem: asyncio.Semaphore) -> Callable[[asyncio.Task[Any]], None]:
    def callback(task: asyncio.Task[Any]) -> None:
        if not task.cancelled() and task.exception() is None:
            sem.release()

    return callback


async def _acquire_within(sem: asyncio.Semaphore, seconds: float) -> bool:
    """Acquire *sem* within *seconds*; never leaks a permit.

    ``asyncio.wait_for(sem.acquire(), ...)`` can lose a permit on Python
    3.10/3.11: when the acquire completes just as the timeout fires, the
    result is dropped and the slot is gone for good, silently shrinking the
    bulkhead. Waiting on a separate task and settling the race explicitly
    keeps every acquired permit accounted for.
    """
    task = asyncio.ensure_future(sem.acquire())
    try:
        await asyncio.wait((task,), timeout=seconds)
    except BaseException:
        # We were cancelled while waiting: give back whatever the task gets.
        if task.done():
            _release_if_acquired(sem)(task)
        else:
            task.cancel()
            task.add_done_callback(_release_if_acquired(sem))
        raise
    if task.done():
        return True
    task.cancel()
    # A permit granted between the timeout and the cancel is released as
    # soon as the task settles, so the slot returns to the pool.
    task.add_done_callback(_release_if_acquired(sem))
    return False


class bulkhead(Policy):
    """Allow at most *max_concurrent* simultaneous calls through.

    Args:
        max_concurrent: Concurrency ceiling.
        max_wait: How long a call may wait for a slot. ``None`` (default)
            waits indefinitely; ``0`` fails fast; any other value waits up to
            that many seconds. On expiry :class:`BulkheadFull` is raised.
            Inside an active :class:`~nopanic.deadline` a call never waits
            past the deadline.
        name: Diagnostic name used in error messages and events.

    One instance carries the shared capacity: decorate every call site that
    should share the same slots with the same instance. Capacity is counted
    **separately** for sync callers (one thread semaphore) and for async
    callers on each event loop (one asyncio semaphore per loop): a bulkhead
    used from sync code and from two event loops admits up to three times
    ``max_concurrent`` in total. Use separate instances per flavour if that
    matters to you.

    Streams (generator functions) hold their slot until they finish. A
    block of code is guarded with ``with bh.guard():`` (sync capacity) or
    ``async with bh.guard():`` (the running loop's capacity).
    """

    __slots__ = ("_async_sems", "_sync_sem", "max_concurrent", "max_wait", "name")

    _STREAMS: ClassVar[frozenset[str]] = frozenset({"sync", "async"})

    def __init__(
        self,
        max_concurrent: int,
        *,
        max_wait: float | None = None,
        name: str | None = None,
    ) -> None:
        self.max_concurrent = positive_int("max_concurrent", max_concurrent)
        self.max_wait = (
            None if max_wait is None else positive_number("max_wait", max_wait, allow_zero=True)
        )
        self.name = name or f"bulkhead-{id(self):x}"
        self._sync_sem = threading.BoundedSemaphore(self.max_concurrent)
        # asyncio primitives are bound to a loop, so keep one semaphore per
        # running loop; entries die with their loop.
        self._async_sems: weakref.WeakKeyDictionary[
            asyncio.AbstractEventLoop, asyncio.Semaphore
        ] = weakref.WeakKeyDictionary()

    @property
    def in_flight(self) -> int:
        """Calls currently holding a slot (sync plus every event loop)."""
        # Reads the semaphores' counters; a snapshot, exact when quiescent.
        busy = self.max_concurrent - self._sync_sem._value
        for sem in list(self._async_sems.values()):
            busy += self.max_concurrent - sem._value
        return busy

    def guard(self) -> _BulkheadGuard:
        """Hold a slot for a block: ``with bh.guard():`` / ``async with bh.guard():``."""
        return _BulkheadGuard(self)

    def _wait_limit(self) -> float | None:
        """max_wait, tightened by an ambient deadline."""
        remaining = _core.time_remaining()
        if remaining is None:
            return self.max_wait
        remaining = max(0.0, remaining)
        return remaining if self.max_wait is None else min(self.max_wait, remaining)

    def _full(self, fn: Callable[..., Any] | None) -> BulkheadFull:
        emit(
            "bulkhead.rejected",
            "bulkhead",
            self.name,
            fn,
            max_concurrent=self.max_concurrent,
            max_wait=self.max_wait,
        )
        return BulkheadFull(
            f"bulkhead {self.name!r} is full "
            f"({self.max_concurrent} concurrent calls, max_wait={self.max_wait})"
        )

    def _acquire_sync(self, fn: Callable[..., Any] | None) -> None:
        if self._sync_sem.acquire(blocking=False):
            return  # a free slot: the common case pays nothing more
        limit = self._wait_limit()
        if limit is None:
            self._sync_sem.acquire()
        elif limit == 0 or not self._sync_sem.acquire(timeout=limit):
            raise self._full(fn)

    async def _acquire_async(self, fn: Callable[..., Any] | None) -> asyncio.Semaphore:
        loop = asyncio.get_running_loop()
        sem = self._async_sems.get(loop)
        if sem is None:
            sem = asyncio.Semaphore(self.max_concurrent)
            self._async_sems[loop] = sem
        if not sem.locked():
            await sem.acquire()  # a free slot: completes without yielding
            return sem
        limit = self._wait_limit()
        if limit is None:
            await sem.acquire()
        elif limit == 0 or not await _acquire_within(sem, limit):
            raise self._full(fn)
        return sem

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        self._acquire_sync(fn)
        try:
            return fn(*args, **kwargs)
        finally:
            self._sync_sem.release()

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        sem = await self._acquire_async(fn)
        try:
            return await fn(*args, **kwargs)
        finally:
            sem.release()

    def _run_sync_gen(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Generator[Any, None, None]:
        self._acquire_sync(fn)
        try:
            with contextlib.closing(fn(*args, **kwargs)) as items:
                yield from items
        finally:
            self._sync_sem.release()

    async def _run_async_gen(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> AsyncGenerator[Any, None]:
        sem = await self._acquire_async(fn)
        try:
            async with contextlib.aclosing(fn(*args, **kwargs)) as items:
                async for item in items:
                    yield item
        finally:
            sem.release()


class _BulkheadGuard:
    """One slot held for a block; a fresh object per ``guard()``."""

    __slots__ = ("_bulkhead", "_sem")

    def __init__(self, bh: bulkhead) -> None:
        self._bulkhead = bh
        self._sem: asyncio.Semaphore | None = None

    def __enter__(self) -> _BulkheadGuard:
        self._bulkhead._acquire_sync(None)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._bulkhead._sync_sem.release()

    async def __aenter__(self) -> _BulkheadGuard:
        self._sem = await self._bulkhead._acquire_async(None)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._sem is not None:
            self._sem.release()
            self._sem = None
