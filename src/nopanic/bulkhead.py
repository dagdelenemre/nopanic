"""Bulkhead policy: cap concurrent calls so one dependency can't sink the ship."""

from __future__ import annotations

import asyncio
import threading
import weakref
from collections.abc import Callable
from typing import Any

from ._core import Policy, positive_number
from .exceptions import BulkheadFull

__all__ = ["bulkhead"]


class bulkhead(Policy):
    """Allow at most *max_concurrent* simultaneous calls through.

    Args:
        max_concurrent: Concurrency ceiling.
        max_wait: How long a call may wait for a slot. ``None`` (default)
            waits indefinitely; ``0`` fails fast; any other value waits up to
            that many seconds. On expiry :class:`BulkheadFull` is raised.
        name: Diagnostic name used in error messages.

    One instance carries the shared capacity: decorate every call site that
    should share the same slots with the same instance. Sync callers share a
    thread semaphore; async callers share one semaphore per event loop.
    """

    __slots__ = ("_async_sems", "_sync_sem", "max_concurrent", "max_wait", "name")

    def __init__(
        self,
        max_concurrent: int,
        *,
        max_wait: float | None = None,
        name: str | None = None,
    ) -> None:
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1")
        self.max_concurrent = max_concurrent
        self.max_wait = (
            None if max_wait is None else positive_number("max_wait", max_wait, allow_zero=True)
        )
        self.name = name or f"bulkhead-{id(self):x}"
        self._sync_sem = threading.BoundedSemaphore(max_concurrent)
        # asyncio primitives are bound to a loop, so keep one semaphore per
        # running loop; entries die with their loop.
        self._async_sems: weakref.WeakKeyDictionary[
            asyncio.AbstractEventLoop, asyncio.Semaphore
        ] = weakref.WeakKeyDictionary()

    def _full(self) -> BulkheadFull:
        return BulkheadFull(
            f"bulkhead {self.name!r} is full "
            f"({self.max_concurrent} concurrent calls, max_wait={self.max_wait})"
        )

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        if self.max_wait is None:
            self._sync_sem.acquire()
        elif not self._sync_sem.acquire(timeout=self.max_wait):
            raise self._full()
        try:
            return fn(*args, **kwargs)
        finally:
            self._sync_sem.release()

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        loop = asyncio.get_running_loop()
        sem = self._async_sems.get(loop)
        if sem is None:
            sem = asyncio.Semaphore(self.max_concurrent)
            self._async_sems[loop] = sem

        if self.max_wait is None:
            await sem.acquire()
        elif self.max_wait == 0:
            if sem.locked():
                raise self._full()
            await sem.acquire()  # value > 0: completes without yielding
        else:
            try:
                await asyncio.wait_for(sem.acquire(), timeout=self.max_wait)
            except (TimeoutError, asyncio.TimeoutError):
                raise self._full() from None
        try:
            return await fn(*args, **kwargs)
        finally:
            sem.release()
