"""Result cache with a stale-while-failing safety net.

Fresh results are served from memory; when the value expires the call runs
again, and if that refresh *fails*, the stale value is served instead of the
error (up to ``stale_ttl`` past expiry). The dependency being down stops
being your outage for as long as your stale window allows.

Refreshes are single-flight: when many callers miss the same key at once,
one of them calls the dependency and the rest share its outcome, so an
expiring hot key does not stampede a backend at the moment it is weakest.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Hashable
from typing import Any, TypeVar

from ._core import ExcFilter, Policy, exc_matches, positive_number
from .events import emit

__all__ = ["cache"]

_MISS = object()


class _Flight:
    """One in-progress refresh of a key, shared by every concurrent caller.

    The leader records the outcome, then signals completion. ``abandoned``
    means the leader was cancelled or interrupted before finishing; its
    followers then refresh on their own instead of inheriting a cancellation
    that was not theirs.
    """

    __slots__ = ("abandoned", "error", "value")

    def __init__(self) -> None:
        self.value: Any = None
        self.error: Exception | None = None
        self.abandoned = False

    def outcome(self) -> Any:
        if self.error is not None:
            raise self.error
        return self.value


class _SyncFlight(_Flight):
    __slots__ = ("done",)

    def __init__(self) -> None:
        super().__init__()
        self.done = threading.Event()


class _AsyncFlight(_Flight):
    __slots__ = ("done",)

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        super().__init__()
        self.done: asyncio.Future[None] = loop.create_future()


_FlightT = TypeVar("_FlightT", bound=_Flight)


class cache(Policy):
    """Cache results by arguments; serve stale on refresh failure.

    Args:
        ttl: Seconds a cached value counts as fresh (served without calling).
        stale_ttl: Extra seconds past ``ttl`` during which an *expired* value
            may still be served, but only when the refresh call fails with an
            exception matching ``on``. ``0`` disables stale serving, leaving
            a plain TTL cache.
        on: Which refresh failures allow serving stale; others propagate.
        maxsize: Entry limit; least-recently-used entries are evicted.
        key: Optional ``key(*args, **kwargs) -> hashable`` to build cache
            keys (use it when arguments are unhashable). By default the
            positional and keyword arguments themselves form the key.
        single_flight: When True (default), concurrent misses of the same
            key share one refresh: a single caller runs the function and the
            others wait for and receive its outcome (value, stale value or
            the same exception). Sync and async callers are deduplicated
            separately, async ones per event loop.
        clock: Monotonic time source; injectable for tests.

    One instance may decorate several functions; entries are keyed per
    function. Arguments must be hashable unless ``key`` is given.
    """

    __slots__ = (
        "_async_flights",
        "_clock",
        "_entries",
        "_key",
        "_lock",
        "_sync_flights",
        "maxsize",
        "on",
        "single_flight",
        "stale_ttl",
        "ttl",
    )

    def __init__(
        self,
        ttl: float,
        *,
        stale_ttl: float = 0.0,
        on: ExcFilter = Exception,
        maxsize: int = 1024,
        key: Callable[..., Hashable] | None = None,
        single_flight: bool = True,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be >= 1")
        self.ttl = positive_number("ttl", ttl)
        self.stale_ttl = positive_number("stale_ttl", stale_ttl, allow_zero=True)
        self.on = on
        self.maxsize = maxsize
        self.single_flight = single_flight
        self._key = key
        self._clock = clock
        self._lock = threading.Lock()
        # key -> (stored_at, value); ordered for LRU eviction.
        self._entries: OrderedDict[Hashable, tuple[float, Any]] = OrderedDict()
        # key -> refresh in progress (single-flight bookkeeping).
        self._sync_flights: dict[Hashable, _SyncFlight] = {}
        self._async_flights: dict[Hashable, _AsyncFlight] = {}

    def clear(self) -> None:
        """Drop every cached entry."""
        with self._lock:
            self._entries.clear()

    def _make_key(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Hashable:
        if self._key is not None:
            return (fn, self._key(*args, **kwargs))
        return (fn, args, tuple(sorted(kwargs.items())))

    def _lookup(self, key: Hashable) -> tuple[Any, float]:
        """Return (value, age); value is _MISS when absent."""
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return _MISS, 0.0
            self._entries.move_to_end(key)
            stored_at, value = entry
            return value, self._clock() - stored_at

    def _store(self, key: Hashable, value: Any) -> None:
        with self._lock:
            self._entries[key] = (self._clock(), value)
            self._entries.move_to_end(key)
            while len(self._entries) > self.maxsize:
                self._entries.popitem(last=False)

    def _serve_stale(self, key: Hashable, exc: Exception) -> Any:
        """Return the stale value, or re-raise *exc* when none is usable."""
        if not exc_matches(exc, self.on):
            raise exc
        value, age = self._lookup(key)
        if value is _MISS or age > self.ttl + self.stale_ttl:
            raise exc
        emit("cache.stale_served", "cache", age=age, exception=exc)
        return value

    def _join(
        self, flights: dict[Hashable, _FlightT], key: Hashable, mine: _FlightT
    ) -> _FlightT:
        """Register *mine* as the refresh for *key*, or return the running one."""
        with self._lock:
            return flights.setdefault(key, mine)

    def _land(self, flights: dict[Hashable, _FlightT], key: Hashable, mine: _FlightT) -> None:
        with self._lock:
            if flights.get(key) is mine:
                del flights[key]

    def _refresh_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any], key: Hashable
    ) -> Any:
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:
            return self._serve_stale(key, exc)
        self._store(key, result)
        return result

    async def _refresh_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any], key: Hashable
    ) -> Any:
        try:
            result = await fn(*args, **kwargs)
        except Exception as exc:
            return self._serve_stale(key, exc)
        self._store(key, result)
        return result

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        key = self._make_key(fn, args, kwargs)
        value, age = self._lookup(key)
        if value is not _MISS and age <= self.ttl:
            return value
        if not self.single_flight:
            return self._refresh_sync(fn, args, kwargs, key)
        mine = _SyncFlight()
        flight = self._join(self._sync_flights, key, mine)
        if flight is not mine:
            flight.done.wait()
            if flight.abandoned:
                return self._refresh_sync(fn, args, kwargs, key)
            return flight.outcome()
        try:
            mine.value = self._refresh_sync(fn, args, kwargs, key)
        except Exception as exc:
            mine.error = exc
            raise
        except BaseException:
            mine.abandoned = True
            raise
        finally:
            self._land(self._sync_flights, key, mine)
            mine.done.set()
        return mine.value

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        key = self._make_key(fn, args, kwargs)
        value, age = self._lookup(key)
        if value is not _MISS and age <= self.ttl:
            return value
        if not self.single_flight:
            return await self._refresh_async(fn, args, kwargs, key)
        loop = asyncio.get_running_loop()
        mine = _AsyncFlight(loop)
        flight = self._join(self._async_flights, key, mine)
        if flight is not mine:
            if flight.done.get_loop() is not loop:
                # The running refresh belongs to another event loop and cannot
                # be awaited from here: refresh independently.
                return await self._refresh_async(fn, args, kwargs, key)
            # shield: a follower giving up must not cancel the shared signal.
            await asyncio.shield(flight.done)
            if flight.abandoned:
                return await self._refresh_async(fn, args, kwargs, key)
            return flight.outcome()
        try:
            mine.value = await self._refresh_async(fn, args, kwargs, key)
        except Exception as exc:
            mine.error = exc
            raise
        except BaseException:
            mine.abandoned = True
            raise
        finally:
            self._land(self._async_flights, key, mine)
            mine.done.set_result(None)
        return mine.value
