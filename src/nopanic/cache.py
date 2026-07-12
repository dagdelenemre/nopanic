"""Result cache with a stale-while-failing safety net.

Fresh results are served from memory; when the value expires the call runs
again, and if that refresh *fails*, the stale value is served instead of the
error (up to ``stale_ttl`` past expiry). The dependency being down stops
being your outage for as long as your stale window allows.
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Hashable
from typing import Any

from ._core import ExcFilter, Policy, exc_matches, positive_number
from .events import emit

__all__ = ["cache"]

_MISS = object()


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
        clock: Monotonic time source; injectable for tests.

    One instance may decorate several functions; entries are keyed per
    function. Arguments must be hashable unless ``key`` is given. Concurrent
    refreshes of the same key are not deduplicated (no single-flight); wrap
    with ``bulkhead`` if a thundering herd matters.
    """

    __slots__ = ("_clock", "_entries", "_key", "_lock", "maxsize", "on", "stale_ttl", "ttl")

    def __init__(
        self,
        ttl: float,
        *,
        stale_ttl: float = 0.0,
        on: ExcFilter = Exception,
        maxsize: int = 1024,
        key: Callable[..., Hashable] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be >= 1")
        self.ttl = positive_number("ttl", ttl)
        self.stale_ttl = positive_number("stale_ttl", stale_ttl, allow_zero=True)
        self.on = on
        self.maxsize = maxsize
        self._key = key
        self._clock = clock
        self._lock = threading.Lock()
        # key -> (stored_at, value); ordered for LRU eviction.
        self._entries: OrderedDict[Hashable, tuple[float, Any]] = OrderedDict()

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

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        key = self._make_key(fn, args, kwargs)
        value, age = self._lookup(key)
        if value is not _MISS and age <= self.ttl:
            return value
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:
            return self._serve_stale(key, exc)
        self._store(key, result)
        return result

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        key = self._make_key(fn, args, kwargs)
        value, age = self._lookup(key)
        if value is not _MISS and age <= self.ttl:
            return value
        try:
            result = await fn(*args, **kwargs)
        except Exception as exc:
            return self._serve_stale(key, exc)
        self._store(key, result)
        return result
