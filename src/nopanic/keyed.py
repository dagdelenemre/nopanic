"""Per-key policies: one breaker (or limiter, or bulkhead) per host, tenant, model...

A breaker guarding "the payments API" trips for everyone when one region is
down; a rate limit shared by all tenants lets one noisy tenant starve the
rest. ``keyed`` keeps a separate policy instance per key, created on first
use from a factory and bounded by an LRU::

    per_host = keyed(
        lambda host: circuit_breaker(name=f"api-{host}", reset_timeout=10.0),
        key=lambda url, **_: urlsplit(url).hostname,
    )

    @per_host
    async def fetch(url, **params): ...

    per_host.get("eu.api.example.com").state   # inspect one of them
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from collections.abc import AsyncGenerator, Callable, Generator, Hashable
from typing import Any, ClassVar, Generic, TypeVar

from ._core import Policy, positive_int

__all__ = ["keyed"]

PolicyT = TypeVar("PolicyT", bound=Policy)


class keyed(Policy, Generic[PolicyT]):
    """Route each call to its own policy instance, chosen by a key.

    Args:
        factory: ``factory(key) -> policy``; called once per new key, outside
            any lock.
        key: ``key(*args, **kwargs) -> hashable``, computed from each call's
            arguments.
        maxsize: Most instances kept; the least recently used one is
            dropped beyond that (and with it its state: an evicted open
            breaker starts again closed).

    Supports whatever the produced policies support: an unsupported stream
    flavour raises ``TypeError`` when the stream is iterated, since the
    policy type is only known once the factory has run.
    """

    __slots__ = ("_instances", "_key", "_lock", "factory", "maxsize")

    _STREAMS: ClassVar[frozenset[str]] = frozenset({"sync", "async"})

    def __init__(
        self,
        factory: Callable[[Any], PolicyT],
        *,
        key: Callable[..., Hashable],
        maxsize: int = 1024,
    ) -> None:
        self.factory = factory
        self._key = key
        self.maxsize = positive_int("maxsize", maxsize)
        self._lock = threading.Lock()
        self._instances: OrderedDict[Hashable, PolicyT] = OrderedDict()

    def get(self, key: Hashable) -> PolicyT:
        """The policy for *key*, created by the factory if needed."""
        with self._lock:
            policy = self._instances.get(key)
            if policy is not None:
                self._instances.move_to_end(key)
                return policy
        created = self.factory(key)  # user code: never under the lock
        with self._lock:
            policy = self._instances.setdefault(key, created)  # a racer may have won
            self._instances.move_to_end(key)
            while len(self._instances) > self.maxsize:
                self._instances.popitem(last=False)
            return policy

    def keys(self) -> list[Hashable]:
        """Keys with a live instance, least recently used first."""
        with self._lock:
            return list(self._instances)

    def _for(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> PolicyT:
        return self.get(self._key(*args, **kwargs))

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        return self._for(args, kwargs)._run_sync(fn, args, kwargs)

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        return await self._for(args, kwargs)._run_async(fn, args, kwargs)

    def _run_sync_gen(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Generator[Any, None, None]:
        return self._for(args, kwargs)._run_sync_gen(fn, args, kwargs)

    def _run_async_gen(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> AsyncGenerator[Any, None]:
        return self._for(args, kwargs)._run_async_gen(fn, args, kwargs)
