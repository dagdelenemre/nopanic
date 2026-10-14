"""Deadline propagation: one time budget for a whole operation.

A timeout bounds one call; a deadline bounds everything inside a block, no
matter how many retries, waits and nested calls it takes::

    with deadline(5.0):              # or: async with deadline(5.0)
        profile = get_profile(uid)   # retries, timeouts, rate-limit waits...
        orders = get_orders(uid)     # ...all share the same 5 seconds

The deadline is ambient (a ``contextvars`` value, so it follows the code
into awaited coroutines and ``timeout`` worker threads) and cooperative:
policies inside the block respect it, nothing is killed from outside.

- ``timeout`` clamps its own limit to the time remaining, and fails at once
  if the deadline has already passed.
- ``retry`` gives up instead of sleeping into a wait that would outlast the
  deadline.
- ``rate_limit``, ``adaptive_rate_limit`` and ``bulkhead`` never wait past
  it; they reject with ``RateLimited`` / ``BulkheadFull`` instead.

Nested deadlines can only shorten the budget, never extend it. Your own code
can read the remainder with :func:`nopanic.time_remaining` and pass it on,
e.g. as an HTTP client's timeout. To *enforce* the budget on a call, put a
``timeout`` inside; it will use whichever limit is tighter.

``deadline`` is also a policy, so ``@deadline(5.0)`` or
``compose(deadline(10.0), retry(...), timeout(2.0))`` gives every call its
own fresh budget.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from contextvars import Token
from types import TracebackType
from typing import Any

from ._core import Policy, _deadline, positive_number

__all__ = ["deadline"]


class deadline(Policy):
    """Run a block, or a decorated call, under a time budget of *seconds*.

    Use as ``with deadline(s):`` / ``async with deadline(s):`` around a
    block, or as a decorator / ``compose`` member to give each call its own
    budget. One instance must not be entered twice at the same time (create
    a new one per block); as a decorator it can be shared freely. Generator
    functions are not supported: a context value set inside a generator
    would leak into the consumer between items.
    """

    __slots__ = ("_token", "seconds")

    def __init__(self, seconds: float) -> None:
        self.seconds = positive_number("seconds", seconds)
        self._token: Token[float | None] | None = None

    def _enter(self) -> Token[float | None]:
        expires = time.monotonic() + self.seconds
        outer = _deadline.get()
        if outer is not None and outer < expires:
            expires = outer  # nested deadlines only ever shorten the budget
        return _deadline.set(expires)

    def __enter__(self) -> deadline:
        if self._token is not None:
            raise RuntimeError("this deadline is already active; create a new one per block")
        self._token = self._enter()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        token, self._token = self._token, None
        if token is not None:
            _deadline.reset(token)

    async def __aenter__(self) -> deadline:
        return self.__enter__()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.__exit__(exc_type, exc, tb)

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        token = self._enter()
        try:
            return fn(*args, **kwargs)
        finally:
            _deadline.reset(token)

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        token = self._enter()
        try:
            return await fn(*args, **kwargs)
        finally:
            _deadline.reset(token)
