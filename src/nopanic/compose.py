"""Combine several policies into a single decorator."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from ._core import F, P, R

__all__ = ["compose"]


class compose:
    """Stack policies into one decorator; the first argument is outermost.

    ``compose(retry(...), timeout(2.0))`` behaves exactly like writing::

        @retry(...)      # outermost: retries the whole timed call
        @timeout(2.0)    # innermost: bounds each individual attempt
        def call(): ...

    A typical production stack, outermost to innermost:
    ``fallback → retry → circuit_breaker → timeout``.

    Like a single policy, a composed stack also runs one-off calls without
    decorating anything: ``stack.call(fn, *args)`` / ``await
    stack.acall(fn, *args)``. Those build the wrapper chain per call (a few
    microseconds); decorate once for hot paths.
    """

    __slots__ = ("policies",)

    def __init__(self, *policies: Callable[[Any], Any]) -> None:
        self.policies = policies

    def __call__(self, fn: F) -> F:
        wrapped: Any = fn
        for policy in reversed(self.policies):
            wrapped = policy(wrapped)
        decorated: F = wrapped
        return decorated

    def call(self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        """Run ``fn(*args, **kwargs)`` through the whole stack (sync)."""
        result: R = self(fn)(*args, **kwargs)
        return result

    async def acall(
        self, fn: Callable[P, Awaitable[R]], /, *args: P.args, **kwargs: P.kwargs
    ) -> R:
        """Run and await ``fn(*args, **kwargs)`` through the whole stack."""

        # Policies pick their wrapper flavour by inspecting the function, and
        # a lambda or bound method returning an awaitable does not look async.
        async def shim(*a: Any, **k: Any) -> Any:
            return await fn(*a, **k)

        result: R = await self(shim)(*args, **kwargs)
        return result
