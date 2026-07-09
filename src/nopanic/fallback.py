"""Fallback policy: degrade gracefully instead of failing."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

from ._core import ExcFilter, Policy, exc_matches

__all__ = ["fallback"]


class fallback(Policy):
    """Return a substitute result when the call raises a matching exception.

    Args:
        handler: Either a plain value to return, or a callable receiving the
            exception and returning the substitute (async callables are
            awaited when the wrapped function is async).
        on: Which exceptions trigger the fallback; others propagate.

    Typically the outermost policy in a stack: everything the inner layers
    could not absorb turns into a degraded-but-valid response.
    """

    __slots__ = ("handler", "on")

    def __init__(self, handler: Any, *, on: ExcFilter = Exception) -> None:
        self.handler = handler
        self.on = on

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            if not exc_matches(exc, self.on):
                raise
            if inspect.iscoroutinefunction(self.handler):
                raise TypeError(
                    "async fallback handler cannot be used with a sync function"
                ) from exc
            if callable(self.handler):
                return self.handler(exc)
            return self.handler

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        try:
            return await fn(*args, **kwargs)
        except Exception as exc:
            if not exc_matches(exc, self.on):
                raise
            if callable(self.handler):
                result = self.handler(exc)
                if inspect.isawaitable(result):
                    return await result
                return result
            return self.handler
