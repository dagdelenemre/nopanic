"""Combine several policies into a single decorator."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

F = TypeVar("F", bound=Callable[..., Any])

__all__ = ["compose"]


def compose(*policies: Callable[[F], F]) -> Callable[[F], F]:
    """Stack policies into one decorator; the first argument is outermost.

    ``compose(retry(...), timeout(2.0))`` behaves exactly like writing::

        @retry(...)      # outermost: retries the whole timed call
        @timeout(2.0)    # innermost: bounds each individual attempt
        def call(): ...

    A typical production stack, outermost to innermost:
    ``fallback → retry → circuit_breaker → timeout``.
    """

    def decorate(fn: F) -> F:
        for policy in reversed(policies):
            fn = policy(fn)
        return fn

    return decorate
