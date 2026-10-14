"""Shared machinery: the ``Policy`` base class and exception filtering.

A policy is a small object that knows how to run a callable with some
resilience behaviour wrapped around it. Applying a policy instance as a
decorator works on both regular and ``async def`` functions — the wrapper
flavour is chosen once, at decoration time.
"""

from __future__ import annotations

import functools
import inspect
import math
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any, ParamSpec, TypeVar

F = TypeVar("F", bound=Callable[..., Any])
P = ParamSpec("P")
R = TypeVar("R")

# An exception filter: a type, a tuple of types, or a predicate over the
# raised exception. Used by retry(on=...), fallback(on=...), breaker(on=...).
ExcFilter = (
    type[BaseException] | tuple[type[BaseException], ...] | Callable[[BaseException], bool]
)


def exc_matches(exc: BaseException, on: ExcFilter) -> bool:
    """Return True if *exc* is selected by the filter *on*."""
    if isinstance(on, (type, tuple)):
        return isinstance(exc, on)
    return bool(on(exc))


def positive_number(name: str, value: float, *, allow_zero: bool = False) -> float:
    """Validate a numeric policy parameter: a real, finite, positive number.

    NaN and infinity are rejected eagerly — accepted silently, they would
    disable threshold comparisons deep inside a policy at the worst possible
    moment (a NaN failure rate never trips a breaker). Booleans are rejected
    because ``True`` quietly meaning ``1.0`` is a bug, not a feature.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number, got {type(value).__name__}")
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite, got {value!r}")
    if value < 0.0 or (value == 0.0 and not allow_zero):
        bound = ">= 0" if allow_zero else "> 0"
        raise ValueError(f"{name} must be {bound}, got {value!r}")
    return float(value)


class Policy(ABC):
    """Base class for all resilience policies.

    Subclasses implement ``_run_sync`` and ``_run_async``; instances are
    then usable as decorators on both sync and async callables, and can be
    stacked or combined with :func:`nopanic.compose`. For a one-off call
    there is nothing to decorate: ``policy.call(fn, *args)`` and
    ``await policy.acall(fn, *args)`` run *fn* under the policy directly.
    """

    # Subclasses declare their own __slots__: policies are long-lived shared
    # objects, and slots make attribute typos loud instead of silent.
    __slots__ = ()

    @abstractmethod
    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any: ...

    @abstractmethod
    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any: ...

    def call(self, fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        """Run ``fn(*args, **kwargs)`` under this policy (sync)."""
        result: R = self._run_sync(fn, args, kwargs)
        return result

    async def acall(
        self, fn: Callable[P, Awaitable[R]], /, *args: P.args, **kwargs: P.kwargs
    ) -> R:
        """Run and await ``fn(*args, **kwargs)`` under this policy.

        *fn* only needs to return an awaitable: a bound method of an async
        client or a lambda wrapping one works as well as an ``async def``.
        """
        result: R = await self._run_async(fn, args, kwargs)
        return result

    def __call__(self, fn: F) -> F:
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                return await self._run_async(fn, args, kwargs)

            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(fn)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            return self._run_sync(fn, args, kwargs)

        return sync_wrapper  # type: ignore[return-value]
