"""Shared machinery: the ``Policy`` base class and exception filtering.

A policy is a small object that knows how to run a callable with some
resilience behaviour wrapped around it. Applying a policy instance as a
decorator works on regular functions, ``async def`` functions and (for the
policies that support them) generator and async generator functions: the
wrapper flavour is chosen once, at decoration time.

This module also owns two pieces of process-wide state that every policy
consults: the ambient deadline (see :mod:`nopanic.deadline`) and the sleep
hook that :mod:`nopanic.testing` uses to make waits instant.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import inspect
import math
import sys
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from contextvars import ContextVar
from typing import Any, ClassVar, ParamSpec, TypeVar

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


def positive_int(name: str, value: int) -> int:
    """Validate a count parameter: an int >= 1 (bools rejected)."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int, got {type(value).__name__}")
    if value < 1:
        raise ValueError(f"{name} must be >= 1, got {value!r}")
    return value


def finite_hint(value: object) -> float | None:
    """Return a usable positive ``retry_after``-style hint, or None."""
    if (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    ):
        return float(value)
    return None


# -- ambient deadline -----------------------------------------------------------

# Absolute time.monotonic() value by which the current logical operation must
# be done, or None. Set by nopanic.deadline; read by every waiting policy.
_deadline: ContextVar[float | None] = ContextVar("nopanic_deadline", default=None)


def time_remaining() -> float | None:
    """Seconds left before the innermost active :class:`~nopanic.deadline`.

    None when no deadline is active. Can be negative once it has passed.
    Handy for handing the remaining budget to a client's own timeout
    parameter: ``client.get(url, timeout=time_remaining())``.
    """
    deadline = _deadline.get()
    if deadline is None:
        return None
    return deadline - time.monotonic()


# -- async backend ------------------------------------------------------------------


def running_trio() -> bool:
    """True when called from inside ``trio.run`` (or AnyIO's trio backend).

    asyncio is checked first and costs one C call; trio is only consulted
    when no asyncio loop is running, and only if the application imported
    it. nopanic never imports trio on its own.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        trio = sys.modules.get("trio")
        if trio is None:
            return False
        try:
            trio.lowlevel.current_task()
        except RuntimeError:
            return False
        return True
    return False


# -- sleeping (patched by nopanic.testing) ------------------------------------------

# When not None, waits are recorded here and skipped (nopanic.testing.instant).
_recorded_sleeps: list[float] | None = None


def sleep(seconds: float) -> None:
    recorded = _recorded_sleeps
    if recorded is not None:
        recorded.append(seconds)
        return
    time.sleep(seconds)


async def asleep(seconds: float) -> None:
    recorded = _recorded_sleeps
    if recorded is not None:
        recorded.append(seconds)
        seconds = 0.0  # still yield: instant, not synchronous
    if running_trio():
        await sys.modules["trio"].sleep(seconds)
    else:
        await asyncio.sleep(seconds)


def fn_name(fn: Callable[..., Any]) -> str:
    return getattr(fn, "__qualname__", None) or repr(fn)


class Policy(ABC):
    """Base class for all resilience policies.

    Subclasses implement ``_run_sync`` and ``_run_async``; instances are
    then usable as decorators on both sync and async callables, and can be
    stacked or combined with :func:`nopanic.compose`. For a one-off call
    there is nothing to decorate: ``policy.call(fn, *args)`` and
    ``await policy.acall(fn, *args)`` run *fn* under the policy directly.

    Policies that can guard a *stream* (a generator or async generator
    function, e.g. an LLM token stream) also implement ``_run_sync_gen`` /
    ``_run_async_gen`` and list the flavours in ``_STREAMS``. Decorating a
    generator function with a policy that cannot guard it raises
    ``TypeError`` at decoration time instead of silently doing nothing.
    """

    # Subclasses declare their own __slots__: policies are long-lived shared
    # objects, and slots make attribute typos loud instead of silent.
    __slots__ = ()

    #: Stream flavours this policy can guard: "sync" and/or "async".
    _STREAMS: ClassVar[frozenset[str]] = frozenset()

    @abstractmethod
    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any: ...

    @abstractmethod
    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any: ...

    def _run_sync_gen(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Generator[Any, None, None]:
        raise TypeError(f"{type(self).__name__} does not support generator functions")

    def _run_async_gen(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> AsyncGenerator[Any, None]:
        raise TypeError(f"{type(self).__name__} does not support async generator functions")

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

    def _require_stream(self, flavour: str, fn: Callable[..., Any]) -> None:
        if flavour not in self._STREAMS:
            kind = "async generator" if flavour == "async" else "generator"
            raise TypeError(
                f"{type(self).__name__} cannot guard the {kind} function {fn_name(fn)}"
            )

    def __call__(self, fn: F) -> F:
        # Async generators first: they are not coroutine functions, and a
        # plain sync wrapper would silently guard nothing.
        if inspect.isasyncgenfunction(fn):
            self._require_stream("async", fn)

            @functools.wraps(fn)
            async def async_gen_wrapper(*args: Any, **kwargs: Any) -> AsyncGenerator[Any, None]:
                async with contextlib.aclosing(self._run_async_gen(fn, args, kwargs)) as items:
                    async for item in items:
                        yield item

            return async_gen_wrapper  # type: ignore[return-value]

        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                return await self._run_async(fn, args, kwargs)

            return async_wrapper  # type: ignore[return-value]

        if inspect.isgeneratorfunction(fn):
            self._require_stream("sync", fn)

            @functools.wraps(fn)
            def sync_gen_wrapper(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
                with contextlib.closing(self._run_sync_gen(fn, args, kwargs)) as items:
                    yield from items

            return sync_gen_wrapper  # type: ignore[return-value]

        @functools.wraps(fn)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            return self._run_sync(fn, args, kwargs)

        return sync_wrapper  # type: ignore[return-value]
