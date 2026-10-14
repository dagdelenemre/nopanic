"""Trio implementations of the few async primitives nopanic needs.

Imported only when a policy finds itself running under ``trio.run`` (or
AnyIO on its trio backend), so trio stays an optional, user-supplied
dependency: nopanic itself never requires it. Everything else in nopanic is
backend-neutral already (contextvars, async generators, plain awaits).
"""

from __future__ import annotations

import math
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

import trio

T = TypeVar("T")


async def await_within(awaitable: Awaitable[T], seconds: float) -> tuple[bool, T | None]:
    """Await under a cancel scope: ``(expired, result)``.

    A ``TimeoutError`` raised by the awaited code itself propagates
    untouched; only the scope's own expiry reports ``expired``.
    """
    with trio.move_on_after(seconds) as scope:
        return False, await awaitable
    if scope.cancelled_caught:
        return True, None
    return False, None  # pragma: no cover - the body always returns


def open_scope(seconds: float) -> trio.CancelScope:
    return trio.CancelScope(deadline=trio.current_time() + seconds)


def current_token() -> trio.lowlevel.TrioToken:
    return trio.lowlevel.current_trio_token()


def new_semaphore(value: int) -> trio.Semaphore:
    return trio.Semaphore(value)


def try_acquire(sem: trio.Semaphore) -> bool:
    try:
        sem.acquire_nowait()
    except trio.WouldBlock:
        return False
    return True


async def acquire_within(sem: trio.Semaphore, seconds: float | None) -> bool:
    if seconds is None:
        await sem.acquire()
        return True
    with trio.move_on_after(seconds):
        await sem.acquire()  # trio's acquire is cancellation-safe: no leaked permit
        return True
    return False


def new_event() -> trio.Event:
    return trio.Event()


async def race(
    fn: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    delay: float,
    max_hedges: int,
    transient: Callable[[BaseException], bool],
    on_launch: Callable[[float], None],
) -> Any:
    """Hedged requests on a nursery: first success wins, losers cancelled.

    Same contract as the asyncio implementation in :mod:`nopanic.hedge`.
    Attempt outcomes travel over a memory channel instead of escaping the
    nursery, so callers see the original exception, never an
    ``ExceptionGroup``.
    """
    send, receive = trio.open_memory_channel[tuple[bool, Any]](math.inf)

    async def attempt() -> None:
        try:
            result = await fn(*args, **kwargs)
        except Exception as exc:
            send.send_nowait((False, exc))
        else:
            send.send_nowait((True, result))

    hedges_left = max_hedges
    running = 1
    failure: BaseException | None = None
    async with trio.open_nursery() as nursery:
        nursery.start_soon(attempt)
        while True:
            with trio.move_on_after(delay if hedges_left > 0 else math.inf) as waited:
                ok, value = await receive.receive()
            if waited.cancelled_caught:
                on_launch(delay)
                nursery.start_soon(attempt)
                hedges_left -= 1
                running += 1
                continue
            running -= 1
            if ok:
                nursery.cancel_scope.cancel()
                return value
            failure = value
            if not transient(value):
                nursery.cancel_scope.cancel()
                break
            if running == 0:
                if hedges_left == 0:
                    break
                on_launch(0.0)
                nursery.start_soon(attempt)
                hedges_left -= 1
                running += 1
    if failure is None:  # pragma: no cover - the loop only breaks on a failure
        raise RuntimeError("hedge finished with no result and no exception")
    raise failure
