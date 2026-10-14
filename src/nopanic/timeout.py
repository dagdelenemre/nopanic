"""Timeout policy: bound how long a single call may take."""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import sys
import threading
from collections.abc import AsyncGenerator, Awaitable, Callable
from types import TracebackType
from typing import Any, ClassVar, TypeVar

from . import _core
from ._core import Policy, fn_name, positive_int, positive_number
from .events import emit

__all__ = ["timeout"]

T = TypeVar("T")


class _Worker:
    """Outcome slot shared between a sync caller and its worker thread."""

    __slots__ = ("abandoned", "done", "error", "finished", "result")

    def __init__(self) -> None:
        self.done = threading.Event()
        self.result: Any = None
        self.error: BaseException | None = None
        self.finished = False
        self.abandoned = False


class timeout(Policy):
    """Fail with :class:`TimeoutError` if the call takes longer than *seconds*.

    Args:
        seconds: Time limit per call.
        first_item: For streams only: limit for the *first* item, which is
            often slower than the rest (an LLM's time to first token).
            Defaults to ``seconds``.
        max_abandoned: Sync calls only: how many timed-out worker threads
            may still be running before new calls fail fast with
            ``TimeoutError`` instead of starting another thread. Bounds the
            resources a hung dependency can pin. ``None`` means unbounded.
        name: Diagnostic name carried by this policy's events.

    Async functions are cancelled cleanly via the event loop. Sync functions
    are executed on a daemon worker thread and **abandoned** on timeout —
    Python cannot kill a running thread, so the underlying work may continue
    in the background even after the caller has received ``TimeoutError``.
    Prefer the async path (or the callee's own timeout parameter, e.g. an
    HTTP client's) when abandonment is not acceptable. The worker runs in a
    copy of the caller's context, so ``contextvars`` (tracing spans, request
    IDs, structured-logging fields) are visible inside the call.

    Only an expiry of *this* timeout raises a new ``TimeoutError``. A
    ``TimeoutError`` raised by the wrapped function itself (its own I/O
    timeout, say) propagates unchanged, like any other user exception.

    Inside an active :class:`~nopanic.deadline` the limit is clamped to the
    time remaining, and a call made after the deadline fails at once.

    On an async generator function the limit applies to *each item* (a
    stalled stream fails even if it started fine), with ``first_item`` for
    the first one. Sync generators are not supported: a worker thread per
    item would be the only way to bound them. ``async with
    policy.guard():`` bounds a block of async code.
    """

    __slots__ = ("_abandoned", "_lock", "first_item", "max_abandoned", "name", "seconds")

    _STREAMS: ClassVar[frozenset[str]] = frozenset({"async"})

    def __init__(
        self,
        seconds: float,
        *,
        first_item: float | None = None,
        max_abandoned: int | None = 64,
        name: str | None = None,
    ) -> None:
        self.seconds = positive_number("seconds", seconds)
        self.first_item = (
            None if first_item is None else positive_number("first_item", first_item)
        )
        self.max_abandoned = (
            None if max_abandoned is None else positive_int("max_abandoned", max_abandoned)
        )
        self.name = name
        self._lock = threading.Lock()
        self._abandoned = 0

    @property
    def abandoned(self) -> int:
        """Timed-out sync calls whose worker threads are still running."""
        with self._lock:
            return self._abandoned

    def guard(self) -> _TimeoutGuard:
        """Bound a block of async code: ``async with policy.guard(): ...``."""
        return _TimeoutGuard(self)

    def _expired(self, fn: Callable[..., Any] | None, seconds: float) -> TimeoutError:
        what = "block" if fn is None else fn_name(fn)
        emit(
            "timeout.expired",
            "timeout",
            self.name,
            fn,
            seconds=seconds,
            function=what,
        )
        return TimeoutError(f"{what} did not finish within {seconds:g}s")

    def _limit(self, fn: Callable[..., Any] | None, seconds: float) -> float:
        """The effective limit: *seconds*, clamped to an ambient deadline."""
        remaining = _core.time_remaining()
        if remaining is None or remaining >= seconds:
            return seconds
        if remaining <= 0.0:
            raise self._expired(fn, 0.0)
        return remaining

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        seconds = self._limit(fn, self.seconds)
        saturated = False
        with self._lock:
            if self.max_abandoned is not None and self._abandoned >= self.max_abandoned:
                saturated = True
                abandoned = self._abandoned
        if saturated:
            emit(
                "timeout.saturated",
                "timeout",
                self.name,
                fn,
                abandoned=abandoned,
                max_abandoned=self.max_abandoned,
            )
            raise TimeoutError(
                f"{fn_name(fn)}: {abandoned} earlier timed-out calls are still running "
                f"(max_abandoned={self.max_abandoned}); failing fast instead of starting "
                "another worker thread"
            )

        state = _Worker()
        context = contextvars.copy_context()

        def target() -> None:
            try:
                state.result = context.run(fn, *args, **kwargs)
            except BaseException as exc:
                state.error = exc
            finally:
                with self._lock:
                    state.finished = True
                    if state.abandoned:
                        self._abandoned -= 1
                state.done.set()

        worker = threading.Thread(target=target, name=f"nopanic-timeout-{fn_name(fn)}", daemon=True)
        worker.start()
        if not state.done.wait(seconds):
            with self._lock:
                # The worker may have finished between the wait and the lock:
                # then its outcome is simply used.
                if not state.finished:
                    state.abandoned = True
                    self._abandoned += 1
            if state.abandoned:
                error = self._expired(fn, seconds)
                error.args = (f"{error.args[0]} (worker thread abandoned, not killed)",)
                raise error
        if state.error is not None:
            raise state.error
        return state.result

    if sys.version_info >= (3, 11):

        async def _await_within(
            self, awaitable: Awaitable[T], seconds: float, fn: Callable[..., Any] | None
        ) -> T:
            # asyncio.timeout runs in the caller's own task (no extra task,
            # context shared) and tells an expiry of ours apart from a
            # TimeoutError the function raised itself: since 3.11 both are
            # the same class, and only the former may be replaced.
            scope = asyncio.timeout(seconds)
            try:
                async with scope:
                    return await awaitable
            except TimeoutError as exc:
                if not scope.expired():
                    raise
                raise self._expired(fn, seconds) from exc

    else:

        async def _await_within(
            self, awaitable: Awaitable[T], seconds: float, fn: Callable[..., Any] | None
        ) -> T:
            try:
                return await asyncio.wait_for(awaitable, timeout=seconds)
            except asyncio.TimeoutError as exc:  # not the builtin TimeoutError on 3.10
                raise self._expired(fn, seconds) from exc

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        seconds = self._limit(fn, self.seconds)
        return await self._await_within(fn(*args, **kwargs), seconds, fn)

    async def _run_async_gen(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> AsyncGenerator[Any, None]:
        limit = self.first_item if self.first_item is not None else self.seconds
        async with contextlib.aclosing(fn(*args, **kwargs)) as items:
            while True:
                seconds = self._limit(fn, limit)
                try:
                    item = await self._await_within(items.__anext__(), seconds, fn)
                except StopAsyncIteration:
                    return
                # The limit covers waiting for the stream, never the time the
                # consumer spends between items: no timer runs across a yield.
                yield item
                limit = self.seconds


class _TimeoutGuard:
    """One bounded async block; a fresh object per ``guard()``."""

    __slots__ = ("_fired", "_handle", "_policy", "_scope", "_seconds")

    def __init__(self, policy: timeout) -> None:
        self._policy = policy
        self._seconds = policy.seconds
        self._scope: Any = None
        self._handle: asyncio.TimerHandle | None = None
        self._fired = False

    def __enter__(self) -> _TimeoutGuard:
        raise TypeError(
            "timeout cannot interrupt a synchronous block; decorate the function instead"
        )

    def __exit__(self, *exc_info: object) -> None:  # pragma: no cover - never entered
        return None

    async def __aenter__(self) -> _TimeoutGuard:
        self._seconds = self._policy._limit(None, self._policy.seconds)
        if sys.version_info >= (3, 11):
            self._scope = asyncio.timeout(self._seconds)
            await self._scope.__aenter__()
        else:
            task = asyncio.current_task()
            if task is None:
                raise RuntimeError("timeout guard must be used inside a task")

            def fire() -> None:
                self._fired = True
                task.cancel()

            self._handle = asyncio.get_running_loop().call_later(self._seconds, fire)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._scope is not None:
            try:
                await self._scope.__aexit__(exc_type, exc, tb)
            except TimeoutError as error:
                if self._scope.expired():
                    raise self._policy._expired(None, self._seconds) from error
                raise
            return
        if self._handle is not None:
            self._handle.cancel()
        if self._fired and isinstance(exc, asyncio.CancelledError):
            raise self._policy._expired(None, self._seconds) from exc
