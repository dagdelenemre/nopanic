"""Circuit breaker: stop hammering a dependency that is already down.

State machine (resilience4j-style, sliding time window):

- **closed** — calls flow through; outcomes are recorded in a sliding time
  window. When at least ``min_calls`` outcomes are in the window and the
  failure rate reaches ``failure_threshold`` (or, when slow-call tracking is
  on, the slow-call rate reaches ``slow_call_threshold``), the breaker opens.
- **open** — calls fail immediately with :class:`CircuitOpen` (no load on the
  dependency). After ``reset_timeout`` seconds the breaker moves to half-open.
- **half_open** — up to ``half_open_max_calls`` probe calls are let through.
  If they all succeed (and are not slow) the breaker closes (window
  cleared); any failure re-opens it.

Two manual states never change on their own: **forced_open** (every call
rejected, see :meth:`CircuitBreaker.force_open`) and **disabled** (every
call passes, nothing recorded, see :meth:`CircuitBreaker.disable`).
:meth:`CircuitBreaker.reset` returns to closed from anywhere.

A breaker instance holds shared state on purpose: create it once and apply
it to every call site that talks to the same dependency.
"""

from __future__ import annotations

import contextlib
import logging
import threading
import time
from collections.abc import AsyncGenerator, Callable, Generator
from dataclasses import dataclass
from types import TracebackType
from typing import Any, ClassVar

from ._core import ExcFilter, Policy, exc_matches, positive_int, positive_number
from ._window import SlidingWindow
from .events import emit
from .exceptions import CircuitOpen

__all__ = ["BreakerMetrics", "CircuitBreaker", "circuit_breaker"]

_log = logging.getLogger(__name__)

_CLOSED = "closed"
_OPEN = "open"
_HALF_OPEN = "half_open"
_FORCED_OPEN = "forced_open"
_DISABLED = "disabled"


@dataclass(frozen=True, slots=True)
class BreakerMetrics:
    """Point-in-time view of a breaker, for health checks and dashboards.

    Attributes:
        state: Current state (see the module docstring).
        calls: Outcomes currently inside the sliding window.
        failures: Failed outcomes inside the window.
        slow_calls: Slow outcomes inside the window (0 unless slow-call
            tracking is enabled).
        failure_rate: ``failures / calls``, or 0.0 for an empty window.
        slow_call_rate: ``slow_calls / calls``, or 0.0 for an empty window.
    """

    state: str
    calls: int
    failures: int
    slow_calls: int
    failure_rate: float
    slow_call_rate: float


class CircuitBreaker(Policy):
    """Failure-rate circuit breaker over a sliding time window.

    Args:
        failure_threshold: Failure rate in [0, 1] that opens the breaker.
        min_calls: Minimum outcomes in the window before the rate is judged
            (prevents one early failure from opening a cold breaker).
        window: Look-back period for the failure rate, in seconds. This is
            not a per-call timeout: a slow call is recorded once it finishes.
        window_buckets: Number of fixed time buckets the window is divided
            into. Window memory is constant in the bucket count regardless
            of traffic; outcomes expire in bucket-sized steps, so more
            buckets means finer expiry precision.
        reset_timeout: Seconds to stay open before allowing probe calls.
        half_open_max_calls: Number of concurrent probe calls allowed while
            half-open; that many successes close the breaker.
        on: Which exceptions count as failures. Exceptions not selected are
            propagated without being recorded at all.
        slow_call_duration: When set, a call taking at least this many
            seconds counts as *slow* (whether it succeeded or failed); for a
            stream, the time to its first item is what counts. A dependency
            that answers, but too slowly, is often as harmful as one that
            fails. ``None`` (default) disables slow-call tracking and costs
            nothing.
        slow_call_threshold: Slow-call rate in (0, 1] that opens the
            breaker, judged like ``failure_threshold``. A slow half-open
            probe counts as a failed probe.
        name: Diagnostic name (appears in ``CircuitOpen`` messages and
            events).
        on_state_change: Optional callback ``(breaker, old_state, new_state)``
            invoked outside the internal lock after every transition. An
            exception raised by the hook is logged and suppressed — it must
            not break the guarded call.
        clock: Monotonic time source; injectable for tests.

    Besides decorating functions (sync, async and generator functions), a
    breaker guards a block of code via ``with breaker.guard():`` or
    ``async with breaker.guard():``.
    """

    __slots__ = (
        "_clock",
        "_half_open_inflight",
        "_half_open_successes",
        "_lock",
        "_opened_at",
        "_slow",
        "_state",
        "_window",
        "failure_threshold",
        "half_open_max_calls",
        "min_calls",
        "name",
        "on",
        "on_state_change",
        "reset_timeout",
        "slow_call_duration",
        "slow_call_threshold",
        "window",
        "window_buckets",
    )

    _STREAMS: ClassVar[frozenset[str]] = frozenset({"sync", "async"})

    def __init__(
        self,
        *,
        failure_threshold: float = 0.5,
        min_calls: int = 5,
        window: float = 30.0,
        window_buckets: int = 10,
        reset_timeout: float = 30.0,
        half_open_max_calls: int = 1,
        on: ExcFilter = Exception,
        slow_call_duration: float | None = None,
        slow_call_threshold: float = 1.0,
        name: str | None = None,
        on_state_change: Callable[[CircuitBreaker, str, str], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        positive_number("failure_threshold", failure_threshold)
        if not 0.0 < failure_threshold <= 1.0:
            raise ValueError("failure_threshold must be in (0, 1]")
        positive_number("slow_call_threshold", slow_call_threshold)
        if not 0.0 < slow_call_threshold <= 1.0:
            raise ValueError("slow_call_threshold must be in (0, 1]")
        self.failure_threshold = float(failure_threshold)
        self.slow_call_threshold = float(slow_call_threshold)
        self.min_calls = positive_int("min_calls", min_calls)
        self.window = positive_number("window", window)
        self.window_buckets = positive_int("window_buckets", window_buckets)
        self.reset_timeout = positive_number("reset_timeout", reset_timeout, allow_zero=True)
        self.half_open_max_calls = positive_int("half_open_max_calls", half_open_max_calls)
        self.slow_call_duration = (
            None
            if slow_call_duration is None
            else positive_number("slow_call_duration", slow_call_duration)
        )
        self._slow = self.slow_call_duration
        self.on = on
        self.name = name or f"breaker-{id(self):x}"
        self.on_state_change = on_state_change
        self._clock = clock

        self._lock = threading.Lock()
        self._state = _CLOSED
        self._window = SlidingWindow(self.window, self.window_buckets)
        self._opened_at = 0.0
        self._half_open_inflight = 0
        self._half_open_successes = 0

    # -- public introspection and control -------------------------------------

    @property
    def state(self) -> str:
        """Current state: ``"closed"``, ``"open"``, ``"half_open"``,
        ``"forced_open"`` or ``"disabled"``.

        Reading the state applies the open → half-open timeout transition,
        so it always reflects what the next call would see.
        """
        with self._lock:
            events = self._maybe_half_open()
        self._fire(events)
        return self._state

    @property
    def metrics(self) -> BreakerMetrics:
        """A consistent snapshot of state and window counters."""
        with self._lock:
            events = self._maybe_half_open()
            calls, failures, slow = self._window.totals(self._clock())
            state = self._state
        self._fire(events)
        return BreakerMetrics(
            state=state,
            calls=calls,
            failures=failures,
            slow_calls=slow,
            failure_rate=failures / calls if calls else 0.0,
            slow_call_rate=slow / calls if calls else 0.0,
        )

    def reset(self) -> None:
        """Force the breaker back to closed and clear the window."""
        self._force(_CLOSED)

    def force_open(self) -> None:
        """Reject every call until :meth:`reset` (maintenance, kill switch)."""
        self._force(_FORCED_OPEN)

    def disable(self) -> None:
        """Let every call through and record nothing until :meth:`reset`."""
        self._force(_DISABLED)

    def guard(self) -> _BreakerGuard:
        """Context manager guarding a block: ``with breaker.guard(): ...``.

        Works with ``with`` and ``async with``. An exception leaving the
        block is judged exactly like one raised by a decorated function.
        """
        return _BreakerGuard(self)

    # -- internals -----------------------------------------------------------

    def _force(self, state: str) -> None:
        with self._lock:
            events = self._transition(state)
            self._window.clear()
            self._half_open_inflight = 0
            self._half_open_successes = 0
        self._fire(events)

    def _fire(self, events: list[tuple[str, str]], fn: Callable[..., Any] | None = None) -> None:
        if not events:
            return
        for old, new in events:
            emit("breaker.state_change", "circuit_breaker", self.name, fn, old=old, new=new)
        if self.on_state_change is None:
            return
        for old, new in events:
            try:
                self.on_state_change(self, old, new)
            except Exception:
                _log.exception(
                    "on_state_change hook for %r raised; ignoring so the call proceeds",
                    self.name,
                )

    def _transition(self, new: str) -> list[tuple[str, str]]:
        # Caller must hold the lock. Returns events to fire after release.
        old = self._state
        if old == new:
            return []
        self._state = new
        return [(old, new)]

    def _maybe_half_open(self) -> list[tuple[str, str]]:
        # Caller must hold the lock.
        if self._state == _OPEN and self._clock() - self._opened_at >= self.reset_timeout:
            self._half_open_inflight = 0
            self._half_open_successes = 0
            return self._transition(_HALF_OPEN)
        return []

    def _before_call(self, fn: Callable[..., Any] | None) -> str:
        """Admit or reject the call; returns the state it was admitted under."""
        reject: CircuitOpen | None = None
        admitted = _CLOSED
        with self._lock:
            events = self._maybe_half_open()
            state = self._state
            if state == _OPEN:
                retry_after = self.reset_timeout - (self._clock() - self._opened_at)
                reject = CircuitOpen(self.name, max(0.0, retry_after))
            elif state == _HALF_OPEN:
                if self._half_open_inflight >= self.half_open_max_calls:
                    reject = CircuitOpen(self.name, 0.0)
                else:
                    self._half_open_inflight += 1
                    admitted = _HALF_OPEN
            elif state == _FORCED_OPEN:
                reject = CircuitOpen(self.name, self.reset_timeout)
            elif state == _DISABLED:
                admitted = _DISABLED
        if events:
            self._fire(events, fn)
        if reject is not None:
            emit(
                "breaker.rejected",
                "circuit_breaker",
                self.name,
                fn,
                retry_after=reject.retry_after,
            )
            raise reject
        return admitted

    def _is_slow(self, elapsed: float | None) -> bool:
        return elapsed is not None and self._slow is not None and elapsed >= self._slow

    def _on_success(self, admitted: str, elapsed: float | None = None) -> None:
        if admitted == _DISABLED:
            return
        # Inlined (hot path): elapsed is None unless slow tracking is on.
        slow = elapsed is not None and self._slow is not None and elapsed >= self._slow
        events: list[tuple[str, str]] = []
        with self._lock:
            if admitted == _HALF_OPEN and self._state == _HALF_OPEN:
                self._half_open_inflight -= 1
                if slow:  # answered, but still too slowly: not healthy yet
                    self._opened_at = self._clock()
                    events = self._transition(_OPEN)
                else:
                    self._half_open_successes += 1
                    if self._half_open_successes >= self.half_open_max_calls:
                        self._window.clear()
                        events = self._transition(_CLOSED)
            elif self._state == _CLOSED:
                now = self._clock()
                self._window.record(now, True, slow)
                if slow:
                    events = self._judge(now)
        if events:
            self._fire(events)

    def _on_failure(self, admitted: str, elapsed: float | None = None) -> None:
        if admitted == _DISABLED:
            return
        slow = self._is_slow(elapsed)
        events: list[tuple[str, str]] = []
        with self._lock:
            now = self._clock()
            if admitted == _HALF_OPEN and self._state == _HALF_OPEN:
                self._half_open_inflight -= 1
                self._opened_at = now
                events = self._transition(_OPEN)
            elif self._state == _CLOSED:
                self._window.record(now, False, slow)
                events = self._judge(now)
        self._fire(events)

    def _judge(self, now: float) -> list[tuple[str, str]]:
        # Caller must hold the lock; the breaker is closed.
        total, failures, slow = self._window.totals(now)
        if total < self.min_calls:
            return []
        if failures / total >= self.failure_threshold or (
            self._slow is not None and slow / total >= self.slow_call_threshold
        ):
            self._opened_at = now
            return self._transition(_OPEN)
        return []

    def _on_ignored(self, admitted: str) -> None:
        # An exception not selected by `on`, or a cancellation: release the
        # probe slot (if any) without recording an outcome.
        if admitted != _HALF_OPEN:
            return
        with self._lock:
            if self._state == _HALF_OPEN:
                self._half_open_inflight -= 1

    def _on_exception(self, admitted: str, exc: BaseException, elapsed: float | None) -> None:
        if isinstance(exc, Exception) and exc_matches(exc, self.on):
            self._on_failure(admitted, elapsed)
        else:
            # Unselected exceptions, and cancellation (a hedge loser, an
            # outer timeout, a client that hung up) or interpreter exit, say
            # nothing about the dependency; but a probe slot this call holds
            # must be returned or the breaker stays half-open forever.
            self._on_ignored(admitted)

    def _elapsed(self, start: float) -> float | None:
        return None if self._slow is None else self._clock() - start

    def _start(self) -> float:
        return 0.0 if self._slow is None else self._clock()

    # -- execution -------------------------------------------------------------

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        admitted = self._before_call(fn)
        if self._slow is None:  # hot path: no clock reads at all
            try:
                result = fn(*args, **kwargs)
            except BaseException as exc:
                self._on_exception(admitted, exc, None)
                raise
            self._on_success(admitted)
            return result
        start = self._clock()
        try:
            result = fn(*args, **kwargs)
        except BaseException as exc:
            self._on_exception(admitted, exc, self._clock() - start)
            raise
        self._on_success(admitted, self._clock() - start)
        return result

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        admitted = self._before_call(fn)
        if self._slow is None:  # hot path: no clock reads at all
            try:
                result = await fn(*args, **kwargs)
            except BaseException as exc:
                self._on_exception(admitted, exc, None)
                raise
            self._on_success(admitted)
            return result
        start = self._clock()
        try:
            result = await fn(*args, **kwargs)
        except BaseException as exc:
            self._on_exception(admitted, exc, self._clock() - start)
            raise
        self._on_success(admitted, self._clock() - start)
        return result

    # Streams: admitted once, judged once when the stream ends. A consumer
    # that stops early after receiving items counts as a success (the
    # dependency delivered); before any item, as no outcome at all.

    def _run_sync_gen(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Generator[Any, None, None]:
        admitted = self._before_call(fn)
        start = self._start()
        delivered = False
        first: float | None = None  # time to first item (slow-call tracking)
        try:
            with contextlib.closing(fn(*args, **kwargs)) as items:
                for item in items:
                    if not delivered:
                        delivered = True
                        first = self._elapsed(start)
                    yield item
        except GeneratorExit:
            if delivered:
                self._on_success(admitted, first)
            else:
                self._on_ignored(admitted)
            raise
        except BaseException as exc:
            self._on_exception(admitted, exc, first if delivered else self._elapsed(start))
            raise
        self._on_success(admitted, first if delivered else self._elapsed(start))

    async def _run_async_gen(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> AsyncGenerator[Any, None]:
        admitted = self._before_call(fn)
        start = self._start()
        delivered = False
        first: float | None = None  # time to first item (slow-call tracking)
        try:
            async with contextlib.aclosing(fn(*args, **kwargs)) as items:
                async for item in items:
                    if not delivered:
                        delivered = True
                        first = self._elapsed(start)
                    yield item
        except GeneratorExit:
            if delivered:
                self._on_success(admitted, first)
            else:
                self._on_ignored(admitted)
            raise
        except BaseException as exc:
            self._on_exception(admitted, exc, first if delivered else self._elapsed(start))
            raise
        self._on_success(admitted, first if delivered else self._elapsed(start))


class _BreakerGuard:
    """One guarded block; a fresh object per ``guard()`` so it is reentrant."""

    __slots__ = ("_admitted", "_breaker", "_start")

    def __init__(self, breaker: CircuitBreaker) -> None:
        self._breaker = breaker
        self._admitted = _CLOSED
        self._start = 0.0

    def __enter__(self) -> _BreakerGuard:
        self._admitted = self._breaker._before_call(None)
        self._start = self._breaker._start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        elapsed = self._breaker._elapsed(self._start)
        if exc is None:
            self._breaker._on_success(self._admitted, elapsed)
        else:
            self._breaker._on_exception(self._admitted, exc, elapsed)

    async def __aenter__(self) -> _BreakerGuard:
        return self.__enter__()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.__exit__(exc_type, exc, tb)


# Lowercase alias so all policies read uniformly at decoration sites.
circuit_breaker = CircuitBreaker
