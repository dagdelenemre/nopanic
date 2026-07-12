"""Circuit breaker: stop hammering a dependency that is already down.

State machine (resilience4j-style, sliding time window):

- **closed** — calls flow through; outcomes are recorded in a sliding time
  window. When at least ``min_calls`` outcomes are in the window and the
  failure rate reaches ``failure_threshold``, the breaker opens.
- **open** — calls fail immediately with :class:`CircuitOpen` (no load on the
  dependency). After ``reset_timeout`` seconds the breaker moves to half-open.
- **half_open** — up to ``half_open_max_calls`` probe calls are let through.
  If they all succeed the breaker closes (window cleared); any failure
  re-opens it.

A breaker instance holds shared state on purpose: create it once and apply
it to every call site that talks to the same dependency.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from collections.abc import Callable
from typing import Any

from ._core import ExcFilter, Policy, exc_matches, positive_number
from .events import emit
from .exceptions import CircuitOpen

__all__ = ["CircuitBreaker", "circuit_breaker"]

_log = logging.getLogger(__name__)

_CLOSED = "closed"
_OPEN = "open"
_HALF_OPEN = "half_open"


class CircuitBreaker(Policy):
    """Failure-rate circuit breaker over a sliding time window.

    Args:
        failure_threshold: Failure rate in [0, 1] that opens the breaker.
        min_calls: Minimum outcomes in the window before the rate is judged
            (prevents one early failure from opening a cold breaker).
        window: Length of the sliding time window, in seconds.
        reset_timeout: Seconds to stay open before allowing probe calls.
        half_open_max_calls: Number of concurrent probe calls allowed while
            half-open; that many successes close the breaker.
        on: Which exceptions count as failures. Exceptions not selected are
            propagated without being recorded at all.
        name: Diagnostic name (appears in ``CircuitOpen`` messages).
        on_state_change: Optional callback ``(breaker, old_state, new_state)``
            invoked outside the internal lock after every transition. An
            exception raised by the hook is logged and suppressed — it must
            not break the guarded call.
        clock: Monotonic time source; injectable for tests.
    """

    __slots__ = (
        "_clock",
        "_half_open_inflight",
        "_half_open_successes",
        "_lock",
        "_opened_at",
        "_outcomes",
        "_state",
        "failure_threshold",
        "half_open_max_calls",
        "min_calls",
        "name",
        "on",
        "on_state_change",
        "reset_timeout",
        "window",
    )

    def __init__(
        self,
        *,
        failure_threshold: float = 0.5,
        min_calls: int = 5,
        window: float = 30.0,
        reset_timeout: float = 30.0,
        half_open_max_calls: int = 1,
        on: ExcFilter = Exception,
        name: str | None = None,
        on_state_change: Callable[[CircuitBreaker, str, str], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not 0.0 < failure_threshold <= 1.0:
            raise ValueError("failure_threshold must be in (0, 1]")
        if min_calls < 1:
            raise ValueError("min_calls must be >= 1")
        if half_open_max_calls < 1:
            raise ValueError("half_open_max_calls must be >= 1")
        self.failure_threshold = failure_threshold
        self.min_calls = min_calls
        self.window = positive_number("window", window)
        self.reset_timeout = positive_number("reset_timeout", reset_timeout, allow_zero=True)
        self.half_open_max_calls = half_open_max_calls
        self.on = on
        self.name = name or f"breaker-{id(self):x}"
        self.on_state_change = on_state_change
        self._clock = clock

        self._lock = threading.Lock()
        self._state = _CLOSED
        self._outcomes: deque[tuple[float, bool]] = deque()
        self._opened_at = 0.0
        self._half_open_inflight = 0
        self._half_open_successes = 0

    # -- public introspection -------------------------------------------------

    @property
    def state(self) -> str:
        """Current state: ``"closed"``, ``"open"`` or ``"half_open"``.

        Reading the state applies the open → half-open timeout transition,
        so it always reflects what the next call would see.
        """
        with self._lock:
            events = self._maybe_half_open()
        self._fire(events)
        return self._state

    def reset(self) -> None:
        """Force the breaker back to closed and clear the window."""
        with self._lock:
            events = self._transition(_CLOSED)
            self._outcomes.clear()
            self._half_open_inflight = 0
            self._half_open_successes = 0
        self._fire(events)

    # -- internals -----------------------------------------------------------

    def _fire(self, events: list[tuple[str, str]]) -> None:
        for old, new in events:
            emit("breaker.state_change", "circuit_breaker", self.name, old=old, new=new)
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

    def _evict(self, now: float) -> None:
        cutoff = now - self.window
        while self._outcomes and self._outcomes[0][0] < cutoff:
            self._outcomes.popleft()

    def _before_call(self) -> str:
        """Admit or reject the call; returns the state it was admitted under."""
        reject: CircuitOpen | None = None
        admitted = _CLOSED
        with self._lock:
            events = self._maybe_half_open()
            if self._state == _OPEN:
                retry_after = self.reset_timeout - (self._clock() - self._opened_at)
                reject = CircuitOpen(self.name, max(0.0, retry_after))
            elif self._state == _HALF_OPEN:
                if self._half_open_inflight >= self.half_open_max_calls:
                    reject = CircuitOpen(self.name, 0.0)
                else:
                    self._half_open_inflight += 1
                    admitted = _HALF_OPEN
        self._fire(events)
        if reject is not None:
            emit("breaker.rejected", "circuit_breaker", self.name, retry_after=reject.retry_after)
            raise reject
        return admitted

    def _on_success(self, admitted: str) -> None:
        with self._lock:
            if admitted == _HALF_OPEN and self._state == _HALF_OPEN:
                self._half_open_inflight -= 1
                self._half_open_successes += 1
                if self._half_open_successes >= self.half_open_max_calls:
                    self._outcomes.clear()
                    events = self._transition(_CLOSED)
                else:
                    events = []
            elif self._state == _CLOSED:
                now = self._clock()
                self._outcomes.append((now, True))
                self._evict(now)
                events = []
            else:
                events = []
        self._fire(events)

    def _on_failure(self, admitted: str) -> None:
        with self._lock:
            now = self._clock()
            events: list[tuple[str, str]] = []
            if admitted == _HALF_OPEN and self._state == _HALF_OPEN:
                self._half_open_inflight -= 1
                self._opened_at = now
                events = self._transition(_OPEN)
            elif self._state == _CLOSED:
                self._outcomes.append((now, False))
                self._evict(now)
                total = len(self._outcomes)
                if total >= self.min_calls:
                    failures = sum(1 for _, ok in self._outcomes if not ok)
                    if failures / total >= self.failure_threshold:
                        self._opened_at = now
                        events = self._transition(_OPEN)
        self._fire(events)

    def _on_ignored(self, admitted: str) -> None:
        # An exception not selected by `on`: release the probe slot (if any)
        # without recording an outcome.
        with self._lock:
            if admitted == _HALF_OPEN and self._state == _HALF_OPEN:
                self._half_open_inflight -= 1

    # -- execution -------------------------------------------------------------

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        admitted = self._before_call()
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:
            if exc_matches(exc, self.on):
                self._on_failure(admitted)
            else:
                self._on_ignored(admitted)
            raise
        self._on_success(admitted)
        return result

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        admitted = self._before_call()
        try:
            result = await fn(*args, **kwargs)
        except Exception as exc:
            if exc_matches(exc, self.on):
                self._on_failure(admitted)
            else:
                self._on_ignored(admitted)
            raise
        self._on_success(admitted)
        return result


# Lowercase alias so all policies read uniformly at decoration sites.
circuit_breaker = CircuitBreaker
