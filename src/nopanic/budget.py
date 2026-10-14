"""Retry budgets: cap retries as a fraction of real traffic.

Per-call retry limits do not prevent retry storms. When a dependency
degrades, every caller retries, and three attempts per call means up to
three times the load on the one thing that is already struggling. A retry
budget (Finagle, gRPC, Envoy) caps retries *across* calls: retries may make
up at most ``ratio`` of first attempts over a sliding window, plus a small
floor so low-traffic clients can still retry at all.

    api_budget = RetryBudget(ratio=0.1)   # retries <= 10% of calls, +1/s floor

    @retry(attempts=4, budget=api_budget)
    def get_user(uid): ...

    @retry(attempts=4, budget=api_budget)   # same budget: same downstream
    def get_orders(uid): ...

When the budget is spent, ``retry`` stops retrying and re-raises the
original exception (``retry.gave_up`` with reason ``budget``), so under a
real outage your retry traffic shrinks to the floor instead of multiplying.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

from ._core import positive_number
from ._window import SlidingWindow

__all__ = ["RetryBudget"]


class RetryBudget:
    """A shared allowance of retries; pass it to one or more ``retry`` policies.

    Args:
        ratio: Retries allowed per first attempt inside the window (0.2 means
            at most one retry for every five calls).
        min_retries_per_sec: Floor that is always allowed, so a client with
            little traffic can still retry. Over the window this amounts to
            ``min_retries_per_sec * window`` retries.
        window: Sliding window in seconds over which calls and retries are
            counted.
        clock: Monotonic time source; injectable for tests.

    Thread-safe; memory is constant (bucketed counters, like the breaker).
    """

    __slots__ = ("_clock", "_floor", "_lock", "_window", "min_retries_per_sec", "ratio", "window")

    def __init__(
        self,
        ratio: float = 0.2,
        *,
        min_retries_per_sec: float = 1.0,
        window: float = 10.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.ratio = positive_number("ratio", ratio, allow_zero=True)
        self.min_retries_per_sec = positive_number(
            "min_retries_per_sec", min_retries_per_sec, allow_zero=True
        )
        self.window = positive_number("window", window)
        self._floor = self.min_retries_per_sec * self.window
        self._clock = clock
        self._lock = threading.Lock()
        # "ok" outcomes are first attempts, "failures" are retries.
        self._window = SlidingWindow(self.window, 10)

    def _allowed(self, now: float) -> tuple[float, int]:
        total, retries, _ = self._window.totals(now)
        return self.ratio * (total - retries) + self._floor, retries

    @property
    def available(self) -> float:
        """Retries that could be spent right now."""
        with self._lock:
            allowed, retries = self._allowed(self._clock())
        return max(0.0, allowed - retries)

    def deposit(self) -> None:
        """Count one first attempt (called by ``retry``)."""
        with self._lock:
            self._window.record(self._clock(), True)

    def try_withdraw(self) -> bool:
        """Spend one retry if the budget allows it (called by ``retry``)."""
        with self._lock:
            now = self._clock()
            allowed, retries = self._allowed(now)
            if retries + 1 > allowed:
                return False
            self._window.record(now, False)
            return True
