"""Fixed-memory sliding time window of outcome counters."""

from __future__ import annotations


class SlidingWindow:
    """N time buckets of (calls, failures, slow calls) plus running totals.

    Replaces a per-call log. Memory is O(buckets) no matter the traffic, and
    both recording an outcome and reading the totals are O(1): totals are
    maintained incrementally, and the bucket sweep runs only when the clock
    enters a new bucket (within a bucket it is a single comparison). This is
    the standard breaker window design (Hystrix, resilience4j); the
    trade-off is that outcomes expire in bucket-sized time steps instead of
    at an exact per-call age.

    The circuit breaker counts failures and slow calls with it; the retry
    budget reuses it to count first attempts and retries (as "failures").

    Not thread-safe on its own: owners hold their lock around every call.
    """

    __slots__ = (
        "_counts",
        "_epochs",
        "_fails",
        "_failures",
        "_last",
        "_n",
        "_slow",
        "_slows",
        "_span",
        "_total",
    )

    _counts: list[int]
    _fails: list[int]
    _slows: list[int]
    _epochs: list[int]
    _last: int
    _total: int
    _failures: int
    _slow: int

    def __init__(self, window: float, buckets: int) -> None:
        self._span = window / buckets
        self._n = buckets
        self.clear()

    def _roll(self, now: float) -> int:
        """Expire buckets that left the window; no-op within a time slot."""
        cur = int(now / self._span)
        if cur != self._last:
            self._last = cur
            floor = cur - self._n + 1
            counts, fails, slows, epochs = self._counts, self._fails, self._slows, self._epochs
            for i in range(self._n):
                epoch = epochs[i]
                if epoch != -1 and not floor <= epoch <= cur:
                    self._total -= counts[i]
                    self._failures -= fails[i]
                    self._slow -= slows[i]
                    counts[i] = 0
                    fails[i] = 0
                    slows[i] = 0
                    epochs[i] = -1
        return cur

    def record(self, now: float, ok: bool, slow: bool = False) -> None:
        cur = self._roll(now)
        i = cur % self._n
        if self._epochs[i] != cur:
            self._epochs[i] = cur  # bucket was cleared by the sweep above
        self._counts[i] += 1
        self._total += 1
        if not ok:
            self._fails[i] += 1
            self._failures += 1
        if slow:
            self._slows[i] += 1
            self._slow += 1

    def totals(self, now: float) -> tuple[int, int, int]:
        """Return (calls, failures, slow calls) currently inside the window."""
        self._roll(now)
        return self._total, self._failures, self._slow

    def clear(self) -> None:
        self._counts = [0] * self._n
        self._fails = [0] * self._n
        self._slows = [0] * self._n
        self._epochs = [-1] * self._n  # absolute time-slot each bucket holds
        self._last = -1  # slot of the most recent sweep
        self._total = 0
        self._failures = 0
        self._slow = 0
