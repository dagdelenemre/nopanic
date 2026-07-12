from __future__ import annotations

import pytest

from nopanic import CircuitBreaker, CircuitOpen, retry
from nopanic.retry import _RETRY_AFTER_FACTOR, _RETRY_AFTER_MARGIN


def padded(hint):
    """The wait an honored hint produces: hint plus the safety margin."""
    return hint * _RETRY_AFTER_FACTOR + _RETRY_AFTER_MARGIN


class Throttled(Exception):
    def __init__(self, retry_after):
        super().__init__(f"429, retry after {retry_after}")
        self.retry_after = retry_after


def test_retry_after_raises_the_delay():
    delays = []

    @retry(attempts=2, on=Throttled, backoff=0, before_sleep=lambda a: delays.append(a.delay))
    def flaky():
        if not delays:
            raise Throttled(0.02)
        return "ok"

    assert flaky() == "ok"
    # Backoff said 0; the server hint won, padded so the deadline is
    # strictly passed despite OS timer granularity.
    assert delays == [pytest.approx(padded(0.02))]


def test_backoff_wins_when_larger():
    delays = []

    @retry(attempts=2, on=Throttled, backoff=0.2, before_sleep=lambda a: delays.append(a.delay))
    def flaky():
        if not delays:
            raise Throttled(0.001)
        return "ok"

    assert flaky() == "ok"
    assert delays == [0.2]  # 0.2 backoff beats the padded ~0.05 hint


def test_honor_retry_after_can_be_disabled():
    delays = []

    @retry(
        attempts=2,
        on=Throttled,
        backoff=0,
        before_sleep=lambda a: delays.append(a.delay),
        honor_retry_after=False,
    )
    def flaky():
        if not delays:
            raise Throttled(30.0)  # would be a long nap if honored
        return "ok"

    assert flaky() == "ok"
    assert delays == [0.0]


def test_bogus_retry_after_values_are_ignored():
    delays = []
    bad_values = iter([float("nan"), float("inf"), "soon", True, -5])

    @retry(attempts=6, on=Throttled, backoff=0, before_sleep=lambda a: delays.append(a.delay))
    def flaky():
        try:
            raise Throttled(next(bad_values))
        except StopIteration:
            return "ok"

    assert flaky() == "ok"
    assert delays == [0.0] * 5  # none of the junk hints were honored


def test_hostile_retry_after_is_capped():
    delays = []

    @retry(
        attempts=2,
        on=Throttled,
        backoff=0,
        retry_after_cap=0.01,
        before_sleep=lambda a: delays.append(a.delay),
    )
    def flaky():
        if not delays:
            raise Throttled(999_999_999)  # hostile server hint
        return "ok"

    assert flaky() == "ok"
    assert delays == [0.01]  # capped, not a 31-year nap


def test_retry_waits_out_an_open_breaker():
    """CircuitOpen carries retry_after; retry can now ride out the open window."""
    # Real clock on purpose: the retry must actually sleep out the window.
    breaker = CircuitBreaker(failure_threshold=1.0, min_calls=1, reset_timeout=0.05)
    calls = []

    # attempts=6 rather than the minimal 3: on Windows, sleep() can wake a
    # hair before the monotonic deadline, in which case an extra attempt
    # gets a CircuitOpen with the tiny remaining retry_after and self-heals.
    @retry(attempts=6, on=(ConnectionError, CircuitOpen), backoff=0)
    @breaker
    def upstream():
        calls.append(1)
        if len(calls) == 1:
            raise ConnectionError("blip")  # opens the breaker
        return "recovered"

    # Attempt 1 fails and opens the breaker; the next attempt(s) hit
    # CircuitOpen and, thanks to honor_retry_after, sleep out the remaining
    # window; the probe then succeeds. The breaker admits the function body
    # exactly twice regardless of how many attempts were rejected.
    assert upstream() == "recovered"
    assert len(calls) == 2
