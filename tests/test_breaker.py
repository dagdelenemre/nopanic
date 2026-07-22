from __future__ import annotations

import asyncio

import pytest

from nopanic import CircuitBreaker, CircuitOpen


def make_breaker(clock, **overrides):
    defaults = dict(
        failure_threshold=0.5,
        min_calls=4,
        window=30.0,
        reset_timeout=10.0,
        clock=clock,
    )
    defaults.update(overrides)
    return CircuitBreaker(**defaults)


def trip(breaker, failures):
    @breaker
    def boom():
        raise ConnectionError("down")

    for _ in range(failures):
        with pytest.raises(ConnectionError):
            boom()


def test_opens_at_failure_threshold(clock):
    breaker = make_breaker(clock)
    trip(breaker, 4)
    assert breaker.state == "open"

    @breaker
    def anything():
        return "never runs"

    with pytest.raises(CircuitOpen) as excinfo:
        anything()
    assert excinfo.value.retry_after == pytest.approx(10.0)


def test_stays_closed_below_min_calls(clock):
    breaker = make_breaker(clock, min_calls=10)
    trip(breaker, 4)
    assert breaker.state == "closed"


def test_successes_keep_failure_rate_low(clock):
    breaker = make_breaker(clock)

    @breaker
    def ok():
        return 1

    @breaker
    def boom():
        raise ConnectionError("down")

    for _ in range(6):
        ok()
    for _ in range(3):  # 3 failures / 9 calls = 33% < 50%
        with pytest.raises(ConnectionError):
            boom()
    assert breaker.state == "closed"


def test_old_outcomes_fall_out_of_window(clock):
    breaker = make_breaker(clock, window=30.0)
    trip(breaker, 3)  # below min_calls, still closed
    clock.advance(31.0)  # those failures leave the window
    trip(breaker, 1)  # only 1 outcome in window now
    assert breaker.state == "closed"


def test_half_open_probe_success_closes(clock):
    breaker = make_breaker(clock)
    trip(breaker, 4)
    assert breaker.state == "open"

    clock.advance(10.0)
    assert breaker.state == "half_open"

    @breaker
    def ok():
        return "recovered"

    assert ok() == "recovered"
    assert breaker.state == "closed"


def test_half_open_probe_failure_reopens(clock):
    breaker = make_breaker(clock)
    trip(breaker, 4)
    clock.advance(10.0)
    trip(breaker, 1)  # the probe fails
    assert breaker.state == "open"

    @breaker
    def anything():
        return 1

    with pytest.raises(CircuitOpen):
        anything()


def test_half_open_rejects_calls_beyond_probe_budget(clock):
    breaker = make_breaker(clock, half_open_max_calls=1)
    trip(breaker, 4)
    clock.advance(10.0)

    probe_started = []

    @breaker
    def slow_probe():
        probe_started.append(1)
        # While this probe is conceptually "in flight" a second call arrives —
        # simulate by calling other() from inside.
        with pytest.raises(CircuitOpen):
            other()
        return "ok"

    @breaker
    def other():
        return "should be rejected"

    assert slow_probe() == "ok"
    assert probe_started == [1]
    assert breaker.state == "closed"


def test_ignored_exceptions_are_not_recorded(clock):
    breaker = make_breaker(clock, on=ConnectionError)

    @breaker
    def bug():
        raise ValueError("a bug, not an outage")

    for _ in range(10):
        with pytest.raises(ValueError):
            bug()
    assert breaker.state == "closed"


def test_state_change_callback_fires_outside_lock(clock):
    events = []
    breaker = make_breaker(
        clock, on_state_change=lambda b, old, new: events.append((old, new, b.state))
    )
    trip(breaker, 4)
    clock.advance(10.0)

    @breaker
    def ok():
        return 1

    ok()
    transitions = [(old, new) for old, new, _ in events]
    assert transitions == [("closed", "open"), ("open", "half_open"), ("half_open", "closed")]
    # Reading b.state inside the callback must not deadlock (checked implicitly).


def test_reset_forces_closed(clock):
    breaker = make_breaker(clock)
    trip(breaker, 4)
    assert breaker.state == "open"
    breaker.reset()
    assert breaker.state == "closed"


def test_shared_across_call_sites(clock):
    breaker = make_breaker(clock)

    @breaker
    def a():
        raise ConnectionError("down")

    @breaker
    def b():
        return "fine"

    for _ in range(4):
        with pytest.raises(ConnectionError):
            a()
    with pytest.raises(CircuitOpen):
        b()


def test_async_breaker(clock):
    breaker = make_breaker(clock)

    @breaker
    async def boom():
        raise ConnectionError("down")

    async def scenario():
        for _ in range(4):
            with pytest.raises(ConnectionError):
                await boom()
        with pytest.raises(CircuitOpen):
            await boom()

    asyncio.run(scenario())
    assert breaker.state == "open"


def test_validation():
    with pytest.raises(ValueError):
        CircuitBreaker(failure_threshold=0.0)
    with pytest.raises(ValueError):
        CircuitBreaker(failure_threshold=1.5)
    with pytest.raises(ValueError):
        CircuitBreaker(min_calls=0)
    with pytest.raises(ValueError):
        CircuitBreaker(window_buckets=0)


def test_window_memory_is_bounded(clock):
    """Regression guard: the window must never keep a per-call record."""
    breaker = make_breaker(clock, min_calls=10**9)  # never opens

    @breaker
    def ok():
        return 1

    for _ in range(10_000):
        ok()
        clock.advance(0.001)

    window = breaker._window
    assert len(window._counts) == breaker.window_buckets  # fixed buckets only
    assert window._total == 10_000  # outcomes are counted, not stored


def test_failure_rate_is_computed_across_buckets(clock):
    """Failures spread over several time buckets still open the breaker."""
    breaker = make_breaker(clock, window=10.0)  # threshold 0.5, min_calls 4

    @breaker
    def boom():
        raise ConnectionError("down")

    for _ in range(4):  # one failure per 1s bucket
        clock.advance(1.0)
        with pytest.raises(ConnectionError):
            boom()
    assert breaker.state == "open"
