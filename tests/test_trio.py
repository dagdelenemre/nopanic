"""Every async policy under trio (and therefore under AnyIO's trio backend)."""

from __future__ import annotations

import pytest

from nopanic import (
    BulkheadFull,
    CircuitBreaker,
    bulkhead,
    cache,
    deadline,
    hedge,
    rate_limit,
    retry,
    time_remaining,
    timeout,
)
from nopanic.testing import instant

trio = pytest.importorskip("trio")

# Same-tick reads give (m + s) - m, which float rounding can put a hair above s.
EPS = 1e-9


class DatabaseTimeout(TimeoutError):
    pass


def test_retry_sleeps_with_trio():
    calls = []

    @retry(attempts=3, backoff=0.01)
    async def flaky():
        calls.append(trio.current_time())
        if len(calls) < 3:
            raise ConnectionError
        return "ok"

    assert trio.run(flaky) == "ok"
    assert calls[2] - calls[0] >= 0.015


def test_testing_instant_under_trio():
    @retry(attempts=3, backoff=30.0)
    async def flaky(calls=[]):  # noqa: B006 (deliberate shared state)
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError
        return "ok"

    with instant() as sleeps:
        assert trio.run(flaky) == "ok"
    assert sleeps == [30.0, 30.0]


def test_timeout_expires_and_cancels():
    cancelled = []

    @timeout(0.05)
    async def slow():
        try:
            await trio.sleep(5.0)
        except trio.Cancelled:
            cancelled.append(True)
            raise

    with pytest.raises(TimeoutError):
        trio.run(slow)
    assert cancelled == [True]


def test_timeout_passes_the_functions_own_timeout_error_through():
    @timeout(5.0)
    async def query():
        raise DatabaseTimeout("mine")

    with pytest.raises(DatabaseTimeout):
        trio.run(query)


def test_timeout_bounds_stream_items():
    @timeout(0.05)
    async def tokens():
        yield 1
        await trio.sleep(5.0)
        yield 2

    async def consume():
        return [item async for item in tokens()]

    with pytest.raises(TimeoutError):
        trio.run(consume)


def test_timeout_guard_under_trio():
    async def scenario():
        async with timeout(0.05).guard():
            await trio.sleep(5.0)

    with pytest.raises(TimeoutError, match="block"):
        trio.run(scenario)


def test_bulkhead_rejects_and_counts():
    bh = bulkhead(1, max_wait=0.02)

    async def scenario():
        release = trio.Event()

        @bh
        async def occupy(task_status=trio.TASK_STATUS_IGNORED):
            task_status.started()
            await release.wait()

        @bh
        async def quick():
            return "in"

        async with trio.open_nursery() as nursery:
            await nursery.start(occupy)
            assert bh.in_flight == 1
            with pytest.raises(BulkheadFull):
                await quick()
            release.set()
        assert bh.in_flight == 0
        assert await quick() == "in"

    trio.run(scenario)


def test_rate_limit_waits_and_refunds_cancelled_waiters():
    limiter = rate_limit(1, per=0.1)

    @limiter
    async def call():
        return 1

    async def scenario():
        await call()
        with trio.move_on_after(0.01):
            await call()  # cancelled while queued: token refunded
        start = trio.current_time()
        await call()
        return trio.current_time() - start

    assert trio.run(scenario) < 0.2


def test_cancelled_breaker_probe_releases_its_slot():
    now = [0.0]
    breaker = CircuitBreaker(min_calls=1, reset_timeout=1.0, clock=lambda: now[0])

    @breaker
    async def call(hang):
        if hang:
            await trio.sleep(10.0)
            return "late"
        return "ok"

    @breaker
    async def boom():
        raise ConnectionError

    async def scenario():
        with pytest.raises(ConnectionError):
            await boom()
        now[0] = 5.0
        with trio.move_on_after(0.01):
            await call(True)
        return await call(False)

    assert trio.run(scenario) == "ok"
    assert breaker.state == "closed"


def test_cache_single_flight_under_trio():
    c = cache(ttl=60.0)
    calls = []

    @c
    async def fetch():
        calls.append(1)
        await trio.sleep(0.01)
        return "v"

    async def scenario():
        results = []

        async def one():
            results.append(await fetch())

        async with trio.open_nursery() as nursery:
            for _ in range(5):
                nursery.start_soon(one)
        return results

    assert trio.run(scenario) == ["v"] * 5
    assert calls == [1]


def test_hedge_under_trio():
    attempts = []

    @hedge(delay=0.02, max_hedges=1)
    async def slow_then_fast():
        attempts.append(1)
        await trio.sleep(1.0 if len(attempts) == 1 else 0.0)
        return len(attempts)

    assert trio.run(slow_then_fast) == 2
    assert len(attempts) == 2


def test_hedge_permanent_error_is_not_wrapped():
    @hedge(delay=0.01, max_hedges=3, on=ConnectionError)
    async def bad():
        raise ValueError("400")

    with pytest.raises(ValueError):  # the original, not an ExceptionGroup
        trio.run(bad)


def test_deadline_under_trio():
    @timeout(30.0)
    async def slow():
        await trio.sleep(5.0)

    async def scenario():
        async with deadline(0.05):
            assert 0 < time_remaining() <= 0.05 + EPS
            await slow()

    with pytest.raises(TimeoutError):
        trio.run(scenario)
