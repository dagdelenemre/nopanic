from __future__ import annotations

import asyncio
import threading
import time

import pytest

from nopanic import (
    BulkheadFull,
    RateLimited,
    bulkhead,
    compose,
    deadline,
    rate_limit,
    retry,
    time_remaining,
    timeout,
)
from nopanic.testing import capture_events


def test_time_remaining_is_none_outside_a_deadline():
    assert time_remaining() is None


def test_nested_deadlines_only_shorten_the_budget():
    with deadline(10.0):
        outer = time_remaining()
        with deadline(60.0):
            assert time_remaining() <= outer  # cannot extend
        with deadline(1.0):
            assert time_remaining() <= 1.0
        assert time_remaining() == pytest.approx(outer, abs=0.5)
    assert time_remaining() is None


def test_deadline_instance_is_not_reentrant():
    budget = deadline(1.0)
    with budget, pytest.raises(RuntimeError), budget:
        pass


def test_retry_gives_up_instead_of_sleeping_past_the_deadline():
    calls = []

    @retry(attempts=10, backoff=5.0)
    def down():
        calls.append(1)
        raise ConnectionError

    start = time.monotonic()
    with capture_events("retry.gave_up") as seen, deadline(1.0), pytest.raises(ConnectionError):
        down()
    assert time.monotonic() - start < 1.0
    assert calls == [1]
    assert seen[0].data["reason"] == "deadline"


def test_timeout_clamps_to_the_remaining_deadline():
    @timeout(30.0)
    async def slow():
        await asyncio.sleep(5.0)

    async def scenario():
        async with deadline(0.05):
            await slow()

    start = time.monotonic()
    with pytest.raises(TimeoutError):
        asyncio.run(scenario())
    assert time.monotonic() - start < 2.0


def test_timeout_after_the_deadline_fails_without_calling():
    calls = []

    @timeout(30.0)
    def work():
        calls.append(1)

    with deadline(0.01):
        time.sleep(0.02)
        with pytest.raises(TimeoutError):
            work()
    assert calls == []


def test_sync_timeout_worker_sees_the_deadline():
    @timeout(5.0)
    def report():
        return time_remaining()

    with deadline(2.0):
        assert 0 < report() <= 2.0


def test_rate_limit_rejects_waits_that_outlast_the_deadline(clock):
    limiter = rate_limit(1, per=10.0, clock=clock)  # waits forever by default

    @limiter
    def call():
        return "ok"

    assert call() == "ok"
    with deadline(1.0), pytest.raises(RateLimited):
        call()  # would wait ~10s: rejected instead


def test_bulkhead_never_waits_past_the_deadline():
    bh = bulkhead(1)  # waits indefinitely by default
    held = threading.Event()
    release = threading.Event()

    @bh
    def occupy():
        held.set()
        release.wait(5.0)

    worker = threading.Thread(target=occupy)
    worker.start()
    try:
        assert held.wait(5.0)
        start = time.monotonic()
        with deadline(0.05), pytest.raises(BulkheadFull):
            bh.call(lambda: None)
        assert time.monotonic() - start < 2.0
    finally:
        release.set()
        worker.join(5.0)


def test_deadline_as_a_policy_gives_each_call_its_own_budget():
    seen = []

    @compose(deadline(3.0), retry(attempts=1))
    def report():
        seen.append(time_remaining())

    report()
    report()
    assert all(0 < r <= 3.0 for r in seen)
    assert time_remaining() is None


def test_async_deadline_policy():
    @deadline(2.0)
    async def report():
        return time_remaining()

    assert 0 < asyncio.run(report()) <= 2.0


def test_validation():
    with pytest.raises(ValueError):
        deadline(0)
