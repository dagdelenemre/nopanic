from __future__ import annotations

import asyncio
import time

import pytest

from nopanic import rate_limit, retry
from nopanic.testing import capture_events, instant


def test_instant_skips_and_records_retry_waits():
    calls = []

    @retry(attempts=3, backoff=30.0)
    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError
        return "ok"

    start = time.monotonic()
    with instant() as sleeps:
        assert flaky() == "ok"
    assert time.monotonic() - start < 1.0
    assert sleeps == [30.0, 30.0]


def test_instant_covers_async_and_rate_limit_waits():
    limiter = rate_limit(1, per=60.0)

    @limiter
    async def call():
        return 1

    async def scenario():
        return [await call() for _ in range(3)]

    with instant() as sleeps:
        assert asyncio.run(scenario()) == [1, 1, 1]
    assert sleeps == pytest.approx([60.0, 120.0], rel=0.01)


def test_instant_restores_real_sleeping():
    with instant():
        pass

    @retry(attempts=2, backoff=0.05)
    def flaky(calls=[]):  # noqa: B006 (deliberate shared state)
        calls.append(1)
        if len(calls) < 2:
            raise ConnectionError

    start = time.monotonic()
    flaky()
    assert time.monotonic() - start >= 0.04


def test_capture_events_filters_by_prefix():
    @retry(attempts=2, backoff=0, name="checkout")
    def down():
        raise ConnectionError

    with capture_events("retry.gave_up") as seen, pytest.raises(ConnectionError):
        down()
    assert [(e.kind, e.name, e.data["reason"]) for e in seen] == [
        ("retry.gave_up", "checkout", "attempts")
    ]
    assert seen[0].function.endswith("down")
