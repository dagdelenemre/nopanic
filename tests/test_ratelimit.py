from __future__ import annotations

import asyncio

import pytest

from nopanic import RateLimited, rate_limit


def test_burst_passes_then_waits(clock):
    rl = rate_limit(2, per=1.0, clock=clock)
    assert rl._reserve() == 0.0
    assert rl._reserve() == 0.0
    # Bucket empty: third caller must wait for one refill interval.
    assert rl._reserve() == pytest.approx(0.5)
    # Fourth caller queues behind the third.
    assert rl._reserve() == pytest.approx(1.0)


def test_tokens_refill_over_time(clock):
    rl = rate_limit(2, per=1.0, clock=clock)
    rl._reserve()
    rl._reserve()
    clock.advance(1.0)  # full refill
    assert rl._reserve() == 0.0
    assert rl._reserve() == 0.0


def test_burst_caps_idle_accumulation(clock):
    rl = rate_limit(10, per=1.0, burst=3, clock=clock)
    clock.advance(100.0)  # long idle: bucket capped at 3, not 1000
    assert rl._reserve() == 0.0
    assert rl._reserve() == 0.0
    assert rl._reserve() == 0.0
    assert rl._reserve() > 0.0


def test_max_wait_rejects_instead_of_waiting(clock):
    rl = rate_limit(1, per=10.0, max_wait=0, clock=clock)
    rl._reserve()
    with pytest.raises(RateLimited):
        rl._reserve()
    # The rejected reservation was refunded: after refill one call goes through.
    clock.advance(10.0)
    assert rl._reserve() == 0.0


def test_end_to_end_sync():
    rl = rate_limit(10_000, per=1.0)

    @rl
    def call():
        return "ok"

    assert all(call() == "ok" for _ in range(20))


def test_end_to_end_async():
    rl = rate_limit(10_000, per=1.0)

    @rl
    async def call():
        return "ok"

    async def scenario():
        return await asyncio.gather(*(call() for _ in range(20)))

    assert asyncio.run(scenario()) == ["ok"] * 20


def test_validation():
    with pytest.raises(ValueError):
        rate_limit(0)
    with pytest.raises(ValueError):
        rate_limit(1, per=0)
