from __future__ import annotations

import asyncio

import pytest

from nopanic import RateLimited, adaptive_rate_limit, looks_throttled


class Throttled(Exception):
    status_code = 429  # matched by the default looks_throttled heuristic

    def __init__(self, retry_after=None):
        super().__init__("429")
        if retry_after is not None:
            self.retry_after = retry_after


def make(clock, **overrides):
    defaults = dict(rate=10.0, per=1.0, min_rate=1.0, increase=1.0, decrease=0.5, clock=clock)
    defaults.update(overrides)
    return adaptive_rate_limit(**defaults)


def test_throttle_error_halves_the_rate(clock):
    arl = make(clock)

    @arl
    def boom():
        raise Throttled()

    with pytest.raises(Throttled):
        boom()
    assert arl.current_rate == pytest.approx(5.0)
    with pytest.raises(Throttled):
        boom()
    assert arl.current_rate == pytest.approx(2.5)


def test_success_recovers_additively_up_to_max(clock):
    arl = make(clock)

    @arl
    def boom():
        raise Throttled()

    @arl
    def ok():
        return 1

    with pytest.raises(Throttled):
        boom()
    assert arl.current_rate == pytest.approx(5.0)
    for _ in range(3):
        clock.advance(1.0)  # keep tokens available
        ok()
    assert arl.current_rate == pytest.approx(8.0)  # +1.0 per success
    for _ in range(10):
        clock.advance(1.0)
        ok()
    assert arl.current_rate == pytest.approx(10.0)  # capped at max_rate (= rate)


def test_rate_never_drops_below_min(clock):
    arl = make(clock, min_rate=4.0)

    @arl
    def boom():
        raise Throttled()

    for _ in range(5):
        clock.advance(1.0)
        with pytest.raises(Throttled):
            boom()
    assert arl.current_rate == pytest.approx(4.0)


def test_retry_after_blocks_the_bucket(clock):
    arl = make(clock)

    @arl
    def boom():
        raise Throttled(retry_after=7.0)

    with pytest.raises(Throttled):
        boom()
    # Next reservation must wait out the server-mandated pause.
    assert arl._reserve() == pytest.approx(7.0)
    clock.advance(7.0)
    assert arl._reserve() == pytest.approx(0.0)


def test_hostile_retry_after_block_is_capped(clock):
    arl = make(clock, max_block=5.0)

    @arl
    def boom():
        raise Throttled(retry_after=999_999_999.0)

    with pytest.raises(Throttled):
        boom()
    assert arl._reserve() <= 5.0  # blocked, but never beyond max_block


def test_non_throttle_errors_do_not_touch_the_rate(clock):
    arl = make(clock)

    @arl
    def bug():
        raise ValueError("not the server's fault")

    with pytest.raises(ValueError):
        bug()
    assert arl.current_rate == pytest.approx(10.0)


def test_max_wait_rejects(clock):
    arl = make(clock, rate=1.0, min_rate=0.1, max_wait=0.0)
    assert arl._reserve() == 0.0
    with pytest.raises(RateLimited):
        arl._reserve()


def test_async_path(clock):
    arl = make(clock)

    @arl
    async def boom():
        raise Throttled()

    @arl
    async def ok():
        return "fine"

    async def scenario():
        with pytest.raises(Throttled):
            await boom()
        return await ok()

    assert asyncio.run(scenario()) == "fine"
    assert arl.current_rate == pytest.approx(6.0)  # halved to 5, +1 on success


def test_looks_throttled_heuristic():
    assert looks_throttled(Throttled(retry_after=1.0))

    class WithStatus(Exception):
        status_code = 429

    assert looks_throttled(WithStatus())

    class Resp:
        status_code = 503

    class WithResponse(Exception):
        response = Resp()

    assert looks_throttled(WithResponse())
    assert not looks_throttled(ValueError("plain bug"))


def test_validation(clock):
    with pytest.raises(ValueError):
        adaptive_rate_limit(10, decrease=1.0, clock=clock)
    with pytest.raises(ValueError):
        adaptive_rate_limit(10, min_rate=20.0, clock=clock)
    with pytest.raises(ValueError):
        adaptive_rate_limit(10, max_rate=5.0, clock=clock)
