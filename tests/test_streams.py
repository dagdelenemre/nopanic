"""Policies on generator and async generator functions (LLM-style streams)."""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from nopanic import (
    CircuitBreaker,
    CircuitOpen,
    RateLimited,
    bulkhead,
    cache,
    compose,
    deadline,
    fallback,
    hedge,
    rate_limit,
    retry,
    timeout,
)


def collect(agen):
    async def run():
        return [item async for item in agen]

    return asyncio.run(run())


def test_retry_reopens_a_stream_that_fails_before_its_first_item():
    opened = []

    @retry(attempts=3, backoff=0)
    async def tokens():
        opened.append(1)
        if len(opened) < 3:
            raise ConnectionError("handshake failed")
        yield "Hello"
        yield "world"

    assert collect(tokens()) == ["Hello", "world"]
    assert len(opened) == 3


def test_retry_never_replays_a_stream_that_already_delivered():
    opened = []

    @retry(attempts=5, backoff=0)
    async def tokens():
        opened.append(1)
        yield "partial"
        raise ConnectionError("dropped mid-stream")

    received = []

    async def run():
        async for item in tokens():
            received.append(item)

    with pytest.raises(ConnectionError):
        asyncio.run(run())
    assert received == ["partial"]
    assert len(opened) == 1  # items cannot be taken back: no retry


def test_retry_sync_generator():
    opened = []

    @retry(attempts=2, backoff=0)
    def rows():
        opened.append(1)
        if len(opened) == 1:
            raise ConnectionError
        yield from (1, 2, 3)

    assert list(rows()) == [1, 2, 3]
    assert len(opened) == 2


def test_timeout_bounds_each_item_with_a_separate_first_item_limit():
    @timeout(0.05, first_item=0.5)
    async def tokens(stall_at):
        await asyncio.sleep(0.1)  # slow first token: allowed by first_item
        yield 0
        for i in range(1, 4):
            await asyncio.sleep(1.0 if i == stall_at else 0)
            yield i

    assert collect(tokens(stall_at=None)) == [0, 1, 2, 3]

    received = []

    async def run():
        async for item in tokens(stall_at=2):
            received.append(item)

    with pytest.raises(TimeoutError):
        asyncio.run(run())
    assert received == [0, 1]  # the stalled stream failed at item 2


def test_timeout_does_not_count_the_consumers_time():
    @timeout(0.05)
    async def tokens():
        for i in range(3):
            yield i

    async def slow_consumer():
        out = []
        async for item in tokens():
            out.append(item)
            await asyncio.sleep(0.1)  # longer than the limit, but our time
        return out

    assert asyncio.run(slow_consumer()) == [0, 1, 2]


def test_breaker_records_stream_outcomes(clock):
    breaker = CircuitBreaker(min_calls=2, failure_threshold=0.5, clock=clock)

    @breaker
    async def tokens(fail):
        yield "a"
        if fail:
            raise ConnectionError("mid-stream")

    for _ in range(2):
        with pytest.raises(ConnectionError):
            collect(tokens(True))
    assert breaker.state == "open"
    with pytest.raises(CircuitOpen):
        collect(tokens(False))


def test_breaker_stream_closed_early_after_items_counts_as_success(clock):
    breaker = CircuitBreaker(min_calls=1, reset_timeout=5.0, clock=clock)
    breaker.force_open()
    breaker.reset()

    @breaker
    def boom():
        raise ConnectionError

    with pytest.raises(ConnectionError):
        boom()
    clock.advance(5.0)  # half-open now

    @breaker
    async def tokens():
        for i in range(100):
            yield i

    async def take_one():
        # Close abandoned async streams explicitly: Python only finalizes them
        # from the event loop later (and asyncio.run may cancel that).
        async with contextlib.aclosing(tokens()) as stream:
            async for item in stream:
                return item

    assert asyncio.run(take_one()) == 0
    assert breaker.state == "closed"  # the probe delivered: healthy


def test_bulkhead_holds_its_slot_for_the_whole_stream():
    bh = bulkhead(1, max_wait=0)

    @bh
    def rows():
        yield 1
        yield 2

    stream = rows()
    assert next(stream) == 1
    assert bh.in_flight == 1
    with pytest.raises(Exception, match="full"):
        list(rows())
    assert list(stream) == [2]
    assert bh.in_flight == 0


def test_rate_limit_charges_one_token_per_stream(clock):
    limiter = rate_limit(1, per=10.0, max_wait=0, clock=clock)

    @limiter
    def rows():
        yield from range(5)

    assert list(rows()) == [0, 1, 2, 3, 4]
    with pytest.raises(RateLimited):
        list(rows())


def test_composed_stream_stack():
    attempts = []
    stack = compose(retry(attempts=2, backoff=0), timeout(1.0), bulkhead(2))

    @stack
    async def tokens():
        attempts.append(1)
        if len(attempts) == 1:
            raise ConnectionError
        yield "x"
        yield "y"

    assert collect(tokens()) == ["x", "y"]
    assert len(attempts) == 2


@pytest.mark.parametrize("policy", [cache(ttl=1.0), fallback("x"), hedge(delay=1.0), deadline(1.0)])
def test_policies_that_cannot_guard_streams_say_so_at_decoration(policy):
    with pytest.raises(TypeError, match="async generator"):

        @policy
        async def tokens():
            yield 1

    with pytest.raises(TypeError, match="generator"):

        @policy
        def rows():
            yield 1


def test_sync_generator_timeout_is_rejected():
    with pytest.raises(TypeError):

        @timeout(1.0)
        def rows():
            yield 1
