from __future__ import annotations

import asyncio

import pytest

from nopanic import (
    BulkheadFull,
    CircuitBreaker,
    CircuitOpen,
    Event,
    RateLimited,
    bulkhead,
    events,
    fallback,
    rate_limit,
    retry,
    subscribe,
    timeout,
    unsubscribe,
)


@pytest.fixture
def seen():
    """Collect every event emitted during a test, with guaranteed cleanup."""
    collected: list[Event] = []
    cancel = subscribe(collected.append)
    yield collected
    cancel()


def kinds(seen):
    return [e.kind for e in seen]


def test_subscribe_returns_working_cancel():
    collected = []
    cancel = subscribe(collected.append)
    events.emit("x.test", "x")
    cancel()
    events.emit("x.test", "x")
    assert len(collected) == 1


def test_unsubscribe_unknown_listener_is_noop():
    unsubscribe(lambda e: None)  # must not raise


def test_listener_errors_are_swallowed(seen, caplog):
    def bad(event):
        raise RuntimeError("listener bug")

    cancel = subscribe(bad)
    try:
        events.emit("x.test", "x")
    finally:
        cancel()
    assert kinds(seen) == ["x.test"]  # the healthy listener still ran


def test_no_listener_fast_path():
    events.emit("nobody.listening", "x")  # must not raise or allocate an Event


def test_retry_emits_attempt_and_gave_up(seen):
    @retry(attempts=2, on=ValueError, backoff=0)
    def always_fails():
        raise ValueError("nope")

    with pytest.raises(ValueError):
        always_fails()

    assert kinds(seen) == ["retry.attempt_failed", "retry.gave_up"]
    assert seen[0].data["attempt"] == 1
    assert isinstance(seen[1].data["exception"], ValueError)


def test_breaker_emits_state_change_and_rejected(seen, clock):
    breaker = CircuitBreaker(failure_threshold=1.0, min_calls=1, reset_timeout=60.0, clock=clock)

    @breaker
    def boom():
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        boom()
    with pytest.raises(CircuitOpen):
        boom()

    assert "breaker.state_change" in kinds(seen)
    assert "breaker.rejected" in kinds(seen)
    change = next(e for e in seen if e.kind == "breaker.state_change")
    assert (change.data["old"], change.data["new"]) == ("closed", "open")
    assert change.name == breaker.name


def test_bulkhead_emits_rejected(seen):
    bh = bulkhead(1, max_wait=0)

    @bh
    async def slow():
        await asyncio.sleep(0.2)

    @bh
    async def rejected():
        return "no"

    async def scenario():
        task = asyncio.ensure_future(slow())
        await asyncio.sleep(0.01)
        with pytest.raises(BulkheadFull):
            await rejected()
        await task

    asyncio.run(scenario())
    assert "bulkhead.rejected" in kinds(seen)


def test_rate_limit_emits_rejected(seen, clock):
    rl = rate_limit(1, per=10.0, max_wait=0, clock=clock)
    rl._reserve()
    with pytest.raises(RateLimited):
        rl._reserve()
    assert "ratelimit.rejected" in kinds(seen)


def test_timeout_emits_expired(seen):
    @timeout(0.05)
    async def slow():
        await asyncio.sleep(1.0)

    with pytest.raises(TimeoutError):
        asyncio.run(slow())
    assert kinds(seen) == ["timeout.expired"]
    assert seen[0].data["seconds"] == 0.05


def test_fallback_emits_used(seen):
    @fallback("plan-b", on=ValueError)
    def broken():
        raise ValueError("x")

    assert broken() == "plan-b"
    assert kinds(seen) == ["fallback.used"]


def test_event_is_immutable(seen):
    events.emit("x.test", "x", detail=1)
    with pytest.raises(AttributeError):
        seen[0].kind = "changed"
