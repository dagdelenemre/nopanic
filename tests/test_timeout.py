from __future__ import annotations

import asyncio
import contextvars
import time

import pytest

from nopanic import events, timeout


def test_fast_sync_call_passes_through():
    @timeout(1.0)
    def quick():
        return 42

    assert quick() == 42


def test_slow_sync_call_times_out():
    @timeout(0.05)
    def slow():
        time.sleep(5.0)
        return "too late"

    start = time.monotonic()
    with pytest.raises(TimeoutError):
        slow()
    # Generous bound: proves we did not wait out the 5s sleep while staying
    # robust on slow, contended CI runners.
    assert time.monotonic() - start < 4.0


def test_sync_exception_is_relayed():
    @timeout(1.0)
    def boom():
        raise KeyError("original")

    with pytest.raises(KeyError, match="original"):
        boom()


def test_fast_async_call_passes_through():
    @timeout(1.0)
    async def quick():
        return "ok"

    assert asyncio.run(quick()) == "ok"


def test_slow_async_call_times_out_and_cancels():
    cancelled = []

    @timeout(0.05)
    async def slow():
        try:
            await asyncio.sleep(5.0)
        except asyncio.CancelledError:
            cancelled.append(True)
            raise
        return "too late"

    with pytest.raises(TimeoutError):
        asyncio.run(slow())
    assert cancelled == [True]


def test_async_exception_is_relayed():
    @timeout(1.0)
    async def boom():
        raise KeyError("original")

    with pytest.raises(KeyError):
        asyncio.run(boom())


def test_validation():
    with pytest.raises(ValueError):
        timeout(0)


class DatabaseTimeout(TimeoutError):
    pass


def test_async_functions_own_timeout_error_is_not_replaced():
    # On 3.11+ asyncio.TimeoutError IS TimeoutError; the policy must only
    # replace an expiry of its own, never the callee's exception.
    seen = []
    cancel = events.subscribe(seen.append)

    @timeout(5.0)
    async def query():
        raise DatabaseTimeout("db took too long")

    try:
        with pytest.raises(DatabaseTimeout, match="db took too long"):
            asyncio.run(query())
    finally:
        cancel()
    assert seen == []  # and no false timeout.expired event


def test_sync_functions_own_timeout_error_is_not_replaced():
    @timeout(5.0)
    def query():
        raise DatabaseTimeout("db took too long")

    with pytest.raises(DatabaseTimeout):
        query()


request_id = contextvars.ContextVar("request_id", default="missing")


def test_sync_worker_sees_the_callers_contextvars():
    @timeout(1.0)
    def handler():
        return request_id.get()

    token = request_id.set("req-42")
    try:
        assert handler() == "req-42"
    finally:
        request_id.reset(token)


def test_async_timeout_runs_in_the_callers_context():
    @timeout(1.0)
    async def handler():
        return request_id.get()

    async def scenario():
        request_id.set("req-7")
        return await handler()

    assert asyncio.run(scenario()) == "req-7"
