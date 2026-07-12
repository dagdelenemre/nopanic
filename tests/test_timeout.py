from __future__ import annotations

import asyncio
import time

import pytest

from nopanic import timeout


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
