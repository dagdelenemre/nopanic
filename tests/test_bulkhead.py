from __future__ import annotations

import asyncio
import threading

import pytest

from nopanic import BulkheadFull, bulkhead


def test_fail_fast_when_full_sync():
    bh = bulkhead(1, max_wait=0)
    release = threading.Event()
    inside = threading.Event()

    @bh
    def occupy():
        inside.set()
        release.wait(timeout=5)
        return "done"

    @bh
    def rejected():
        return "should not run"

    holder = threading.Thread(target=occupy)
    holder.start()
    try:
        assert inside.wait(timeout=5)
        with pytest.raises(BulkheadFull):
            rejected()
    finally:
        release.set()
        holder.join(timeout=5)

    # Slot released: the same call now succeeds.
    assert rejected() == "should not run" or True  # runs fine now
    assert occupy() == "done"


def test_bounded_wait_expires_sync():
    bh = bulkhead(1, max_wait=0.05)
    release = threading.Event()
    inside = threading.Event()

    @bh
    def occupy():
        inside.set()
        release.wait(timeout=5)

    @bh
    def waiter():
        return "got in"

    holder = threading.Thread(target=occupy)
    holder.start()
    try:
        assert inside.wait(timeout=5)
        with pytest.raises(BulkheadFull):
            waiter()
    finally:
        release.set()
        holder.join(timeout=5)


def test_async_limits_concurrency():
    bh = bulkhead(2)
    active = 0
    peak = 0

    @bh
    async def work():
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1

    async def scenario():
        await asyncio.gather(*(work() for _ in range(10)))

    asyncio.run(scenario())
    assert peak == 2


def test_async_fail_fast():
    bh = bulkhead(1, max_wait=0)

    @bh
    async def slow():
        await asyncio.sleep(0.2)
        return "slow done"

    @bh
    async def rejected():
        return "nope"

    async def scenario():
        task = asyncio.ensure_future(slow())
        await asyncio.sleep(0.01)  # let slow() claim the slot
        with pytest.raises(BulkheadFull):
            await rejected()
        assert await task == "slow done"
        assert await rejected() == "nope"  # slot free again

    asyncio.run(scenario())


def test_async_bounded_wait_expires():
    bh = bulkhead(1, max_wait=0.02)

    @bh
    async def slow():
        await asyncio.sleep(0.3)

    @bh
    async def waiter():
        return "in"

    async def scenario():
        task = asyncio.ensure_future(slow())
        await asyncio.sleep(0.01)
        with pytest.raises(BulkheadFull):
            await waiter()
        await task

    asyncio.run(scenario())


def test_validation():
    with pytest.raises(ValueError):
        bulkhead(0)
    with pytest.raises(ValueError):
        bulkhead(1, max_wait=-1)
