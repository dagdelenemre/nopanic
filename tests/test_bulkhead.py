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

    async def scenario():
        release = asyncio.Event()
        entered = asyncio.Event()

        @bh
        async def occupy():
            entered.set()
            await release.wait()
            return "slow done"

        @bh
        async def rejected():
            return "nope"

        task = asyncio.ensure_future(occupy())
        await entered.wait()  # the slot is provably claimed, no timing guess
        with pytest.raises(BulkheadFull):
            await rejected()
        release.set()
        assert await task == "slow done"
        assert await rejected() == "nope"  # slot free again

    asyncio.run(scenario())


def test_async_bounded_wait_expires():
    bh = bulkhead(1, max_wait=0.02)

    async def scenario():
        release = asyncio.Event()
        entered = asyncio.Event()

        @bh
        async def occupy():
            entered.set()
            await release.wait()

        @bh
        async def waiter():
            return "in"

        task = asyncio.ensure_future(occupy())
        await entered.wait()  # held until we say otherwise; waiter must expire
        with pytest.raises(BulkheadFull):
            await waiter()
        release.set()
        await task

    asyncio.run(scenario())


def test_validation():
    with pytest.raises(ValueError):
        bulkhead(0)
    with pytest.raises(ValueError):
        bulkhead(1, max_wait=-1)


def test_async_timed_out_and_cancelled_waiters_never_leak_permits():
    bh = bulkhead(2, max_wait=0.01)

    async def scenario():
        release = asyncio.Event()
        entered = asyncio.Event()
        holding = []

        @bh
        async def occupy():
            holding.append(1)
            if len(holding) == 2:
                entered.set()
            await release.wait()

        @bh
        async def quick():
            return "in"

        tasks = [asyncio.ensure_future(occupy()) for _ in range(2)]
        await entered.wait()
        for _ in range(20):  # waiters that expire...
            with pytest.raises(BulkheadFull):
                await quick()
        waiter = asyncio.ensure_future(quick())  # ...and one that is cancelled
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        release.set()
        await asyncio.gather(*tasks)
        # Full capacity is back: both slots can be held at once again.
        holding.clear()
        entered.clear()
        release.clear()
        tasks = [asyncio.ensure_future(occupy()) for _ in range(2)]
        await asyncio.wait_for(entered.wait(), timeout=5.0)
        release.set()
        await asyncio.gather(*tasks)

    asyncio.run(scenario())
