from __future__ import annotations

import asyncio
import time

import pytest

from nopanic import hedge


def test_hedge_beats_slow_first_attempt():
    calls = []

    @hedge(delay=0.05)
    async def variable():
        call_no = len(calls) + 1
        calls.append(call_no)
        if call_no == 1:
            await asyncio.sleep(6.0)  # pathologically slow first attempt
        return f"result-{call_no}"

    start = time.monotonic()
    result = asyncio.run(variable())
    elapsed = time.monotonic() - start

    assert result == "result-2"
    assert elapsed < 5.0  # returned well before the slow attempt would finish
    assert len(calls) == 2


def test_fast_first_attempt_never_hedges():
    calls = []

    @hedge(delay=2.0)  # generous: a stalled CI runner must not fire the hedge
    async def quick():
        calls.append(1)
        return "fast"

    assert asyncio.run(quick()) == "fast"
    assert len(calls) == 1


def test_failed_attempt_hedges_immediately():
    calls = []

    @hedge(delay=10.0)  # delay so large only a failure can trigger the hedge
    async def flaky():
        calls.append(1)
        if len(calls) == 1:
            raise ConnectionError("first attempt died")
        return "recovered"

    start = time.monotonic()
    assert asyncio.run(flaky()) == "recovered"
    assert time.monotonic() - start < 8.0  # well under the 10s hedge delay
    assert len(calls) == 2


def test_all_attempts_fail_raises_last():
    @hedge(delay=0.01, max_hedges=2)
    async def doomed():
        raise ConnectionError("everything is down")

    with pytest.raises(ConnectionError):
        asyncio.run(doomed())


def test_sync_function_rejected():
    @hedge(delay=0.1)
    def sync_fn():
        return 1

    with pytest.raises(TypeError, match="async"):
        sync_fn()


def test_validation():
    with pytest.raises(ValueError):
        hedge(delay=-1)
    with pytest.raises(ValueError):
        hedge(delay=0.1, max_hedges=0)
