from __future__ import annotations

import asyncio

import pytest

from nopanic import fallback


def test_static_value():
    @fallback([], on=ConnectionError)
    def fetch():
        raise ConnectionError("down")

    assert fetch() == []


def test_handler_receives_exception():
    @fallback(lambda exc: f"degraded: {exc}", on=ValueError)
    def compute():
        raise ValueError("bad input")

    assert compute() == "degraded: bad input"


def test_non_matching_exception_propagates():
    @fallback("default", on=ConnectionError)
    def bug():
        raise KeyError("a real bug")

    with pytest.raises(KeyError):
        bug()


def test_success_bypasses_fallback():
    @fallback("default")
    def fine():
        return "real"

    assert fine() == "real"


def test_async_with_sync_handler():
    @fallback(lambda exc: "degraded")
    async def fetch():
        raise ConnectionError("down")

    assert asyncio.run(fetch()) == "degraded"


def test_async_with_async_handler():
    async def rescue(exc):
        await asyncio.sleep(0)
        return "async degraded"

    @fallback(rescue)
    async def fetch():
        raise ConnectionError("down")

    assert asyncio.run(fetch()) == "async degraded"


def test_async_handler_on_sync_function_is_an_error():
    async def rescue(exc):
        return "nope"

    @fallback(rescue)
    def fetch():
        raise ConnectionError("down")

    with pytest.raises(TypeError, match="async fallback handler"):
        fetch()
