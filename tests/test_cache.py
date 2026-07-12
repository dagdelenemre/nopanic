from __future__ import annotations

import asyncio

import pytest

from nopanic import Event, cache, subscribe


def test_fresh_hit_skips_the_call(clock):
    calls = []
    c = cache(ttl=10.0, clock=clock)

    @c
    def fetch(x):
        calls.append(x)
        return x * 2

    assert fetch(3) == 6
    assert fetch(3) == 6
    assert calls == [3]  # second call served from cache


def test_expiry_triggers_refresh(clock):
    calls = []
    c = cache(ttl=10.0, clock=clock)

    @c
    def fetch(x):
        calls.append(x)
        return len(calls)

    assert fetch(1) == 1
    clock.advance(11.0)
    assert fetch(1) == 2
    assert calls == [1, 1]


def test_failure_within_stale_window_serves_stale(clock):
    c = cache(ttl=10.0, stale_ttl=60.0, on=ConnectionError, clock=clock)
    healthy = [True]

    @c
    def fetch():
        if not healthy[0]:
            raise ConnectionError("down")
        return "fresh-value"

    assert fetch() == "fresh-value"
    healthy[0] = False
    clock.advance(30.0)  # expired but within ttl+stale_ttl
    assert fetch() == "fresh-value"  # stale served instead of the error


def test_failure_beyond_stale_window_raises(clock):
    c = cache(ttl=10.0, stale_ttl=5.0, clock=clock)
    healthy = [True]

    @c
    def fetch():
        if not healthy[0]:
            raise ConnectionError("down")
        return "value"

    fetch()
    healthy[0] = False
    clock.advance(16.0)  # past ttl + stale_ttl
    with pytest.raises(ConnectionError):
        fetch()


def test_non_matching_failure_propagates_even_with_stale(clock):
    c = cache(ttl=1.0, stale_ttl=100.0, on=ConnectionError, clock=clock)
    healthy = [True]

    @c
    def fetch():
        if not healthy[0]:
            raise KeyError("a bug, not an outage")
        return "value"

    fetch()
    healthy[0] = False
    clock.advance(2.0)
    with pytest.raises(KeyError):
        fetch()


def test_stale_serve_emits_event(clock):
    seen: list[Event] = []
    cancel = subscribe(seen.append)
    try:
        c = cache(ttl=1.0, stale_ttl=100.0, clock=clock)
        healthy = [True]

        @c
        def fetch():
            if not healthy[0]:
                raise ConnectionError("down")
            return "v"

        fetch()
        healthy[0] = False
        clock.advance(2.0)
        fetch()
    finally:
        cancel()
    assert [e.kind for e in seen] == ["cache.stale_served"]
    assert seen[0].data["age"] == pytest.approx(2.0)


def test_no_stale_ttl_means_plain_ttl_cache(clock):
    c = cache(ttl=1.0, clock=clock)
    healthy = [True]

    @c
    def fetch():
        if not healthy[0]:
            raise ConnectionError("down")
        return "v"

    fetch()
    healthy[0] = False
    clock.advance(2.0)
    with pytest.raises(ConnectionError):
        fetch()


def test_lru_eviction(clock):
    calls = []
    c = cache(ttl=100.0, maxsize=2, clock=clock)

    @c
    def fetch(x):
        calls.append(x)
        return x

    fetch(1), fetch(2), fetch(3)  # 1 evicted (LRU)
    fetch(1)
    assert calls == [1, 2, 3, 1]


def test_entries_are_keyed_per_function(clock):
    c = cache(ttl=100.0, clock=clock)

    @c
    def a():
        return "a-result"

    @c
    def b():
        return "b-result"

    assert a() == "a-result"
    assert b() == "b-result"


def test_kwargs_order_does_not_matter(clock):
    calls = []
    c = cache(ttl=100.0, clock=clock)

    @c
    def fetch(*, x, y):
        calls.append((x, y))
        return x + y

    assert fetch(x=1, y=2) == 3
    assert fetch(y=2, x=1) == 3
    assert calls == [(1, 2)]


def test_custom_key_for_unhashable_args(clock):
    calls = []
    c = cache(ttl=100.0, key=lambda payload: payload["id"], clock=clock)

    @c
    def fetch(payload):
        calls.append(payload["id"])
        return payload["id"] * 10

    assert fetch({"id": 7, "noise": [1, 2]}) == 70
    assert fetch({"id": 7, "noise": [3, 4]}) == 70  # same id -> cached
    assert calls == [7]


def test_async_path(clock):
    calls = []
    c = cache(ttl=10.0, stale_ttl=60.0, clock=clock)
    healthy = [True]

    @c
    async def fetch(x):
        calls.append(x)
        if not healthy[0]:
            raise ConnectionError("down")
        return x * 2

    async def scenario():
        assert await fetch(5) == 10
        assert await fetch(5) == 10  # fresh hit
        healthy[0] = False
        clock.advance(30.0)
        assert await fetch(5) == 10  # stale served

    asyncio.run(scenario())
    assert calls == [5, 5]  # second real call was the failed refresh


def test_clear(clock):
    calls = []
    c = cache(ttl=100.0, clock=clock)

    @c
    def fetch():
        calls.append(1)
        return "v"

    fetch(), fetch()
    c.clear()
    fetch()
    assert len(calls) == 2


def test_validation():
    with pytest.raises(ValueError):
        cache(ttl=0)
    with pytest.raises(ValueError):
        cache(ttl=1.0, stale_ttl=-1)
    with pytest.raises(ValueError):
        cache(ttl=1.0, maxsize=0)
