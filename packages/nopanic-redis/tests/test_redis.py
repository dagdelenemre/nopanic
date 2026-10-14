"""Runs against fakeredis (with Lua) by default; set NOPANIC_REDIS_URL to also
run every test against a real Redis server (CI does)."""

from __future__ import annotations

import asyncio
import os
import uuid

import fakeredis
import pytest
import redis
import redis.asyncio

from nopanic import CircuitOpen, RateLimited, deadline
from nopanic.testing import capture_events, instant
from nopanic_redis import redis_circuit_breaker, redis_rate_limit

REDIS_URL = os.environ.get("NOPANIC_REDIS_URL")
BACKENDS = ["fake", "real"] if REDIS_URL else ["fake"]


class Clock:
    def __init__(self) -> None:
        self.now = 1_700_000_000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture(params=BACKENDS)
def clients(request):
    """Two sync clients on one server: two processes sharing state."""
    if request.param == "real":
        made = [redis.Redis.from_url(REDIS_URL) for _ in range(2)]
    else:
        server = fakeredis.FakeServer()
        made = [fakeredis.FakeRedis(server=server) for _ in range(2)]
    yield made
    for client in made:
        client.close()


@pytest.fixture(params=BACKENDS)
def async_client(request):
    if request.param == "real":
        return redis.asyncio.Redis.from_url(REDIS_URL)
    return fakeredis.FakeAsyncRedis()


@pytest.fixture
def name():
    return f"test-{uuid.uuid4().hex}"  # isolates tests sharing a real server


def test_breaker_state_is_shared_between_processes(clients, clock, name):
    a = redis_circuit_breaker(clients[0], name=name, min_calls=2, reset_timeout=10.0, clock=clock)
    b = redis_circuit_breaker(clients[1], name=name, min_calls=2, reset_timeout=10.0, clock=clock)

    @a
    def from_a():
        raise ConnectionError("down")

    @b
    def from_b():
        return "ok"

    for _ in range(2):
        with pytest.raises(ConnectionError):
            from_a()
    with pytest.raises(CircuitOpen) as excinfo:
        from_b()  # the other "process" sees the open breaker
    assert excinfo.value.retry_after == pytest.approx(10.0)
    assert b.get_state() == "open"

    clock.now += 10.0
    assert from_b() == "ok"  # half-open probe succeeds and closes it, fleet-wide
    assert a.get_state() == "closed"


def test_breaker_window_and_threshold(clients, clock, name):
    breaker = redis_circuit_breaker(
        clients[0], name=name, min_calls=4, failure_threshold=0.5, window=10.0, clock=clock
    )

    @breaker
    def call(ok):
        if not ok:
            raise ConnectionError

    call(True)
    call(True)
    call(True)
    with pytest.raises(ConnectionError):
        call(False)
    assert breaker.get_state() == "closed"  # 1 of 4
    clock.now += 11.0  # the successes age out
    with pytest.raises(ConnectionError):
        call(False)
    call(True)
    call(True)
    with pytest.raises(ConnectionError):
        call(False)
    assert breaker.get_state() == "open"  # 2 of 4 inside the window


def test_breaker_ignores_unselected_and_cancelled(clients, clock, name):
    breaker = redis_circuit_breaker(
        clients[0], name=name, min_calls=1, on=ConnectionError, clock=clock
    )

    @breaker
    def call():
        raise KeyError

    with pytest.raises(KeyError):
        call()
    assert breaker.get_state() == "closed"


def test_failed_probe_reopens_and_events_are_emitted(clients, clock, name):
    breaker = redis_circuit_breaker(
        clients[0], name=name, min_calls=1, reset_timeout=5.0, clock=clock
    )

    @breaker
    def down():
        raise ConnectionError

    with capture_events("breaker.state_change") as seen:
        with pytest.raises(ConnectionError):
            down()
        clock.now += 5.0
        with pytest.raises(ConnectionError):
            down()
    assert [(e.data["old"], e.data["new"]) for e in seen] == [
        ("closed", "open"),
        ("open", "half_open"),
        ("half_open", "open"),
    ]
    breaker.reset()
    assert breaker.get_state() == "closed"


def test_rate_limit_is_shared_between_processes(clients, clock, name):
    a = redis_rate_limit(clients[0], name=name, rate=2, per=10.0, max_wait=0, clock=clock)
    b = redis_rate_limit(clients[1], name=name, rate=2, per=10.0, max_wait=0, clock=clock)

    @a
    def from_a():
        return "a"

    @b
    def from_b():
        return "b"

    assert from_a() == "a"
    assert from_b() == "b"
    with pytest.raises(RateLimited) as excinfo:
        from_a()  # the shared bucket is empty
    assert excinfo.value.retry_after == pytest.approx(5.0)
    clock.now += 5.0
    assert from_b() == "b"


def test_rate_limit_waits_instantly_under_testing(clients, clock, name):
    limiter = redis_rate_limit(clients[0], name=name, rate=1, per=10.0, clock=clock)

    @limiter
    def call():
        return 1

    with instant() as sleeps:
        call()
        call()
    assert sleeps == [pytest.approx(10.0)]


def test_rate_limit_respects_deadline(clients, clock, name):
    limiter = redis_rate_limit(clients[0], name=name, rate=1, per=60.0, clock=clock)

    @limiter
    def call():
        return 1

    call()
    with deadline(1.0), pytest.raises(RateLimited):
        call()


class BrokenRedis:
    """A sync client whose server is unreachable (fails at once, no retries)."""

    def execute_command(self, *args, **kwargs):
        raise redis.ConnectionError("unreachable")

    def register_script(self, script):
        def run(keys=(), args=()):
            raise redis.ConnectionError("unreachable")

        return run


def test_store_failure_fails_open_by_default(clock):
    broken = BrokenRedis()
    breaker = redis_circuit_breaker(broken, name="x", clock=clock)
    limiter = redis_rate_limit(broken, name="y", rate=1, clock=clock)

    @limiter
    @breaker
    def call():
        return "served"

    with capture_events() as seen:
        assert call() == "served"
    assert {e.kind for e in seen} == {"breaker.store_error", "ratelimit.store_error"}


def test_store_failure_can_fail_closed(clock):
    broken = BrokenRedis()

    @redis_circuit_breaker(broken, name="x", fail_open=False, clock=clock)
    def call():
        return "served"

    with pytest.raises(CircuitOpen):
        call()


def test_async_breaker_and_rate_limit(async_client, clock, name):
    breaker = redis_circuit_breaker(async_client, name=name, min_calls=1, clock=clock)
    limiter = redis_rate_limit(async_client, name=name, rate=10, clock=clock)

    @limiter
    @breaker
    async def call(ok):
        if not ok:
            raise ConnectionError
        return "ok"

    async def scenario():
        assert await call(True) == "ok"
        with pytest.raises(ConnectionError):
            await call(False)
        with pytest.raises(CircuitOpen):
            await call(True)
        assert await breaker.get_state() == "open"
        await breaker.reset()
        await async_client.aclose()

    asyncio.run(scenario())


def test_client_flavour_must_match(clients, async_client, clock, name):
    sync_policy = redis_rate_limit(clients[0], name=name, rate=1, clock=clock)

    @sync_policy
    async def wrong():
        return 1

    with pytest.raises(TypeError, match=r"redis\.asyncio"):
        asyncio.run(wrong())


def test_validation(clients):
    with pytest.raises(ValueError):
        redis_circuit_breaker(clients[0], name="")
    with pytest.raises(ValueError):
        redis_rate_limit(clients[0], name="x", rate=0)
