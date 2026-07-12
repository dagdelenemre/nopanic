"""Thread-safety stress tests for the stateful policies."""

from __future__ import annotations

import contextlib
import threading

import pytest

from nopanic import CircuitBreaker, adaptive_rate_limit, cache, events, rate_limit


def hammer(worker, threads=8, iterations=300):
    """Run *worker(thread_index)* concurrently; collect any error."""
    errors = []

    def run(index):
        try:
            for _ in range(iterations):
                worker(index)
        except Exception as exc:
            errors.append(exc)

    pool = [threading.Thread(target=run, args=(i,)) for i in range(threads)]
    for t in pool:
        t.start()
    for t in pool:
        t.join(timeout=30)
    assert not errors, errors[:3]


def test_cache_under_concurrent_mixed_load():
    c = cache(ttl=0.001, stale_ttl=10.0, maxsize=64)

    @c
    def fetch(x):
        if x % 7 == 0:
            raise ConnectionError("periodic failure")
        return x

    def worker(index):
        for x in range(20):
            with contextlib.suppress(ConnectionError):
                fetch((index * 20 + x) % 100)

    hammer(worker)
    assert len(c._entries) <= 64  # maxsize invariant survived the stampede


def test_adaptive_rate_stays_in_bounds_under_concurrency():
    arl = adaptive_rate_limit(1_000_000.0, min_rate=10.0, increase=5.0, max_wait=0.0)

    class Throttled(Exception):
        status_code = 429

    @arl
    def call(fail):
        if fail:
            raise Throttled()
        return 1

    def worker(index):
        with contextlib.suppress(Exception):
            call(index % 3 == 0)

    hammer(worker)
    assert 10.0 <= arl.current_rate <= 1_000_000.0


def test_breaker_under_concurrent_failures():
    breaker = CircuitBreaker(failure_threshold=0.5, min_calls=10, reset_timeout=0.001)

    @breaker
    def flaky(fail):
        if fail:
            raise ConnectionError("down")
        return 1

    def worker(index):
        with contextlib.suppress(Exception):
            flaky(index % 2 == 0)

    hammer(worker)
    assert breaker.state in ("closed", "open", "half_open")  # and no deadlock


def test_rate_limit_token_accounting_under_concurrency():
    rl = rate_limit(1_000_000.0)

    @rl
    def call():
        return 1

    hammer(lambda index: call())
    assert rl._tokens <= rl._capacity


def test_subscribe_unsubscribe_race_with_emits():
    seen = []

    def worker(index):
        if index % 2 == 0:
            cancel = events.subscribe(seen.append)
            cancel()
        else:
            events.emit("stress.test", "stress")

    hammer(worker)
    # No assertion on counts (racy by design); the test is that nothing broke
    # and no listener leaked past its cancel.
    events.emit("stress.final", "stress")
    final = [e for e in seen if e.kind == "stress.final"]
    assert final == []


def test_no_stray_listeners_left_behind():
    # Guard for the whole suite: policies must not auto-register listeners.
    marker = []
    cancel = events.subscribe(marker.append)
    cancel()
    events.emit("leak.check", "stress")
    assert marker == []


@pytest.mark.parametrize("threads", [2, 16])
def test_cache_hit_is_consistent_across_threads(threads):
    c = cache(ttl=100.0)
    calls = []
    lock = threading.Lock()

    @c
    def fetch():
        with lock:
            calls.append(1)
        return "value"

    hammer(lambda index: fetch(), threads=threads, iterations=200)
    # Without single-flight a few duplicate refreshes may race at startup,
    # but the count must stay tiny rather than growing with load.
    assert len(calls) <= threads
