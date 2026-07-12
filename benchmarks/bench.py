"""Per-call overhead of each nopanic policy on its hot (success) path.

Run:  python benchmarks/bench.py
Numbers are microseconds per call, single thread, best of 5 rounds.
Absolute values depend on your machine; the point is the overhead
relative to a bare function call.
"""

from __future__ import annotations

import time

from nopanic import (
    CircuitBreaker,
    adaptive_rate_limit,
    bulkhead,
    cache,
    events,
    rate_limit,
    retry,
)

N = 200_000
ROUNDS = 5


def bare(x):
    return x


def measure(label, fn):
    best = float("inf")
    for _ in range(ROUNDS):
        start = time.perf_counter()
        for i in range(N):
            fn(i)
        best = min(best, time.perf_counter() - start)
    print(f"{label:<42} {best / N * 1e6:8.3f} us/call")
    return best / N


def main():
    print(f"N={N} calls/round, best of {ROUNDS} rounds\n")
    base = measure("bare function (baseline)", bare)

    measure("retry() success path", retry()(bare))
    measure("circuit_breaker() success path", CircuitBreaker()(bare))
    measure("rate_limit(1e9) hot path", rate_limit(1e9)(bare))
    measure("adaptive_rate_limit(1e9) success path", adaptive_rate_limit(1e9)(bare))
    measure("bulkhead(64) sync path", bulkhead(64)(bare))

    cached = cache(ttl=3600.0)(lambda x: x)
    cached(1)  # warm the single entry

    def cache_hit(i):
        return cached(1)

    measure("cache() fresh hit", cache_hit)

    def emit_only(i):
        events.emit("bench.tick", "bench")

    measure("events.emit, zero listeners", emit_only)

    cancel = events.subscribe(lambda e: None)
    measure("events.emit, one no-op listener", emit_only)
    cancel()

    print(f"\nbaseline was {base * 1e6:.3f} us/call; subtract it to get pure overhead")


if __name__ == "__main__":
    main()
