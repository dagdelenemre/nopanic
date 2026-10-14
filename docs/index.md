# nopanic

**The unified resilience toolkit for Python.** Retries, circuit breakers, timeouts, rate limits, bulkheads, hedged requests, retry budgets and deadlines: one decorator-based API, identical for sync and async code (streams included), with **zero runtime dependencies**.

```python
from nopanic import backoff, circuit_breaker, retry, timeout

llm_breaker = circuit_breaker(failure_threshold=0.5, reset_timeout=15.0, name="llm")

@retry(attempts=4, on=ConnectionError, backoff=backoff.full_jitter(base=0.2, cap=10.0))
@llm_breaker
@timeout(30.0)
async def call_llm(prompt: str) -> str:
    ...
```

```
pip install nopanic
```

Python 3.10+, fully typed. Runs on asyncio, Trio and AnyIO.

## Why

Java has resilience4j, .NET has Polly. Python has a drawer of single-purpose parts that were never designed to work together. These patterns only pay off when they **compose**: a timeout per attempt, retries that respect an open circuit and a shared retry budget, a deadline for the whole operation, a fallback that catches whatever is left. nopanic is that set of parts, built as one system.

| You need | nopanic |
| --- | --- |
| Retry with backoff, Retry-After, result checks | [`retry`](policies/retry.md) |
| Stop calling what is down (and what is too slow) | [`circuit_breaker`](policies/circuit-breaker.md) |
| Bound each call, each stream item | [`timeout`](policies/timeout.md) |
| Respect quotas, or discover them (AIMD) | [`rate_limit`, `adaptive_rate_limit`](policies/rate-limit.md) |
| Cap concurrency | [`bulkhead`](policies/bulkhead.md) |
| Degrade gracefully | [`fallback`](policies/fallback.md) |
| Fix tail latency | [`hedge`](policies/hedge.md) |
| Survive outages with stale data | [`cache`](policies/cache.md) |
| One time budget per operation | [`deadline`](policies/deadline.md) |
| Stop retry storms | [`RetryBudget`](policies/retry-budget.md) |
| One breaker per host or tenant | [`keyed`](policies/keyed.md) |
| Shared state across a fleet | [`nopanic-redis`](distributed.md) |
| Metrics and traces | [`events`, `nopanic-otel`](observability.md) |

## Principles

- **Zero dependencies.** A resilience library must not be a reliability risk itself.
- **Sync and async are equals.** One API; the wrapper flavour is chosen at decoration time.
- **No wrapper exceptions.** Your `except ConnectionError:` keeps working; policies add only their own precise signals (`CircuitOpen`, `BulkheadFull`, `RateLimited`) and the builtin `TimeoutError`.
- **Composition over configuration.** Small orthogonal policies and `compose`, not one object with forty options.
- **Fail loudly at decoration time.** A policy that cannot guard what you decorated says so immediately.
- **Testable time.** Injectable clocks and [`testing.instant()`](testing.md): your test suite never sleeps.
