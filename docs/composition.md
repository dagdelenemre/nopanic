# Composition

Policies are small on purpose; their value comes from stacking them. Stack decorators directly (the one closest to the function runs innermost), or name a stack once with `compose` (the first argument is outermost):

```python
from nopanic import CircuitOpen, backoff, circuit_breaker, compose, fallback, retry, timeout

llm = circuit_breaker(failure_threshold=0.5, min_calls=8, reset_timeout=20.0, name="llm")

resilient = compose(
    fallback(lambda exc: "Sorry, try again later.", on=(ConnectionError, CircuitOpen, TimeoutError)),
    retry(attempts=3, on=(ConnectionError, TimeoutError), backoff=backoff.full_jitter(0.2)),
    llm,
    timeout(30.0),
)

@resilient
async def ask(prompt: str) -> str: ...
```

## The recommended order

Outermost to innermost:

1. `fallback`: sees only what nothing else could absorb.
2. `deadline`: one budget for everything below.
3. `retry` (with a `RetryBudget`): retries whole guarded attempts.
4. `circuit_breaker`: every attempt's outcome feeds it; while open, attempts fail instantly.
5. `rate_limit` / `adaptive_rate_limit` / `bulkhead`: admission control per attempt.
6. `timeout`: bounds each attempt.

A few interactions worth knowing:

- **`timeout` inside `retry`** gives each attempt a fresh limit, usually the intent. Outside `retry`, it bounds all attempts together (prefer `deadline` for that).
- **`retry` and an open breaker:** `CircuitOpen` is retried only if you include it in `on`. With it included, `retry` waits out the breaker's `retry_after` (capped by `retry_after_cap`); without it, an open breaker fails fast.
- **`retry` and a rate limiter with `max_wait`:** `RateLimited` carries `retry_after`, so including it in `on` makes `retry` wait exactly long enough.
- **`adaptive_rate_limit` and a breaker:** the limiter's default `looks_throttled` ignores `CircuitOpen`, so a tripped breaker never collapses the adaptive rate.

## One-off calls

Every policy and every `compose` stack runs a call without decorating anything:

```python
user = resilient.call(client.get_user, user_id)
user = await resilient.acall(async_client.get_user, user_id)
```

`acall` accepts anything that returns an awaitable: an `async def`, a bound method of an async client, or a lambda. On a stack, `call` / `acall` build the wrapper chain per call (a few microseconds); decorate once on hot paths.

## Blocks of code

Stateful policies guard blocks as well as functions:

```python
with payments_breaker.guard(), payments_bulkhead.guard():
    ...

async with timeout(5.0).guard():
    ...
```
