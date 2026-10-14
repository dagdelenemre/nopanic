# Getting started

## Install

```
pip install nopanic
```

No dependencies. Optional add-ons: [`nopanic-otel`](observability.md#opentelemetry) and [`nopanic-redis`](distributed.md).

## A policy is a decorator

Every policy is an object you apply as a decorator. It works the same on `def`, `async def`, and (for the policies where it makes sense) generator functions:

```python
from nopanic import retry

@retry(attempts=3, on=ConnectionError)
def fetch_profile(user_id): ...

@retry(attempts=3, on=ConnectionError)
async def fetch_profile_async(user_id): ...
```

## Stateful policies are shared on purpose

Breakers, bulkheads and rate limiters hold state. Create **one instance per dependency** and use it at every call site that talks to that dependency:

```python
from nopanic import circuit_breaker, rate_limit

payments = circuit_breaker(name="payments", min_calls=10, reset_timeout=15.0)
payments_quota = rate_limit(100, per=1.0, name="payments")

@payments_quota
@payments
def charge(order): ...

@payments_quota
@payments
def refund(order): ...
```

## Stack them in the right order

The decorator closest to the function runs innermost. A typical production stack, outermost to innermost:

```python
from nopanic import CircuitOpen, backoff, compose, deadline, fallback, retry, timeout

resilient = compose(
    fallback(lambda exc: CACHED_ANSWER, on=(ConnectionError, TimeoutError, CircuitOpen)),
    deadline(10.0),                                   # the whole operation
    retry(attempts=3, on=(ConnectionError, TimeoutError), backoff=backoff.full_jitter(0.2)),
    payments,                                         # the breaker
    payments_quota,                                   # the rate limit
    timeout(2.0),                                     # each attempt
)

@resilient
def quote(order): ...
```

Read it inside out: each attempt gets 2 seconds, waits for a token, feeds the breaker; transient failures retry with jitter; the whole thing never exceeds 10 seconds; anything left becomes a graceful answer. See [Composition](composition.md).

## No function to decorate?

```python
user = retry(attempts=3).call(client.get_user, user_id)        # sync
user = await resilient.acall(async_client.get_user, user_id)    # async
```

`call` / `acall` exist on every policy and every `compose` stack.

## What a policy raises

User exceptions are never wrapped: when retries run out you get the original exception back. Policies raise only:

| Exception | When |
| --- | --- |
| `CircuitOpen` | The breaker rejects a call (has `retry_after`) |
| `BulkheadFull` | No slot within `max_wait` |
| `RateLimited` | No token within `max_wait` (has `retry_after`) |
| `TimeoutError` (builtin) | A `timeout` expired |

The first three subclass `ResilienceError`.
