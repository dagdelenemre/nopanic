# retry

Re-run a call that failed with a transient error, waiting between attempts.

```python
from nopanic import backoff, retry

@retry(
    attempts=5,                                       # total, including the first
    on=(ConnectionError, TimeoutError),               # or a predicate: lambda e: ...
    backoff=backoff.full_jitter(base=0.1, cap=30.0),  # the default strategy
    giveup=lambda e: getattr(e, "status", 0) == 401,  # never retry hopeless errors
    before_sleep=lambda a: log.warning("attempt %d failed: %s", a.attempt, a.exception),
)
def fetch(): ...
```

When attempts run out, the **original exception is re-raised**. There is no `RetryError` to unwrap.

## Backoff strategies

| Strategy | Delays |
| --- | --- |
| `backoff.fixed(delay=1.0)` | constant |
| `backoff.exponential(base=0.1, factor=2.0, cap=30.0)` | deterministic growth |
| `backoff.full_jitter(base=0.1, factor=2.0, cap=30.0)` | uniform(0, exponential): recommended for shared services |
| `backoff.decorrelated_jitter(base=0.1, cap=30.0)` | uniform(base, previous x 3) |
| a number | fixed seconds |
| any iterable of floats | your own; retrying stops when it runs out |

## Retry-After

An exception with a numeric `retry_after` attribute (seconds) raises the wait to at least that long, plus a small margin (5% + 50 ms) so the server's window has really passed. `CircuitOpen` and `RateLimited` carry it; for HTTP clients, set it from the `Retry-After` header (see the [HTTP guide](../guides/http.md)). Server-sent hints are capped by `retry_after_cap` (default 60 s) so a hostile server cannot park your client. Disable with `honor_retry_after=False`.

## Time budgets

`max_time=20.0` bounds the whole retry loop: retrying stops as soon as the next wait would end past the budget, so no attempt ever starts late. An ambient [`deadline`](deadline.md) works the same way, across every policy inside it.

## Retrying on results

Some clients return an error response instead of raising. `on_result` treats a return value as a failure:

```python
@retry(attempts=4, on_result=lambda response: response.status_code in (429, 502, 503))
async def get(url):
    return await client.get(url)
```

When retrying stops, the last result is returned as-is. In `before_sleep`, `RetryAttempt.result` holds the rejected value and `RetryAttempt.exception` is None.

## Retry budgets

`retry(budget=...)` shares a [`RetryBudget`](retry-budget.md) across calls and policies, so retries stay a small fraction of real traffic during an outage.

## Streams

On a generator or async generator function, retry re-opens a stream that fails **before its first item**. After an item was delivered, a failure propagates: delivered items cannot be taken back. `on_result` does not apply to streams.

## Events

`retry.attempt_failed` (attempt, delay, exception) before each wait and `retry.gave_up` (attempt, exception, reason) when it stops; reason is one of `attempts`, `giveup`, `backoff`, `max_time`, `deadline`, `budget`. A non-matching exception propagates silently.
