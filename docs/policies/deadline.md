# deadline

A timeout bounds one call. A deadline bounds a whole operation, however many retries, waits and nested calls it takes.

```python
from nopanic import deadline, time_remaining

async with deadline(5.0):                 # or: with deadline(5.0):
    profile = await get_profile(uid)      # retries, timeouts, rate-limit waits...
    orders = await get_orders(uid)        # ...all share the same 5 seconds
```

The deadline is an ambient value (a `contextvars` variable): it follows your code into awaited coroutines, Trio tasks and `timeout` worker threads. Policies inside it cooperate:

| Policy | Inside a deadline |
| --- | --- |
| `timeout` | Clamps its limit to the time remaining; fails at once if the deadline has passed |
| `retry` | Gives up instead of sleeping into a wait that would outlast it (reason `deadline`) |
| `rate_limit`, `adaptive_rate_limit`, `redis_rate_limit` | Reject with `RateLimited` instead of queueing past it |
| `bulkhead` | Rejects with `BulkheadFull` instead of waiting past it |

Nested deadlines can only shorten the budget, never extend it.

## It cooperates, it does not interrupt

`deadline` declares a budget; it does not cancel anything by itself. To *enforce* it on a call, put a `timeout` inside: it uses whichever limit is tighter. Pass the rest to your own clients:

```python
response = httpx.get(url, timeout=time_remaining())
```

`time_remaining()` returns the seconds left, or None outside a deadline.

## As a policy

`deadline` is also a policy, giving each call its own budget:

```python
@compose(deadline(10.0), retry(attempts=5), timeout(3.0))
def sync_inventory(): ...
```

One instance must not be entered as a `with` block twice at the same time (create one per block); as a decorator it can be shared freely. Generator functions are not supported: a context value set inside a generator would leak into the consumer between items.
