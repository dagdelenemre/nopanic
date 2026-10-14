# RetryBudget

Per-call retry limits do not stop retry storms. When a dependency degrades, every client retries: three attempts per call can triple the load on the one service that is already struggling. A retry budget (the Finagle, gRPC and Envoy approach) caps retries **across** calls, as a fraction of real traffic.

```python
from nopanic import RetryBudget, retry

user_service = RetryBudget(ratio=0.1)   # retries <= 10% of calls, plus a floor

@retry(attempts=4, budget=user_service)
def get_user(uid): ...

@retry(attempts=4, budget=user_service)  # one budget per downstream, shared
def get_orders(uid): ...
```

## How it counts

Within a sliding `window` (default 10 s):

```
allowed retries = ratio x first attempts + min_retries_per_sec x window
```

- `ratio` (default 0.2): retries earned per first attempt.
- `min_retries_per_sec` (default 1.0): a floor, so a client with little traffic can still retry at all.

When the budget is spent, `retry` stops and re-raises the original exception (event `retry.gave_up`, reason `budget`). Under a real outage, retry traffic shrinks to the floor instead of multiplying; once things recover, the budget refills with traffic.

`budget.available` reports retries spendable right now. The budget is thread-safe and uses constant memory (bucketed counters, like the breaker).
