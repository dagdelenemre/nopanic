# Across processes (Redis)

A `circuit_breaker` lives in one process. Behind a load balancer with 40 workers, each worker discovers a dead dependency on its own, and a `rate_limit(100)` lets 4,000 calls through. `nopanic-redis` keeps that state in Redis, so the fleet trips together and shares one quota.

```
pip install nopanic-redis
```

```python
import redis
from nopanic_redis import redis_circuit_breaker, redis_rate_limit

r = redis.Redis()   # redis.asyncio.Redis() for async functions

payments = redis_circuit_breaker(r, name="payments", min_calls=20, reset_timeout=15.0)
openai_quota = redis_rate_limit(r, name="openai", rate=500, per=60.0, max_wait=5.0)

@openai_quota
@payments
def charge(order): ...
```

## Semantics

- **Same model as the local policies.** `redis_circuit_breaker` takes the parameters of `circuit_breaker` (failure threshold over a bucketed sliding window, `min_calls`, `reset_timeout`, `half_open_max_calls`, `on`); `redis_rate_limit` behaves like `rate_limit` (waiting, `max_wait`, `RateLimited.retry_after`, deadlines, refunds for cancelled waiters).
- **Identity is the name.** Every instance with the same `name` and `prefix` is the same breaker or bucket, whichever process created it. Keys carry a Redis Cluster hash tag, so one policy's keys share a slot.
- **One atomic script per decision.** A breaker costs two round trips per call (admit, record), a rate limit one.
- **Wall-clock time.** Decisions use the callers' `time.time()` (injectable via `clock=`). Keep hosts NTP-synced; skew only shifts window boundaries slightly.
- **Redis down is not your outage.** By default (`fail_open=True`) calls go through unguarded and a `breaker.store_error` / `ratelimit.store_error` event is emitted. `fail_open=False` rejects with `CircuitOpen` / `RateLimited` instead.
- **Client flavour decides call flavour.** A `redis.Redis` client serves sync functions, a `redis.asyncio.Redis` client async ones. To guard both, create two instances with the same name: they share state.
- `get_state()` and `reset()` act fleet-wide (they return awaitables with an async client). Streams are not supported.

Combine freely with local policies: a local `bulkhead` per process and a shared `redis_rate_limit` per fleet is a common pairing.
