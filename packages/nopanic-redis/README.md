# nopanic-redis

Redis-backed circuit breakers and rate limits for [nopanic](https://github.com/dagdelenemre/nopanic), shared by every process and host that uses them.

```
pip install nopanic-redis
```

A `nopanic.circuit_breaker` lives in one process. Behind a load balancer with 40 workers, each worker has to discover a dead dependency on its own, and a `rate_limit(100)` lets 4,000 calls through. These policies keep that state in Redis, so the fleet trips together and shares one quota:

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

- **Same model as the in-process policies.** `redis_circuit_breaker` takes the parameters of `nopanic.CircuitBreaker` (failure-rate threshold over a bucketed sliding window, `min_calls`, `reset_timeout`, `half_open_max_calls`, `on`). `redis_rate_limit` is `nopanic.rate_limit` (token bucket, `burst`, waiting by default, `RateLimited(retry_after=...)` with `max_wait`, ambient `deadline` respected, cancelled waiters refunded).
- **Identity is the name.** Every instance with the same `name` and `prefix` is the same breaker or bucket, whichever process created it. Keys use a Redis Cluster hash tag, so each policy's keys stay in one slot.
- **Atomic and cheap.** Each decision is one Lua script, one round trip: a breaker costs two per call (admit, record), a rate limit one.
- **Clocks.** Time comes from the callers' wall clocks (`time.time`, injectable via `clock=`); keep hosts NTP-synced. Skew only shifts window boundaries slightly.
- **Redis down is not your outage.** With `fail_open=True` (default) calls go through unguarded and a `breaker.store_error` / `ratelimit.store_error` event is emitted; `fail_open=False` rejects with `CircuitOpen` / `RateLimited` instead.
- **Sync or async by client.** A `redis.Redis` client serves sync functions, a `redis.asyncio.Redis` client serves async ones (use two instances with the same name to guard both; they share state).
- `breaker.get_state()` and `breaker.reset()` act fleet-wide (awaitable with an async client). Streams are not supported.

Events are the usual nopanic events, so `nopanic-otel` and `events.log_to` work unchanged.

## License

MIT.
