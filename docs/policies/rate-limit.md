# rate_limit and adaptive_rate_limit

Client-side rate limiting with a token bucket: be a good client before the server has to tell you.

## rate_limit

```python
from nopanic import rate_limit

embeddings_quota = rate_limit(90, per=60.0, burst=10, name="embeddings")  # 90/min, bursts of 10

@embeddings_quota
async def embed(text): ...
```

- Throttled calls **wait** for their token by default, roughly first come, first served. Waiting callers reserve their token up front; one that is cancelled while queued gives it back.
- `max_wait=N` raises `RateLimited` instead when the wait would exceed N seconds (`max_wait=0`: never wait). The exception's `retry_after` is the wait it refused, which an outer `retry` honors.
- Inside an active [`deadline`](deadline.md), a call never waits past it.
- `limiter.available_tokens` shows the bucket (negative while callers are queued).
- `with limiter.guard():` / `async with` takes a token for a block; a stream takes one token when it opens.

## adaptive_rate_limit

Do not know the server's real limit? Let it tell you. AIMD, the scheme TCP uses for congestion control:

```python
from nopanic import adaptive_rate_limit

api = adaptive_rate_limit(50, per=1.0, min_rate=5)   # start at 50/s, never below 5/s

@api
async def call_api(): ...
```

- A "slow down" error (HTTP 429/503 or anything carrying `retry_after`) multiplies the rate by `decrease` (default 0.5), floored at `min_rate`.
- Every success adds `increase` (default 0.1 calls per period), capped at `max_rate` (default: the starting rate).
- An explicit `retry_after` on the error blocks the bucket for that long, capped by `max_block` (default 60 s).
- The throttle error still propagates; pair with `retry` to re-attempt it.
- `throttle_on` decides what counts as pushback. The default, `looks_throttled`, checks `retry_after`, `status`/`status_code` and `response.status_code` (requests and httpx errors), and never treats nopanic's own `CircuitOpen` / `RateLimited` as server pushback.
- `api.current_rate` and `api.available_tokens` show its state.

## Across processes

A local limiter limits one process. For a quota shared by every worker, use [`redis_rate_limit`](../distributed.md); for one limiter per tenant or model, [`keyed`](keyed.md).

## Events

`ratelimit.waited` (seconds), `ratelimit.rejected` (would_wait, max_wait), `ratelimit.throttled` (old_rate, new_rate, retry_after; adaptive only).
