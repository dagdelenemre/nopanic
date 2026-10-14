# Changelog

## 0.1.0 (2026-10-14)

Initial release, for nopanic 0.3.0.

- `redis_circuit_breaker`: fleet-wide failure-rate circuit breaker with a
  bucketed sliding window, half-open probes and atomic Lua scripts.
- `redis_rate_limit`: fleet-wide token bucket with waiting, `max_wait`,
  deadline support and refunds for cancelled waiters.
- Sync (`redis.Redis`) and async (`redis.asyncio.Redis`) clients, fail-open
  by default when Redis is unreachable.
