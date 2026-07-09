# Changelog

## 0.1.0 (2026-07-09)

Initial release.

- `retry` with pluggable backoff strategies (`fixed`, `exponential`,
  `full_jitter`, `decorrelated_jitter`), exception filters, `giveup`
  predicate and `before_sleep` hook. Re-raises the original exception on
  exhaustion.
- `circuit_breaker` — failure-rate breaker over a sliding time window with
  closed/open/half-open states, probe budget, `on_state_change` hook and
  injectable clock.
- `timeout` — event-loop cancellation for async, abandoned worker thread for
  sync.
- `rate_limit` — token-bucket client-side rate limiter; waits by default,
  fails fast with `RateLimited` when `max_wait` is set.
- `bulkhead` — concurrency cap with optional bounded wait; per-event-loop
  semaphores for async.
- `fallback` — substitute value or handler on matching exceptions.
- `hedge` — hedged requests for tail-latency control (async only).
- `compose` — combine policies into a single reusable decorator.
- Zero runtime dependencies, `py.typed`, Python 3.10–3.13.
- Hardening: NaN/infinity/boolean rejection on all numeric parameters,
  `__slots__` on policies, no `assert` in library code (`python -O` safe),
  observability hooks cannot break guarded calls, hedge losers are cancelled
  and reaped, exhausted backoff iterators end retrying gracefully.
