# Changelog

## 0.2.0 (2026-07-12)

New capabilities, no breaking changes.

- `events`: a process-global observability stream. `events.subscribe()`
  receives a typed `Event` for everything every policy does (retry attempts,
  breaker transitions, rejections, throttle adjustments, stale cache
  serves). Listeners can never break the guarded call.
- `adaptive_rate_limit`: AIMD client-side rate limiting. Throttle responses
  (429/503 or anything carrying `retry_after`) cut the rate
  multiplicatively, successes recover it additively, and an explicit
  `Retry-After` blocks the bucket for exactly that long. Ships with the
  `looks_throttled` heuristic covering requests/httpx error shapes.
- `cache`: per-arguments result cache with a stale-while-failing window;
  when a refresh fails, the expired value is served for up to `stale_ttl`
  extra seconds instead of the error. LRU-bounded, custom `key` support.
- `retry` now honors a numeric `retry_after` attribute on raised exceptions
  (HTTP Retry-After, `CircuitOpen`), raising the wait to at least that
  long; disable with `honor_retry_after=False`. Server-sent hints are
  capped (`retry_after_cap`, default 60s) so a hostile server cannot park
  the client; the same cap exists as `max_block` on `adaptive_rate_limit`.
- Performance: success paths measured and tuned (retry no longer builds a
  backoff iterator unless a failure happens); `benchmarks/bench.py` added,
  per-policy overhead documented in the README (~0.1 to ~1.3 us/call).
- Thread-safety stress tests for cache, adaptive rate limit, breaker and
  the event registry.
- Repo: AGENTS.md guide for AI coding agents, with Cursor/Copilot/Claude
  pointer files.

## 0.1.3 (2026-07-10)

- Metadata-only release: corrected the author name. No code changes.

## 0.1.2 (2026-07-10)

- Metadata-only release: project contact email updated to
  dagdelen.cyber@gmail.com. No code changes.

## 0.1.1 (2026-07-10)

- Packaging fix: the 0.1.0 source distribution accidentally included local
  scratch files (test experiments and a local tool config; no secrets, and
  installed wheels were never affected). The sdist now uses an explicit
  allowlist. The 0.1.0 release has been removed from PyPI.

## 0.1.0 (2026-07-09)

Initial release.

- `retry`: pluggable backoff strategies (`fixed`, `exponential`,
  `full_jitter`, `decorrelated_jitter`), exception filters, `giveup`
  predicate and `before_sleep` hook. Re-raises the original exception on
  exhaustion.
- `circuit_breaker`: failure-rate breaker over a sliding time window with
  closed/open/half-open states, probe budget, `on_state_change` hook and
  injectable clock.
- `timeout`: event-loop cancellation for async, abandoned worker thread for
  sync.
- `rate_limit`: token-bucket client-side rate limiter; waits by default,
  fails fast with `RateLimited` when `max_wait` is set.
- `bulkhead`: concurrency cap with optional bounded wait; per-event-loop
  semaphores for async.
- `fallback`: substitute value or handler on matching exceptions.
- `hedge`: hedged requests for tail-latency control (async only).
- `compose`: combine policies into a single reusable decorator.
- Zero runtime dependencies, `py.typed`, Python 3.10-3.14.
- Hardening: NaN/infinity/boolean rejection on all numeric parameters,
  `__slots__` on policies, no `assert` in library code (`python -O` safe),
  observability hooks cannot break guarded calls, hedge losers are cancelled
  and reaped, exhausted backoff iterators end retrying gracefully.
