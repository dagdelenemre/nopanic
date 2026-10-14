# Changelog

## 0.3.0 (2026-10-14)

Correctness fixes found in a full review, plus the most requested missing
pieces. Upgrading is recommended for everyone using `circuit_breaker`
with async code or `timeout` on Python 3.11+.

Fixes:

- `circuit_breaker`: a half-open probe that was cancelled (a `hedge` loser,
  an outer timeout, a client that disconnected) or interrupted kept its
  probe slot forever. The breaker then stayed half-open and rejected every
  call with `CircuitOpen(retry_after=0)` until `reset()`. Cancellation now
  releases the slot without recording an outcome.
- `timeout` (async, Python 3.11+): a `TimeoutError` raised by the wrapped
  function itself was replaced by the policy's own `TimeoutError` and
  reported as a `timeout.expired` event, breaking the "no wrapper
  exceptions" guarantee. Only an expiry of the policy's own deadline is
  replaced now. The async path also no longer spawns an extra task.
- `timeout` (sync): the worker thread now runs in a copy of the caller's
  `contextvars` context, so tracing spans, request IDs and logging context
  are visible inside the call.
- `bulkhead` (async, `max_wait > 0`): replaced `asyncio.wait_for` around the
  semaphore, which on Python 3.10/3.11 could drop a permit acquired at the
  instant of the timeout and silently shrink the bulkhead for good.
- `CircuitOpen` could not be unpickled, which broke it across process pools,
  `multiprocessing` and task-queue result backends.
- `looks_throttled` no longer treats nopanic's own signals (`CircuitOpen`,
  `RateLimited`) as server pushback: an adaptive limiter wrapped around a
  tripped breaker used to collapse its own rate.
- `retry(attempts=...)` now rejects booleans and non-integers.

New:

- `retry(max_time=...)`: a total time budget. Retrying stops as soon as the
  next wait would end past the deadline, so no attempt starts late.
- `retry(on_result=...)`: retry on a rejected return value (an HTTP client
  that returns a 503 response instead of raising). When retrying stops the
  last result is returned as-is; no wrapper exception. `RetryAttempt` gains
  a `result` field, and its `exception` is None for result-triggered
  retries.
- `retry(clock=...)` for deterministic `max_time` tests.
- `policy.call(fn, *args, **kwargs)` and `await policy.acall(fn, ...)` on
  every policy and on `compose` stacks: guard a one-off call (a third-party
  SDK method, a lambda) without defining a decorated function. Fully typed
  via `ParamSpec`.
- `cache` refreshes are now single-flight (roadmap item): concurrent misses
  of the same key share one call to the dependency and its outcome instead
  of stampeding it. On by default; `single_flight=False` restores the old
  behavior.
- `RateLimited.retry_after`: the wait that exceeded `max_wait`, so an outer
  `retry` waits exactly long enough.

Compatibility notes: `compose` is now a class (still a decorator, same
call signature). Type checkers will see `RetryAttempt.exception` as
optional.

## 0.2.1 (2026-07-13)

Performance and memory fix for the circuit breaker, found by load testing.
No breaking changes; adds one parameter.

- The failure-rate window is now bucketed (Hystrix-style): `window_buckets`
  fixed time buckets of counters (default 10) with running totals, instead
  of one record per call. Window memory is now constant regardless of
  traffic (previously ~88 MB per million in-window calls) and the rate
  check is O(1) (previously a full-window scan per failure, which degraded
  quadratically under sustained partial failure and serialized threads
  behind the breaker lock: a 1.6M-call multi-threaded load that took over
  40 minutes now completes in about 2 seconds).
- Outcomes now expire in bucket-sized steps (window/window_buckets seconds)
  rather than at exact per-call age; open/close behavior is otherwise
  unchanged and the full test suite passes as-is.
- CI now also builds the wheel, installs it into a clean environment and
  runs the test suite against the installed package, so packaging defects
  cannot hide behind the editable install.

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
  long plus a small safety margin (5% + 50ms: "retry after X" means "not
  before X", and on Windows `time.sleep` can wake before the monotonic
  deadline, which would burn attempts against a still-closed window);
  disable with `honor_retry_after=False`. Server-sent hints are capped
  (`retry_after_cap`, default 60s) so a hostile server cannot park the
  client; the same cap exists as `max_block` on `adaptive_rate_limit`.
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
