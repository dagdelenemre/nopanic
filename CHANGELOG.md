# Changelog

## 0.3.0 (2026-10-14)

Correctness fixes found in a full review, streaming support, retry
budgets, deadline propagation, per-key policies, live metrics, test
helpers, Trio/AnyIO support, a public extension API, two add-on packages
(`nopanic-otel`, `nopanic-redis`) and a documentation site. Upgrading is recommended for everyone using `circuit_breaker`
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
- `retry(attempts=...)` now rejects booleans and non-integers; so do all
  other count parameters (`min_calls`, `max_concurrent`, `max_hedges`, ...).
- Generator and async generator functions (LLM token streams) were
  silently left unguarded: a sync wrapper was applied and the policy did
  nothing. They are now guarded properly (see Streams below), and policies
  that cannot guard a stream raise `TypeError` at decoration time.
- `timeout` (sync): every timed-out call left a worker thread behind with
  no bound, so a hung dependency could exhaust threads. Abandoned workers
  are now capped per policy (`max_abandoned=64`); beyond that, calls fail
  fast with `TimeoutError` and a `timeout.saturated` event.
- `rate_limit` / `adaptive_rate_limit`: a waiter cancelled while queued kept
  its reserved token, pushing every later caller back (five cancelled
  waiters delayed the next call by six seconds instead of one). Cancelled
  waiters now give their token back.
- `hedge` raced duplicates even for permanent errors. New `on=` filter: a
  non-matching exception cancels the other attempts and propagates at once.
- Events did not say which function they were about. `Event` now has a
  `function` field (qualified name of the guarded function).

Streams:

- `retry` re-opens a stream that fails before its first item; after items
  were delivered a failure propagates (no replay).
- `timeout` bounds each item of an async stream, with `first_item=` for the
  first one (time to first token); the consumer's own time between items
  is never counted.
- `circuit_breaker` records one outcome per stream; `bulkhead` holds its
  slot for the whole stream; the rate limiters take one token per stream.

Resilience at scale:

- `RetryBudget`: retries capped as a fraction of real traffic across calls
  (Finagle/gRPC style), shared by any number of `retry(budget=...)`
  policies. Stops retry storms from multiplying load during an outage.
- `deadline(seconds)` and `time_remaining()`: an ambient time budget for a
  whole operation. `timeout` clamps to it, `retry` stops before sleeping
  past it, rate limiters and bulkheads reject instead of queueing past it.
  Also usable as a decorator or `compose` member.
- `keyed(factory, key=...)`: one policy instance per host, tenant or model,
  created on first use and LRU-bounded.
- `circuit_breaker(slow_call_duration=..., slow_call_threshold=...)`: slow
  calls can trip the breaker too; a slow half-open probe re-opens it.
- `CircuitBreaker.force_open()` and `.disable()` manual states.

Observability and ergonomics:

- Live metrics: `CircuitBreaker.metrics` (`BreakerMetrics`),
  `bulkhead.in_flight`, `rate_limit.available_tokens` (also adaptive),
  `timeout.abandoned`, `cache.size`, `RetryBudget.available`.
- `guard()` context managers for blocks of code: `with breaker.guard():`,
  `with bulkhead.guard():`, `with limiter.guard():` (sync and async), and
  `async with timeout(...).guard():`.
- `name=` on every policy, carried by its events; `retry.gave_up` events
  carry a `reason` (attempts, giveup, backoff, max_time, deadline, budget).
- `events.log_to(logger)`: one log line per event, with structured `extra`
  fields.
- `nopanic.testing`: `instant()` skips and records every wait, and
  `capture_events()` collects events, so test suites never sleep.
- Bulkhead documentation now states that sync callers and each event loop
  get separate capacity.

Ecosystem:

- Trio support (and therefore AnyIO on either backend): every async policy
  detects the running library per call, with no configuration and no new
  dependency. Under Trio, `hedge` races on a nursery and raises the original
  exception, never an `ExceptionGroup`.
- `nopanic.extend`: the public building blocks for writing your own
  policies (`Policy`, `emit`, `sleep`/`asleep`, `time_remaining`,
  validation helpers). The add-on packages are built only on it.
- New package `nopanic-otel` 0.1.0: OpenTelemetry counters, histograms and
  span events for every event, plus observable gauges for live state.
- New package `nopanic-redis` 0.1.0: circuit breakers and rate limits
  shared across processes and hosts through Redis.
- Documentation site (MkDocs) with a page per policy, concepts, and
  integration guides for httpx/requests, FastAPI and the Anthropic and
  OpenAI SDKs; published to GitHub Pages from CI.

Other new:

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
optional. Sync `timeout` now fails fast once 64 abandoned workers are still
running (`max_abandoned=None` restores the old unbounded behavior).
`CircuitBreaker.state` can also be `"forced_open"` or `"disabled"`.

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
