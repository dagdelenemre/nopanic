# Observability

## One event stream

Every policy reports what it does as a typed `Event`: retries, breaker transitions, rejections, throttle adjustments, stale cache serves.

```python
from nopanic import events

cancel = events.subscribe(
    lambda e: metrics.increment(e.kind, tags={"name": e.name, "function": e.function})
)
```

| Field | Meaning |
| --- | --- |
| `kind` | e.g. `retry.attempt_failed`, `breaker.state_change` |
| `policy` | the emitting policy type |
| `name` | the policy's `name=` (every policy takes one), or None |
| `function` | qualified name of the guarded function, or None for blocks |
| `data` | event-specific payload |

Listeners run synchronously on the calling thread; keep them fast. A listener that raises is logged and ignored: observability never breaks the call it observes. With no listeners, emitting costs a single attribute read.

### Event kinds

| Kind | Data |
| --- | --- |
| `retry.attempt_failed` | attempt, delay, exception (and result for `on_result` retries) |
| `retry.gave_up` | attempt, exception, reason (attempts, giveup, backoff, max_time, deadline, budget) |
| `breaker.state_change` | old, new |
| `breaker.rejected` | retry_after |
| `bulkhead.rejected` | max_concurrent, max_wait |
| `ratelimit.waited` | seconds |
| `ratelimit.rejected` | would_wait, max_wait |
| `ratelimit.throttled` | old_rate, new_rate, retry_after |
| `timeout.expired` | seconds, function |
| `timeout.saturated` | abandoned, max_abandoned |
| `fallback.used` | exception |
| `hedge.launched` | delay |
| `cache.stale_served` | age, exception |
| `breaker.store_error`, `ratelimit.store_error` | exception, fail_open (nopanic-redis) |

## Logging in one line

```python
import logging
from nopanic import events

events.log_to(logging.getLogger("resilience"))   # level=logging.INFO by default
```

One line per event, with `extra` fields `nopanic_kind`, `nopanic_policy`, `nopanic_name` and `nopanic_function` for structured-logging handlers.

## Live state

For dashboards and health checks, read state directly:

| Policy | Property |
| --- | --- |
| `circuit_breaker` | `.state`, `.metrics` |
| `bulkhead` | `.in_flight` |
| `rate_limit`, `adaptive_rate_limit` | `.available_tokens`, `.current_rate` (adaptive) |
| `timeout` | `.abandoned` |
| `cache` | `.size` |
| `RetryBudget` | `.available` |
| `keyed` | `.keys()`, `.get(key)` |

```python
@app.get("/health")
def health():
    m = payments.metrics
    return {"payments": m.state, "failure_rate": m.failure_rate}
```

## OpenTelemetry

```
pip install nopanic-otel
```

```python
import nopanic_otel

nopanic_otel.instrument()                        # counters, histograms, span events
nopanic_otel.observe(payments, reports, quota)   # observable gauges
```

`instrument()` records every event as the `nopanic.events` counter (attributes `nopanic.kind`, `nopanic.policy`, `nopanic.name`, `code.function`), retry and rate-limit waits as the `nopanic.retry.delay` and `nopanic.ratelimit.wait` histograms, and adds an event to the **current span**, so each trace shows the retries, rejections and transitions that happened inside that request. `observe()` registers gauges for breaker state and rates, bulkhead in-flight calls, rate-limit tokens and adaptive rate, abandoned timeout workers, cache size and retry budget; `keyed` policies are expanded with a `nopanic.key` attribute. Both return a callable that undoes them. Only `opentelemetry-api` is required; configure the SDK as usual.
