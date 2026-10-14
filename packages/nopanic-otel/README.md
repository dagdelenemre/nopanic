# nopanic-otel

OpenTelemetry metrics, span events and live gauges for [nopanic](https://github.com/dagdelenemre/nopanic) resilience policies.

```
pip install nopanic-otel
```

```python
import nopanic_otel
from nopanic import circuit_breaker, bulkhead

payments = circuit_breaker(name="payments")
reports = bulkhead(20, name="reports")

nopanic_otel.instrument()                 # every event: counter + span event
nopanic_otel.observe(payments, reports)   # live gauges, read on each collection
```

Configure the OpenTelemetry SDK (providers and exporters) as usual; this package only needs `opentelemetry-api`.

## What you get

**`instrument()`** turns every nopanic event into:

- `nopanic.events` (counter), with attributes `nopanic.kind` (e.g. `retry.attempt_failed`, `breaker.state_change`), `nopanic.policy`, `nopanic.name` and `code.function`;
- `nopanic.retry.delay` and `nopanic.ratelimit.wait` (histograms, seconds);
- an event on the **current span** (`nopanic.retry.attempt_failed`, `nopanic.breaker.rejected`, ...) carrying the event data, so each trace shows the retries, rejections and transitions that happened inside that request.

Pass `metrics_enabled=False` or `span_events=False` to keep only one side, and `meter_provider=` to use a specific provider.

**`observe(*policies)`** registers observable gauges:

| Gauge | Source |
| --- | --- |
| `nopanic.breaker.state` | 0 closed, 1 half_open, 2 open, 3 forced_open, 4 disabled |
| `nopanic.breaker.failure_rate`, `.slow_call_rate`, `.calls` | `breaker.metrics` |
| `nopanic.bulkhead.in_flight` | `bulkhead.in_flight` |
| `nopanic.ratelimit.available_tokens`, `nopanic.ratelimit.rate` | rate limiters (rate: adaptive only) |
| `nopanic.timeout.abandoned` | sync `timeout` workers still running |
| `nopanic.cache.size` | `cache.size` |
| `nopanic.retry_budget.available` | `RetryBudget.available` |

`keyed` policies are expanded: each instance is reported with a `nopanic.key` attribute. Both functions return a callable that undoes them.

## License

MIT.
