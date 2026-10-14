# Changelog

## 0.1.0 (2026-10-14)

Initial release, for nopanic 0.3.0.

- `instrument()`: every nopanic event as a `nopanic.events` counter, retry
  and rate-limit waits as histograms, and span events on the current span.
- `observe()`: observable gauges for circuit breakers, bulkheads, rate
  limiters, timeouts, caches, retry budgets and `keyed` policies.
