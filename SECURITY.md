# Security Policy

## Supported versions

Only the latest release receives security fixes. `nopanic` is pre-1.0;
pin your version and read the changelog before upgrading.

## Reporting a vulnerability

Please **do not open a public issue** for suspected vulnerabilities. Instead:

- use GitHub's private vulnerability reporting ("Report a vulnerability" under
  the Security tab), or
- email <dagdelen.cyber@gmail.com> with a description and reproduction.

You will get an acknowledgement within 72 hours and a fix or a coordinated
disclosure plan within 30 days.

## Threat model & hardening notes

`nopanic` is deliberately small-surface:

- **Zero runtime dependencies**: nothing to supply-chain-attack transitively;
  `pip-audit` runs in CI for the dev toolchain.
- **No I/O, no network, no filesystem access, no `eval`/`exec`, no `pickle`,
  no subprocesses.** The library only wraps callables you give it.
- **Input validation.** All numeric parameters reject NaN, infinity, booleans
  and out-of-range values at construction time, so a bad config fails loudly
  at import instead of silently disabling a breaker in production.
- **No `assert` in library code.** Correctness does not change under
  `python -O` (verified in CI).
- **Jitter uses the stdlib PRNG on purpose** (lint rule S311 suppressed):
  backoff jitter shapes load, it is not a security boundary, and a CSPRNG
  would only add cost.
- **Hooks cannot break calls.** Exceptions from observability callbacks
  (`before_sleep`, `on_state_change`) are logged to the `nopanic.*`
  loggers and suppressed.
- **Error messages leak nothing** beyond the wrapped function's qualified
  name and the policy's own configuration values.

Operational cautions (by design, documented rather than hidden):

- The **sync `timeout`** abandons its worker thread on expiry, because Python
  cannot kill threads. The abandoned call keeps running until it returns; prefer the
  async path or the callee's native timeout when that matters.
- A **waiting `rate_limit` or `bulkhead`** can queue callers without bound by
  default. If untrusted parties control your call volume, set `max_wait` so
  pressure turns into fast `RateLimited`/`BulkheadFull` failures instead of
  unbounded memory growth.
