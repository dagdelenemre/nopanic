# Contributing

Thanks for helping keep Python calm when everything else is on fire.

## Setup

```
pip install -e .[dev]
```

## Before you open a PR

All of these must pass (CI runs them on 3 OSes x Python 3.10-3.14):

```
ruff check .
mypy src          # strict mode
pytest -q         # includes the Trio tests (trio is a dev dependency)
```

The add-on packages under `packages/` (`nopanic-otel`, `nopanic-redis`) have
their own tests and dependencies:

```
pip install -e "packages/nopanic-otel[dev]" -e "packages/nopanic-redis[dev]"
pytest -q packages/nopanic-otel/tests packages/nopanic-redis/tests
```

The Redis tests use fakeredis; set `NOPANIC_REDIS_URL=redis://localhost:6379/0`
to also run them against a real server. The documentation site builds with
`pip install -r docs/requirements.txt` and `mkdocs serve`.

## Ground rules

- **Zero runtime dependencies.** This is non-negotiable; a resilience library
  must not itself be a reliability or supply-chain risk. Dev-only tools are fine.
  Integrations that need a third-party library (OpenTelemetry, Redis) live in
  their own packages under `packages/`, built only on `nopanic.extend`.
- **Sync and async parity.** Every policy behaves identically for `def` and
  `async def` (documented exceptions: `hedge` is async-only; sync `timeout`
  abandons its thread).
- **No wrapper exceptions.** User exceptions are re-raised unchanged; policies
  add only their own precise signals (`CircuitOpen`, `BulkheadFull`,
  `RateLimited`).
- **No `assert` in `src/`**: it disappears under `python -O`.
- **Testable time.** Anything time-dependent takes an injectable `clock`;
  tests must not `sleep()` for correctness (tiny sleeps for real concurrency
  are acceptable).
- New behaviour needs tests; changed behaviour needs a CHANGELOG entry.
- If the public API changes, update `llms.txt` and the pages under `docs/`
  in the same PR so users and AI coding agents see an accurate surface.

## Reporting security issues

See [SECURITY.md](SECURITY.md), never public issues.
