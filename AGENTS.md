# Agent guide for the nopanic repository

This file is for AI coding agents (and humans in a hurry) **working on this
repo**. If you only want to *use* the library, read [llms.txt](llms.txt)
instead: it is the compact API reference.

## What this project is

nopanic is a zero-dependency Python resilience toolkit: retry, circuit
breaker, timeout, rate limit (fixed and adaptive/AIMD), bulkhead, fallback,
hedged requests, a stale-while-failing cache, retry budgets, deadlines and
per-key policies, all as composable decorators working identically on
`def` and `async def` (and, where it makes sense, on generator functions).

Layout: one policy per module in `src/nopanic/`; shared machinery in
`_core.py` (Policy base class and stream dispatch, exception filters,
numeric validation, the ambient deadline, the sleep hook used by
`testing.instant()`); the bucketed counter window in `_window.py`;
observability in `events.py`; test helpers for users in `testing.py`;
the public toolkit for policy authors in `extend.py`; Trio primitives in
`_trio.py` (imported only when running under trio); tests mirror modules
in `tests/` (streams in `tests/test_streams.py`, Trio in `tests/test_trio.py`).

Add-on packages live in `packages/nopanic-otel` and `packages/nopanic-redis`:
separate PyPI projects with their own version, CHANGELOG and tests, built
only on the public API (`nopanic`, `nopanic.events`, `nopanic.extend`).
The documentation site is `docs/` + `mkdocs.yml` (MkDocs Material,
published to GitHub Pages by `.github/workflows/docs.yml`).

## Commands

```
pip install -e .[dev]     # setup
ruff check .              # lint (includes flake8-bandit security rules)
mypy src                  # strict typing, must stay clean
pytest -q                 # full suite, must stay under ~5s
python benchmarks/bench.py  # hot-path overhead; keep policies ~1us/call
pytest -q packages/nopanic-otel/tests packages/nopanic-redis/tests  # add-ons
mkdocs build --strict     # docs (pip install -r docs/requirements.txt)
```

CI runs all of this on 3 OSes x Python 3.10-3.14 plus `pip-audit`, a
`python -O` import check, the add-on packages (Redis against a real server)
and a strict docs build.

## Invariants (do not break these)

1. **Zero runtime dependencies.** Nothing gets added to `[project.dependencies]`.
   Third-party integrations go in a package under `packages/`. The core never
   imports trio itself; it only uses it when the application is running it.
2. **Sync/async parity.** Every policy implements `_run_sync` and
   `_run_async` with identical semantics. Documented exceptions: `hedge` is
   async-only; sync `timeout` abandons its worker thread (capped by
   `max_abandoned`); `timeout` guards async streams and async blocks only.
   Stream support is declared in `_STREAMS`; anything else must raise
   `TypeError` at decoration time, never silently pass through.
3. **No wrapper exceptions.** User exceptions re-raise unchanged. Policies
   raise only `CircuitOpen` / `BulkheadFull` / `RateLimited` (subclasses of
   `ResilienceError`) and builtin `TimeoutError`.
4. **Hooks and event listeners never break the guarded call**: catch, log to
   the module logger, continue.
5. **No `assert` in `src/`** (stripped under `python -O`).
6. **Validate numeric parameters** with `_core.positive_number` (rejects
   NaN, infinity, bool). Anything numeric coming from a *remote* source
   (e.g. Retry-After hints) must be capped before it is slept on.
7. **Injectable time.** Stateful policies take `clock=`; tests must not
   sleep for correctness (tiny real sleeps for actual concurrency are fine).
   Policies sleep only through `_core.sleep` / `_core.asleep` so that
   `testing.instant()` can skip the wait, and every wait honors the ambient
   deadline (`_core.time_remaining()`).
8. **Hot path stays cheap.** The success path of any policy must not
   allocate or lock more than it already does; run the benchmark when
   touching it. Emission with zero listeners must stay a near-no-op.
9. **Locks are never held while calling user code** (callbacks, listeners,
   the wrapped function) and exceptions are raised outside locks when a
   listener might observe state.

## When you change the public API

Update all of these in the same change, or the change is incomplete:
`llms.txt`, `README.md`, `CHANGELOG.md`, tests, and the `__all__` lists
(kept alphabetically sorted). New sdist-worthy top-level files must be added
to the `only-include` allowlist in `pyproject.toml`.

## Style

ruff (line length 100) and mypy strict are the arbiters. Docstrings explain
behaviour and trade-offs, not implementation trivia. Comments state
constraints the code cannot express. No em dashes in docs or metadata.
Commit messages: short imperative subject, no AI attribution trailers.

## Releases

Versions are bumped in `pyproject.toml` AND `src/nopanic/__init__.py`
together (for an add-on: its `pyproject.toml` and its `__version__`). Docs-only changes do not bump the version. Build with
`python -m build`, verify with `twine check dist/*` and inspect the sdist
file list for leaks before uploading. The maintainer uploads to PyPI and
tags `vX.Y.Z` on the released commit.
