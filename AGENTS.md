# Agent guide for the nopanic repository

This file is for AI coding agents (and humans in a hurry) **working on this
repo**. If you only want to *use* the library, read [llms.txt](llms.txt)
instead: it is the compact API reference.

## What this project is

nopanic is a zero-dependency Python resilience toolkit: retry, circuit
breaker, timeout, rate limit (fixed and adaptive/AIMD), bulkhead, fallback,
hedged requests and a stale-while-failing cache, all as composable
decorators working identically on `def` and `async def`.

Layout: one policy per module in `src/nopanic/`; shared machinery in
`_core.py` (Policy base class, exception filters, numeric validation);
observability in `events.py`; tests mirror modules in `tests/`.

## Commands

```
pip install -e .[dev]     # setup
ruff check .              # lint (includes flake8-bandit security rules)
mypy src                  # strict typing, must stay clean
pytest -q                 # full suite, must stay under ~5s
python benchmarks/bench.py  # hot-path overhead; keep policies ~1us/call
```

CI runs all of this on 3 OSes x Python 3.10-3.14 plus `pip-audit` and a
`python -O` import check.

## Invariants (do not break these)

1. **Zero runtime dependencies.** Nothing gets added to `[project.dependencies]`.
2. **Sync/async parity.** Every policy implements `_run_sync` and
   `_run_async` with identical semantics. Documented exceptions: `hedge` is
   async-only; sync `timeout` abandons its worker thread.
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
together. Docs-only changes do not bump the version. Build with
`python -m build`, verify with `twine check dist/*` and inspect the sdist
file list for leaks before uploading. The maintainer uploads to PyPI and
tags `vX.Y.Z` on the released commit.
