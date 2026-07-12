# nopanic

Read [AGENTS.md](AGENTS.md) first: it is the source of truth for working in
this repository (commands, invariants, style, release process). The compact
API reference for *using* the library is [llms.txt](llms.txt).

Non-negotiables: zero runtime dependencies, sync/async parity, no wrapper
exceptions, no `assert` in `src/`, strict mypy and ruff must stay clean,
and `pytest -q` must pass in seconds, not minutes.
