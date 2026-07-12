# Copilot instructions for nopanic

Read AGENTS.md at the repository root: it is the source of truth for
commands, invariants, style and the release process. The compact API
reference for using the library is llms.txt.

Key rules: zero runtime dependencies, identical sync/async semantics for
every policy, no wrapper exceptions around user errors, no `assert` in
`src/`, numeric inputs validated (and remote-sourced values capped), strict
mypy and ruff must stay clean, public API changes must update llms.txt,
README.md and CHANGELOG.md in the same change.
