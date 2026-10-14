"""Public building blocks for writing your own policies.

nopanic's add-on packages (``nopanic-otel``, ``nopanic-redis``) are built
only on this module, and so can yours. Everything here is covered by the
same compatibility promise as the rest of the public API.

A minimal policy::

    from nopanic.extend import Policy, emit

    class audit(Policy):
        __slots__ = ()

        def _run_sync(self, fn, args, kwargs):
            emit("audit.call", "audit", None, fn)
            return fn(*args, **kwargs)

        async def _run_async(self, fn, args, kwargs):
            emit("audit.call", "audit", None, fn)
            return await fn(*args, **kwargs)

Rules your policy should follow to behave like the built-in ones: re-raise
user exceptions unchanged, never hold a lock while calling user code, wait
only through :func:`sleep` / :func:`asleep` (so ``testing.instant()`` and
deadlines work), respect :func:`time_remaining` in every wait, and declare
stream support in ``_STREAMS`` (implementing ``_run_sync_gen`` /
``_run_async_gen``) or let decoration of generators fail loudly.
"""

from __future__ import annotations

from ._core import (
    ExcFilter,
    Policy,
    asleep,
    exc_matches,
    finite_hint,
    fn_name,
    positive_int,
    positive_number,
    running_trio,
    sleep,
    time_remaining,
)
from .events import emit

__all__ = [
    "ExcFilter",
    "Policy",
    "asleep",
    "emit",
    "exc_matches",
    "finite_hint",
    "fn_name",
    "positive_int",
    "positive_number",
    "running_trio",
    "sleep",
    "time_remaining",
]
