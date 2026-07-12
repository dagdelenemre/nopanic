"""Lightweight observability events emitted by every policy.

Subscribe once, see everything your resilience layer does: retries, breaker
transitions, rejected calls, throttle adjustments, stale cache serves.

    from nopanic import events

    cancel = events.subscribe(lambda e: log.info("%s %s %s", e.kind, e.name, e.data))
    ...
    cancel()  # stop listening

Listeners are process-global and called synchronously on the calling thread;
keep them fast (hand off to a queue for heavy work). A listener that raises
is logged and ignored: observability is never allowed to break the call it
observes. When there are no listeners the emission cost is a single
attribute read, so the hot path stays effectively free.

Event kinds currently emitted:

- ``retry.attempt_failed``  (attempt, delay, exception)
- ``retry.gave_up``         (attempt, exception)
- ``breaker.state_change``  (old, new)
- ``breaker.rejected``      (retry_after)
- ``bulkhead.rejected``     (max_concurrent, max_wait)
- ``ratelimit.waited``      (seconds)
- ``ratelimit.rejected``    (would_wait, max_wait)
- ``ratelimit.throttled``   (old_rate, new_rate, retry_after)  [adaptive]
- ``timeout.expired``       (seconds, function)
- ``fallback.used``         (exception)
- ``hedge.launched``        (delay)
- ``cache.stale_served``    (age, exception)

OpenTelemetry, StatsD or plain logging integrations are one listener away;
see the README for a recipe.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

__all__ = ["Event", "subscribe", "unsubscribe"]

_log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Event:
    """One observable occurrence inside a policy.

    Attributes:
        kind: Dotted event type, e.g. ``"retry.attempt_failed"``.
        policy: Policy type that emitted it, e.g. ``"retry"``.
        name: Instance name for policies that have one (breaker, bulkhead),
            otherwise None.
        data: Event-specific payload; see the module docstring.
    """

    kind: str
    policy: str
    name: str | None
    data: Mapping[str, Any]


Listener = Callable[[Event], None]

_lock = threading.Lock()
# Copy-on-write tuple: emit() reads it without taking the lock.
_listeners: tuple[Listener, ...] = ()


def subscribe(listener: Listener) -> Callable[[], None]:
    """Register a listener for all policy events; returns an unsubscribe callable."""
    global _listeners
    with _lock:
        _listeners = (*_listeners, listener)

    def cancel() -> None:
        unsubscribe(listener)

    return cancel


def unsubscribe(listener: Listener) -> None:
    """Remove a previously registered listener (no-op if absent)."""
    global _listeners
    with _lock:
        _listeners = tuple(one for one in _listeners if one is not listener)


def emit(kind: str, policy: str, name: str | None = None, **data: Any) -> None:
    """Deliver an event to all listeners; used internally by policies."""
    listeners = _listeners
    if not listeners:
        return
    event = Event(kind, policy, name, data)
    for listener in listeners:
        try:
            listener(event)
        except Exception:
            _log.exception("event listener raised; ignoring so the call proceeds")
