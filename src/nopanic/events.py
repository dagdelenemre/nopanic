"""Lightweight observability events emitted by every policy.

Subscribe once, see everything your resilience layer does: retries, breaker
transitions, rejected calls, throttle adjustments, stale cache serves.

    from nopanic import events

    cancel = events.subscribe(lambda e: log.info("%s %s %s", e.kind, e.name, e.data))
    ...
    cancel()  # stop listening

Or, for plain logging, just ``events.log_to(logging.getLogger("resilience"))``.

Listeners are process-global and called synchronously on the calling thread;
keep them fast (hand off to a queue for heavy work). A listener that raises
is logged and ignored: observability is never allowed to break the call it
observes. When there are no listeners the emission cost is a single
attribute read, so the hot path stays effectively free.

Every event carries the emitting policy type, the policy's ``name`` (when
one was given) and the qualified name of the guarded ``function``. Event
kinds currently emitted:

- ``retry.attempt_failed``  (attempt, delay, exception[, result])
- ``retry.gave_up``         (attempt, exception, reason[, result]); reason is
  one of attempts, giveup, backoff, max_time, deadline, budget
- ``breaker.state_change``  (old, new)
- ``breaker.rejected``      (retry_after)
- ``bulkhead.rejected``     (max_concurrent, max_wait)
- ``ratelimit.waited``      (seconds)
- ``ratelimit.rejected``    (would_wait, max_wait)
- ``ratelimit.throttled``   (old_rate, new_rate, retry_after)  [adaptive]
- ``timeout.expired``       (seconds, function)
- ``timeout.saturated``     (abandoned, max_abandoned)  [sync]
- ``fallback.used``         (exception)
- ``hedge.launched``        (delay)
- ``cache.stale_served``    (age, exception)
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

__all__ = ["Event", "log_to", "subscribe", "unsubscribe"]

_log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Event:
    """One observable occurrence inside a policy.

    Attributes:
        kind: Dotted event type, e.g. ``"retry.attempt_failed"``.
        policy: Policy type that emitted it, e.g. ``"retry"``.
        name: The policy instance's ``name``, or None when it has none.
        data: Event-specific payload; see the module docstring.
        function: Qualified name of the guarded function, when known.
    """

    kind: str
    policy: str
    name: str | None
    data: Mapping[str, Any]
    function: str | None = None


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


def log_to(logger: logging.Logger | None = None, level: int = logging.INFO) -> Callable[[], None]:
    """Log every event to *logger* (default ``nopanic``); returns an unsubscribe callable.

    One line per event, lazily formatted, with the event fields as
    ``extra`` (``nopanic_kind``, ``nopanic_policy``, ``nopanic_name``,
    ``nopanic_function``) for structured-logging handlers.
    """
    target = logger if logger is not None else logging.getLogger("nopanic")

    def listener(event: Event) -> None:
        if not target.isEnabledFor(level):
            return
        target.log(
            level,
            "nopanic %s name=%s function=%s %s",
            event.kind,
            event.name,
            event.function,
            dict(event.data),
            extra={
                "nopanic_kind": event.kind,
                "nopanic_policy": event.policy,
                "nopanic_name": event.name,
                "nopanic_function": event.function,
            },
        )

    return subscribe(listener)


def emit(
    kind: str,
    policy: str,
    name: str | None = None,
    fn: Callable[..., Any] | None = None,
    /,
    **data: Any,
) -> None:
    """Deliver an event to all listeners; used internally by policies."""
    listeners = _listeners
    if not listeners:
        return
    function = None if fn is None else (getattr(fn, "__qualname__", None) or repr(fn))
    event = Event(kind, policy, name, data, function)
    for listener in listeners:
        try:
            listener(event)
        except Exception:
            _log.exception("event listener raised; ignoring so the call proceeds")
