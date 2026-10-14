"""Helpers for testing code that uses nopanic.

Resilience code is full of waiting: backoff between retries, rate-limit
queues. Your test suite should not be::

    from nopanic import testing

    def test_checkout_survives_a_flaky_gateway():
        with testing.instant() as sleeps:
            assert checkout(order) == "paid"
        assert len(sleeps) == 2          # it did back off twice, instantly

    def test_breaker_reports_opening():
        with testing.capture_events("breaker.") as seen:
            ...
        assert [e.data["new"] for e in seen] == ["open"]

Both helpers act process-wide for the duration of the ``with`` block (all
threads), so do not run tests that use them in parallel threads of one
process (pytest-xdist uses processes and is fine).
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from . import _core
from .events import Event, subscribe

__all__ = ["capture_events", "instant"]


@contextmanager
def instant() -> Iterator[list[float]]:
    """Skip every wait nopanic would sleep, recording the requested seconds.

    Covers retry backoff (including honored ``retry_after`` hints) and
    rate-limit waits, sync and async (async waits still yield to the event
    loop once). Timeouts are not affected: they bound real work.
    """
    previous = _core._recorded_sleeps
    sleeps: list[float] = []
    _core._recorded_sleeps = sleeps
    try:
        yield sleeps
    finally:
        _core._recorded_sleeps = previous


@contextmanager
def capture_events(prefix: str = "") -> Iterator[list[Event]]:
    """Collect every event whose kind starts with *prefix* (default: all)."""
    seen: list[Event] = []
    cancel = subscribe(lambda event: seen.append(event) if event.kind.startswith(prefix) else None)
    try:
        yield seen
    finally:
        cancel()
