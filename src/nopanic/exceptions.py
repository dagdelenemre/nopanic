"""Exception hierarchy for nopanic.

Every exception raised *by a policy itself* (as opposed to re-raised user
exceptions) derives from :class:`ResilienceError`, so callers can catch the
whole family with a single ``except``.
"""

from __future__ import annotations

__all__ = ["BulkheadFull", "CircuitOpen", "RateLimited", "ResilienceError"]


class ResilienceError(Exception):
    """Base class for all errors raised by nopanic policies."""


class CircuitOpen(ResilienceError):
    """Raised when a call is rejected because the circuit breaker is open.

    Attributes:
        name: Name of the breaker that rejected the call.
        retry_after: Seconds until the breaker will transition to half-open
            and allow a probe call. ``0.0`` when the breaker is half-open but
            all probe slots are taken.
    """

    def __init__(self, name: str, retry_after: float) -> None:
        self.name = name
        self.retry_after = retry_after
        super().__init__(f"circuit {name!r} is open; retry in {retry_after:.2f}s")


class BulkheadFull(ResilienceError):
    """Raised when a bulkhead rejects a call because capacity is exhausted."""


class RateLimited(ResilienceError):
    """Raised when a rate limiter rejects a call instead of waiting.

    Only raised when ``max_wait`` is set and the required wait exceeds it;
    by default the rate limiter waits instead of failing.
    """
