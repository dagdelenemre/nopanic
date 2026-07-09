"""nopanic — the unified resilience toolkit for Python.

Retries, circuit breakers, timeouts, rate limits, bulkheads and hedged
requests behind one decorator-based API that works identically for sync and
async code. Zero runtime dependencies.

Quick taste::

    from nopanic import retry, circuit_breaker, timeout, backoff

    api_breaker = circuit_breaker(failure_threshold=0.5, reset_timeout=15.0)

    @retry(attempts=4, on=ConnectionError, backoff=backoff.full_jitter(0.2))
    @api_breaker
    @timeout(10.0)
    async def call_upstream() -> dict: ...
"""

from __future__ import annotations

from . import backoff
from ._core import Policy
from .breaker import CircuitBreaker, circuit_breaker
from .bulkhead import bulkhead
from .compose import compose
from .exceptions import BulkheadFull, CircuitOpen, RateLimited, ResilienceError
from .fallback import fallback
from .hedge import hedge
from .ratelimit import rate_limit
from .retry import RetryAttempt, retry
from .timeout import timeout

__version__ = "0.1.0"

__all__ = [
    "BulkheadFull",
    "CircuitBreaker",
    "CircuitOpen",
    "Policy",
    "RateLimited",
    "ResilienceError",
    "RetryAttempt",
    "__version__",
    "backoff",
    "bulkhead",
    "circuit_breaker",
    "compose",
    "fallback",
    "hedge",
    "rate_limit",
    "retry",
    "timeout",
]
