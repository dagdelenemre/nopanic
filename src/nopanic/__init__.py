"""nopanic — the unified resilience toolkit for Python.

Retries, circuit breakers, timeouts, rate limits, bulkheads, hedged
requests, retry budgets and deadlines behind one decorator-based API that
works identically for sync and async code, streams included. Zero runtime
dependencies.

Quick taste::

    from nopanic import retry, circuit_breaker, timeout, backoff

    api_breaker = circuit_breaker(failure_threshold=0.5, reset_timeout=15.0)

    @retry(attempts=4, on=ConnectionError, backoff=backoff.full_jitter(0.2))
    @api_breaker
    @timeout(10.0)
    async def call_upstream() -> dict: ...
"""

from __future__ import annotations

from . import backoff, events
from ._core import Policy, time_remaining
from .adaptive import adaptive_rate_limit, looks_throttled
from .breaker import BreakerMetrics, CircuitBreaker, circuit_breaker
from .budget import RetryBudget
from .bulkhead import bulkhead
from .cache import cache
from .compose import compose
from .deadline import deadline
from .events import Event, subscribe, unsubscribe
from .exceptions import BulkheadFull, CircuitOpen, RateLimited, ResilienceError
from .fallback import fallback
from .hedge import hedge
from .keyed import keyed
from .ratelimit import rate_limit
from .retry import RetryAttempt, retry
from .timeout import timeout

__version__ = "0.3.0"

__all__ = [
    "BreakerMetrics",
    "BulkheadFull",
    "CircuitBreaker",
    "CircuitOpen",
    "Event",
    "Policy",
    "RateLimited",
    "ResilienceError",
    "RetryAttempt",
    "RetryBudget",
    "__version__",
    "adaptive_rate_limit",
    "backoff",
    "bulkhead",
    "cache",
    "circuit_breaker",
    "compose",
    "deadline",
    "events",
    "fallback",
    "hedge",
    "keyed",
    "looks_throttled",
    "rate_limit",
    "retry",
    "subscribe",
    "time_remaining",
    "timeout",
    "unsubscribe",
]
