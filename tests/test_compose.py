from __future__ import annotations

import asyncio

import pytest

from nopanic import CircuitOpen, compose, fallback, retry, timeout


def test_compose_applies_first_policy_outermost():
    calls = []

    stack = compose(
        retry(attempts=3, on=TimeoutError, backoff=0),
        timeout(0.05),
    )

    @stack
    async def flaky():
        calls.append(1)
        if len(calls) < 2:
            await asyncio.sleep(1.0)  # first attempt times out
        return "ok"

    assert asyncio.run(flaky()) == "ok"
    assert len(calls) == 2  # retry saw the timeout and re-ran the call


def test_full_production_stack_degrades_gracefully():
    from nopanic import CircuitBreaker

    breaker = CircuitBreaker(failure_threshold=0.5, min_calls=2, reset_timeout=60.0)

    stack = compose(
        fallback("cached-answer"),
        # honor_retry_after=False: otherwise the retry would (correctly!)
        # sleep out the breaker's 60s open window instead of failing fast.
        retry(attempts=2, on=(ConnectionError, CircuitOpen), backoff=0, honor_retry_after=False),
        breaker,
    )

    @stack
    def upstream():
        raise ConnectionError("hard down")

    # Breaker opens during the retries, fallback absorbs everything.
    assert upstream() == "cached-answer"
    assert breaker.state == "open"
    # Subsequent calls short-circuit through the open breaker into the fallback.
    assert upstream() == "cached-answer"


def test_plain_decorator_stacking_is_equivalent():
    calls = []

    @retry(attempts=2, on=ValueError, backoff=0)
    @fallback("unused", on=KeyError)
    def flaky():
        calls.append(1)
        if len(calls) < 2:
            raise ValueError("retry me")
        return "ok"

    assert flaky() == "ok"


def test_compose_with_no_policies_is_identity():
    @compose()
    def f():
        return 7

    assert f() == 7


def test_composed_wrapper_keeps_name():
    @compose(retry(), timeout(1.0))
    def named():
        return 1

    assert named.__name__ == "named"


def test_retry_does_not_catch_circuit_open_unless_asked():
    from nopanic import CircuitBreaker

    breaker = CircuitBreaker(failure_threshold=1.0, min_calls=1, reset_timeout=60.0)

    @retry(attempts=5, on=ConnectionError, backoff=0)
    @breaker
    def upstream():
        raise ConnectionError("down")

    # First failure opens the breaker; the retry's second attempt hits
    # CircuitOpen, which is not in `on`, so it propagates immediately.
    with pytest.raises(CircuitOpen):
        upstream()
