"""Tests locking in the hardening guarantees documented in SECURITY.md."""

from __future__ import annotations

import asyncio
import logging
import math

import pytest

from nopanic import (
    CircuitBreaker,
    backoff,
    bulkhead,
    fallback,
    hedge,
    rate_limit,
    retry,
    timeout,
)

NAN = float("nan")
INF = float("inf")


@pytest.mark.parametrize(
    "build",
    [
        lambda: timeout(NAN),
        lambda: timeout(INF),
        lambda: timeout(-1),
        lambda: rate_limit(INF),
        lambda: rate_limit(5, per=NAN),
        lambda: rate_limit(5, burst=0.5),
        lambda: rate_limit(5, max_wait=-0.1),
        lambda: bulkhead(1, max_wait=NAN),
        lambda: hedge(delay=INF),
        lambda: hedge(delay=-1),
        lambda: CircuitBreaker(window=NAN),
        lambda: CircuitBreaker(window=0),
        lambda: CircuitBreaker(reset_timeout=-1),
        lambda: CircuitBreaker(failure_threshold=NAN),
        lambda: backoff.fixed(-1),
        lambda: backoff.fixed(NAN),
        lambda: backoff.exponential(base=NAN),
        lambda: backoff.exponential(factor=0),
        lambda: backoff.full_jitter(cap=-1),
        lambda: backoff.decorrelated_jitter(base=INF),
    ],
)
def test_poisonous_numeric_parameters_rejected(build):
    with pytest.raises(ValueError):
        build()


def test_boolean_parameters_rejected():
    with pytest.raises(TypeError):
        timeout(True)
    with pytest.raises(TypeError):
        rate_limit(5, max_wait=False)


def test_policies_use_slots():
    for policy in [timeout(1.0), retry(), bulkhead(1), rate_limit(5), hedge(delay=1.0),
                   fallback("x"), CircuitBreaker()]:
        with pytest.raises(AttributeError):
            policy.no_such_attribute = 1  # typo-proof: no __dict__


def test_exhausted_backoff_iterator_stops_retrying():
    class OneDelay:
        def __iter__(self):
            return iter([0.0])

    calls = []

    @retry(attempts=10, on=ValueError, backoff=OneDelay())
    def always_fails():
        calls.append(1)
        raise ValueError("original error")

    with pytest.raises(ValueError, match="original error"):
        always_fails()
    assert len(calls) == 2  # first try + the single delay the strategy allowed


def test_before_sleep_hook_error_is_logged_not_raised(caplog):
    def bad_hook(attempt):
        raise RuntimeError("hook bug")

    calls = []

    @retry(attempts=3, on=ValueError, backoff=0, before_sleep=bad_hook)
    def flaky():
        calls.append(1)
        if len(calls) < 2:
            raise ValueError("transient")
        return "ok"

    with caplog.at_level(logging.ERROR, logger="nopanic.retry"):
        assert flaky() == "ok"
    assert "before_sleep" in caplog.text


def test_state_change_hook_error_is_logged_not_raised(clock, caplog):
    def bad_hook(breaker, old, new):
        raise RuntimeError("hook bug")

    breaker = CircuitBreaker(
        failure_threshold=0.5, min_calls=2, window=30.0, reset_timeout=10.0,
        clock=clock, on_state_change=bad_hook,
    )

    @breaker
    def boom():
        raise ConnectionError("down")

    with caplog.at_level(logging.ERROR, logger="nopanic.breaker"):
        for _ in range(2):
            with pytest.raises(ConnectionError):
                boom()
    assert breaker.state == "open"  # the hook bug did not block the transition
    assert "on_state_change" in caplog.text


def test_hedge_losers_are_cancelled_and_reaped_before_return():
    calls = {"n": 0}
    loser_cancelled = []

    @hedge(delay=0.02)
    async def variable():
        calls["n"] += 1
        if calls["n"] == 1:
            try:
                await asyncio.sleep(5.0)
            except asyncio.CancelledError:
                loser_cancelled.append(True)
                raise
            return "slow"
        return "fast"

    assert asyncio.run(variable()) == "fast"
    # The finally-block reap ran before the result was returned, so the
    # cancellation has already been observed by the losing attempt.
    assert loser_cancelled == [True]


def test_retry_attempt_records_are_immutable():
    from nopanic import RetryAttempt

    record = RetryAttempt(attempt=1, exception=ValueError("x"), delay=0.5)
    with pytest.raises(AttributeError):
        record.delay = 99.0


def test_nan_failure_threshold_cannot_create_never_tripping_breaker():
    # The scenario the validation exists for: NaN comparisons are always
    # False, so an unvalidated NaN threshold would never open the breaker.
    assert not (NAN >= 0.5)  # demonstrating the trap itself
    with pytest.raises(ValueError):
        CircuitBreaker(failure_threshold=NAN)
    assert math.isnan(NAN)
