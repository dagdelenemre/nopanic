from __future__ import annotations

import asyncio

import pytest

from nopanic import RetryAttempt, backoff, retry


def test_succeeds_after_transient_failures():
    calls = []

    @retry(attempts=3, on=ConnectionError, backoff=0)
    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError("boom")
        return "ok"

    assert flaky() == "ok"
    assert len(calls) == 3


def test_exhaustion_reraises_original_exception():
    @retry(attempts=2, on=ConnectionError, backoff=0)
    def always_fails():
        raise ConnectionError("still down")

    with pytest.raises(ConnectionError, match="still down"):
        always_fails()


def test_non_matching_exception_propagates_immediately():
    calls = []

    @retry(attempts=5, on=ConnectionError, backoff=0)
    def wrong_error():
        calls.append(1)
        raise ValueError("not retriable")

    with pytest.raises(ValueError):
        wrong_error()
    assert len(calls) == 1


def test_predicate_filter():
    calls = []

    @retry(attempts=3, on=lambda e: "retry me" in str(e), backoff=0)
    def flaky():
        calls.append(1)
        raise RuntimeError("retry me" if len(calls) < 3 else "done retrying")

    with pytest.raises(RuntimeError, match="done retrying"):
        flaky()
    assert len(calls) == 3


def test_giveup_short_circuits():
    calls = []

    @retry(attempts=5, on=ConnectionError, backoff=0, giveup=lambda e: "fatal" in str(e))
    def fatal():
        calls.append(1)
        raise ConnectionError("fatal handshake failure")

    with pytest.raises(ConnectionError):
        fatal()
    assert len(calls) == 1


def test_before_sleep_callback_receives_attempt_info():
    seen: list[RetryAttempt] = []

    @retry(attempts=3, on=ValueError, backoff=0, before_sleep=seen.append)
    def flaky():
        if len(seen) < 2:
            raise ValueError("nope")
        return "ok"

    assert flaky() == "ok"
    assert [a.attempt for a in seen] == [1, 2]
    assert all(isinstance(a.exception, ValueError) for a in seen)


def test_attempts_must_be_positive():
    with pytest.raises(ValueError):
        retry(attempts=0)


def test_async_retry():
    calls = []

    @retry(attempts=3, on=ConnectionError, backoff=0)
    async def flaky():
        calls.append(1)
        if len(calls) < 2:
            raise ConnectionError("boom")
        return "ok"

    assert asyncio.iscoroutinefunction(flaky)
    assert asyncio.run(flaky()) == "ok"
    assert len(calls) == 2


def test_wrapper_preserves_metadata():
    @retry()
    def documented():
        """docstring"""

    assert documented.__name__ == "documented"
    assert documented.__doc__ == "docstring"


def test_exponential_backoff_sequence():
    seq = iter(backoff.exponential(base=1.0, factor=2.0, cap=5.0))
    assert [next(seq) for _ in range(4)] == [1.0, 2.0, 4.0, 5.0]


def test_full_jitter_stays_in_bounds():
    seq = iter(backoff.full_jitter(base=1.0, factor=2.0, cap=4.0))
    for expected_cap in [1.0, 2.0, 4.0, 4.0, 4.0]:
        delay = next(seq)
        assert 0.0 <= delay <= expected_cap


def test_decorrelated_jitter_respects_cap():
    seq = iter(backoff.decorrelated_jitter(base=0.5, cap=2.0))
    for _ in range(20):
        assert 0.5 <= next(seq) <= 2.0
