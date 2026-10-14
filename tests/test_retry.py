from __future__ import annotations

import asyncio
import inspect
import time

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

    assert inspect.iscoroutinefunction(flaky)
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


def test_max_time_stops_before_a_wait_that_would_overrun(clock, monkeypatch):
    calls = []
    sleeps = []
    monkeypatch.setattr(time, "sleep", lambda s: (sleeps.append(s), clock.advance(s)))

    @retry(attempts=10, backoff=5.0, max_time=12.0, clock=clock)
    def flaky():
        calls.append(clock.now)
        clock.advance(1.0)  # each attempt takes 1s of (fake) time
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        flaky()
    # Attempts start at t=0, 6 and 12 (each fails after 1s, then waits 5s).
    # After the third, a wait would end at t=18 > 12: give up, do not sleep.
    assert [t - 1000.0 for t in calls] == [0.0, 6.0, 12.0]
    assert sleeps == [5.0, 5.0]


def test_max_time_never_sleeps_past_the_budget():
    calls = []

    @retry(attempts=5, backoff=30.0, max_time=1.0)
    def down():
        calls.append(1)
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        down()  # returns immediately: the first 30s wait already overruns
    assert calls == [1]


def test_on_result_retries_rejected_values():
    responses = iter([503, 503, 200])
    attempts = []

    @retry(attempts=5, backoff=0, on_result=lambda status: status >= 500,
           before_sleep=attempts.append)
    def fetch():
        return next(responses)

    assert fetch() == 200
    assert [(a.attempt, a.exception, a.result) for a in attempts] == [
        (1, None, 503),
        (2, None, 503),
    ]


def test_on_result_returns_last_result_when_exhausted():
    calls = []

    @retry(attempts=3, backoff=0, on_result=lambda r: r is None)
    def lookup():
        calls.append(1)
        return None

    assert lookup() is None  # no wrapper exception, the value as-is
    assert len(calls) == 3


def test_on_result_async():
    responses = iter(["busy", "done"])

    @retry(attempts=3, backoff=0, on_result=lambda r: r == "busy")
    async def poll():
        return next(responses)

    assert asyncio.run(poll()) == "done"


def test_policy_call_and_acall_run_without_decorating():
    class Client:
        def __init__(self):
            self.calls = 0

        def get(self, path, *, ok_after=2):
            self.calls += 1
            if self.calls < ok_after:
                raise ConnectionError("flaky")
            return f"GET {path}"

        async def aget(self, path):
            return self.get(path)

    policy = retry(attempts=3, backoff=0)
    client = Client()
    assert policy.call(client.get, "/a", ok_after=2) == "GET /a"
    assert client.calls == 2

    client = Client()
    assert asyncio.run(policy.acall(client.aget, "/b")) == "GET /b"
    # A lambda returning a coroutine works too: no `async def` needed.
    client = Client()
    assert asyncio.run(policy.acall(lambda: client.aget("/c"))) == "GET /c"
