from __future__ import annotations

import pytest

from nopanic import RetryBudget, retry
from nopanic.testing import capture_events, instant


def test_budget_caps_retries_as_a_fraction_of_calls(clock):
    budget = RetryBudget(ratio=0.5, min_retries_per_sec=0, clock=clock)
    attempts = []

    @retry(attempts=5, backoff=0, budget=budget)
    def down():
        attempts.append(1)
        raise ConnectionError

    with capture_events("retry.gave_up") as seen:
        for _ in range(10):
            with pytest.raises(ConnectionError):
                down()
    # 10 first attempts earn 5 retries in total, not 10 * 4.
    assert len(attempts) == 15
    assert {e.data["reason"] for e in seen} == {"budget"}


def test_budget_floor_allows_retries_without_traffic(clock):
    budget = RetryBudget(ratio=0.0, min_retries_per_sec=0.2, window=10.0, clock=clock)
    assert budget.available == pytest.approx(2.0)
    assert budget.try_withdraw()
    assert budget.try_withdraw()
    assert not budget.try_withdraw()


def test_spent_budget_recovers_as_the_window_slides(clock):
    budget = RetryBudget(ratio=0.0, min_retries_per_sec=0.1, window=10.0, clock=clock)
    assert budget.try_withdraw()
    assert not budget.try_withdraw()
    clock.advance(11.0)
    assert budget.try_withdraw()


def test_budget_is_shared_between_retry_policies(clock):
    budget = RetryBudget(ratio=0.0, min_retries_per_sec=0.1, window=10.0, clock=clock)
    calls = {"a": 0, "b": 0}

    @retry(attempts=3, backoff=0, budget=budget)
    def a():
        calls["a"] += 1
        raise ConnectionError

    @retry(attempts=3, backoff=0, budget=budget)
    def b():
        calls["b"] += 1
        raise ConnectionError

    with instant():
        for fn in (a, b):
            with pytest.raises(ConnectionError):
                fn()
    assert calls == {"a": 2, "b": 1}  # one retry in the floor, a spent it


def test_validation():
    with pytest.raises(ValueError):
        RetryBudget(ratio=-1)
    with pytest.raises(ValueError):
        RetryBudget(window=0)
    with pytest.raises(TypeError):
        RetryBudget(min_retries_per_sec=True)
