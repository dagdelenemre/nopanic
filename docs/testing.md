# Testing

Resilience code is full of waiting. Your test suite should not be.

## Skip every wait

```python
from nopanic import testing

def test_checkout_survives_a_flaky_gateway():
    with testing.instant() as sleeps:
        assert checkout(order) == "paid"
    assert len(sleeps) == 2   # it did back off twice, instantly
```

`testing.instant()` skips every wait nopanic would sleep (retry backoff including honored `retry_after` hints, rate-limit queues; sync and async, asyncio and Trio) and records the requested seconds. Timeouts are not affected: they bound real work.

## Assert on what happened

```python
def test_breaker_opens_under_failures():
    with testing.capture_events("breaker.") as seen:
        ...
    assert [e.data["new"] for e in seen if e.kind == "breaker.state_change"] == ["open"]
```

`capture_events(prefix)` collects every event whose kind starts with `prefix` (all events by default).

Both helpers act process-wide for the duration of the block (all threads). pytest-xdist runs tests in separate processes, which is fine; do not use them from tests running in parallel threads of one process.

## Control time

Stateful policies take a `clock=` (any `() -> float` monotonic source), so state that depends on time is fully deterministic:

```python
class FakeClock:
    def __init__(self):
        self.now = 0.0
    def __call__(self):
        return self.now

clock = FakeClock()
breaker = circuit_breaker(min_calls=1, reset_timeout=30.0, clock=clock)
...
clock.now += 30.0   # the breaker is half-open now, no sleeping involved
```

`circuit_breaker`, `rate_limit`, `adaptive_rate_limit`, `cache`, `RetryBudget` and `retry` (for `max_time`) accept `clock=`. `deadline` uses the real `time.monotonic`.
