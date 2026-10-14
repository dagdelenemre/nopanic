# circuit_breaker

Stop calling a dependency that is failing, give it time to recover, then probe carefully.

```python
from nopanic import CircuitOpen, circuit_breaker

payments = circuit_breaker(
    failure_threshold=0.5,   # open at >= 50% failures...
    min_calls=10,            # ...once 10 outcomes are in the window
    window=30.0,             # sliding window, seconds
    reset_timeout=15.0,      # stay open this long, then probe
    name="payments",
)

@payments
async def charge(order): ...
```

`CircuitBreaker` is the class; `circuit_breaker` is an alias that reads like the other policies.

## States

| State | Behaviour |
| --- | --- |
| `closed` | Calls pass; outcomes are recorded in the window. Opens when the window holds at least `min_calls` outcomes and the failure rate (or slow-call rate) reaches its threshold. |
| `open` | Calls fail instantly with `CircuitOpen(name, retry_after)`. After `reset_timeout`, moves to half-open. |
| `half_open` | Up to `half_open_max_calls` probes pass. All succeed: closed (window cleared). Any failure: open again. |
| `forced_open` | Set by `force_open()`: every call rejected until `reset()`. |
| `disabled` | Set by `disable()`: every call passes, nothing recorded, until `reset()`. |

Exceptions not selected by `on` are neither failures nor successes. A cancelled call (a hedge loser, an outer timeout, a client that hung up) releases any probe slot it held without recording an outcome.

## Slow calls

A dependency that answers in eight seconds is often as harmful as one that fails:

```python
search = circuit_breaker(slow_call_duration=2.0, slow_call_threshold=0.5, min_calls=20)
```

Calls taking at least `slow_call_duration` seconds count as slow (for streams: time to the first item). The breaker opens when the slow-call rate reaches `slow_call_threshold`, and a slow half-open probe re-opens it. Slow-call tracking costs nothing when it is off.

## The window

The window is `window_buckets` (default 10) time buckets of counters: memory is constant no matter the traffic, and both recording and judging are O(1). Outcomes expire in bucket-sized steps (`window / window_buckets` seconds).

## Inspecting and controlling

```python
payments.state        # "closed" | "open" | "half_open" | "forced_open" | "disabled"
payments.metrics      # BreakerMetrics(state, calls, failures, slow_calls, failure_rate, slow_call_rate)
payments.reset()
payments.force_open() # maintenance, kill switch
payments.disable()
```

`on_state_change=lambda breaker, old, new: ...` runs after every transition, outside the lock; an exception in it is logged and ignored.

## Blocks, streams, many processes

- `with payments.guard():` / `async with payments.guard():` judges a block of code like a call.
- On a stream, the breaker admits once and records one outcome when the stream ends. A consumer that stops early after receiving items counts as a success.
- To share one breaker across processes or hosts, use [`nopanic-redis`](../distributed.md). For one breaker per host or tenant, use [`keyed`](keyed.md).

## Events

`breaker.state_change` (old, new), `breaker.rejected` (retry_after).
