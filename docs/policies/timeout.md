# timeout

Fail with the builtin `TimeoutError` when a call takes too long.

```python
from nopanic import timeout

@timeout(2.0)
async def lookup(key): ...   # cancelled cleanly on expiry
```

## Async

The call is cancelled through the event loop (asyncio or Trio). Only an expiry of *this* timeout raises a new `TimeoutError`: a `TimeoutError` raised by your own code (a database driver's, say) propagates unchanged.

## Sync

A sync function runs on a worker thread and is **abandoned, not killed** on expiry: Python cannot stop a running thread, so the work may continue in the background. The worker runs in a copy of the caller's `contextvars`, so tracing spans and request IDs are visible inside.

Abandoned workers are capped per policy by `max_abandoned` (default 64). When that many timed-out calls are still running, new calls fail fast with `TimeoutError` (event `timeout.saturated`) instead of starting threads: a hung dependency cannot exhaust your threads. `policy.abandoned` reports the current count; `max_abandoned=None` removes the cap.

Prefer the callee's own timeout parameter (an HTTP client's `timeout=`) when abandonment is not acceptable.

## Streams

On an async generator function, the limit applies to **each item**, so a stalled stream fails even if it started fine. `first_item` sets a separate limit for the first one (an LLM's time to first token):

```python
@timeout(10.0, first_item=30.0)
async def tokens(prompt):
    async for token in client.stream(prompt):
        yield token
```

The consumer's own time between items is never counted. Sync generators are not supported (they would need a thread per item).

## Blocks

```python
async with timeout(5.0).guard():
    await step_one()
    await step_two()
```

A sync `with` raises `TypeError`: a synchronous block cannot be interrupted.

## Deadlines

Inside an active [`deadline`](deadline.md), the limit is clamped to the time remaining, and a call made after the deadline fails at once without running.

## Events

`timeout.expired` (seconds, function), `timeout.saturated` (abandoned, max_abandoned).
