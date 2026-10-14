# bulkhead

Cap concurrent calls so one slow dependency cannot take every thread, connection or task you have.

```python
from nopanic import bulkhead

reports = bulkhead(20, max_wait=0, name="reports")   # at most 20 in flight, reject the 21st

@reports
async def render_report(report_id): ...
```

- `max_wait=None` (default) waits for a slot indefinitely; `0` fails fast; `N` waits up to N seconds. On expiry: `BulkheadFull`.
- Inside an active [`deadline`](deadline.md), a call never waits past it.
- `reports.in_flight` reports slots held right now.
- `with reports.guard():` / `async with reports.guard():` holds a slot for a block; a stream holds its slot until it finishes.

!!! note "Capacity per flavour"
    Sync callers share one thread semaphore; async callers share one semaphore per event loop (or Trio run). These capacities are **separate**: a bulkhead used from sync code and from one event loop admits up to twice `max_concurrent` in total. Use one instance per flavour if that matters.

Under untrusted load, always set `max_wait`: backpressure should become fast failure, not an unbounded queue.

## Events

`bulkhead.rejected` (max_concurrent, max_wait).
