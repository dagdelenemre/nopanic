# hedge

Fix the p99, not the median: if a call is slow, race a duplicate and take whichever finishes first ("The Tail at Scale", Dean and Barroso, 2013).

```python
from nopanic import hedge

@hedge(delay=0.8, max_hedges=1, on=(ConnectionError, TimeoutError))
async def complete(prompt): ...
```

- After `delay` seconds without a result, another attempt starts, up to `max_hedges` extra attempts.
- The first success wins; the other attempts are cancelled **and awaited**, so nothing keeps running behind your back.
- A failure matching `on` lets the others continue and starts the next hedge at once. Any other exception is permanent: the rest are cancelled and it propagates immediately (a 400 is not repeated).
- If every attempt fails, the last failure is raised.
- Async only (asyncio and Trio), and only for **idempotent** operations: hedging duplicates work by design.

## Events

`hedge.launched` (delay).
