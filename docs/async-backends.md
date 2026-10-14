# asyncio, Trio and AnyIO

Every async policy works on **asyncio** and **Trio**, and therefore on **AnyIO** with either backend. There is nothing to configure and no extra dependency: nopanic detects the running library on each call.

```python
import trio
from nopanic import retry, timeout

@retry(attempts=3, on=ConnectionError)
@timeout(5.0)
async def fetch():
    ...

trio.run(fetch)
```

| Concern | asyncio | Trio |
| --- | --- | --- |
| Waiting (retry backoff, rate limits) | `asyncio.sleep` | `trio.sleep` |
| `timeout` | `asyncio.timeout` (3.11+), `wait_for` (3.10) | cancel scope |
| `bulkhead` | one `asyncio.Semaphore` per loop | one `trio.Semaphore` per `trio.run` |
| `cache` single-flight | shared future per loop | `trio.Event` per run |
| `hedge` | tasks, losers cancelled and reaped | nursery; you get the original exception, never an `ExceptionGroup` |
| Cancellation | `CancelledError` | `trio.Cancelled` |

Cancellation is never treated as a failure: a cancelled breaker probe releases its slot, a cancelled rate-limit waiter gives its token back, a cancelled cache leader hands over to its followers.

Trio is detected only if your application imported it; nopanic never imports it on its own.
