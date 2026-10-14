# Streams

LLM tokens, file chunks, paginated rows: decorate a generator or async generator function and the policies guard the stream.

```python
from nopanic import circuit_breaker, retry, timeout

llm_breaker = circuit_breaker(name="llm", slow_call_duration=20.0)

@retry(attempts=3, on=ConnectionError)
@timeout(10.0, first_item=30.0)   # 30 s to the first token, then 10 s per token
@llm_breaker                       # one outcome per stream
async def stream_completion(prompt):
    async for token in client.stream(prompt):
        yield token

async for token in stream_completion("Hello"):
    print(token, end="")
```

## What each policy does with a stream

| Policy | Behaviour |
| --- | --- |
| `retry` | Re-opens a stream that fails **before its first item**. After items were delivered, a failure propagates: they cannot be taken back. |
| `timeout` | Async generators only. Bounds **each item**; `first_item` for the first one. The consumer's own time between items never counts. |
| `circuit_breaker` | Admits once, records one outcome when the stream ends. Slow-call tracking measures time to the first item. A consumer that stops early after receiving items counts as a success; before any item, as no outcome. |
| `bulkhead` | Holds its slot until the stream finishes. |
| `rate_limit`, `adaptive_rate_limit` | One token when the stream opens; the adaptive limiter learns from how it ends. |
| `cache`, `fallback`, `hedge`, `deadline` | Not supported: `TypeError` at decoration time. |

A policy that cannot guard a stream says so when you decorate, instead of silently doing nothing.

## Close what you abandon

As with any async generator, close a stream you stop reading early, so its policies see the end:

```python
import contextlib

async with contextlib.aclosing(stream_completion(prompt)) as tokens:
    async for token in tokens:
        if done(token):
            break
```

Without it, Python finalizes the generator later from the event loop (and `asyncio.run` may cancel that at shutdown).
