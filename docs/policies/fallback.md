# fallback

Return a substitute instead of failing.

```python
from nopanic import CircuitOpen, fallback

@fallback(lambda exc: [], on=(ConnectionError, TimeoutError, CircuitOpen))
async def recommendations(user_id): ...

@fallback("temporarily unavailable", on=ConnectionError)   # a plain value works too
def status_banner(): ...
```

- A callable `handler` receives the exception and returns the substitute; on an async function, an async handler is awaited.
- Anything not selected by `on` propagates.
- Place it **outermost** in a stack: it should only see what every other layer could not absorb.
- Streams are not supported (`TypeError` at decoration time).

## Events

`fallback.used` (exception).
