# keyed

One policy instance per key: a breaker per host, a rate limit per tenant, a bulkhead per model.

```python
from urllib.parse import urlsplit
from nopanic import circuit_breaker, keyed

per_host = keyed(
    lambda host: circuit_breaker(name=f"api-{host}", reset_timeout=10.0),
    key=lambda url, **params: urlsplit(url).hostname,
)

@per_host
async def fetch(url, **params): ...
```

One region going down no longer trips the breaker for every region; one noisy tenant no longer starves the others.

- `key(*args, **kwargs)` computes the key from each call's arguments.
- `factory(key)` creates the instance on first use (outside any lock).
- At most `maxsize` (default 1024) instances are kept, least recently used dropped first. Dropping an instance drops its state: an evicted open breaker starts again closed.
- `per_host.get(key)` returns (creating if needed) one instance; `per_host.keys()` lists the live ones.
- Streams work when the produced policy supports them.

[`nopanic-otel`](../observability.md#opentelemetry) reports every instance of a `keyed` policy with a `nopanic.key` attribute.
