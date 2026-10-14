# cache

Cache results by arguments, and serve the last good value when a refresh fails.

```python
from nopanic import cache

@cache(ttl=60.0, stale_ttl=3600.0, on=(ConnectionError, TimeoutError))
def exchange_rates(base): ...
```

- Values younger than `ttl` are served without calling.
- When a refresh **fails** with an exception matching `on`, an expired value is served for up to `stale_ttl` more seconds instead of the error (event `cache.stale_served`). `stale_ttl=0` makes it a plain TTL cache.
- **Single-flight:** concurrent misses of one key make one call; every caller gets its outcome (value, stale value or the same exception). A cancelled leader hands over to its followers. `single_flight=False` turns it off.
- LRU-bounded by `maxsize` (default 1024); `cache.size`, `cache.clear()`.
- Keys are the arguments, which must be hashable; pass `key=lambda *args, **kwargs: ...` otherwise. One instance may decorate several functions; entries are kept per function.
- Streams cannot be cached (`TypeError` at decoration time).

## Events

`cache.stale_served` (age, exception).
