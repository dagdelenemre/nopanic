# HTTP with httpx and requests

The rules of thumb: retry transport errors, timeouts, 429 and 5xx; never retry other 4xx (they are permanent); honor `Retry-After`; always set a transport timeout too.

## Retry-After, as nopanic understands it

nopanic honors a numeric `retry_after` attribute (seconds) on the exception. Set it from the header once, in one place:

```python
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

def retry_after_seconds(value: str | None) -> float | None:
    """Parse a Retry-After header: delay-seconds or an HTTP date."""
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())
```

`retry` caps the hint by `retry_after_cap` (default 60 s), so a hostile header cannot park your client.

## httpx (async)

```python
import httpx
from nopanic import RetryBudget, adaptive_rate_limit, backoff, circuit_breaker, retry, timeout

client = httpx.AsyncClient(base_url="https://api.example.com", timeout=10.0)

def retriable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):          # connect errors, read timeouts...
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status == 429 or status >= 500
    return False

api_budget = RetryBudget(ratio=0.1)
api_breaker = circuit_breaker(name="example-api", min_calls=20, on=retriable)
api_rate = adaptive_rate_limit(50, per=1.0, min_rate=5)   # learns the real limit from 429s

@retry(attempts=4, on=retriable, backoff=backoff.full_jitter(0.2, cap=10.0), budget=api_budget)
@api_breaker
@api_rate
@timeout(15.0)
async def get_json(path: str, **params) -> dict:
    response = await client.get(path, params=params)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        exc.retry_after = retry_after_seconds(exc.response.headers.get("retry-after"))
        raise
    return response.json()
```

`on=retriable` on the breaker means a 404 is neither a failure nor a success: only the dependency's own failures count. `adaptive_rate_limit`'s default `looks_throttled` recognizes the 429 through `exc.response.status_code` and the `retry_after` attribute.

## requests (sync)

```python
import requests
from nopanic import backoff, retry

def retriable(exc: BaseException) -> bool:
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    return (
        isinstance(exc, requests.HTTPError)
        and exc.response is not None
        and (exc.response.status_code == 429 or exc.response.status_code >= 500)
    )

@retry(attempts=4, on=retriable, backoff=backoff.full_jitter(base=0.2, cap=10.0))
def get_json(url: str) -> dict:
    response = requests.get(url, timeout=10)
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        exc.retry_after = retry_after_seconds(response.headers.get("Retry-After"))
        raise
    return response.json()
```

## Responses that do not raise

If you would rather not raise on status codes, retry on the result instead:

```python
@retry(attempts=4, on=httpx.TransportError, on_result=lambda r: r.status_code in (429, 502, 503, 504))
async def get(path: str) -> httpx.Response:
    return await client.get(path)
```

## Hand the remaining budget to the client

Inside a [`deadline`](../policies/deadline.md), give each request only the time that is left:

```python
from nopanic import time_remaining

response = await client.get(path, timeout=time_remaining() or 10.0)
```
