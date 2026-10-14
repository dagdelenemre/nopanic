# FastAPI

Three things make a FastAPI service resilient with nopanic: policies created once at import time, a deadline per request, and nopanic's signals mapped to the right HTTP answers.

## Policies live at module level

```python
# resilience.py
from nopanic import RetryBudget, bulkhead, circuit_breaker, rate_limit

payments_breaker = circuit_breaker(name="payments", min_calls=20, reset_timeout=15.0)
payments_budget = RetryBudget(ratio=0.1)
search_capacity = bulkhead(50, max_wait=0.5, name="search")
partner_quota = rate_limit(100, per=1.0, max_wait=2.0, name="partner")
```

They are shared by every request on purpose: one breaker per dependency, one capacity per resource. Create them once, never per request.

## A deadline per request

```python
from fastapi import FastAPI, Request
from nopanic import deadline

app = FastAPI()

@app.middleware("http")
async def request_deadline(request: Request, call_next):
    async with deadline(10.0):
        return await call_next(request)
```

Every retry, timeout and queue inside the request now shares the same 10 seconds, and `time_remaining()` tells your own clients how much is left.

## Map signals to HTTP

```python
import math

from fastapi.responses import JSONResponse
from nopanic import BulkheadFull, CircuitOpen, RateLimited

def _retry_after(seconds: float | None) -> dict[str, str]:
    return {} if seconds is None else {"Retry-After": str(max(1, math.ceil(seconds)))}

@app.exception_handler(CircuitOpen)
async def circuit_open(request: Request, exc: CircuitOpen):
    return JSONResponse({"detail": f"{exc.name} is unavailable"}, status_code=503,
                        headers=_retry_after(exc.retry_after))

@app.exception_handler(BulkheadFull)
async def bulkhead_full(request: Request, exc: BulkheadFull):
    return JSONResponse({"detail": "too busy, try again shortly"}, status_code=503,
                        headers=_retry_after(1.0))

@app.exception_handler(RateLimited)
async def rate_limited(request: Request, exc: RateLimited):
    return JSONResponse({"detail": "rate limited"}, status_code=429,
                        headers=_retry_after(exc.retry_after))

@app.exception_handler(TimeoutError)
async def timed_out(request: Request, exc: TimeoutError):
    return JSONResponse({"detail": "upstream timed out"}, status_code=504)
```

Your callers get honest, retryable answers with a `Retry-After` they can respect, instead of a 500 and a stack trace.

## Guard outbound calls and hot endpoints

```python
from nopanic import retry, timeout

@retry(attempts=3, on=(ConnectionError, TimeoutError), budget=payments_budget)
@payments_breaker
@timeout(3.0)
async def charge(order_id: str) -> dict: ...

@app.post("/orders/{order_id}/pay")
async def pay(order_id: str):
    return await charge(order_id)

@app.get("/search")
async def search(q: str):
    async with search_capacity.guard():   # shed load instead of queueing forever
        return await run_search(q)
```

## Health and metrics

```python
@app.get("/health/dependencies")
async def dependencies():
    m = payments_breaker.metrics
    return {
        "payments": {"state": m.state, "failure_rate": m.failure_rate},
        "search_in_flight": search_capacity.in_flight,
    }
```

For dashboards, `nopanic_otel.observe(payments_breaker, search_capacity, partner_quota)` exports the same state as OpenTelemetry gauges, and `nopanic_otel.instrument()` puts every retry and rejection on the request's trace. See [Observability](../observability.md).

With several workers, a per-process breaker trips per process. To make the whole deployment trip together, use [`redis_circuit_breaker`](../distributed.md).
