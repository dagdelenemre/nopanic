# nopanic

**The unified resilience toolkit for Python.**

Retries, circuit breakers, timeouts, rate limits, bulkheads, hedged requests, retry budgets and deadlines: one decorator-based API, identical for sync and async code (LLM token streams included), with **zero runtime dependencies**.

```python
from nopanic import retry, circuit_breaker, timeout, backoff

openai_breaker = circuit_breaker(failure_threshold=0.5, reset_timeout=15.0, name="openai")

@retry(attempts=4, on=ConnectionError, backoff=backoff.full_jitter(base=0.2, cap=10.0))
@openai_breaker
@timeout(30.0)
async def call_llm(prompt: str) -> str:
    ...
```

## Why

Java has [resilience4j](https://github.com/resilience4j/resilience4j). .NET has [Polly](https://github.com/App-vNext/Polly). Python has a drawer of single-purpose parts:

| Need | Today in Python | nopanic |
| --- | --- | --- |
| Retry + backoff | tenacity / backoff / stamina | ✅ `retry` |
| Circuit breaker | pybreaker (sync-oriented, aging) | ✅ `circuit_breaker` (sliding window, sync + async) |
| Timeout | roll your own per framework | ✅ `timeout` |
| Client-side rate limit | roll your own token bucket | ✅ `rate_limit` |
| Concurrency bulkhead | raw semaphores | ✅ `bulkhead` |
| Fallback / graceful degradation | try/except sprawl | ✅ `fallback` |
| Hedged requests (tail latency) | nothing | ✅ `hedge` |
| Adaptive (AIMD) rate limiting | nothing | ✅ `adaptive_rate_limit` |
| Cache that survives outages | roll your own | ✅ `cache` (stale-while-failing) |
| Retry budgets (stop retry storms) | nothing | ✅ `RetryBudget` |
| Deadline propagation | nothing | ✅ `deadline` |
| Streaming (LLM tokens) | nothing | ✅ every stateful policy guards generators |
| One breaker / limiter per host or tenant | roll your own | ✅ `keyed` |
| One event stream for all of it | per-library logging | ✅ `events.subscribe` / `events.log_to` |
| **All of the above, composable** | none | ✅ `compose` |

These patterns only pay off when they **compose**: a timeout per attempt, retries that respect an open circuit, a fallback that catches whatever is left. Wiring that out of three libraries with three philosophies is exactly the code nobody wants to own. Every service that calls another service needs this, and in the LLM era *every* app calls flaky, rate-limited, high-latency remote APIs.

## Install

```
pip install nopanic
```

Python 3.10+. No dependencies. Fully typed (`py.typed`).

## The toolkit

Every policy is a decorator that works on both `def` and `async def` functions. Policies with shared state (breaker, bulkhead, rate limit) are created once and applied to every call site that talks to the same dependency. Every policy also takes an optional `name=` that shows up in its events.

### `retry`: try again, politely

```python
from nopanic import retry, backoff

@retry(
    attempts=5,
    on=(ConnectionError, TimeoutError),          # or a predicate: lambda e: ...
    backoff=backoff.full_jitter(base=0.1, cap=30.0),
    giveup=lambda e: getattr(e, "status", 0) == 401,   # don't retry hopeless errors
    before_sleep=lambda a: log.warning("attempt %d failed: %s", a.attempt, a.exception),
)
def fetch(): ...
```

On exhaustion the **original exception is re-raised**, so there is no wrapper to unwrap. Backoff strategies: `fixed`, `exponential`, `full_jitter` (default, AWS-style), `decorrelated_jitter`, or any iterable of floats.

Exceptions carrying a numeric `retry_after` attribute (build one from an HTTP 429's Retry-After header, or let `CircuitOpen` / `RateLimited` provide it) raise the wait to at least that long; disable with `honor_retry_after=False`.

Bound the total time, not just the attempt count, and retry on bad *results* for clients that do not raise:

```python
@retry(attempts=6, max_time=20.0, on_result=lambda r: r.status_code in (429, 502, 503))
async def get(url): return await client.get(url)
```

`max_time` never starts an attempt past the deadline; when `on_result` retries run out, the last result is returned as-is.

#### Retry budgets: no more retry storms

Three attempts per call means up to three times the load on a dependency that is already struggling, from every client at once. A `RetryBudget` caps retries *across* calls, as a fraction of real traffic (the Finagle / gRPC / Envoy approach):

```python
from nopanic import RetryBudget, retry

api_budget = RetryBudget(ratio=0.1)   # retries <= 10% of calls (+1/s floor)

@retry(attempts=4, budget=api_budget)
def get_user(uid): ...

@retry(attempts=4, budget=api_budget)   # one budget per downstream
def get_orders(uid): ...
```

Under a real outage your retry traffic shrinks to the floor instead of multiplying. When the budget is spent, the original exception is re-raised (`retry.gave_up`, reason `budget`).

### `circuit_breaker`: stop hammering what's already down

```python
from nopanic import circuit_breaker, CircuitOpen

payments = circuit_breaker(
    failure_threshold=0.5,   # open at >=50% failures...
    min_calls=10,            # ...once 10 outcomes are in the window
    window=30.0,             # sliding time window (seconds)
    reset_timeout=15.0,      # then let a probe through
    name="payments",
    on_state_change=lambda b, old, new: log.warning("%s: %s -> %s", b.name, old, new),
)

@payments
async def charge(order): ...
```

While open, calls fail instantly with `CircuitOpen(retry_after=...)` instead of stacking up on a dead dependency. One instance = one dependency; decorate as many functions with it as you like, and check `payments.state` anytime.

- **Slow calls trip it too.** `slow_call_duration=2.0, slow_call_threshold=0.5` opens the breaker when half the calls take 2s or more: a dependency that answers too slowly is often as harmful as one that fails.
- **Manual control.** `force_open()` (maintenance, kill switch), `disable()` (let everything through, record nothing), `reset()`.
- **Health checks.** `payments.metrics` gives a consistent snapshot: state, calls, failures, slow calls and both rates.
- **Blocks, not just functions.** `with payments.guard():` / `async with payments.guard():`.

### `timeout`: bound every attempt

```python
@timeout(2.0)
async def lookup(): ...   # cancelled cleanly via the event loop
```

Sync functions run on a worker thread and are *abandoned* (not killed) on timeout. This is documented and deliberate, and still usually what you want at a system boundary. The worker sees the caller's `contextvars`, and a `TimeoutError` raised by your own code passes through untouched. Abandoned workers are capped (`max_abandoned=64` per policy): once that many timed-out calls are still running, new calls fail fast instead of piling up threads behind a hung dependency. `async with timeout(2.0).guard():` bounds a block of async code.

### `rate_limit`: be a good client

```python
rl = rate_limit(90, per=60.0, burst=10)   # 90 calls/min, bursts of 10

@rl
async def embed(text): ...
```

Token bucket; throttled calls **wait** by default (roughly FIFO), and a waiter that is cancelled gives its token back. Set `max_wait` to fail fast with `RateLimited` instead. `rl.available_tokens` shows the bucket; `with rl.guard():` takes a token for a block.

### `bulkhead`: contain the blast radius

```python
bh = bulkhead(20, max_wait=0)   # at most 20 in flight, reject the 21st

@bh
async def render_report(): ...
```

`bh.in_flight` reports current usage; `with bh.guard():` / `async with bh.guard():` holds a slot for a block. Capacity is counted separately for sync callers and for each event loop.

### `fallback`: degrade, don't die

```python
@fallback(lambda exc: CACHED_ANSWER, on=(ConnectionError, CircuitOpen))
async def recommendations(user): ...
```

### `hedge`: fix the p99, not the median

```python
@hedge(delay=0.8, max_hedges=1)
async def complete(prompt): ...   # if slow after 800ms, race a duplicate call
```

The first success wins; losers are cancelled ("The Tail at Scale"). For idempotent async calls only. Pass `on=` with your transient errors so a permanent failure (a 400) propagates at once instead of being raced `max_hedges` more times.

### `adaptive_rate_limit`: find the server's real limit

```python
arl = adaptive_rate_limit(50, per=1.0, min_rate=5)   # start at 50/s, never below 5/s

@arl
async def call_api(): ...
```

Classic AIMD, the scheme TCP uses for congestion control: a 429/503 (or any exception carrying `retry_after`) cuts the rate multiplicatively, every success earns a small additive recovery, and an explicit `Retry-After` blocks the bucket for exactly that long. You converge on whatever rate the server actually sustains instead of hardcoding a guess. Inspect it live via `arl.current_rate`.

### `cache`: an outage is not your outage

```python
@cache(ttl=60.0, stale_ttl=3600.0, on=(ConnectionError, TimeoutError))
def exchange_rates(base): ...
```

Fresh values are served from memory; when a refresh *fails*, the expired value is served for up to `stale_ttl` more seconds instead of the error. Refreshes are single-flight: a hundred concurrent misses on one key make one call to the dependency, not a hundred. LRU-bounded, per-arguments keying, injectable clock.

### `deadline`: one time budget for the whole operation

A timeout bounds one call; a deadline bounds everything inside a block, however many retries, waits and nested calls it takes:

```python
from nopanic import deadline, time_remaining

async with deadline(5.0):
    profile = await get_profile(uid)    # retries, timeouts, rate-limit waits...
    orders = await get_orders(uid)      # ...all share the same 5 seconds
```

Policies inside cooperate: `timeout` clamps to the time remaining, `retry` gives up instead of sleeping past it, and `rate_limit` / `bulkhead` reject instead of queueing past it. Nested deadlines only shorten the budget. Hand the rest to your own clients with `time_remaining()`. `deadline` is also a policy: `compose(deadline(10.0), retry(...), timeout(2.0))` gives every call its own budget.

### Streams: LLM tokens, file chunks, paginated rows

Decorate a generator or async generator function and the policies guard the stream:

```python
@retry(attempts=3, on=ConnectionError)
@timeout(10.0, first_item=30.0)    # 30s to the first token, then 10s per token
@llm_breaker                       # outcome recorded when the stream ends
async def stream_completion(prompt):
    async for token in client.stream(prompt):
        yield token
```

`retry` re-opens a stream that fails *before* its first item; once items were delivered, a failure propagates (they cannot be taken back). `timeout` bounds each item, never the consumer's own time between items. `circuit_breaker`, `bulkhead`, `rate_limit` and `adaptive_rate_limit` treat the whole stream as one call. Policies that cannot guard a stream (`cache`, `fallback`, `hedge`, `deadline`, and `timeout` on a *sync* generator) raise `TypeError` at decoration time instead of silently doing nothing. Close async streams you abandon early (`contextlib.aclosing`), as with any async generator.

### `keyed`: one policy per host, tenant or model

```python
from urllib.parse import urlsplit
from nopanic import keyed, circuit_breaker

per_host = keyed(
    lambda host: circuit_breaker(name=f"api-{host}", reset_timeout=10.0),
    key=lambda url, **_: urlsplit(url).hostname,
)

@per_host
async def fetch(url, **params): ...

per_host.get("eu.api.example.com").state
```

One region going down no longer trips the breaker for every region; one noisy tenant no longer starves the others' rate limit. Instances are created on first use and LRU-bounded (`maxsize`).

## Observability: one stream for everything

```python
import logging
from nopanic import events

events.log_to(logging.getLogger("resilience"))     # one line per event

cancel = events.subscribe(                         # or anything custom
    lambda e: metrics.increment(e.kind, tags={"name": e.name, "fn": e.function})
)
```

Every policy emits typed events (`retry.attempt_failed`, `breaker.state_change`, `ratelimit.throttled`, `cache.stale_served`, ...), each carrying the policy's `name` and the guarded `function`. A listener is all it takes to wire OpenTelemetry, StatsD or metrics counters; listeners can never break the calls they observe, and with no listeners the overhead is a single attribute read. For dashboards and health checks, read state directly: `breaker.metrics`, `bulkhead.in_flight`, `rate_limit.available_tokens`, `adaptive_rate_limit.current_rate`, `timeout.abandoned`, `cache.size`, `RetryBudget.available`.

## Testing code that uses nopanic

```python
from nopanic import testing

def test_checkout_survives_a_flaky_gateway():
    with testing.instant() as sleeps:             # no real backoff or queueing
        assert checkout(order) == "paid"
    assert len(sleeps) == 2

def test_breaker_reports_opening():
    with testing.capture_events("breaker.") as seen:
        ...
```

Stateful policies also take an injectable `clock=` for fully deterministic tests.

## Composition: the whole point

Stack decorators (innermost runs closest to the call), or name the stack once and reuse it:

```python
from nopanic import compose, fallback, retry, circuit_breaker, timeout, backoff, CircuitOpen

llm = circuit_breaker(failure_threshold=0.5, min_calls=8, reset_timeout=20.0, name="llm")

resilient = compose(                              # first = outermost
    fallback(lambda e: "Sorry, try again later.", on=(ConnectionError, CircuitOpen, TimeoutError)),
    retry(attempts=3, on=(ConnectionError, TimeoutError), backoff=backoff.full_jitter(0.2)),
    llm,
    timeout(30.0),
)

@resilient
async def ask(prompt: str) -> str: ...
```

Reading inside-out: each attempt gets 30s -> outcomes feed the breaker -> transient failures retry with jitter -> anything left becomes a graceful answer.

No function to decorate? Guard a one-off call directly; this works on every policy and on `compose` stacks:

```python
user = await resilient.acall(client.users.get, user_id)      # async
rows = payments_breaker.call(db.execute, "SELECT ...")       # sync
```

## Recipe: retrying HTTP the right way

Retry transport errors, timeouts, 429 and 5xx; never retry other 4xx (they are permanent, retrying them is just noise):

```python
import requests
from nopanic import retry, backoff

def _retriable(e):
    if isinstance(e, (requests.ConnectionError, requests.Timeout)):
        return True
    return (isinstance(e, requests.HTTPError) and e.response is not None
            and (e.response.status_code == 429 or e.response.status_code >= 500))

@retry(attempts=4, on=_retriable, backoff=backoff.full_jitter(base=0.2, cap=10.0))
def get_json(url):
    r = requests.get(url, timeout=10)   # always set a transport timeout too
    r.raise_for_status()
    return r.json()
```

## Design principles

- **Zero dependencies.** A resilience library must not be a reliability risk itself.
- **Sync and async are equals.** One API; the wrapper flavour is chosen at decoration time, not per call.
- **No wrapper exceptions.** Your `except ConnectionError:` keeps working; policies only add their own precise signals (`CircuitOpen`, `BulkheadFull`, `RateLimited`).
- **Composition over configuration.** Small orthogonal policies + `compose`, instead of one mega-object with 40 kwargs.
- **Testable time.** Breakers and rate limiters take an injectable `clock`, and `nopanic.testing.instant()` skips every wait, so your test suite never sleeps.
- **Fail loudly at decoration time.** A policy that cannot guard what you decorated (a stream, a sync function for `hedge`) says so immediately.

## Roadmap

- OpenTelemetry helper package on top of `events` (spans + metrics)
- Trio/AnyIO support
- Pluggable shared state (e.g. Redis) for breakers and rate limits across processes, as a separate package
- Documentation site and integration guides (httpx, FastAPI, OpenAI/Anthropic SDKs)

## Performance

Resilience must not become its own latency problem. Per-call overhead on the hot (success) path, single thread, measured with [benchmarks/bench.py](benchmarks/bench.py):

| Policy | Overhead per call |
| --- | --- |
| `retry` | ~0.2 us |
| `rate_limit` | ~0.6 us |
| `adaptive_rate_limit` | ~0.9 us |
| `cache` (fresh hit) | ~0.9 us |
| `circuit_breaker` | ~1.1 us |
| `bulkhead` | ~1.3 us |
| `events.emit`, no listeners | ~0.1 us |

For scale: a fast HTTP round trip costs 5,000+ us, an LLM call millions. Numbers vary by machine; run the benchmark yourself. Design rules that keep it this way: success paths allocate nothing (retry builds its backoff iterator only after the first failure), the breaker's failure window is bucketed counters with constant memory no matter the traffic, event emission with zero listeners is a single attribute read, and locks are held for nanoseconds and never while user code runs.

## Security & hardening

- Zero runtime dependencies means no transitive supply chain; CI runs `pip-audit`, ruff's flake8-bandit rules and a `python -O` smoke check (no `assert` in library code).
- All numeric parameters reject NaN/infinity/booleans at construction time: a bad config fails at import, not by silently never tripping a breaker in production.
- Observability hooks (`before_sleep`, `on_state_change`) and event listeners can never break the call they observe: their exceptions are logged and suppressed.
- Server-controlled delays are capped: a hostile `Retry-After: 999999999` cannot park your client (`retry_after_cap` on `retry`, `max_block` on `adaptive_rate_limit`, both defaulting to 60s).
- `hedge` cancels **and reaps** losing attempts, leaving no orphaned tasks and no "exception was never retrieved" noise.
- Under untrusted load, set `max_wait` on `rate_limit`/`bulkhead` so backpressure becomes fast failure instead of unbounded queueing.
- A hung dependency cannot exhaust threads through sync `timeout`: abandoned workers are capped (`max_abandoned`), beyond which calls fail fast.
- `RetryBudget` bounds retry amplification, so your clients cannot turn a partial outage into a self-inflicted one.

Vulnerability reports: see [SECURITY.md](SECURITY.md). Contributions: [CONTRIBUTING.md](CONTRIBUTING.md).

## For AI coding agents

A compact, machine-oriented API reference lives in [llms.txt](llms.txt). Point your agent at it for the full surface in one read.

## Development

```
pip install -e .[dev]
pytest
ruff check .
mypy src
```

## License

MIT.
