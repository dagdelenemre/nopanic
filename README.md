# nopanic

**The unified resilience toolkit for Python.**

Retries, circuit breakers, timeouts, rate limits, bulkheads and hedged requests — one decorator-based API, identical for sync and async code, with **zero runtime dependencies**.

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

Java has [resilience4j](https://github.com/resilience4j/resilience4j). .NET has [Polly](https://github.com/App-vNext/Polly). Python has… a drawer of single-purpose parts:

| Need | Today in Python | nopanic |
| --- | --- | --- |
| Retry + backoff | tenacity / backoff / stamina | ✅ `retry` |
| Circuit breaker | pybreaker (sync-oriented, aging) | ✅ `circuit_breaker` (sliding window, sync + async) |
| Timeout | roll your own per framework | ✅ `timeout` |
| Client-side rate limit | roll your own token bucket | ✅ `rate_limit` |
| Concurrency bulkhead | raw semaphores | ✅ `bulkhead` |
| Fallback / graceful degradation | try/except sprawl | ✅ `fallback` |
| Hedged requests (tail latency) | nothing | ✅ `hedge` |
| **All of the above, composable** | — | ✅ `compose` |

These patterns only pay off when they **compose**: a timeout per attempt, retries that respect an open circuit, a fallback that catches whatever is left. Wiring that out of three libraries with three philosophies is exactly the code nobody wants to own. Every service that calls another service needs this — and in the LLM era, *every* app calls flaky, rate-limited, high-latency remote APIs.

## Install

```
pip install nopanic
```

Python 3.10+. No dependencies. Fully typed (`py.typed`).

## The toolkit

Every policy is a decorator that works on both `def` and `async def` functions. Policies with shared state (breaker, bulkhead, rate limit) are created once and applied to every call site that talks to the same dependency.

### `retry` — try again, politely

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

On exhaustion the **original exception is re-raised** — no wrapper to unwrap. Backoff strategies: `fixed`, `exponential`, `full_jitter` (default, AWS-style), `decorrelated_jitter`, or any iterable of floats.

### `circuit_breaker` — stop hammering what's already down

```python
from nopanic import circuit_breaker, CircuitOpen

payments = circuit_breaker(
    failure_threshold=0.5,   # open at ≥50% failures…
    min_calls=10,            # …once 10 outcomes are in the window
    window=30.0,             # sliding time window (seconds)
    reset_timeout=15.0,      # then let a probe through
    name="payments",
    on_state_change=lambda b, old, new: log.warning("%s: %s → %s", b.name, old, new),
)

@payments
async def charge(order): ...
```

While open, calls fail instantly with `CircuitOpen(retry_after=…)` instead of stacking up on a dead dependency. One instance = one dependency; decorate as many functions with it as you like, and check `payments.state` anytime.

### `timeout` — bound every attempt

```python
@timeout(2.0)
async def lookup(): ...   # cancelled cleanly via the event loop
```

Sync functions run on a worker thread and are *abandoned* (not killed) on timeout — documented, deliberate, and still usually what you want at a system boundary.

### `rate_limit` — be a good client

```python
rl = rate_limit(90, per=60.0, burst=10)   # 90 calls/min, bursts of 10

@rl
async def embed(text): ...
```

Token bucket; throttled calls **wait** by default (roughly FIFO). Set `max_wait` to fail fast with `RateLimited` instead.

### `bulkhead` — contain the blast radius

```python
bh = bulkhead(20, max_wait=0)   # at most 20 in flight, reject the 21st

@bh
async def render_report(): ...
```

### `fallback` — degrade, don't die

```python
@fallback(lambda exc: CACHED_ANSWER, on=(ConnectionError, CircuitOpen))
async def recommendations(user): ...
```

### `hedge` — fix the p99, not the median

```python
@hedge(delay=0.8, max_hedges=1)
async def complete(prompt): ...   # if slow after 800ms, race a duplicate call
```

The first success wins; losers are cancelled ("The Tail at Scale"). For idempotent async calls only.

## Composition — the whole point

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

Reading inside-out: each attempt gets 30 s → outcomes feed the breaker → transient failures retry with jitter → anything left becomes a graceful answer.

## Design principles

- **Zero dependencies.** A resilience library must not be a reliability risk itself.
- **Sync and async are equals.** One API; the wrapper flavour is chosen at decoration time, not per call.
- **No wrapper exceptions.** Your `except ConnectionError:` keeps working; policies only add their own precise signals (`CircuitOpen`, `BulkheadFull`, `RateLimited`).
- **Composition over configuration.** Small orthogonal policies + `compose`, instead of one mega-object with 40 kwargs.
- **Testable time.** Breakers and rate limiters take an injectable `clock` — no `sleep()` in your test suite.

## Roadmap

- `on_event` observability hooks with OpenTelemetry helpers
- Adaptive (AIMD) rate limiting driven by 429/`Retry-After`
- Cache policy (stale-while-revalidate fallback)
- Trio/AnyIO support

## Security & hardening

- Zero runtime dependencies — no transitive supply chain; CI runs `pip-audit`, ruff's flake8-bandit rules and a `python -O` smoke check (no `assert` in library code).
- All numeric parameters reject NaN/infinity/booleans at construction time — a bad config fails at import, not by silently never tripping a breaker in production.
- Observability hooks (`before_sleep`, `on_state_change`) can never break the call they observe: their exceptions are logged and suppressed.
- `hedge` cancels **and reaps** losing attempts — no orphaned tasks, no "exception was never retrieved" noise.
- Under untrusted load, set `max_wait` on `rate_limit`/`bulkhead` so backpressure becomes fast failure instead of unbounded queueing.

Vulnerability reports: see [SECURITY.md](SECURITY.md). Contributions: [CONTRIBUTING.md](CONTRIBUTING.md).

## Development

```
pip install -e .[dev]
pytest
ruff check .
mypy src
```

## License

MIT.
