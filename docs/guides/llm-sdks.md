# LLM SDKs (Anthropic, OpenAI)

LLM APIs are the textbook case for resilience: rate limited, occasionally overloaded, high and variable latency, and streamed. The SDKs retry a little on their own; nopanic gives you the rest: retry budgets, deadlines, a breaker, adaptive rate limits, per-token timeouts and graceful fallbacks.

!!! tip "One retry layer, not two"
    Both SDKs retry connection errors, 429 and 5xx by default (`max_retries=2`). When nopanic's `retry` owns retrying, set `max_retries=0` on the client, so budgets, deadlines and events see every attempt and retries do not multiply.

## Anthropic

```python
import anthropic
from nopanic import (
    CircuitOpen, RetryBudget, adaptive_rate_limit, backoff, circuit_breaker,
    compose, fallback, retry, timeout,
)

client = anthropic.AsyncAnthropic(max_retries=0, timeout=120.0)
MODEL = "claude-opus-5-5"

def transient(exc: BaseException) -> bool:
    """Rate limits, overload (529), server errors and network failures."""
    return isinstance(
        exc, (anthropic.RateLimitError, anthropic.InternalServerError, anthropic.APIConnectionError)
    )

def pushback(exc: BaseException) -> bool:
    """What should slow the adaptive limiter down: 429 and 529 overloaded."""
    return isinstance(exc, anthropic.RateLimitError) or (
        isinstance(exc, anthropic.APIStatusError) and exc.type == "overloaded_error"
    )

def with_retry_after(exc: anthropic.APIStatusError) -> anthropic.APIStatusError:
    """Expose the retry-after header the way nopanic's retry reads it."""
    value = exc.response.headers.get("retry-after")
    try:
        exc.retry_after = float(value) if value else None
    except ValueError:
        exc.retry_after = None
    return exc

claude_budget = RetryBudget(ratio=0.1)
claude_breaker = circuit_breaker(name="anthropic", on=transient, min_calls=10, reset_timeout=30.0)
claude_rate = adaptive_rate_limit(50, per=60.0, min_rate=5, throttle_on=pushback)

resilient = compose(
    fallback(lambda exc: None, on=(CircuitOpen, TimeoutError)),
    retry(attempts=4, on=transient, backoff=backoff.full_jitter(1.0, cap=30.0), budget=claude_budget),
    claude_breaker,
    claude_rate,
    timeout(120.0),
)

@resilient
async def ask(prompt: str) -> str | None:
    try:
        message = await client.messages.create(
            model=MODEL,
            max_tokens=16000,
            messages=[{"role": "user", "content": prompt}],
        )
    except anthropic.APIStatusError as exc:
        raise with_retry_after(exc)
    return "".join(block.text for block in message.content if block.type == "text")
```

`retry` waits at least as long as the `retry-after` header asks (capped at `retry_after_cap`, default 60 s); the adaptive limiter halves its rate on each 429 or 529 and creeps back up on success; the breaker stops the flood when the API is really down; the fallback turns an open breaker or a timeout into `None` for the caller to handle.

### Streaming

Decorate an async generator: `retry` re-opens the stream if it fails before the first token, `timeout` bounds the time to the first token and then each following one, and the breaker records one outcome per stream.

```python
@retry(attempts=3, on=transient, budget=claude_budget)
@timeout(30.0, first_item=90.0)
@claude_breaker
async def stream_answer(prompt: str):
    try:
        async with client.messages.stream(
            model=MODEL,
            max_tokens=64000,
            messages=[{"role": "user", "content": prompt}],
        ) as stream:
            async for text in stream.text_stream:
                yield text
    except anthropic.APIStatusError as exc:
        raise with_retry_after(exc)

async for text in stream_answer("Explain circuit breakers"):
    print(text, end="", flush=True)
```

## OpenAI

The same shape applies to the `openai` package:

```python
import openai
from nopanic import RetryBudget, backoff, circuit_breaker, retry, timeout

client = openai.AsyncOpenAI(max_retries=0)

def transient(exc: BaseException) -> bool:
    return isinstance(
        exc,
        (openai.RateLimitError, openai.InternalServerError,
         openai.APIConnectionError, openai.APITimeoutError),
    )

openai_breaker = circuit_breaker(name="openai", on=transient, min_calls=10)

@retry(attempts=4, on=transient, backoff=backoff.full_jitter(1.0, cap=30.0),
       budget=RetryBudget(ratio=0.1))
@openai_breaker
@timeout(120.0)
async def complete(prompt: str) -> str:
    try:
        response = await client.chat.completions.create(
            model="your-model", messages=[{"role": "user", "content": prompt}]
        )
    except openai.APIStatusError as exc:
        value = exc.response.headers.get("retry-after")
        exc.retry_after = float(value) if value and value.replace(".", "", 1).isdigit() else None
        raise
    return response.choices[0].message.content or ""
```

## Across many workers

Provider quotas are per account, not per process. With several workers, share one budget with [`redis_rate_limit`](../distributed.md) (and, if you like, one breaker with `redis_circuit_breaker`), or use [`keyed`](../policies/keyed.md) to keep a separate limiter per model.
