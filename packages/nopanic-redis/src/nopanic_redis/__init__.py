"""Shared state for nopanic: circuit breakers and rate limits across processes.

A ``nopanic.circuit_breaker`` lives in one process. Behind a load balancer
with 40 workers, a dead dependency has to fail 40 times as often before
every worker notices, and a ``rate_limit(100)`` lets 4,000 calls through.
These policies keep that state in Redis instead, so the whole fleet trips
together and shares one quota::

    import redis
    from nopanic_redis import redis_circuit_breaker, redis_rate_limit

    r = redis.Redis()                       # or redis.asyncio.Redis() for async code

    payments = redis_circuit_breaker(r, name="payments", reset_timeout=15.0)
    quota = redis_rate_limit(r, name="openai", rate=500, per=60.0)

    @quota
    @payments
    def charge(order): ...

Every decision is one atomic Lua script (one round trip). Instances with the
same ``name`` and ``prefix`` share state, whichever process or client
created them. Time comes from the caller's wall clock (``time.time``), so
keep hosts NTP-synced; skew only shifts window boundaries slightly.

When Redis itself fails, ``fail_open=True`` (default) lets calls through
unguarded and emits a ``*.store_error`` event: your resilience layer must
not become the outage. ``fail_open=False`` rejects them instead.
"""

from __future__ import annotations

import inspect
import time
from collections.abc import Callable
from typing import Any

from nopanic import CircuitOpen, RateLimited
from nopanic.extend import (
    ExcFilter,
    Policy,
    asleep,
    emit,
    exc_matches,
    positive_int,
    positive_number,
    sleep,
    time_remaining,
)

__all__ = ["redis_circuit_breaker", "redis_rate_limit"]
__version__ = "0.1.0"

_BEFORE = """
local now, reset, max_probes = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local state = redis.call('HGET', KEYS[1], 'state') or 'closed'
local moved = ''
if state == 'open' then
  local opened = tonumber(redis.call('HGET', KEYS[1], 'opened_at') or '0')
  if now - opened >= reset then
    state = 'half_open'
    moved = 'open>half_open'
    redis.call('HSET', KEYS[1], 'state', state, 'inflight', 0, 'successes', 0)
  else
    return {'reject', tostring(reset - (now - opened)), moved}
  end
end
if state == 'half_open' then
  local inflight = tonumber(redis.call('HGET', KEYS[1], 'inflight') or '0')
  if inflight >= max_probes then
    return {'reject', '0', moved}
  end
  redis.call('HINCRBY', KEYS[1], 'inflight', 1)
  return {'half_open', '0', moved}
end
return {state, '0', moved}
"""

_RECORD = """
local now, admitted, outcome = tonumber(ARGV[1]), ARGV[2], ARGV[3]
local span, buckets = tonumber(ARGV[4]), tonumber(ARGV[5])
local min_calls, threshold = tonumber(ARGV[6]), tonumber(ARGV[7])
local max_probes, ttl = tonumber(ARGV[8]), tonumber(ARGV[9])
local state = redis.call('HGET', KEYS[1], 'state') or 'closed'
if admitted == 'half_open' then
  if state ~= 'half_open' then return '' end
  redis.call('HINCRBY', KEYS[1], 'inflight', -1)
  if outcome == 'ignore' then return '' end
  if outcome == 'fail' then
    redis.call('HSET', KEYS[1], 'state', 'open', 'opened_at', ARGV[1])
    return 'half_open>open'
  end
  if redis.call('HINCRBY', KEYS[1], 'successes', 1) >= max_probes then
    redis.call('HSET', KEYS[1], 'state', 'closed')
    redis.call('DEL', KEYS[2])
    return 'half_open>closed'
  end
  return ''
end
if state ~= 'closed' or outcome == 'ignore' then return '' end
local slot = math.floor(now / span)
redis.call('HINCRBY', KEYS[2], slot .. ':c', 1)
if outcome == 'fail' then redis.call('HINCRBY', KEYS[2], slot .. ':f', 1) end
redis.call('EXPIRE', KEYS[2], ttl)
if outcome ~= 'fail' then return '' end
local fields = redis.call('HGETALL', KEYS[2])
local total, fails = 0, 0
for i = 1, #fields, 2 do
  local field = fields[i]
  local sep = string.find(field, ':', 1, true)
  local field_slot = tonumber(string.sub(field, 1, sep - 1))
  if field_slot <= slot - buckets then
    redis.call('HDEL', KEYS[2], field)
  elseif string.sub(field, sep + 1) == 'c' then
    total = total + tonumber(fields[i + 1])
  else
    fails = fails + tonumber(fields[i + 1])
  end
end
if total >= min_calls and fails / total >= threshold then
  redis.call('HSET', KEYS[1], 'state', 'open', 'opened_at', ARGV[1])
  redis.call('DEL', KEYS[2])
  return 'closed>open'
end
return ''
"""

_RESERVE = """
local now, rate, capacity = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local max_wait, ttl = tonumber(ARGV[4]), tonumber(ARGV[5])
local tokens = tonumber(redis.call('HGET', KEYS[1], 't') or ARGV[3])
local updated = tonumber(redis.call('HGET', KEYS[1], 'u') or ARGV[1])
if now > updated then
  tokens = math.min(capacity, tokens + (now - updated) * rate)
  updated = now
end
tokens = tokens - 1
local wait = 0
if tokens < 0 then wait = -tokens / rate end
if max_wait >= 0 and wait > max_wait then
  return {'reject', tostring(wait)}
end
redis.call('HSET', KEYS[1], 't', tostring(tokens), 'u', tostring(updated))
redis.call('EXPIRE', KEYS[1], ttl)
return {'ok', tostring(wait)}
"""

_REFUND = """
local capacity = tonumber(ARGV[1])
local tokens = tonumber(redis.call('HGET', KEYS[1], 't') or ARGV[1])
redis.call('HSET', KEYS[1], 't', tostring(math.min(capacity, tokens + 1)))
return 1
"""


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


class _RedisPolicy(Policy):
    """Sync or async flavour is fixed by the client: ``redis.Redis`` serves
    sync functions, ``redis.asyncio.Redis`` async ones."""

    __slots__ = ("_async", "_client", "_clock", "fail_open", "name", "prefix")

    _KIND = ""

    def __init__(
        self,
        client: Any,
        name: str,
        prefix: str,
        fail_open: bool,
        clock: Callable[[], float],
    ) -> None:
        if not name:
            raise ValueError("name is required: it identifies the shared state in Redis")
        self._client = client
        self._async = inspect.iscoroutinefunction(getattr(client, "execute_command", None))
        self.name = name
        self.prefix = prefix
        self.fail_open = fail_open
        self._clock = clock

    def _key(self, suffix: str) -> str:
        # The {hash tag} keeps all keys of one policy in one Redis Cluster slot.
        return f"{self.prefix}:{self._KIND}:{{{self.name}}}:{suffix}"

    def _need(self, async_call: bool) -> None:
        if async_call and not self._async:
            raise TypeError(f"{self.name}: async functions need a redis.asyncio client")
        if not async_call and self._async:
            raise TypeError(f"{self.name}: sync functions need a synchronous redis client")

    def _store_error(self, fn: Callable[..., Any] | None, exc: Exception) -> None:
        emit(
            f"{self._KIND}.store_error",
            f"redis_{self._KIND}",
            self.name,
            fn,
            exception=exc,
            fail_open=self.fail_open,
        )


class redis_circuit_breaker(_RedisPolicy):
    """A failure-rate circuit breaker whose state is shared through Redis.

    Same model and parameters as :class:`nopanic.CircuitBreaker`: a sliding
    window of ``window_buckets`` time buckets, ``min_calls`` before the rate
    is judged, ``reset_timeout`` open time, then ``half_open_max_calls``
    probes, fleet-wide.

    Args:
        client: ``redis.Redis`` (sync functions) or ``redis.asyncio.Redis``
            (async functions).
        name: Identifies the breaker in Redis; required. Every instance with
            this name (and prefix) is the same breaker.
        prefix: Key prefix (default ``"nopanic"``).
        fail_open: When Redis is unreachable, let calls through unguarded
            (default) instead of rejecting them with ``CircuitOpen``.
        clock: Wall-clock time source shared by all hosts; injectable for
            tests.

    ``get_state()`` and ``reset()`` talk to Redis: they return awaitables
    when the client is async. Streams are not supported.
    """

    __slots__ = (
        "_before",
        "_record",
        "failure_threshold",
        "half_open_max_calls",
        "min_calls",
        "on",
        "reset_timeout",
        "window",
        "window_buckets",
    )

    _KIND = "breaker"

    def __init__(
        self,
        client: Any,
        *,
        name: str,
        failure_threshold: float = 0.5,
        min_calls: int = 5,
        window: float = 30.0,
        window_buckets: int = 10,
        reset_timeout: float = 30.0,
        half_open_max_calls: int = 1,
        on: ExcFilter = Exception,
        prefix: str = "nopanic",
        fail_open: bool = True,
        clock: Callable[[], float] = time.time,
    ) -> None:
        super().__init__(client, name, prefix, fail_open, clock)
        positive_number("failure_threshold", failure_threshold)
        if not 0.0 < failure_threshold <= 1.0:
            raise ValueError("failure_threshold must be in (0, 1]")
        self.failure_threshold = float(failure_threshold)
        self.min_calls = positive_int("min_calls", min_calls)
        self.window = positive_number("window", window)
        self.window_buckets = positive_int("window_buckets", window_buckets)
        self.reset_timeout = positive_number("reset_timeout", reset_timeout, allow_zero=True)
        self.half_open_max_calls = positive_int("half_open_max_calls", half_open_max_calls)
        self.on = on
        self._before = client.register_script(_BEFORE)
        self._record = client.register_script(_RECORD)

    # -- script arguments -----------------------------------------------------

    def _keys(self) -> list[str]:
        return [self._key("state"), self._key("window")]

    def _before_args(self) -> list[Any]:
        return [repr(self._clock()), repr(self.reset_timeout), self.half_open_max_calls]

    def _record_args(self, admitted: str, outcome: str) -> list[Any]:
        return [
            repr(self._clock()),
            admitted,
            outcome,
            repr(self.window / self.window_buckets),
            self.window_buckets,
            self.min_calls,
            repr(self.failure_threshold),
            self.half_open_max_calls,
            int(self.window) + 60,
        ]

    def _transition(self, moved: Any, fn: Callable[..., Any] | None) -> None:
        text = _text(moved)
        if text:
            old, new = text.split(">")
            emit("breaker.state_change", "redis_circuit_breaker", self.name, fn, old=old, new=new)

    def _admit(self, reply: Any, fn: Callable[..., Any]) -> str:
        decision, retry_after, moved = (_text(part) for part in reply)
        self._transition(moved, fn)
        if decision == "reject":
            error = CircuitOpen(self.name, max(0.0, float(retry_after)))
            emit(
                "breaker.rejected",
                "redis_circuit_breaker",
                self.name,
                fn,
                retry_after=error.retry_after,
            )
            raise error
        return decision

    def _unreachable(self, fn: Callable[..., Any], exc: Exception) -> str:
        self._store_error(fn, exc)
        if self.fail_open:
            return "bypass"
        raise CircuitOpen(self.name, 0.0) from exc

    def _outcome(self, exc: BaseException | None) -> str:
        if exc is None:
            return "ok"
        if isinstance(exc, Exception) and exc_matches(exc, self.on):
            return "fail"
        return "ignore"

    # -- sync -----------------------------------------------------------------

    def _report(self, fn: Callable[..., Any], admitted: str, exc: BaseException | None) -> None:
        if admitted == "bypass":
            return
        try:
            moved = self._record(
                keys=self._keys(), args=self._record_args(admitted, self._outcome(exc))
            )
        except Exception as error:  # the store failing must never mask the call's outcome
            self._store_error(fn, error)
            return
        self._transition(moved, fn)

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        self._need(async_call=False)
        try:
            reply = self._before(keys=self._keys(), args=self._before_args())
        except Exception as exc:
            admitted = self._unreachable(fn, exc)
        else:
            admitted = self._admit(reply, fn)
        try:
            result = fn(*args, **kwargs)
        except BaseException as exc:
            self._report(fn, admitted, exc)
            raise
        self._report(fn, admitted, None)
        return result

    # -- async ----------------------------------------------------------------

    async def _areport(
        self, fn: Callable[..., Any], admitted: str, exc: BaseException | None
    ) -> None:
        if admitted == "bypass":
            return
        try:
            moved = await self._record(
                keys=self._keys(), args=self._record_args(admitted, self._outcome(exc))
            )
        except Exception as error:
            self._store_error(fn, error)
            return
        self._transition(moved, fn)

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        self._need(async_call=True)
        try:
            reply = await self._before(keys=self._keys(), args=self._before_args())
        except Exception as exc:
            admitted = self._unreachable(fn, exc)
        else:
            admitted = self._admit(reply, fn)
        try:
            result = await fn(*args, **kwargs)
        except BaseException as exc:
            await self._areport(fn, admitted, exc)
            raise
        await self._areport(fn, admitted, None)
        return result

    # -- inspection -------------------------------------------------------------

    def get_state(self) -> Any:
        """``"closed"``, ``"open"`` or ``"half_open"`` as stored in Redis.

        A breaker whose reset timeout has elapsed still reads ``"open"``
        until the next call moves it to half-open. Returns an awaitable with
        an async client.
        """
        key = self._key("state")
        if self._async:

            async def read() -> str:
                value = await self._client.hget(key, "state")
                return "closed" if value is None else _text(value)

            return read()
        value = self._client.hget(key, "state")
        return "closed" if value is None else _text(value)

    def reset(self) -> Any:
        """Close the breaker and clear its window, fleet-wide."""
        return self._client.delete(*self._keys())


class redis_rate_limit(_RedisPolicy):
    """A token-bucket rate limit shared by every process using it.

    Same semantics as :class:`nopanic.rate_limit`: ``rate`` calls per ``per``
    seconds with bursts up to ``burst``; callers wait for their token by
    default, or raise ``RateLimited`` (with ``retry_after``) when the wait
    would exceed ``max_wait`` or an ambient ``nopanic.deadline``. A caller
    cancelled while waiting gives its token back.

    Args:
        client: ``redis.Redis`` (sync functions) or ``redis.asyncio.Redis``
            (async functions).
        name: Identifies the bucket in Redis; required.
        rate, per, burst, max_wait: As in ``nopanic.rate_limit``.
        prefix: Key prefix (default ``"nopanic"``).
        fail_open: When Redis is unreachable, let calls through (default)
            instead of rejecting them with ``RateLimited``.
        clock: Wall-clock time source shared by all hosts.

    Streams are not supported.
    """

    __slots__ = ("_capacity", "_refill_per_sec", "_refund", "_reserve", "max_wait")

    _KIND = "ratelimit"

    def __init__(
        self,
        client: Any,
        *,
        name: str,
        rate: float,
        per: float = 1.0,
        burst: float | None = None,
        max_wait: float | None = None,
        prefix: str = "nopanic",
        fail_open: bool = True,
        clock: Callable[[], float] = time.time,
    ) -> None:
        super().__init__(client, name, prefix, fail_open, clock)
        rate = positive_number("rate", rate)
        per = positive_number("per", per)
        if burst is not None:
            burst = positive_number("burst", burst)
            if burst < 1:
                raise ValueError("burst must be >= 1")
        self.max_wait = (
            None if max_wait is None else positive_number("max_wait", max_wait, allow_zero=True)
        )
        self._refill_per_sec = rate / per
        self._capacity = burst if burst is not None else rate
        self._reserve = client.register_script(_RESERVE)
        self._refund = client.register_script(_REFUND)

    def _limit(self) -> float:
        remaining = time_remaining()
        limit = self.max_wait
        if remaining is not None:
            remaining = max(0.0, remaining)
            limit = remaining if limit is None else min(limit, remaining)
        return -1.0 if limit is None else limit

    def _reserve_args(self) -> list[Any]:
        ttl = int(self._capacity / self._refill_per_sec) + 60
        return [
            repr(self._clock()),
            repr(self._refill_per_sec),
            repr(self._capacity),
            repr(self._limit()),
            ttl,
        ]

    def _wait_for(self, reply: Any, fn: Callable[..., Any]) -> float:
        decision, wait_text = (_text(part) for part in reply)
        wait = float(wait_text)
        if decision == "reject":
            emit(
                "ratelimit.rejected",
                "redis_rate_limit",
                self.name,
                fn,
                would_wait=wait,
                max_wait=self.max_wait,
            )
            raise RateLimited(
                f"rate limit {self.name!r} would require waiting {wait:.3f}s "
                f"(max_wait={self.max_wait})",
                retry_after=wait,
            )
        if wait > 0:
            emit("ratelimit.waited", "redis_rate_limit", self.name, fn, seconds=wait)
        return wait

    def _unreachable(self, fn: Callable[..., Any], exc: Exception) -> None:
        self._store_error(fn, exc)
        if not self.fail_open:
            raise RateLimited(
                f"rate limit {self.name!r} store unavailable", retry_after=None
            ) from exc

    def _run_sync(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        self._need(async_call=False)
        try:
            reply = self._reserve(keys=[self._key("bucket")], args=self._reserve_args())
        except Exception as exc:
            self._unreachable(fn, exc)
        else:
            wait = self._wait_for(reply, fn)
            if wait > 0:
                try:
                    sleep(wait)
                except BaseException:
                    self._give_back()
                    raise
        return fn(*args, **kwargs)

    def _give_back(self) -> None:
        try:
            self._refund(keys=[self._key("bucket")], args=[repr(self._capacity)])
        except Exception as exc:
            self._store_error(None, exc)

    async def _run_async(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        self._need(async_call=True)
        try:
            reply = await self._reserve(keys=[self._key("bucket")], args=self._reserve_args())
        except Exception as exc:
            self._unreachable(fn, exc)
        else:
            wait = self._wait_for(reply, fn)
            if wait > 0:
                try:
                    await asleep(wait)
                except BaseException:
                    # Cancelled while queued: return the token, but never let
                    # the refund itself hold up the cancellation for long.
                    try:
                        await self._refund(keys=[self._key("bucket")], args=[repr(self._capacity)])
                    except Exception as exc:
                        self._store_error(fn, exc)
                    raise
        return await fn(*args, **kwargs)
