# Writing your own policy

`nopanic.extend` holds the public building blocks the built-in policies and the add-on packages are made of.

```python
from nopanic.extend import Policy, emit

class audit(Policy):
    """Emit an event for every call."""

    __slots__ = ("name",)

    def __init__(self, name=None):
        self.name = name

    def _run_sync(self, fn, args, kwargs):
        emit("audit.call", "audit", self.name, fn)
        return fn(*args, **kwargs)

    async def _run_async(self, fn, args, kwargs):
        emit("audit.call", "audit", self.name, fn)
        return await fn(*args, **kwargs)
```

That is all a policy needs: it now works as a decorator on sync and async functions, in `compose` stacks, and through `call` / `acall`.

## What is available

| Name | Purpose |
| --- | --- |
| `Policy` | Base class: implement `_run_sync` and `_run_async` |
| `emit(kind, policy, name, fn, **data)` | Publish an event to `nopanic.events` listeners |
| `sleep(seconds)`, `asleep(seconds)` | Wait in a way `testing.instant()` can skip (asyncio and Trio) |
| `time_remaining()` | Seconds left in the ambient `deadline`, or None |
| `running_trio()` | True when called from inside `trio.run` |
| `exc_matches(exc, on)`, `ExcFilter` | The `on=` filter logic every policy uses |
| `positive_number`, `positive_int`, `finite_hint` | Parameter and remote-hint validation |
| `fn_name(fn)` | A callable's qualified name |

## Behave like the built-in policies

- Re-raise user exceptions unchanged; raise only your own precise signals.
- Never hold a lock while calling user code (the wrapped function, hooks, listeners).
- Wait only through `sleep` / `asleep`, and never past `time_remaining()`.
- Treat cancellation (`BaseException` that is not an `Exception`) as "no outcome", and release whatever the call held.
- Validate numeric parameters at construction time, and cap anything that comes from a remote side before sleeping on it.
- To guard streams, implement `_run_sync_gen` / `_run_async_gen` and list `"sync"` / `"async"` in the class attribute `_STREAMS`; otherwise decorating a generator fails loudly with `TypeError`, as it should.
