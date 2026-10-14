"""OpenTelemetry for nopanic: metrics and span events for every resilience decision.

Two calls cover it::

    import nopanic_otel

    nopanic_otel.instrument()                   # counters, histograms, span events
    nopanic_otel.observe(payments_breaker,      # live gauges for stateful policies
                         api_bulkhead, api_quota)

``instrument()`` subscribes to :mod:`nopanic.events`. Every event becomes:

- one increment of the ``nopanic.events`` counter, with attributes
  ``nopanic.kind``, ``nopanic.policy``, ``nopanic.name`` and
  ``code.function``;
- a sample of the ``nopanic.retry.delay`` / ``nopanic.ratelimit.wait``
  histograms where the event carries a wait;
- an event on the *current span* (``nopanic.retry.attempt_failed`` and so
  on), so a trace shows every retry, rejection and breaker transition that
  happened inside the request it belongs to.

``observe()`` registers observable gauges read on each collection: breaker
state and rates, bulkhead in-flight calls, rate-limit tokens and adaptive
rate, abandoned timeout workers, cache size, retry budget, and every
instance inside a ``keyed`` policy. Both return a callable that undoes them.

Only ``opentelemetry-api`` is required; configure the SDK (providers,
exporters) as usual in your application.
"""

from __future__ import annotations

import threading
import weakref
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from opentelemetry import metrics, trace
from opentelemetry.metrics import CallbackOptions, MeterProvider, Observation

from nopanic import (
    CircuitBreaker,
    RetryBudget,
    adaptive_rate_limit,
    bulkhead,
    cache,
    keyed,
    rate_limit,
    timeout,
)
from nopanic.events import Event, subscribe

__all__ = ["instrument", "observe"]
__version__ = "0.1.0"

_SCOPE = "nopanic_otel"

# Breaker states as gauge values (resilience4j uses the same encoding idea).
_BREAKER_STATES = {"closed": 0, "half_open": 1, "open": 2, "forced_open": 3, "disabled": 4}

Attributes = dict[str, str | bool | int | float]


def _attribute(value: Any) -> str | bool | int | float | None:
    if value is None:
        return None
    if isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, BaseException):
        return f"{type(value).__qualname__}: {value}"
    return str(value)


def _event_attributes(event: Event) -> Attributes:
    attributes: Attributes = {"nopanic.kind": event.kind, "nopanic.policy": event.policy}
    if event.name is not None:
        attributes["nopanic.name"] = event.name
    if event.function is not None:
        attributes["code.function"] = event.function
    return attributes


def _span_attributes(event: Event, base: Attributes) -> Attributes:
    attributes = dict(base)
    for key, value in event.data.items():
        converted = _attribute(value)
        if converted is not None:
            attributes[f"nopanic.{key}"] = converted
    return attributes


def instrument(
    *,
    meter_provider: MeterProvider | None = None,
    metrics_enabled: bool = True,
    span_events: bool = True,
) -> Callable[[], None]:
    """Export every nopanic event as metrics and/or span events.

    Args:
        meter_provider: Provider to create instruments on; the global one by
            default.
        metrics_enabled: Record the counter and histograms.
        span_events: Add an event to the current span (when it is recording).

    Returns:
        A callable that stops the export.
    """
    meter = (meter_provider or metrics.get_meter_provider()).get_meter(_SCOPE, __version__)
    counter = meter.create_counter(
        "nopanic.events", unit="{event}", description="Resilience events emitted by nopanic"
    )
    retry_delay = meter.create_histogram(
        "nopanic.retry.delay", unit="s", description="Waits scheduled between retry attempts"
    )
    ratelimit_wait = meter.create_histogram(
        "nopanic.ratelimit.wait", unit="s", description="Time callers waited for a token"
    )

    def listener(event: Event) -> None:
        base = _event_attributes(event)
        if metrics_enabled:
            counter.add(1, base)
            if event.kind == "retry.attempt_failed":
                retry_delay.record(float(event.data["delay"]), base)
            elif event.kind == "ratelimit.waited":
                ratelimit_wait.record(float(event.data["seconds"]), base)
        if span_events:
            span = trace.get_current_span()
            if span.is_recording():
                span.add_event(f"nopanic.{event.kind}", _span_attributes(event, base))

    return subscribe(listener)


# -- gauges ---------------------------------------------------------------------


class _Gauges:
    """One set of observable gauges per meter provider, shared by observe() calls."""

    def __init__(self, meter_provider: MeterProvider) -> None:
        self._lock = threading.Lock()
        self._policies: list[tuple[object, str]] = []
        meter = meter_provider.get_meter(_SCOPE, __version__)
        gauges: Mapping[str, tuple[str, str]] = {
            "nopanic.breaker.state": (
                "{state}",
                "0 closed, 1 half_open, 2 open, 3 forced_open, 4 disabled",
            ),
            "nopanic.breaker.failure_rate": ("1", "Failure rate inside the window"),
            "nopanic.breaker.slow_call_rate": ("1", "Slow-call rate inside the window"),
            "nopanic.breaker.calls": ("{call}", "Outcomes inside the window"),
            "nopanic.bulkhead.in_flight": ("{call}", "Calls holding a bulkhead slot"),
            "nopanic.ratelimit.available_tokens": ("{token}", "Tokens in the bucket"),
            "nopanic.ratelimit.rate": ("{call}/s", "Current adaptive rate"),
            "nopanic.timeout.abandoned": ("{thread}", "Abandoned sync timeout workers"),
            "nopanic.cache.size": ("{entry}", "Cached entries"),
            "nopanic.retry_budget.available": ("{retry}", "Retries spendable now"),
        }
        for name, (unit, description) in gauges.items():
            meter.create_observable_gauge(
                name, callbacks=[self._callback(name)], unit=unit, description=description
            )

    def add(self, policy: object, label: str) -> None:
        with self._lock:
            self._policies.append((policy, label))

    def remove(self, policy: object) -> None:
        with self._lock:
            self._policies = [(p, label) for p, label in self._policies if p is not policy]

    def _callback(self, gauge: str) -> Callable[[CallbackOptions], Iterable[Observation]]:
        def callback(options: CallbackOptions) -> Iterable[Observation]:
            with self._lock:
                policies = list(self._policies)
            observations: list[Observation] = []
            for policy, label in policies:
                observations.extend(_observe(gauge, policy, {"nopanic.name": label}))
            return observations

        return callback


def _observe(gauge: str, policy: object, attributes: Attributes) -> list[Observation]:
    if isinstance(policy, keyed):
        found: list[Observation] = []
        live_keys = policy.keys()  # keyed is not a dict: keys() is a snapshot
        for key in live_keys:
            child = {**attributes, "nopanic.key": str(key)}
            found.extend(_observe(gauge, policy.get(key), child))
        return found
    value: float | None = None
    if isinstance(policy, CircuitBreaker):
        if gauge.startswith("nopanic.breaker."):
            m = policy.metrics
            value = {
                "nopanic.breaker.state": float(_BREAKER_STATES.get(m.state, -1)),
                "nopanic.breaker.failure_rate": m.failure_rate,
                "nopanic.breaker.slow_call_rate": m.slow_call_rate,
                "nopanic.breaker.calls": float(m.calls),
            }[gauge]
    elif isinstance(policy, bulkhead):
        if gauge == "nopanic.bulkhead.in_flight":
            value = float(policy.in_flight)
    elif isinstance(policy, (rate_limit, adaptive_rate_limit)):
        if gauge == "nopanic.ratelimit.available_tokens":
            value = policy.available_tokens
        elif gauge == "nopanic.ratelimit.rate" and isinstance(policy, adaptive_rate_limit):
            value = policy.current_rate
    elif isinstance(policy, timeout):
        if gauge == "nopanic.timeout.abandoned":
            value = float(policy.abandoned)
    elif isinstance(policy, cache):
        if gauge == "nopanic.cache.size":
            value = float(policy.size)
    elif isinstance(policy, RetryBudget) and gauge == "nopanic.retry_budget.available":
        value = policy.available
    return [] if value is None else [Observation(value, attributes)]


# One gauge set per provider; weak so test-scoped providers can be collected.
_registry: weakref.WeakKeyDictionary[MeterProvider, _Gauges] = weakref.WeakKeyDictionary()
_registry_lock = threading.Lock()


def observe(
    *policies: object, meter_provider: MeterProvider | None = None
) -> Callable[[], None]:
    """Report live state of stateful policies as observable gauges.

    Accepts circuit breakers, bulkheads, rate limiters (plain and adaptive),
    timeouts, caches, retry budgets and ``keyed`` policies (each instance
    reported with a ``nopanic.key`` attribute). The ``nopanic.name``
    attribute is the policy's ``name``, or its type and position when it
    has none. Returns a callable that stops observing these policies.
    """
    provider = meter_provider or metrics.get_meter_provider()
    with _registry_lock:
        gauges = _registry.get(provider)
        if gauges is None:
            gauges = _registry[provider] = _Gauges(provider)
    for index, policy in enumerate(policies):
        label = getattr(policy, "name", None) or f"{type(policy).__name__}-{index}"
        gauges.add(policy, str(label))

    def stop() -> None:
        for policy in policies:
            gauges.remove(policy)

    return stop
