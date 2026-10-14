from __future__ import annotations

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import nopanic_otel
from nopanic import (
    CircuitBreaker,
    RetryBudget,
    bulkhead,
    keyed,
    rate_limit,
    retry,
)


@pytest.fixture
def reader():
    reader = InMemoryMetricReader()
    return reader, MeterProvider(metric_readers=[reader])


def points(reader, name):
    data = reader.get_metrics_data()
    found = []
    if data is None:  # nothing recorded at all
        return found
    for resource in data.resource_metrics:
        for scope in resource.scope_metrics:
            for metric in scope.metrics:
                if metric.name == name:
                    found.extend(metric.data.data_points)
    return found


def test_events_become_counters_and_histograms(reader):
    reader, provider = reader
    stop = nopanic_otel.instrument(meter_provider=provider, span_events=False)
    try:

        @retry(attempts=3, backoff=0.0, name="checkout")
        def down():
            raise ConnectionError("down")

        with pytest.raises(ConnectionError):
            down()
    finally:
        stop()
    counts = {
        dict(p.attributes)["nopanic.kind"]: p.value for p in points(reader, "nopanic.events")
    }
    assert counts == {"retry.attempt_failed": 2, "retry.gave_up": 1}
    attrs = dict(points(reader, "nopanic.events")[0].attributes)
    assert attrs["nopanic.name"] == "checkout"
    assert attrs["code.function"].endswith("down")
    (delays,) = points(reader, "nopanic.retry.delay")
    assert delays.count == 2


def test_stop_unsubscribes(reader):
    reader, provider = reader
    nopanic_otel.instrument(meter_provider=provider, span_events=False)()

    @retry(attempts=2, backoff=0.0)
    def down():
        raise ConnectionError

    with pytest.raises(ConnectionError):
        down()
    assert points(reader, "nopanic.events") == []


def test_span_events_land_on_the_current_span():
    exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = tracer_provider.get_tracer("test")
    stop = nopanic_otel.instrument(metrics_enabled=False)
    try:

        @retry(attempts=2, backoff=0.0)
        def down():
            raise ConnectionError("boom")

        with tracer.start_as_current_span("request"), pytest.raises(ConnectionError):
            down()
    finally:
        stop()
    (span,) = exporter.get_finished_spans()
    names = [e.name for e in span.events]
    assert names == ["nopanic.retry.attempt_failed", "nopanic.retry.gave_up"]
    assert span.events[0].attributes["nopanic.exception"] == "ConnectionError: boom"
    assert span.events[1].attributes["nopanic.reason"] == "attempts"


def test_observe_reports_live_state(reader):
    reader, provider = reader
    now = [0.0]
    breaker = CircuitBreaker(min_calls=1, name="payments", clock=lambda: now[0])
    bh = bulkhead(4, name="reports")
    limiter = rate_limit(10, name="quota")
    budget = RetryBudget(ratio=0.0, min_retries_per_sec=0.5, window=10.0)
    per_host = keyed(lambda host: CircuitBreaker(name=host), key=lambda host: host)

    @per_host
    def fetch(host):
        return host

    fetch("eu")

    @breaker
    def boom():
        raise ConnectionError

    with pytest.raises(ConnectionError):
        boom()

    stop = nopanic_otel.observe(breaker, bh, limiter, budget, per_host, meter_provider=provider)
    try:
        state = {
            dict(p.attributes)["nopanic.name"]: p.value
            for p in points(reader, "nopanic.breaker.state")
        }
        assert state == {"payments": 2, "keyed-4": 0}  # open; the keyed child closed
        keyed_point = [
            p for p in points(reader, "nopanic.breaker.state")
            if dict(p.attributes).get("nopanic.key") == "eu"
        ]
        assert keyed_point
        (in_flight,) = points(reader, "nopanic.bulkhead.in_flight")
        assert in_flight.value == 0
        (tokens,) = points(reader, "nopanic.ratelimit.available_tokens")
        assert tokens.value == pytest.approx(10.0)
        (available,) = points(reader, "nopanic.retry_budget.available")
        assert available.value == pytest.approx(5.0)
    finally:
        stop()
    assert points(reader, "nopanic.bulkhead.in_flight") == []
