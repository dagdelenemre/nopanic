from __future__ import annotations

import asyncio

import pytest

from nopanic import CircuitBreaker, CircuitOpen, keyed, rate_limit


def test_each_key_gets_its_own_breaker(clock):
    per_host = keyed(
        lambda host: CircuitBreaker(min_calls=1, name=host, clock=clock),
        key=lambda host, path: host,
    )

    @per_host
    def fetch(host, path):
        if host == "down.example":
            raise ConnectionError(host)
        return f"{host}{path}"

    with pytest.raises(ConnectionError):
        fetch("down.example", "/")
    with pytest.raises(CircuitOpen):
        fetch("down.example", "/")
    assert fetch("up.example", "/x") == "up.example/x"  # unaffected
    assert per_host.get("down.example").state == "open"
    assert per_host.get("up.example").state == "closed"
    assert per_host.keys() == ["down.example", "up.example"]


def test_lru_bounds_the_instances():
    made = []

    def factory(key):
        made.append(key)
        return rate_limit(100)

    per_tenant = keyed(factory, key=lambda tenant: tenant, maxsize=2)

    @per_tenant
    def call(tenant):
        return tenant

    for tenant in ["a", "b", "a", "c", "a"]:
        call(tenant)
    assert per_tenant.keys() == ["c", "a"]  # "b" was least recently used
    assert made == ["a", "b", "c"]


def test_async_and_kwargs_keys():
    per_model = keyed(lambda model: rate_limit(1000), key=lambda prompt, *, model: model)

    @per_model
    async def complete(prompt, *, model):
        return f"{model}:{prompt}"

    assert asyncio.run(complete("hi", model="small")) == "small:hi"
    assert per_model.keys() == ["small"]


def test_validation():
    with pytest.raises(ValueError):
        keyed(lambda k: rate_limit(1), key=lambda: 0, maxsize=0)
