"""Backoff strategies.

A strategy is any iterable of floats: iterating it yields the delay (in
seconds) to sleep before each successive retry. Strategies are plain frozen
dataclasses, so they are cheap, hashable and safe to share between policies;
each execution of a policy starts a fresh iterator.

``full_jitter`` (capped exponential growth with full jitter, per the AWS
architecture blog) is the recommended default for anything that talks to a
shared service — it avoids retry stampedes.

Jitter deliberately uses the stdlib PRNG (``random``): it shapes load, it is
not a security boundary, and a CSPRNG would add cost for no benefit. The
corresponding lint rule (S311) is suppressed project-wide for this reason.
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from ._core import positive_number

__all__ = [
    "Backoff",
    "as_backoff",
    "decorrelated_jitter",
    "exponential",
    "fixed",
    "full_jitter",
]


@runtime_checkable
class Backoff(Protocol):
    """Anything that can be iterated to produce per-retry delays in seconds."""

    def __iter__(self) -> Iterator[float]: ...


@dataclass(frozen=True, slots=True)
class fixed:
    """The same delay before every retry."""

    delay: float = 1.0

    def __post_init__(self) -> None:
        positive_number("delay", self.delay, allow_zero=True)

    def __iter__(self) -> Iterator[float]:
        while True:
            yield self.delay


@dataclass(frozen=True, slots=True)
class exponential:
    """Deterministic exponential growth: base, base*factor, ... capped at *cap*."""

    base: float = 0.1
    factor: float = 2.0
    cap: float = 30.0

    def __post_init__(self) -> None:
        positive_number("base", self.base, allow_zero=True)
        positive_number("factor", self.factor)
        positive_number("cap", self.cap, allow_zero=True)

    def __iter__(self) -> Iterator[float]:
        delay = self.base
        while True:
            yield min(delay, self.cap)
            delay *= self.factor


@dataclass(frozen=True, slots=True)
class full_jitter:
    """Exponential growth with full jitter: uniform(0, min(cap, base*factor**n)).

    The recommended default against shared services; see "Exponential Backoff
    and Jitter" (AWS Architecture Blog, 2015).
    """

    base: float = 0.1
    factor: float = 2.0
    cap: float = 30.0

    def __post_init__(self) -> None:
        positive_number("base", self.base, allow_zero=True)
        positive_number("factor", self.factor)
        positive_number("cap", self.cap, allow_zero=True)

    def __iter__(self) -> Iterator[float]:
        delay = self.base
        while True:
            yield random.uniform(0.0, min(delay, self.cap))
            delay *= self.factor


@dataclass(frozen=True, slots=True)
class decorrelated_jitter:
    """Decorrelated jitter: each delay is uniform(base, previous*3), capped."""

    base: float = 0.1
    cap: float = 30.0

    def __post_init__(self) -> None:
        positive_number("base", self.base, allow_zero=True)
        positive_number("cap", self.cap, allow_zero=True)

    def __iter__(self) -> Iterator[float]:
        delay = self.base
        while True:
            delay = min(self.cap, random.uniform(self.base, delay * 3))
            yield delay


def as_backoff(value: float | int | Backoff) -> Backoff:
    """Coerce a bare number into a :class:`fixed` strategy."""
    if isinstance(value, bool):
        raise TypeError("backoff must be a number or a Backoff strategy, not a bool")
    if isinstance(value, (int, float)):
        return fixed(float(value))
    return value
