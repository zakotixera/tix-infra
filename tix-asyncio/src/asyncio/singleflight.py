"""In-flight duplicate suppression without caching or detached tasks."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Hashable
from dataclasses import dataclass, field
from typing import Generic, TypeVar, cast

import anyio

T = TypeVar("T")


class FlightCancelled(Exception):
    """The leader was cancelled; followers may elect to retry."""


@dataclass(slots=True)
class _Flight(Generic[T]):
    owner: int
    done: anyio.Event = field(default_factory=anyio.Event)
    value: T | None = None
    error: BaseException | None = None


class SingleFlight(Generic[T]):
    def __init__(self) -> None:
        self._flights: dict[Hashable, _Flight[T]] = {}

    @property
    def active(self) -> int:
        return len(self._flights)

    async def do(self, key: Hashable, operation: Callable[[], Awaitable[T]]) -> T:
        owner = anyio.get_current_task().id
        existing = self._flights.get(key)
        if existing is not None:
            if existing.owner == owner:
                raise RuntimeError("recursive singleflight for the same key")
            await existing.done.wait()
            if existing.error is not None:
                raise existing.error
            return cast(T, existing.value)
        flight: _Flight[T] = _Flight(owner)
        self._flights[key] = flight
        try:
            flight.value = await operation()
            return flight.value
        except anyio.get_cancelled_exc_class():
            flight.error = FlightCancelled("singleflight leader was cancelled")
            raise
        except BaseException as error:
            flight.error = error
            raise
        finally:
            self._flights.pop(key)
            flight.done.set()
