"""Components describe ownership without depending on a DI container."""

from __future__ import annotations

import math
from collections.abc import Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

import anyio


class LifecycleState(StrEnum):
    NEW = "new"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class Health:
    healthy: bool
    detail: str = ""


@dataclass(frozen=True, slots=True)
class OwnedResource:
    value: Any = field(repr=False)
    close: Callable[[], Awaitable[object]] = field(repr=False)


class LifecycleContext:
    """Only declared dependencies are visible to a component's hooks."""

    def __init__(
        self,
        resources: Mapping[str, Any],
        allowed: tuple[str, ...],
        shutdown: anyio.Event,
        component: str,
    ) -> None:
        self._resources = resources
        self._allowed = frozenset(allowed)
        self._shutdown = shutdown
        self.component = component

    def get(self, name: str) -> Any:
        if name not in self._allowed:
            raise KeyError(f"{name!r} is not a declared dependency of {self.component!r}")
        value = self._resources[name]
        return value.value if isinstance(value, OwnedResource) else value

    @property
    def shutdown_requested(self) -> bool:
        return self._shutdown.is_set()

    async def wait_shutdown(self) -> None:
        await self._shutdown.wait()

    def request_shutdown(self) -> None:
        self._shutdown.set()


StartHook = Callable[[LifecycleContext], Awaitable[Any]]
StopHook = Callable[[LifecycleContext, Any], Awaitable[object]]
HealthHook = Callable[[LifecycleContext, Any], Awaitable[Health]]


@dataclass(frozen=True, slots=True)
class Component:
    name: str
    start: StartHook = field(repr=False)
    stop: StopHook | None = field(default=None, repr=False)
    requires: tuple[str, ...] = ()
    health: HealthHook | None = field(default=None, repr=False)
    start_timeout: float = 30.0
    stop_timeout: float = 10.0
    health_timeout: float = 5.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "requires", tuple(self.requires))
        if not self.name:
            raise ValueError("component name is required")
        if len(set(self.requires)) != len(self.requires):
            raise ValueError("duplicate component dependency")
        for timeout in (self.start_timeout, self.stop_timeout, self.health_timeout):
            if not math.isfinite(timeout) or timeout <= 0:
                raise ValueError("hook timeouts must be positive and finite")


def resource_component(
    name: str,
    factory: Callable[[LifecycleContext], AbstractAsyncContextManager[Any]],
    *,
    requires: tuple[str, ...] = (),
    start_timeout: float = 30.0,
    stop_timeout: float = 10.0,
) -> Component:
    """Adapt resource contexts, not task groups/cancel scopes requiring task affinity."""

    async def start(context: LifecycleContext) -> OwnedResource:
        manager = factory(context)
        value = await manager.__aenter__()

        async def close() -> object:
            return await manager.__aexit__(None, None, None)

        return OwnedResource(value, close)

    return Component(
        name, start, requires=requires, start_timeout=start_timeout, stop_timeout=stop_timeout
    )
