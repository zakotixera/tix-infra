"""Dependency-ordered startup, shielded rollback, and exhaustive release."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from contextvars import ContextVar
from dataclasses import dataclass
from time import perf_counter
from types import TracebackType
from typing import Any

import anyio

from .component import Component, Health, LifecycleContext, LifecycleState, OwnedResource
from .graph import compile_graph

_IN_HOOK: ContextVar[frozenset[int]] = ContextVar("tix_lifecycle_hook", default=frozenset())


class ComponentError(RuntimeError):
    def __init__(self, component: str, phase: str) -> None:
        self.component = component
        self.phase = phase
        super().__init__(f"component {component!r} failed during {phase}")


@dataclass(frozen=True, slots=True)
class LifecycleEvent:
    component: str
    phase: str
    duration: float
    error_type: str | None


class LifecycleManager:
    def __init__(
        self,
        components: Iterable[Component],
        *,
        concurrency: int = 16,
        observers: Iterable[Callable[[LifecycleEvent], object]] = (),
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be positive")
        self._layers = compile_graph(components)
        self._concurrency = concurrency
        self._observers = tuple(observers)
        self._resources: dict[str, Any] = {}
        self._shutdown = anyio.Event()
        self._lock = anyio.Lock()
        self._state = LifecycleState.NEW
        self._context_claimed = False

    @property
    def state(self) -> LifecycleState:
        return self._state

    @property
    def layers(self) -> tuple[tuple[str, ...], ...]:
        return tuple(tuple(component.name for component in layer) for layer in self._layers)

    def get(self, name: str) -> Any:
        if self._state != LifecycleState.RUNNING:
            raise RuntimeError("resources are available to the application only while running")
        resource = self._resources[name]
        return resource.value if isinstance(resource, OwnedResource) else resource

    def request_shutdown(self) -> None:
        self._shutdown.set()

    async def wait_shutdown(self) -> None:
        await self._shutdown.wait()

    def _check_reentrant(self) -> None:
        if id(self) in _IN_HOOK.get():
            raise RuntimeError("lifecycle operations cannot be called from their own hooks")

    def _context(self, component: Component) -> LifecycleContext:
        return LifecycleContext(self._resources, component.requires, self._shutdown, component.name)

    def _notify(self, event: LifecycleEvent) -> list[Exception]:
        errors: list[Exception] = []
        for observer in self._observers:
            try:
                observer(event)
            except Exception as error:
                errors.append(error)
        return errors

    async def _parallel(
        self, components: tuple[Component, ...], operation: Callable[[Component], Awaitable[None]]
    ) -> None:
        pending = iter(components)

        async def worker() -> None:
            for component in pending:
                await operation(component)

        async with anyio.create_task_group() as group:
            for _ in range(min(self._concurrency, len(components))):
                group.start_soon(worker)

    async def _start_one(self, component: Component) -> None:
        token = _IN_HOOK.set(_IN_HOOK.get() | {id(self)})
        started = perf_counter()
        error_type: str | None = None
        failure: BaseException | None = None
        try:
            with anyio.fail_after(component.start_timeout):
                resource = await component.start(self._context(component))
            # There must be no cancellation checkpoint between acquisition and registration.
            self._resources[component.name] = resource
        except BaseException as error:
            error_type = type(error).__name__
            if isinstance(error, Exception):
                failure = ComponentError(component.name, "start")
                failure.__cause__ = error
                raise failure from error
            failure = error
            raise
        finally:
            try:
                errors = self._notify(
                    LifecycleEvent(component.name, "start", perf_counter() - started, error_type)
                )
                if errors:
                    raise BaseExceptionGroup(
                        "startup observers failed", [*([failure] if failure else []), *errors]
                    )
            finally:
                _IN_HOOK.reset(token)

    async def start(self) -> None:
        self._check_reentrant()
        async with self._lock:
            if self._state == LifecycleState.RUNNING:
                return
            if self._state != LifecycleState.NEW:
                raise RuntimeError("lifecycle managers are single-use")
            self._state = LifecycleState.STARTING
            try:
                for layer in self._layers:
                    await self._parallel(layer, self._start_one)
                self._state = LifecycleState.RUNNING
            except BaseException as failure:
                self._shutdown.set()
                try:
                    with anyio.CancelScope(shield=True):
                        errors = await self._cleanup()
                finally:
                    self._state = LifecycleState.FAILED
                if errors:
                    raise BaseExceptionGroup(
                        "startup and rollback failed", [failure, *errors]
                    ) from None
                raise

    async def _stop_one(self, component: Component) -> list[Exception]:
        token = _IN_HOOK.set(_IN_HOOK.get() | {id(self)})
        started = perf_counter()
        failures: list[Exception] = []
        resource = self._resources[component.name]
        value = resource.value if isinstance(resource, OwnedResource) else resource
        try:
            if component.stop is not None:
                try:
                    with anyio.fail_after(component.stop_timeout):
                        await component.stop(self._context(component), value)
                except Exception as error:
                    failure = ComponentError(component.name, "stop")
                    failure.__cause__ = error
                    failures.append(failure)
            if isinstance(resource, OwnedResource):
                try:
                    with anyio.fail_after(component.stop_timeout):
                        await resource.close()
                except Exception as error:
                    failure = ComponentError(component.name, "close")
                    failure.__cause__ = error
                    failures.append(failure)
        finally:
            self._resources.pop(component.name, None)
            try:
                failures.extend(
                    self._notify(
                        LifecycleEvent(
                            component.name,
                            "stop",
                            perf_counter() - started,
                            type(failures[0].__cause__).__name__ if failures else None,
                        )
                    )
                )
            finally:
                _IN_HOOK.reset(token)
        return failures

    async def _cleanup(self) -> list[Exception]:
        errors: list[Exception] = []
        for layer in reversed(self._layers):
            failures: dict[str, list[Exception]] = {}

            async def stop_one(
                component: Component, failures: dict[str, list[Exception]] = failures
            ) -> None:
                failures[component.name] = await self._stop_one(component)

            acquired = tuple(component for component in layer if component.name in self._resources)
            await self._parallel(acquired, stop_one)
            for component in acquired:
                errors.extend(failures[component.name])
        return errors

    async def stop(self) -> None:
        self._check_reentrant()
        self._shutdown.set()
        # Shield lock acquisition too: __aexit__ often runs in an already-cancelled scope.
        with anyio.CancelScope(shield=True):
            async with self._lock:
                if self._state in (LifecycleState.STOPPED, LifecycleState.FAILED):
                    return
                self._shutdown.set()
                self._state = LifecycleState.STOPPING
                try:
                    errors = await self._cleanup()
                except BaseException:
                    self._state = LifecycleState.FAILED
                    raise
                self._state = LifecycleState.FAILED if errors else LifecycleState.STOPPED
                if errors:
                    raise ExceptionGroup("lifecycle shutdown failed", errors)

    async def health(self) -> dict[str, Health]:
        self._check_reentrant()
        async with self._lock:
            if self._state != LifecycleState.RUNNING:
                return {
                    component.name: Health(False, self._state.value)
                    for layer in self._layers
                    for component in layer
                }
            reports: dict[str, Health] = {}

            async def probe(component: Component) -> None:
                token = _IN_HOOK.set(_IN_HOOK.get() | {id(self)})
                try:
                    if component.health is None:
                        reports[component.name] = Health(True)
                    else:
                        with anyio.fail_after(component.health_timeout):
                            report = await component.health(
                                self._context(component), self.get(component.name)
                            )
                        if not isinstance(report, Health):
                            raise TypeError("health hooks must return Health")
                        reports[component.name] = report
                except Exception as error:
                    reports[component.name] = Health(False, type(error).__name__)
                finally:
                    _IN_HOOK.reset(token)

            await self._parallel(tuple(c for layer in self._layers for c in layer), probe)
            return reports

    async def __aenter__(self) -> LifecycleManager:
        if self._state != LifecycleState.NEW or self._context_claimed:
            raise RuntimeError("lifecycle context managers are single-use")
        self._context_claimed = True
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        try:
            await self.stop()
        except BaseException as cleanup_error:
            if exc is not None:
                raise BaseExceptionGroup(
                    "application and shutdown failed", [exc, cleanup_error]
                ) from None
            raise
