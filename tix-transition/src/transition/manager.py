"""Serialized dispatch, bounded diagnostics, and explicit state restoration."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable, Iterable
from contextvars import ContextVar
from dataclasses import dataclass
from threading import RLock
from time import perf_counter
from typing import Any

import anyio

from .backend import build_backend
from .definition import Event, MachineDefinition

_DISPATCHING: ContextVar[frozenset[int]] = ContextVar("tix_dispatching", default=frozenset())


class InvalidEvent(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class TransitionRecord:
    sequence: int
    event: str
    source: str
    target: str
    accepted: bool
    duration: float
    error_type: str | None


@dataclass(frozen=True, slots=True)
class Checkpoint:
    definition: str
    version: str
    state: str


class _Manager:
    def __init__(
        self,
        definition: MachineDefinition,
        *,
        asynchronous: bool,
        context: Any = None,
        history_limit: int = 0,
        observers: Iterable[Callable[[TransitionRecord], object]] = (),
    ) -> None:
        if history_limit < 0:
            raise ValueError("history_limit must be nonnegative")
        self.definition = definition
        self.context = context
        self._machine, self._model = build_backend(definition, asynchronous=asynchronous)
        self._history: deque[TransitionRecord] = deque(maxlen=history_limit)
        self._observers = tuple(observers)
        self._sequence = 0

    @property
    def state(self) -> str:
        return str(self._model._tix_state)

    @property
    def is_final(self) -> bool:
        return bool(self._machine.get_state(self.state).final)

    @property
    def history(self) -> tuple[TransitionRecord, ...]:
        return tuple(self._history)

    def available_events(self) -> tuple[str, ...]:
        """Structural availability only; does not run side-effecting guards."""
        return tuple(self._machine.get_triggers(self.state))

    def checkpoint(self) -> Checkpoint:
        return Checkpoint(self.definition.name, self.definition.version, self.state)

    def _restore(self, checkpoint: Checkpoint) -> None:
        if (checkpoint.definition, checkpoint.version) != (
            self.definition.name,
            self.definition.version,
        ):
            raise ValueError("checkpoint definition/version mismatch")
        if checkpoint.state not in self._machine.states:
            raise ValueError("checkpoint state is not defined")
        self._machine.set_state(checkpoint.state, model=self._model)

    def _check_reentrant(self) -> None:
        if id(self) in _DISPATCHING.get():
            raise RuntimeError("reentrant dispatch or restore from a callback")

    def _event(self, name: str, payload: dict[str, Any]) -> Event:
        backend_event = self._machine.events.get(name)
        if backend_event is None or self.state not in backend_event.transitions:
            raise InvalidEvent(f"event {name!r} is unavailable from {self.state!r}")
        return Event(name, self.state, self.context, payload)

    def _finish(
        self, event: Event, accepted: bool, started: float, failure: BaseException | None
    ) -> TransitionRecord:
        self._sequence += 1
        record = TransitionRecord(
            self._sequence,
            event.name,
            event.source,
            self.state,
            accepted,
            perf_counter() - started,
            type(failure).__name__ if failure else None,
        )
        self._history.append(record)
        observer_errors: list[Exception] = []
        for observer in self._observers:
            try:
                observer(record)
            except Exception as error:
                observer_errors.append(error)
        if observer_errors:
            errors = ([failure] if failure else []) + observer_errors
            raise BaseExceptionGroup("dispatch completed; observers failed", errors)
        return record


class TransitionManager(_Manager):
    def __init__(
        self,
        definition: MachineDefinition,
        *,
        context: Any = None,
        history_limit: int = 0,
        observers: Iterable[Callable[[TransitionRecord], object]] = (),
    ) -> None:
        super().__init__(
            definition,
            asynchronous=False,
            context=context,
            history_limit=history_limit,
            observers=observers,
        )
        self._lock = RLock()

    def dispatch(self, name: str, **payload: Any) -> TransitionRecord:
        self._check_reentrant()
        with self._lock:
            event = self._event(name, payload)
            token = _DISPATCHING.set(_DISPATCHING.get() | {id(self)})
            started, accepted, failure = perf_counter(), False, None
            try:
                accepted = bool(self._machine.events[name].trigger(self._model, _tix_event=event))
            except BaseException as error:
                failure = error
                raise
            finally:
                try:
                    record = self._finish(event, accepted, started, failure)
                finally:
                    _DISPATCHING.reset(token)
            return record

    def restore(self, checkpoint: Checkpoint) -> None:
        """Restore state only: no callbacks, external effects, or history replay."""
        self._check_reentrant()
        with self._lock:
            self._restore(checkpoint)


class AsyncTransitionManager(_Manager):
    """Uses the upstream asyncio extension; Trio is intentionally unsupported."""

    def __init__(
        self,
        definition: MachineDefinition,
        *,
        context: Any = None,
        history_limit: int = 0,
        observers: Iterable[Callable[[TransitionRecord], object]] = (),
    ) -> None:
        super().__init__(
            definition,
            asynchronous=True,
            context=context,
            history_limit=history_limit,
            observers=observers,
        )
        self._lock = anyio.Lock()

    async def dispatch(self, name: str, **payload: Any) -> TransitionRecord:
        asyncio.get_running_loop()
        self._check_reentrant()
        async with self._lock:
            event = self._event(name, payload)
            token = _DISPATCHING.set(_DISPATCHING.get() | {id(self)})
            started, accepted, failure = perf_counter(), False, None
            try:
                accepted = bool(
                    await self._machine.events[name].trigger(self._model, _tix_event=event)
                )
            except BaseException as error:
                failure = error
                raise
            finally:
                try:
                    record = self._finish(event, accepted, started, failure)
                finally:
                    _DISPATCHING.reset(token)
            return record

    async def restore(self, checkpoint: Checkpoint) -> None:
        asyncio.get_running_loop()
        self._check_reentrant()
        async with self._lock:
            self._restore(checkpoint)
