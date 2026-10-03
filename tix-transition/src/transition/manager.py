"""Serialized sync and async manager entry points."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from dataclasses import replace
from threading import RLock
from time import perf_counter
from typing import Any

import anyio

from .callbacks import complete_async, complete_sync
from .core import _DISPATCHING, _Manager
from .core import Checkpoint as Checkpoint
from .core import InvalidEvent as InvalidEvent
from .core import TransitionRecord as TransitionRecord
from .definition import MachineDefinition, State, Transition


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

    def dispatch(self, name: str, *args: Any, **payload: Any) -> TransitionRecord:
        self._check_reentrant()
        with self._lock:
            event = self._event(name, payload, args)
            token = _DISPATCHING.set(_DISPATCHING.get() | {id(self)})
            started, accepted = perf_counter(), False
            failure: BaseException | None = None
            try:
                try:
                    accepted = bool(
                        self._machine.events[name].trigger(self._model, _tix_event=event)
                    )
                except BaseException as error:
                    failure = error
                errors: list[BaseException] = []
                if self.definition.finalize_event or (
                    failure is not None and self.definition.on_exception
                ):
                    completion = replace(event, state=self.state, result=accepted, error=failure)
                    errors = complete_sync(self.definition, completion)
                return self._finish(event, accepted, started, failure, errors)
            finally:
                _DISPATCHING.reset(token)

    def trigger(self, name: str, *args: Any, **payload: Any) -> bool:
        return self.dispatch(name, *args, **payload).accepted

    def may_trigger(self, name: str, *args: Any, **payload: Any) -> bool:
        """Run upstream preparation and guards without changing state or history."""
        self._check_reentrant()
        with self._lock:
            if name not in self.available_events():
                return False
            event = self._event(name, payload, args)
            token = _DISPATCHING.set(_DISPATCHING.get() | {id(self)})
            try:
                return bool(self._model.may_trigger(name, _tix_event=event))
            finally:
                _DISPATCHING.reset(token)

    def add_states(self, *states: State) -> None:
        self._check_reentrant()
        with self._lock:
            self._add_states(states)

    def add_transition(self, transition: Transition) -> None:
        self._check_reentrant()
        with self._lock:
            self._add_transition(transition)

    def remove_transition(self, trigger: str, source: str = "*", dest: str | None = "*") -> None:
        self._check_reentrant()
        with self._lock:
            self._remove_transition(trigger, source, dest)

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

    async def dispatch(self, name: str, *args: Any, **payload: Any) -> TransitionRecord:
        asyncio.get_running_loop()
        self._check_reentrant()
        async with self._lock:
            event = self._event(name, payload, args)
            token = _DISPATCHING.set(_DISPATCHING.get() | {id(self)})
            started, accepted = perf_counter(), False
            failure: BaseException | None = None
            try:
                try:
                    accepted = bool(
                        await self._machine.events[name].trigger(self._model, _tix_event=event)
                    )
                except BaseException as error:
                    failure = error
                errors: list[BaseException] = []
                if self.definition.finalize_event or (
                    failure is not None and self.definition.on_exception
                ):
                    completion = replace(event, state=self.state, result=accepted, error=failure)
                    errors = await complete_async(self.definition, completion)
                return self._finish(event, accepted, started, failure, errors)
            finally:
                _DISPATCHING.reset(token)

    async def trigger(self, name: str, *args: Any, **payload: Any) -> bool:
        return (await self.dispatch(name, *args, **payload)).accepted

    async def may_trigger(self, name: str, *args: Any, **payload: Any) -> bool:
        asyncio.get_running_loop()
        self._check_reentrant()
        async with self._lock:
            if name not in self.available_events():
                return False
            event = self._event(name, payload, args)
            token = _DISPATCHING.set(_DISPATCHING.get() | {id(self)})
            try:
                return bool(await self._model.may_trigger(name, _tix_event=event))
            finally:
                _DISPATCHING.reset(token)

    async def add_states(self, *states: State) -> None:
        self._check_reentrant()
        async with self._lock:
            self._add_states(states)

    async def add_transition(self, transition: Transition) -> None:
        self._check_reentrant()
        async with self._lock:
            self._add_transition(transition)

    async def remove_transition(
        self, trigger: str, source: str = "*", dest: str | None = "*"
    ) -> None:
        self._check_reentrant()
        async with self._lock:
            self._remove_transition(trigger, source, dest)

    async def restore(self, checkpoint: Checkpoint) -> None:
        asyncio.get_running_loop()
        self._check_reentrant()
        async with self._lock:
            self._restore(checkpoint)
