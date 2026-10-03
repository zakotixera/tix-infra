"""Serialized dispatch, bounded diagnostics, and explicit state restoration."""

from __future__ import annotations

from asyncio import CancelledError
from collections import deque
from collections.abc import Callable, Iterable
from contextvars import ContextVar
from dataclasses import dataclass, replace
from time import perf_counter
from types import MappingProxyType
from typing import Any

from .backend import build_backend
from .definition import Event, MachineDefinition, State, Transition

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
        self._asynchronous = asynchronous
        self.context = context
        self._machine, self._model = build_backend(definition, asynchronous=asynchronous)
        self._states = MappingProxyType({state.name: state for state in definition.states})
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

    @property
    def states(self) -> MappingProxyType[str, State]:
        return self._states

    def get_state(self, name: str) -> State:
        """Return the immutable state definition, not a mutable backend object."""
        return self.states[name]

    def is_state(self, name: str) -> bool:
        return self.state == name

    def get_triggers(self, *states: str) -> tuple[str, ...]:
        return tuple(self._machine.get_triggers(*(states or (self.state,))))

    def get_transitions(
        self, trigger: str = "*", source: str = "*", dest: str | None = "*"
    ) -> tuple[Transition, ...]:
        return tuple(
            transition
            for transition in self.definition.transitions
            if self._matching_sources(transition, trigger, source, dest)
        )

    def _matching_sources(
        self, transition: Transition, trigger: str, source: str, dest: str | None
    ) -> tuple[str, ...]:
        if trigger != "*" and transition.event != trigger:
            return ()
        return tuple(
            state
            for state in self.definition.sources(transition)
            if (source == "*" or state == source)
            and (
                dest == "*"
                or transition.target == dest
                or (transition.target == "=" and dest == state)
            )
        )

    def _replace_definition(self, definition: MachineDefinition) -> None:
        """Build before swapping so invalid structural edits leave the machine intact."""
        state = self.state
        if state not in {item.name for item in definition.states}:
            raise ValueError("cannot remove the active state")
        machine, model = build_backend(definition, asynchronous=self._asynchronous)
        machine.set_state(state, model=model)
        self.definition, self._machine, self._model = definition, machine, model
        self._states = MappingProxyType({state.name: state for state in definition.states})

    def _add_states(self, states: tuple[State, ...]) -> None:
        self._replace_definition(
            replace(self.definition, states=(*self.definition.states, *states))
        )

    def _add_transition(self, transition: Transition) -> None:
        self._replace_definition(
            replace(self.definition, transitions=(*self.definition.transitions, transition))
        )

    def _remove_transition(self, trigger: str, source: str, dest: str | None) -> None:
        retained = []
        for transition in self.definition.transitions:
            sources = self.definition.sources(transition)
            removed = set(self._matching_sources(transition, trigger, source, dest))
            remaining = tuple(state for state in sources if state not in removed)
            if remaining == sources:
                retained.append(transition)
            elif remaining:
                retained.append(replace(transition, source=remaining))
        if tuple(retained) != self.definition.transitions:
            self._replace_definition(replace(self.definition, transitions=tuple(retained)))

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

    def _event(self, name: str, payload: dict[str, Any], args: tuple[Any, ...] = ()) -> Event:
        backend_event = self._machine.events.get(name)
        if backend_event is None or self.state not in backend_event.transitions:
            raise InvalidEvent(f"event {name!r} is unavailable from {self.state!r}")
        return Event(name, self.state, self.context, payload, args, state=self.state)

    def _finish(
        self,
        event: Event,
        accepted: bool,
        started: float,
        failure: BaseException | None,
        hook_errors: Iterable[BaseException] = (),
    ) -> TransitionRecord:
        errors = ([failure] if failure is not None else []) + list(hook_errors)
        recorded_error = errors[0] if errors else None
        self._sequence += 1
        record = TransitionRecord(
            self._sequence,
            event.name,
            event.source,
            self.state,
            accepted,
            perf_counter() - started,
            type(recorded_error).__name__ if recorded_error is not None else None,
        )
        self._history.append(record)
        for observer in self._observers:
            try:
                observer(record)
            except BaseException as error:
                errors.append(error)
                if not isinstance(error, Exception):
                    break
        if errors:
            # Preserve native cancellation, including repeated cancellation at a hook.
            if all(isinstance(error, CancelledError) for error in errors) or (
                len(errors) == 1 and (failure is not None or not isinstance(errors[0], Exception))
            ):
                raise errors[0]
            raise BaseExceptionGroup(
                "dispatch completed; completion hooks or observers failed", errors
            )
        return record
