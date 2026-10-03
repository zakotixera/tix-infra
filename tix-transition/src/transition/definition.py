"""Reusable descriptions with no mutable machine state."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any


@dataclass(frozen=True, slots=True)
class Event:
    name: str
    source: str
    context: Any = field(repr=False)
    payload: Mapping[str, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))


Callback = Callable[[Event], Any]


@dataclass(frozen=True, slots=True)
class State:
    name: str
    on_enter: tuple[Callback, ...] = ()
    on_exit: tuple[Callback, ...] = ()
    final: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "on_enter", tuple(self.on_enter))
        object.__setattr__(self, "on_exit", tuple(self.on_exit))


@dataclass(frozen=True, slots=True)
class Transition:
    event: str
    source: str | tuple[str, ...]
    target: str | None
    guards: tuple[Callback, ...] = ()
    before: tuple[Callback, ...] = ()
    after: tuple[Callback, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.source, str):
            object.__setattr__(self, "source", tuple(self.source))
        for name in ("guards", "before", "after"):
            object.__setattr__(self, name, tuple(getattr(self, name)))


@dataclass(frozen=True, slots=True)
class MachineDefinition:
    states: tuple[State, ...]
    transitions: tuple[Transition, ...]
    initial: str
    name: str = "machine"
    version: str = "1"

    def __post_init__(self) -> None:
        object.__setattr__(self, "states", tuple(self.states))
        object.__setattr__(self, "transitions", tuple(self.transitions))
        names = {state.name for state in self.states}
        if len(names) != len(self.states) or not names or "" in names or "*" in names:
            raise ValueError("state names must be nonempty, unique, and not '*'")
        if self.initial not in names:
            raise ValueError("initial state is not defined")
        if not self.name or not self.version:
            raise ValueError("definition name and version are required")
        finals = {state.name for state in self.states if state.final}
        for transition in self.transitions:
            if not transition.event:
                raise ValueError("event names must not be empty")
            sources = self.sources(transition)
            if not sources or len(set(sources)) != len(sources) or not set(sources) <= names:
                raise ValueError("transition sources must be unique, defined states")
            if transition.target is not None and transition.target not in names:
                raise ValueError("transition target is not defined")
            if finals.intersection(sources):
                raise ValueError("final states cannot have outgoing transitions")

    def sources(self, transition: Transition) -> tuple[str, ...]:
        if transition.source == "*":
            return tuple(state.name for state in self.states if not state.final)
        if isinstance(transition.source, str):
            return (transition.source,)
        return transition.source

    def mermaid(self) -> str:
        identifiers = {state.name: f"s{index}" for index, state in enumerate(self.states)}
        lines = ["stateDiagram-v2"]
        for state in self.states:
            lines.append(f"    state {json.dumps(state.name)} as {identifiers[state.name]}")
            if state.final:
                lines.append(f"    {identifiers[state.name]} --> [*]")
        lines.append(f"    [*] --> {identifiers[self.initial]}")
        for transition in self.transitions:
            for source in self.sources(transition):
                label = transition.event.replace("\n", " ").replace("\r", " ")
                suffix = " (internal)" if transition.target is None else ""
                lines.append(
                    f"    {identifiers[source]} --> {identifiers[transition.target or source]}: "
                    f"{label}{suffix}"
                )
        return "\n".join(lines)
