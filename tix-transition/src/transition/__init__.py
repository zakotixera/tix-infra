"""Unmodified pytransitions classes for its full modeling and extension APIs."""

import transitions as native
from transitions import Machine, MachineError
from transitions import extensions as extensions
from transitions.core import Event as NativeEvent
from transitions.core import EventData
from transitions.core import State as NativeState
from transitions.core import Transition as NativeTransition
from transitions.extensions import (
    AsyncMachine,
    GraphMachine,
    HierarchicalAsyncMachine,
    HierarchicalGraphMachine,
    HierarchicalMachine,
    LockedHierarchicalMachine,
    LockedMachine,
    MachineFactory,
)

from .definition import Callback, Event, MachineDefinition, State, Transition
from .manager import (
    AsyncTransitionManager,
    Checkpoint,
    InvalidEvent,
    TransitionManager,
    TransitionRecord,
)

__all__ = [
    "AsyncMachine",
    "AsyncTransitionManager",
    "Callback",
    "Checkpoint",
    "Event",
    "EventData",
    "GraphMachine",
    "HierarchicalAsyncMachine",
    "HierarchicalGraphMachine",
    "HierarchicalMachine",
    "InvalidEvent",
    "LockedHierarchicalMachine",
    "LockedMachine",
    "Machine",
    "MachineDefinition",
    "MachineError",
    "MachineFactory",
    "NativeEvent",
    "NativeState",
    "NativeTransition",
    "State",
    "Transition",
    "TransitionManager",
    "TransitionRecord",
    "extensions",
    "native",
]
