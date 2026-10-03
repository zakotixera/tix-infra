"""Process lifecycle orchestration independent of Tix's other managers."""

from .adapters import lifespan, serve
from .component import (
    Component,
    Health,
    LifecycleContext,
    LifecycleState,
    OwnedResource,
    resource_component,
)
from .graph import compile_graph
from .manager import ComponentError, LifecycleEvent, LifecycleManager

__all__ = [
    "Component",
    "ComponentError",
    "Health",
    "LifecycleContext",
    "LifecycleEvent",
    "LifecycleManager",
    "LifecycleState",
    "OwnedResource",
    "compile_graph",
    "lifespan",
    "resource_component",
    "serve",
]
