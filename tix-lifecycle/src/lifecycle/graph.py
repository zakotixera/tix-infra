"""Compile dependency layers in O(vertices + edges)."""

from __future__ import annotations

from collections.abc import Iterable

from .component import Component


def compile_graph(components: Iterable[Component]) -> tuple[tuple[Component, ...], ...]:
    components = tuple(components)
    by_name = {component.name: component for component in components}
    if len(by_name) != len(components):
        raise ValueError("duplicate component name")
    incoming = {component.name: len(component.requires) for component in components}
    dependents: dict[str, list[str]] = {name: [] for name in by_name}
    for component in components:
        for dependency in component.requires:
            if dependency not in by_name:
                raise ValueError(f"unknown dependency {dependency!r} of {component.name!r}")
            dependents[dependency].append(component.name)
    ready = [name for name in by_name if incoming[name] == 0]
    layers: list[tuple[Component, ...]] = []
    visited = 0
    while ready:
        layers.append(tuple(by_name[name] for name in ready))
        visited += len(ready)
        next_layer: list[str] = []
        for name in ready:
            for dependent in dependents[name]:
                incoming[dependent] -= 1
                if incoming[dependent] == 0:
                    next_layer.append(dependent)
        ready = next_layer
    if visited != len(components):
        cyclic = [name for name, count in incoming.items() if count]
        raise ValueError(f"dependency cycle involving: {', '.join(cyclic)}")
    return tuple(layers)
