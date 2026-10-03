"""Typed callback adaptation, context lookup and exhaustive completion hooks."""

from __future__ import annotations

import inspect
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from .definition import Callback, Event, MachineDefinition


def resolve(callback: Callback, event: Event) -> Any:
    if not isinstance(callback, str):
        return callback
    value = event.context
    for part in callback.split("."):
        value = value[part] if isinstance(value, Mapping) else getattr(value, part)
    return value


def invoke_sync(callback: Callback, event: Event) -> Any:
    resolved = resolve(callback, event)
    result = resolved(event) if callable(resolved) else resolved
    if inspect.isawaitable(result):
        if inspect.iscoroutine(result):
            result.close()
        raise TypeError("async callbacks require AsyncTransitionManager")
    return result


async def invoke_async(callback: Callback, event: Event) -> Any:
    resolved = resolve(callback, event)
    result = resolved(event) if callable(resolved) else resolved
    return await result if inspect.isawaitable(result) else result


def adapt_event(data: Any) -> Event:
    original: Event = data.kwargs["_tix_event"]
    return replace(
        original,
        target=data.transition.dest if data.transition is not None else None,
        state=data.state.name,
        result=data.result,
        error=data.error,
    )


def complete_sync(definition: MachineDefinition, event: Event) -> list[BaseException]:
    errors: list[BaseException] = []
    hooks = (definition.on_exception if event.error is not None else ()) + definition.finalize_event
    for hook in hooks:
        try:
            invoke_sync(hook, event)
        except BaseException as error:
            errors.append(error)
            if not isinstance(error, Exception):
                break
    return errors


async def complete_async(definition: MachineDefinition, event: Event) -> list[BaseException]:
    errors: list[BaseException] = []
    hooks = (definition.on_exception if event.error is not None else ()) + definition.finalize_event
    for hook in hooks:
        try:
            await invoke_async(hook, event)
        except BaseException as error:
            errors.append(error)
            # Stop on cancellation without discarding earlier hook failures.
            if not isinstance(error, Exception):
                break
    return errors
