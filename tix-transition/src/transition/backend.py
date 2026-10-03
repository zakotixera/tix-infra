"""Narrow adapters around transitions; no duplicate state transition engine."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Iterable
from types import SimpleNamespace
from typing import Any, cast

from transitions import Machine
from transitions.extensions.asyncio import AsyncMachine, AsyncTransition

from .definition import Callback, MachineDefinition


class _SerialAsyncTransition(AsyncTransition):
    # Upstream inherits synchronous annotations on its async extension.
    async def _eval_conditions(self, event_data: Any) -> bool:  # type: ignore[override]
        for condition in cast(Iterable[Any], self.conditions):
            if not await condition.check(event_data):
                return False
        return True


class _SerialAsyncMachine(AsyncMachine):
    transition_cls = _SerialAsyncTransition

    @staticmethod
    async def await_all(callables: Iterable[Callable[[], Awaitable[Any]]]) -> list[Any]:
        # Ordered callbacks avoid gather() siblings outliving a failed dispatch.
        return [await callback() for callback in callables]

    async def process_context(self, func: Callable[[], Awaitable[Any]], model: Any) -> Any:
        # Our manager serializes events and disables native queuing. Upstream's
        # process_context converts CancelledError to False; cancellation must escape.
        return await func()


def _sync(callback: Callback) -> Callable[[Any], Any]:
    def invoke(data: Any) -> Any:
        result = callback(data.kwargs["_tix_event"])
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise TypeError("async callbacks require AsyncTransitionManager")
        return result

    return invoke


def _async(callback: Callback) -> Callable[[Any], Awaitable[Any]]:
    async def invoke(data: Any) -> Any:
        result = callback(data.kwargs["_tix_event"])
        return await result if inspect.isawaitable(result) else result

    return invoke


def build_backend(definition: MachineDefinition, *, asynchronous: bool) -> tuple[Any, Any]:
    adapt = _async if asynchronous else _sync
    states = [
        {
            "name": state.name,
            "on_enter": [adapt(callback) for callback in state.on_enter],
            "on_exit": [adapt(callback) for callback in state.on_exit],
            "final": state.final,
        }
        for state in definition.states
    ]
    transitions = [
        {
            "trigger": transition.event,
            "source": list(definition.sources(transition)),
            "dest": transition.target,
            "conditions": [adapt(callback) for callback in transition.guards],
            "before": [adapt(callback) for callback in transition.before],
            "after": [adapt(callback) for callback in transition.after],
        }
        for transition in definition.transitions
    ]
    model = SimpleNamespace()
    factory = _SerialAsyncMachine if asynchronous else Machine
    machine = factory(
        model=model,
        states=states,
        transitions=transitions,
        initial=definition.initial,
        auto_transitions=False,
        send_event=True,
        queued=False,
        ignore_invalid_triggers=False,
        model_attribute="_tix_state",
    )
    return machine, model
