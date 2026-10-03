"""Narrow adapters around transitions; no duplicate state transition engine."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from types import SimpleNamespace
from typing import Any, cast

from transitions import Machine
from transitions.extensions.asyncio import AsyncMachine, AsyncTransition

from .callbacks import adapt_event, invoke_async, invoke_sync
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
        return invoke_sync(callback, adapt_event(data))

    return invoke


def _async(callback: Callback) -> Callable[[Any], Awaitable[Any]]:
    async def invoke(data: Any) -> Any:
        return await invoke_async(callback, adapt_event(data))

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
            "conditions": [
                adapt(callback) for callback in (*transition.guards, *transition.conditions)
            ],
            "unless": [adapt(callback) for callback in transition.unless],
            "prepare": [adapt(callback) for callback in transition.prepare],
            "before": [adapt(callback) for callback in transition.before],
            "after": [adapt(callback) for callback in transition.after],
        }
        for transition in definition.transitions
    ]
    model = SimpleNamespace()
    # Upstream's async extension inherits synchronous constructor annotations.
    factory: Any = _SerialAsyncMachine if asynchronous else Machine
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
        prepare_event=[adapt(callback) for callback in definition.prepare_event],
        before_state_change=[adapt(callback) for callback in definition.before_state_change],
        after_state_change=[adapt(callback) for callback in definition.after_state_change],
        on_final=[adapt(callback) for callback in definition.on_final],
    )
    return machine, model
