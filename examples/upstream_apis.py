"""Run with `uv run python examples/upstream_apis.py`. No external services."""

from __future__ import annotations

from typing import Any

from tix_asyncio import Event, TaskManager, anyio, asyncio, map_unordered
from tix_transition import (
    HierarchicalMachine,
    MachineDefinition,
    State,
    Transition,
    TransitionManager,
)


async def main() -> None:
    # Real upstream hierarchy and generated trigger methods remain available.
    native = HierarchicalMachine(
        states=["idle", {"name": "work", "children": ["ready", "done"], "initial": "ready"}],
        initial="idle",
        queued=True,
        transitions=[["begin", "idle", "work"], ["finish", "work_ready", "work_done"]],
    )
    native.begin()

    completed: list[int] = []
    workflow = TransitionManager(
        MachineDefinition(
            (State("ready"), State("finished", final=True)),
            (Transition("finish", "ready", "finished", conditions=("can_finish",)),),
            "ready",
            finalize_event=(lambda event: print("Managed result:", event.result),),
        ),
        context={"can_finish": lambda event: len(completed) == event.args[0]},
    )

    async def worker(stop: Event, *, task_status: Any) -> None:
        task_status.started("ready")
        await stop.wait()

    async def square(value: int) -> int:
        # Exact stdlib task semantics; this example uses an asyncio event loop.
        task = asyncio.create_task(asyncio.sleep(0, result=value * value))
        return await task

    stop = Event()
    async with TaskManager(concurrency=2) as tasks:
        assert await tasks.start(worker, stop) == "ready"
        async with map_unordered(square, range(5), concurrency=2) as results:
            async for result in results:
                completed.append(result)
        stop.set()
        await tasks.wait_idle()
    assert workflow.may_trigger("finish", 5)
    assert workflow.trigger("finish", 5)
    native.finish()
    print("Native hierarchy:", native.state)
    print("Streaming results:", sorted(completed))


if __name__ == "__main__":
    anyio.run(main)
