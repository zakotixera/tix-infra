"""Run with `uv run python examples/service.py`; no external services required."""

from __future__ import annotations

from dataclasses import dataclass

import anyio
from tix_asyncio import RetryPolicy, TaskManager, retry
from tix_config import ConfigManager, EnvSource, MappingSource
from tix_lifecycle import Component, Health, LifecycleContext, LifecycleManager
from tix_transition import MachineDefinition, State, Transition, TransitionManager


@dataclass(frozen=True)
class Settings:
    concurrency: int

    @classmethod
    def decode(cls, data: dict[str, object]) -> Settings:
        value = data["concurrency"]
        if type(value) is not int or value < 1:
            raise ValueError("concurrency must be a positive integer")
        return cls(value)


async def main() -> None:
    config = ConfigManager(
        [MappingSource({"concurrency": 4}, "defaults"), EnvSource(prefix="TIX_DEMO_")],
        validators=[lambda snapshot: snapshot.decode(Settings.decode)],
    )
    await anyio.to_thread.run_sync(config.reload)
    settings = config.snapshot.decode(Settings.decode)

    workflow = TransitionManager(
        MachineDefinition(
            (State("ready"), State("processing"), State("finished", final=True)),
            (
                Transition("begin", "ready", "processing"),
                Transition("complete", "processing", "finished"),
            ),
            "ready",
            name="demo",
            version="1",
        ),
        history_limit=10,
    )

    async def open_store(context: LifecycleContext) -> dict[int, int]:
        return {}

    async def open_service(context: LifecycleContext) -> dict[int, int]:
        return context.get("store")

    async def check_store(context: LifecycleContext, store: dict[int, int]) -> Health:
        return Health(True, f"{len(store)} records")

    async def close_store(context: LifecycleContext, store: dict[int, int]) -> None:
        print(f"Released store with {len(store)} records")

    lifecycle = LifecycleManager(
        [
            Component("store", open_store, close_store, health=check_store),
            Component("service", open_service, requires=("store",)),
        ]
    )
    async with lifecycle:
        workflow.dispatch("begin")
        store = lifecycle.get("service")

        async def process(value: int) -> int:
            async def operation() -> int:
                await anyio.lowlevel.checkpoint()
                store[value] = value * value
                return store[value]

            return await retry(operation, policy=RetryPolicy(attempts=2))

        # This scope drains work before the lifecycle scope releases dependencies.
        async with TaskManager(concurrency=settings.concurrency) as tasks:
            handles = [await tasks.submit(process, value) for value in range(10)]
        print("Results:", [await handle.result() for handle in handles])
        workflow.dispatch("complete")
        print("Workflow:", workflow.state)
        print("Health:", await lifecycle.health())


if __name__ == "__main__":
    anyio.run(main)
